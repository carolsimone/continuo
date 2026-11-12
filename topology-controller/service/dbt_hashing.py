"""The four parts of a dbt node's content_hash and their fold.

A dbt node's content_hash is `fold4(source, shared, config, compiled)`:

- source: a model's or snapshot's `raw_code`, normalised (line endings, outer
  whitespace); a test's compiled SQL, masked; a seed's raw file bytes, as the
  SHA-256 the compile extras recorded, else dbt's own checksum as written.
- shared: `shared_fold` of the code units the node runs that its team writes
  (see `shared_units`): the non-built-in macros its build reaches, the functions
  it calls, a model's unit tests and, for a model, seed or snapshot, the service
  units every build of the project runs (on-run hooks, the root project's
  `generate_schema_name` / `generate_alias_name`, a non-empty flags block).
- compiled: a model's or snapshot's compiled SQL, masked; `""` for every other node.
- config: the SHA-256 of the RFC 8785 canonical JSON of `hashed_config`.

Masking replaces the values a compile writes that change on every run, or with the
target, rather than with the code: the manifest's `invocation_id`, its
`run_started_at` in the ISO "T" form and the space form, and the node's own
`database` as a whole identifier.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from continuo_contracts.canonical import canonical_json, normalise_text
from continuo_contracts.fingerprints import hashed_config_denylist
from continuo_contracts.hashing import fold4, shared_fold

from domain.model import CompileExtras

_INVOCATION = "__CONTINUO_INVOCATION_ID__"
_RUN_STARTED = "__CONTINUO_RUN_STARTED_AT__"
_DATABASE = "__CONTINUO_DATABASE__"

_SOURCE_FROM_RAW_CODE = frozenset({"model", "snapshot"})
_FOLDS_SERVICE_UNITS = frozenset({"model", "seed", "snapshot"})
_MATERIALIZED_BY_TYPE = {"seed": "seed", "snapshot": "snapshot", "test": "test"}
_EXPLICIT_OVERRIDES_FLAG = "require_explicit_package_overrides_for_builtin_materializations"

_JINJA_BLOCK = re.compile(r"\{\{(.*?)\}\}|\{%(.*?)%\}", re.DOTALL)
_JINJA_CALL = re.compile(r"(?<![\w.])([A-Za-z_]\w*)(?:\.([A-Za-z_]\w*))?\s*\(")
_HOOK_ID = re.compile(r"-on-run-(start|end)-\d+$")
_FUNCTION_BODY_KEYS = ("raw_code", "language", "arguments", "returns")
_OVERLOAD_BODY_KEYS = ("arguments", "returns", "raw_body")

# Locality of a materialization macro's package: the root project wins over an
# imported package, which wins over a built-in one.
_ROOT, _IMPORTED, _BUILT_IN = 2, 1, 0


@dataclass(frozen=True)
class CodeUnit:
    """One unit of code a node's shared part folds: its id, the SHA-256 of its
    bytes, its text, the folded macros it calls directly and the name of the
    input it stands for (`macros`, `functions`, `unit_tests`, `on_run_start`, ...)."""
    unit_id: str
    digest: str
    source: str
    depends_on: tuple[str, ...]
    input_name: str


@dataclass(frozen=True)
class RunMask:
    """The values of one compile that a node's rendered text may carry."""
    invocation_id: str
    run_started_at: str

    @classmethod
    def from_metadata(cls, metadata: Mapping) -> RunMask:
        """The manifest metadata's `invocation_id` and `run_started_at`, "" when absent."""
        return cls(
            invocation_id=metadata.get("invocation_id") or "",
            run_started_at=metadata.get("run_started_at") or "",
        )


@dataclass(frozen=True)
class ManifestContext:
    """What hashing a node needs from the manifest and the compile around it.

    `builtin_packages` are dbt's own (`dbt`, `dbt_<adapter_type>`): their macros
    are walked but never folded. They assume the adapter has no parent adapter
    whose package dbt also loads as internal (dbt-postgres and dbt-trino have
    none); an adapter built on another one would have that package's macros folded. `unit_tests_by_node` maps a node id to the unit
    tests that name it; `service_units` are the units every model, seed and
    snapshot folds. The context memoizes each macro's closure, since every node
    of a manifest walks the same macros.
    """
    run: RunMask
    extras: CompileExtras | None
    denylist: frozenset[str]
    root_project: str = ""
    adapter_type: str = ""
    builtin_packages: frozenset[str] = frozenset({"dbt"})
    macros: Mapping[str, dict] = field(default_factory=dict)
    macros_by_name: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    functions: Mapping[str, dict] = field(default_factory=dict)
    unit_tests_by_node: Mapping[str, tuple[CodeUnit, ...]] = field(default_factory=dict)
    service_units: Mapping[str, CodeUnit] = field(default_factory=dict)
    explicit_package_overrides_required: bool = True
    _closures: dict[str, frozenset[str]] = field(default_factory=dict, init=False, compare=False, repr=False)
    _macro_units: dict[str, CodeUnit] = field(default_factory=dict, init=False, compare=False, repr=False)

    @classmethod
    def from_manifest(cls, manifest: Mapping, extras: CompileExtras | None) -> ManifestContext:
        """The context of one manifest and its compile extras. Raises the library's
        `ContractsError` for a flags block or unit test canonical JSON refuses."""
        metadata = manifest.get("metadata") or {}
        adapter_type = metadata.get("adapter_type") or ""
        macros = manifest.get("macros") or {}
        by_name: dict[str, list[str]] = {}
        for macro_id, macro in macros.items():
            by_name.setdefault(_macro_name(macro_id, macro), []).append(macro_id)
        flags = extras.project_flags if extras is not None else {}
        ctx = cls(
            run=RunMask.from_metadata(metadata),
            extras=extras,
            denylist=hashed_config_denylist(),
            root_project=metadata.get("project_name") or "",
            adapter_type=adapter_type,
            builtin_packages=frozenset({"dbt", f"dbt_{adapter_type}"} if adapter_type else {"dbt"}),
            macros=macros,
            macros_by_name={name: tuple(sorted(ids)) for name, ids in by_name.items()},
            functions=manifest.get("functions") or {},
            unit_tests_by_node=_unit_tests_by_node(manifest.get("unit_tests") or {}),
            explicit_package_overrides_required=flags.get(_EXPLICIT_OVERRIDES_FLAG, True) is not False,
        )
        return replace(ctx, service_units=_service_units(manifest, flags, ctx))


def mask_text(text: str, run: RunMask, database: str) -> str:
    """Replace the invocation id, both forms of the run start time and `database`
    as a whole identifier (never inside `warehouse_stage` or `my_warehouse`)."""
    if run.invocation_id:
        text = text.replace(run.invocation_id, _INVOCATION)
    if run.run_started_at:
        text = text.replace(run.run_started_at, _RUN_STARTED)
        text = text.replace(run.run_started_at.replace("T", " ", 1), _RUN_STARTED)
    if database:
        text = re.sub(rf"(?<![A-Za-z0-9_$]){re.escape(database)}(?![A-Za-z0-9_$])", _DATABASE, text)
    return text


def mask_value(value: Any, run: RunMask, database: str) -> Any:
    """`value` with `mask_text` applied to every string inside its dicts and lists."""
    if isinstance(value, str):
        return mask_text(value, run, database)
    if isinstance(value, Mapping):
        return {key: mask_value(item, run, database) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [mask_value(item, run, database) for item in value]
    return value


def drop_empty(value: Any) -> Any:
    """`value` with every `None`, `{}` and `[]` removed, at every depth (a container
    that holds only such values is removed too). `False`, `0` and `""` stay."""
    if isinstance(value, Mapping):
        kept = {key: drop_empty(item) for key, item in value.items()}
        return {key: item for key, item in kept.items() if not _empty(item)}
    if isinstance(value, list | tuple):
        kept_items = [drop_empty(item) for item in value]
        return [item for item in kept_items if not _empty(item)]
    return value


def _empty(value: Any) -> bool:
    return value is None or value == {} or value == []


def hashed_config(node: Mapping, ctx: ManifestContext) -> dict:
    """The node's config as the config part hashes it.

    The denylisted keys are dropped and the rendered values masked; a test's
    `severity` is lower-cased; `relation` carries the schema and alias the node
    writes, in their declared spelling; a model whose contract is enforced adds
    its declared columns in order and its model-level constraints, masked like
    the rest (a foreign key's `to` names the node's database). Nulls and empty
    containers are dropped last.
    """
    database = node.get("database") or ""
    raw_config = node.get("config") or {}
    config = mask_value({k: v for k, v in raw_config.items() if k not in ctx.denylist}, ctx.run, database)
    if node.get("resource_type") == "test" and isinstance(config.get("severity"), str):
        config["severity"] = config["severity"].lower()
    config["relation"] = {"schema": node.get("schema") or "", "alias": node.get("alias") or node.get("name") or ""}
    if _contract_enforced(node):
        config["columns"] = mask_value([
            {"name": c.get("name"), "data_type": c.get("data_type"),
             "constraints": c.get("constraints"), "quote": c.get("quote")}
            for c in (node.get("columns") or {}).values()
        ], ctx.run, database)
        config["constraints"] = mask_value(node.get("constraints"), ctx.run, database)
    return drop_empty(config)


def _contract_enforced(node: Mapping) -> bool:
    """Whether the node is a model whose contract is enforced: dbt enforces contracts
    on models only, so a seed, snapshot or test that sets the flag is not."""
    return node.get("resource_type") == "model" and (
        ((node.get("config") or {}).get("contract") or {}).get("enforced") is True)


def config_part(config: Mapping) -> str:
    """The SHA-256 of the canonical JSON of a hashed config."""
    return hashlib.sha256(canonical_json(config)).hexdigest()


def source_part(node: Mapping, ctx: ManifestContext) -> tuple[str, str, bool]:
    """The node's source part, the name of the input it hashes, and whether the
    part is dbt's `path` checksum (a seed over 1 MiB with no extras entry)."""
    resource_type = node.get("resource_type")
    if resource_type in _SOURCE_FROM_RAW_CODE:
        return _sha256(normalise_text(node.get("raw_code") or "")), "raw_code", False
    if resource_type == "seed":
        seed = ctx.extras.seeds.get(node.get("unique_id", "")) if ctx.extras is not None else None
        if seed is not None:
            return seed.sha256, "seed_bytes", False
        checksum = node.get("checksum") or {}
        return checksum.get("checksum") or "", "seed_checksum", checksum.get("name") == "path"
    code = node.get("compiled_code") or node.get("raw_code") or ""
    if not code:
        return hashlib.sha256(canonical_json(node)).hexdigest(), "compiled_code", False
    return _sha256(mask_text(code, ctx.run, node.get("database") or "")), "compiled_code", False


def compiled_part(node: Mapping, ctx: ManifestContext) -> str:
    """A model's or snapshot's masked compiled SQL, hashed; "" for every other node
    and for a model with no compiled SQL."""
    compiled = node.get("compiled_code") or ""
    if node.get("resource_type") not in _SOURCE_FROM_RAW_CODE or not compiled:
        return ""
    return _sha256(mask_text(compiled, ctx.run, node.get("database") or ""))


def resolve_name(name: str, package: str, ctx: ManifestContext) -> tuple[str, ...]:
    """The macros a bare call of `name` from `package` may run, in dbt's search
    order: the package's own macro of that name (unless the package is built in),
    then the root project's, then every imported package's, then the adapter's and
    dbt's. The first step the manifest holds a macro for answers. The imported step
    answers with every imported package's macro of that name, since dbt picks one of
    them by an order the manifest does not record and folding an extra macro moves
    the hash only when that macro changes. Empty when no macro of that name exists."""
    for owner in (package if package not in ctx.builtin_packages else "", ctx.root_project):
        if owner and f"macro.{owner}.{name}" in ctx.macros:
            return (f"macro.{owner}.{name}",)
    imported = tuple(m for m in ctx.macros_by_name.get(name, ()) if not _is_built_in(m, ctx))
    if imported:
        return imported
    for owner in (f"dbt_{ctx.adapter_type}" if ctx.adapter_type else "", "dbt"):
        if owner and f"macro.{owner}.{name}" in ctx.macros:
            return (f"macro.{owner}.{name}",)
    return ()


def static_macro_calls(raw_code: str, package: str, ctx: ManifestContext) -> list[str]:
    """The macros `raw_code` calls inside its `{{ ... }}` and `{% ... %}` blocks, in
    order of first call. `pkg.name(...)` names `macro.<pkg>.<name>`, a bare call
    is resolved with `resolve_name`; a call the manifest holds no macro for (`ref`,
    `config`, `range`, `adapter.dispatch`) is dropped. This finds a macro a model
    calls only inside a block its compile did not render (`is_incremental()`
    before the table exists)."""
    calls: dict[str, None] = {}
    for block in _JINJA_BLOCK.finditer(raw_code):
        for call in _JINJA_CALL.finditer(block.group(1) or block.group(2) or ""):
            first, second = call.group(1), call.group(2)
            if second:
                named = f"macro.{first}.{second}"
                macro_ids: tuple[str, ...] = (named,) if named in ctx.macros else ()
            else:
                macro_ids = resolve_name(first, package, ctx)
            for macro_id in macro_ids:
                calls.setdefault(macro_id)
    return list(calls)


def materialization_macro(materialized: str, ctx: ManifestContext) -> str | None:
    """The macro dbt builds a `materialized` node with, as dbt chooses it: an
    adapter-specific materialization beats a default one, then the root project
    beats an imported package, which beats a built-in one. An imported package's
    macro is passed over while the project requires explicit overrides of a
    built-in materialization and a built-in package defines one of that name."""
    candidates: list[tuple[tuple[int, int], str]] = []
    for specificity, flavour in ((1, ctx.adapter_type), (0, "default")):
        if not flavour:
            continue
        for macro_id in ctx.macros_by_name.get(f"materialization_{materialized}_{flavour}", ()):
            candidates.append(((specificity, _locality(macro_id, ctx)), macro_id))
    if ctx.explicit_package_overrides_required and any(rank[1] == _BUILT_IN for rank, _ in candidates):
        candidates = [c for c in candidates if c[0][1] != _IMPORTED]
    return max(candidates)[1] if candidates else None


def macro_closure(node: Mapping, ctx: ManifestContext) -> set[str]:
    """The non-built-in macros the node's build reaches.

    The roots are the node's `depends_on.macros`, the static calls of its raw
    code, its materialization and its strategy macro (an incremental model's
    `get_incremental_<strategy>_sql`, a snapshot's `snapshot_<strategy>_strategy`).
    The walk follows every macro's `depends_on.macros`, built-ins included, and on
    every step runs the root project's macro in place of a built-in one of the
    same name, as dbt does."""
    package = node.get("package_name") or ""
    roots = list((node.get("depends_on") or {}).get("macros") or [])
    roots += static_macro_calls(node.get("raw_code") or "", package, ctx)
    materialized = _MATERIALIZED_BY_TYPE.get(node.get("resource_type") or "") \
        or (node.get("config") or {}).get("materialized") or ""
    materialization = materialization_macro(materialized, ctx) if materialized else None
    roots += [materialization] if materialization else []
    roots += _strategy_macros(node, package, ctx)
    return _closure_of_roots(roots, ctx)


def shared_units(node: Mapping, ctx: ManifestContext) -> dict[str, CodeUnit]:
    """The units the node's shared part folds, by unit id: its macro closure, the
    functions it calls (`functions_reached`) with the macro closure of each
    function's `depends_on.macros`, a model's unit tests and, for a model, seed or
    snapshot, the service units. A test folds no service unit."""
    units = _macro_units(macro_closure(node, ctx), ctx)
    for function_id in functions_reached(node, ctx):
        function = ctx.functions[function_id]
        units[function_id] = _function_unit(function_id, function, ctx)
        units.update(_macro_units(_closure_of_roots((function.get("depends_on") or {}).get("macros") or [], ctx), ctx))
    resource_type = node.get("resource_type")
    if resource_type == "model":
        units.update({u.unit_id: u for u in ctx.unit_tests_by_node.get(node.get("unique_id") or "", ())})
    if resource_type in _FOLDS_SERVICE_UNITS:
        units.update(ctx.service_units)
    return units


def functions_reached(node: Mapping, ctx: ManifestContext) -> list[str]:
    """The functions the manifest holds that the node calls: the `function.*` ids of
    its `depends_on.nodes`, and of each reached function's own `depends_on.nodes`,
    each once, in order of first reach."""
    reached: dict[str, None] = {}
    pending = list((node.get("depends_on") or {}).get("nodes") or [])
    while pending:
        node_id = pending.pop(0)
        if node_id in reached or not node_id.startswith("function.") or node_id not in ctx.functions:
            continue
        reached[node_id] = None
        pending += (ctx.functions[node_id].get("depends_on") or {}).get("nodes") or []
    return list(reached)


@dataclass(frozen=True)
class DbtHashParts:
    """A dbt node's four content_hash parts, their fold, the hashed config, the
    units of the shared part, the names of the inputs the parts hash and whether
    the source part is dbt's `path` checksum."""
    source_hash: str
    shared_code_hash: str
    config_hash: str
    compiled_hash: str
    content_hash: str
    hashed_config: dict
    units: Mapping[str, CodeUnit]
    inputs: frozenset[str]
    path_checksum: bool


def hash_dbt_node(node: Mapping, ctx: ManifestContext) -> DbtHashParts:
    """Hash a dbt node. Raises the library's `ContractsError` for a value canonical
    JSON or `shared_fold` refuses and `UnicodeEncodeError` for text UTF-8 cannot
    encode."""
    config = hashed_config(node, ctx)
    source, source_input, path_checksum = source_part(node, ctx)
    units = shared_units(node, ctx)
    shared = shared_fold({u.unit_id: u.digest for u in units.values()})
    config_hash = config_part(config)
    compiled = compiled_part(node, ctx)
    inputs = {source_input, "relation.schema", "relation.alias"}
    inputs |= {f"config.{key}" for key in config if key not in ("relation", "columns", "constraints")}
    if compiled:
        inputs.add("compiled_code")
    if _contract_enforced(node):
        inputs |= {"contract.columns", "constraints"}
    inputs |= {u.input_name for u in units.values()}
    return DbtHashParts(
        source_hash=source,
        shared_code_hash=shared,
        config_hash=config_hash,
        compiled_hash=compiled,
        content_hash=fold4(source, shared, config_hash, compiled),
        hashed_config=config,
        units=units,
        inputs=frozenset(inputs),
        path_checksum=path_checksum,
    )


def _macro_package(macro_id: str, macro: Mapping | None) -> str:
    """A macro's package, from the manifest or else from its `macro.<package>.<name>` id."""
    return (macro or {}).get("package_name") or (macro_id.split(".", 2) + ["", ""])[1]


def _macro_name(macro_id: str, macro: Mapping | None) -> str:
    return (macro or {}).get("name") or (macro_id.split(".", 2) + ["", "", ""])[2]


def _is_built_in(macro_id: str, ctx: ManifestContext) -> bool:
    return _macro_package(macro_id, ctx.macros.get(macro_id)) in ctx.builtin_packages


def _folds(macro_id: str, ctx: ManifestContext) -> bool:
    """Whether a macro is folded: the manifest holds it and its package is not built in."""
    return macro_id in ctx.macros and not _is_built_in(macro_id, ctx)


def _locality(macro_id: str, ctx: ManifestContext) -> int:
    package = _macro_package(macro_id, ctx.macros.get(macro_id))
    if package == ctx.root_project:
        return _ROOT
    return _BUILT_IN if package in ctx.builtin_packages else _IMPORTED


def _runs_as(macro_id: str, ctx: ManifestContext) -> str:
    """The macro that runs for `macro_id`: the root project's macro of the same
    name in place of a built-in one, else `macro_id` itself."""
    if not ctx.root_project or not _is_built_in(macro_id, ctx):
        return macro_id
    override = f"macro.{ctx.root_project}.{_macro_name(macro_id, ctx.macros.get(macro_id))}"
    return override if override in ctx.macros else macro_id


def _closure_of_roots(roots: Iterable[str], ctx: ManifestContext) -> set[str]:
    """The folded macros reached from `roots`, each run as `_runs_as` picks."""
    reached: set[str] = set()
    for root in roots:
        reached |= _closure_of(_runs_as(root, ctx), ctx)
    return reached


def _closure_of(macro_id: str, ctx: ManifestContext) -> frozenset[str]:
    """The folded macros reached from `macro_id` (itself included).

    One depth-first walk (Tarjan's strongly connected components) stores the
    closure of every macro it reaches, not only of `macro_id`: the macros of a
    cycle share one closure, stored once the whole cycle is walked, and a macro
    whose closure is already stored is not walked again.
    """
    memo = ctx._closures
    if macro_id in memo:
        return memo[macro_id]
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    partial: dict[str, set[str]] = {}
    component: list[str] = []
    on_component: set[str] = set()
    work: list[tuple[str, Iterator[str]]] = []

    def enter(current: str) -> None:
        index[current] = low[current] = len(index)
        partial[current] = {current} if _folds(current, ctx) else set()
        component.append(current)
        on_component.add(current)
        work.append((current, iter(_runs_next(current, ctx))))

    enter(macro_id)
    while work:
        current, successors = work[-1]
        descended = False
        for successor in successors:
            if successor in memo:
                partial[current] |= memo[successor]
            elif successor not in index:
                enter(successor)
                descended = True
                break
            elif successor in on_component:
                low[current] = min(low[current], index[successor])
        if descended:
            continue
        work.pop()
        parent = work[-1][0] if work else None
        if low[current] == index[current]:
            members: list[str] = []
            union: set[str] = set()
            while True:
                member = component.pop()
                on_component.discard(member)
                members.append(member)
                union |= partial[member]
                if member == current:
                    break
            closure = frozenset(union)
            for member in members:
                memo[member] = closure
            if parent is not None:
                partial[parent] |= closure
        elif parent is not None:
            low[parent] = min(low[parent], low[current])
            partial[parent] |= partial[current]
    return memo[macro_id]


def _runs_next(macro_id: str, ctx: ManifestContext) -> list[str]:
    """The macros `macro_id` calls, each run as `_runs_as` picks; none for a macro
    the manifest does not hold. A root macro that calls the built-in it overrides
    (`dbt.should_full_refresh()` inside the root `should_full_refresh`) runs that
    built-in itself, so the built-in is walked and what it calls is reached."""
    macro = ctx.macros.get(macro_id)
    if macro is None:
        return []
    runs: list[str] = []
    for dep in (macro.get("depends_on") or {}).get("macros") or []:
        target = _runs_as(dep, ctx)
        runs.append(dep if target == macro_id and dep != macro_id else target)
    return runs


def _macro_units(macro_ids: Iterable[str], ctx: ManifestContext) -> dict[str, CodeUnit]:
    return {macro_id: _macro_unit(macro_id, "macros", ctx) for macro_id in sorted(macro_ids)}


def _function_unit(function_id: str, function: Mapping, ctx: ManifestContext) -> CodeUnit:
    """A function as a unit: the SHA-256 of the canonical JSON of its body record
    (`raw_code`, `language`, `arguments`, `returns` and each overload's
    `arguments`, `returns` and `raw_body`, as far as the manifest carries them,
    its `hashed_config` and its masked `compiled_code`), with every `description`
    dropped, so a documentation edit moves no caller. Raises the library's
    `ContractsError` for a value canonical JSON refuses."""
    body = {key: _without_descriptions(function[key]) for key in _FUNCTION_BODY_KEYS if key in function}
    if "overloads" in function:
        body["overloads"] = [{key: _without_descriptions(overload[key]) for key in _OVERLOAD_BODY_KEYS if key in overload}
                             for overload in function["overloads"] or []]
    body["config"] = hashed_config(function, ctx)
    if isinstance(function.get("compiled_code"), str):
        body["compiled_code"] = mask_text(function["compiled_code"], ctx.run, function.get("database") or "")
    text = canonical_json(body)
    return CodeUnit(function_id, hashlib.sha256(text).hexdigest(), text.decode("utf-8"), (), "functions")


def _macro_unit(macro_id: str, input_name: str, ctx: ManifestContext) -> CodeUnit:
    """The unit of a macro the manifest holds: its `macro_sql` and the folded
    macros it runs directly."""
    key = f"{input_name}\x00{macro_id}"
    unit = ctx._macro_units.get(key)
    if unit is None:
        macro = ctx.macros[macro_id]
        sql = macro.get("macro_sql") or ""
        deps = tuple(dict.fromkeys(d for d in _runs_next(macro_id, ctx) if _folds(d, ctx)))
        unit = CodeUnit(macro_id, _sha256(sql), sql, deps, input_name)
        ctx._macro_units[key] = unit
    return unit


def _strategy_macros(node: Mapping, package: str, ctx: ManifestContext) -> tuple[str, ...]:
    config = node.get("config") or {}
    if node.get("resource_type") == "snapshot":
        strategy = config.get("strategy")
        return resolve_name(f"snapshot_{strategy}_strategy", package, ctx) if strategy else ()
    if node.get("resource_type") == "model" and config.get("materialized") == "incremental":
        return resolve_name(f"get_incremental_{config.get('incremental_strategy') or 'default'}_sql", package, ctx)
    return ()


def _unit_tests_by_node(unit_tests: Mapping[str, dict]) -> dict[str, tuple[CodeUnit, ...]]:
    """Each unit test as a unit, under every node id its `depends_on.nodes` names.
    Its digest is dbt's own checksum, or the SHA-256 of its canonical JSON without
    `created_at` when dbt wrote none; only that fallback can be refused, so a
    fixture value canonical JSON cannot write fails only a unit test without a
    checksum."""
    by_node: dict[str, list[CodeUnit]] = {}
    for unit_id in sorted(unit_tests):
        definition = {k: v for k, v in unit_tests[unit_id].items() if k != "created_at"}
        checksum = definition.get("checksum")
        if isinstance(checksum, str) and checksum:
            digest = checksum
        else:
            digest = hashlib.sha256(canonical_json(definition)).hexdigest()
        source = json.dumps(definition, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        unit = CodeUnit(unit_id, digest, source, (), "unit_tests")
        for node_id in (definition.get("depends_on") or {}).get("nodes") or []:
            by_node.setdefault(node_id, []).append(unit)
    return {node_id: tuple(units) for node_id, units in by_node.items()}


def _service_units(manifest: Mapping, flags: Mapping, ctx: ManifestContext) -> dict[str, CodeUnit]:
    """The units every model, seed and snapshot of the project folds: each on-run
    hook (an operation node) with the macros it reaches (its `depends_on.macros`
    and the static calls of its raw code), the root project's
    `generate_schema_name` and `generate_alias_name`, and the flags block when it
    is not empty."""
    units: dict[str, CodeUnit] = {}
    for node_id, node in (manifest.get("nodes") or {}).items():
        if node.get("resource_type") != "operation":
            continue
        code = node.get("raw_code") or ""
        units[node_id] = CodeUnit(node_id, _sha256(code), code, (), _hook_input(node_id, node))
        roots = list((node.get("depends_on") or {}).get("macros") or [])
        roots += static_macro_calls(code, node.get("package_name") or "", ctx)
        units.update(_macro_units(_closure_of_roots(roots, ctx), ctx))
    for name in ("generate_schema_name", "generate_alias_name"):
        macro_id = f"macro.{ctx.root_project}.{name}"
        if ctx.root_project and macro_id in ctx.macros:
            units[macro_id] = _macro_unit(macro_id, name, ctx)
    if flags:
        text = canonical_json(dict(flags))
        units["project_flags"] = CodeUnit("project_flags", hashlib.sha256(text).hexdigest(), text.decode("utf-8"), (),
                                          "project_flags")
    return units


def _without_descriptions(value: Any) -> Any:
    """`value` with every `description` key removed from its dicts, at every depth."""
    if isinstance(value, Mapping):
        return {key: _without_descriptions(item) for key, item in value.items() if key != "description"}
    if isinstance(value, list | tuple):
        return [_without_descriptions(item) for item in value]
    return value


def _hook_input(node_id: str, node: Mapping) -> str:
    """`on_run_end` or `on_run_start`: from the `-on-run-<kind>-<n>` suffix of the
    hook's id, else from its tags, else `on_run_start`."""
    suffix = _HOOK_ID.search(node_id)
    if suffix is not None:
        return f"on_run_{suffix.group(1)}"
    return "on_run_end" if "on-run-end" in (node.get("tags") or []) else "on_run_start"


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
