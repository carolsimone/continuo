"""Parses a dbt manifest.json into ManifestNodes and the shared code they use.

Every node carries a content_hash folded from its own source, the shared code it
uses (macros, functions, unit tests and the project's service units), its hashed
config and, for a model or snapshot, its compiled SQL (service/dbt_hashing.py
holds the parts and their fold). A seed the compile extras name also carries its
column fingerprint, CSV header and column types, and the parse reports the
service's provenance: the manifest's dbt version, the extras' package versions
and the seeds whose source part is still dbt's `path` checksum. A model, seed or
snapshot a gate leaves out (no owner, a local stub, an untagged node of an
installed dbt package) is reported through the caller's `skipped` list, never
raised. A model or snapshot of the project with an owner and no tag is kept
with an empty schedule (a dormant node); a seed of the project with no tag
takes the `seed` schedule. Which nodes are left out comes from
continuo-contracts' `dbt` module, so a producer's CI and this parse answer alike.

Each node the gates keep gets its ODCS contract, which the library generates from
the node and its tests and the parse checks. The node's run facts (its table,
relation, service, kind, schedule, criticality, owner, file path,
materialization and timeout) are read back from that contract, so the topology
says what the contract says. Its schema keeps the spelling the project
declares, which the contract's id holds in lower case. A node whose contract
fails its checks, whose contract's id is not the node's unique_id, or that the
library cannot read, is not a node of the result but one FailedNode of it, so a
release names every invalid contract at once. A seed the compile extras name is
also checked against its CSV header: a column that the header and the seed's
contract do not both have makes the contract invalid.
"""
import json
import logging
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from typing import Any

from continuo_contracts import dbt
from continuo_contracts.errors import ContractsError, DbtNodeError
from continuo_contracts.validate import Issue

from domain.contract_vocabulary import ParseFailureKind
from domain.exceptions import InvalidNodeSettingError
from domain.model import (
    CompileExtras,
    ContractFamily,
    FailedNode,
    ManifestNode,
    NodeContract,
    NodeType,
    ParsedArtifact,
    SeedExtras,
    ServiceProvenance,
    SkippedNode,
    SkipReason,
    relation_key,
)
from domain.node_timeout import timeout_seconds
from service.compile_extras import seed_columns_hash
from service.contract_validation import ContractValidator
from service.contracts_library import VALIDATOR
from service.dbt_hashing import CodeUnit, DbtHashParts, ManifestContext, hash_dbt_node
from service import manifest_shape
from service.errors import ContractGenerationError
from service.release_timings import ReleaseTimings, Stage

logger = logging.getLogger(__name__)

# What a contract failure that no checked contract explains is counted as: the
# library could not read the manifest entry it generates the contract from.
_GENERATE = "generate"
_MANIFEST_SHAPE = "manifest_shape"

# The stage a failure of a contract's checks is counted under, and the reason of
# a column that a seed's CSV header and its contract do not both have: one
# failure for each such column.
_VALIDATE = "validate"
_SEED_HEADER = "seed_header"

# The generic test whose target `dbt.generate` resolves from `depends_on.nodes`,
# dbt's namespace for its own generic tests, and the macro dbt resolves a bare
# `relationships` to.
_RELATIONSHIPS = "relationships"
_DBT_NAMESPACE = "dbt"
_DBT_RELATIONSHIPS_MACRO = "macro.dbt.test_relationships"


def _no_progress() -> None:
    """The progress callback of a caller nobody watches."""


@contextmanager
def _rejecting_hash_input(label: str) -> Iterator[None]:
    """Raise a value the hashing refuses (a value canonical JSON cannot write or
    `shared_fold` refuses, text UTF-8 cannot encode) as an InvalidNodeSettingError
    naming where it was found: it is the artifact's fault, so the release is
    rejected instead of retried."""
    try:
        yield
    except (ContractsError, UnicodeEncodeError) as error:
        raise InvalidNodeSettingError(f"{label}: {error}") from error


@contextmanager
def _rejecting_node_shape() -> Iterator[None]:
    """Raise a field the library cannot read (an `fqn` that is not a list of
    strings) as an InvalidNodeSettingError carrying the library's message, which
    starts with the node's id: it is the artifact's fault, so the release is
    rejected instead of retried."""
    try:
        yield
    except DbtNodeError as error:
        raise InvalidNodeSettingError(str(error)) from error


def _hash(label: str, node: dict, ctx: ManifestContext) -> DbtHashParts:
    with _rejecting_hash_input(label):
        return hash_dbt_node(node, ctx)


_RESOURCE_TYPE_TO_NODE_TYPE = {
    "model":    NodeType.DBT_MODEL,
    "seed":     NodeType.DBT_SEED,
    "snapshot": NodeType.DBT_SNAPSHOT,
}


def _tolerant_service(node: dict) -> str:
    """The service of a node as the first entry of a well-formed fqn, with `_`
    written `-`, or "" for an fqn of any other shape: the report of a node the
    parse does not build must not raise on the field that kept it out."""
    fqn = node.get("fqn")
    first = fqn[0] if isinstance(fqn, list) and fqn else ""
    return first.replace("_", "-") if isinstance(first, str) else ""


def _skipped(node_id: str, node: dict, reason: SkipReason) -> SkippedNode:
    """The report of a node a gate left out. A node the parse never builds can
    lack an fqn, or carry one of another shape, without raising."""
    return SkippedNode(node_id=node_id, service=_tolerant_service(node), reason=reason)


def _service(node_id: str, node: dict) -> str:
    """The service of a dbt test the parse builds: the first entry of its fqn,
    with `_` written `-`. dbt writes an fqn for every node, so a test with none
    does not say which service it belongs to, and the artifact is refused."""
    with _rejecting_node_shape():
        service = dbt.node_service(node)
    _require_fqn(node_id, node)
    return service


def _require_fqn(node_id: str, node: dict) -> None:
    """Refuse a node the parse builds that has no fqn entry: dbt writes an fqn
    for every node, so such a node does not say which service it belongs to."""
    if not node.get("fqn"):
        raise InvalidNodeSettingError(f"{node_id}: fqn must be a non-empty list of strings")


def _seed_full_refresh(resource_type: str, config: dict) -> bool | None:
    """A seed's config.full_refresh when it is a bool; None for any other value
    and for every node that is not a seed."""
    value = config.get("full_refresh")
    return value if resource_type == "seed" and isinstance(value, bool) else None


def _stage(timings: ReleaseTimings | None, stage: Stage) -> AbstractContextManager[object]:
    """One node's run of `stage` on `timings`, which reads no memory; nothing is
    timed without a record."""
    if timings is None:
        return nullcontext()
    return timings.stage(stage, sample_memory=False, nodes=1)


def _count_failure(timings: ReleaseTimings | None, stage: str, reason: str) -> None:
    if timings is not None:
        timings.add_contract_failure(stage, reason)


def _ignore_cache(hit: bool) -> None:
    """The cache callback of a parse with no timing record."""


def _manifest_call[T](call: Callable[[], T], timings: ReleaseTimings | None) -> T:
    """Run a library call that reads the manifest as a whole (its project, the
    join of its tests to its nodes). A field it cannot read is no one node's
    fault: it is counted and raised as an InvalidNodeSettingError carrying the
    library's message, which starts with `manifest` or the test's id. Any other
    error is a defect of the library, raised as a ContractGenerationError."""
    try:
        return call()
    except DbtNodeError as error:
        _count_failure(timings, _GENERATE, _MANIFEST_SHAPE)
        raise InvalidNodeSettingError(str(error)) from error
    except Exception as error:
        raise ContractGenerationError(f"manifest: the contracts library failed: {error!r}") from error


def _require_readable_nodes(manifest: Mapping[str, Any], timings: ReleaseTimings | None) -> None:
    """Refuse a manifest whose nodes hold a field in a shape the hashing and the parse cannot
    read (see `manifest_shape`): counted like any field the library cannot read, and raised as
    an InvalidNodeSettingError, since the artifact is at fault and a retry cannot fix it."""
    reason = manifest_shape.refusal(manifest["nodes"])
    if reason is not None:
        _count_failure(timings, _GENERATE, _MANIFEST_SHAPE)
        raise InvalidNodeSettingError(reason)


def _resolves_its_target(test: Mapping[str, Any]) -> bool:
    """Whether `dbt.generate` resolves the target of `test` from `depends_on.nodes`: it is
    dbt's own `relationships` test, and it has a tested node (an `attached_node`).

    This is the library's own reading of a test. A generic test named `relationships` is
    dbt's when its namespace is `dbt`, or when it has none and the first entry of its
    `depends_on.macros` is dbt's macro for it (`macro.dbt.test_relationships`): a macro of
    that name that a project or a package writes is not dbt's, and neither is another
    namespace. Without an `attached_node` the library names no target and writes a `custom`
    rule whatever `depends_on.nodes` holds, as it does for a test that is not dbt's. A field
    of another shape is the library's to refuse, with its own message."""
    metadata = test.get("test_metadata")
    depends_on = test.get("depends_on")
    if not isinstance(metadata, Mapping) or not isinstance(depends_on, Mapping):
        return False
    if metadata.get("name") != _RELATIONSHIPS or not test.get("attached_node"):
        return False
    namespace = metadata.get("namespace")
    if namespace:
        return namespace == _DBT_NAMESPACE
    macros = depends_on.get("macros")
    return isinstance(macros, list) and bool(macros) and macros[0] == _DBT_RELATIONSHIPS_MACRO


class _HeldTargets:
    """The tests of a manifest as `dbt.generate` is given them: a relationships test
    whose `depends_on.nodes` names an id that is neither a node nor a source of the
    manifest is given without those ids.

    `generate` resolves the target of a dbt `relationships` test that has an
    `attached_node` from the entry of `depends_on.nodes` beside it, and refuses an entry
    the manifest does not hold. A manifest whose producer left the target out (the
    baseline uploader drops every `local_stub` model) can name one. Without it the test
    names no target, which the library writes as a `custom` rule that keeps the test's
    `to` and `field`. A target the manifest holds stays, so such a test stays strict.
    Only a test `_resolves_its_target` accepts is changed: `generate` resolves the target
    of no other test, and one it already writes as `custom` is given as the very object
    the manifest holds, so the contract's `source_sha256`, which covers every test's
    `depends_on.nodes`, reads what the manifest says. Whether the test is also strict, and
    on a declared column, is the library's to decide: when it is not, the library writes
    the same `custom` rule from the copy.

    A changed test is a shallow copy, made once however often it is given; the manifest
    itself never changes, since its hashes and the parse's own test pass read it as it is.
    `count` is how many tests were given without a target."""

    def __init__(self, manifest: Mapping[str, Any]) -> None:
        self._held = {
            key for kind in ("nodes", "sources")
            if isinstance(entries := manifest.get(kind), Mapping) for key in entries
        }
        self._copies: dict[int, Mapping[str, Any]] = {}

    @property
    def count(self) -> int:
        return len(self._copies)

    def of(self, tests: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
        return [self._test(test) for test in tests]

    def _test(self, test: Mapping[str, Any]) -> Mapping[str, Any]:
        if id(test) in self._copies:
            return self._copies[id(test)]
        if not _resolves_its_target(test):
            return test
        depends_on = test["depends_on"]
        listed = depends_on.get("nodes")
        # A field of another shape is the library's to refuse, with its own message.
        if not isinstance(listed, list) or not all(isinstance(entry, str) for entry in listed):
            return test
        held = [entry for entry in listed if entry in self._held]
        if len(held) == len(listed):
            return test
        without = {**test, "depends_on": {**depends_on, "nodes": held}}
        self._copies[id(test)] = without
        return without


def _failed_node(name: str, node: dict, resource_type: str, detail: str) -> FailedNode:
    """The invalid contract of a node the gates did not leave out. Its service
    and file path are read tolerantly, as the node may hold either in a shape
    the library refused."""
    file_path = node.get("original_file_path")
    return FailedNode(
        node_id=name,
        kind=ParseFailureKind.INVALID_CONTRACT,
        service=_tolerant_service(node),
        file_path=file_path if isinstance(file_path, str) else "",
        node_type=_RESOURCE_TYPE_TO_NODE_TYPE[resource_type],
        detail=detail,
    )


def _shape_failure(
    node_id: str, node: dict, resource_type: str, error: DbtNodeError, timings: ReleaseTimings | None
) -> FailedNode:
    """The invalid contract of a node whose field the library cannot read. It is
    named by its unique_id when the library can read that, else by its manifest
    id, and the detail is the library's message, which starts with the manifest
    id and names the field."""
    _count_failure(timings, _GENERATE, _MANIFEST_SHAPE)
    try:
        name = dbt.unique_id(node)
    except DbtNodeError:
        name = node_id
    return _failed_node(name, node, resource_type, str(error))


def _issues_failure(
    name: str, node_id: str, node: dict, resource_type: str, issues: list[Issue], header_issues: list[str],
    timings: ReleaseTimings | None,
) -> FailedNode:
    """The invalid contract of a node whose generated contract fails its checks,
    or of a seed whose declared columns differ from its CSV header: every issue,
    each counted by its reason.

    The detail lists the contract's issues, each `<path>: <message>`, and then the
    header's messages as the library words them, all joined with `; `, as the lite
    check reports a seed's contract issues before its header lines. A detail with
    header messages starts with the seed's manifest id, which they do not carry,
    as the detail of a node the library cannot read starts with its id."""
    for issue in issues:
        _count_failure(timings, _VALIDATE, issue.reason)
    for _ in header_issues:
        _count_failure(timings, _VALIDATE, _SEED_HEADER)
    detail = "; ".join([*(str(issue) for issue in issues), *header_issues])
    if header_issues:
        detail = f"{node_id}: {detail}"
    return _failed_node(name, node, resource_type, detail)


def _library_defect(node_id: str, error: Exception) -> ContractGenerationError:
    return ContractGenerationError(f"{node_id}: the contracts library failed: {error!r}")


def _topology_facts(node_id: str, facts: dbt.RunFacts) -> tuple[NodeType, int | None]:
    """The node type and the task timeout in seconds that a valid contract's run
    facts give. The library writes only a kind and a timeout its checks accept, so
    a value the topology cannot take is a defect, raised as a
    ContractGenerationError."""
    try:
        return NodeType(facts.kind), timeout_seconds(facts.timeout_minutes)
    except ValueError as error:
        raise _library_defect(node_id, error) from error


def _id_issue(node: dict, document: Mapping[str, Any]) -> Issue | None:
    """The issue of a generated contract whose id is not the unique_id continuo gives
    its node, or None when the two are equal.

    The library lowers `<schema>.<table>` as one string, while the node's unique_id
    lowers the declared schema and the table each on its own (relation_key). The two
    differ only where `str.lower` writes a capital Σ in another form, such as a schema
    that ends in one (`DATAΣ.Orders`). The bundle keys a contract by its node's
    unique_id, and a reader of the bundle refuses an entry whose document carries
    another id, so such a contract is invalid. Its id is written by the library, so the
    team renames the schema or the table. Counted as the library's `identity` reason."""
    contract_id = document["id"]
    schema, table = node["schema"], document["name"]
    node_key = relation_key(schema, table)
    if contract_id == node_key:
        return None
    return Issue(
        "id", "identity",
        f"the contract's id is {contract_id!r}, and continuo names its node {node_key!r}: the schema"
        f" {schema!r} and the table {table!r} lower-case differently apart than joined, so rename the schema"
        " or the table",
    )


def _header_issues(node_id: str, document: Mapping[str, Any], header: Sequence[str] | None) -> list[str]:
    """What a seed's CSV `header` and the columns its generated `document` declares
    disagree on, in the library's words; none for a seed with no known header."""
    if header is None:
        return []
    try:
        declared = [column["name"] for column in document["schema"][0].get("properties", [])]
        return dbt.seed_header_issues(declared, header)
    except Exception as error:
        raise _library_defect(node_id, error) from error


def _contract(
    node_id: str,
    node: dict,
    resource_type: str,
    tests: list[Mapping[str, Any]],
    manifest: Mapping[str, Any],
    *,
    seed: SeedExtras | None,
    validator: ContractValidator,
    timings: ReleaseTimings | None,
) -> tuple[dbt.Generated, dbt.RunFacts, str | None] | FailedNode:
    """The checked contract of a kept node, the run facts it gives and, for a seed
    the compile extras name (`seed`, None for any other node), the fingerprint of
    its columns; or the FailedNode that says why the node has none.

    A field the library cannot read, a contract with issues, a contract whose id is
    not the node's unique_id and a seed whose declared columns differ from the CSV
    header its extras give are the team's to fix, so each is a FailedNode. A node
    with more than one of the last three gets one FailedNode that names every
    problem. The seed's extras are fingerprinted as soon as the contract is
    generated, which refuses text canonical JSON cannot write (an
    InvalidNodeSettingError naming the node) before any header text is compared
    or put in a failed node. Any other error from the library is a defect, raised
    as a ContractGenerationError: `generate` never returns a contract canonical
    JSON cannot write, so the check failing on one is a defect too."""
    try:
        with _stage(timings, Stage.CONTRACT_GENERATION):
            generated = dbt.generate(node, tests, manifest=manifest)
    except DbtNodeError as error:
        return _shape_failure(node_id, node, resource_type, error, timings)
    except Exception as error:
        raise _library_defect(node_id, error) from error
    columns_hash = None
    if seed is not None:
        with _rejecting_hash_input(generated.document["id"]):
            columns_hash = seed_columns_hash(seed)
    on_cache = timings.add_validation_cache if timings is not None else _ignore_cache
    try:
        with _stage(timings, Stage.CONTRACT_VALIDATION):
            issues = validator.check_dbt(generated.document, on_cache=on_cache)
    except Exception as error:
        raise _library_defect(node_id, error) from error
    mismatch = _id_issue(node, generated.document)
    if mismatch is not None:
        issues = [*issues, mismatch]
    header_issues = _header_issues(node_id, generated.document, seed.header if seed is not None else None)
    if issues or header_issues:
        return _issues_failure(
            generated.document["id"], node_id, node, resource_type, issues, header_issues, timings,
        )
    try:
        facts = dbt.run_facts(generated.document)
    except Exception as error:
        raise _library_defect(node_id, error) from error
    return generated, facts, columns_hash


def parse_manifest(
    manifest_path: str,
    image_tag: str = "",
    extras: CompileExtras | None = None,
    *,
    skipped: list[SkippedNode] | None = None,
    declared_service: str = "",
    validator: ContractValidator = VALIDATOR,
    progress: Callable[[], None] = _no_progress,
    timings: ReleaseTimings | None = None,
) -> ParsedArtifact:
    """Parse a dbt manifest into its nodes, the shared code they use and the
    invalid contracts of the nodes that could not be built.

    `extras` is the compile-extras.json of the same service when its compile wrote
    one. A seed's source part is the SHA-256 of its file bytes the extras record,
    or dbt's own checksum when the extras are absent or do not name the seed; a
    seed the extras name also carries seed_columns_hash, seed_header and
    seed_column_types from its entry, which stay empty otherwise. The header the
    extras give a seed is compared with the columns its contract declares, in both
    directions and by exact name, and a difference is an invalid contract; a seed
    the extras do not name has no known header, is not compared, and is listed in
    the result's `unchecked_seeds`.

    A model, seed or snapshot that a gate leaves out is never an error: when
    `skipped` is given, the node is appended to it with its reason, and nothing
    is logged here, so the caller reports every skip in one place. The gates are
    `dbt.skip_reason`'s and run in this order: no owner, a `local_stub` tag, then
    no tag on a node of an installed dbt package. dbt applies a project's
    top-level `+meta` to the nodes of its installed packages too, so a package's
    node can carry the project's owner; with no tag it is not the service's to
    schedule. A node of a package that carries a tag is kept under the package's
    service, so the caller's service check refuses it. The project is the
    manifest's own `metadata.project_name`, else `declared_service`. When neither
    is known, no node is left out for belonging to a package.

    Each node the gates keep, in manifest order, gets its contract from
    `dbt.generate` (timed as `contract_generation` on `timings`), from the node
    and the tests `dbt.tests_by_node` joins to it, and the contract is checked
    with `validator.check_dbt` (timed as `contract_validation`, each cache lookup
    counted). A valid contract becomes the node's `contract`, and the node's run
    facts come from `dbt.run_facts` of it: its table, relation, service, node
    type, schedule, criticality, owner, file path, materialization and timeout.
    Its schema is the manifest's, in the spelling the project declares, while
    its unique_id is the contract's id, in lower case. Everything else (its SQL,
    code, config, hashes, declared columns, seed settings and test count) comes
    from the manifest. Only such a node counts the tests attached to it. A dbt
    relationships test with an `attached_node` whose target the manifest holds
    neither as a node nor as a source (a `local_stub` model an uploader left out)
    is given to `generate` without that target, so the contract carries it as a
    `custom` rule; the result's `relationship_tests_without_target` counts such
    tests. Any other test is given as the manifest holds it.

    A contract with issues, a contract whose id is not the unique_id the node gets
    (the library lowers `<schema>.<table>` as one string, the unique_id each part on
    its own, and a capital Σ can lower differently), a seed whose declared columns
    differ from its CSV header, and a node field the library cannot read, is a
    FailedNode of the result (invalid_contract) naming the node, its service, its
    file and every issue, each issue counted on `timings` by its reason (an id that
    is not the unique_id as `validate`/`identity`, a column of a seed the header and
    the contract disagree on as `validate`/`seed_header`, a field the library
    cannot read as `generate`/`manifest_shape`). `progress` is
    called once for each model, seed and snapshot a gate does not leave out,
    after its node or its FailedNode is made.

    A manifest the library cannot read as a whole (a `metadata.project_name` that
    is not a string, a test whose node links are not strings) raises
    InvalidNodeSettingError with the library's message, which starts with
    `manifest` or the test's id, and so does a kept node or a test with no `fqn`
    entry and a test with an `fqn` that is not a list of strings. So does a node, a test or
    an on-run hook that is not an object or holds a field in a shape the hashing reads (a
    `compiled_code` that is not text, a `depends_on` that is not an object: see `manifest_shape`),
    with a message that starts with `manifest`. So does text
    UTF-8 cannot encode in the extras of a seed (checked when its contract is
    generated, before its header is compared). Any other error from the library
    raises ContractGenerationError.
    """
    with open(manifest_path) as f:
        manifest = json.load(f)

    project = _manifest_call(lambda: dbt.project_service(manifest, declared_service), timings)
    tests_of = _manifest_call(lambda: dbt.tests_by_node(manifest), timings)
    _require_readable_nodes(manifest, timings)
    with _rejecting_hash_input(manifest_path):
        ctx = ManifestContext.from_manifest(manifest, extras)
    held_targets = _HeldTargets(manifest)
    nodes = []
    failed: list[FailedNode] = []
    node_by_id: dict[str, ManifestNode] = {}
    used_units: dict[str, CodeUnit] = {}
    path_checksum_seeds: list[str] = []
    unchecked_seeds: list[str] = []
    for node_id, node in manifest["nodes"].items():
        resource_type = node["resource_type"]
        if resource_type not in dbt.RESOURCE_TYPES:
            # A test is read by the second pass, so it is not "unsupported".
            if resource_type != "test":
                logger.debug("Skipping node with unsupported resource_type",
                             extra={"node_id": node_id, "resource_type": resource_type})
            continue

        node.setdefault("unique_id", node_id)
        try:
            reason = dbt.skip_reason(node, project=project)
        except DbtNodeError as error:
            failed.append(_shape_failure(node_id, node, resource_type, error, timings))
            progress()
            continue
        except Exception as error:
            raise _library_defect(node_id, error) from error
        if reason is not None:
            if skipped is not None:
                skipped.append(_skipped(node_id, node, SkipReason(reason)))
            continue

        _require_fqn(node_id, node)
        seed = extras.seeds.get(node_id) if resource_type == "seed" and extras is not None else None
        built = _contract(
            node_id, node, resource_type, held_targets.of(tests_of.get(node_id, [])), manifest,
            seed=seed, validator=validator, timings=timings,
        )
        if isinstance(built, FailedNode):
            failed.append(built)
            progress()
            continue
        generated, facts, columns_hash = built
        node_type, task_timeout = _topology_facts(node_id, facts)

        parts = _hash(facts.unique_id, node, ctx)
        used_units.update(parts.units)

        manifest_node = ManifestNode(
            table_name=facts.table,
            # The schema as the project declares it: dbt builds the relation
            # under that spelling, and validation's SQL and DDL name it so. The
            # contract's id holds it in lower case (facts.schema), as unique_id
            # does.
            schema_name=node["schema"],
            resolved_relation=facts.physical_name,
            materialized=facts.materialized,
            full_refresh=_seed_full_refresh(resource_type, node.get("config") or {}),
            service_name=facts.service,
            owner=facts.owner,
            schedule_name=facts.schedule,
            criticality=facts.criticality,
            dependency_sqls=[node["compiled_code"]] if node.get("compiled_code") else [],
            candidate_sql=node.get("compiled_code", ""),
            node_type=node_type,
            content_hash=parts.content_hash,
            image_tag=image_tag,
            original_file_path=facts.file_path,
            raw_code=node.get("raw_code", ""),
            config=node.get("config") or {},
            source_hash=parts.source_hash,
            shared_code_hash=parts.shared_code_hash,
            config_hash=parts.config_hash,
            compiled_hash=parts.compiled_hash,
            hashed_config=parts.hashed_config,
            code_unit_ids=sorted(parts.units),
            timeout_seconds=task_timeout,
            declared_columns=len(node.get("columns") or {}),
            contract=NodeContract(
                family=ContractFamily.DBT,
                document=generated.document,
                path=facts.file_path,
                source_sha256=generated.source_sha256,
            ),
        )
        if resource_type == "model":
            manifest_node.test_count += len(ctx.unit_tests_by_node.get(node_id, ()))
        if resource_type == "seed":
            if seed is not None:
                manifest_node.seed_columns_hash = columns_hash
                manifest_node.seed_header = seed.header
                manifest_node.seed_column_types = dict(seed.column_types)
            else:
                unchecked_seeds.append(manifest_node.unique_id)
            if parts.path_checksum:
                path_checksum_seeds.append(manifest_node.unique_id)
        nodes.append(manifest_node)
        node_by_id[node_id] = manifest_node
        progress()

    # Second pass: tests. Each attaches its count to the tracked node(s) it
    # tests, and — when it tests at least one tracked node — becomes a
    # validation-only node of its own: its compiled SQL is bind-checked in
    # the candidate schema like a model is built there, so a test that names
    # a column the release dropped rejects the release at its source. A test
    # carries no owner or schedule of its own and is never scheduled, so
    # neither is required of it.
    test_nodes: list[ManifestNode] = []
    for node_id, node in manifest["nodes"].items():
        if node.get("resource_type") != "test":
            continue
        attached = node.get("attached_node")
        targets = [attached] if attached else node.get("depends_on", {}).get("nodes", [])
        counted = set()
        tracked = False
        for t in targets:
            tgt = node_by_id.get(t)
            if tgt is not None and id(tgt) not in counted:
                tgt.test_count += 1
                counted.add(id(tgt))
                tracked = True
        if not tracked:
            continue
        compiled = node.get("compiled_code", "")
        node.setdefault("unique_id", node_id)
        test_service = _service(node_id, node)
        parts = _hash(node_id, node, ctx)
        used_units.update(parts.units)
        test_nodes.append(ManifestNode(
            table_name=node["name"],
            schema_name=node["schema"],
            service_name=test_service,
            owner="",
            schedule_name="",
            criticality="SECONDARY",
            dependency_sqls=[compiled] if compiled else [],
            candidate_sql=compiled,
            node_type=NodeType.DBT_TEST,
            content_hash=parts.content_hash,
            image_tag=image_tag,
            original_file_path=node.get("original_file_path", ""),
            raw_code=node.get("raw_code", ""),
            config=node.get("config") or {},
            source_hash=parts.source_hash,
            shared_code_hash=parts.shared_code_hash,
            config_hash=parts.config_hash,
            compiled_hash=parts.compiled_hash,
            hashed_config=parts.hashed_config,
            code_unit_ids=sorted(parts.units),
            identity=node_id,
        ))
    nodes.extend(test_nodes)

    shared_code = {
        unit_id: {
            "source": unit.source,
            "checksum": unit.digest,
            "depends_on": [dep for dep in unit.depends_on if dep in used_units],
        }
        for unit_id, unit in sorted(used_units.items())
    }
    provenance = ServiceProvenance(
        dbt_version=(manifest.get("metadata") or {}).get("dbt_version", ""),
        packages=dict(extras.packages) if extras is not None else {},
        path_checksum_seeds=tuple(sorted(path_checksum_seeds)),
    )
    return ParsedArtifact(
        nodes=nodes, shared_code=shared_code, provenance=provenance, failed=tuple(failed),
        unchecked_seeds=tuple(sorted(unchecked_seeds)),
        relationship_tests_without_target=held_targets.count,
    )
