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
the node and its tests and the parse checks. The node's run facts (its schema,
table, relation, service, kind, schedule, criticality, owner, file path,
materialization and timeout) are read back from that contract, so the topology
says what the contract says. A node whose contract fails its checks, or that the
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
from domain.exceptions import ContractGenerationError, InvalidNodeSettingError
from domain.model import (
    CompileExtras,
    ContractFamily,
    FailedNode,
    ManifestNode,
    NodeContract,
    NodeType,
    ParsedArtifact,
    ServiceProvenance,
    SkippedNode,
    SkipReason,
)
from service.compile_extras import seed_columns_hash
from service.contract_validation import ContractValidator
from service.contracts_library import VALIDATOR
from service.dbt_hashing import CodeUnit, DbtHashParts, ManifestContext, hash_dbt_node
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
    header: Sequence[str] | None,
    validator: ContractValidator,
    timings: ReleaseTimings | None,
) -> tuple[dbt.Generated, dbt.RunFacts] | FailedNode:
    """The checked contract of a kept node and the run facts it gives, or the
    FailedNode that says why the node has none.

    A field the library cannot read, a contract with issues and a seed whose
    declared columns differ from its CSV `header` (None when the header is not
    known, which is never an issue) are the team's to fix, so each is a
    FailedNode. A seed with both gets one FailedNode that names every problem.
    Any other error from the library is a defect, raised as a
    ContractGenerationError: `generate` never returns a contract canonical JSON
    cannot write, so the check failing on one is a defect too."""
    try:
        with _stage(timings, Stage.CONTRACT_GENERATION):
            generated = dbt.generate(node, tests, manifest=manifest)
    except DbtNodeError as error:
        return _shape_failure(node_id, node, resource_type, error, timings)
    except Exception as error:
        raise _library_defect(node_id, error) from error
    on_cache = timings.add_validation_cache if timings is not None else _ignore_cache
    try:
        with _stage(timings, Stage.CONTRACT_VALIDATION):
            issues = validator.check_dbt(generated.document, on_cache=on_cache)
    except Exception as error:
        raise _library_defect(node_id, error) from error
    header_issues = _header_issues(node_id, generated.document, header)
    if issues or header_issues:
        return _issues_failure(
            generated.document["id"], node_id, node, resource_type, issues, header_issues, timings,
        )
    try:
        facts = dbt.run_facts(generated.document)
    except Exception as error:
        raise _library_defect(node_id, error) from error
    return generated, facts


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
    facts come from `dbt.run_facts` of it: its schema (the contract id's, in
    lower case), table, relation, service, node type, schedule, criticality,
    owner, file path, materialization and timeout. Everything else (its SQL,
    code, config, hashes, declared columns, seed settings and test count) comes
    from the manifest. Only such a node counts the tests attached to it.

    A contract with issues, a seed whose declared columns differ from its CSV
    header, and a node field the library cannot read, is a FailedNode of the
    result (invalid_contract) naming the node, its service, its file and every
    issue, each issue counted on `timings` by its reason (a column of a seed the
    header and the contract disagree on as `validate`/`seed_header`, a field the
    library cannot read as `generate`/`manifest_shape`). `progress` is
    called once for each model, seed and snapshot a gate does not leave out,
    after its node or its FailedNode is made.

    A manifest the library cannot read as a whole (a `metadata.project_name` that
    is not a string, a test whose node links are not strings) raises
    InvalidNodeSettingError with the library's message, which starts with
    `manifest` or the test's id, and so does a kept node or a test with no `fqn`
    entry and a test with an `fqn` that is not a list of strings. Any other error
    from the library raises ContractGenerationError.
    """
    with open(manifest_path) as f:
        manifest = json.load(f)

    with _rejecting_hash_input(manifest_path):
        ctx = ManifestContext.from_manifest(manifest, extras)
    project = _manifest_call(lambda: dbt.project_service(manifest, declared_service), timings)
    tests_of = _manifest_call(lambda: dbt.tests_by_node(manifest), timings)
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
            node_id, node, resource_type, tests_of.get(node_id, []), manifest,
            header=seed.header if seed is not None else None, validator=validator, timings=timings,
        )
        if isinstance(built, FailedNode):
            failed.append(built)
            progress()
            continue
        generated, facts = built

        parts = _hash(facts.unique_id, node, ctx)
        used_units.update(parts.units)

        manifest_node = ManifestNode(
            table_name=facts.table,
            schema_name=facts.schema,
            resolved_relation=facts.physical_name,
            materialized=facts.materialized,
            full_refresh=_seed_full_refresh(resource_type, node.get("config") or {}),
            service_name=facts.service,
            owner=facts.owner,
            schedule_name=facts.schedule,
            criticality=facts.criticality,
            dependency_sqls=[node["compiled_code"]] if node.get("compiled_code") else [],
            candidate_sql=node.get("compiled_code", ""),
            node_type=NodeType(facts.kind),
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
            timeout_seconds=facts.timeout_minutes * 60 if facts.timeout_minutes is not None else None,
            declared_columns=len(node.get("columns") or {}),
            contract=NodeContract(
                family=ContractFamily.DBT,
                document=generated.document,
                path=node["original_file_path"],
                source_sha256=generated.source_sha256,
            ),
        )
        if resource_type == "model":
            manifest_node.test_count += len(ctx.unit_tests_by_node.get(node_id, ()))
        if resource_type == "seed":
            if seed is not None:
                with _rejecting_hash_input(facts.unique_id):
                    manifest_node.seed_columns_hash = seed_columns_hash(seed)
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
    )
