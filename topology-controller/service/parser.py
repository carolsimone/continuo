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
takes the `seed` schedule. Which nodes are left out, a kept node's service,
schedule and table name all come from continuo-contracts' `dbt` module, so a
producer's CI and this parse answer alike.
"""
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager

from continuo_contracts import dbt
from continuo_contracts.errors import ContractsError, DbtNodeError

from domain.exceptions import InvalidNodeSettingError
from domain.model import (
    CompileExtras,
    ManifestNode,
    NodeType,
    ParsedArtifact,
    ServiceProvenance,
    SkippedNode,
    SkipReason,
)
from domain.node_timeout import timeout_seconds
from service.compile_extras import seed_columns_hash
from service.dbt_hashing import CodeUnit, DbtHashParts, ManifestContext, hash_dbt_node

logger = logging.getLogger(__name__)


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
    """Raise a field the library cannot read (a `tags` that is not a list of strings,
    an `fqn` of another shape, a `name` that is empty) as an InvalidNodeSettingError
    carrying the library's message, which starts with the node's id: it is the
    artifact's fault, so the release is rejected instead of retried."""
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


def _skipped(node_id: str, node: dict, reason: SkipReason) -> SkippedNode:
    """The report of a node a gate left out. The service is read tolerantly, as
    the first entry of a well-formed fqn: a node the parse never builds can lack
    an fqn, or carry one of another shape, without raising."""
    fqn = node.get("fqn")
    first = fqn[0] if isinstance(fqn, list) and fqn else ""
    service = first.replace("_", "-") if isinstance(first, str) else ""
    return SkippedNode(node_id=node_id, service=service, reason=reason)


def _service(node_id: str, node: dict) -> str:
    """The service of a node the parse builds, a kept node or a test: the first
    entry of its fqn, with `_` written `-`. dbt writes an fqn for every node, so a
    node with none does not say which service it belongs to, and the artifact is
    refused."""
    with _rejecting_node_shape():
        service = dbt.node_service(node)
    if not node.get("fqn"):
        raise InvalidNodeSettingError(f"{node_id}: fqn must be a non-empty list of strings")
    return service


def _seed_full_refresh(resource_type: str, config: dict) -> bool | None:
    """A seed's config.full_refresh when it is a bool; None for any other value
    and for every node that is not a seed."""
    value = config.get("full_refresh")
    return value if resource_type == "seed" and isinstance(value, bool) else None


def parse_manifest(
    manifest_path: str,
    image_tag: str = "",
    extras: CompileExtras | None = None,
    *,
    skipped: list[SkippedNode] | None = None,
    declared_service: str = "",
) -> ParsedArtifact:
    """Parse a dbt manifest into its nodes and the shared code they use.

    `extras` is the compile-extras.json of the same service when its compile wrote
    one. A seed's source part is the SHA-256 of its file bytes the extras record,
    or dbt's own checksum when the extras are absent or do not name the seed; a
    seed the extras name also carries seed_columns_hash, seed_header and
    seed_column_types from its entry, which stay empty otherwise.

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

    A field the library cannot read (a `tags` that is not a list of strings on a
    node the earlier gates let through, an `fqn` that is not a list, a
    `metadata.project_name` that is not a string) raises InvalidNodeSettingError
    with the library's message, which starts with the node's id, and so does a
    kept node or a test with no `fqn` entry.
    """
    with open(manifest_path) as f:
        manifest = json.load(f)

    with _rejecting_hash_input(manifest_path):
        ctx = ManifestContext.from_manifest(manifest, extras)
    with _rejecting_node_shape():
        project = dbt.project_service(manifest, declared_service)
    nodes = []
    node_by_id: dict[str, ManifestNode] = {}
    used_units: dict[str, CodeUnit] = {}
    path_checksum_seeds: list[str] = []
    for node_id, node in manifest["nodes"].items():
        resource_type = node["resource_type"]
        if resource_type not in dbt.RESOURCE_TYPES:
            # A test is read by the second pass, so it is not "unsupported".
            if resource_type != "test":
                logger.debug("Skipping node with unsupported resource_type",
                             extra={"node_id": node_id, "resource_type": resource_type})
            continue

        node.setdefault("unique_id", node_id)
        with _rejecting_node_shape():
            reason = dbt.skip_reason(node, project=project)
        if reason is not None:
            if skipped is not None:
                skipped.append(_skipped(node_id, node, SkipReason(reason)))
            continue

        meta = node["config"]["meta"]
        owner = meta["owner"]
        criticality = meta.get("criticality", "SECONDARY")

        service_name = _service(node_id, node)
        with _rejecting_node_shape():
            schedule_name = dbt.schedule(node)
            table_name = dbt.table_name(node)
        label = f"{node['schema']}.{table_name}".lower()
        parts = _hash(label, node, ctx)
        used_units.update(parts.units)

        manifest_node = ManifestNode(
            table_name=table_name,
            schema_name=node["schema"],
            resolved_relation=node.get("alias") or table_name,
            materialized=(node.get("config") or {}).get("materialized") or "",
            full_refresh=_seed_full_refresh(resource_type, node.get("config") or {}),
            service_name=service_name,
            owner=owner,
            schedule_name=schedule_name,
            criticality=criticality,
            dependency_sqls=[node["compiled_code"]] if node.get("compiled_code") else [],
            candidate_sql=node.get("compiled_code", ""),
            node_type=_RESOURCE_TYPE_TO_NODE_TYPE[resource_type],
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
            declared_columns=len(node.get("columns") or {}),
        )
        if resource_type == "model":
            manifest_node.test_count += len(ctx.unit_tests_by_node.get(node_id, ()))
        if resource_type == "seed":
            seed = extras.seeds.get(node_id) if extras is not None else None
            if seed is not None:
                with _rejecting_hash_input(label):
                    manifest_node.seed_columns_hash = seed_columns_hash(seed)
                manifest_node.seed_header = seed.header
                manifest_node.seed_column_types = dict(seed.column_types)
            if parts.path_checksum:
                path_checksum_seeds.append(manifest_node.unique_id)
        try:
            manifest_node.timeout_seconds = timeout_seconds(meta.get("timeout_minutes"))
        except ValueError as exc:
            raise InvalidNodeSettingError(
                f"{manifest_node.unique_id}: meta.timeout_minutes {exc}"
            ) from exc
        nodes.append(manifest_node)
        node_by_id[node_id] = manifest_node

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
    return ParsedArtifact(nodes=nodes, shared_code=shared_code, provenance=provenance)
