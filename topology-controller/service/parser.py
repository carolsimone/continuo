"""Parses a dbt manifest.json into ManifestNodes and the shared code they use.

Every node carries a content_hash folded from its own source, the shared code it
uses (macros, functions, unit tests and the project's service units), its hashed
config and, for a model or snapshot, its compiled SQL (service/dbt_hashing.py
holds the parts and their fold). A seed the compile extras name also carries its
column fingerprint, CSV header and column types, and the parse reports the
service's provenance: the manifest's dbt version, the extras' package versions
and the seeds whose source part is still dbt's `path` checksum. A model, seed or
snapshot a gate leaves out (no owner, a local stub) is reported through the
caller's `skipped` list, never raised. A model or snapshot with an owner and no
tag is kept with an empty schedule (a dormant node); a seed with no tag takes
the `seed` schedule.
"""
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager

from continuo_contracts.errors import ContractsError

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

SUPPORTED_RESOURCE_TYPES = {"model", "seed", "snapshot"}


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


def _hash(label: str, node: dict, ctx: ManifestContext) -> DbtHashParts:
    with _rejecting_hash_input(label):
        return hash_dbt_node(node, ctx)


_RESOURCE_TYPE_TO_NODE_TYPE = {
    "model":    NodeType.DBT_MODEL,
    "seed":     NodeType.DBT_SEED,
    "snapshot": NodeType.DBT_SNAPSHOT,
}

# The schedule each resource type takes when it carries no tag. A seed loads on
# the "seed" schedule; a model or snapshot has none, so it is a dormant node that
# only a manual run executes.
_RESOURCE_TYPE_UNTAGGED_SCHEDULE: dict[str, str] = {
    "model":    "",
    "seed":     "seed",
    "snapshot": "",
}


def _skipped(node_id: str, node: dict, reason: SkipReason) -> SkippedNode:
    """The report of a node a gate left out. The service is read tolerantly: a
    node the parse never builds can still lack an fqn without raising."""
    service = (node.get("fqn") or [""])[0].replace("_", "-")
    return SkippedNode(node_id=node_id, service=service, reason=reason)


def _table_name(node: dict) -> str:
    """The table name a dbt node is known by: its name, or "<name>.v<version>"
    for a versioned model, so each version of a model is a node of its own and
    the name is also the dbt selector of that version."""
    version = node.get("version")
    if version is None or version == "":
        return node["name"]
    return f"{node['name']}.v{version}"


def parse_manifest(
    manifest_path: str,
    image_tag: str = "",
    extras: CompileExtras | None = None,
    *,
    skipped: list[SkippedNode] | None = None,
) -> ParsedArtifact:
    """Parse a dbt manifest into its nodes and the shared code they use.

    `extras` is the compile-extras.json of the same service when its compile wrote
    one. A seed's source part is the SHA-256 of its file bytes the extras record,
    or dbt's own checksum when the extras are absent or do not name the seed; a
    seed the extras name also carries seed_columns_hash, seed_header and
    seed_column_types from its entry, which stay empty otherwise.

    A model, seed or snapshot that a gate leaves out (no owner, a local stub)
    is never an error: when `skipped` is given, the node is appended to it with
    its reason, and nothing is logged here, so the caller reports every skip in
    one place. The gates run in that order, so a node with neither owner nor tag
    is reported as having no owner.
    """
    with open(manifest_path) as f:
        manifest = json.load(f)

    with _rejecting_hash_input(manifest_path):
        ctx = ManifestContext.from_manifest(manifest, extras)
    nodes = []
    node_by_id: dict[str, ManifestNode] = {}
    used_units: dict[str, CodeUnit] = {}
    path_checksum_seeds: list[str] = []
    for node_id, node in manifest["nodes"].items():
        resource_type = node["resource_type"]
        if resource_type not in SUPPORTED_RESOURCE_TYPES:
            # A test is read by the second pass, so it is not "unsupported".
            if resource_type != "test":
                logger.debug("Skipping node with unsupported resource_type",
                             extra={"node_id": node_id, "resource_type": resource_type})
            continue

        owner = (node.get("config", {}).get("meta") or {}).get("owner")
        if not owner:
            if skipped is not None:
                skipped.append(_skipped(node_id, node, SkipReason.NO_OWNER))
            continue

        tags = node.get("tags", [])
        if "local_stub" in tags:
            if skipped is not None:
                skipped.append(_skipped(node_id, node, SkipReason.LOCAL_STUB))
            continue

        schedule_name = tags[0] if tags else _RESOURCE_TYPE_UNTAGGED_SCHEDULE[resource_type]

        meta = node.get("config", {}).get("meta") or {}
        criticality = meta.get("criticality", "SECONDARY")

        node.setdefault("unique_id", node_id)
        table_name = _table_name(node)
        label = f"{node['schema']}.{table_name}".lower()
        parts = _hash(label, node, ctx)
        used_units.update(parts.units)

        manifest_node = ManifestNode(
            table_name=table_name,
            schema_name=node["schema"],
            resolved_relation=node.get("alias") or table_name,
            materialized=(node.get("config") or {}).get("materialized") or "",
            service_name=node["fqn"][0].replace("_", "-"),
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
        parts = _hash(node_id, node, ctx)
        used_units.update(parts.units)
        test_nodes.append(ManifestNode(
            table_name=node["name"],
            schema_name=node["schema"],
            service_name=node["fqn"][0].replace("_", "-"),
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
