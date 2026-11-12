"""Parses a dbt manifest.json into ManifestNodes and the shared code (macros) they use.

Every node carries a content_hash folded from its own source, the shared code it
uses, its hashed config and, for a model or snapshot, its compiled SQL
(service/dbt_hashing.py holds the source, compiled and config parts). A model,
seed or snapshot a gate leaves out (no owner, no tag to take a schedule from, a
local stub) is reported through the caller's `skipped` list, never raised.
"""
import hashlib
import json
import logging
from dataclasses import dataclass

from continuo_contracts.errors import ContractsError
from continuo_contracts.hashing import fold4

from domain.exceptions import InvalidNodeSettingError
from domain.model import CompileExtras, ManifestNode, NodeType, ParsedArtifact, SkippedNode, SkipReason
from domain.node_timeout import timeout_seconds
from service.dbt_hashing import ManifestContext, compiled_part, config_part, hashed_config, source_part

logger = logging.getLogger(__name__)

SUPPORTED_RESOURCE_TYPES = {"model", "seed", "snapshot"}


def _transitive_macro_ids(direct_ids: list[str], macros: dict) -> set[str]:
    """Resolve the transitive closure of a node's macro dependencies.

    Walks macro->macro edges (each macro's own `depends_on.macros`) so a change to
    a macro reached only through another macro still affects the dependent node,
    mirroring dbt's transitive `state:modified.macros` behaviour. Ids absent from
    the manifest's macro map (e.g. unresolved built-ins) stay in the closure but
    contribute no checksum.
    """
    seen: set[str] = set()
    stack = list(direct_ids)
    while stack:
        mid = stack.pop()
        if mid in seen:
            continue
        seen.add(mid)
        macro = macros.get(mid)
        if macro:
            stack.extend(macro.get("depends_on", {}).get("macros", []))
    return seen


def _shared_code_hash(unit_ids: set[str], macros: dict) -> str:
    """Fold of the source checksums of the given shared-code units; "" if none."""
    unit_hashes = sorted(
        hashlib.sha256((macros[mid].get("macro_sql") or "").encode()).hexdigest()
        for mid in unit_ids if mid in macros
    )
    if not unit_hashes:
        return ""
    return hashlib.sha256("".join(unit_hashes).encode()).hexdigest()


@dataclass(frozen=True)
class _HashParts:
    source_hash: str
    shared_code_hash: str
    config_hash: str
    compiled_hash: str
    content_hash: str
    hashed_config: dict


def _hash_parts(label: str, node: dict, ctx: ManifestContext, unit_ids: set[str], macros: dict) -> _HashParts:
    """The four parts of a dbt node's content_hash and their fold.

    A value the hashing refuses (a config value canonical JSON cannot write, text
    UTF-8 cannot encode) is the artifact's fault, so it is raised as an
    InvalidNodeSettingError naming the node, which rejects the release instead of
    retrying it.
    """
    try:
        config = hashed_config(node, ctx)
        source = source_part(node, ctx)[0]
        shared = _shared_code_hash(unit_ids, macros)
        config_hash = config_part(config)
        compiled = compiled_part(node, ctx)
        content_hash = fold4(source, shared, config_hash, compiled)
    except (ContractsError, UnicodeEncodeError) as error:
        raise InvalidNodeSettingError(f"{label}: {error}") from error
    return _HashParts(
        source_hash=source,
        shared_code_hash=shared,
        config_hash=config_hash,
        compiled_hash=compiled,
        content_hash=content_hash,
        hashed_config=config,
    )


_RESOURCE_TYPE_TO_NODE_TYPE = {
    "model":    NodeType.DBT_MODEL,
    "seed":     NodeType.DBT_SEED,
    "snapshot": NodeType.DBT_SNAPSHOT,
}

# Resource types that have a default schedule when no tags are present.
# Types absent from this map require explicit tags or the node is dropped.
_RESOURCE_TYPE_DEFAULT_SCHEDULE: dict[str, str | None] = {
    "model":    None,
    "seed":     "seed",
    "snapshot": None,
}


def _skipped(node_id: str, node: dict, reason: SkipReason) -> SkippedNode:
    """The report of a node a gate left out. The service is read tolerantly: a
    node the parse never builds can still lack an fqn without raising."""
    service = (node.get("fqn") or [""])[0].replace("_", "-")
    return SkippedNode(node_id=node_id, service=service, reason=reason)


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
    or dbt's own checksum when the extras are absent or do not name the seed.

    A model, seed or snapshot that a gate leaves out (no owner, no tag to take
    a schedule from, a local stub) is never an error: when `skipped` is given,
    the node is appended to it with its reason, and nothing is logged here, so
    the caller reports every skip in one place.
    """
    with open(manifest_path) as f:
        manifest = json.load(f)

    macros = manifest.get("macros", {})
    ctx = ManifestContext.from_manifest(manifest, extras)
    nodes = []
    node_by_id: dict[str, ManifestNode] = {}
    used_unit_ids: set[str] = set()
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
        default_schedule = _RESOURCE_TYPE_DEFAULT_SCHEDULE.get(resource_type)
        if not tags and default_schedule is None:
            if skipped is not None:
                skipped.append(_skipped(node_id, node, SkipReason.NO_TAG))
            continue

        schedule_name = tags[0] if tags else default_schedule

        meta = node.get("config", {}).get("meta") or {}
        criticality = meta.get("criticality", "SECONDARY")

        direct_unit_ids = list(node.get("depends_on", {}).get("macros", []))
        transitive_ids = _transitive_macro_ids(direct_unit_ids, macros)
        used_unit_ids |= transitive_ids
        parts = _hash_parts(f"{node['schema']}.{node['name']}".lower(), node, ctx, transitive_ids, macros)

        manifest_node = ManifestNode(
            table_name=node["name"],
            schema_name=node["schema"],
            resolved_relation=node.get("alias") or node["name"],
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
            code_unit_ids=direct_unit_ids,
            declared_columns=len(node.get("columns") or {}),
        )
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
        direct_unit_ids = list(node.get("depends_on", {}).get("macros", []))
        transitive_ids = _transitive_macro_ids(direct_unit_ids, macros)
        used_unit_ids |= transitive_ids
        parts = _hash_parts(node_id, node, ctx, transitive_ids, macros)
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
            code_unit_ids=direct_unit_ids,
            identity=node_id,
        ))
    nodes.extend(test_nodes)

    shared_code = {
        mid: {
            "source": macros[mid].get("macro_sql") or "",
            "checksum": hashlib.sha256((macros[mid].get("macro_sql") or "").encode()).hexdigest(),
            "depends_on": list(macros[mid].get("depends_on", {}).get("macros", [])),
        }
        for mid in sorted(used_unit_ids) if mid in macros
    }
    return ParsedArtifact(nodes=nodes, shared_code=shared_code)
