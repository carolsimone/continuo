import hashlib
import json
import logging
from domain.model import ManifestNode

logger = logging.getLogger(__name__)

SUPPORTED_RESOURCE_TYPES = {"model", "seed", "snapshot"}


def _node_source_hash(node: dict) -> str:
    """Return a non-empty, change-sensitive fingerprint for a dbt node's own source.

    Prefers dbt's own per-node `checksum.checksum` (a sha256 of the node's source
    file). release-controller uses content_hash as the SOLE change detector, so an
    empty value would make later edits to the node undetectable (empty == empty).
    For any node dbt did not check-sum, fall back to a deterministic sha256 over the
    node's source (`raw_code`/`compiled_code`) or, failing that, a stable JSON dump —
    so the fingerprint is never empty and still changes when the node changes.
    """
    checksum = node.get("checksum", {}).get("checksum", "")
    if checksum:
        return checksum
    basis = node.get("raw_code") or node.get("compiled_code") or ""
    if not basis:
        basis = json.dumps(node, sort_keys=True, default=str)
    return "sha256:" + hashlib.sha256(basis.encode()).hexdigest()


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


CONFIG_HASH_DENYLIST = {"meta", "docs", "description", "grants", "tags"}


def _config_hash(node: dict) -> str:
    """Fingerprint of the node's resolved config, denylist-filtered.

    Out-of-file config (dbt_project.yml / schema.yml properties) reaches only the
    manifest's resolved config, never dbt's file checksum — without this component
    a materialization change would promote with no revalidation. Unknown keys
    participate by default: a spurious revalidation is cheaper than a silently
    missed change.
    """
    cfg = {k: v for k, v in (node.get("config") or {}).items()
           if k not in CONFIG_HASH_DENYLIST}
    return hashlib.sha256(json.dumps(cfg, sort_keys=True, default=str).encode()).hexdigest()


def _shared_code_hash(unit_ids: set[str], macros: dict) -> str:
    """Fold of the source checksums of the given shared-code units; "" if none."""
    unit_hashes = sorted(
        hashlib.sha256((macros[mid].get("macro_sql") or "").encode()).hexdigest()
        for mid in unit_ids if mid in macros
    )
    if not unit_hashes:
        return ""
    return hashlib.sha256("".join(unit_hashes).encode()).hexdigest()


def _content_hash(source_hash: str, shared_code_hash: str, config_hash: str) -> str:
    """Single change-detection fingerprint: any component change flips it."""
    return "sha256:" + hashlib.sha256(
        f"{source_hash}|{shared_code_hash}|{config_hash}".encode()
    ).hexdigest()

_RESOURCE_TYPE_TO_NODE_TYPE = {
    "model":    "dbt-model",
    "seed":     "dbt-seed",
    "snapshot": "dbt-snapshot",
}

# Resource types that have a default schedule when no tags are present.
# Types absent from this map require explicit tags or the node is dropped.
_RESOURCE_TYPE_DEFAULT_SCHEDULE: dict[str, str | None] = {
    "model":    None,
    "seed":     "seed",
    "snapshot": None,
}


def parse_manifest(
    manifest_path: str, manifest_version: str, image_tag: str = ""
) -> tuple[list[ManifestNode], dict]:
    with open(manifest_path) as f:
        manifest = json.load(f)

    macros = manifest.get("macros", {})
    nodes = []
    node_by_id: dict[str, ManifestNode] = {}
    used_unit_ids: set[str] = set()
    for node_id, node in manifest["nodes"].items():
        resource_type = node["resource_type"]
        if resource_type not in SUPPORTED_RESOURCE_TYPES:
            logger.error("Skipping node with unsupported resource_type",
                         extra={"node_id": node_id, "resource_type": resource_type})
            continue

        owner = node.get("config", {}).get("meta", {}).get("owner")
        if not owner:
            logger.warning("Skipping node missing required meta.owner",
                           extra={"node_id": node_id})
            continue

        tags = node.get("tags", [])
        if "local_stub" in tags:
            logger.info("Skipping local_stub node", extra={"node_id": node_id})
            continue
        default_schedule = _RESOURCE_TYPE_DEFAULT_SCHEDULE.get(resource_type)
        if not tags and default_schedule is None:
            logger.warning("Skipping node missing required tags (schedule)",
                           extra={"node_id": node_id})
            continue

        schedule_name = tags[0] if tags else default_schedule

        criticality = node.get("config", {}).get("meta", {}).get("criticality", "SECONDARY")

        direct_unit_ids = list(node.get("depends_on", {}).get("macros", []))
        transitive_ids = _transitive_macro_ids(direct_unit_ids, macros)
        used_unit_ids |= transitive_ids
        source_hash = _node_source_hash(node)
        shared_hash = _shared_code_hash(transitive_ids, macros)
        config_hash = _config_hash(node)

        manifest_node = ManifestNode(
            table_name=node["name"],
            schema_name=node["schema"],
            service_name=node["fqn"][0].replace("_", "-"),
            owner=owner,
            schedule_name=schedule_name,
            criticality=criticality,
            dependency_sqls=[node["compiled_code"]] if node.get("compiled_code") else [],
            candidate_sql=node.get("compiled_code", ""),
            node_type=_RESOURCE_TYPE_TO_NODE_TYPE[resource_type],
            content_hash=_content_hash(source_hash, shared_hash, config_hash),
            manifest_version=manifest_version,
            image_tag=image_tag,
            original_file_path=node.get("original_file_path", ""),
            raw_code=node.get("raw_code", ""),
            config=node.get("config") or {},
            source_hash=source_hash,
            shared_code_hash=shared_hash,
            config_hash=config_hash,
            code_unit_ids=direct_unit_ids,
        )
        nodes.append(manifest_node)
        node_by_id[node_id] = manifest_node

    # Second pass: count tests attached to each tracked node. Generic tests
    # carry attached_node; singular tests carry only depends_on.nodes. A test
    # attributes once, to attached_node when present else to each tracked
    # depends_on target.
    for node_id, node in manifest["nodes"].items():
        if node.get("resource_type") != "test":
            continue
        attached = node.get("attached_node")
        targets = [attached] if attached else node.get("depends_on", {}).get("nodes", [])
        counted = set()
        for t in targets:
            tgt = node_by_id.get(t)
            if tgt is not None and id(tgt) not in counted:
                tgt.test_count += 1
                counted.add(id(tgt))

    shared_code = {
        mid: {
            "source": macros[mid].get("macro_sql") or "",
            "checksum": hashlib.sha256((macros[mid].get("macro_sql") or "").encode()).hexdigest(),
            "depends_on": list(macros[mid].get("depends_on", {}).get("macros", [])),
        }
        for mid in sorted(used_unit_ids) if mid in macros
    }
    return nodes, shared_code
