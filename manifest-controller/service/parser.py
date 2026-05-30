import hashlib
import json
import logging
from domain.model import ManifestNode

logger = logging.getLogger(__name__)

SUPPORTED_RESOURCE_TYPES = {"model", "seed", "snapshot"}


def _content_hash(node: dict) -> str:
    """Return a non-empty, change-sensitive fingerprint for a dbt node.

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


def parse_manifest(manifest_path: str, manifest_version: str, image_tag: str = "") -> list[ManifestNode]:
    with open(manifest_path) as f:
        manifest = json.load(f)

    nodes = []
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
        default_schedule = _RESOURCE_TYPE_DEFAULT_SCHEDULE.get(resource_type)
        if not tags and default_schedule is None:
            logger.warning("Skipping node missing required tags (schedule)",
                           extra={"node_id": node_id})
            continue

        schedule_name = tags[0] if tags else default_schedule

        criticality = node.get("config", {}).get("meta", {}).get("criticality", "SECONDARY")

        nodes.append(ManifestNode(
            table_name=node["name"],
            schema_name=node["schema"],
            service_name=node["fqn"][0].replace("_", "-"),
            owner=owner,
            schedule_name=schedule_name,
            criticality=criticality,
            compiled_sql=node.get("compiled_code", ""),
            node_type=_RESOURCE_TYPE_TO_NODE_TYPE[resource_type],
            content_hash=_content_hash(node),
            manifest_version=manifest_version,
            image_tag=image_tag,
        ))
    return nodes
