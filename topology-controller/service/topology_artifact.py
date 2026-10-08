"""The topology artifact: one immutable document per release holding every
node of its resolved topology, stored gzipped at
tenants/<tenant>/topologies/<release>/topology.json.gz.

topology-controller writes it for every candidate and verification release;
release-controller's Go writer writes the same document for the topologies it
announces itself. Readers verify the SHA-256 an event carries against the
stored bytes. Both writers produce the same canonical JSON byte for byte —
pkg/topologyartifact.CanonicalJSON in Go, canonical_topology_json here — and
tests/fixtures/topology_v1.json, a copy of the fixture the Go package reads,
pins the two together.
"""
import json
from collections.abc import Iterable, Mapping

SCHEMA_VERSION = 1

# Every string field of an artifact node. The document is written with sorted
# keys, so the order here does not reach the bytes.
_STRING_FIELDS = (
    "unique_id", "schema_name", "table_name", "resolved_relation_id",
    "service_name", "node_type", "content_hash", "image_tag",
    "original_file_path", "schedule",
)


def topology_artifact_key(tenant_id: str, release_id: str) -> str:
    """The S3 object key of a release's topology artifact."""
    return f"tenants/{tenant_id}/topologies/{release_id}/topology.json.gz"


def _artifact_node(node: Mapping) -> dict:
    """The fields of one node the artifact keeps, defaulted the way the Go
    document's zero values are: a key outside the artifact's node shape is
    dropped, a missing string is "", a missing test_count 0, missing or null
    upstreams an empty list (upstreams sorted), and an empty secret_ref is left out."""
    out = {name: str(node.get(name) or "") for name in _STRING_FIELDS}
    out["test_count"] = int(node.get("test_count") or 0)
    out["upstream_unique_ids"] = sorted(str(u) for u in node.get("upstream_unique_ids") or [])
    secret_ref = node.get("secret_ref") or ""
    if secret_ref:
        out["secret_ref"] = str(secret_ref)
    return out


def canonical_topology_json(*, tenant_id: str, release_id: str, nodes: Iterable[Mapping]) -> bytes:
    """The artifact document as canonical JSON bytes.

    Nodes are sorted by unique_id (a stable sort, as Go's), object keys are
    sorted, separators are compact, and text is UTF-8 with no escaping beyond
    what JSON requires — except U+2028 and U+2029, which Go's encoding/json
    always escapes, so they are escaped here too.
    """
    document = {
        "schema_version": SCHEMA_VERSION,
        "tenant_id": tenant_id,
        "release_id": release_id,
        "nodes": sorted((_artifact_node(n) for n in nodes), key=lambda n: n["unique_id"]),
    }
    text = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    for char, escape in ((chr(0x2028), "\\" + "u2028"), (chr(0x2029), "\\" + "u2029")):
        text = text.replace(char, escape)
    return text.encode("utf-8")
