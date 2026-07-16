from __future__ import annotations
from dataclasses import dataclass, field


@dataclass(frozen=True)
class RuntimeManifestRef:
    """Points at the pre-built dbt runtime manifest a service's nodes execute against.

    The four fields travel together on the candidate topology event: the artifact's
    location, its content digest, the dbt-core version that wrote it (a partial
    parse is only loadable by the version that produced it), and the digest of the
    parse context it was produced under. Frozen so a ref compares by value —
    two manifests declaring the same service must resolve to the same artifact.
    """
    uri: str
    sha256: str
    dbt_version: str
    parse_context_sha256: str

    def to_wire(self) -> dict[str, str]:
        return {
            "runtime_manifest_uri": self.uri,
            "runtime_manifest_sha256": self.sha256,
            "runtime_manifest_dbt_version": self.dbt_version,
            "runtime_manifest_parse_context_sha256": self.parse_context_sha256,
        }


@dataclass
class ManifestFile:
    path: str
    version: str
    image_tag: str = ""
    declared_service: str = ""
    # None when the release carries no runtime manifest beside its manifest.json,
    # which is how releases compiled without the runtime exporter present.
    runtime_manifest: RuntimeManifestRef | None = None


@dataclass
class ServiceMetadata:
    manifest_version: str
    image_tag: str


@dataclass
class UpstreamDep:
    table_name: str
    schema_name: str
    service_name: str


@dataclass
class ManifestNode:
    table_name: str
    schema_name: str
    service_name: str
    owner: str
    schedule_name: str
    criticality: str  # "REGULATORY" | "CORE" | "SECONDARY"
    compiled_sql: str
    node_type: str = "dbt-model"  # dbt-model | dbt-seed | dbt-snapshot
    dbt_unique_id: str = ""  # dbt's own node key, e.g. "model.service_1.table_a"
    content_hash: str = ""  # dbt's per-node source checksum (checksum.checksum)
    manifest_version: str = ""
    image_tag: str = ""
    original_file_path: str = ""  # dbt original_file_path (project-relative)
    test_count: int = 0  # number of dbt tests attached to this node
    upstream_deps: list[UpstreamDep] = field(default_factory=list)


@dataclass
class NodeRegistryEntry:
    table_name: str
    schema_name: str
    service_name: str
    owner: str


@dataclass
class NodeRegistry:
    entries: list[NodeRegistryEntry]

    def to_lookup(self) -> dict[tuple[str, str], NodeRegistryEntry]:
        return {(e.schema_name.lower(), e.table_name.lower()): e for e in self.entries}
