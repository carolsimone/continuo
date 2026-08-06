from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class ManifestFile:
    path: str
    version: str
    image_tag: str = ""
    declared_service: str = ""


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
    dependency_sqls: list[str] = field(default_factory=list)
    candidate_sql: str = ""
    node_type: str = "dbt-model"  # dbt-model | dbt-seed | dbt-snapshot
    content_hash: str = ""  # sha256:-prefixed fold of source_hash|shared_code_hash|config_hash
    manifest_version: str = ""
    image_tag: str = ""
    original_file_path: str = ""  # dbt original_file_path (project-relative)
    test_count: int = 0  # number of dbt tests attached to this node
    upstream_deps: list[UpstreamDep] = field(default_factory=list)
    raw_code: str = ""  # node's own source (dbt raw_code), pre-compilation
    config: dict = field(default_factory=dict)  # node's resolved dbt config
    source_hash: str = ""  # fingerprint of this node's own source (see _node_source_hash)
    shared_code_hash: str = ""  # fold of transitive shared-code checksums; "" if none
    config_hash: str = ""  # fingerprint of resolved config, denylist-filtered
    code_unit_ids: list[str] = field(default_factory=list)  # direct shared-code (macro) deps
    runtime: str = "dbt"


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
