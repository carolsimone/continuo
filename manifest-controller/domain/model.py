from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class ManifestFile:
    path: str
    version: str


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
    manifest_version: str = ""
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
