from __future__ import annotations
from dataclasses import dataclass, field
from enum import StrEnum


class NodeType(StrEnum):
    DBT_MODEL = "dbt-model"
    DBT_SEED = "dbt-seed"
    DBT_SNAPSHOT = "dbt-snapshot"
    PYTHON_MODEL = "python-model"


class Runtime(StrEnum):
    DBT = "dbt"
    PYTHON = "python"


class ManifestKind(StrEnum):
    """Which artifact dialect a service's release payload speaks.

    Carried per manifest_keys entry on release.requested:v1. Absent means dbt,
    so a producer that predates python support needs no change.
    """
    DBT = "dbt"
    PYTHON = "python"


@dataclass(frozen=True)
class ManifestRequest:
    """One service's artifact to fetch for a release: its S3 object key plus the
    kind that decides which parser reads it."""
    service: str
    key: str
    kind: str = ManifestKind.DBT


@dataclass
class ManifestFile:
    path: str
    version: str
    image_tag: str = ""
    declared_service: str = ""
    # The raw wire value, not narrowed to ManifestKind: an unrecognized kind is
    # a permanent failure the handler reports, not an exception at decode time.
    kind: str = ManifestKind.DBT


@dataclass
class ServiceMetadata:
    manifest_version: str
    image_tag: str


@dataclass
class UpstreamDep:
    table_name: str
    schema_name: str
    service_name: str

    @property
    def unique_id(self) -> str:
        """The upstream node's identity key: "<schema>.<table>", lowercased.

        Must match ManifestNode.unique_id exactly — the two are compared against
        each other when building DEPENDS_ON edges in the orchestrator's Neo4j
        topology. Deriving this property the same way ensures the identities
        always agree, preventing silent divergence from mixed-case declarations.
        """
        return f"{self.schema_name.lower()}.{self.table_name.lower()}"


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
    output_columns: list[dict] = field(default_factory=list)  # python nodes' declared
    # output shape ({name, type, nullable}); empty for dbt nodes
    node_type: NodeType = NodeType.DBT_MODEL
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
    runtime: Runtime = Runtime.DBT

    @property
    def unique_id(self) -> str:
        """The node's identity across the whole system: "<schema>.<table>",
        lowercased.

        The same string keys the topology entry, the code-bundle node map, and
        the node's candidate-artifact S3 object, so it is derived in one place.
        It is lowercased because the warehouse lookups that resolve a reference
        to this node already fold case (NodeRegistry.to_lookup and
        service/rewriter.py), so two declarations differing only in case name
        one relation and must not produce two identities. The declared
        schema_name and table_name are left untouched — they render into SQL
        and DDL, where the declared spelling is what addresses the relation.
        """
        return f"{self.schema_name.lower()}.{self.table_name.lower()}"


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
