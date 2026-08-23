from __future__ import annotations
from dataclasses import dataclass, field
from enum import StrEnum


class NodeType(StrEnum):
    DBT_MODEL = "dbt-model"
    DBT_SEED = "dbt-seed"
    DBT_SNAPSHOT = "dbt-snapshot"
    PYTHON_MODEL = "python-model"
    PYTHON_CSV = "python-csv"


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
    resolved_relation: str = ""  # the name this node's build actually writes: a
    # dbt node's alias when it overrides one, else "" (see resolved_relation_id,
    # which falls back to table_name). A python node has no alias concept, so
    # its parser sets this to its declared table_name directly.
    csv_source: str = ""  # the csv uri for python-csv nodes; empty otherwise —
    # NOT schema-rewritten, it is a file location, not a warehouse reference.

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

    @property
    def resolved_relation_id(self) -> str:
        """The physical warehouse relation this node's build actually writes:
        "<schema>.<resolved name>", lowercased, mirroring how unique_id is
        minted.

        unique_id is keyed on the DECLARED name (table_name); this is keyed on
        the RESOLVED one — a dbt node's alias when it has one, else the same
        declared name. dbt writes to <schema>.<alias>, not <schema>.<name>, so
        two nodes with different declared names but the same alias write the
        same table and must be caught as a collision that unique_id alone
        would miss; two nodes sharing a declared name but given different
        aliases write different tables and must not be flagged. Falls back to
        table_name here — not only in the dbt parser — so any node built
        without going through that parser (a python node, or a node built
        directly in a test) still resolves correctly.
        """
        resolved = self.resolved_relation or self.table_name
        return f"{self.schema_name.lower()}.{resolved.lower()}"


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
