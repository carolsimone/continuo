from __future__ import annotations
from dataclasses import dataclass, field
from enum import StrEnum

from domain.contract_vocabulary import ParseFailureKind
from domain.contract_vocabulary import NodeRuntime as Runtime
from domain.contract_vocabulary import NodeType

DEFAULT_TENANT_ID = "default"
"""The tenant every release belongs to on a single-tenant install. It prefixes
the topology artifact's key and stamps every event this service writes."""


class ManifestKind(StrEnum):
    """Which artifact dialect a service's release payload speaks.

    Carried per manifest_keys entry on release.requested:v1. Absent means dbt,
    so a producer that predates python support needs no change.
    """
    DBT = "dbt"
    PYTHON = "python"


class SkipReason(StrEnum):
    """Why the parse leg leaves a dbt node out of the topology without failing
    the release: it declares no owner, it is a local stub, or it is a node of an
    installed dbt package (not of the project the manifest was compiled from)
    that carries no tag. A value is the string the logs report. A model or
    snapshot of the project with an owner and no schedule tag is not left out:
    it is a dormant node with an empty schedule, and an untagged seed takes the
    schedule `seed`. A node of an installed package with an owner and a tag (and
    no `local_stub` tag) is not left out either: it is kept under the package's
    service, which the release's service check refuses."""
    NO_OWNER = "no_owner"
    LOCAL_STUB = "local_stub"
    INSTALLED_PACKAGE = "installed_package"


@dataclass(frozen=True)
class SkippedNode:
    """One dbt node the parse leg left out of the topology, and why. A skip is
    never a failure: the node simply is not part of the release."""
    node_id: str
    service: str
    reason: SkipReason


@dataclass(frozen=True)
class FailedNode:
    """One node the parse leg could not resolve, with where its source lives.

    The topology is never published for a release with a broken node, so the
    location travels here: it is what lets release-controller record a
    per-node result and the remediation agent open the right file.
    """
    node_id: str
    kind: ParseFailureKind
    service: str
    file_path: str
    node_type: str
    detail: str


@dataclass(frozen=True)
class TopologyArtifactRef:
    """Where a release's topology artifact lives, the SHA-256 of its stored
    bytes, and how many nodes it holds: the reference the parse result carries
    instead of the nodes themselves."""
    uri: str
    sha256: str
    node_count: int


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
    image_tag: str = ""
    declared_service: str = ""
    # The raw wire value, not narrowed to ManifestKind: an unrecognized kind is
    # a permanent failure the handler reports, not an exception at decode time.
    kind: str = ManifestKind.DBT
    size_bytes: int = 0  # the fetched file's size on disk
    # The downloaded compile-extras.json that sits beside a dbt manifest; "" when
    # the object is absent or the artifact is not dbt.
    extras_path: str = ""


@dataclass(frozen=True)
class SeedExtras:
    """What the compile recorded about one seed: the SHA-256 of its file bytes,
    its CSV header and the column types its configuration declares."""
    sha256: str
    header: tuple[str, ...]
    column_types: dict[str, str]


@dataclass(frozen=True)
class CompileExtras:
    """The contents of a compile-extras.json: what a dbt compile knows about a
    service that its manifest does not carry. `seeds` is keyed by the seed's dbt
    unique id and `packages` maps each installed dbt distribution to its version."""
    service: str
    release_id: str
    seeds: dict[str, SeedExtras]
    project_flags: dict
    packages: dict[str, str]


@dataclass(frozen=True)
class ServiceProvenance:
    """What one service's dbt compile ran with: the dbt version its manifest
    names, the installed dbt distributions and their versions (empty without
    compile extras), and the unique ids of its seeds whose source part is
    still dbt's `path` checksum, sorted."""
    dbt_version: str
    packages: dict[str, str]
    path_checksum_seeds: tuple[str, ...]


@dataclass
class UpstreamDep:
    table_name: str
    schema_name: str
    service_name: str

    @property
    def unique_id(self) -> str:
        """The upstream node's identity key: "<schema>.<table>", lowercased,
        where table is the upstream's declared table_name ("<name>.v<version>"
        for a versioned dbt model).

        Must match ManifestNode.unique_id exactly — the two are compared against
        each other when building DEPENDS_ON edges in catalog's Neo4j
        topology. Deriving this property the same way ensures the identities
        always agree, preventing silent divergence from mixed-case declarations.
        """
        return f"{self.schema_name.lower()}.{self.table_name.lower()}"


@dataclass(frozen=True)
class TableRef:
    """A table a node's SQL names, as written, lower-cased.

    catalog is empty when the SQL does not name one. key is the "<schema>.<table>"
    identity a registry lookup and a node's unique_id use.
    """

    schema: str
    table: str
    catalog: str = ""

    @property
    def key(self) -> str:
        return f"{self.schema}.{self.table}"


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
    content_hash: str = ""  # sha256:-prefixed fold of the parts: a dbt node's
    # source_hash|shared_code_hash|config_hash|compiled_hash, a python node's first three
    image_tag: str = ""
    original_file_path: str = ""  # dbt original_file_path (project-relative)
    test_count: int = 0  # number of dbt tests attached to this node
    upstream_deps: list[UpstreamDep] = field(default_factory=list)
    raw_code: str = ""  # node's own source (dbt raw_code), pre-compilation
    config: dict = field(default_factory=dict)  # node's resolved dbt config
    source_hash: str = ""  # fingerprint of this node's own source (see dbt_hashing.source_part)
    shared_code_hash: str = ""  # shared_fold of the node's shared-code units; "" if none
    config_hash: str = ""  # fingerprint of the hashed config (dbt) or code projection (python)
    compiled_hash: str = ""  # fingerprint of a dbt model's or snapshot's masked
    # compiled SQL; "" for every other node
    hashed_config: dict | None = None  # the config a dbt node's config_hash hashes;
    # None for a python node
    code_unit_ids: list[str] = field(default_factory=list)  # the shared-code units its
    # shared part folds, sorted
    runtime: Runtime = Runtime.DBT
    resolved_relation: str = ""  # the name this node's build actually writes: a
    # dbt node's alias when it overrides one, else "" (see resolved_relation_id,
    # which falls back to table_name). A python node has no alias concept, so
    # its parser sets this to its declared table_name directly.
    csv_source: str = ""  # the csv uri for python-csv nodes; empty otherwise —
    # NOT schema-rewritten, it is a file location, not a warehouse reference.
    secret_ref: str = ""  # the continuo-api-* Secret a python-api node's pod
    # receives as env vars; empty when the node names none.
    timeout_seconds: int | None = None  # the node's declared task timeout in
    # seconds (dbt meta.timeout_minutes); None when the node declares none.
    identity: str = ""  # explicit unique_id for a node that is not a relation
    # (a dbt test): dbt's own manifest unique_id. Empty for every relation
    # node, whose identity is "<schema>.<table>".
    declared_columns: int = 0  # how many columns the node declares: a dbt
    # node's `columns` block, a python node's output columns.
    seed_columns_hash: str = ""  # a dbt seed's column fingerprint
    # (compile_extras.seed_columns_hash); "" without an extras entry for the seed
    seed_header: tuple[str, ...] = ()  # a dbt seed's CSV header, in file order;
    # () without an extras entry for the seed
    seed_column_types: dict[str, str] = field(default_factory=dict)  # a dbt
    # seed's configured column types as its compile wrote them; {} without an
    # extras entry for the seed
    projection: str = ""  # a python node's code projection as JSON text; ""
    # for a dbt node
    materialized: str = ""  # a dbt node's config.materialized ("table",
    # "view", "incremental", "ephemeral", ...); "" for a python node
    full_refresh: bool | None = None  # a dbt seed's config.full_refresh when it is
    # a bool; None for a seed that does not set one and for every other node

    @property
    def seed_columns(self) -> list[dict[str, str]]:
        """A dbt seed's columns in file order, each as {"name", "type"}: the type is
        the configured one, "" for a column whose type the seed does not declare.
        Empty without a header."""
        return [{"name": h, "type": self.seed_column_types.get(h, "")} for h in self.seed_header]

    @property
    def unique_id(self) -> str:
        """The node's identity across the whole system: "<schema>.<table>",
        lowercased. A versioned dbt model's table_name is "<name>.v<version>",
        so each version is a node of its own: "<schema>.<name>.v<version>".

        The same string keys the topology entry, the code-bundle node map, and
        the node's candidate-artifact S3 object, so it is derived in one place.
        It is lowercased because the warehouse lookups that resolve a reference
        to this node already fold case (NodeRegistry.to_lookup and
        service/rewriter.py), so two declarations differing only in case name
        one relation and must not produce two identities. The declared
        schema_name and table_name are left untouched — they render into SQL
        and DDL, where the declared spelling is what addresses the relation.

        A dbt test is not a relation: it writes no table and dbt already
        gives it a globally unique manifest id, so that id is its identity
        (identity), never "<schema>.<name>".
        """
        if self.identity:
            return self.identity
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

        A versioned dbt model resolves to its alias, which dbt defaults to
        "<name>_v<version>".

        A dbt test writes no relation and resolves to "" so it can never
        claim, or collide on, a table.
        """
        if self.node_type == NodeType.DBT_TEST:
            return ""
        return f"{self.schema_name.lower()}.{self.relation.lower()}"

    @property
    def relation(self) -> str:
        """The name of the relation this node's build writes, in its declared
        spelling: resolved_relation when set, else table_name. A reference to
        the node in another node's SQL names this relation, and a node reading
        it is reading itself."""
        return self.resolved_relation or self.table_name

    @property
    def relation_name(self) -> str:
        """relation when it differs from table_name (an alias, or a versioned
        model's "<name>_v<version>"), else "". A dbt test writes no relation,
        so its relation_name is always ""."""
        if self.node_type == NodeType.DBT_TEST or self.relation == self.table_name:
            return ""
        return self.relation


@dataclass
class NodeRegistryEntry:
    """One node a release's SQL can reference: its declared table_name (which
    names it, see ManifestNode.unique_id), the relation its build writes (which
    SQL names it by) and whether it is an ephemeral dbt model, which writes no
    relation and reaches its readers as a CTE dbt inlines into their SQL."""
    table_name: str
    schema_name: str
    service_name: str
    owner: str
    relation: str
    ephemeral: bool = False


@dataclass
class NodeRegistry:
    """Every node of a release that SQL can reference: each model, seed,
    snapshot and python node, never a test, which writes no relation."""
    entries: list[NodeRegistryEntry]

    @classmethod
    def of_nodes(cls, nodes: list[ManifestNode]) -> NodeRegistry:
        """The registry of a release's nodes: one entry per node that is not a
        dbt test, keyed later by the relation the node writes."""
        return cls(entries=[
            NodeRegistryEntry(
                table_name=n.table_name,
                schema_name=n.schema_name,
                service_name=n.service_name,
                owner=n.owner,
                relation=n.relation,
                ephemeral=n.materialized == "ephemeral",
            )
            for n in nodes
            if n.node_type != NodeType.DBT_TEST
        ])

    def to_lookup(self) -> dict[tuple[str, str], NodeRegistryEntry]:
        """Each entry keyed by (schema, relation), lowercased: the key a
        schema-qualified table reference in SQL resolves through, so a
        reference to an alias finds the node that writes it."""
        return {(e.schema_name.lower(), e.relation.lower()): e for e in self.entries}

    def ephemeral_ctes(self) -> dict[tuple[str, str], NodeRegistryEntry]:
        """Each ephemeral entry keyed by (service, CTE name): dbt inlines an
        ephemeral model into a reader of the same project as a CTE named
        "__dbt__cte__<relation>", so a CTE of that name in a node of that
        service is a read of the ephemeral model."""
        return {
            (e.service_name, "__dbt__cte__" + e.relation.lower()): e
            for e in self.entries
            if e.ephemeral
        }


@dataclass(frozen=True)
class ParsedArtifact:
    """The result of parsing one service's artifact: its nodes, the shared
    code (dbt macros) they use, keyed by unit id, and, for a dbt manifest, what
    its compile ran with (None for a python contract)."""
    nodes: list[ManifestNode]
    shared_code: dict[str, dict]
    provenance: ServiceProvenance | None = None
