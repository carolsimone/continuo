from __future__ import annotations
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from domain.contract_vocabulary import NodeRuntime, NodeType
from domain.model import ManifestNode, TableRef, UpstreamDep


@dataclass(frozen=True)
class SystemRelations:
    """The relations a warehouse engine keeps for itself: its catalog views and
    internal tables, which no release produces and which therefore never become
    nodes.

    A relation is a system relation when its schema is one of schemas, when its
    schema starts with one of schema_prefixes, or when its catalog is one of
    catalogs. Every name is lower-case, as a TableRef holds it.
    """

    schemas: frozenset[str] = frozenset()
    schema_prefixes: tuple[str, ...] = ()
    catalogs: frozenset[str] = frozenset()

    def matches(self, ref: TableRef) -> bool:
        return (
            ref.schema in self.schemas
            or ref.schema.startswith(self.schema_prefixes)
            or ref.catalog in self.catalogs
        )


def unknown_external_nodes(
    nodes: Sequence[ManifestNode],
    reads: Mapping[str, Sequence[TableRef]],
    *,
    system: SystemRelations,
) -> list[ManifestNode]:
    """The nodes for the tables that nodes read and no node builds, sorted by
    unique_id.

    reads holds the tables each reading node's SQL names, by the node's
    unique_id; a node with no entry (a dbt test, or a node with no SQL) reads
    nothing. A table counts as built when a node that is not a dbt test writes
    it, under its declared name or under the name its build actually writes
    (an aliased dbt model). A table in one of the system's relations is the
    warehouse engine's own and never becomes a node. Each table left is one
    unknown-external node, whatever the number of readers or the spelling of its name:
    refs are lower-case, so "<schema>.<table>" is the key.

    The only state it changes is the readers' upstream_deps, which gain one
    edge to each unknown-external table they read, once. The returned nodes have no
    service, schedule, owner, content or image, and no builder: nothing in the
    release produces them.
    """
    built: set[str] = set()
    for node in nodes:
        if node.node_type == NodeType.DBT_TEST:
            continue
        built.add(f"{node.schema_name.lower()}.{node.table_name.lower()}")
        built.add(node.resolved_relation_id)

    found: dict[str, ManifestNode] = {}
    for node in nodes:
        if node.node_type == NodeType.DBT_TEST:
            continue
        edges: set[str] = set()
        for ref in reads.get(node.unique_id, ()):
            if ref.key in built or system.matches(ref) or ref.key in edges:
                continue
            edges.add(ref.key)
            if ref.key not in found:
                found[ref.key] = ManifestNode(
                    table_name=ref.table,
                    schema_name=ref.schema,
                    service_name="",
                    owner="",
                    schedule_name="",
                    criticality="",
                    node_type=NodeType.UNKNOWN_EXTERNAL,
                    runtime=NodeRuntime.EXTERNAL,
                )
            node.upstream_deps.append(UpstreamDep(table_name=ref.table, schema_name=ref.schema, service_name=""))
    return [found[key] for key in sorted(found)]
