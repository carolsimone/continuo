from collections.abc import Sequence
from domain.model import NodeRegistryEntry, TableRef, UpstreamDep


def resolve_upstream_deps(
    refs: Sequence[TableRef],
    registry: dict[tuple[str, str], NodeRegistryEntry],
) -> list[UpstreamDep]:
    """Resolve the tables a node's SQL names to the registry's nodes.

    A ref whose (schema, table) the registry does not hold is an external table
    or source no known service owns, and is skipped. dbt seeds are registered
    (the in-memory registry is built from every parsed node, seeds included),
    so a seed reference resolves to an upstream dep like any other node. The
    registry knows a table by schema and name, whatever its catalog, so refs
    that differ only in catalog resolve to one dep. The deps come back in the
    order of each table's first ref.
    """
    deps: list[UpstreamDep] = []
    resolved: set[tuple[str, str]] = set()
    for ref in refs:
        entry = registry.get((ref.schema, ref.table))
        if entry is None or (ref.schema, ref.table) in resolved:
            continue
        resolved.add((ref.schema, ref.table))
        deps.append(UpstreamDep(
            table_name=entry.table_name,
            schema_name=entry.schema_name,
            service_name=entry.service_name,
        ))
    return deps
