from collections.abc import Collection, Mapping, Sequence
from domain.model import NodeRegistryEntry, TableRef, UpstreamDep


def resolve_upstream_deps(
    refs: Sequence[TableRef],
    registry: dict[tuple[str, str], NodeRegistryEntry],
    *,
    service_name: str = "",
    ctes: Collection[str] = (),
    ephemeral_ctes: Mapping[tuple[str, str], NodeRegistryEntry] | None = None,
) -> list[UpstreamDep]:
    """Resolve the tables a node's SQL names to the registry's nodes.

    registry is NodeRegistry.to_lookup(): a ref resolves through the relation
    it names, so a reference to an alias finds the node that writes it. A ref
    whose (schema, relation) the registry does not hold is an external table
    or source no known service owns, and is skipped. dbt seeds are registered
    (the in-memory registry is built from every parsed node, seeds included),
    so a seed reference resolves to an upstream dep like any other node. The
    registry knows a table by schema and name, whatever its catalog, so refs
    that differ only in catalog resolve to one dep.

    ephemeral_ctes is NodeRegistry.ephemeral_ctes(), ctes the names of the CTEs
    the node's SQL defines (referenced_tables collects them) and service_name
    the node's service: dbt inlines an ephemeral model into its readers as a
    CTE, so a CTE named like an ephemeral model of the node's own service makes
    that model an upstream. Every table read inside that CTE's body stays a
    direct upstream as well, since the node's SQL reads it.

    The ephemeral upstreams come first, in CTE name order, then the others in
    the order of each table's first ref.
    """
    deps: list[UpstreamDep] = []
    resolved: set[tuple[str, str]] = set()

    def add(entry: NodeRegistryEntry) -> None:
        key = (entry.schema_name.lower(), entry.relation.lower())
        if key in resolved:
            return
        resolved.add(key)
        deps.append(UpstreamDep(
            table_name=entry.table_name,
            schema_name=entry.schema_name,
            service_name=entry.service_name,
        ))

    if ephemeral_ctes:
        for cte_name in sorted(ctes):
            ephemeral = ephemeral_ctes.get((service_name, cte_name))
            if ephemeral is not None:
                add(ephemeral)

    for ref in refs:
        entry = registry.get((ref.schema, ref.table))
        if entry is not None:
            add(entry)
    return deps
