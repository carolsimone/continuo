import sqlglot
from sqlglot import exp
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from domain.model import ManifestNode, NodeRegistryEntry, UpstreamDep


def resolve_upstream_deps(
    node: ManifestNode,
    registry: dict[tuple[str, str], NodeRegistryEntry],
) -> list[UpstreamDep]:
    deps: list[UpstreamDep] = []
    seen: set[tuple[str, str]] = set()

    for dep_sql in node.dependency_sqls:
        if not dep_sql:
            continue

        try:
            parsed = sqlglot.parse_one(dep_sql, dialect="postgres")
        except (sqlglot.errors.ParseError, sqlglot.errors.TokenError) as exc:
            raise InvalidCompiledSqlError(node_table_name=node.table_name, detail=str(exc)) from exc

        cte_names = {cte.alias.lower() for cte in parsed.find_all(exp.CTE)}

        for table in parsed.find_all(exp.Table):
            name = table.name.lower()
            schema_raw = table.db

            # CTEs are always unqualified — skip before the schema guard fires.
            if name in cte_names:
                continue

            # Skip qualified self-references; unqualified self-references fall
            # through to the guard below and raise.
            if schema_raw and (schema_raw.lower(), name) == (node.schema_name.lower(), node.table_name.lower()):
                continue

            if not schema_raw:
                raise UnqualifiedTableReferenceError(
                    table_name=name,
                    node_table_name=node.table_name,
                )

            key = (schema_raw.lower(), name)
            if key in seen:
                continue
            if key not in registry:
                # Table is not in the registry — it's an external table or source
                # not owned by any known service. Skip it.
                # Note: dbt seeds ARE registered (pass 2 writes them to the registry CSV),
                # so seed references are resolved as upstream deps, not skipped here.
                continue
            seen.add(key)
            entry = registry[key]
            deps.append(UpstreamDep(
                table_name=entry.table_name,
                schema_name=entry.schema_name,
                service_name=entry.service_name,
            ))

    return deps
