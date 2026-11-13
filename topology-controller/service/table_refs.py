import sqlglot
from sqlglot import exp
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from domain.model import ManifestNode, TableRef


def referenced_tables(node: ManifestNode, *, dialect: str, ctes: set[str] | None = None) -> list[TableRef]:
    """Every table the node's dependency SQLs name, in first-seen order.

    A table is returned once per catalog, schema and table, however many of the
    node's SQLs name it. The catalog is part of the identity because the
    system-relation filter that follows (domain.external.SystemRelations) decides
    per catalog: on an engine that keeps its own relations in a catalog,
    system.runtime.nodes and warehouse.runtime.nodes share a schema and a name,
    and merging them by schema and name would let whichever the SQL names first
    hide the other.

    A reference to a CTE and a schema-qualified read of the node's own relation
    (ManifestNode.relation, the name its build writes) are not tables the node
    reads and are skipped. Only an unqualified name is
    taken for a CTE reference: a CTE is always unqualified, so a qualified name
    is a table even when a CTE of the same name is in scope. A table with no
    schema that is not a CTE reference raises UnqualifiedTableReferenceError:
    which relation it means depends on the session's search path, so the SQL is
    rejected rather than guessed at. A
    table with a schema and no name (a schema-qualified function call such as
    public.my_fn(1) or pg_catalog.generate_series(1, 3), which sqlglot models
    as a table) names no relation and is skipped.

    ctes, when given, receives the lower-cased name of every CTE the node's SQLs
    define: the dependency resolver reads an ephemeral dbt model, which dbt
    inlines into its readers as a CTE, from those names.

    dialect is the sqlglot dialect of the warehouse the install targets; it
    decides which syntax parses, so a query valid on one engine can raise
    InvalidCompiledSqlError under another. It is required rather than defaulted
    so a caller cannot silently fall back to one engine's rules.
    """
    refs: list[TableRef] = []
    seen: set[TableRef] = set()

    for dep_sql in node.dependency_sqls:
        if not dep_sql:
            continue

        try:
            parsed = sqlglot.parse_one(dep_sql, dialect=dialect)
        except (sqlglot.errors.ParseError, sqlglot.errors.TokenError) as exc:
            raise InvalidCompiledSqlError(node_table_name=node.table_name, detail=str(exc)) from exc

        cte_names = {cte.alias.lower() for cte in parsed.find_all(exp.CTE)}
        if ctes is not None:
            ctes.update(cte_names)

        for table in parsed.find_all(exp.Table):
            name = table.name.lower()
            schema = table.db.lower()

            # A CTE is always unqualified, so only an unqualified name can be
            # one: skip it before the schema guard fires. A qualified name is a
            # table even when a CTE of the same name is in scope.
            if not schema and name in cte_names:
                continue

            # Skip qualified self-references; unqualified self-references fall
            # through to the guard below and raise.
            if schema and (schema, name) == (node.schema_name.lower(), node.relation.lower()):
                continue

            if not schema:
                raise UnqualifiedTableReferenceError(
                    table_name=name,
                    node_table_name=node.table_name,
                )

            if not name:
                continue

            ref = TableRef(schema=schema, table=name, catalog=table.catalog.lower())
            if ref in seen:
                continue
            seen.add(ref)
            refs.append(ref)

    return refs
