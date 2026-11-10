import sqlglot
from sqlglot import exp
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from domain.model import ManifestNode, TableRef


def referenced_tables(node: ManifestNode, *, dialect: str) -> list[TableRef]:
    """Every table the node's dependency SQLs name, in first-seen order.

    A table is returned once, however many of the node's SQLs name it. CTE names
    and schema-qualified reads of the node's own relation are not tables the
    node reads and are skipped. A table with no schema raises
    UnqualifiedTableReferenceError: which relation it means depends on the
    session's search path, so the SQL is rejected rather than guessed at. A
    table with a schema and no name (a schema-qualified function call such as
    public.my_fn(1) or pg_catalog.generate_series(1, 3), which sqlglot models
    as a table) names no relation and is skipped.

    dialect is the sqlglot dialect of the warehouse the install targets; it
    decides which syntax parses, so a query valid on one engine can raise
    InvalidCompiledSqlError under another. It is required rather than defaulted
    so a caller cannot silently fall back to one engine's rules.
    """
    refs: list[TableRef] = []
    seen: set[str] = set()

    for dep_sql in node.dependency_sqls:
        if not dep_sql:
            continue

        try:
            parsed = sqlglot.parse_one(dep_sql, dialect=dialect)
        except (sqlglot.errors.ParseError, sqlglot.errors.TokenError) as exc:
            raise InvalidCompiledSqlError(node_table_name=node.table_name, detail=str(exc)) from exc

        cte_names = {cte.alias.lower() for cte in parsed.find_all(exp.CTE)}

        for table in parsed.find_all(exp.Table):
            name = table.name.lower()
            schema = table.db.lower()

            # CTEs are always unqualified — skip before the schema guard fires.
            if name in cte_names:
                continue

            # Skip qualified self-references; unqualified self-references fall
            # through to the guard below and raise.
            if schema and (schema, name) == (node.schema_name.lower(), node.table_name.lower()):
                continue

            if not schema:
                raise UnqualifiedTableReferenceError(
                    table_name=name,
                    node_table_name=node.table_name,
                )

            if not name:
                continue

            ref = TableRef(schema=schema, table=name, catalog=table.catalog.lower())
            if ref.key in seen:
                continue
            seen.add(ref.key)
            refs.append(ref)

    return refs
