"""Rewrite a node's candidate SQL so its schema-qualified references point at the
blue/green candidate schema instead of production.

Blue/green validation builds a changed node's full upstream closure as empty
tables in an isolated candidate schema, in dependency order, then builds the node
against them — touching nothing in production. The models are never edited: every
schema-qualified reference that resolves to a known graph node (the same registry
the dependency resolver uses) is redirected here, at parse time, to the candidate
schema. Unqualified references (a CTE reference is one), tables owned by no
service, and the warehouse engine's own system relations (information_schema,
pg_catalog, Trino's system catalog) are left untouched: a system relation is
never a node, so it has no copy in the candidate schema.
"""
import re

import sqlglot
from sqlglot import exp

from domain.ingested import SystemRelations
from domain.model import NodeRegistryEntry, TableRef

_NON_SCHEMA_SAFE = re.compile(r"[^a-zA-Z0-9_]")


def candidate_schema_name(release_id: str) -> str:
    """Derive the candidate schema name from a release_id.

    Mirrors release-controller's sanitizeSchemaSuffix exactly: any character that
    is not [A-Za-z0-9_] becomes '_'. Both sides must agree so the rewritten refs
    match the schema execution-controller creates.
    """
    return "_candidate_" + _NON_SCHEMA_SAFE.sub("_", release_id)


def rewrite_to_candidate_schema(
    sql: str,
    registry: dict[tuple[str, str], NodeRegistryEntry],
    candidate_schema: str,
    self_schema: str = "",
    self_table: str = "",
    *,
    dialect: str,
    system_relations: SystemRelations,
) -> str:
    """Return sql with every known-node schema-qualified reference
    redirected to candidate_schema. Empty input (e.g. a seed, which has no SQL)
    is returned unchanged.

    registry is NodeRegistry.to_lookup(), keyed by the relation each node
    writes, so a reference to an alias is redirected too. Only the schema is
    replaced: the table keeps the spelling the SQL gives it.

    A system relation (see system_relations) is never redirected, whatever the
    registry holds: the engine's own table has no copy in the candidate schema,
    and a node that builds a table with the same schema and name is a different
    relation. The catalog of a read matters only for that system-relation check
    (the engine's own catalog, such as Trino's `system`). A read that is not a
    system relation is matched to the registry by its schema and relation name alone,
    whatever catalog it names, and a read of a registered table is redirected
    and keeps the catalog it named.

    A qualified self-reference (self_schema.self_table, where self_table is the
    node's relation, e.g. an incremental model selecting from {{ this }}) is
    left on its production schema: the validation runner drops and recreates
    <candidate>.<relation>, so a self-reference rewritten to the candidate
    schema would read a relation that no longer exists. This mirrors the
    dependency resolver, which also skips self-references.

    dialect is the sqlglot dialect of the warehouse the install targets. It is
    used to both read and re-render the SQL, so it decides the syntax of the
    text uploaded for validation — the postgres dialect renders a cast as
    CAST(x AS TEXT) where Trino needs CAST(x AS VARCHAR). It is required rather
    than defaulted so a caller cannot silently emit one engine's SQL to another.

    system_relations are the relations of that same engine, the ones the
    dependency resolver and the ingested-node step also treat as the engine's own.
    It is required for the same reason: a caller that forgot it would redirect an
    engine's relation to a schema that does not exist.
    """
    if not sql:
        return sql

    parsed = sqlglot.parse_one(sql, dialect=dialect)
    self_ref = (self_schema.lower(), self_table.lower())

    for table in parsed.find_all(exp.Table):
        name = table.name.lower()
        schema_raw = table.db
        # An unqualified name, a CTE reference included, carries no schema to
        # redirect. A qualified name is a table even when a CTE of the same
        # name is in scope.
        if not schema_raw:
            continue
        key = (schema_raw.lower(), name)
        if key == self_ref:
            continue
        ref = TableRef(schema=key[0], table=name, catalog=table.catalog.lower())
        if system_relations.matches(ref):
            continue
        if key in registry:
            table.set("db", exp.to_identifier(candidate_schema, quoted=True))

    return parsed.sql(dialect=dialect)
