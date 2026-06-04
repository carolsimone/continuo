"""Rewrite a node's compiled SQL so its schema-qualified references point at the
blue/green candidate schema instead of production.

Blue/green validation builds a changed node's full upstream closure as empty
tables in an isolated candidate schema, in dependency order, then builds the node
against them — touching nothing in production. The models are never edited: every
schema-qualified reference that resolves to a known graph node (the same registry
the dependency resolver uses) is redirected here, at parse time, to the candidate
schema. Unqualified references, CTEs, and tables owned by no service are left
untouched.
"""
import re

import sqlglot
from sqlglot import exp

from domain.model import NodeRegistryEntry

_NON_SCHEMA_SAFE = re.compile(r"[^a-zA-Z0-9_]")


def candidate_schema_name(release_id: str) -> str:
    """Derive the candidate schema name from a release_id.

    Mirrors release-controller's sanitizeSchemaSuffix exactly: any character that
    is not [A-Za-z0-9_] becomes '_'. Both sides must agree so the rewritten refs
    match the schema the executor creates.
    """
    return "_candidate_" + _NON_SCHEMA_SAFE.sub("_", release_id)


def rewrite_to_candidate_schema(
    compiled_sql: str,
    registry: dict[tuple[str, str], NodeRegistryEntry],
    candidate_schema: str,
    self_schema: str = "",
    self_table: str = "",
) -> str:
    """Return compiled_sql with every known-node schema-qualified reference
    redirected to candidate_schema. Empty input (e.g. a seed, which has no SQL)
    is returned unchanged.

    A qualified self-reference (self_schema.self_table — e.g. an incremental model
    selecting from {{ this }}) is left on its production schema: the validation
    runner drops and recreates <candidate>.<table>, so a self-reference rewritten
    to the candidate schema would read a relation that no longer exists. This
    mirrors the dependency resolver, which also skips self-references.
    """
    if not compiled_sql:
        return compiled_sql

    parsed = sqlglot.parse_one(compiled_sql, dialect="postgres")
    cte_names = {cte.alias.lower() for cte in parsed.find_all(exp.CTE)}
    self_ref = (self_schema.lower(), self_table.lower())

    for table in parsed.find_all(exp.Table):
        name = table.name.lower()
        schema_raw = table.db
        # CTE references and unqualified names carry no schema to redirect.
        if name in cte_names or not schema_raw:
            continue
        key = (schema_raw.lower(), name)
        if key == self_ref:
            continue
        if key in registry:
            table.set("db", exp.to_identifier(candidate_schema, quoted=True))

    return parsed.sql(dialect="postgres")
