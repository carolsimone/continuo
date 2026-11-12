import pytest
import sqlglot
from sqlglot import exp

from config.config import _SYSTEM_RELATIONS
from domain.model import NodeRegistry, NodeRegistryEntry
from service.rewriter import candidate_schema_name, rewrite_to_candidate_schema

POSTGRES = _SYSTEM_RELATIONS["postgres"]
TRINO = _SYSTEM_RELATIONS["trino"]


def _registry(*tables: str, schema: str = "public") -> dict:
    entries = [
        NodeRegistryEntry(table_name=t, schema_name=schema, service_name="svc", owner="o", relation=t)
        for t in tables
    ]
    return NodeRegistry(entries=entries).to_lookup()


def _schemas_of(sql_text: str, dialect: str = "postgres") -> set[str]:
    parsed = sqlglot.parse_one(sql_text, dialect=dialect)
    return {t.db for t in parsed.find_all(exp.Table) if t.db}


def _rewrite(*args, dialect: str = "postgres", system_relations=POSTGRES, **kwargs) -> str:
    """Rewrite against the postgres dialect and its system relations unless a
    test names another engine.

    rewrite_to_candidate_schema takes dialect and system_relations as required
    keywords so production code cannot emit one engine's SQL to another; these
    cases are about redirection behaviour, not engine selection, so they pin the
    default engine here.
    """
    return rewrite_to_candidate_schema(
        *args, dialect=dialect, system_relations=system_relations, **kwargs,
    )


def _qualified_tables(sql_text: str, dialect: str) -> list[str]:
    """Every table the SQL names as catalog.schema.table, omitting the parts it
    leaves out, in source order."""
    parsed = sqlglot.parse_one(sql_text, dialect=dialect)
    return [
        ".".join(part for part in (t.catalog, t.db, t.name) if part)
        for t in parsed.find_all(exp.Table)
    ]


def test_candidate_schema_name_sanitizes():
    assert candidate_schema_name("rel-840dd5c-4") == "_candidate_rel_840dd5c_4"
    assert candidate_schema_name("e2e-rel-abcd1234") == "_candidate_e2e_rel_abcd1234"


def test_rewrites_known_node_ref():
    out = _rewrite("SELECT id FROM public.users", _registry("users"), "_candidate_r1")
    assert _schemas_of(out) == {"_candidate_r1"}


def test_leaves_unknown_ref_untouched():
    out = _rewrite("SELECT id FROM public.external_thing", _registry("users"), "_candidate_r1")
    assert _schemas_of(out) == {"public"}


def test_rewrites_multiple_known_refs():
    out = _rewrite(
        "SELECT a.id FROM public.table_a a LEFT JOIN public.table_b b ON a.id = b.id",
        _registry("table_a", "table_b"),
        "_candidate_r1",
    )
    assert _schemas_of(out) == {"_candidate_r1"}


def test_mixed_known_and_unknown_refs():
    # table_b is not a known node → left on its production schema.
    out = _rewrite(
        "SELECT a.id FROM public.table_a a LEFT JOIN public.table_b b ON a.id = b.id",
        _registry("table_a"),
        "_candidate_r1",
    )
    assert _schemas_of(out) == {"_candidate_r1", "public"}


def test_cte_reference_not_rewritten():
    out = _rewrite(
        "WITH cte AS (SELECT id FROM public.users) SELECT * FROM cte",
        _registry("users"),
        "_candidate_r1",
    )
    # the real table (users) is redirected; the CTE alias carries no schema.
    assert _schemas_of(out) == {"_candidate_r1"}


def test_a_qualified_table_is_rewritten_when_a_cte_has_its_name():
    # A CTE is always unqualified, so public.users is the table even though a
    # CTE of the same name is in scope. Leaving it on its production schema
    # makes validation read production instead of the candidate schema.
    out = _rewrite(
        "WITH users AS (SELECT 1 AS id) "
        "SELECT u.id FROM users u JOIN public.users p ON p.id = u.id",
        _registry("users"),
        "_candidate_r1",
    )
    assert _schemas_of(out) == {"_candidate_r1"}


def test_an_unqualified_reference_to_a_cte_is_not_given_a_schema():
    out = _rewrite(
        "WITH users AS (SELECT 1 AS id) SELECT id FROM users",
        _registry("users"),
        "_candidate_r1",
    )
    assert _schemas_of(out) == set()


def test_a_qualified_system_read_is_not_redirected_when_a_cte_has_its_name():
    # The CTE of the same name does not make the qualified read a CTE reference,
    # and the system-relation rule still keeps it on the engine's own schema.
    out = _rewrite(
        "WITH tables AS (SELECT 1 AS id) "
        "SELECT * FROM tables t JOIN information_schema.tables i ON 1 = 1",
        _registry("tables", schema="information_schema"),
        "_candidate_r1",
    )
    assert _schemas_of(out) == {"information_schema"}


def test_self_reference_not_rewritten():
    # A qualified self-reference (e.g. an incremental model selecting from itself)
    # must stay on its prod schema — the runner drops+recreates the candidate copy,
    # so a self-ref rewritten to the candidate schema would read a dropped relation.
    out = _rewrite(
        "SELECT id FROM public.orders WHERE id > 0",
        _registry("orders"),
        "_candidate_r1",
        self_schema="public",
        self_table="orders",
    )
    assert _schemas_of(out) == {"public"}


def test_self_reference_left_but_real_upstream_rewritten():
    out = _rewrite(
        "SELECT o.id FROM public.orders o JOIN public.users u ON o.uid = u.id",
        _registry("orders", "users"),
        "_candidate_r1",
        self_schema="public",
        self_table="orders",
    )
    # orders (self) stays on prod; users (a real upstream) is redirected.
    assert _schemas_of(out) == {"public", "_candidate_r1"}


def test_empty_sql_returns_empty():
    assert _rewrite("", _registry("users"), "_candidate_r1") == ""


def test_dialect_governs_the_rendered_sql():
    # The rewriter re-renders the parse tree, so the dialect decides the syntax
    # of the text uploaded for validation — not just how the input is read.
    # A cast renders as TEXT on postgres and VARCHAR on trino; emitting the
    # postgres form to a Trino warehouse is SQL that engine rejects.
    sql = "SELECT CAST(id AS TEXT) FROM public.users"
    registry = _registry("users")

    assert "CAST(id AS TEXT)" in _rewrite(sql, registry, "_candidate_r1", dialect="postgres")
    assert "CAST(id AS VARCHAR)" in _rewrite(sql, registry, "_candidate_r1", dialect="trino")


def test_dialect_rewrites_schema_under_any_engine():
    # Redirection itself is engine-independent: the known-node reference lands
    # on the candidate schema whichever dialect renders it.
    out = _rewrite("SELECT id FROM public.users", _registry("users"), "_candidate_r1", dialect="trino")
    assert _schemas_of(out, dialect="trino") == {"_candidate_r1"}


def test_dialect_is_required():
    # Production callers must name the engine; there is no implicit postgres
    # fallback that would emit the wrong engine's SQL to the warehouse.
    with pytest.raises(TypeError):
        rewrite_to_candidate_schema(
            "SELECT 1", _registry("users"), "_candidate_r1", system_relations=POSTGRES,
        )


def test_system_relations_are_required():
    # Production callers must name the engine's system relations; there is no
    # empty default that would redirect an engine's own relation to a candidate
    # schema that does not hold it.
    with pytest.raises(TypeError):
        rewrite_to_candidate_schema(
            "SELECT 1", _registry("users"), "_candidate_r1", dialect="postgres",
        )


def test_a_trino_system_catalog_read_is_not_redirected_to_the_candidate_schema():
    # system.runtime.nodes is the engine's own table. A node that builds
    # runtime.nodes puts ('runtime', 'nodes') in the registry, but redirecting
    # the engine's table to the candidate schema names one that does not exist.
    out = _rewrite(
        "SELECT * FROM system.runtime.nodes",
        _registry("nodes", schema="runtime"),
        "_candidate_r1",
        dialect="trino",
        system_relations=TRINO,
    )
    assert _qualified_tables(out, "trino") == ["system.runtime.nodes"]


def test_a_trino_read_under_another_catalog_is_still_redirected():
    # The registry knows a table by schema and name: the database part of a
    # read that is not the engine's own is ignored, so the redirect keeps the
    # catalog the SQL named.
    out = _rewrite(
        "SELECT * FROM warehouse.runtime.nodes",
        _registry("nodes", schema="runtime"),
        "_candidate_r1",
        dialect="trino",
        system_relations=TRINO,
    )
    assert _qualified_tables(out, "trino") == ["warehouse._candidate_r1.nodes"]


@pytest.mark.parametrize("sql", [
    "SELECT * FROM system.runtime.nodes n JOIN warehouse.runtime.nodes w ON 1 = 1",
    "SELECT * FROM warehouse.runtime.nodes w JOIN system.runtime.nodes n ON 1 = 1",
])
def test_a_system_read_and_a_real_read_of_the_same_table_name_part_ways_in_either_order(sql):
    out = _rewrite(
        sql, _registry("nodes", schema="runtime"), "_candidate_r1",
        dialect="trino", system_relations=TRINO,
    )
    assert sorted(_qualified_tables(out, "trino")) == [
        "system.runtime.nodes", "warehouse._candidate_r1.nodes",
    ]


def test_an_information_schema_read_is_not_redirected_even_when_the_registry_names_it():
    # No release builds information_schema.tables, so the registry misses it
    # in practice. The rewriter does not lean on that: a system relation is
    # left alone whatever the registry holds.
    out = _rewrite(
        "SELECT * FROM information_schema.tables",
        _registry("tables", schema="information_schema"),
        "_candidate_r1",
        dialect="trino",
        system_relations=TRINO,
    )
    assert _qualified_tables(out, "trino") == ["information_schema.tables"]


def test_a_postgres_system_schema_read_is_not_redirected_even_when_the_registry_names_it():
    out = _rewrite(
        "SELECT c.relname FROM pg_catalog.pg_class c JOIN public.users u ON 1 = 1",
        _registry("pg_class", schema="pg_catalog") | _registry("users"),
        "_candidate_r1",
    )
    assert sorted(_qualified_tables(out, "postgres")) == ["_candidate_r1.users", "pg_catalog.pg_class"]
