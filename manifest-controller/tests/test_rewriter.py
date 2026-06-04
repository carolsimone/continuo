import sqlglot
from sqlglot import exp

from domain.model import NodeRegistry, NodeRegistryEntry
from service.rewriter import candidate_schema_name, rewrite_to_candidate_schema


def _registry(*tables: str, schema: str = "public") -> dict:
    entries = [
        NodeRegistryEntry(table_name=t, schema_name=schema, service_name="svc", owner="o")
        for t in tables
    ]
    return NodeRegistry(entries=entries).to_lookup()


def _schemas_of(sql_text: str) -> set[str]:
    parsed = sqlglot.parse_one(sql_text, dialect="postgres")
    return {t.db for t in parsed.find_all(exp.Table) if t.db}


def test_candidate_schema_name_sanitizes():
    assert candidate_schema_name("rel-840dd5c-4") == "_candidate_rel_840dd5c_4"
    assert candidate_schema_name("e2e-rel-abcd1234") == "_candidate_e2e_rel_abcd1234"


def test_rewrites_known_node_ref():
    out = rewrite_to_candidate_schema("SELECT id FROM public.users", _registry("users"), "_candidate_r1")
    assert _schemas_of(out) == {"_candidate_r1"}


def test_leaves_unknown_ref_untouched():
    out = rewrite_to_candidate_schema("SELECT id FROM public.external_thing", _registry("users"), "_candidate_r1")
    assert _schemas_of(out) == {"public"}


def test_rewrites_multiple_known_refs():
    out = rewrite_to_candidate_schema(
        "SELECT a.id FROM public.table_a a LEFT JOIN public.table_b b ON a.id = b.id",
        _registry("table_a", "table_b"),
        "_candidate_r1",
    )
    assert _schemas_of(out) == {"_candidate_r1"}


def test_mixed_known_and_unknown_refs():
    # table_b is not a known node → left on its production schema.
    out = rewrite_to_candidate_schema(
        "SELECT a.id FROM public.table_a a LEFT JOIN public.table_b b ON a.id = b.id",
        _registry("table_a"),
        "_candidate_r1",
    )
    assert _schemas_of(out) == {"_candidate_r1", "public"}


def test_cte_reference_not_rewritten():
    out = rewrite_to_candidate_schema(
        "WITH cte AS (SELECT id FROM public.users) SELECT * FROM cte",
        _registry("users"),
        "_candidate_r1",
    )
    # the real table (users) is redirected; the CTE alias carries no schema.
    assert _schemas_of(out) == {"_candidate_r1"}


def test_empty_sql_returns_empty():
    assert rewrite_to_candidate_schema("", _registry("users"), "_candidate_r1") == ""
