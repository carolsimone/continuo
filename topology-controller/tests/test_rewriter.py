import pytest
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


def _schemas_of(sql_text: str, dialect: str = "postgres") -> set[str]:
    parsed = sqlglot.parse_one(sql_text, dialect=dialect)
    return {t.db for t in parsed.find_all(exp.Table) if t.db}


def _rewrite(*args, dialect: str = "postgres", **kwargs) -> str:
    """Rewrite against the postgres dialect unless a test names another.

    rewrite_to_candidate_schema takes dialect as a required keyword so
    production code cannot emit one engine's SQL to another; these cases are
    about redirection behaviour, not dialect selection, so they pin the default
    engine here.
    """
    return rewrite_to_candidate_schema(*args, dialect=dialect, **kwargs)


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
        rewrite_to_candidate_schema("SELECT 1", _registry("users"), "_candidate_r1")
