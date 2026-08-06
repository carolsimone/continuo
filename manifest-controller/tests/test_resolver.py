import pytest
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from domain.model import ManifestNode, NodeRegistry, NodeRegistryEntry
from service.resolver import resolve_upstream_deps


def _make_registry(*table_names: str) -> dict:
    entries = [
        NodeRegistryEntry(
            table_name=t,
            schema_name="public",
            service_name="service-1",
            owner="data-platform",
        )
        for t in table_names
    ]
    return NodeRegistry(entries=entries).to_lookup()


def _make_node(table_name: str, sql: str = "", *, dependency_sqls: list[str] | None = None) -> ManifestNode:
    if dependency_sqls is None:
        dependency_sqls = [sql] if sql else []
    return ManifestNode(
        table_name=table_name,
        schema_name="public",
        service_name="service-1",
        owner="data-platform",
        schedule_name="daily",
        criticality="SECONDARY",
        dependency_sqls=dependency_sqls,
        candidate_sql=sql,
    )


def test_resolves_single_upstream():
    registry = _make_registry("users")
    node = _make_node("orders", "SELECT id FROM public.users")
    deps = resolve_upstream_deps(node, registry)
    assert len(deps) == 1
    assert deps[0].table_name == "users"


def test_skips_self_reference():
    registry = _make_registry("orders")
    node = _make_node("orders", "SELECT id FROM public.orders")
    deps = resolve_upstream_deps(node, registry)
    assert deps == []


def test_skips_cte_names():
    registry = _make_registry("raw_orders")
    sql = """
        WITH cte AS (SELECT id FROM public.raw_orders)
        SELECT id FROM cte
    """
    node = _make_node("orders", sql)
    deps = resolve_upstream_deps(node, registry)
    assert len(deps) == 1
    assert deps[0].table_name == "raw_orders"


def test_deduplicates_upstreams():
    registry = _make_registry("users")
    sql = "SELECT a.id, b.id FROM public.users a JOIN public.users b ON a.id = b.id"
    node = _make_node("orders", sql)
    deps = resolve_upstream_deps(node, registry)
    assert len(deps) == 1


def test_skips_unknown_table():
    # Tables not in the registry (seeds, sources, external refs) are silently
    # skipped — they are not cross-service model dependencies.
    registry = _make_registry("users")
    node = _make_node("orders", "SELECT id FROM public.unknown_table")
    deps = resolve_upstream_deps(node, registry)
    assert deps == []


def test_empty_dep_sql_entry_is_skipped_but_real_query_still_resolves():
    # A blank dependency_sqls entry (e.g. a seed slot in a mixed list) must be
    # skipped by the `if not dep_sql: continue` guard without short-circuiting
    # resolution of the other, real queries in the same list.
    registry = _make_registry("users")
    node = _make_node("orders", dependency_sqls=["", "SELECT id FROM public.users"])
    deps = resolve_upstream_deps(node, registry)
    assert len(deps) == 1
    assert deps[0].table_name == "users"


def test_multiple_upstreams():
    registry = _make_registry("users", "products")
    sql = "SELECT u.id, p.id FROM public.users u JOIN public.products p ON u.id = p.user_id"
    node = _make_node("orders", sql)
    deps = resolve_upstream_deps(node, registry)
    assert len(deps) == 2
    names = {d.table_name for d in deps}
    assert names == {"users", "products"}


def test_unqualified_table_raises():
    registry = _make_registry("users")
    node = _make_node("orders", "SELECT id FROM users")
    with pytest.raises(UnqualifiedTableReferenceError) as exc_info:
        resolve_upstream_deps(node, registry)
    assert exc_info.value.table_name == "users"
    assert exc_info.value.node_table_name == "orders"


def test_resolves_same_table_name_in_different_schemas():
    # Core regression test: same table_name in two schemas must resolve
    # to the correct service — the bug this change fixes.
    lookup = NodeRegistry(entries=[
        NodeRegistryEntry("orders", "public", "service-a", "team-a"),
        NodeRegistryEntry("orders", "billing", "service-b", "team-b"),
    ]).to_lookup()
    node = _make_node("summary", "SELECT id FROM billing.orders")
    deps = resolve_upstream_deps(node, lookup)
    assert len(deps) == 1
    assert deps[0].service_name == "service-b"


def test_schema_mismatch_returns_no_dep():
    # If SQL references billing.orders but registry only has public.orders,
    # no dep should be resolved — schema mismatch means different tables.
    registry = _make_registry("orders")  # schema_name="public"
    node = _make_node("summary", "SELECT id FROM billing.orders")
    deps = resolve_upstream_deps(node, registry)
    assert deps == []


def test_unparseable_sql_raises_invalid_compiled_sql_error():
    # A stray Jinja-tuple artifact (e.g. a trailing comma inside a {{ config(...) }}
    # call rendering as "('',)") produces text sqlglot cannot parse as SQL.
    registry = _make_registry("users")
    node = _make_node("orders", "('',)\nSELECT id FROM public.users")
    with pytest.raises(InvalidCompiledSqlError) as exc_info:
        resolve_upstream_deps(node, registry)
    assert exc_info.value.node_table_name == "orders"


def test_untokenizable_sql_raises_invalid_compiled_sql_error():
    # An unterminated string literal fails sqlglot's tokenizer (TokenError,
    # a sibling of ParseError) — it must surface as the same domain error,
    # not escape as an infrastructure exception.
    registry = _make_registry("users")
    node = _make_node("orders", "SELECT 'unterminated FROM public.users")
    with pytest.raises(InvalidCompiledSqlError) as exc_info:
        resolve_upstream_deps(node, registry)
    assert exc_info.value.node_table_name == "orders"


def test_postgres_dialect_sql_resolves():
    # Compiled SQL is PostgreSQL (the only warehouse this system targets);
    # postgres-specific operators like ARRAY[...] @> ARRAY[...] parse only
    # under the postgres dialect and must not be rejected as invalid SQL.
    registry = _make_registry("users")
    node = _make_node("orders", "SELECT id FROM public.users WHERE ARRAY[1] @> ARRAY[1]")
    deps = resolve_upstream_deps(node, registry)
    assert len(deps) == 1
    assert deps[0].table_name == "users"


def test_deps_unioned_and_deduped_across_multiple_dependency_sqls():
    registry = _make_registry("table_a", "table_b", "table_c")
    node = _make_node("orders", dependency_sqls=[
        "select a from public.table_a join public.table_b on 1=1",
        "select id from public.table_a",          # table_a repeats
        "select x from public.table_c",
    ])
    deps = resolve_upstream_deps(node, registry)
    assert sorted(d.table_name for d in deps) == ["table_a", "table_b", "table_c"]


def test_empty_dependency_sqls_yields_no_deps():
    assert resolve_upstream_deps(_make_node("orders", dependency_sqls=[]), {}) == []


def test_parse_error_in_any_query_raises_invalid_compiled_sql():
    node = _make_node("orders", dependency_sqls=["select 1", "select from from"])
    with pytest.raises(InvalidCompiledSqlError):
        resolve_upstream_deps(node, {})


def test_cte_in_one_query_does_not_exempt_the_same_name_in_another():
    # cte_names is scoped per query inside the loop — a CTE alias in one
    # dependency_sqls entry must not exempt a same-named real table reference
    # in a different entry from schema resolution.
    registry = _make_registry("users")
    node = _make_node("orders", dependency_sqls=[
        "WITH users AS (SELECT 1) SELECT * FROM users",
        "SELECT id FROM public.users",
    ])
    deps = resolve_upstream_deps(node, registry)
    assert [d.table_name for d in deps] == ["users"]
