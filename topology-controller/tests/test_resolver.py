import pytest
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from domain.model import ManifestNode, NodeRegistry, NodeRegistryEntry, TableRef
from service.resolver import resolve_upstream_deps
from service.table_refs import referenced_tables


def _make_registry(*table_names: str) -> dict:
    entries = [
        NodeRegistryEntry(
            table_name=t,
            schema_name="public",
            service_name="service-1",
            owner="data-platform",
            relation=t,
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


def _resolve(node: ManifestNode, registry: dict, dialect: str = "postgres") -> list:
    """Resolve against the postgres dialect unless a test names another.

    referenced_tables takes dialect as a required keyword so production code
    cannot fall back to one engine's rules; these cases are about resolution
    behaviour, not dialect selection, so they pin the default engine here.
    """
    return resolve_upstream_deps(referenced_tables(node, dialect=dialect), registry)


def test_resolves_single_upstream():
    registry = _make_registry("users")
    node = _make_node("orders", "SELECT id FROM public.users")
    deps = _resolve(node, registry)
    assert len(deps) == 1
    assert deps[0].table_name == "users"


def test_skips_self_reference():
    registry = _make_registry("orders")
    node = _make_node("orders", "SELECT id FROM public.orders")
    deps = _resolve(node, registry)
    assert deps == []


def test_skips_cte_names():
    registry = _make_registry("raw_orders")
    sql = """
        WITH cte AS (SELECT id FROM public.raw_orders)
        SELECT id FROM cte
    """
    node = _make_node("orders", sql)
    deps = _resolve(node, registry)
    assert len(deps) == 1
    assert deps[0].table_name == "raw_orders"


def test_deduplicates_upstreams():
    registry = _make_registry("users")
    sql = "SELECT a.id, b.id FROM public.users a JOIN public.users b ON a.id = b.id"
    node = _make_node("orders", sql)
    deps = _resolve(node, registry)
    assert len(deps) == 1


def test_one_registered_table_read_under_two_catalogs_gives_one_dependency():
    # The registry knows a table by (schema, table) and ignores the catalog, so
    # refs that differ only in catalog are one dependency.
    registry = _make_registry("orders")
    node = _make_node("summary", "SELECT * FROM hive.public.orders a JOIN other.public.orders b ON 1=1")
    refs = referenced_tables(node, dialect="trino")
    assert [(r.catalog, r.schema, r.table) for r in refs] == [("hive", "public", "orders"), ("other", "public", "orders")]
    deps = resolve_upstream_deps(refs, registry)
    assert [d.unique_id for d in deps] == ["public.orders"]


def test_dependencies_come_in_the_order_of_each_tables_first_ref():
    registry = _make_registry("a", "b")
    refs = [
        TableRef(schema="public", table="b", catalog="hive"),
        TableRef(schema="public", table="a", catalog="hive"),
        TableRef(schema="public", table="b", catalog="other"),
        TableRef(schema="public", table="a", catalog="other"),
    ]
    assert [d.unique_id for d in resolve_upstream_deps(refs, registry)] == ["public.b", "public.a"]


def test_refs_of_two_registered_tables_give_two_dependencies_in_ref_order():
    registry = _make_registry("users", "products")
    refs = [
        TableRef(schema="public", table="products", catalog="hive"),
        TableRef(schema="public", table="users", catalog="hive"),
    ]
    assert [d.unique_id for d in resolve_upstream_deps(refs, registry)] == ["public.products", "public.users"]


def test_skips_unknown_table():
    # Tables not in the registry (seeds, sources, external refs) are silently
    # skipped — they are not cross-service model dependencies.
    registry = _make_registry("users")
    node = _make_node("orders", "SELECT id FROM public.unknown_table")
    deps = _resolve(node, registry)
    assert deps == []


def test_empty_dep_sql_entry_is_skipped_but_real_query_still_resolves():
    # A blank dependency_sqls entry (e.g. a seed slot in a mixed list) must be
    # skipped by the `if not dep_sql: continue` guard without short-circuiting
    # resolution of the other, real queries in the same list.
    registry = _make_registry("users")
    node = _make_node("orders", dependency_sqls=["", "SELECT id FROM public.users"])
    deps = _resolve(node, registry)
    assert len(deps) == 1
    assert deps[0].table_name == "users"


def test_multiple_upstreams():
    registry = _make_registry("users", "products")
    sql = "SELECT u.id, p.id FROM public.users u JOIN public.products p ON u.id = p.user_id"
    node = _make_node("orders", sql)
    deps = _resolve(node, registry)
    assert len(deps) == 2
    names = {d.table_name for d in deps}
    assert names == {"users", "products"}


def test_unqualified_table_raises():
    registry = _make_registry("users")
    node = _make_node("orders", "SELECT id FROM users")
    with pytest.raises(UnqualifiedTableReferenceError) as exc_info:
        _resolve(node, registry)
    assert exc_info.value.table_name == "users"
    assert exc_info.value.node_table_name == "orders"


def test_resolves_same_table_name_in_different_schemas():
    # Core regression test: same table_name in two schemas must resolve
    # to the correct service — the bug this change fixes.
    lookup = NodeRegistry(entries=[
        NodeRegistryEntry("orders", "public", "service-a", "team-a", relation="orders"),
        NodeRegistryEntry("orders", "billing", "service-b", "team-b", relation="orders"),
    ]).to_lookup()
    node = _make_node("summary", "SELECT id FROM billing.orders")
    deps = _resolve(node, lookup)
    assert len(deps) == 1
    assert deps[0].service_name == "service-b"


def test_schema_mismatch_returns_no_dep():
    # If SQL references billing.orders but registry only has public.orders,
    # no dep should be resolved — schema mismatch means different tables.
    registry = _make_registry("orders")  # schema_name="public"
    node = _make_node("summary", "SELECT id FROM billing.orders")
    deps = _resolve(node, registry)
    assert deps == []


def test_unparseable_sql_raises_invalid_compiled_sql_error():
    # A stray Jinja-tuple artifact (e.g. a trailing comma inside a {{ config(...) }}
    # call rendering as "('',)") produces text sqlglot cannot parse as SQL.
    registry = _make_registry("users")
    node = _make_node("orders", "('',)\nSELECT id FROM public.users")
    with pytest.raises(InvalidCompiledSqlError) as exc_info:
        _resolve(node, registry)
    assert exc_info.value.node_table_name == "orders"


def test_untokenizable_sql_raises_invalid_compiled_sql_error():
    # An unterminated string literal fails sqlglot's tokenizer (TokenError,
    # a sibling of ParseError) — it must surface as the same domain error,
    # not escape as an infrastructure exception.
    registry = _make_registry("users")
    node = _make_node("orders", "SELECT 'unterminated FROM public.users")
    with pytest.raises(InvalidCompiledSqlError) as exc_info:
        _resolve(node, registry)
    assert exc_info.value.node_table_name == "orders"


def test_postgres_dialect_sql_resolves():
    # Postgres-specific operators like ARRAY[...] @> ARRAY[...] parse only under
    # the postgres dialect and must not be rejected as invalid SQL on a postgres
    # install.
    registry = _make_registry("users")
    node = _make_node("orders", "SELECT id FROM public.users WHERE ARRAY[1] @> ARRAY[1]")
    deps = _resolve(node, registry)
    assert len(deps) == 1
    assert deps[0].table_name == "users"


def test_dialect_argument_selects_the_parser():
    # The dialect reaches sqlglot rather than being ignored: the postgres-only
    # @> operator parses under postgres and fails under trino, so the same SQL
    # resolves one way and raises the other.
    registry = _make_registry("users")
    node = _make_node("orders", "SELECT id FROM public.users WHERE ARRAY[1] @> ARRAY[1]")

    assert [d.table_name for d in _resolve(node, registry, dialect="postgres")] == ["users"]

    with pytest.raises(InvalidCompiledSqlError):
        _resolve(node, registry, dialect="trino")


def test_dialect_is_required():
    # Production callers must name the engine; there is no implicit fallback to
    # postgres that would silently read another engine's SQL under the wrong
    # rules.
    with pytest.raises(TypeError):
        referenced_tables(_make_node("orders", "SELECT 1"))


def test_deps_unioned_and_deduped_across_multiple_dependency_sqls():
    registry = _make_registry("table_a", "table_b", "table_c")
    node = _make_node("orders", dependency_sqls=[
        "select a from public.table_a join public.table_b on 1=1",
        "select id from public.table_a",          # table_a repeats
        "select x from public.table_c",
    ])
    deps = _resolve(node, registry)
    assert sorted(d.table_name for d in deps) == ["table_a", "table_b", "table_c"]


def test_empty_dependency_sqls_yields_no_deps():
    assert _resolve(_make_node("orders", dependency_sqls=[]), {}) == []


def test_parse_error_in_any_query_raises_invalid_compiled_sql():
    node = _make_node("orders", dependency_sqls=["select 1", "select from from"])
    with pytest.raises(InvalidCompiledSqlError):
        _resolve(node, {})


def test_cte_in_one_query_does_not_exempt_the_same_name_in_another():
    # cte_names is scoped per query inside the loop — a CTE alias in one
    # dependency_sqls entry must not exempt a same-named real table reference
    # in a different entry from schema resolution.
    registry = _make_registry("users")
    node = _make_node("orders", dependency_sqls=[
        "WITH users AS (SELECT 1) SELECT * FROM users",
        "SELECT id FROM public.users",
    ])
    deps = _resolve(node, registry)
    assert [d.table_name for d in deps] == ["users"]


def test_deps_follow_ref_order_and_skip_refs_the_registry_does_not_hold():
    registry = _make_registry("table_a", "table_b")
    refs = [
        TableRef(schema="public", table="table_b"),
        TableRef(schema="raw", table="external_source"),
        TableRef(schema="public", table="table_a"),
    ]
    assert [d.table_name for d in resolve_upstream_deps(refs, registry)] == ["table_b", "table_a"]


def test_a_qualified_table_is_resolved_when_a_cte_has_its_name():
    # A CTE is always unqualified, so public.users is the table even though a
    # CTE of the same name is in scope. Skipping it by name drops the upstream
    # dependency and the release promotes without the edge.
    registry = _make_registry("users")
    sql = """
        WITH users AS (SELECT 1 AS id)
        SELECT u.id FROM users u JOIN public.users p ON p.id = u.id
    """
    deps = _resolve(_make_node("orders", sql), registry)
    assert [d.table_name for d in deps] == ["users"]


def test_an_unqualified_reference_to_a_cte_is_still_skipped():
    registry = _make_registry("users")
    sql = "WITH users AS (SELECT 1 AS id) SELECT id FROM users"
    assert _resolve(_make_node("orders", sql), registry) == []


def _entry(table_name: str, *, relation: str = "", service: str = "service-1", ephemeral: bool = False):
    return NodeRegistryEntry(table_name, "public", service, "team", relation=relation or table_name, ephemeral=ephemeral)


def test_a_reference_to_an_alias_resolves_to_the_node_that_writes_it():
    lookup = NodeRegistry(entries=[_entry("orders", relation="Orders_Tbl")]).to_lookup()
    deps = _resolve(_make_node("summary", 'SELECT id FROM public."Orders_Tbl"'), lookup)
    assert [d.unique_id for d in deps] == ["public.orders"]


def test_a_self_reference_by_alias_is_skipped():
    lookup = NodeRegistry(entries=[_entry("orders", relation="orders_tbl")]).to_lookup()
    node = _make_node("orders", "SELECT id FROM public.orders_tbl")
    node.resolved_relation = "orders_tbl"
    assert _resolve(node, lookup) == []


def _resolve_with_ephemerals(node: ManifestNode, registry: NodeRegistry) -> list:
    ctes: set[str] = set()
    refs = referenced_tables(node, dialect="postgres", ctes=ctes)
    return resolve_upstream_deps(
        refs, registry.to_lookup(),
        service_name=node.service_name, ctes=ctes, ephemeral_ctes=registry.ephemeral_ctes(),
    )


def test_an_ephemeral_cte_is_an_upstream_and_the_tables_it_reads_stay_upstreams():
    registry = NodeRegistry(entries=[_entry("eph", ephemeral=True), _entry("raw_orders")])
    sql = """
        WITH __dbt__cte__eph AS (SELECT id FROM public.raw_orders)
        SELECT id FROM __dbt__cte__eph
    """
    deps = _resolve_with_ephemerals(_make_node("orders", sql), registry)
    assert {d.unique_id for d in deps} == {"public.eph", "public.raw_orders"}


def test_an_ephemeral_cte_of_another_service_is_not_an_upstream():
    registry = NodeRegistry(entries=[_entry("eph", service="service-2", ephemeral=True)])
    sql = "WITH __dbt__cte__eph AS (SELECT 1 AS id) SELECT id FROM __dbt__cte__eph"
    deps = _resolve_with_ephemerals(_make_node("orders", sql), registry)
    assert deps == []


def test_a_cte_named_like_an_ephemeral_is_no_upstream_without_ephemeral_ctes():
    lookup = NodeRegistry(entries=[_entry("eph", ephemeral=True)]).to_lookup()
    sql = "WITH __dbt__cte__eph AS (SELECT 1 AS id) SELECT id FROM __dbt__cte__eph"
    assert _resolve(_make_node("orders", sql), lookup) == []
