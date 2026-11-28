import pytest
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from domain.model import ManifestNode
from service.table_refs import referenced_tables


def _node(*sqls: str, schema: str = "analytics", table: str = "m") -> ManifestNode:
    return ManifestNode(
        table_name=table,
        schema_name=schema,
        project_name="service-1",
        owner="data-platform",
        schedule_name="daily",
        criticality="SECONDARY",
        dependency_sqls=list(sqls),
    )


def test_every_named_table_lower_cased_in_first_seen_order():
    refs = referenced_tables(_node('SELECT * FROM "continuo_dbt"."RAW"."External_Probe" JOIN raw.b ON 1=1'), dialect="postgres")
    assert [(r.catalog, r.schema, r.table) for r in refs] == [("continuo_dbt", "raw", "external_probe"), ("", "raw", "b")]


def test_a_table_named_twice_across_sqls_is_returned_once():
    refs = referenced_tables(_node("SELECT * FROM raw.a", "SELECT * FROM RAW.A"), dialect="postgres")
    assert [r.key for r in refs] == ["raw.a"]


def _read_tuples(sql: str, dialect: str = "trino") -> list[tuple[str, str, str]]:
    return [(r.catalog, r.schema, r.table) for r in referenced_tables(_node(sql), dialect=dialect)]


def test_the_same_schema_and_table_under_two_catalogs_returns_both_in_first_seen_order():
    """The system-relation filter that follows decides per catalog, so a read of
    system.runtime.nodes must not hide a read of warehouse.runtime.nodes."""
    assert _read_tuples("SELECT * FROM system.runtime.nodes JOIN warehouse.runtime.nodes ON 1=1") == [
        ("system", "runtime", "nodes"), ("warehouse", "runtime", "nodes"),
    ]


def test_the_catalogs_come_back_in_the_order_the_sql_reads_them():
    assert _read_tuples("SELECT * FROM warehouse.runtime.nodes JOIN system.runtime.nodes ON 1=1") == [
        ("warehouse", "runtime", "nodes"), ("system", "runtime", "nodes"),
    ]


def test_two_reads_of_one_catalog_schema_and_table_in_any_case_return_one_ref():
    assert _read_tuples("SELECT * FROM Warehouse.Raw.Orders JOIN warehouse.raw.orders ON 1=1") == [
        ("warehouse", "raw", "orders"),
    ]
    refs = referenced_tables(
        _node("SELECT * FROM warehouse.raw.orders", 'SELECT * FROM "WAREHOUSE"."RAW"."ORDERS"'), dialect="trino",
    )
    assert [(r.catalog, r.schema, r.table) for r in refs] == [("warehouse", "raw", "orders")]


def test_a_read_with_a_catalog_and_the_same_read_without_one_are_two_refs():
    assert _read_tuples("SELECT * FROM raw.orders JOIN warehouse.raw.orders ON 1=1", "postgres") == [
        ("", "raw", "orders"), ("warehouse", "raw", "orders"),
    ]


def test_ctes_and_qualified_self_reads_are_skipped():
    sql = "WITH x AS (SELECT * FROM raw.a) SELECT * FROM x JOIN analytics.m ON 1=1"
    assert [r.key for r in referenced_tables(_node(sql), dialect="postgres")] == ["raw.a"]


def test_a_qualified_table_is_returned_when_a_cte_has_its_name():
    """A CTE is always unqualified, so public.users is a table even though a CTE
    named users is in scope; the unqualified users beside it is the CTE."""
    sql = "WITH users AS (SELECT 1 AS id) SELECT * FROM public.users JOIN users ON 1=1"
    assert [r.key for r in referenced_tables(_node(sql), dialect="postgres")] == ["public.users"]


def test_a_qualified_table_that_shares_a_ctes_name_is_returned_whichever_is_read_first():
    sql = "WITH users AS (SELECT 1 AS id) SELECT * FROM users JOIN public.users ON 1=1"
    assert [r.key for r in referenced_tables(_node(sql), dialect="postgres")] == ["public.users"]


def test_an_unqualified_reference_to_a_cte_is_skipped_and_does_not_raise():
    sql = "WITH users AS (SELECT 1 AS id) SELECT * FROM users"
    assert referenced_tables(_node(sql), dialect="postgres") == []


def test_an_unqualified_table_that_is_not_a_cte_raises_beside_a_cte_of_another_name():
    with pytest.raises(UnqualifiedTableReferenceError) as exc_info:
        referenced_tables(_node("WITH users AS (SELECT 1) SELECT * FROM users JOIN orders ON 1=1"), dialect="postgres")
    assert exc_info.value.table_name == "orders"


def test_a_cte_in_one_sql_does_not_hide_the_same_name_unqualified_in_another():
    with pytest.raises(UnqualifiedTableReferenceError) as exc_info:
        referenced_tables(_node("WITH users AS (SELECT 1) SELECT * FROM users", "SELECT * FROM users"), dialect="postgres")
    assert exc_info.value.table_name == "users"


def test_a_nameless_table_with_no_schema_still_raises_unqualified():
    with pytest.raises(UnqualifiedTableReferenceError):
        referenced_tables(_node("SELECT * FROM generate_series(1, 3)"), dialect="postgres")


def test_unnest_and_values_name_no_table():
    assert referenced_tables(_node("SELECT * FROM (VALUES (1)) AS v(x)"), dialect="postgres") == []
    assert referenced_tables(_node("SELECT * FROM unnest(ARRAY[1,2]) AS u(x)"), dialect="postgres") == []


def test_a_schema_qualified_function_call_names_no_table():
    """sqlglot models public.my_fn(1) as a table with a schema and no name, which
    reads no relation."""
    assert referenced_tables(_node("SELECT * FROM public.my_fn(1)"), dialect="postgres") == []
    assert referenced_tables(_node("SELECT * FROM pg_catalog.generate_series(1, 3)"), dialect="postgres") == []


def test_invalid_sql_raises_invalid_compiled_sql():
    with pytest.raises(InvalidCompiledSqlError):
        referenced_tables(_node("SELECT FROM WHERE ("), dialect="postgres")


# A name with no schema is a CTE reference only where a CTE of that name is in
# scope, the rule continuo-python-runtime's validate applies to the same reads.
ENGINE_DIALECTS = ["postgres", "trino"]


def _unqualified_table(sql: str, dialect: str) -> str:
    with pytest.raises(UnqualifiedTableReferenceError) as exc_info:
        referenced_tables(_node(sql), dialect=dialect)
    return exc_info.value.table_name


@pytest.mark.parametrize("dialect", ENGINE_DIALECTS)
def test_a_cte_does_not_hide_a_table_inside_its_own_body(dialect):
    sql = "WITH orders AS (SELECT * FROM orders) SELECT * FROM orders"
    assert _unqualified_table(sql, dialect) == "orders"


@pytest.mark.parametrize("dialect", ENGINE_DIALECTS)
def test_a_cte_is_not_visible_before_it_is_defined(dialect):
    sql = "WITH a AS (SELECT * FROM b), b AS (SELECT 1 AS x) SELECT * FROM a"
    assert _unqualified_table(sql, dialect) == "b"


@pytest.mark.parametrize("dialect", ENGINE_DIALECTS)
def test_a_cte_defined_in_a_subquery_is_not_visible_outside_it(dialect):
    sql = "SELECT * FROM (WITH users AS (SELECT 1 AS id) SELECT * FROM users) s JOIN users ON 1 = 1"
    assert _unqualified_table(sql, dialect) == "users"


_A_CTE_READS_ITSELF = "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT * FROM t"


@pytest.mark.parametrize(
    ("sql", "dialect"),
    [
        pytest.param(_A_CTE_READS_ITSELF, "postgres", id="a-cte-reads-itself-postgres"),
        pytest.param(_A_CTE_READS_ITSELF, "trino", id="a-cte-reads-itself-trino"),
        pytest.param(
            "WITH RECURSIVE a AS (SELECT * FROM b), b AS (SELECT 1 AS x) SELECT * FROM a",
            "postgres",
            id="a-cte-reads-one-defined-after-it-postgres",
        ),
    ],
)
def test_a_recursive_with_sees_all_its_ctes(sql, dialect):
    assert referenced_tables(_node(sql), dialect=dialect) == []


@pytest.mark.parametrize("dialect", ENGINE_DIALECTS)
def test_a_recursive_with_still_refuses_an_unqualified_real_table(dialect):
    sql = "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t JOIN orders ON 1 = 1) SELECT * FROM t"
    assert _unqualified_table(sql, dialect) == "orders"


@pytest.mark.parametrize("dialect", ENGINE_DIALECTS)
@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a) SELECT * FROM b", id="an-earlier-cte"),
        pytest.param(
            "WITH users AS (SELECT 1 AS id) SELECT * FROM users WHERE id IN (SELECT id FROM users)",
            id="a-correlated-subquery",
        ),
    ],
)
def test_earlier_ctes_and_correlated_subqueries_see_the_cte(sql, dialect):
    assert referenced_tables(_node(sql), dialect=dialect) == []


@pytest.mark.parametrize("dialect", ENGINE_DIALECTS)
def test_ctes_still_lists_every_cte_name(dialect):
    """The ephemeral resolver reads every CTE name, in scope or not, the CTE that
    shadows a table of its own name included."""
    names: set[str] = set()
    referenced_tables(
        _node(
            "WITH Orders AS (SELECT * FROM raw.orders) SELECT * FROM orders",
            "SELECT * FROM (WITH inner_cte AS (SELECT 1 AS id) SELECT * FROM inner_cte) s JOIN raw.b ON 1 = 1",
        ),
        dialect=dialect,
        ctes=names,
    )
    assert names == {"orders", "inner_cte"}
    shadowing: set[str] = set()
    with pytest.raises(UnqualifiedTableReferenceError):
        referenced_tables(
            _node("WITH orders AS (SELECT * FROM orders) SELECT * FROM orders"), dialect=dialect, ctes=shadowing,
        )
    assert shadowing == {"orders"}


# Shapes where sqlglot decides which node carries a WITH clause: a set
# operation in a CTE body, a WITH nested in a CTE body, a subquery in a CTE
# body and a CTE local to one subquery. Each pins what the scope rule sees there.
SCOPE_SHAPES_THAT_READ_ONLY_CTES = [
    pytest.param(
        "WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a UNION ALL SELECT * FROM a) SELECT * FROM b",
        [],
        id="a-union-in-a-cte-body-sees-an-earlier-cte",
    ),
    pytest.param(
        "WITH a AS (WITH inner_cte AS (SELECT 1 AS x) SELECT * FROM inner_cte) SELECT * FROM a",
        [],
        id="a-with-nested-in-a-cte-body-is-seen-inside-that-body",
    ),
    pytest.param(
        "WITH a AS (SELECT 1 AS x), b AS (SELECT (SELECT max(x) FROM a) AS m) SELECT * FROM b",
        [],
        id="a-scalar-subquery-in-a-cte-body-sees-an-earlier-cte",
    ),
    pytest.param(
        "WITH a AS (SELECT 1 AS x), b AS (SELECT 1 AS y WHERE EXISTS (SELECT 1 FROM a)) SELECT * FROM b",
        [],
        id="an-exists-subquery-in-a-cte-body-sees-an-earlier-cte",
    ),
    pytest.param(
        "WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM raw.t CROSS JOIN LATERAL (SELECT * FROM a) l) SELECT * FROM b",
        ["raw.t"],
        id="a-lateral-subquery-in-a-cte-body-sees-an-earlier-cte",
    ),
]


@pytest.mark.parametrize("dialect", ENGINE_DIALECTS)
@pytest.mark.parametrize(("sql", "keys"), SCOPE_SHAPES_THAT_READ_ONLY_CTES)
def test_a_cte_in_scope_is_seen_whatever_node_carries_the_with(sql, keys, dialect):
    assert [r.key for r in referenced_tables(_node(sql), dialect=dialect)] == keys


SCOPE_SHAPES_THAT_READ_A_TABLE = [
    pytest.param(
        "WITH b AS (SELECT 1 AS x UNION ALL SELECT * FROM b) SELECT * FROM b",
        "b",
        id="a-union-in-a-cte-body-does-not-see-its-own-cte",
    ),
    pytest.param(
        "WITH a AS (SELECT 1 AS x UNION ALL SELECT * FROM c), c AS (SELECT 1 AS x) SELECT * FROM a",
        "c",
        id="a-union-in-a-cte-body-does-not-see-a-later-cte",
    ),
    pytest.param(
        "WITH a AS (WITH inner_cte AS (SELECT 1 AS x) SELECT * FROM inner_cte), "
        "b AS (SELECT * FROM inner_cte) SELECT * FROM b",
        "inner_cte",
        id="a-with-nested-in-a-cte-body-is-not-seen-by-a-later-sibling-cte",
    ),
    pytest.param(
        "SELECT * FROM (WITH u AS (SELECT 1 AS id) SELECT * FROM u) s1 JOIN (SELECT * FROM u) s2 ON 1 = 1",
        "u",
        id="a-subquery-local-cte-is-not-seen-by-a-sibling-subquery",
    ),
]


@pytest.mark.parametrize("dialect", ENGINE_DIALECTS)
@pytest.mark.parametrize(("sql", "table_name"), SCOPE_SHAPES_THAT_READ_A_TABLE)
def test_a_cte_out_of_scope_is_not_seen_whatever_node_carries_the_with(sql, table_name, dialect):
    assert _unqualified_table(sql, dialect) == table_name
