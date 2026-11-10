import pytest
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from domain.model import ManifestNode
from service.table_refs import referenced_tables


def _node(*sqls: str, schema: str = "analytics", table: str = "m") -> ManifestNode:
    return ManifestNode(
        table_name=table,
        schema_name=schema,
        service_name="service-1",
        owner="data-platform",
        schedule_name="daily",
        criticality="SECONDARY",
        dependency_sqls=list(sqls),
    )


def test_every_named_table_lower_cased_in_first_seen_order():
    refs = referenced_tables(_node('SELECT * FROM "continuo_dbt"."RAW"."Ingest_Probe" JOIN raw.b ON 1=1'), dialect="postgres")
    assert [(r.catalog, r.schema, r.table) for r in refs] == [("continuo_dbt", "raw", "ingest_probe"), ("", "raw", "b")]


def test_a_table_named_twice_across_sqls_is_returned_once():
    refs = referenced_tables(_node("SELECT * FROM raw.a", "SELECT * FROM RAW.A"), dialect="postgres")
    assert [r.key for r in refs] == ["raw.a"]


def test_ctes_and_qualified_self_reads_are_skipped():
    sql = "WITH x AS (SELECT * FROM raw.a) SELECT * FROM x JOIN analytics.m ON 1=1"
    assert [r.key for r in referenced_tables(_node(sql), dialect="postgres")] == ["raw.a"]


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
