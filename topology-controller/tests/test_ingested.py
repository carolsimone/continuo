import pytest

from config.config import _SYSTEM_RELATIONS
from domain.contract_vocabulary import NodeRuntime, NodeType
from domain.ingested import SystemRelations, ingested_nodes
from domain.model import ManifestNode, TableRef, UpstreamDep
from service.table_refs import referenced_tables

PG = _SYSTEM_RELATIONS["postgres"]
TRINO = _SYSTEM_RELATIONS["trino"]


def _model(table, schema="analytics", *, alias="", node_type=NodeType.DBT_MODEL, service="service-1", sql=""):
    """A node that builds schema.table (under alias when it has one), and when
    sql is given, one that reads what the sql names."""
    return ManifestNode(
        table_name=table, schema_name=schema, service_name=service, owner="team-a",
        schedule_name="daily", criticality="CORE", node_type=node_type,
        resolved_relation=alias, dependency_sqls=[sql] if sql else [],
    )


def _ref(key):
    schema, table = key.split(".")
    return TableRef(schema=schema, table=table)


@pytest.mark.parametrize("engine,ref,system", [
    ("postgres", TableRef("information_schema", "tables"), True),
    ("postgres", TableRef("pg_catalog", "pg_class"), True),
    ("postgres", TableRef("pg_toast", "pg_toast_1"), True),
    ("postgres", TableRef("pg_temp_3", "scratch"), True),
    ("postgres", TableRef("raw", "orders"), False),
    ("postgres", TableRef("pgx", "orders"), False),
    ("trino", TableRef("information_schema", "tables"), True),
    ("trino", TableRef("information_schema", "tables", catalog="hive"), True),
    ("trino", TableRef("runtime", "nodes", catalog="system"), True),
    ("trino", TableRef("raw", "orders", catalog="hive"), False),
    ("trino", TableRef("raw", "orders"), False),
])
def test_system_relations_per_engine(engine, ref, system):
    assert _SYSTEM_RELATIONS[engine].matches(ref) is system


def test_a_catalog_only_matches_for_an_engine_that_lists_it():
    """Postgres lists no catalog, so a catalog name never makes a relation a
    system one there."""
    assert not _SYSTEM_RELATIONS["postgres"].matches(TableRef("raw", "orders", catalog="system"))


def test_a_relation_matches_on_any_one_of_the_three_rules():
    relations = SystemRelations(
        schemas=frozenset({"meta"}), schema_prefixes=("sys_",), catalogs=frozenset({"admin"}),
    )
    assert relations.matches(TableRef("meta", "t"))
    assert relations.matches(TableRef("sys_stats", "t"))
    assert relations.matches(TableRef("anything", "t", catalog="admin"))
    assert not relations.matches(TableRef("raw", "t", catalog="warehouse"))


def test_a_table_no_node_builds_becomes_one_ingested_node_with_an_edge_per_reader():
    a, b = _model("probe_reads_source"), _model("probe_reads_raw_name")
    out = ingested_nodes(
        [a, b], {a.unique_id: [_ref("raw.ingest_probe")], b.unique_id: [_ref("raw.ingest_probe")]}, system=PG,
    )
    assert [n.unique_id for n in out] == ["raw.ingest_probe"]
    n = out[0]
    assert (
        n.node_type, n.runtime, n.service_name, n.schedule_name, n.content_hash,
        n.image_tag, n.original_file_path, n.test_count, n.upstream_deps,
    ) == (NodeType.INGESTED, NodeRuntime.EXTERNAL, "", "", "", "", "", 0, [])
    assert n.resolved_relation_id == "raw.ingest_probe"
    assert [d.unique_id for d in a.upstream_deps] == ["raw.ingest_probe"] == [d.unique_id for d in b.upstream_deps]


def test_an_ingested_node_carries_no_owner_criticality_or_sql():
    a = _model("reader")
    (n,) = ingested_nodes([a], {a.unique_id: [_ref("raw.ext")]}, system=PG)
    assert (n.owner, n.criticality, n.dependency_sqls, n.candidate_sql, n.raw_code, n.code_unit_ids) == ("", "", [], "", "", [])
    assert (n.table_name, n.schema_name) == ("ext", "raw")


def test_the_edge_an_ingested_table_gives_its_reader_names_no_service():
    a = _model("reader")
    ingested_nodes([a], {a.unique_id: [_ref("raw.ext")]}, system=PG)
    (dep,) = a.upstream_deps
    assert (dep.schema_name, dep.table_name, dep.service_name) == ("raw", "ext", "")


def test_one_node_for_every_spelling_of_a_table():
    """referenced_tables lower-cases what it reads, and a catalog prefix does
    not make a second table."""
    a = _model("reader_a", sql='SELECT * FROM "continuo_dbt"."RAW"."Ingest_Probe"')
    b = _model("reader_b", sql="SELECT * FROM raw.ingest_probe")
    c = _model("reader_c", sql='SELECT * FROM "other_db".raw."INGEST_PROBE"')
    reads = {n.unique_id: referenced_tables(n, dialect="postgres") for n in (a, b, c)}
    out = ingested_nodes([a, b, c], reads, system=PG)
    assert [(n.unique_id, n.schema_name, n.table_name) for n in out] == [("raw.ingest_probe", "raw", "ingest_probe")]
    for reader in (a, b, c):
        assert [d.unique_id for d in reader.upstream_deps] == ["raw.ingest_probe"]


def test_a_reader_gets_one_edge_for_two_reads_of_the_same_table():
    a = _model("reader")
    out = ingested_nodes([a], {a.unique_id: [_ref("raw.ext"), TableRef("raw", "ext", catalog="other")]}, system=PG)
    assert [n.unique_id for n in out] == ["raw.ext"]
    assert [d.unique_id for d in a.upstream_deps] == ["raw.ext"]


def test_a_system_read_before_or_after_a_real_one_gives_the_same_node_and_edge():
    """Trino's system relations are a catalog, and system.runtime.nodes and
    warehouse.runtime.nodes share a schema and a table: the real table is
    ingested whichever of the two reads comes first."""
    system_ref, real_ref = TableRef("runtime", "nodes", catalog="system"), TableRef("runtime", "nodes", catalog="warehouse")
    results = []
    for reads in ([system_ref, real_ref], [real_ref, system_ref]):
        reader = _model("reader")
        out = ingested_nodes([reader], {reader.unique_id: reads}, system=TRINO)
        results.append(([n.unique_id for n in out], [d.unique_id for d in reader.upstream_deps]))
    assert results == [(["runtime.nodes"], ["runtime.nodes"])] * 2


def test_a_reader_of_only_a_system_catalog_table_gets_no_node_and_no_edge():
    reader = _model("reader")
    reads = {reader.unique_id: [TableRef("runtime", "nodes", catalog="system")]}
    assert ingested_nodes([reader], reads, system=TRINO) == []
    assert reader.upstream_deps == []


def test_one_table_read_under_two_non_system_catalogs_is_one_node_and_one_edge():
    reader = _model("reader")
    reads = {reader.unique_id: [TableRef("raw", "orders", catalog="hive"), TableRef("raw", "orders", catalog="other")]}
    out = ingested_nodes([reader], reads, system=TRINO)
    assert [n.unique_id for n in out] == ["raw.orders"]
    assert [d.unique_id for d in reader.upstream_deps] == ["raw.orders"]


def test_the_real_table_is_ingested_from_the_sql_in_either_order_of_the_two_reads():
    results = []
    for sql in (
        "SELECT * FROM system.runtime.nodes JOIN warehouse.runtime.nodes ON 1=1",
        "SELECT * FROM warehouse.runtime.nodes JOIN system.runtime.nodes ON 1=1",
    ):
        reader = _model("reader", sql=sql)
        out = ingested_nodes([reader], {reader.unique_id: referenced_tables(reader, dialect="trino")}, system=TRINO)
        results.append(([n.unique_id for n in out], [d.unique_id for d in reader.upstream_deps]))
    assert results == [(["runtime.nodes"], ["runtime.nodes"])] * 2


def test_a_reader_keeps_the_edges_it_already_has():
    reader = _model("reader")
    reader.upstream_deps = [UpstreamDep(table_name="orders", schema_name="analytics", service_name="service-1")]
    ingested_nodes([reader], {reader.unique_id: [_ref("raw.ext")]}, system=PG)
    assert [d.unique_id for d in reader.upstream_deps] == ["analytics.orders", "raw.ext"]


def test_a_table_a_node_builds_is_not_ingested():
    reader, builder = _model("reader"), _model("x", schema="raw")
    assert ingested_nodes([reader, builder], {reader.unique_id: [_ref("raw.x")]}, system=PG) == []
    assert reader.upstream_deps == []


def test_a_table_is_built_under_the_name_its_builder_declares_in_any_case():
    reader, builder = _model("reader"), _model("Orders", schema="RAW")
    assert ingested_nodes([reader, builder], {reader.unique_id: [_ref("raw.orders")]}, system=PG) == []


def test_a_table_written_under_an_alias_is_not_ingested():
    reader, builder = _model("reader"), _model("orders_v2", alias="orders")
    assert ingested_nodes([reader, builder], {reader.unique_id: [_ref("analytics.orders")]}, system=PG) == []
    assert reader.upstream_deps == []


def test_a_table_read_under_the_declared_name_of_an_aliased_node_is_not_ingested():
    reader, builder = _model("reader"), _model("orders_v2", alias="orders")
    assert ingested_nodes([reader, builder], {reader.unique_id: [_ref("analytics.orders_v2")]}, system=PG) == []


def test_a_test_does_not_build_the_table_it_is_named_after():
    """A dbt test writes no relation, so a table that only a test's name
    matches is still read from outside."""
    reader = _model("reader")
    test = _model("probe", schema="raw", node_type=NodeType.DBT_TEST)
    out = ingested_nodes([reader, test], {reader.unique_id: [_ref("raw.probe")]}, system=PG)
    assert [n.unique_id for n in out] == ["raw.probe"]


def test_system_tables_never_make_a_node():
    reader = _model("reader")
    reads = {reader.unique_id: [_ref("pg_catalog.pg_class"), _ref("information_schema.tables")]}
    assert ingested_nodes([reader], reads, system=PG) == []
    assert reader.upstream_deps == []


def test_a_system_table_beside_an_outside_table_leaves_only_the_outside_one():
    reader = _model("reader")
    reads = {reader.unique_id: [_ref("pg_catalog.pg_class"), _ref("raw.ext")]}
    assert [n.unique_id for n in ingested_nodes([reader], reads, system=PG)] == ["raw.ext"]
    assert [d.unique_id for d in reader.upstream_deps] == ["raw.ext"]


def test_a_test_reading_an_outside_table_makes_nothing():
    """The handler records no reads for a dbt test, so a test beside a model
    that reads nothing outside yields no node and keeps its upstreams."""
    model = _model("orders")
    test = _model("not_null_orders_id", schema="analytics_audit", node_type=NodeType.DBT_TEST)
    assert ingested_nodes([model, test], {model.unique_id: []}, system=PG) == []
    assert test.upstream_deps == []


def test_a_test_with_reads_still_gets_no_edge_and_makes_no_node():
    """A test that reaches the function with reads is ignored all the same: the
    function never gives a test an edge."""
    test = _model("probe", schema="analytics_audit", node_type=NodeType.DBT_TEST)
    assert ingested_nodes([test], {test.unique_id: [_ref("raw.ext")]}, system=PG) == []
    assert test.upstream_deps == []


def test_two_services_reading_one_table_share_one_node():
    a, b = _model("reader_a", service="service-1"), _model("reader_b", service="service-2")
    out = ingested_nodes([a, b], {a.unique_id: [_ref("raw.ext")], b.unique_id: [_ref("raw.ext")]}, system=PG)
    assert [n.unique_id for n in out] == ["raw.ext"]
    assert [d.unique_id for d in a.upstream_deps] == ["raw.ext"] == [d.unique_id for d in b.upstream_deps]


def test_nodes_come_out_sorted_by_unique_id():
    a = _model("reader")
    out = ingested_nodes([a], {a.unique_id: [_ref("raw.z"), _ref("raw.a"), _ref("raw.m")]}, system=PG)
    assert [n.unique_id for n in out] == ["raw.a", "raw.m", "raw.z"]


def test_the_output_does_not_depend_on_the_order_of_the_readers():
    a, b = _model("reader_a"), _model("reader_b")
    reads = {a.unique_id: [_ref("raw.z")], b.unique_id: [_ref("raw.a")]}
    forward = [n.unique_id for n in ingested_nodes([a, b], reads, system=PG)]
    c, d = _model("reader_a"), _model("reader_b")
    backward = [n.unique_id for n in ingested_nodes([d, c], reads, system=PG)]
    assert forward == backward == ["raw.a", "raw.z"]


def test_a_python_csv_node_makes_nothing():
    """A python-csv node has no SQL, so the handler records no table for it."""
    csv = _model("prices", schema="raw", node_type=NodeType.PYTHON_CSV)
    csv.csv_source = "s3://bucket/prices.csv"
    reads = {csv.unique_id: referenced_tables(csv, dialect="postgres")}
    assert reads == {csv.unique_id: []}
    assert ingested_nodes([csv], reads, system=PG) == []
    assert csv.upstream_deps == []


def test_a_python_node_reading_an_outside_table_behaves_like_a_dbt_model():
    py = _model("scores", node_type=NodeType.PYTHON_NODE, sql="SELECT id FROM raw.ext")
    reads = {py.unique_id: referenced_tables(py, dialect="postgres")}
    out = ingested_nodes([py], reads, system=PG)
    assert [n.unique_id for n in out] == ["raw.ext"]
    assert [d.unique_id for d in py.upstream_deps] == ["raw.ext"]


def test_a_node_with_no_reads_entry_gets_no_edge():
    a, b = _model("reader"), _model("silent")
    ingested_nodes([a, b], {a.unique_id: [_ref("raw.ext")]}, system=PG)
    assert b.upstream_deps == []


def test_no_outside_table_means_no_node_and_no_change():
    a = _model("reader")
    assert ingested_nodes([a], {a.unique_id: []}, system=PG) == []
    assert ingested_nodes([], {}, system=PG) == []
    assert a.upstream_deps == []
