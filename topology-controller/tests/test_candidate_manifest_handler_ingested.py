"""The ingested nodes the candidate handler adds to a release's topology: one
for every table some node reads and no node builds."""
import json
from unittest.mock import MagicMock

from config.config import _SYSTEM_RELATIONS
from domain.model import ManifestFile, ManifestKind
from service import candidate_manifest_handler
from service.topology_artifact import canonical_topology_json
from service.release_timings import Stage
from tests.fakes import (
    FakeBundleUploader,
    FakeClock,
    RecordingArtifactWriter,
    build_handler,
    dbt_node,
    make_uploader,
    python_contract,
    python_entry,
    python_handler,
    source_of,
    source_with_nodes,
)

QUOTED_SOURCE_READ = 'SELECT * FROM "continuo_dbt"."raw"."ingest_probe"'
RAW_NAME_READ = "SELECT * FROM raw.ingest_probe"


def _probe_release(tmp_path, *, reverse=False):
    """Two models reading raw.ingest_probe, one by the source's quoted three-part
    name and one by its bare two-part name."""
    a = dbt_node("probe_reads_source") | {"compiled_code": QUOTED_SOURCE_READ}
    b = dbt_node("probe_reads_raw_name") | {"compiled_code": RAW_NAME_READ}
    return source_with_nodes(tmp_path, *((b, a) if reverse else (a, b)))


def _topology(source, **handler_options):
    """Run the handler on source and return the nodes of the topology artifact
    it wrote, by unique_id."""
    writer = RecordingArtifactWriter()
    build_handler(source, MagicMock(), make_uploader(), artifact_writer=writer, **handler_options).handle(release_id="rel-1")
    return {n["unique_id"]: n for n in writer.nodes}


def test_the_probe_release_writes_one_ingested_node_and_two_edges(tmp_path):
    by_id = _topology(_probe_release(tmp_path))
    assert by_id["raw.ingest_probe"] == {
        "unique_id": "raw.ingest_probe", "schema_name": "raw", "table_name": "ingest_probe",
        "resolved_relation_id": "raw.ingest_probe", "service_name": "", "node_type": "ingested",
        "test_count": 0, "content_hash": "", "image_tag": "", "original_file_path": "",
        "upstream_unique_ids": [], "schedule": "",
    }
    assert by_id["test_schema.probe_reads_source"]["upstream_unique_ids"] == ["raw.ingest_probe"]
    assert by_id["test_schema.probe_reads_raw_name"]["upstream_unique_ids"] == ["raw.ingest_probe"]
    assert len(by_id) == 3


def test_an_ingested_node_gets_no_candidate_object_and_no_bundle_entry(tmp_path):
    uploader, bundle_uploader = make_uploader(), FakeBundleUploader()
    build_handler(
        _probe_release(tmp_path), MagicMock(), uploader, bundle_uploader=bundle_uploader,
    ).handle(release_id="rel-1")
    assert {c.kwargs["unique_id"] for c in uploader.upload.call_args_list} == {
        "test_schema.probe_reads_source", "test_schema.probe_reads_raw_name",
    }
    ((_, bundle),) = bundle_uploader.uploads
    assert sorted(bundle["nodes"]) == ["test_schema.probe_reads_raw_name", "test_schema.probe_reads_source"]


def test_a_reader_of_an_ingested_table_still_reads_it_where_it_is(tmp_path):
    """An ingested node is not in the rewriter's lookup, so the candidate
    schema never takes the table's place in a reader's SQL."""
    uploader = make_uploader()
    build_handler(_probe_release(tmp_path), MagicMock(), uploader).handle(release_id="rel-1")
    for call in uploader.upload.call_args_list:
        assert "ingest_probe" in call.kwargs["sql"]
        assert "_candidate_rel_1" not in call.kwargs["sql"]


def test_an_ingested_node_never_takes_an_image_tag(tmp_path):
    by_id = _topology(_probe_release(tmp_path), image_tags={"service-1": "registry/service-1:abc"})
    assert by_id["raw.ingest_probe"]["image_tag"] == ""
    assert by_id["test_schema.probe_reads_source"]["image_tag"] == "registry/service-1:abc"


def test_the_release_counts_its_ingested_nodes_under_the_external_runtime(tmp_path):
    timings = build_handler(_probe_release(tmp_path), MagicMock(), make_uploader()).handle(release_id="rel-1")
    assert timings.nodes == {"dbt": 2, "python": 0, "external": 1}
    assert timings.columns == {"dbt": 0, "python": 0, "external": 0}


def test_the_resolve_stage_times_the_ingested_step_in_its_one_run(tmp_path):
    """Making the ingested nodes is part of resolving: it adds no stage and no
    second run of the resolve stage."""
    timings = build_handler(
        _probe_release(tmp_path), MagicMock(), make_uploader(), clock=FakeClock(step=1.0),
    ).handle(release_id="rel-1")
    resolve = timings.stages[Stage.RESOLVE]
    assert (resolve.runs, resolve.seconds, resolve.counts) == (1, 1.0, {"nodes": 2})


def test_a_table_a_node_builds_is_no_ingested_node(tmp_path):
    reader = dbt_node("reader") | {"compiled_code": "SELECT * FROM test_schema.built"}
    built = dbt_node("built")
    by_id = _topology(source_with_nodes(tmp_path, reader, built))
    assert sorted(by_id) == ["test_schema.built", "test_schema.reader"]
    assert by_id["test_schema.reader"]["upstream_unique_ids"] == ["test_schema.built"]


def test_a_table_written_under_an_alias_is_no_ingested_node(tmp_path):
    """dbt writes test_schema.orders for the model named orders_v2 whose alias is
    orders: a read of that table is a read of a table a node builds."""
    reader = dbt_node("reader") | {"compiled_code": "SELECT * FROM test_schema.orders"}
    aliased = dbt_node("orders_v2") | {"alias": "orders"}
    assert sorted(_topology(source_with_nodes(tmp_path, reader, aliased))) == ["test_schema.orders_v2", "test_schema.reader"]


def test_a_system_table_is_no_ingested_node(tmp_path):
    reader = dbt_node("reader") | {
        "compiled_code": "SELECT c.relname FROM pg_catalog.pg_class c JOIN information_schema.tables t ON true",
    }
    by_id = _topology(source_with_nodes(tmp_path, reader))
    assert list(by_id) == ["test_schema.reader"]
    assert by_id["test_schema.reader"]["upstream_unique_ids"] == []


def test_a_schema_qualified_function_call_is_no_ingested_node(tmp_path):
    """public.my_fn(1) is a function call, not a table: it makes no node and no
    edge, beside a real table the same model reads."""
    reader = dbt_node("reader") | {
        "compiled_code": "SELECT * FROM public.my_fn(1) f JOIN raw.ingest_probe p ON true",
    }
    by_id = _topology(source_with_nodes(tmp_path, reader))
    assert sorted(by_id) == ["raw.ingest_probe", "test_schema.reader"]
    assert by_id["test_schema.reader"]["upstream_unique_ids"] == ["raw.ingest_probe"]


def test_a_model_reading_only_a_schema_qualified_function_call_has_no_upstream(tmp_path):
    reader = dbt_node("reader") | {"compiled_code": "SELECT * FROM public.my_fn(1)"}
    by_id = _topology(source_with_nodes(tmp_path, reader))
    assert list(by_id) == ["test_schema.reader"]
    assert by_id["test_schema.reader"]["upstream_unique_ids"] == []


def test_the_handler_asks_the_system_relations_it_was_built_with(tmp_path):
    """Trino names its system relations by catalog, so a read of system.runtime.nodes
    makes no node under the trino relations while a read of a hive table does."""
    reader = dbt_node("reader") | {
        "compiled_code": "SELECT * FROM system.runtime.nodes n, hive.raw.orders o",
    }
    by_id = _topology(
        source_with_nodes(tmp_path, reader), dialect="trino", system_relations=_SYSTEM_RELATIONS["trino"],
    )
    assert sorted(by_id) == ["raw.orders", "test_schema.reader"]


SYSTEM_AND_REAL_READS = (
    "SELECT * FROM system.runtime.nodes n JOIN warehouse.runtime.nodes w ON 1=1",
    "SELECT * FROM warehouse.runtime.nodes w JOIN system.runtime.nodes n ON 1=1",
)


def _trino_topology_bytes(tmp_path, compiled_code):
    writer = RecordingArtifactWriter()
    reader = dbt_node("reader") | {"compiled_code": compiled_code}
    build_handler(
        source_with_nodes(tmp_path, reader), MagicMock(), make_uploader(), artifact_writer=writer,
        dialect="trino", system_relations=_SYSTEM_RELATIONS["trino"],
    ).handle(release_id="rel-1")
    return writer.nodes, canonical_topology_json(tenant_id="default", release_id="rel-1", nodes=writer.nodes)


def test_a_system_catalog_read_before_or_after_a_real_table_read_writes_the_real_table_and_its_edge(tmp_path):
    """On Trino system.runtime.nodes is the engine's own and warehouse.runtime.nodes
    is a real table with the same schema and name: the topology holds the real
    table and its edge whichever the SQL names first, in the same bytes."""
    written = [_trino_topology_bytes(tmp_path, sql) for sql in SYSTEM_AND_REAL_READS]
    for nodes, _ in written:
        by_id = {n["unique_id"]: n for n in nodes}
        assert sorted(by_id) == ["runtime.nodes", "test_schema.reader"]
        assert by_id["runtime.nodes"]["node_type"] == "ingested"
        assert by_id["test_schema.reader"]["upstream_unique_ids"] == ["runtime.nodes"]
    assert written[0][1] == written[1][1]


def test_a_read_of_only_a_system_catalog_table_writes_no_node_and_no_edge(tmp_path):
    nodes, _ = _trino_topology_bytes(tmp_path, "SELECT * FROM system.runtime.nodes")
    assert [n["unique_id"] for n in nodes] == ["test_schema.reader"]
    assert nodes[0]["upstream_unique_ids"] == []


def _trino_topology_with_a_node_building_runtime_nodes(tmp_path, compiled_code):
    """The topology of a release on Trino with a reader of compiled_code and a
    node that builds runtime.nodes, by unique_id."""
    writer = RecordingArtifactWriter()
    reader = dbt_node("reader") | {"compiled_code": compiled_code}
    builder = dbt_node("nodes") | {"schema": "runtime"}
    build_handler(
        source_with_nodes(tmp_path, reader, builder), MagicMock(), make_uploader(), artifact_writer=writer,
        dialect="trino", system_relations=_SYSTEM_RELATIONS["trino"],
    ).handle(release_id="rel-1")
    return {n["unique_id"]: n for n in writer.nodes}


def test_a_system_catalog_read_gets_no_edge_to_a_node_that_builds_the_same_schema_and_table(tmp_path):
    """system.runtime.nodes is the engine's own: a node that builds runtime.nodes
    is not what the reader reads, so the reader gets no edge to it."""
    by_id = _trino_topology_with_a_node_building_runtime_nodes(tmp_path, "SELECT * FROM system.runtime.nodes")
    assert sorted(by_id) == ["runtime.nodes", "test_schema.reader"]
    assert by_id["runtime.nodes"]["node_type"] != "ingested"
    assert by_id["test_schema.reader"]["upstream_unique_ids"] == []


def test_a_read_under_a_non_system_catalog_gets_an_edge_to_the_node_that_builds_the_table(tmp_path):
    """The registry knows a table by schema and name: the database part of a
    read that is not the engine's own is ignored."""
    by_id = _trino_topology_with_a_node_building_runtime_nodes(tmp_path, "SELECT * FROM warehouse.runtime.nodes")
    assert sorted(by_id) == ["runtime.nodes", "test_schema.reader"]
    assert by_id["runtime.nodes"]["node_type"] != "ingested"
    assert by_id["test_schema.reader"]["upstream_unique_ids"] == ["runtime.nodes"]


def test_a_system_read_beside_a_real_read_of_a_built_table_gets_one_edge_to_the_built_node(tmp_path):
    for sql in SYSTEM_AND_REAL_READS:
        by_id = _trino_topology_with_a_node_building_runtime_nodes(tmp_path, sql)
        assert sorted(by_id) == ["runtime.nodes", "test_schema.reader"]
        assert by_id["runtime.nodes"]["node_type"] != "ingested"
        assert by_id["test_schema.reader"]["upstream_unique_ids"] == ["runtime.nodes"]


def _trino_candidate_sqls(tmp_path, **readers):
    """The candidate SQL the handler uploads for each reader of a release on Trino
    in which a node builds runtime.nodes, by the reader's name. readers maps a
    reader's name to its compiled code."""
    uploader = make_uploader()
    nodes = [dbt_node(name) | {"compiled_code": sql} for name, sql in readers.items()]
    builder = dbt_node("nodes") | {"schema": "runtime"}
    build_handler(
        source_with_nodes(tmp_path, builder, *nodes), MagicMock(), uploader,
        dialect="trino", system_relations=_SYSTEM_RELATIONS["trino"],
    ).handle(release_id="rel-1")
    sqls = {c.kwargs["unique_id"]: c.kwargs["sql"] for c in uploader.upload.call_args_list}
    return {name: sqls[f"test_schema.{name}"] for name in readers}


def test_a_system_catalog_read_stays_on_the_system_catalog_while_a_real_read_goes_to_the_candidate_schema(tmp_path):
    """A node builds runtime.nodes, so the registry knows ('runtime', 'nodes'). The
    engine's own system.runtime.nodes is not that node: its candidate SQL names
    the table where it is, while warehouse.runtime.nodes, which the node does
    build, is redirected to the release's candidate schema."""
    sqls = _trino_candidate_sqls(
        tmp_path,
        sys_reader="SELECT * FROM system.runtime.nodes",
        real_reader="SELECT * FROM warehouse.runtime.nodes",
    )
    assert sqls["sys_reader"] == "SELECT * FROM system.runtime.nodes"
    assert sqls["real_reader"] == 'SELECT * FROM warehouse."_candidate_rel_1".nodes'


def test_a_system_read_beside_a_real_read_of_a_built_table_is_split_in_either_order(tmp_path):
    for first, second in (("system", "warehouse"), ("warehouse", "system")):
        sqls = _trino_candidate_sqls(
            tmp_path,
            mixed=f"SELECT * FROM {first}.runtime.nodes a JOIN {second}.runtime.nodes b ON 1 = 1",
        )
        sql = sqls["mixed"]
        assert "system.runtime.nodes" in sql, sql
        assert 'warehouse."_candidate_rel_1".nodes' in sql, sql
        assert sql.count("_candidate_rel_1") == 1, sql


def test_a_python_nodes_system_read_is_left_alone_in_its_uploaded_spec(tmp_path):
    """The python builder rewrites each declared read with the same rule: a read
    of the engine's system.runtime.nodes stays, one of the built
    warehouse.runtime.nodes is redirected."""
    builder = python_entry(schema="runtime", table="nodes", reads={"one": "select 1 as id"})
    reader = python_entry(table="reader", reads={
        "a_system": "SELECT * FROM system.runtime.nodes",
        "b_built": "SELECT * FROM warehouse.runtime.nodes",
    })
    source = source_of(ManifestFile(
        path=python_contract(tmp_path, builder, reader), declared_service="service-py", kind=ManifestKind.PYTHON,
    ))
    spec_uploader = make_uploader("s3://x.json")
    python_handler(
        source, MagicMock(), spec_uploader=spec_uploader, dialect="trino",
        system_relations=_SYSTEM_RELATIONS["trino"],
    ).handle(release_id="rel-1")
    specs = {c.kwargs["unique_id"]: c.kwargs["spec"] for c in spec_uploader.upload.call_args_list}
    assert specs["test_schema.reader"]["reads"] == [
        "SELECT * FROM system.runtime.nodes",
        'SELECT * FROM warehouse."_candidate_rel_1".nodes',
    ]


def test_a_dbt_test_reading_an_outside_table_makes_no_node_and_no_edge(tmp_path):
    model = dbt_node("orders")
    test = {
        "unique_id": "test.service-1.assert_outside_is_empty", "name": "assert_outside_is_empty",
        "schema": "test_schema_dbt_test__audit", "fqn": ["service-1", "assert_outside_is_empty"],
        "resource_type": "test", "attached_node": "model.service-1.orders",
        "depends_on": {"nodes": ["model.service-1.orders"], "macros": []},
        "original_file_path": "tests/assert_outside_is_empty.sql", "config": {},
        "checksum": {"name": "sha256", "checksum": "hash-test"},
        "compiled_code": (
            "SELECT * FROM test_schema.orders "
            "WHERE id NOT IN (SELECT id FROM raw.outside_only_a_test_reads)"
        ),
    }
    by_id = _topology(source_with_nodes(tmp_path, model, test))
    assert "raw.outside_only_a_test_reads" not in by_id
    assert by_id["test.service-1.assert_outside_is_empty"]["upstream_unique_ids"] == ["test_schema.orders"]


def test_a_python_node_read_of_an_outside_table_makes_an_ingested_node(tmp_path):
    entry = python_entry(reads={"ext": "select id from raw.ext"})
    source = source_of(ManifestFile(
        path=python_contract(tmp_path, entry), declared_service="service-py", kind=ManifestKind.PYTHON,
    ))
    writer, spec_uploader = RecordingArtifactWriter(), make_uploader("s3://x.json")
    python_handler(source, MagicMock(), spec_uploader=spec_uploader, artifact_writer=writer).handle(release_id="rel-1")
    by_id = {n["unique_id"]: n for n in writer.nodes}
    assert sorted(by_id) == ["raw.ext", "test_schema.py_metrics"]
    assert by_id["raw.ext"]["node_type"] == "ingested"
    assert by_id["test_schema.py_metrics"]["upstream_unique_ids"] == ["raw.ext"]
    assert [c.kwargs["unique_id"] for c in spec_uploader.upload.call_args_list] == ["test_schema.py_metrics"]


def test_a_python_csv_node_makes_no_ingested_node(tmp_path):
    entry = python_entry(
        kind="python-csv", script="", reads={"csv": "s3://drops/orders.csv"},
        source_hash="csvsrc000", shared_code_hash="", config_hash="csvcfg000",
    )
    source = source_of(ManifestFile(
        path=python_contract(tmp_path, entry), declared_service="service-py", kind=ManifestKind.PYTHON,
    ))
    writer = RecordingArtifactWriter()
    python_handler(source, MagicMock(), artifact_writer=writer).handle(release_id="rel-1")
    assert [n["node_type"] for n in writer.nodes] == ["python-csv"]
    assert writer.nodes[0]["upstream_unique_ids"] == []


def test_a_release_with_a_failed_node_publishes_the_failure_and_no_ingested_nodes(tmp_path, monkeypatch):
    """The ingested nodes are made only once every node has resolved."""
    calls = []
    monkeypatch.setattr(
        candidate_manifest_handler, "ingested_nodes", lambda *a, **k: calls.append(1) or [],
    )
    broken = dbt_node("broken") | {"compiled_code": "SELECT * FROM unqualified_table"}
    reader = dbt_node("reader") | {"compiled_code": RAW_NAME_READ}
    publisher, writer = MagicMock(), RecordingArtifactWriter()
    build_handler(
        source_with_nodes(tmp_path, broken, reader), publisher, make_uploader(), artifact_writer=writer,
    ).handle(release_id="rel-1")
    publisher.publish_failed.assert_called_once()
    publisher.publish_ok.assert_not_called()
    assert writer.writes == []
    assert calls == []


def test_a_release_with_no_outside_table_gets_the_topology_it_always_got(tmp_path):
    a, b = dbt_node("a"), dbt_node("b") | {"compiled_code": "SELECT * FROM test_schema.a"}
    by_id = _topology(source_with_nodes(tmp_path, a, b))
    assert sorted(by_id) == ["test_schema.a", "test_schema.b"]
    assert all(n["node_type"] != "ingested" for n in by_id.values())


def test_the_same_release_always_writes_the_same_bytes(tmp_path):
    def written_bytes(*, reverse):
        writer = RecordingArtifactWriter()
        build_handler(
            _probe_release(tmp_path, reverse=reverse), MagicMock(), make_uploader(), artifact_writer=writer,
        ).handle(release_id="rel-1")
        return canonical_topology_json(tenant_id="default", release_id="rel-1", nodes=writer.nodes)

    first, second, reordered = written_bytes(reverse=False), written_bytes(reverse=False), written_bytes(reverse=True)
    assert first == second == reordered
    ids = [n["unique_id"] for n in json.loads(first)["nodes"]]
    assert ids == ["raw.ingest_probe", "test_schema.probe_reads_raw_name", "test_schema.probe_reads_source"]
