from unittest.mock import MagicMock

from domain.model import ManifestNode, NodeRegistryEntry, NodeType, Runtime
from service.candidate_artifacts import (
    DbtSqlArtifactBuilder, PythonSpecArtifactBuilder, RewriteContext,
)


def _ctx(**overrides):
    registry = {
        ("test_schema", "users"): NodeRegistryEntry(
            table_name="users", schema_name="test_schema",
            service_name="service-1", owner="team-a",
        ),
    }
    base = dict(
        release_id="rel-1", registry=registry,
        candidate_schema="_candidate_rel_1", dialect="postgres",
    )
    base.update(overrides)
    return RewriteContext(**base)


def test_dbt_builder_rewrites_uploads_and_returns_the_artifact_key():
    """The dbt builder owns the whole per-node artifact step: rewrite the
    compiled SQL to the candidate schema, upload it, and hand back the single
    topology key that references it."""
    uploader = MagicMock()
    uploader.upload.return_value = "s3://continuo/candidate-sql/rel-1/candidate_test_schema.orders.sql"
    node = ManifestNode(
        table_name="orders", schema_name="test_schema", service_name="service-2",
        owner="team-b", schedule_name="daily", criticality="SECONDARY",
        dependency_sqls=["SELECT * FROM test_schema.users"],
        candidate_sql="SELECT * FROM test_schema.users",
        node_type=NodeType.DBT_MODEL, runtime=Runtime.DBT,
    )

    keys = DbtSqlArtifactBuilder(uploader).build(node, _ctx())

    uploader.upload.assert_called_once()
    called = uploader.upload.call_args.kwargs
    assert called["release_id"] == "rel-1"
    assert called["unique_id"] == "test_schema.orders"
    assert '"_candidate_rel_1".users' in called["sql"]
    assert keys == {
        "candidate_artifact_uri":
            "s3://continuo/candidate-sql/rel-1/candidate_test_schema.orders.sql",
    }


def test_dbt_builder_leaves_a_self_reference_on_the_production_schema():
    """An incremental model selecting from itself must not be redirected: the
    validation runner drops and recreates the candidate table, so a rewritten
    self-reference would read a relation that no longer exists."""
    uploader = MagicMock()
    uploader.upload.return_value = "s3://x"
    node = ManifestNode(
        table_name="users", schema_name="test_schema", service_name="service-1",
        owner="team-a", schedule_name="daily", criticality="SECONDARY",
        candidate_sql="SELECT * FROM test_schema.users",
        node_type=NodeType.DBT_MODEL, runtime=Runtime.DBT,
    )

    DbtSqlArtifactBuilder(uploader).build(node, _ctx())

    assert "_candidate_rel_1" not in uploader.upload.call_args.kwargs["sql"]


def test_python_builder_uploads_rewritten_reads_columns_and_config():
    """The spec is the python node's whole validation input: every declared read
    redirected at the candidate schema so bind-checking hits the release's own
    upstreams, plus the declared output shape and physical layout."""
    uploader = MagicMock()
    uploader.upload.return_value = "s3://continuo/candidate-sql/rel-1/candidate_test_schema.py_metrics.json"
    node = ManifestNode(
        table_name="py_metrics", schema_name="test_schema", service_name="service-py",
        owner="team-py", schedule_name="daily", criticality="SECONDARY",
        dependency_sqls=[
            "select id from test_schema.users",
            "select count(*) from test_schema.elsewhere",
        ],
        candidate_sql="",
        output_columns=[{"name": "id", "type": "INTEGER", "nullable": False}],
        config={"indexes": [{"columns": ["id"], "unique": True}]},
        node_type=NodeType.PYTHON_MODEL, runtime=Runtime.PYTHON,
    )

    keys = PythonSpecArtifactBuilder(uploader).build(node, _ctx())

    called = uploader.upload.call_args.kwargs
    assert called["unique_id"] == "test_schema.py_metrics"
    spec = called["spec"]
    # A read on a registered node is redirected; a table no service owns is not.
    # rewrite_to_candidate_schema force-quotes only the schema identifier, not
    # the table, so the rewritten form is "_candidate_rel_1".users.
    assert '"_candidate_rel_1".users' in spec["reads"][0]
    assert "_candidate_rel_1" not in spec["reads"][1]
    assert spec["output_columns"] == [{"name": "id", "type": "INTEGER", "nullable": False}]
    assert spec["config"] == {"indexes": [{"columns": ["id"], "unique": True}]}
    assert keys == {
        "candidate_artifact_uri":
            "s3://continuo/candidate-sql/rel-1/candidate_test_schema.py_metrics.json",
    }


def test_python_builder_preserves_read_order():
    """dependency_sqls is already ordered by read name, and the runner
    bind-checks in list order: keep it stable so the object is reproducible."""
    uploader = MagicMock()
    uploader.upload.return_value = "s3://x"
    node = ManifestNode(
        table_name="py_metrics", schema_name="test_schema", service_name="service-py",
        owner="team-py", schedule_name="daily", criticality="SECONDARY",
        dependency_sqls=["select 1 as a", "select 2 as b", "select 3 as c"],
        output_columns=[{"name": "a", "type": "INTEGER", "nullable": True}],
        node_type=NodeType.PYTHON_MODEL, runtime=Runtime.PYTHON,
    )

    PythonSpecArtifactBuilder(uploader).build(node, _ctx())

    reads = uploader.upload.call_args.kwargs["spec"]["reads"]
    assert [r.split()[1] for r in reads] == ["1", "2", "3"]


def test_python_builder_includes_csv_source_for_csv_nodes():
    """A csv node's spec carries csv_source so the runner knows to
    range-fetch the header rather than run a SELECT."""
    uploader = MagicMock()
    uploader.upload.return_value = "s3://continuo/candidate-sql/rel-1/candidate_test_schema.orders_csv.json"
    node = ManifestNode(
        table_name="orders_csv", schema_name="test_schema", service_name="service-py",
        owner="team-py", schedule_name="daily", criticality="SECONDARY",
        dependency_sqls=[],
        output_columns=[{"name": "order_id", "type": "INTEGER", "nullable": False}],
        node_type=NodeType.PYTHON_CSV, runtime=Runtime.PYTHON,
        csv_source="s3://drops/orders.csv",
    )

    PythonSpecArtifactBuilder(uploader).build(node, _ctx())

    spec = uploader.upload.call_args.kwargs["spec"]
    assert spec["reads"] == []
    assert spec["csv_source"] == "s3://drops/orders.csv"


def test_python_builder_omits_csv_source_for_model_nodes():
    """A model node's spec must stay byte-identical to before csv_source
    existed: no csv_source key at all when the node has none."""
    uploader = MagicMock()
    uploader.upload.return_value = "s3://x"
    node = ManifestNode(
        table_name="py_metrics", schema_name="test_schema", service_name="service-py",
        owner="team-py", schedule_name="daily", criticality="SECONDARY",
        dependency_sqls=["select 1"],
        output_columns=[{"name": "a", "type": "INTEGER", "nullable": True}],
        node_type=NodeType.PYTHON_MODEL, runtime=Runtime.PYTHON,
    )

    PythonSpecArtifactBuilder(uploader).build(node, _ctx())

    spec = uploader.upload.call_args.kwargs["spec"]
    assert "csv_source" not in spec


def test_python_builder_leaves_a_self_reference_on_the_production_schema():
    """check_binds runs BEFORE build_empty_from_columns in the same Job, so a
    self-reference redirected to the candidate schema would bind against a table
    that does not exist yet."""
    uploader = MagicMock()
    uploader.upload.return_value = "s3://x"
    registry = {
        ("test_schema", "py_metrics"): NodeRegistryEntry(
            table_name="py_metrics", schema_name="test_schema",
            service_name="service-py", owner="team-py",
        ),
    }
    node = ManifestNode(
        table_name="py_metrics", schema_name="test_schema", service_name="service-py",
        owner="team-py", schedule_name="daily", criticality="SECONDARY",
        dependency_sqls=["select id from test_schema.py_metrics"],
        output_columns=[{"name": "id", "type": "INTEGER", "nullable": True}],
        node_type=NodeType.PYTHON_MODEL, runtime=Runtime.PYTHON,
    )

    PythonSpecArtifactBuilder(uploader).build(node, _ctx(registry=registry))

    assert "_candidate_rel_1" not in uploader.upload.call_args.kwargs["spec"]["reads"][0]
