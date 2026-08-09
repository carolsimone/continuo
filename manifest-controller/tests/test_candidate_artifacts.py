from unittest.mock import MagicMock

from domain.model import ManifestNode, NodeRegistryEntry, NodeType, Runtime
from service.candidate_artifacts import DbtSqlArtifactBuilder, RewriteContext


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
