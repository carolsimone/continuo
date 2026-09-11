import json
from unittest.mock import MagicMock
from adapters.redis.candidate_publisher import CandidateManifestPublisher
from domain.model import FailedNode, NodeType
from domain.contract_vocabulary import ParseFailureKind
from streams_contract import MANIFEST_LOADED_CANDIDATE_V1


def _make():
    redis_mock = MagicMock()
    pub = CandidateManifestPublisher(redis_mock, MANIFEST_LOADED_CANDIDATE_V1)
    return pub, redis_mock


def test_publish_ok_serialises_topology():
    pub, redis_mock = _make()
    topology = [
        {
            "unique_id": "service_1.table_a",
            "schema_name": "service_1",
            "table_name": "table_a",
            "service_name": "service-1",
            "image_tag": "",
            "upstream_unique_ids": [],
            "schedule": "hourly",
        }
    ]
    pub.publish_ok(release_id="rel-1", topology=topology)
    redis_mock.xadd.assert_called_once()
    args, kwargs = redis_mock.xadd.call_args
    assert args[0] == MANIFEST_LOADED_CANDIDATE_V1
    body = json.loads(args[1]["payload"])
    assert body["release_id"] == "rel-1"
    assert body["status"] == "ok"
    assert body["topology"] == topology
    assert "failure_kind" not in body
    assert "failed_nodes" not in body


def test_publish_failed_serialises_kind_and_failed_nodes():
    pub, redis_mock = _make()
    pub.publish_failed(
        release_id="rel-2",
        failure_kind=ParseFailureKind.INVALID_SQL,
        detail="1 node failed to parse: analytics.fx",
        failed_nodes=[
            FailedNode(
                node_id="analytics.fx",
                kind=ParseFailureKind.INVALID_SQL,
                service="fx",
                file_path="models/fx.sql",
                node_type=NodeType.DBT_MODEL,
                detail="Expecting ). Line 3, Col: 12.",
            )
        ],
    )
    redis_mock.xadd.assert_called_once()
    body = json.loads(redis_mock.xadd.call_args[0][1]["payload"])
    assert body == {
        "release_id": "rel-2",
        "status": "failed",
        "failure_kind": "invalid_sql",
        "detail": "1 node failed to parse: analytics.fx",
        "failed_nodes": [
            {
                "node_id": "analytics.fx",
                "kind": "invalid_sql",
                "service": "fx",
                "file_path": "models/fx.sql",
                "node_type": "dbt-model",
                "detail": "Expecting ). Line 3, Col: 12.",
            }
        ],
    }
    assert "error_class" not in body


def test_publish_failed_defaults_to_no_failed_nodes():
    pub, redis_mock = _make()
    pub.publish_failed(release_id="rel-3", failure_kind=ParseFailureKind.INTERNAL, detail="s3 down")
    body = json.loads(redis_mock.xadd.call_args[0][1]["payload"])
    assert body["failure_kind"] == "internal"
    assert body["failed_nodes"] == []


def test_publish_failed_strips_ansi_escapes_from_every_detail():
    pub, redis_mock = _make()
    sqlglot_text = "Required keyword missing. Line 1, Col: 26.\n  select a from t \x1b[4mwhere\x1b[0m"
    pub.publish_failed(
        release_id="rel-4",
        failure_kind=ParseFailureKind.INVALID_SQL,
        detail="summary \x1b[1mbold\x1b[0m",
        failed_nodes=[
            FailedNode(
                node_id="a.b", kind=ParseFailureKind.INVALID_SQL, service="s",
                file_path="models/b.sql", node_type=NodeType.DBT_MODEL, detail=sqlglot_text,
            )
        ],
    )
    raw = redis_mock.xadd.call_args[0][1]["payload"]
    assert "\x1b" not in raw
    body = json.loads(raw)
    assert body["detail"] == "summary bold"
    assert body["failed_nodes"][0]["detail"] == "Required keyword missing. Line 1, Col: 26.\n  select a from t where"


def test_publish_ok_with_empty_topology():
    pub, redis_mock = _make()
    pub.publish_ok(release_id="rel-3", topology=[])
    body = json.loads(redis_mock.xadd.call_args[0][1]["payload"])
    assert body == {"release_id": "rel-3", "status": "ok", "topology": [], "code_bundle_uri": ""}


def test_publish_ok_carries_code_bundle_uri():
    pub, redis_mock = _make()
    pub.publish_ok(release_id="rel-1", topology=[], code_bundle_uri="s3://b/code-bundles/rel-1/bundle.json")
    body = json.loads(redis_mock.xadd.call_args.args[1]["payload"])
    assert body["code_bundle_uri"] == "s3://b/code-bundles/rel-1/bundle.json"
