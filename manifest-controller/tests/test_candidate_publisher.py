import json
from unittest.mock import MagicMock
from adapters.redis.candidate_publisher import CandidateManifestPublisher
from domain.model import RuntimeManifestRef
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
    pub.publish_ok(release_id="rel-1", topology=topology, runtime_manifests={})
    redis_mock.xadd.assert_called_once()
    args, kwargs = redis_mock.xadd.call_args
    assert args[0] == MANIFEST_LOADED_CANDIDATE_V1
    body = json.loads(args[1]["payload"])
    assert body["release_id"] == "rel-1"
    assert body["status"] == "ok"
    assert body["topology"] == topology
    assert body["runtime_manifests"] == {}
    assert "error_class" not in body
    assert "error_detail" not in body


def test_publish_ok_includes_one_runtime_ref_per_service():
    """The publisher serialises the refs itself; callers pass domain objects."""
    pub, redis_mock = _make()
    pub.publish_ok(release_id="r1", topology=[], runtime_manifests={
        "service-1": RuntimeManifestRef(
            uri="s3://continuo/service-1/r1/partial_parse.msgpack",
            sha256="a" * 64,
            dbt_version="1.12.0b1",
            parse_context_sha256="b" * 64,
        ),
        "service-2": RuntimeManifestRef(
            uri="s3://continuo/service-2/r1/partial_parse.msgpack",
            sha256="c" * 64,
            dbt_version="1.12.0b1",
            parse_context_sha256="d" * 64,
        ),
    })
    body = json.loads(redis_mock.xadd.call_args[0][1]["payload"])
    assert body["runtime_manifests"]["service-1"] == {
        "runtime_manifest_uri": "s3://continuo/service-1/r1/partial_parse.msgpack",
        "runtime_manifest_sha256": "a" * 64,
        "runtime_manifest_dbt_version": "1.12.0b1",
        "runtime_manifest_parse_context_sha256": "b" * 64,
    }
    assert body["runtime_manifests"]["service-2"]["runtime_manifest_sha256"] == "c" * 64


def test_publish_failed_includes_error_class_and_detail():
    pub, redis_mock = _make()
    pub.publish_failed(
        release_id="rel-2",
        error_class="UnqualifiedTableReference",
        error_detail="ref 'orders' missing schema",
    )
    redis_mock.xadd.assert_called_once()
    body = json.loads(redis_mock.xadd.call_args[0][1]["payload"])
    assert body == {
        "release_id": "rel-2",
        "status": "failed",
        "error_class": "UnqualifiedTableReference",
        "error_detail": "ref 'orders' missing schema",
    }


def test_publish_ok_with_empty_topology():
    pub, redis_mock = _make()
    pub.publish_ok(release_id="rel-3", topology=[], runtime_manifests={})
    body = json.loads(redis_mock.xadd.call_args[0][1]["payload"])
    assert body == {
        "release_id": "rel-3",
        "status": "ok",
        "topology": [],
        "runtime_manifests": {},
    }
