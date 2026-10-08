import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from adapters.redis.candidate_publisher import CandidateManifestPublisher, manifest_loaded_candidate_event_id
from domain.contract_vocabulary import ParseFailureKind
from domain.model import DEFAULT_TENANT_ID, FailedNode, NodeType, TopologyArtifactRef
from streams_contract import MANIFEST_LOADED_CANDIDATE_V2

_FIXTURES = Path(__file__).parent / "fixtures"
_AT = datetime(2026, 10, 8, 12, 0, 0, 123456, tzinfo=timezone.utc)
_ENVELOPE_KEYS = ("event_id", "tenant_id", "occurred_at", "producer", "schema_version")
_ARTIFACT = TopologyArtifactRef(
    uri="s3://continuo/tenants/default/topologies/rel-1/topology.json.gz",
    sha256="ab" * 32,
    node_count=3,
)


def _make(producer="topology-controller", at=_AT):
    redis_mock = MagicMock()
    pub = CandidateManifestPublisher(
        redis_mock, MANIFEST_LOADED_CANDIDATE_V2, producer=producer, clock=lambda: at,
    )
    return pub, redis_mock


def _fields(redis_mock) -> dict:
    redis_mock.xadd.assert_called_once()
    stream, fields = redis_mock.xadd.call_args.args
    assert stream == MANIFEST_LOADED_CANDIDATE_V2
    return fields


def _payload(redis_mock) -> dict:
    return json.loads(_fields(redis_mock)["payload"])


def _fixture(name):
    fixture = json.loads((_FIXTURES / name).read_text(encoding="utf-8"))
    given = fixture["input"]
    assert given["tenant_id"] == DEFAULT_TENANT_ID
    at = datetime.fromisoformat(given["occurred_at"].replace("Z", "+00:00"))
    return given, at, fixture["fields"]


def _assert_matches(got: dict, want: dict) -> None:
    assert set(got) == set(want)
    for key in _ENVELOPE_KEYS:
        assert got[key] == want[key], key
    assert json.loads(got["payload"]) == want["payload"]


def test_publish_ok_matches_the_golden_fixture():
    """release-controller decodes this entry with pkg/events; the Go test reads
    the same fixture, so both sides agree on every field, the event id included."""
    given, at, want = _fixture("manifest_loaded_candidate_v2_ok.json")
    payload = given["payload"]
    pub, redis_mock = _make(producer=given["producer"], at=at)

    pub.publish_ok(
        release_id=payload["release_id"],
        artifact=TopologyArtifactRef(
            uri=payload["topology_uri"], sha256=payload["topology_sha256"], node_count=payload["node_count"],
        ),
        code_bundle_uri=payload["code_bundle_uri"],
    )

    _assert_matches(_fields(redis_mock), want)


def test_publish_failed_matches_the_golden_fixture():
    given, at, want = _fixture("manifest_loaded_candidate_v2_failed.json")
    payload = given["payload"]
    pub, redis_mock = _make(producer=given["producer"], at=at)

    pub.publish_failed(
        release_id=payload["release_id"],
        failure_kind=ParseFailureKind(payload["failure_kind"]),
        detail=payload["detail"],
        failed_nodes=[
            FailedNode(
                node_id=n["node_id"], kind=ParseFailureKind(n["kind"]), service=n["service"],
                file_path=n["file_path"], node_type=n["node_type"], detail=n["detail"],
            )
            for n in payload["failed_nodes"]
        ],
    )

    _assert_matches(_fields(redis_mock), want)


def test_publish_ok_carries_the_artifact_reference_and_no_nodes():
    pub, redis_mock = _make()

    pub.publish_ok(release_id="rel-1", artifact=_ARTIFACT, code_bundle_uri="s3://continuo/code-bundles/rel-1/bundle.json")

    fields = _fields(redis_mock)
    assert json.loads(fields["payload"]) == {
        "release_id": "rel-1",
        "status": "ok",
        "topology_uri": "s3://continuo/tenants/default/topologies/rel-1/topology.json.gz",
        "topology_sha256": "ab" * 32,
        "node_count": 3,
        "code_bundle_uri": "s3://continuo/code-bundles/rel-1/bundle.json",
    }
    assert fields["event_id"] == manifest_loaded_candidate_event_id("default", "rel-1")
    assert fields["tenant_id"] == "default"
    assert fields["producer"] == "topology-controller"
    assert fields["schema_version"] == "1"
    assert fields["occurred_at"] == "2026-10-08T12:00:00.123456Z"


def test_the_event_id_is_stable_per_release_and_includes_the_tenant():
    """A redelivered release.requested re-publishes its result under the same id."""
    one = manifest_loaded_candidate_event_id("default", "rel-1")
    assert one == manifest_loaded_candidate_event_id("default", "rel-1")
    assert one != manifest_loaded_candidate_event_id("default", "rel-2")
    assert one != manifest_loaded_candidate_event_id("acme", "rel-1")


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

    assert _payload(redis_mock) == {
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


def test_publish_failed_defaults_to_no_failed_nodes():
    pub, redis_mock = _make()
    pub.publish_failed(release_id="rel-3", failure_kind=ParseFailureKind.INTERNAL, detail="s3 down")

    body = _payload(redis_mock)
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

    raw = _fields(redis_mock)["payload"]
    assert "\x1b" not in raw
    body = json.loads(raw)
    assert body["detail"] == "summary bold"
    assert body["failed_nodes"][0]["detail"] == "Required keyword missing. Line 1, Col: 26.\n  select a from t where"


def test_publish_does_not_cap_the_stream():
    # The dead-letter-controller's trim loop bounds every stream, so neither
    # publish path passes a MAXLEN/MINID cap to XADD.
    redis_mock = MagicMock()
    pub = CandidateManifestPublisher(redis_mock, MANIFEST_LOADED_CANDIDATE_V2, producer="topology-controller")
    pub.publish_ok(release_id="rel-1", artifact=_ARTIFACT, code_bundle_uri="")
    pub.publish_failed(release_id="rel-2", failure_kind=ParseFailureKind.INVALID_SQL, detail="boom")

    assert redis_mock.xadd.call_count == 2
    for call in redis_mock.xadd.call_args_list:
        assert "maxlen" not in call.kwargs
        assert "minid" not in call.kwargs
        assert "approximate" not in call.kwargs
