import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from adapters.json_log import JsonFormatter
from adapters.redis.candidate_publisher import CandidateManifestPublisher, release_parsed_event_id
from adapters.redis.keys import MissingStreamError, key
from domain.contract_vocabulary import ParseFailureKind
from domain.model import DEFAULT_TENANT_ID, ContractBundleRef, FailedNode, NodeType, TopologyArtifactRef
from streams_contract import RELEASE_PARSED_V1

_FIXTURES = Path(__file__).parent / "fixtures"
_AT = datetime(2026, 10, 8, 12, 0, 0, 123456, tzinfo=timezone.utc)
_ENVELOPE_KEYS = ("event_id", "tenant_id", "occurred_at", "producer", "schema_version")
_ARTIFACT = TopologyArtifactRef(
    uri="s3://continuo/tenants/default/topologies/rel-1/topology.json.gz",
    sha256="ab" * 32,
    node_count=3,
)
_CONTRACT_BUNDLE = ContractBundleRef(
    uri="s3://continuo/tenants/default/topologies/rel-1/contracts.json.gz",
    sha256="cd" * 32,
    by_family={"dbt": 3, "python": 2},
)
# What the handler hands the publisher about the parse: how long it took and what
# it wrote. A failed parse carries the two durations, an ok parse the counts too.
_DURATIONS = {"parse_ms": 4180, "contracts_ms": 1260}
_OK_TIMINGS = {**_DURATIONS, "contracts_by_family": {"dbt": 3, "python": 2}}


def _make(producer="topology-controller", at=_AT, continuo_contracts="0.2.0"):
    redis_mock = MagicMock()
    pub = CandidateManifestPublisher(
        redis_mock, RELEASE_PARSED_V1, producer=producer, continuo_contracts=continuo_contracts,
        clock=lambda: at,
    )
    return pub, redis_mock


def _fields(redis_mock) -> dict:
    redis_mock.xadd.assert_called_once()
    stream, fields = redis_mock.xadd.call_args.args
    assert stream == key(RELEASE_PARSED_V1)
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


def test_publish_ok_matches_the_shared_ok_fixture():
    """release-controller decodes this entry with pkg/events; the Go test reads
    the same fixture, so both sides agree on every field, the event id included."""
    given, at, want = _fixture("release_parsed_v1_ok.json")
    payload = given["payload"]
    pub, redis_mock = _make(producer=given["producer"], at=at)

    pub.publish_ok(
        release_id=payload["release_id"],
        artifact=TopologyArtifactRef(
            uri=payload["topology_uri"], sha256=payload["topology_sha256"], node_count=payload["node_count"],
        ),
        code_bundle_uri=payload["code_bundle_uri"],
        contract_bundle=ContractBundleRef(
            uri=payload["contract_bundle_uri"], sha256=payload["contract_bundle_sha256"],
            by_family=payload["contracts_by_family"],
        ),
        parse_ms=payload["parse_ms"],
        contracts_ms=payload["contracts_ms"],
        contracts_by_family=payload["contracts_by_family"],
    )

    _assert_matches(_fields(redis_mock), want)


def test_publish_failed_matches_the_shared_failed_fixture():
    given, at, want = _fixture("release_parsed_v1_failed.json")
    payload = given["payload"]
    pub, redis_mock = _make(producer=given["producer"], at=at)

    pub.publish_failed(
        release_id=payload["release_id"],
        failure_kind=ParseFailureKind(payload["failure_kind"]),
        detail=payload["detail"],
        failed_nodes=[
            FailedNode(
                node_id=n["node_id"], kind=ParseFailureKind(n["kind"]), project=n["project"],
                file_path=n["file_path"], node_type=n["node_type"], detail=n["detail"],
            )
            for n in payload["failed_nodes"]
        ],
        parse_ms=payload["parse_ms"],
        contracts_ms=payload["contracts_ms"],
    )

    _assert_matches(_fields(redis_mock), want)


def test_publish_ok_carries_the_artifact_reference_and_no_nodes():
    pub, redis_mock = _make()

    pub.publish_ok(
        release_id="rel-1", artifact=_ARTIFACT, code_bundle_uri="s3://continuo/code-bundles/rel-1/bundle.json",
        contract_bundle=_CONTRACT_BUNDLE, **_OK_TIMINGS,
    )

    fields = _fields(redis_mock)
    assert json.loads(fields["payload"]) == {
        "release_id": "rel-1",
        "status": "ok",
        "topology_uri": "s3://continuo/tenants/default/topologies/rel-1/topology.json.gz",
        "topology_sha256": "ab" * 32,
        "node_count": 3,
        "code_bundle_uri": "s3://continuo/code-bundles/rel-1/bundle.json",
        "contract_bundle_uri": "s3://continuo/tenants/default/topologies/rel-1/contracts.json.gz",
        "contract_bundle_sha256": "cd" * 32,
        "continuo_contracts": "0.2.0",
        "parse_ms": 4180,
        "contracts_ms": 1260,
        "contracts_by_family": {"dbt": 3, "python": 2},
    }
    assert fields["event_id"] == release_parsed_event_id("default", "rel-1")
    assert fields["tenant_id"] == "default"
    assert fields["producer"] == "topology-controller"
    assert fields["schema_version"] == "1"
    assert fields["occurred_at"] == "2026-10-08T12:00:00.123456Z"


def test_the_event_id_is_stable_per_release_and_includes_the_tenant():
    """A redelivered release.requested re-publishes its result under the same id."""
    one = release_parsed_event_id("default", "rel-1")
    assert one == release_parsed_event_id("default", "rel-1")
    assert one != release_parsed_event_id("default", "rel-2")
    assert one != release_parsed_event_id("acme", "rel-1")


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
                project="fx",
                file_path="models/fx.sql",
                node_type=NodeType.DBT_MODEL,
                detail="Expecting ). Line 3, Col: 12.",
            )
        ],
        **_DURATIONS,
    )

    assert _payload(redis_mock) == {
        "release_id": "rel-2",
        "status": "failed",
        "failure_kind": "invalid_sql",
        "continuo_contracts": "0.2.0",
        "parse_ms": 4180,
        "contracts_ms": 1260,
        "detail": "1 node failed to parse: analytics.fx",
        "failed_nodes": [
            {
                "node_id": "analytics.fx",
                "kind": "invalid_sql",
                "project": "fx",
                "file_path": "models/fx.sql",
                "node_type": "dbt-model",
                "detail": "Expecting ). Line 3, Col: 12.",
            }
        ],
    }


def test_publish_ok_carries_the_durations_and_the_counts():
    pub, redis_mock = _make()

    pub.publish_ok(
        release_id="rel-1", artifact=_ARTIFACT, code_bundle_uri="", contract_bundle=_CONTRACT_BUNDLE,
        parse_ms=4180, contracts_ms=1260, contracts_by_family={"dbt": 3},
    )

    payload = _payload(redis_mock)
    assert (payload["parse_ms"], payload["contracts_ms"]) == (4180, 1260)
    assert payload["contracts_by_family"] == {"dbt": 3}, "a family with no contract has no key"
    assert all(type(payload[name]) is int for name in ("parse_ms", "contracts_ms"))


def test_publish_ok_with_no_contracts_carries_an_empty_count():
    """An ok release that wrote no contract (an empty release) reports zero
    contracts, which a producer that does not write the count never does."""
    pub, redis_mock = _make()

    pub.publish_ok(
        release_id="rel-1", artifact=_ARTIFACT, code_bundle_uri="", contract_bundle=_CONTRACT_BUNDLE,
        parse_ms=12, contracts_ms=0, contracts_by_family={},
    )

    assert _payload(redis_mock)["contracts_by_family"] == {}


def test_publish_failed_carries_the_durations():
    pub, redis_mock = _make()

    pub.publish_failed(
        release_id="rel-2", failure_kind=ParseFailureKind.INVALID_SQL, detail="x", parse_ms=310, contracts_ms=45,
    )

    payload = _payload(redis_mock)
    assert (payload["parse_ms"], payload["contracts_ms"]) == (310, 45)
    assert all(type(payload[name]) is int for name in ("parse_ms", "contracts_ms"))
    assert "contracts_by_family" not in payload, "a failed release wrote no bundle to count"


def test_both_outcomes_announce_the_library_version_the_publisher_was_built_with():
    """A release-controller that reads the entry knows which continuo-contracts
    wrote the release's contracts, whether the release passed or failed."""
    pub, redis_mock = _make(continuo_contracts="9.8.7")
    pub.publish_ok(
        release_id="rel-1", artifact=_ARTIFACT, code_bundle_uri="", contract_bundle=_CONTRACT_BUNDLE,
        **_OK_TIMINGS,
    )
    assert _payload(redis_mock)["continuo_contracts"] == "9.8.7"

    pub, redis_mock = _make(continuo_contracts="9.8.7")
    pub.publish_failed(release_id="rel-2", failure_kind=ParseFailureKind.INVALID_CONTRACT, detail="x", **_DURATIONS)
    assert _payload(redis_mock)["continuo_contracts"] == "9.8.7"


def test_publish_failed_defaults_to_no_failed_nodes():
    pub, redis_mock = _make()
    pub.publish_failed(release_id="rel-3", failure_kind=ParseFailureKind.INTERNAL, detail="s3 down", **_DURATIONS)

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
                node_id="a.b", kind=ParseFailureKind.INVALID_SQL, project="s",
                file_path="models/b.sql", node_type=NodeType.DBT_MODEL, detail=sqlglot_text,
            )
        ],
        **_DURATIONS,
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
    pub = CandidateManifestPublisher(
        redis_mock, RELEASE_PARSED_V1, producer="topology-controller", continuo_contracts="0.2.0",
    )
    pub.publish_ok(
        release_id="rel-1", artifact=_ARTIFACT, code_bundle_uri="", contract_bundle=_CONTRACT_BUNDLE,
        **_OK_TIMINGS,
    )
    pub.publish_failed(release_id="rel-2", failure_kind=ParseFailureKind.INVALID_SQL, detail="boom", **_DURATIONS)

    assert redis_mock.xadd.call_count == 2
    for call in redis_mock.xadd.call_args_list:
        assert "maxlen" not in call.kwargs
        assert "minid" not in call.kwargs
        assert "approximate" not in call.kwargs


def test_publisher_writes_the_prefixed_key_without_creating_it():
    """XADD names the prefixed key and carries NOMKSTREAM, so an append never
    creates a stream the streams-init step has not."""
    pub, redis_mock = _make()
    pub.publish_ok(
        release_id="rel-1", artifact=_ARTIFACT, code_bundle_uri="", contract_bundle=_CONTRACT_BUNDLE,
        **_OK_TIMINGS,
    )
    pub.publish_failed(release_id="rel-2", failure_kind=ParseFailureKind.INVALID_SQL, detail="boom", **_DURATIONS)

    assert redis_mock.xadd.call_count == 2
    for call in redis_mock.xadd.call_args_list:
        assert call.args[0] == "continuo:" + RELEASE_PARSED_V1
        assert call.kwargs["nomkstream"] is True


def test_publisher_raises_when_the_stream_is_missing():
    """Redis answers an XADD with NOMKSTREAM on a missing stream with nil. The
    publisher raises, so the release's message is redelivered once the
    streams-init step has created the stream."""
    pub, redis_mock = _make()
    redis_mock.xadd.return_value = None

    with pytest.raises(MissingStreamError) as err:
        pub.publish_ok(
            release_id="rel-1", artifact=_ARTIFACT, code_bundle_uri="", contract_bundle=_CONTRACT_BUNDLE,
            **_OK_TIMINGS,
        )
    assert str(err.value) == f"stream continuo:{RELEASE_PARSED_V1} does not exist; the streams-init step creates it"

    with pytest.raises(MissingStreamError):
        pub.publish_failed(release_id="rel-2", failure_kind=ParseFailureKind.INTERNAL, detail="x", **_DURATIONS)


def test_the_parse_failed_line_prints_its_release_kind_detail_and_failed_node_count(caplog):
    pub, _ = _make()
    node = FailedNode("a.b", ParseFailureKind.INVALID_SQL, "svc", "models/b.sql", NodeType.DBT_MODEL, "bad")
    with caplog.at_level(logging.ERROR, logger="adapters.redis.candidate_publisher"):
        pub.publish_failed("rel-9", ParseFailureKind.INVALID_SQL, "1 node failed to parse: a.b", [node], **_DURATIONS)
    line = json.loads(JsonFormatter().format(caplog.records[-1]))
    assert (line["level"], line["msg"]) == ("ERROR", "Published release.parsed failed")
    assert (line["release_id"], line["failure_kind"], line["detail"], line["failed_nodes"]) == (
        "rel-9", "invalid_sql", "1 node failed to parse: a.b", 1)


def test_the_ok_line_prints_its_release_node_count_topology_uri_and_contract_bundle_uri(caplog):
    pub, _ = _make()
    with caplog.at_level(logging.INFO, logger="adapters.redis.candidate_publisher"):
        pub.publish_ok(
            release_id="rel-1", artifact=_ARTIFACT, code_bundle_uri="", contract_bundle=_CONTRACT_BUNDLE,
            **_OK_TIMINGS,
        )
    line = json.loads(JsonFormatter().format(caplog.records[-1]))
    assert (line["release_id"], line["node_count"], line["topology_uri"], line["contract_bundle_uri"]) == (
        "rel-1", 3, _ARTIFACT.uri, _CONTRACT_BUNDLE.uri)
