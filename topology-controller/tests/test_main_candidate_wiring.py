import logging
import json
import sys
from types import SimpleNamespace

import pytest

import main
from adapters.metrics.prometheus import TopologyMetrics
from adapters.process import MonotonicClock, ResidentMemoryProbe
from adapters.redis.error_class import ErrorClass, classify, is_infrastructure
from adapters.redis.fence import PermissionFenceError
from adapters.topology_artifact_writer import TopologyArtifactWriter
from config.config import _SYSTEM_RELATIONS, SERVICE_NAME
from config.secrets import EnvOrFileCredentialProvider
from domain.model import ManifestRequest
from service.errors import PermanentMessageError
from streams_contract import (
    RELEASE_REQUESTED_V1,
    MANIFEST_LOADED_CANDIDATE_V2,
    TOPOLOGY_CONTROLLER_RELEASE_REQUESTED,
)


class _RecordingConsumer:
    instances = []

    def __init__(self, redis_client, stream_name, group_name, message_handler, service_name, observer=None):
        self.stream_name = stream_name
        self.observer = observer
        self.service_name = service_name
        self.group_name = group_name
        self.message_handler = message_handler
        _RecordingConsumer.instances.append(self)

    def start(self):
        pass

    def refresh_heartbeat(self):
        pass


class _NoopThread:
    def __init__(self, target, daemon, name=None):
        self.target = target
        self.daemon = daemon
        self.name = name

    def start(self):
        pass

    def join(self):
        pass


def _common_monkeypatches(monkeypatch):
    monkeypatch.setattr(main, "validate", lambda: None)
    monkeypatch.setattr(main, "S3_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setattr(main, "S3_BUCKET", "continuo")
    monkeypatch.setattr(main, "S3_ENV", "local")
    monkeypatch.setattr(main, "REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(main, "RELEASE_REQUESTED_STREAM", RELEASE_REQUESTED_V1)
    monkeypatch.setattr(main, "RELEASE_REQUESTED_GROUP", TOPOLOGY_CONTROLLER_RELEASE_REQUESTED)
    monkeypatch.setattr(main, "MANIFEST_LOADED_CANDIDATE_STREAM", MANIFEST_LOADED_CANDIDATE_V2)
    monkeypatch.setattr(main, "CandidateManifestPublisher", lambda *a, **kw: object())
    monkeypatch.setattr(main, "CandidateSqlUploader", lambda *a, **kw: object())
    monkeypatch.setattr(main, "CodeBundleUploader", lambda *a, **kw: object())
    monkeypatch.setattr(main.redis, "from_url", lambda *a, **kw: object())
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    # The fake Redis client answers no command, so the permission fence stays
    # off whatever REDIS_ACL_MODE the test process inherits (the compose
    # container sets perService); the fence's own test sets it.
    monkeypatch.setenv("REDIS_ACL_MODE", "shared")
    monkeypatch.setitem(
        sys.modules, "boto3",
        SimpleNamespace(client=lambda *a, **kw: object()),
    )
    _RecordingConsumer.instances = []
    monkeypatch.setattr(main, "Consumer", _RecordingConsumer)
    monkeypatch.setattr(main.threading, "Thread", _NoopThread)
    # The real health server binds a socket in its constructor; these wiring
    # tests only care about the candidate-consumer wiring, so stub it out
    # rather than open a real port per test run. tests/test_health_server.py
    # covers the health server itself.
    monkeypatch.setattr(main, "start_health_server", lambda *a, **kw: None)
    # Same for the metrics listener: tests/test_metrics_server.py covers it, and
    # METRICS_PORT is cleared so the default port is what the wiring asks for.
    monkeypatch.delenv("METRICS_PORT", raising=False)
    monkeypatch.setattr(main, "start_metrics_server", lambda *a, **kw: None)


class _InlineThread(_NoopThread):
    """Runs its target when started, so main() sees the consumer's outcome."""

    def start(self):
        self.target()


def test_main_exits_non_zero_and_logs_when_the_consumer_cannot_start(monkeypatch, caplog):
    _common_monkeypatches(monkeypatch)
    monkeypatch.setattr(main.threading, "Thread", _InlineThread)

    def _failing_start(self):
        raise RuntimeError("consumer group does not exist")

    monkeypatch.setattr(_RecordingConsumer, "start", _failing_start)

    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as exited:
        main.main()

    assert exited.value.code == 1
    assert any(
        record.exc_info and "consumer group does not exist" in str(record.exc_info[1])
        for record in caplog.records
    )


def test_main_configures_json_logging_at_the_log_level(monkeypatch):
    _common_monkeypatches(monkeypatch)
    monkeypatch.setenv("LOG_LEVEL", "debug")
    levels = []
    monkeypatch.setattr(main, "configure_logging", lambda level: levels.append(level))

    main.main()

    assert levels == [logging.DEBUG]


def test_main_returns_when_the_consumer_ends_cleanly(monkeypatch):
    _common_monkeypatches(monkeypatch)
    monkeypatch.setattr(main.threading, "Thread", _InlineThread)

    main.main()


def test_main_starts_one_candidate_consumer(monkeypatch):
    _common_monkeypatches(monkeypatch)
    main.main()
    streams = [c.stream_name for c in _RecordingConsumer.instances]
    assert streams == [RELEASE_REQUESTED_V1]


def test_main_starts_health_server_wired_to_the_candidate_consumer(monkeypatch):
    """main() must wire the health server to the *same* consumer + thread it
    starts, not a disconnected/default one — otherwise /health can't detect
    this consumer's loop going stale."""
    _common_monkeypatches(monkeypatch)
    calls = []
    monkeypatch.setattr(
        main, "start_health_server",
        lambda port, consumer, thread: calls.append((port, consumer, thread)),
    )
    main.main()
    assert len(calls) == 1
    port, consumer, thread = calls[0]
    assert isinstance(port, int) and port > 0
    assert consumer is _RecordingConsumer.instances[0]
    assert isinstance(thread, _NoopThread)


def test_main_binds_the_metrics_port_before_the_consumer_starts(monkeypatch):
    _common_monkeypatches(monkeypatch)
    events = []
    monkeypatch.setattr(main, "start_metrics_server", lambda port, registry: events.append(("metrics", port)))

    class _RecordingThread(_NoopThread):
        def start(self):
            events.append(("thread", self.name))

    monkeypatch.setattr(main.threading, "Thread", _RecordingThread)
    main.main()
    assert events[0] == ("metrics", 9464)
    assert ("thread", "consumer-release-requested") in events[1:]


def test_main_wires_one_metrics_object_to_the_listener_the_handler_and_the_consumer(monkeypatch):
    _common_monkeypatches(monkeypatch)
    captured = {}
    served = []
    monkeypatch.setattr(main, "start_metrics_server", lambda port, registry: served.append(registry))

    class FakeCandidateHandler:
        def __init__(self, **kw):
            captured.update(kw)

        def handle(self, release_id):
            captured["release_id"] = release_id

    monkeypatch.setattr(main, "CandidateManifestHandler", FakeCandidateHandler)
    monkeypatch.setattr(main, "S3Source", lambda **kw: SimpleNamespace(cleanup=lambda: None, kwargs=kw))

    main.main()
    _RecordingConsumer.instances[0].message_handler({b"payload": json.dumps({
        "release_id": "rel-1", "manifest_keys": [], "image_tags": {},
    }).encode()})

    assert isinstance(captured["observer"], TopologyMetrics)
    assert served == [captured["observer"].registry]
    assert _RecordingConsumer.instances[0].observer is captured["observer"]


def test_main_exits_non_zero_when_the_metrics_port_is_taken(monkeypatch, caplog):
    _common_monkeypatches(monkeypatch)

    def taken(port, registry):
        raise OSError(98, "Address already in use")

    monkeypatch.setattr(main, "start_metrics_server", taken)
    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as exited:
        main.main()
    assert exited.value.code == 1
    assert _RecordingConsumer.instances == []
    assert any(r.getMessage() == "cannot listen on the metrics port" and r.port == 9464 for r in caplog.records)


def test_main_consumer_group_correct(monkeypatch):
    _common_monkeypatches(monkeypatch)
    main.main()
    assert len(_RecordingConsumer.instances) == 1
    consumer = _RecordingConsumer.instances[0]
    assert consumer.stream_name == RELEASE_REQUESTED_V1
    assert consumer.group_name == TOPOLOGY_CONTROLLER_RELEASE_REQUESTED


def test_main_candidate_handler_dispatches_with_manifest_keys(monkeypatch):
    """handle_release_requested builds S3Source from manifest_keys list."""
    _common_monkeypatches(monkeypatch)
    captured = {}

    class FakeCandidateHandler:
        def __init__(self, **kw):
            captured.update(kw)

        def handle(self, release_id):
            captured["release_id"] = release_id

    monkeypatch.setattr(main, "CandidateManifestHandler", FakeCandidateHandler)
    monkeypatch.setattr(main, "S3Source", lambda **kw: SimpleNamespace(cleanup=lambda: None, kwargs=kw))

    main.main()
    candidate_consumer = next(
        c for c in _RecordingConsumer.instances if c.stream_name == RELEASE_REQUESTED_V1
    )
    payload = json.dumps({
        "release_id": "rel-77",
        "manifest_keys": [
            {"service": "service-1", "s3_uri": "s3://continuo/service-1/rel-77/manifest.json"},
            {"service": "service-2", "s3_uri": "s3://continuo/service-2/rel-77/manifest.json"},
        ],
    })
    candidate_consumer.message_handler({b"payload": payload.encode()})

    assert captured["release_id"] == "rel-77"
    src = captured["source"]
    assert src.kwargs["bucket"] == "continuo"
    assert src.kwargs["keys"] == [
        ManifestRequest(service="service-1", key="service-1/rel-77/manifest.json"),
        ManifestRequest(service="service-2", key="service-2/rel-77/manifest.json"),
    ]
    assert captured["bundle_uploader"] is not None
    assert captured["dialect"] == "postgres"
    assert captured["system_relations"] is _SYSTEM_RELATIONS["postgres"]
    assert isinstance(captured["clock"], MonotonicClock)
    assert isinstance(captured["memory"], ResidentMemoryProbe)
    assert captured["progress"] == candidate_consumer.refresh_heartbeat


def test_main_passes_the_configured_engines_dialect_to_the_handler(monkeypatch):
    """main resolves WAREHOUSE_ENGINE once and injects the dialect.

    The composition root owns engine selection so the SQL stages never read
    config themselves; this pins that a non-default engine actually reaches the
    handler rather than every install silently running postgres.
    """
    _common_monkeypatches(monkeypatch)
    monkeypatch.setenv("WAREHOUSE_ENGINE", "trino")
    captured = {}

    class FakeCandidateHandler:
        def __init__(self, **kw):
            captured["dialect"] = kw["dialect"]
            captured["system_relations"] = kw["system_relations"]

        def handle(self, release_id):
            pass

    monkeypatch.setattr(main, "CandidateManifestHandler", FakeCandidateHandler)
    monkeypatch.setattr(main, "S3Source", lambda **kw: SimpleNamespace(cleanup=lambda: None))

    main.main()
    candidate_consumer = next(
        c for c in _RecordingConsumer.instances if c.stream_name == RELEASE_REQUESTED_V1
    )
    payload = json.dumps({
        "release_id": "rel-78",
        "manifest_keys": [
            {"service": "service-1", "s3_uri": "s3://continuo/service-1/rel-78/manifest.json"},
        ],
    })
    candidate_consumer.message_handler({b"payload": payload.encode()})

    assert captured["dialect"] == "trino"
    assert captured["system_relations"] is _SYSTEM_RELATIONS["trino"]


def test_both_runtimes_have_an_artifact_builder(monkeypatch):
    """A runtime with no builder fails the release at parse time; the
    composition root is the only place that can prevent it."""
    _common_monkeypatches(monkeypatch)
    monkeypatch.setattr(main, "CandidateSpecUploader", lambda *a, **kw: object())
    captured = {}
    monkeypatch.setattr(main, "S3Source", lambda **kw: object())

    def _fake_handler(**kw):
        # `dict.setdefault(...) or SimpleNamespace(...)` would return the
        # (truthy, non-empty) builders dict itself rather than the namespace,
        # since a non-empty dict is truthy; capture explicitly instead so the
        # fake still returns something with a .handle() to call below.
        captured["builders"] = kw["artifact_builders"]
        return SimpleNamespace(handle=lambda release_id: None)

    monkeypatch.setattr(main, "CandidateManifestHandler", _fake_handler)
    monkeypatch.setattr(main, "Consumer", _RecordingConsumer)
    monkeypatch.setattr(main.threading, "Thread", _NoopThread)
    monkeypatch.setattr(main, "start_health_server", lambda *a, **kw: None)
    _RecordingConsumer.instances = []

    main.main()
    _RecordingConsumer.instances[0].message_handler({b"payload": json.dumps({
        "release_id": "rel-1",
        "manifest_keys": [
            {"service": "s1", "s3_uri": "s3://continuo/s1/rel-1/manifest.json"},
        ],
    }).encode()})

    assert set(captured["builders"]) == {"dbt", "python"}


def test_main_names_the_service_on_the_consumer(monkeypatch):
    """The consumer stamps SERVICE_NAME as the producer of its dead letters."""
    _common_monkeypatches(monkeypatch)
    main.main()
    assert _RecordingConsumer.instances[0].service_name == SERVICE_NAME == "topology-controller"


def test_the_handler_surfaces_the_parsers_permanent_error_without_building_a_source(monkeypatch):
    """Decoding lives in the release.requested binding; the handler only runs
    it first, so a malformed message reaches the consumer as the permanent
    error it dead-letters and nothing downstream is built."""
    _common_monkeypatches(monkeypatch)
    built = []
    monkeypatch.setattr(main, "S3Source", lambda **kw: built.append(kw))
    monkeypatch.setattr(main, "CandidateManifestHandler", lambda **kw: built.append(kw))
    main.main()

    handler = _RecordingConsumer.instances[0].message_handler
    with pytest.raises(PermanentMessageError) as raised:
        handler({b"payload": b"not json {{{"})

    assert classify(raised.value) is ErrorClass.PERMANENT
    assert built == []


def test_the_handler_gives_the_parser_the_configured_bucket_as_the_default(monkeypatch):
    """A release that lists no manifests names no bucket, so the source reads
    from the install's configured one."""
    _common_monkeypatches(monkeypatch)
    captured = {}
    monkeypatch.setattr(main, "S3Source", lambda **kw: captured.update(kw) or object())
    monkeypatch.setattr(main, "CandidateManifestHandler",
                        lambda **kw: SimpleNamespace(handle=lambda release_id: None))
    main.main()

    _RecordingConsumer.instances[0].message_handler(
        {b"payload": json.dumps({"release_id": "rel-1", "manifest_keys": []}).encode()}
    )

    assert captured["bucket"] == "continuo"


def test_main_logs_at_the_level_log_level_names(monkeypatch):
    _common_monkeypatches(monkeypatch)
    monkeypatch.setenv("LOG_LEVEL", "debug")
    root = logging.getLogger()
    previous = root.level
    try:
        main.main()
        assert root.level == logging.DEBUG
    finally:
        root.setLevel(previous)


def test_main_refuses_an_unknown_log_level(monkeypatch):
    _common_monkeypatches(monkeypatch)
    monkeypatch.setenv("LOG_LEVEL", "verbose")
    with pytest.raises(RuntimeError, match="LOG_LEVEL"):
        main.main()


@pytest.mark.parametrize("payload_tags, handed", [
    ({"service-1": "reg/service-1:abc"}, {"service-1": "reg/service-1:abc"}),
    (None, None),
], ids=["present", "absent"])
def test_main_hands_the_handler_the_requests_image_tags(monkeypatch, payload_tags, handed):
    _common_monkeypatches(monkeypatch)
    captured = {}
    monkeypatch.setattr(main, "S3Source", lambda **kw: SimpleNamespace(cleanup=lambda: None))
    monkeypatch.setattr(main, "CandidateManifestHandler",
                        lambda **kw: captured.update(kw) or SimpleNamespace(handle=lambda release_id: None))
    main.main()
    payload = {"release_id": "rel-1", "manifest_keys": []}
    if payload_tags is not None:
        payload["image_tags"] = payload_tags

    _RecordingConsumer.instances[0].message_handler({b"payload": json.dumps(payload).encode()})

    assert captured["image_tags"] == handed


def test_main_hands_each_handler_the_artifact_writer_and_the_outage_test(monkeypatch):
    """The handler writes the artifact through the composition root's writer and
    re-raises an S3 outage only when the consumer's own classifier calls it one."""
    _common_monkeypatches(monkeypatch)
    captured = {}
    monkeypatch.setattr(main, "S3Source", lambda **kw: SimpleNamespace(cleanup=lambda: None))
    monkeypatch.setattr(main, "CandidateManifestHandler",
                        lambda **kw: captured.update(kw) or SimpleNamespace(handle=lambda release_id: None))
    main.main()

    _RecordingConsumer.instances[0].message_handler({b"payload": json.dumps({
        "release_id": "rel-1", "manifest_keys": [], "image_tags": {},
    }).encode()})

    assert isinstance(captured["artifact_writer"], TopologyArtifactWriter)
    assert captured["is_infrastructure_error"] is is_infrastructure


def test_main_publishes_parse_results_on_the_v2_stream_as_this_service(monkeypatch):
    _common_monkeypatches(monkeypatch)
    built = []
    monkeypatch.setattr(main, "CandidateManifestPublisher",
                        lambda *a, **kw: built.append((a, kw)) or object())

    main.main()

    ((args, kwargs),) = built
    assert args[1] == MANIFEST_LOADED_CANDIDATE_V2
    assert kwargs["producer"] == SERVICE_NAME


def test_main_signs_in_to_redis_through_the_credential_provider(monkeypatch):
    """The Redis client takes its credentials from the provider, which reads
    them on every connection, and the URL carries none."""
    _common_monkeypatches(monkeypatch)
    calls = []
    monkeypatch.setattr(main.redis, "from_url", lambda *a, **kw: calls.append((a, kw)) or object())

    main.main()

    ((args, kwargs),) = calls
    assert args == ("redis://localhost:6379/0",)
    assert isinstance(kwargs["credential_provider"], EnvOrFileCredentialProvider)


def test_main_hands_boto3_the_keys_read_through_read_secret(monkeypatch, tmp_path):
    _common_monkeypatches(monkeypatch)
    key_file = tmp_path / "AWS_ACCESS_KEY_ID"
    key_file.write_text("file-key\n")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID_FILE", str(key_file))
    clients = []
    monkeypatch.setitem(
        sys.modules, "boto3",
        SimpleNamespace(client=lambda *a, **kw: clients.append(kw) or object()),
    )

    main.main()

    assert clients[0]["aws_access_key_id"] == "file-key"
    assert clients[0]["aws_secret_access_key"] == "secret"


def test_main_runs_the_permission_fence_on_its_redis_client_before_consuming(monkeypatch):
    """main checks the Redis login it signs in as, in the REDIS_ACL_MODE the
    install declares, before any consumer starts."""
    _common_monkeypatches(monkeypatch)
    client = object()
    monkeypatch.setattr(main.redis, "from_url", lambda *a, **kw: client)
    monkeypatch.setenv("REDIS_ACL_MODE", "perService")
    calls = []

    def _fence(redis_client, mode):
        calls.append((redis_client, mode, len(_RecordingConsumer.instances)))

    monkeypatch.setattr(main, "check_permission_fence", _fence)
    main.main()
    assert calls == [(client, "perService", 0)]


def test_main_stops_when_the_permission_fence_refuses_the_login(monkeypatch):
    _common_monkeypatches(monkeypatch)

    def _fence(redis_client, mode):
        raise PermissionFenceError("redis login keywide can read keys outside its own")

    monkeypatch.setattr(main, "check_permission_fence", _fence)
    with pytest.raises(PermissionFenceError):
        main.main()
    assert _RecordingConsumer.instances == []
