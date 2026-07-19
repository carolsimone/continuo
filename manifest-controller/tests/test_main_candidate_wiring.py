import json
import sys
from types import SimpleNamespace
import main
from streams_contract import (
    RELEASE_REQUESTED_V1,
    MANIFEST_LOADED_CANDIDATE_V1,
    MANIFEST_CONTROLLER_RELEASE_REQUESTED,
)


class _RecordingConsumer:
    instances = []

    def __init__(self, redis_client, stream_name, group_name, message_handler):
        self.stream_name = stream_name
        self.group_name = group_name
        self.message_handler = message_handler
        _RecordingConsumer.instances.append(self)

    def start(self):
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
    monkeypatch.setattr(main, "RELEASE_REQUESTED_GROUP", MANIFEST_CONTROLLER_RELEASE_REQUESTED)
    monkeypatch.setattr(main, "MANIFEST_LOADED_CANDIDATE_STREAM", MANIFEST_LOADED_CANDIDATE_V1)
    monkeypatch.setattr(main, "CandidateManifestPublisher", lambda *a, **kw: object())
    monkeypatch.setattr(main, "CandidateSqlUploader", lambda *a, **kw: object())
    monkeypatch.setattr(main.redis, "from_url", lambda *a, **kw: object())
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


def test_main_consumer_group_correct(monkeypatch):
    _common_monkeypatches(monkeypatch)
    main.main()
    assert len(_RecordingConsumer.instances) == 1
    consumer = _RecordingConsumer.instances[0]
    assert consumer.stream_name == RELEASE_REQUESTED_V1
    assert consumer.group_name == MANIFEST_CONTROLLER_RELEASE_REQUESTED


def test_main_candidate_handler_dispatches_with_manifest_keys(monkeypatch):
    """handle_release_requested builds S3Source from manifest_keys list."""
    _common_monkeypatches(monkeypatch)
    captured = {}

    class FakeCandidateHandler:
        def __init__(self, source, publisher, uploader):
            captured["source"] = source
            captured["publisher"] = publisher
            captured["uploader"] = uploader

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
        ("service-1", "service-1/rel-77/manifest.json"),
        ("service-2", "service-2/rel-77/manifest.json"),
    ]


def test_main_candidate_handler_rejects_missing_payload(monkeypatch):
    _common_monkeypatches(monkeypatch)
    monkeypatch.setattr(main, "CandidateManifestHandler", lambda **kw: SimpleNamespace(handle=lambda release_id: None))
    monkeypatch.setattr(main, "S3Source", lambda **kw: SimpleNamespace(cleanup=lambda: None))
    main.main()
    candidate_consumer = next(
        c for c in _RecordingConsumer.instances if c.stream_name == RELEASE_REQUESTED_V1
    )
    import pytest
    with pytest.raises(ValueError, match="missing payload"):
        candidate_consumer.message_handler({})


def test_main_candidate_handler_rejects_invalid_json_payload(monkeypatch):
    _common_monkeypatches(monkeypatch)
    monkeypatch.setattr(main, "CandidateManifestHandler", lambda **kw: SimpleNamespace(handle=lambda release_id: None))
    monkeypatch.setattr(main, "S3Source", lambda **kw: SimpleNamespace(cleanup=lambda: None))
    main.main()
    candidate_consumer = next(
        c for c in _RecordingConsumer.instances if c.stream_name == RELEASE_REQUESTED_V1
    )
    import pytest
    with pytest.raises(ValueError, match="not valid JSON"):
        candidate_consumer.message_handler({b"payload": b"not json {{{"})


def test_main_candidate_handler_rejects_payload_missing_release_id(monkeypatch):
    _common_monkeypatches(monkeypatch)
    monkeypatch.setattr(main, "CandidateManifestHandler", lambda **kw: SimpleNamespace(handle=lambda release_id: None))
    monkeypatch.setattr(main, "S3Source", lambda **kw: SimpleNamespace(cleanup=lambda: None))
    main.main()
    candidate_consumer = next(
        c for c in _RecordingConsumer.instances if c.stream_name == RELEASE_REQUESTED_V1
    )
    import json as _json
    import pytest
    with pytest.raises(ValueError, match="missing release_id or manifest_keys"):
        candidate_consumer.message_handler({b"payload": _json.dumps({"manifest_keys": []}).encode()})


def test_main_candidate_handler_rejects_payload_missing_manifest_keys(monkeypatch):
    _common_monkeypatches(monkeypatch)
    monkeypatch.setattr(main, "CandidateManifestHandler", lambda **kw: SimpleNamespace(handle=lambda release_id: None))
    monkeypatch.setattr(main, "S3Source", lambda **kw: SimpleNamespace(cleanup=lambda: None))
    main.main()
    candidate_consumer = next(
        c for c in _RecordingConsumer.instances if c.stream_name == RELEASE_REQUESTED_V1
    )
    import json as _json
    import pytest
    with pytest.raises(ValueError, match="missing release_id or manifest_keys"):
        candidate_consumer.message_handler({b"payload": _json.dumps({"release_id": "x"}).encode()})


def test_main_candidate_handler_rejects_manifest_key_missing_service_field(monkeypatch):
    """An entry without a 'service' field is a permanent malformed-payload error."""
    _common_monkeypatches(monkeypatch)
    monkeypatch.setattr(main, "CandidateManifestHandler", lambda **kw: SimpleNamespace(handle=lambda release_id: None))
    monkeypatch.setattr(main, "S3Source", lambda **kw: SimpleNamespace(cleanup=lambda: None))
    main.main()
    candidate_consumer = next(
        c for c in _RecordingConsumer.instances if c.stream_name == RELEASE_REQUESTED_V1
    )
    import json as _json
    import pytest
    payload = _json.dumps({
        "release_id": "rel-x",
        "manifest_keys": [
            {"s3_uri": "s3://continuo/service-1/rel-x/manifest.json"},  # missing "service"
        ],
    })
    with pytest.raises(ValueError, match="missing or empty 'service' field"):
        candidate_consumer.message_handler({b"payload": payload.encode()})
