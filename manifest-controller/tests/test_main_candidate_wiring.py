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
    monkeypatch.setattr(main.redis, "from_url", lambda *a, **kw: object())
    monkeypatch.setitem(
        sys.modules, "boto3",
        SimpleNamespace(client=lambda *a, **kw: object()),
    )
    _RecordingConsumer.instances = []
    monkeypatch.setattr(main, "Consumer", _RecordingConsumer)
    monkeypatch.setattr(main.threading, "Thread", _NoopThread)


def test_main_starts_one_candidate_consumer(monkeypatch):
    _common_monkeypatches(monkeypatch)
    main.main()
    streams = [c.stream_name for c in _RecordingConsumer.instances]
    assert streams == [RELEASE_REQUESTED_V1]


def test_main_consumer_group_correct(monkeypatch):
    _common_monkeypatches(monkeypatch)
    main.main()
    assert len(_RecordingConsumer.instances) == 1
    consumer = _RecordingConsumer.instances[0]
    assert consumer.stream_name == RELEASE_REQUESTED_V1
    assert consumer.group_name == MANIFEST_CONTROLLER_RELEASE_REQUESTED


def test_main_candidate_handler_dispatches_with_release_id_and_uri(monkeypatch):
    _common_monkeypatches(monkeypatch)
    captured = {}

    class FakeCandidateHandler:
        def __init__(self, source, publisher):
            captured["source"] = source
            captured["publisher"] = publisher

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
        "manifests_uri": "s3://continuo/releases/rel-77/manifests/",
    })
    candidate_consumer.message_handler({b"payload": payload.encode()})

    assert captured["release_id"] == "rel-77"
    src = captured["source"]
    assert src.kwargs["bucket"] == "continuo"
    assert src.kwargs["prefix"] == "releases/rel-77/manifests/"


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


def test_main_candidate_handler_rejects_payload_missing_fields(monkeypatch):
    _common_monkeypatches(monkeypatch)
    monkeypatch.setattr(main, "CandidateManifestHandler", lambda **kw: SimpleNamespace(handle=lambda release_id: None))
    monkeypatch.setattr(main, "S3Source", lambda **kw: SimpleNamespace(cleanup=lambda: None))
    main.main()
    candidate_consumer = next(
        c for c in _RecordingConsumer.instances if c.stream_name == RELEASE_REQUESTED_V1
    )
    import json as _json
    import pytest
    with pytest.raises(ValueError, match="missing release_id or manifests_uri"):
        candidate_consumer.message_handler({b"payload": _json.dumps({"release_id": "x"}).encode()})
