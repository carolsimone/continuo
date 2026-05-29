import json
import sys
from types import SimpleNamespace
import main


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
    monkeypatch.setattr(main, "REGISTRY_PATH", "/tmp/registry.csv")
    monkeypatch.setattr(main, "MANIFESTS_BASE", "/tmp/manifests")
    monkeypatch.setattr(main, "S3_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setattr(main, "S3_BUCKET", "continuo")
    monkeypatch.setattr(main, "S3_ENV", "local")
    monkeypatch.setattr(main, "REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(main, "MANIFEST_LOADED_STREAM", "manifest.loaded:v1")
    monkeypatch.setattr(main, "REDIS_STREAM", "update.graph:v1")
    monkeypatch.setattr(main, "REDIS_GROUP", "manifest-controller-update-graph")
    monkeypatch.setattr(main, "RELEASE_REQUESTED_STREAM", "release.requested:v1")
    monkeypatch.setattr(main, "RELEASE_REQUESTED_GROUP", "manifest-controller-release-requested")
    monkeypatch.setattr(main, "MANIFEST_LOADED_CANDIDATE_STREAM", "manifest.loaded.candidate:v1")
    monkeypatch.setattr(main, "FilesystemRegistryRepository", lambda path: object())
    monkeypatch.setattr(main, "ManifestLoadedPublisher", lambda *a, **kw: object())
    monkeypatch.setattr(main, "CandidateManifestPublisher", lambda *a, **kw: object())
    monkeypatch.setattr(main.redis, "from_url", lambda *a, **kw: object())
    monkeypatch.setitem(
        sys.modules, "boto3",
        SimpleNamespace(client=lambda *a, **kw: object()),
    )
    _RecordingConsumer.instances = []
    monkeypatch.setattr(main, "Consumer", _RecordingConsumer)
    monkeypatch.setattr(main.threading, "Thread", _NoopThread)


def test_main_starts_two_consumers(monkeypatch):
    _common_monkeypatches(monkeypatch)
    main.main()
    streams = sorted(c.stream_name for c in _RecordingConsumer.instances)
    assert streams == ["release.requested:v1", "update.graph:v1"]


def test_main_consumer_groups_correct(monkeypatch):
    _common_monkeypatches(monkeypatch)
    main.main()
    by_stream = {c.stream_name: c.group_name for c in _RecordingConsumer.instances}
    assert by_stream["update.graph:v1"] == "manifest-controller-update-graph"
    assert by_stream["release.requested:v1"] == "manifest-controller-release-requested"


def test_main_update_graph_handler_dispatches_on_source_local(monkeypatch):
    _common_monkeypatches(monkeypatch)
    seen = {}

    class FakeManifestHandler:
        def __init__(self, source, manifest_publisher, registry_repo):
            seen["source_obj"] = source

        def handle(self):
            seen["handle"] = True

    monkeypatch.setattr(main, "ManifestHandler", FakeManifestHandler)

    class FakeLocal:
        def cleanup(self):
            seen["local_cleaned"] = True

    class FakeS3:
        def __init__(self, **kw):
            self.kw = kw

        def cleanup(self):
            seen["s3_cleaned"] = True

    monkeypatch.setattr(main, "LocalFilesystemSource", lambda base: FakeLocal())
    monkeypatch.setattr(main, "S3Source", lambda **kw: FakeS3(**kw))

    main.main()
    update_graph_consumer = next(
        c for c in _RecordingConsumer.instances if c.stream_name == "update.graph:v1"
    )
    update_graph_consumer.message_handler({b"source": b"local"})
    assert seen.get("handle") is True
    assert seen.get("local_cleaned") is True


def test_main_update_graph_handler_rejects_unknown_source(monkeypatch):
    _common_monkeypatches(monkeypatch)
    monkeypatch.setattr(main, "ManifestHandler", lambda **kw: SimpleNamespace(handle=lambda: None))
    monkeypatch.setattr(main, "LocalFilesystemSource", lambda base: SimpleNamespace(cleanup=lambda: None))
    monkeypatch.setattr(main, "S3Source", lambda **kw: SimpleNamespace(cleanup=lambda: None))

    main.main()
    update_graph_consumer = next(
        c for c in _RecordingConsumer.instances if c.stream_name == "update.graph:v1"
    )
    import pytest
    with pytest.raises(ValueError, match="unknown source"):
        update_graph_consumer.message_handler({b"source": b"mars"})


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
        c for c in _RecordingConsumer.instances if c.stream_name == "release.requested:v1"
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
        c for c in _RecordingConsumer.instances if c.stream_name == "release.requested:v1"
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
        c for c in _RecordingConsumer.instances if c.stream_name == "release.requested:v1"
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
        c for c in _RecordingConsumer.instances if c.stream_name == "release.requested:v1"
    )
    import json as _json
    import pytest
    with pytest.raises(ValueError, match="missing release_id or manifests_uri"):
        candidate_consumer.message_handler({b"payload": _json.dumps({"release_id": "x"}).encode()})
