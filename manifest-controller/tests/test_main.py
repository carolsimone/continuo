import sys
from types import SimpleNamespace

import main


class _FakeSource:
    def __init__(self) -> None:
        self.cleaned_up = False

    def cleanup(self) -> None:
        self.cleaned_up = True


class _FakeManifestHandler:
    def __init__(self, source, graph_client, registry_repo) -> None:
        self.source = source
        self.graph_client = graph_client
        self.registry_repo = registry_repo

    def handle(self):
        return ["daily"], {"service-a": "v3"}


class _FakePublisher:
    def __init__(self, redis_client, stream_name: str) -> None:
        self.calls = []

    def publish(self, event_id: str, schedule_names: list[str], manifest_versions: dict[str, str]) -> None:
        self.calls.append((event_id, schedule_names, manifest_versions))


class _FakeConsumer:
    def __init__(self, redis_client, stream_name: str, group_name: str, handler_factory) -> None:
        self.handler_factory = handler_factory

    def start(self) -> None:
        self.handler_factory("local")


def test_main_propagates_manifest_versions_to_publisher(monkeypatch):
    fake_source = _FakeSource()
    fake_publisher = _FakePublisher(None, "schedules.loaded:v1")

    monkeypatch.setattr(main, "validate", lambda: None)
    monkeypatch.setattr(main, "GRAPH_GRPC_ADDR", "localhost:9000")
    monkeypatch.setattr(main, "REGISTRY_PATH", "/tmp/registry.csv")
    monkeypatch.setattr(main, "MANIFESTS_BASE", "/tmp/manifests")
    monkeypatch.setattr(main, "S3_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setattr(main, "S3_BUCKET", "bucket")
    monkeypatch.setattr(main, "S3_ENV", "local")
    monkeypatch.setattr(main, "REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(main, "REDIS_STREAM", "trigger")
    monkeypatch.setattr(main, "REDIS_GROUP", "group")
    monkeypatch.setattr(main, "SCHEDULES_LOADED_STREAM", "schedules.loaded:v1")
    monkeypatch.setattr(main, "GraphClient", lambda address: object())
    monkeypatch.setattr(main, "FilesystemRegistryRepository", lambda path: object())
    monkeypatch.setattr(main, "LocalFilesystemSource", lambda base_path: fake_source)
    monkeypatch.setattr(main, "S3Source", lambda **kwargs: fake_source)
    monkeypatch.setattr(main, "ManifestHandler", _FakeManifestHandler)
    monkeypatch.setattr(main, "SchedulesLoadedPublisher", lambda redis_client, stream_name: fake_publisher)
    monkeypatch.setattr(main, "Consumer", _FakeConsumer)
    monkeypatch.setattr(main.redis, "from_url", lambda *args, **kwargs: object())
    monkeypatch.setattr(main.uuid, "uuid4", lambda: "evt-1")
    monkeypatch.setitem(
        sys.modules,
        "boto3",
        SimpleNamespace(client=lambda *args, **kwargs: object()),
    )

    main.main()

    assert fake_publisher.calls == [("evt-1", ["daily"], {"service-a": "v3"})]
    assert fake_source.cleaned_up is True
