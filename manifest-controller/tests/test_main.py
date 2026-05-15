import sys
from types import SimpleNamespace

import main


class _FakeSource:
    def __init__(self) -> None:
        self.cleaned_up = False

    def cleanup(self) -> None:
        self.cleaned_up = True


class _FakeManifestHandler:
    def __init__(self, source, manifest_publisher, registry_repo) -> None:
        self.source = source
        self.manifest_publisher = manifest_publisher
        self.registry_repo = registry_repo

    def handle(self):
        pass


class _FakeConsumer:
    def __init__(self, redis_client, stream_name: str, group_name: str, handler_factory) -> None:
        self.handler_factory = handler_factory

    def start(self) -> None:
        self.handler_factory("local")


def test_main_handles_event_and_cleans_up(monkeypatch):
    fake_source = _FakeSource()

    monkeypatch.setattr(main, "validate", lambda: None)
    monkeypatch.setattr(main, "REGISTRY_PATH", "/tmp/registry.csv")
    monkeypatch.setattr(main, "MANIFESTS_BASE", "/tmp/manifests")
    monkeypatch.setattr(main, "S3_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setattr(main, "S3_BUCKET", "bucket")
    monkeypatch.setattr(main, "S3_ENV", "local")
    monkeypatch.setattr(main, "REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(main, "MANIFEST_LOADED_STREAM", "manifest.loaded:v1")
    monkeypatch.setattr(main, "FilesystemRegistryRepository", lambda path: object())
    monkeypatch.setattr(main, "LocalFilesystemSource", lambda base_path: fake_source)
    monkeypatch.setattr(main, "S3Source", lambda **kwargs: fake_source)
    monkeypatch.setattr(main, "ManifestHandler", _FakeManifestHandler)
    monkeypatch.setattr(main, "ManifestLoadedPublisher", lambda redis_client, stream_name: object())
    monkeypatch.setattr(main, "Consumer", _FakeConsumer)
    monkeypatch.setattr(main.redis, "from_url", lambda *args, **kwargs: object())
    monkeypatch.setitem(
        sys.modules,
        "boto3",
        SimpleNamespace(client=lambda *args, **kwargs: object()),
    )

    main.main()

    assert fake_source.cleaned_up is True
