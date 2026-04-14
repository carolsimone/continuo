import os
import pytest


def test_validate_raises_listing_all_missing(monkeypatch):
    """validate() raises RuntimeError naming every missing Tier 1 var."""
    tier1 = [
        "REDIS_URL", "REDIS_STREAM", "REDIS_GROUP",
        "GRAPH_GRPC_ADDR", "REGISTRY_PATH", "MANIFESTS_BASE",
        "S3_ENDPOINT_URL", "S3_BUCKET", "S3_ENV", "AWS_DEFAULT_REGION",
    ]
    for key in tier1:
        monkeypatch.delenv(key, raising=False)

    from config.config import validate
    with pytest.raises(RuntimeError) as exc_info:
        validate()

    msg = str(exc_info.value)
    for key in tier1:
        assert key in msg, f"expected {key} in error message, got: {msg}"


def test_validate_passes_when_all_required_set(monkeypatch):
    """validate() does not raise when all Tier 1 vars are present."""
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379")
    monkeypatch.setenv("REDIS_STREAM", "update.graph:v1")
    monkeypatch.setenv("REDIS_GROUP", "manifest-controller")
    monkeypatch.setenv("GRAPH_GRPC_ADDR", "graph:50052")
    monkeypatch.setenv("REGISTRY_PATH", "/data/registry.csv")
    monkeypatch.setenv("MANIFESTS_BASE", "/manifests")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localstack:4566")
    monkeypatch.setenv("S3_BUCKET", "continuo")
    monkeypatch.setenv("S3_ENV", "local")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    from config.config import validate
    validate()  # must not raise
