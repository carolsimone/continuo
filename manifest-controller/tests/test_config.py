import os
import pytest


def test_validate_raises_listing_all_missing(monkeypatch):
    """validate() raises RuntimeError naming every missing Tier 1 var."""
    tier1 = [
        "REDIS_URL",
        "REGISTRY_PATH", "MANIFESTS_BASE",
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
    monkeypatch.setenv("REGISTRY_PATH", "/data/registry.csv")
    monkeypatch.setenv("MANIFESTS_BASE", "/manifests")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localstack:4566")
    monkeypatch.setenv("S3_BUCKET", "continuo")
    monkeypatch.setenv("S3_ENV", "local")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    from config.config import validate
    validate()  # must not raise


def test_release_requested_stream_constant_sourced_from_contract():
    from streams_contract import (
        RELEASE_REQUESTED_V1,
        MANIFEST_CONTROLLER_RELEASE_REQUESTED,
        MANIFEST_LOADED_CANDIDATE_V1,
    )
    from config.config import (
        RELEASE_REQUESTED_STREAM,
        RELEASE_REQUESTED_GROUP,
        MANIFEST_LOADED_CANDIDATE_STREAM,
    )
    assert RELEASE_REQUESTED_STREAM == RELEASE_REQUESTED_V1
    assert RELEASE_REQUESTED_GROUP == MANIFEST_CONTROLLER_RELEASE_REQUESTED
    assert MANIFEST_LOADED_CANDIDATE_STREAM == MANIFEST_LOADED_CANDIDATE_V1
