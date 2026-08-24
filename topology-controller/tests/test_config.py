import os
import pytest


def test_validate_raises_listing_all_missing(monkeypatch):
    """validate() raises RuntimeError naming every missing required var."""
    required = [
        "REDIS_URL",
        "S3_ENDPOINT_URL", "S3_BUCKET", "S3_ENV", "AWS_DEFAULT_REGION",
    ]
    for key in required:
        monkeypatch.delenv(key, raising=False)

    from config.config import validate
    with pytest.raises(RuntimeError) as exc_info:
        validate()

    msg = str(exc_info.value)
    for key in required:
        assert key in msg, f"expected {key} in error message, got: {msg}"


def test_validate_passes_when_all_required_set(monkeypatch):
    """validate() does not raise when all required vars are present."""
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BUCKET", "continuo")
    monkeypatch.setenv("S3_ENV", "local")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    from config.config import validate
    validate()  # must not raise


def test_warehouse_dialect_defaults_to_postgres(monkeypatch):
    """An install that never set an engine keeps running postgres."""
    monkeypatch.delenv("WAREHOUSE_ENGINE", raising=False)
    from config.config import warehouse_dialect
    assert warehouse_dialect() == "postgres"


def test_warehouse_dialect_empty_string_falls_back_to_postgres(monkeypatch):
    """A rendered-but-empty ConfigMap value is treated as unset, not as an engine."""
    monkeypatch.setenv("WAREHOUSE_ENGINE", "")
    from config.config import warehouse_dialect
    assert warehouse_dialect() == "postgres"


@pytest.mark.parametrize("engine,dialect", [("postgres", "postgres"), ("trino", "trino")])
def test_warehouse_dialect_maps_supported_engines(monkeypatch, engine, dialect):
    """Every engine the chart allows in validation.engine maps to a dialect."""
    monkeypatch.setenv("WAREHOUSE_ENGINE", engine)
    from config.config import warehouse_dialect
    assert warehouse_dialect() == dialect


def test_warehouse_dialect_rejects_unknown_engine(monkeypatch):
    """An engine with no dialect mapping fails loudly.

    Falling back to postgres would emit postgres SQL to a warehouse that is not
    postgres, which fails later and further from the cause.
    """
    monkeypatch.setenv("WAREHOUSE_ENGINE", "duckdb")
    from config.config import warehouse_dialect
    with pytest.raises(RuntimeError) as exc_info:
        warehouse_dialect()
    msg = str(exc_info.value)
    assert "duckdb" in msg
    assert "postgres" in msg and "trino" in msg


def test_validate_rejects_unsupported_engine(monkeypatch):
    """The unsupported-engine failure lands at startup, via validate()."""
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BUCKET", "continuo")
    monkeypatch.setenv("S3_ENV", "local")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("WAREHOUSE_ENGINE", "duckdb")

    from config.config import validate
    with pytest.raises(RuntimeError, match="duckdb"):
        validate()


def test_supported_engines_match_the_chart(monkeypatch):
    """The engines this service maps are exactly the ones the chart accepts.

    templates/_helpers.tpl fails an install whose validation.engine is not in
    its supported list; an engine the chart allows but this service cannot map
    would render fine and then crash topology-controller at boot.
    """
    from config.config import _ENGINE_DIALECTS
    assert set(_ENGINE_DIALECTS) == {"postgres", "trino"}


def test_release_requested_stream_constant_sourced_from_contract():
    from streams_contract import (
        RELEASE_REQUESTED_V1,
        TOPOLOGY_CONTROLLER_RELEASE_REQUESTED,
        MANIFEST_LOADED_CANDIDATE_V1,
    )
    from config.config import (
        RELEASE_REQUESTED_STREAM,
        RELEASE_REQUESTED_GROUP,
        MANIFEST_LOADED_CANDIDATE_STREAM,
    )
    assert RELEASE_REQUESTED_STREAM == RELEASE_REQUESTED_V1
    assert RELEASE_REQUESTED_GROUP == TOPOLOGY_CONTROLLER_RELEASE_REQUESTED
    assert MANIFEST_LOADED_CANDIDATE_STREAM == MANIFEST_LOADED_CANDIDATE_V1
