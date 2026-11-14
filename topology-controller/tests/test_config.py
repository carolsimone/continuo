import logging
import os
import pytest


def test_validate_raises_listing_all_missing(monkeypatch):
    """validate() raises RuntimeError naming every missing required var."""
    required = [
        "REDIS_URL", "REDIS_PASSWORD",
        "S3_ENDPOINT_URL", "S3_BUCKET", "S3_ENV", "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
    ]
    for key in required:
        monkeypatch.delenv(key, raising=False)
        monkeypatch.delenv(f"{key}_FILE", raising=False)

    from config.config import validate
    with pytest.raises(RuntimeError) as exc_info:
        validate()

    msg = str(exc_info.value)
    for key in required:
        assert key in msg, f"expected {key} in error message, got: {msg}"


def test_validate_passes_when_all_required_set(monkeypatch):
    """validate() does not raise when all required vars are present."""
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379")
    monkeypatch.setenv("REDIS_PASSWORD", "redis-pw")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BUCKET", "continuo")
    monkeypatch.setenv("S3_ENV", "local")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

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
    monkeypatch.setenv("REDIS_PASSWORD", "redis-pw")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BUCKET", "continuo")
    monkeypatch.setenv("S3_ENV", "local")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
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


def test_system_relations_cover_exactly_the_supported_engines():
    """Every engine with a dialect also has a system-relation list, and the
    chart accepts no other engine, so an unknown-external table is never judged against
    another engine's catalog."""
    from config.config import _ENGINE_DIALECTS, _SYSTEM_RELATIONS
    assert set(_SYSTEM_RELATIONS) == set(_ENGINE_DIALECTS) == {"postgres", "trino"}


def test_an_unknown_engine_has_no_system_relations(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_ENGINE", "duckdb")
    from config.config import warehouse_system_relations
    with pytest.raises(RuntimeError, match="unsupported WAREHOUSE_ENGINE"):
        warehouse_system_relations()


@pytest.mark.parametrize("engine", ["postgres", "trino"])
def test_system_relations_for_returns_the_engines_own_entry(engine):
    """The lookup returns the entry of the engine it is asked for, not another
    engine's, whatever the environment says."""
    from config.config import _SYSTEM_RELATIONS, system_relations_for
    assert system_relations_for(engine) is _SYSTEM_RELATIONS[engine]


def test_system_relations_for_an_unknown_engine_names_it():
    from config.config import system_relations_for
    with pytest.raises(RuntimeError, match="unsupported WAREHOUSE_ENGINE 'duckdb'"):
        system_relations_for("duckdb")


@pytest.mark.parametrize("raw", [None, ""])
def test_system_relations_default_to_postgres(monkeypatch, raw):
    """An unset or empty engine reads as postgres, as warehouse_dialect does."""
    if raw is None:
        monkeypatch.delenv("WAREHOUSE_ENGINE", raising=False)
    else:
        monkeypatch.setenv("WAREHOUSE_ENGINE", raw)
    from config.config import _SYSTEM_RELATIONS, warehouse_system_relations
    assert warehouse_system_relations() is _SYSTEM_RELATIONS["postgres"]


@pytest.mark.parametrize("engine", ["postgres", "trino"])
def test_system_relations_follow_the_configured_engine(monkeypatch, engine):
    monkeypatch.setenv("WAREHOUSE_ENGINE", engine)
    from config.config import _SYSTEM_RELATIONS, warehouse_system_relations
    assert warehouse_system_relations() is _SYSTEM_RELATIONS[engine]


def test_validate_requires_system_relations_for_the_configured_engine(monkeypatch):
    """validate() resolves the system relations, so an engine that has a
    dialect but no system-relation list stops the service at startup."""
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379")
    monkeypatch.setenv("REDIS_PASSWORD", "redis-pw")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BUCKET", "continuo")
    monkeypatch.setenv("S3_ENV", "local")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("WAREHOUSE_ENGINE", "trino")

    from config import config
    monkeypatch.setattr(
        config, "_SYSTEM_RELATIONS", {"postgres": config._SYSTEM_RELATIONS["postgres"]},
    )
    with pytest.raises(RuntimeError, match="unsupported WAREHOUSE_ENGINE 'trino'"):
        config.validate()


def test_release_requested_stream_constant_sourced_from_contract():
    from streams_contract import (
        RELEASE_REQUESTED_V1,
        TOPOLOGY_CONTROLLER_RELEASE_REQUESTED,
        MANIFEST_LOADED_CANDIDATE_V2,
    )
    from config.config import (
        RELEASE_REQUESTED_STREAM,
        RELEASE_REQUESTED_GROUP,
        MANIFEST_LOADED_CANDIDATE_STREAM,
    )
    assert RELEASE_REQUESTED_STREAM == RELEASE_REQUESTED_V1
    assert RELEASE_REQUESTED_GROUP == TOPOLOGY_CONTROLLER_RELEASE_REQUESTED
    assert MANIFEST_LOADED_CANDIDATE_STREAM == MANIFEST_LOADED_CANDIDATE_V2


def test_every_failure_kind_the_handler_can_publish_is_in_the_contract():
    """The handler names kinds by enum member, so an undeclared kind cannot
    compile; this pins the members it relies on to the generated contract so
    a regenerated vocabulary that drops one fails here, not in production."""
    import re
    from pathlib import Path
    from domain.contract_vocabulary import ParseFailureKind

    src = (Path(__file__).parent.parent / "service" / "candidate_manifest_handler.py").read_text()
    used = set(re.findall(r"ParseFailureKind\.([A-Z_]+)", src))
    assert used <= {m.name for m in ParseFailureKind}
    assert {"INVALID_ARTIFACT", "INTERNAL"} <= used


@pytest.mark.parametrize("raw,level", [
    ("", logging.INFO),
    ("debug", logging.DEBUG),
    ("DEBUG", logging.DEBUG),
    ("Info", logging.INFO),
    ("warn", logging.WARNING),
    ("WARNING", logging.WARNING),
    ("error", logging.ERROR),
])
def test_log_level_maps_every_accepted_name(monkeypatch, raw, level):
    """LOG_LEVEL names a level case-insensitively; empty means INFO."""
    monkeypatch.setenv("LOG_LEVEL", raw)
    from config.config import log_level
    assert log_level() == level


def test_log_level_defaults_to_info_when_unset(monkeypatch):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    from config.config import log_level
    assert log_level() == logging.INFO


def test_log_level_rejects_an_unknown_name(monkeypatch):
    """An unknown level fails loudly, naming every accepted one."""
    monkeypatch.setenv("LOG_LEVEL", "verbose")
    from config.config import log_level
    with pytest.raises(RuntimeError) as exc_info:
        log_level()
    msg = str(exc_info.value)
    assert "verbose" in msg
    for name in ("debug", "info", "warn", "warning", "error"):
        assert name in msg


def test_validate_rejects_an_unknown_log_level(monkeypatch):
    """The unknown-level failure lands at startup, via validate()."""
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379")
    monkeypatch.setenv("REDIS_PASSWORD", "redis-pw")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BUCKET", "continuo")
    monkeypatch.setenv("S3_ENV", "local")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("LOG_LEVEL", "verbose")

    from config.config import validate
    with pytest.raises(RuntimeError, match="verbose"):
        validate()


def _set_non_credential_env(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BUCKET", "continuo")
    monkeypatch.setenv("S3_ENV", "local")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def test_validate_accepts_password_files(monkeypatch, tmp_path):
    """A credential given only as <NAME>_FILE counts as present."""
    _set_non_credential_env(monkeypatch)
    for name in ("REDIS_PASSWORD", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        path = tmp_path / name
        path.write_text("value\n")
        monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv(f"{name}_FILE", str(path))

    from config.config import validate
    validate()  # must not raise


def test_validate_names_a_credential_with_neither_form(monkeypatch, tmp_path):
    _set_non_credential_env(monkeypatch)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.delenv("REDIS_PASSWORD", raising=False)
    monkeypatch.delenv("REDIS_PASSWORD_FILE", raising=False)

    from config.config import validate
    with pytest.raises(RuntimeError, match="REDIS_PASSWORD"):
        validate()


@pytest.mark.parametrize("url", [
    "redis://user:pw@redis:6379",
    "redis://:pw@redis:6379/0",
])
def test_validate_rejects_a_url_with_credentials(monkeypatch, url):
    _set_non_credential_env(monkeypatch)
    monkeypatch.setenv("REDIS_PASSWORD", "redis-pw")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("REDIS_URL", url)

    from config.config import validate
    with pytest.raises(RuntimeError) as exc_info:
        validate()
    assert str(exc_info.value) == (
        "REDIS_URL must not carry credentials; set REDIS_USERNAME and REDIS_PASSWORD"
    )


def test_redis_acl_mode_defaults_to_shared(monkeypatch):
    monkeypatch.delenv("REDIS_ACL_MODE", raising=False)
    from config.config import REDIS_ACL_MODE_SHARED, redis_acl_mode
    assert redis_acl_mode() == REDIS_ACL_MODE_SHARED


def test_redis_acl_mode_reads_per_service(monkeypatch):
    monkeypatch.setenv("REDIS_ACL_MODE", "perService")
    from config.config import REDIS_ACL_MODE_PER_SERVICE, redis_acl_mode
    assert redis_acl_mode() == REDIS_ACL_MODE_PER_SERVICE


def test_validate_rejects_an_unknown_redis_acl_mode(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379")
    monkeypatch.setenv("REDIS_PASSWORD", "redis-pw")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BUCKET", "continuo")
    monkeypatch.setenv("S3_ENV", "local")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("REDIS_ACL_MODE", "perservice")

    from config.config import validate
    with pytest.raises(RuntimeError, match="REDIS_ACL_MODE 'perservice'"):
        validate()


def test_metrics_port_defaults_to_9464(monkeypatch):
    monkeypatch.delenv("METRICS_PORT", raising=False)
    from config.config import metrics_port
    assert metrics_port() == 9464


def test_metrics_port_empty_string_falls_back_to_the_default(monkeypatch):
    """A rendered-but-empty ConfigMap value is treated as unset."""
    monkeypatch.setenv("METRICS_PORT", "")
    from config.config import metrics_port
    assert metrics_port() == 9464


def test_metrics_port_reads_the_variable(monkeypatch):
    monkeypatch.setenv("METRICS_PORT", "9465")
    from config.config import metrics_port
    assert metrics_port() == 9465


@pytest.mark.parametrize("raw", ["0", "65536", "-1", "abc", "9_464", " 9464", "94.64", "٩٤٦٤"])
def test_metrics_port_refuses_anything_but_a_whole_number_from_1_to_65535(monkeypatch, raw):
    monkeypatch.setenv("METRICS_PORT", raw)
    from config.config import metrics_port
    with pytest.raises(RuntimeError, match="METRICS_PORT"):
        metrics_port()


def test_validate_refuses_a_bad_metrics_port(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379")
    monkeypatch.setenv("REDIS_PASSWORD", "redis-pw")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BUCKET", "continuo")
    monkeypatch.setenv("S3_ENV", "local")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("METRICS_PORT", "abc")

    from config.config import validate
    with pytest.raises(RuntimeError, match="METRICS_PORT"):
        validate()
