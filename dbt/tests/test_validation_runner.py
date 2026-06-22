"""Unit tests for validation_runner.load_candidate_sql.

No database or localstack required — boto3 is patched with a MagicMock.
"""
from unittest.mock import MagicMock, patch

import pytest

from psycopg2 import errors as pg_errors

from base.validation_runner import load_candidate_sql, _parse_s3_uri, _ensure_schema, main


# ---------------------------------------------------------------------------
# _parse_s3_uri helper
# ---------------------------------------------------------------------------


def test_parse_s3_uri_basic():
    bucket, key = _parse_s3_uri("s3://my-bucket/path/to/file.sql")
    assert bucket == "my-bucket"
    assert key == "path/to/file.sql"


def test_parse_s3_uri_nested_key():
    bucket, key = _parse_s3_uri("s3://bucket/a/b/c.sql")
    assert bucket == "bucket"
    assert key == "a/b/c.sql"


def test_parse_s3_uri_rejects_missing_key_bucket_only():
    """s3://bucket-only (no slash after bucket) must raise ValueError."""
    with pytest.raises(ValueError, match="missing bucket or key"):
        _parse_s3_uri("s3://bucket-only")


def test_parse_s3_uri_rejects_empty_key():
    """s3://bucket/ (slash but empty key) must raise ValueError."""
    with pytest.raises(ValueError, match="missing bucket or key"):
        _parse_s3_uri("s3://bucket/")


# ---------------------------------------------------------------------------
# load_candidate_sql: non-empty URI fetches from S3
# ---------------------------------------------------------------------------


def test_load_candidate_sql_fetches_from_s3(monkeypatch):
    """With CANDIDATE_SQL_URI set, boto3 is called and the SQL body is returned."""
    monkeypatch.setenv("CANDIDATE_SQL_URI", "s3://continuo/candidate-sql/r/n.sql")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localstack:4566")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    mock_body = MagicMock()
    mock_body.read.return_value = b"SELECT 1"
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": mock_body}

    with patch("base.validation_runner.boto3.client", return_value=mock_s3) as mock_client:
        result = load_candidate_sql()

    assert result == "SELECT 1"
    mock_client.assert_called_once()
    mock_s3.get_object.assert_called_once_with(Bucket="continuo", Key="candidate-sql/r/n.sql")


def test_load_candidate_sql_returns_raw_body_without_stripping(monkeypatch):
    """load_candidate_sql does NOT strip the SQL — caller (main) does that."""
    monkeypatch.setenv("CANDIDATE_SQL_URI", "s3://continuo/key.sql")

    mock_body = MagicMock()
    mock_body.read.return_value = b"  SELECT 2  \n"
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": mock_body}

    with patch("base.validation_runner.boto3.client", return_value=mock_s3):
        result = load_candidate_sql()

    # load_candidate_sql decodes but does NOT strip — that is main()'s job
    assert result == "  SELECT 2  \n"


# ---------------------------------------------------------------------------
# load_candidate_sql: empty/absent URI → no S3 call, returns ""
# ---------------------------------------------------------------------------


def test_load_candidate_sql_no_uri_returns_empty(monkeypatch):
    """When CANDIDATE_SQL_URI is absent, load_candidate_sql returns '' with no S3 call."""
    monkeypatch.delenv("CANDIDATE_SQL_URI", raising=False)

    mock_client = MagicMock()
    with patch("base.validation_runner.boto3.client", mock_client):
        result = load_candidate_sql()

    assert result == ""
    mock_client.assert_not_called()


# ---------------------------------------------------------------------------
# main: a missing URI for a model/snapshot node is a validation error (exit != 0)
# ---------------------------------------------------------------------------


def test_main_missing_uri_fails_validation(monkeypatch):
    """A model/snapshot node with no CANDIDATE_SQL_URI must fail (non-zero exit),
    not silently report itself validated. No S3 call and no DB connection occur."""
    monkeypatch.setenv("DBT_TARGET_SCHEMA", "_candidate_r")
    monkeypatch.setenv("TABLE_NAME", "orders")
    monkeypatch.delenv("CANDIDATE_SQL_URI", raising=False)

    mock_client = MagicMock()
    with patch("base.validation_runner.boto3.client", mock_client):
        with pytest.raises(SystemExit) as exc:
            main()

    assert exc.value.code != 0
    mock_client.assert_not_called()


def test_load_candidate_sql_empty_uri_returns_empty(monkeypatch):
    """When CANDIDATE_SQL_URI is set to an empty string, returns '' with no S3 call."""
    monkeypatch.setenv("CANDIDATE_SQL_URI", "")

    mock_client = MagicMock()
    with patch("base.validation_runner.boto3.client", mock_client):
        result = load_candidate_sql()

    assert result == ""
    mock_client.assert_not_called()


# ---------------------------------------------------------------------------
# _ensure_schema: race-safe candidate-schema creation
# ---------------------------------------------------------------------------


class _FakeCursor:
    """Records every execute() call and optionally raises on the CREATE SCHEMA.

    A statement is the CREATE when it is a psycopg2 Composed (not the advisory
    lock/unlock, which are plain SQL strings).
    """

    def __init__(self, raise_on_create=None):
        self._raise_on_create = raise_on_create
        self.calls = []  # list of statement objects passed to execute()

    def execute(self, statement, params=None):
        self.calls.append(statement)
        # The lock/unlock statements are plain strings; the CREATE is a Composed.
        if self._raise_on_create is not None and not isinstance(statement, str):
            raise self._raise_on_create


def _call_strings(cur):
    """Render each recorded statement as a string for assertions."""
    return [s if isinstance(s, str) else s.__class__.__name__ for s in cur.calls]


def test_ensure_schema_locks_before_create_and_unlocks_after():
    """The candidate schema create is wrapped in advisory lock then unlock."""
    cur = _FakeCursor()

    _ensure_schema(cur, "_candidate_relA")

    rendered = _call_strings(cur)
    # Order: take advisory lock -> create schema (a Composed) -> release lock.
    assert "pg_advisory_lock" in rendered[0]
    assert rendered[1] == "Composed"  # the CREATE SCHEMA statement
    assert "pg_advisory_unlock" in rendered[2]
    assert len(cur.calls) == 3


def test_ensure_schema_tolerates_concurrent_duplicate_schema():
    """A racing creator raising DuplicateSchema is swallowed; the lock is still
    released and the function returns normally (the schema now exists)."""
    cur = _FakeCursor(raise_on_create=pg_errors.DuplicateSchema("already exists"))

    # Must not raise — a concurrent create reaches the desired end state.
    _ensure_schema(cur, "_candidate_relB")

    rendered = _call_strings(cur)
    assert "pg_advisory_lock" in rendered[0]
    assert "pg_advisory_unlock" in rendered[-1], "lock must be released even when create raced"


def test_ensure_schema_tolerates_concurrent_unique_violation():
    """A racing creator raising UniqueViolation on pg_namespace is swallowed and
    the advisory lock is released."""
    cur = _FakeCursor(raise_on_create=pg_errors.UniqueViolation("pg_namespace"))

    _ensure_schema(cur, "_candidate_relC")

    assert "pg_advisory_unlock" in _call_strings(cur)[-1]


def test_ensure_schema_propagates_unexpected_error_but_releases_lock():
    """An error that is NOT a duplicate/unique violation propagates, but the
    advisory lock is still released first (finally block)."""
    cur = _FakeCursor(raise_on_create=RuntimeError("connection reset"))

    with pytest.raises(RuntimeError, match="connection reset"):
        _ensure_schema(cur, "_candidate_relD")

    assert "pg_advisory_unlock" in _call_strings(cur)[-1]
