"""Unit tests for validation_runner.load_candidate_sql.

No database or localstack required — boto3 is patched with a MagicMock.
"""
from unittest.mock import MagicMock, patch

import pytest

from base.validation_runner import load_candidate_sql, _parse_s3_uri, main


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
