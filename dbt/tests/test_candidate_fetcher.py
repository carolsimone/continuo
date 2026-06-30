"""Unit tests for candidate_fetcher.main — the validation Job's S3 download sidecar.

No S3 access required — boto3 is patched with a MagicMock.
"""
from unittest import mock

import pytest

import candidate_fetcher  # pythonpath="s3-sidecar" in pyproject


def test_downloads_object_to_local_path(tmp_path, monkeypatch):
    dest = tmp_path / "candidate.sql"
    monkeypatch.setenv("CANDIDATE_SQL_URI", "s3://continuo/candidate-sql/rel-1/svc.orders.sql")
    monkeypatch.setenv("CANDIDATE_SQL_PATH", str(dest))
    fake = mock.MagicMock()
    fake.get_object.return_value = {"Body": mock.MagicMock(read=lambda: b"select 1")}
    monkeypatch.setattr(candidate_fetcher.boto3, "client", lambda *a, **k: fake)

    candidate_fetcher.main()

    _, kwargs = fake.get_object.call_args
    assert kwargs["Bucket"] == "continuo"
    assert kwargs["Key"] == "candidate-sql/rel-1/svc.orders.sql"
    assert dest.read_bytes() == b"select 1"


def test_missing_uri_exits_nonzero(monkeypatch, tmp_path):
    monkeypatch.delenv("CANDIDATE_SQL_URI", raising=False)
    monkeypatch.setenv("CANDIDATE_SQL_PATH", str(tmp_path / "candidate.sql"))
    with pytest.raises(SystemExit) as e:
        candidate_fetcher.main()
    assert e.value.code != 0


def test_missing_path_exits_nonzero(monkeypatch):
    monkeypatch.setenv("CANDIDATE_SQL_URI", "s3://continuo/k.sql")
    monkeypatch.delenv("CANDIDATE_SQL_PATH", raising=False)
    with pytest.raises(SystemExit) as e:
        candidate_fetcher.main()
    assert e.value.code != 0


def test_malformed_uri_exits_nonzero(monkeypatch, tmp_path):
    monkeypatch.setenv("CANDIDATE_SQL_URI", "not-an-s3-uri")
    monkeypatch.setenv("CANDIDATE_SQL_PATH", str(tmp_path / "candidate.sql"))
    with pytest.raises(SystemExit) as e:
        candidate_fetcher.main()
    assert e.value.code != 0


def test_get_object_failure_exits_nonzero(monkeypatch, tmp_path):
    monkeypatch.setenv("CANDIDATE_SQL_URI", "s3://continuo/k.sql")
    monkeypatch.setenv("CANDIDATE_SQL_PATH", str(tmp_path / "candidate.sql"))
    fake = mock.MagicMock()
    fake.get_object.side_effect = RuntimeError("connection refused")
    monkeypatch.setattr(candidate_fetcher.boto3, "client", lambda *a, **k: fake)
    with pytest.raises(SystemExit) as e:
        candidate_fetcher.main()
    assert e.value.code != 0
