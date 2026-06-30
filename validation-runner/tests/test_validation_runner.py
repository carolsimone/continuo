"""Unit tests for validation_runner — inline S3 fetch, flat imports, mocked boto3/DB."""
import json
from unittest.mock import MagicMock, patch

import pytest

import validation_result
from validation_runner import load_candidate_sql, main


def _mock_s3_body(payload: bytes):
    """Return a MagicMock s3 client whose get_object().['Body'].read() yields payload."""
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=payload))}
    return s3


# --- load_candidate_sql: fetches from S3 (CANDIDATE_SQL_URI), no local file ---

def test_load_candidate_sql_empty_when_uri_unset(monkeypatch):
    monkeypatch.delenv("CANDIDATE_SQL_URI", raising=False)
    assert load_candidate_sql() == ""


def test_load_candidate_sql_fetches_and_decodes_utf8(monkeypatch):
    monkeypatch.setenv("CANDIDATE_SQL_URI", "s3://continuo/candidate-sql/rel-1/svc.orders.sql")
    s3 = _mock_s3_body(b"  select 2  \n")
    with patch("validation_runner.s3_common.make_s3_client", return_value=s3):
        assert load_candidate_sql() == "  select 2  \n"
    s3.get_object.assert_called_once_with(Bucket="continuo", Key="candidate-sql/rel-1/svc.orders.sql")


def test_load_candidate_sql_raises_on_bad_uri(monkeypatch):
    monkeypatch.setenv("CANDIDATE_SQL_URI", "not-an-s3-uri")
    with pytest.raises(ValueError):
        load_candidate_sql()


# --- main(): dispatch on VALIDATION_OP (unchanged contract) ---

def _set_pg_env(monkeypatch):
    for k, v in {
        "DBT_TARGET_SCHEMA": "_candidate_relA", "TABLE_NAME": "orders",
        "DBT_POSTGRES_HOST": "h", "DBT_POSTGRES_DB": "d", "DBT_POSTGRES_USER": "u",
    }.items():
        monkeypatch.setenv(k, v)


def test_main_build_from_sql_calls_adapter_and_emits_success(monkeypatch, capsys):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    adapter = MagicMock()
    with patch("validation_runner.adapter_from_env", return_value=adapter), \
         patch("validation_runner.load_candidate_sql", return_value="SELECT 1 AS id"):
        main()
    adapter.ensure_schema.assert_called_once_with("_candidate_relA")
    adapter.build_empty_from_sql.assert_called_once_with("_candidate_relA", "orders", "SELECT 1 AS id")
    adapter.clone_empty_from_prod.assert_not_called()
    adapter.close.assert_called_once()
    out = capsys.readouterr().out
    assert validation_result.SENTINEL_BEGIN in out and '"status":"success"' in out


def test_main_build_from_sql_empty_candidate_sql_errors(monkeypatch, capsys):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    with patch("validation_runner.load_candidate_sql", return_value=""):
        with pytest.raises(SystemExit) as exc:
            main()
    assert exc.value.code == 2
    assert '"status":"error"' in capsys.readouterr().out


def test_main_build_from_sql_s3_error_emits_error_block(monkeypatch, capsys):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    with patch("validation_runner.load_candidate_sql", side_effect=RuntimeError("S3 down")):
        with pytest.raises(SystemExit) as exc:
            main()
    assert exc.value.code == 1
    assert '"status":"error"' in capsys.readouterr().out


def test_main_clone_from_prod_calls_adapter(monkeypatch, capsys):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "clone_from_prod")
    monkeypatch.setenv("PROD_SCHEMA", "analytics")
    adapter = MagicMock()
    with patch("validation_runner.adapter_from_env", return_value=adapter):
        main()
    adapter.ensure_schema.assert_called_once_with("_candidate_relA")
    adapter.clone_empty_from_prod.assert_called_once_with("_candidate_relA", "analytics", "orders")
    adapter.build_empty_from_sql.assert_not_called()
    assert '"status":"success"' in capsys.readouterr().out


def test_main_clone_from_prod_missing_prod_schema_exits(monkeypatch):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "clone_from_prod")
    monkeypatch.delenv("PROD_SCHEMA", raising=False)
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
