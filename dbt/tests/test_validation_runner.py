"""Unit tests for validation_runner.load_candidate_sql.

No database or S3 access required — load_candidate_sql reads a local file,
and warehouse calls are patched with MagicMock.
"""
import json
from unittest.mock import MagicMock, patch

import pytest

from base import validation_result
from base.validation_runner import load_candidate_sql, main


# ---------------------------------------------------------------------------
# load_candidate_sql: reads from local file at CANDIDATE_SQL_PATH
# ---------------------------------------------------------------------------


def test_load_candidate_sql_reads_local_file(tmp_path, monkeypatch):
    p = tmp_path / "candidate.sql"
    p.write_text("select 1")
    monkeypatch.setenv("CANDIDATE_SQL_PATH", str(p))
    assert load_candidate_sql() == "select 1"


def test_load_candidate_sql_empty_when_path_unset(monkeypatch):
    monkeypatch.delenv("CANDIDATE_SQL_PATH", raising=False)
    assert load_candidate_sql() == ""


def test_load_candidate_sql_empty_when_file_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("CANDIDATE_SQL_PATH", str(tmp_path / "nope.sql"))
    assert load_candidate_sql() == ""


# ---------------------------------------------------------------------------
# main: a missing CANDIDATE_SQL_PATH file for a model/snapshot node is a
# validation error (exit != 0)
# ---------------------------------------------------------------------------


def test_main_missing_path_fails_validation(monkeypatch):
    """A model/snapshot node with no CANDIDATE_SQL_PATH file must fail (non-zero exit),
    not silently report itself validated. No S3 call and no DB connection occur."""
    monkeypatch.setenv("DBT_TARGET_SCHEMA", "_candidate_r")
    monkeypatch.setenv("TABLE_NAME", "orders")
    monkeypatch.delenv("CANDIDATE_SQL_PATH", raising=False)

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code != 0


# ---------------------------------------------------------------------------
# main() dispatch on VALIDATION_OP
# ---------------------------------------------------------------------------


def _set_pg_env(monkeypatch):
    for k, v in {
        "DBT_TARGET_SCHEMA": "_candidate_relA", "TABLE_NAME": "orders",
        "DBT_POSTGRES_HOST": "h", "DBT_POSTGRES_DB": "d", "DBT_POSTGRES_USER": "u",
    }.items():
        monkeypatch.setenv(k, v)


def _emitted_doc(out):
    block = out[out.index(validation_result.SENTINEL_BEGIN):]
    return json.loads(block.splitlines()[1])


def test_main_build_from_sql_calls_adapter_and_emits_success(monkeypatch, capsys):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    adapter = MagicMock()
    with patch("base.validation_runner.adapter_from_env", return_value=adapter), \
         patch("base.validation_runner.load_candidate_sql", return_value="SELECT 1 AS id"):
        main()
    adapter.ensure_schema.assert_called_once_with("_candidate_relA")
    adapter.build_empty_from_sql.assert_called_once_with("_candidate_relA", "orders", "SELECT 1 AS id")
    adapter.clone_empty_from_prod.assert_not_called()
    adapter.close.assert_called_once()
    out = capsys.readouterr().out
    assert validation_result.SENTINEL_BEGIN in out and '"status":"success"' in out


def test_main_build_from_sql_missing_candidate_sql_errors(monkeypatch, capsys):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    with patch("base.validation_runner.load_candidate_sql", return_value=""):
        with pytest.raises(SystemExit) as exc:
            main()
    assert exc.value.code == 2
    assert '"status":"error"' in capsys.readouterr().out


def test_main_clone_from_prod_calls_adapter(monkeypatch, capsys):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "clone_from_prod")
    monkeypatch.setenv("PROD_SCHEMA", "analytics")
    adapter = MagicMock()
    with patch("base.validation_runner.adapter_from_env", return_value=adapter):
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
