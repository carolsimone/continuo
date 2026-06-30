"""Unit tests for validation_runner — hand-written fakes, no MagicMock/unittest.mock."""
import pytest

import validation_result
import validation_runner
from validation_runner import load_candidate_sql, main
from warehouse import WarehouseAdapter


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeWarehouseAdapter(WarehouseAdapter):
    """Records every adapter call; no live DB required."""

    def __init__(self):
        self.schemas_ensured = []
        self.builds = []    # list of (schema, table, sql)
        self.clones = []    # list of (candidate_schema, prod_schema, table)
        self.closed = False

    def ensure_schema(self, schema: str) -> None:
        self.schemas_ensured.append(schema)

    def build_empty_from_sql(self, schema: str, table: str, compiled_sql: str) -> None:
        self.builds.append((schema, table, compiled_sql))

    def clone_empty_from_prod(self, candidate_schema: str, prod_schema: str, table: str) -> None:
        self.clones.append((candidate_schema, prod_schema, table))

    def close(self) -> None:
        self.closed = True


class _FakeBody:
    """Mimics the S3 streaming body returned inside get_object()["Body"]."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class FakeS3Client:
    """Returns pre-loaded bytes for known (bucket, key) pairs; records calls made."""

    def __init__(self, objects: dict):
        # objects: {(bucket, key): bytes}
        self._objects = objects
        self.calls = []  # list of {"Bucket": ..., "Key": ...}

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        self.calls.append({"Bucket": Bucket, "Key": Key})
        data = self._objects.get((Bucket, Key))
        if data is None:
            raise RuntimeError(f"FakeS3Client: unknown key s3://{Bucket}/{Key}")
        return {"Body": _FakeBody(data)}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _set_pg_env(monkeypatch):
    for k, v in {
        "DBT_TARGET_SCHEMA": "_candidate_relA",
        "TABLE_NAME": "orders",
        "DBT_POSTGRES_HOST": "h",
        "DBT_POSTGRES_DB": "d",
        "DBT_POSTGRES_USER": "u",
    }.items():
        monkeypatch.setenv(k, v)


# ---------------------------------------------------------------------------
# load_candidate_sql
# ---------------------------------------------------------------------------

def test_load_candidate_sql_empty_when_uri_unset(monkeypatch):
    monkeypatch.delenv("CANDIDATE_SQL_URI", raising=False)
    assert load_candidate_sql() == ""


def test_load_candidate_sql_fetches_and_decodes_utf8(monkeypatch):
    payload = b"  select 2  \n"
    fake_s3 = FakeS3Client({("continuo", "candidate-sql/rel-1/svc.orders.sql"): payload})
    monkeypatch.setenv("CANDIDATE_SQL_URI", "s3://continuo/candidate-sql/rel-1/svc.orders.sql")
    monkeypatch.setattr(validation_runner.s3_common, "make_s3_client", lambda: fake_s3)

    result = load_candidate_sql()

    assert result == "  select 2  \n"
    assert fake_s3.calls == [{"Bucket": "continuo", "Key": "candidate-sql/rel-1/svc.orders.sql"}]


def test_load_candidate_sql_raises_on_bad_uri(monkeypatch):
    monkeypatch.setenv("CANDIDATE_SQL_URI", "not-an-s3-uri")
    with pytest.raises(ValueError):
        load_candidate_sql()


# ---------------------------------------------------------------------------
# main — build_from_sql
# ---------------------------------------------------------------------------

def test_main_build_from_sql_calls_adapter_and_emits_success(monkeypatch, capsys):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    fake_adapter = FakeWarehouseAdapter()
    monkeypatch.setattr(validation_runner, "adapter_from_env", lambda: fake_adapter)
    monkeypatch.setattr(validation_runner, "load_candidate_sql", lambda: "SELECT 1 AS id")

    main()

    assert fake_adapter.schemas_ensured == ["_candidate_relA"]
    assert fake_adapter.builds == [("_candidate_relA", "orders", "SELECT 1 AS id")]
    assert fake_adapter.clones == []
    assert fake_adapter.closed is True
    out = capsys.readouterr().out
    assert validation_result.SENTINEL_BEGIN in out
    assert '"status":"success"' in out
    assert out.strip().endswith(validation_result.SENTINEL_END)


def test_main_build_from_sql_empty_candidate_sql_errors(monkeypatch, capsys):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    monkeypatch.setattr(validation_runner, "load_candidate_sql", lambda: "")

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 2
    assert '"status":"error"' in capsys.readouterr().out


def test_main_build_from_sql_s3_error_emits_error_block(monkeypatch, capsys):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")

    def _raise_s3_error():
        raise RuntimeError("S3 down")

    monkeypatch.setattr(validation_runner, "load_candidate_sql", _raise_s3_error)

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert validation_result.SENTINEL_BEGIN in out
    assert '"status":"error"' in out
    assert out.strip().endswith(validation_result.SENTINEL_END)


# ---------------------------------------------------------------------------
# main — clone_from_prod
# ---------------------------------------------------------------------------

def test_main_clone_from_prod_calls_adapter(monkeypatch, capsys):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "clone_from_prod")
    monkeypatch.setenv("PROD_SCHEMA", "analytics")
    fake_adapter = FakeWarehouseAdapter()
    monkeypatch.setattr(validation_runner, "adapter_from_env", lambda: fake_adapter)

    main()

    assert fake_adapter.schemas_ensured == ["_candidate_relA"]
    assert fake_adapter.clones == [("_candidate_relA", "analytics", "orders")]
    assert fake_adapter.builds == []
    assert fake_adapter.closed is True
    assert '"status":"success"' in capsys.readouterr().out


def test_main_clone_from_prod_missing_prod_schema_exits(monkeypatch):
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "clone_from_prod")
    monkeypatch.delenv("PROD_SCHEMA", raising=False)

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 2


# ---------------------------------------------------------------------------
# main — unknown VALIDATION_OP
# ---------------------------------------------------------------------------

def test_main_unknown_validation_op_exits_2(monkeypatch, capsys):
    """The else branch in main() must emit a structured error block and exit 2."""
    _set_pg_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "bogus")

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 2
    out = capsys.readouterr().out
    assert validation_result.SENTINEL_BEGIN in out
    assert '"status":"error"' in out
    assert out.strip().endswith(validation_result.SENTINEL_END)
