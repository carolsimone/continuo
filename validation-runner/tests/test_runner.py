"""Unit tests for the runner — hand-written fakes, no live DB or S3."""
import pytest
from continuo_validation_contract import result
from continuo_validation_contract.port import AdapterDiscoveryError, WarehouseAdapter

from continuo_validation_runner import runner


class FakeWarehouseAdapter(WarehouseAdapter):
    """Records every adapter call; no live DB required."""

    def __init__(self):
        self.schemas_ensured = []
        self.builds = []    # list of (schema, table, sql)
        self.clones = []    # list of (candidate_schema, prod_schema, table)
        self.closed = False

    @classmethod
    def required_env(cls) -> list[str]:
        """Return required environment variables."""
        return []

    @classmethod
    def from_env(cls) -> "FakeWarehouseAdapter":
        """Create adapter from environment."""
        return cls()

    def ensure_schema(self, schema: str) -> None:
        """Record schema ensure call."""
        self.schemas_ensured.append(schema)

    def build_empty_from_sql(self, schema: str, table: str, compiled_sql: str) -> None:
        """Record build call."""
        self.builds.append((schema, table, compiled_sql))

    def clone_empty_from_prod(self, candidate_schema: str, prod_schema: str, table: str) -> None:
        """Record clone call."""
        self.clones.append((candidate_schema, prod_schema, table))

    def close(self) -> None:
        """Record close call."""
        self.closed = True


def _install_fake_adapter(monkeypatch, adapter, required=()):
    """Patch discovery to return a plugin class wrapping *adapter*."""

    class _Plugin:
        @staticmethod
        def required_env() -> list[str]:
            return list(required)

        @staticmethod
        def from_env() -> WarehouseAdapter:
            return adapter

    monkeypatch.setattr(runner, "discover_adapter", lambda: ("fake", _Plugin))


class _FakeBody:
    """Mimics the S3 streaming body returned inside get_object()["Body"]."""

    def __init__(self, data: bytes) -> None:
        """Store data."""
        self._data = data

    def read(self) -> bytes:
        """Return stored data."""
        return self._data


class FakeS3Client:
    """Returns pre-loaded bytes for known (bucket, key) pairs; records calls made."""

    def __init__(self, objects: dict):
        """Initialize with object mapping."""
        self._objects = objects
        self.calls = []

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        """Retrieve object or raise."""
        self.calls.append({"Bucket": Bucket, "Key": Key})
        data = self._objects.get((Bucket, Key))
        if data is None:
            raise RuntimeError(f"FakeS3Client: unknown key s3://{Bucket}/{Key}")
        return {"Body": _FakeBody(data)}


def _set_common_env(monkeypatch):
    """Set common environment variables."""
    monkeypatch.setenv("DBT_TARGET_SCHEMA", "_candidate_relA")
    monkeypatch.setenv("TABLE_NAME", "orders")


# --------------------------------------------------------------------------
# load_candidate_sql
# --------------------------------------------------------------------------

def test_load_candidate_sql_empty_when_uri_unset(monkeypatch):
    """Return empty string when CANDIDATE_SQL_URI is unset."""
    monkeypatch.delenv("CANDIDATE_SQL_URI", raising=False)
    assert runner.load_candidate_sql() == ""


def test_load_candidate_sql_fetches_and_decodes_utf8(monkeypatch):
    """Fetch and decode UTF-8 SQL from S3."""
    fake_s3 = FakeS3Client({("continuo", "candidate-sql/rel-1/svc.orders.sql"): b"  select 2  \n"})
    monkeypatch.setenv("CANDIDATE_SQL_URI", "s3://continuo/candidate-sql/rel-1/svc.orders.sql")
    monkeypatch.setattr(runner.s3, "make_s3_client", lambda: fake_s3)
    assert runner.load_candidate_sql() == "  select 2  \n"
    assert fake_s3.calls == [{"Bucket": "continuo", "Key": "candidate-sql/rel-1/svc.orders.sql"}]


def test_load_candidate_sql_raises_on_bad_uri(monkeypatch):
    """Raise ValueError on invalid S3 URI."""
    monkeypatch.setenv("CANDIDATE_SQL_URI", "not-an-s3-uri")
    with pytest.raises(ValueError):
        runner.load_candidate_sql()


# --------------------------------------------------------------------------
# main — build_from_sql / clone_from_prod / unknown op (ported behaviors)
# --------------------------------------------------------------------------

def test_main_build_from_sql_calls_adapter_and_emits_success(monkeypatch, capsys):
    """Build from SQL and emit success block."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    fake = FakeWarehouseAdapter()
    _install_fake_adapter(monkeypatch, fake)
    monkeypatch.setattr(runner, "load_candidate_sql", lambda: "SELECT 1 AS id")

    runner.main()

    assert fake.schemas_ensured == ["_candidate_relA"]
    assert fake.builds == [("_candidate_relA", "orders", "SELECT 1 AS id")]
    assert fake.clones == []
    assert fake.closed is True
    out = capsys.readouterr().out
    assert result.SENTINEL_BEGIN in out
    assert '"status":"success"' in out
    assert out.strip().endswith(result.SENTINEL_END)


def test_main_uses_node_id_env_when_set(monkeypatch, capsys):
    """The structured block's unique_id echoes NODE_ID when the executor sets it."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    monkeypatch.setenv("NODE_ID", "model.svc.orders_v2")
    fake = FakeWarehouseAdapter()
    _install_fake_adapter(monkeypatch, fake)
    monkeypatch.setattr(runner, "load_candidate_sql", lambda: "SELECT 1 AS id")

    runner.main()

    out = capsys.readouterr().out
    assert '"unique_id":"model.svc.orders_v2"' in out


def test_main_falls_back_to_model_table_when_node_id_unset(monkeypatch, capsys):
    """Without NODE_ID, unique_id falls back to model.<table> for compatibility."""
    _set_common_env(monkeypatch)
    monkeypatch.delenv("NODE_ID", raising=False)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    fake = FakeWarehouseAdapter()
    _install_fake_adapter(monkeypatch, fake)
    monkeypatch.setattr(runner, "load_candidate_sql", lambda: "SELECT 1 AS id")

    runner.main()

    out = capsys.readouterr().out
    assert '"unique_id":"model.orders"' in out


def test_main_build_from_sql_empty_candidate_sql_errors(monkeypatch, capsys):
    """Exit 2 when candidate SQL is empty."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    monkeypatch.setattr(runner, "load_candidate_sql", lambda: "")
    with pytest.raises(SystemExit) as exc:
        runner.main()
    assert exc.value.code == 2
    assert '"status":"error"' in capsys.readouterr().out


def test_main_build_from_sql_s3_error_emits_error_block(monkeypatch, capsys):
    """Exit 1 on S3 fetch error."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")

    def _raise():
        raise RuntimeError("S3 down")

    monkeypatch.setattr(runner, "load_candidate_sql", _raise)
    with pytest.raises(SystemExit) as exc:
        runner.main()
    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert result.SENTINEL_BEGIN in out
    assert '"status":"error"' in out


def test_main_clone_from_prod_calls_adapter(monkeypatch, capsys):
    """Clone from prod and emit success block."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "clone_from_prod")
    monkeypatch.setenv("PROD_SCHEMA", "analytics")
    fake = FakeWarehouseAdapter()
    _install_fake_adapter(monkeypatch, fake)

    runner.main()

    assert fake.schemas_ensured == ["_candidate_relA"]
    assert fake.clones == [("_candidate_relA", "analytics", "orders")]
    assert fake.builds == []
    assert fake.closed is True
    assert '"status":"success"' in capsys.readouterr().out


def test_main_clone_from_prod_missing_prod_schema_exits(monkeypatch, capsys):
    """Exit 2 when PROD_SCHEMA is missing, with a structured error block."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "clone_from_prod")
    monkeypatch.delenv("PROD_SCHEMA", raising=False)
    with pytest.raises(SystemExit) as exc:
        runner.main()
    assert exc.value.code == 2
    out = capsys.readouterr().out
    assert result.SENTINEL_BEGIN in out
    assert "missing required env var PROD_SCHEMA" in out


def test_main_missing_dbt_target_schema_exits_2_with_block(monkeypatch, capsys):
    """Exit 2 when DBT_TARGET_SCHEMA is missing, with a structured error block."""
    monkeypatch.delenv("DBT_TARGET_SCHEMA", raising=False)
    monkeypatch.setenv("TABLE_NAME", "orders")
    with pytest.raises(SystemExit) as exc:
        runner.main()
    assert exc.value.code == 2
    out = capsys.readouterr().out
    assert result.SENTINEL_BEGIN in out
    assert "missing required env var DBT_TARGET_SCHEMA" in out


def test_main_unknown_validation_op_exits_2(monkeypatch, capsys):
    """Exit 2 on unknown VALIDATION_OP."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "bogus")
    with pytest.raises(SystemExit) as exc:
        runner.main()
    assert exc.value.code == 2
    out = capsys.readouterr().out
    assert result.SENTINEL_BEGIN in out
    assert '"status":"error"' in out


# --------------------------------------------------------------------------
# main — NEW behaviors: discovery failure, missing required adapter env
# --------------------------------------------------------------------------

def test_main_discovery_failure_exits_2_with_error_block(monkeypatch, capsys):
    """Exit 2 on adapter discovery failure."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "clone_from_prod")
    monkeypatch.setenv("PROD_SCHEMA", "analytics")

    def _fail():
        raise AdapterDiscoveryError("no warehouse adapter installed")

    monkeypatch.setattr(runner, "discover_adapter", _fail)
    with pytest.raises(SystemExit) as exc:
        runner.main()
    assert exc.value.code == 2
    out = capsys.readouterr().out
    assert result.SENTINEL_BEGIN in out
    assert "no warehouse adapter installed" in out


def test_main_missing_required_env_exits_2_naming_vars(monkeypatch, capsys):
    """Exit 2 when adapter required env vars are missing."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "clone_from_prod")
    monkeypatch.setenv("PROD_SCHEMA", "analytics")
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    fake = FakeWarehouseAdapter()
    _install_fake_adapter(monkeypatch, fake, required=["POSTGRES_HOST"])
    with pytest.raises(SystemExit) as exc:
        runner.main()
    assert exc.value.code == 2
    out = capsys.readouterr().out
    assert "POSTGRES_HOST" in out
    assert '"status":"error"' in out
    assert fake.closed is False  # never connected


# --------------------------------------------------------------------------
# main — sentinel-block invariant across every block-emitting exit path
# --------------------------------------------------------------------------

def _setup_success(monkeypatch):
    """Arrange a build_from_sql run that reaches the success block."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    fake = FakeWarehouseAdapter()
    _install_fake_adapter(monkeypatch, fake)
    monkeypatch.setattr(runner, "load_candidate_sql", lambda: "SELECT 1 AS id")


def _setup_empty_candidate_sql(monkeypatch):
    """Arrange a build_from_sql run whose candidate SQL is empty."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")
    monkeypatch.setattr(runner, "load_candidate_sql", lambda: "")


def _setup_s3_error(monkeypatch):
    """Arrange a build_from_sql run whose S3 fetch raises."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "build_from_sql")

    def _raise():
        raise RuntimeError("S3 down")

    monkeypatch.setattr(runner, "load_candidate_sql", _raise)


def _setup_unknown_op(monkeypatch):
    """Arrange a run with an unrecognized VALIDATION_OP."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "bogus")


def _setup_discovery_failure(monkeypatch):
    """Arrange a clone_from_prod run whose adapter discovery fails."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "clone_from_prod")
    monkeypatch.setenv("PROD_SCHEMA", "analytics")

    def _fail():
        raise AdapterDiscoveryError("no warehouse adapter installed")

    monkeypatch.setattr(runner, "discover_adapter", _fail)


def _setup_missing_required_env(monkeypatch):
    """Arrange a clone_from_prod run whose adapter is missing required env."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "clone_from_prod")
    monkeypatch.setenv("PROD_SCHEMA", "analytics")
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    fake = FakeWarehouseAdapter()
    _install_fake_adapter(monkeypatch, fake, required=["POSTGRES_HOST"])


def _setup_missing_dbt_target_schema(monkeypatch):
    """Arrange a run missing the required DBT_TARGET_SCHEMA env var."""
    monkeypatch.delenv("DBT_TARGET_SCHEMA", raising=False)
    monkeypatch.setenv("TABLE_NAME", "orders")


def _setup_missing_prod_schema(monkeypatch):
    """Arrange a clone_from_prod run missing the required PROD_SCHEMA env var."""
    _set_common_env(monkeypatch)
    monkeypatch.setenv("VALIDATION_OP", "clone_from_prod")
    monkeypatch.delenv("PROD_SCHEMA", raising=False)


# (setup, expected SystemExit code, or None when main() returns normally)
_SENTINEL_SCENARIOS = [
    ("success", _setup_success, None),
    ("empty_candidate_sql", _setup_empty_candidate_sql, 2),
    ("s3_error", _setup_s3_error, 1),
    ("unknown_op", _setup_unknown_op, 2),
    ("discovery_failure", _setup_discovery_failure, 2),
    ("missing_required_env", _setup_missing_required_env, 2),
    ("missing_dbt_target_schema", _setup_missing_dbt_target_schema, 2),
    ("missing_prod_schema", _setup_missing_prod_schema, 2),
]


def test_main_emits_exactly_one_sentinel_block_as_last_stdout_line(monkeypatch, capsys):
    """Every block-emitting exit path prints exactly one sentinel block, block-last.

    The contract (see ``result.py``) is: exactly ONE sentinel-framed block, as the
    terminal non-empty stdout line, on every outcome that emits one. Exercises all
    eight block-emitting paths through ``main()`` — success, empty candidate SQL,
    S3-fetch error, unknown VALIDATION_OP, adapter discovery failure, missing
    required adapter env, missing DBT_TARGET_SCHEMA, and missing PROD_SCHEMA — each
    in its own isolated monkeypatch context so scenarios cannot leak patches into
    one another.
    """
    for name, setup, expected_exit in _SENTINEL_SCENARIOS:
        with monkeypatch.context() as mp:
            setup(mp)
            capsys.readouterr()  # drain output from any prior scenario
            if expected_exit is None:
                runner.main()
            else:
                with pytest.raises(SystemExit) as exc:
                    runner.main()
                assert exc.value.code == expected_exit, name

            out = capsys.readouterr().out
            assert out.count(result.SENTINEL_BEGIN) == 1, name
            assert out.count(result.SENTINEL_END) == 1, name
            assert out.strip().splitlines()[-1] == result.SENTINEL_END, name
