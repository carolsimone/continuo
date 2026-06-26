"""Unit tests for base.warehouse — mocked cursor, no live database."""
import pytest
from psycopg2 import errors as pg_errors
from psycopg2 import sql as pg_sql

from base.warehouse import PostgresAdapter


class _FakeCursor:
    """Records execute() calls; optionally raises a given error on the CREATE.

    The CREATE statements are psycopg2 Composed objects; the advisory lock/unlock
    are plain strings — used to tell them apart (mirrors test_validation_runner).
    """

    def __init__(self, raise_on_composed=None):
        self._raise = raise_on_composed
        self.calls = []  # (statement, params)

    def execute(self, statement, params=None):
        self.calls.append((statement, params))
        if self._raise is not None and not isinstance(statement, str):
            raise self._raise

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, cur):
        self._cur = cur
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return self._cur

    def close(self):
        self.closed = True


def _stmt_text(composed) -> str:
    """Concatenate the literal SQL fragments of a Composed (DB-free rendering)."""
    return "".join(p.string for p in composed.seq if isinstance(p, pg_sql.SQL))


def _rendered(cur):
    return [s if isinstance(s, str) else s.__class__.__name__ for s, _ in cur.calls]


def test_postgres_adapter_sets_autocommit():
    conn = _FakeConn(_FakeCursor())
    PostgresAdapter(conn)
    assert conn.autocommit is True


def test_ensure_schema_locks_creates_unlocks():
    cur = _FakeCursor()
    PostgresAdapter(_FakeConn(cur)).ensure_schema("_candidate_relA")
    rendered = _rendered(cur)
    assert "pg_advisory_lock" in rendered[0]
    assert rendered[1] == "Composed"  # CREATE SCHEMA
    assert "pg_advisory_unlock" in rendered[2]
    assert len(cur.calls) == 3


def test_ensure_schema_tolerates_duplicate_schema_and_unlocks():
    cur = _FakeCursor(raise_on_composed=pg_errors.DuplicateSchema("exists"))
    PostgresAdapter(_FakeConn(cur)).ensure_schema("_candidate_relB")  # must not raise
    assert "pg_advisory_unlock" in _rendered(cur)[-1]


def test_ensure_schema_propagates_unexpected_error_but_unlocks():
    cur = _FakeCursor(raise_on_composed=RuntimeError("connection reset"))
    with pytest.raises(RuntimeError, match="connection reset"):
        PostgresAdapter(_FakeConn(cur)).ensure_schema("_candidate_relC")
    assert "pg_advisory_unlock" in _rendered(cur)[-1]


def test_build_empty_from_sql_drops_then_ctas_with_no_data():
    cur = _FakeCursor()
    PostgresAdapter(_FakeConn(cur)).build_empty_from_sql(
        "_candidate_relA", "orders", "SELECT 1 AS id"
    )
    assert _rendered(cur) == ["Composed", "Composed"]
    drop, create = cur.calls[0][0], cur.calls[1][0]
    assert "DROP TABLE IF EXISTS" in _stmt_text(drop)
    create_text = _stmt_text(create)
    assert "CREATE TABLE" in create_text
    assert "WITH NO DATA" in create_text


def test_build_empty_from_sql_strips_trailing_semicolon():
    cur = _FakeCursor()
    PostgresAdapter(_FakeConn(cur)).build_empty_from_sql(
        "_candidate_relA", "orders", "SELECT 1 AS id ;  "
    )
    # The inner SQL is embedded as a pg_sql.SQL part of the CREATE Composed; the
    # trailing ';' must be stripped so it nests inside AS ( ... ).
    inner = [p.string for p in cur.calls[1][0].seq if isinstance(p, pg_sql.SQL)]
    assert any(s.strip() == "SELECT 1 AS id" for s in inner)
