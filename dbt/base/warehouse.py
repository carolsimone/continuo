"""Warehouse adapter port + Postgres implementation for image-free validation.

Validation builds each node as an EMPTY table in the candidate schema, either from
the node's compiled SELECT (models/snapshots) or by cloning an existing prod table's
shape (unchanged upstreams, including seeds). The DDL is engine-specific, so it lives
behind the WarehouseAdapter port; Postgres is the only adapter for now.
"""
import os
from abc import ABC, abstractmethod

import psycopg2
from psycopg2 import errors as pg_errors
from psycopg2 import sql as pg_sql


class WarehouseAdapter(ABC):
    @abstractmethod
    def ensure_schema(self, schema: str) -> None: ...

    @abstractmethod
    def build_empty_from_sql(self, schema: str, table: str, compiled_sql: str) -> None: ...

    @abstractmethod
    def clone_empty_from_prod(self, candidate_schema: str, prod_schema: str, table: str) -> None: ...

    @abstractmethod
    def close(self) -> None: ...


class PostgresAdapter(WarehouseAdapter):
    def __init__(self, conn) -> None:
        self._conn = conn
        self._conn.autocommit = True

    def ensure_schema(self, schema: str) -> None:
        # Race-safe: root validation nodes dispatch in parallel and can collide on
        # CREATE SCHEMA. Serialize on a session advisory lock keyed by schema name;
        # tolerate DuplicateSchema/UniqueViolation as a second line of defense.
        with self._conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (schema,))
            try:
                stmt = pg_sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    pg_sql.Identifier(schema)
                )
                print(f"-- ensuring candidate schema {schema} exists", flush=True)
                try:
                    cur.execute(stmt)
                except (pg_errors.DuplicateSchema, pg_errors.UniqueViolation):
                    print(
                        f"-- schema {schema} already exists (concurrent create); continuing",
                        flush=True,
                    )
            finally:
                cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (schema,))

    def build_empty_from_sql(self, schema: str, table: str, compiled_sql: str) -> None:
        # Strip any trailing terminator so the SELECT nests cleanly inside AS ( ... ).
        inner = compiled_sql.strip().rstrip(";").strip()
        with self._conn.cursor() as cur:
            cur.execute(
                pg_sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    pg_sql.Identifier(schema), pg_sql.Identifier(table)
                )
            )
            cur.execute(
                pg_sql.SQL("CREATE TABLE {}.{} AS ({}) WITH NO DATA").format(
                    pg_sql.Identifier(schema),
                    pg_sql.Identifier(table),
                    pg_sql.SQL(inner),
                )
            )

    def clone_empty_from_prod(self, candidate_schema: str, prod_schema: str, table: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                pg_sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    pg_sql.Identifier(candidate_schema), pg_sql.Identifier(table)
                )
            )
            cur.execute(
                pg_sql.SQL(
                    "CREATE TABLE {}.{} AS SELECT * FROM {}.{} WHERE 1=0"
                ).format(
                    pg_sql.Identifier(candidate_schema),
                    pg_sql.Identifier(table),
                    pg_sql.Identifier(prod_schema),
                    pg_sql.Identifier(table),
                )
            )

    def close(self) -> None:
        self._conn.close()


def adapter_from_env() -> WarehouseAdapter:
    raise NotImplementedError  # Task 4
