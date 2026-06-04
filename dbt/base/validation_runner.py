#!/usr/bin/env python3
"""Build a single node as an empty table in the candidate schema (blue/green validation).

The executor passes the node's compiled SQL with every schema-qualified reference
already rewritten to the candidate schema (CANDIDATE_SQL). We materialize it
``WITH NO DATA`` so the SQL is validated against the (empty) upstream tables built
earlier in dependency order, without touching production. stdout is captured as the
per-node validation log; a non-zero exit marks the node failed.
"""
import os
import sys

import psycopg2
from psycopg2 import sql


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"validation_runner: missing required env var {name}", file=sys.stderr)
        sys.exit(2)
    return value


def main() -> None:
    schema = _require("DBT_TARGET_SCHEMA")
    table = _require("TABLE_NAME")
    candidate_sql = _require("CANDIDATE_SQL")

    conn = psycopg2.connect(
        host=_require("DBT_POSTGRES_HOST"),
        port=os.environ.get("DBT_POSTGRES_PORT", "5432"),
        dbname=_require("DBT_POSTGRES_DB"),
        user=_require("DBT_POSTGRES_USER"),
        password=os.environ.get("DBT_POSTGRES_PASSWORD", ""),
    )
    conn.autocommit = True
    try:
        statements = (
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)),
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(sql.Identifier(schema), sql.Identifier(table)),
            # CANDIDATE_SQL is the compiled model SELECT with schema refs rewritten to
            # the candidate schema. WITH NO DATA validates structure against the empty
            # upstreams built earlier, without scanning or loading any rows.
            sql.SQL("CREATE TABLE {}.{} AS ({}) WITH NO DATA").format(
                sql.Identifier(schema), sql.Identifier(table), sql.SQL(candidate_sql)
            ),
        )
        with conn.cursor() as cur:
            for stmt in statements:
                print(f"-- executing:\n{stmt.as_string(conn)}", flush=True)
                cur.execute(stmt)
        print(f"validation_runner: built {schema}.{table} (empty)", flush=True)
    except Exception as exc:  # surface any DB error as the per-node validation log
        print(f"validation_runner: ERROR building {schema}.{table}: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
