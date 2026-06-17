#!/usr/bin/env python3
"""Build a single node as an empty table in the candidate schema (blue/green validation).

The executor passes ``CANDIDATE_SQL_URI`` — an ``s3://bucket/key`` reference to the
node's compiled SQL with every schema-qualified reference already rewritten to the
candidate schema. We fetch the SQL at runtime, then materialize it ``WITH NO DATA``
so the SQL is validated against the (empty) upstream tables built earlier in
dependency order, without touching production. stdout is captured as the per-node
validation log; a non-zero exit marks the node failed.

Seeds run ``dbt seed --empty`` via a separate code path. When ``CANDIDATE_SQL_URI``
is absent or empty this runner exits cleanly (exit 0) — there is nothing to
validate.
"""
import os
import sys

import boto3


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"validation_runner: missing required env var {name}", file=sys.stderr)
        sys.exit(2)
    return value


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Parse an ``s3://bucket/key`` URI into ``(bucket, key)``.

    >>> _parse_s3_uri("s3://continuo/candidate-sql/rel/node.sql")
    ('continuo', 'candidate-sql/rel/node.sql')
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"invalid S3 URI (must start with s3://): {uri!r}")
    without_scheme = uri[len("s3://"):]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"invalid S3 URI (missing bucket or key): {uri!r}")
    return bucket, key


def load_candidate_sql() -> str:
    """Fetch the candidate SQL for this node from S3 via ``CANDIDATE_SQL_URI``.

    Returns the raw UTF-8 body of the S3 object, without stripping — the caller
    is responsible for any ``.strip().rstrip(";").strip()`` normalization.

    Returns ``""`` when ``CANDIDATE_SQL_URI`` is absent or empty (seed/empty node
    — nothing to validate). No S3 connection is made in that case.
    """
    uri = os.environ.get("CANDIDATE_SQL_URI", "")
    if not uri:
        return ""
    bucket, key = _parse_s3_uri(uri)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION"),
    )
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return body.decode("utf-8")


def _ensure_schema(cur, schema: str) -> None:
    """Create the candidate schema, tolerating a concurrent create.

    ``CREATE SCHEMA IF NOT EXISTS`` is not atomic under concurrency: root
    validation nodes have no gating upstreams and dispatch in parallel, so
    several pods can race this statement and raise DuplicateSchema /
    UniqueViolation on pg_namespace.  Either means the schema now exists,
    which is the desired outcome — swallow it.
    (autocommit is on, so the failed statement leaves no aborted transaction.)
    """
    from psycopg2 import errors as pg_errors
    from psycopg2 import sql as pg_sql

    stmt = pg_sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(pg_sql.Identifier(schema))
    print(f"-- executing:\n{stmt.as_string(cur.connection)}", flush=True)
    try:
        cur.execute(stmt)
    except (pg_errors.DuplicateSchema, pg_errors.UniqueViolation):
        print(f"-- schema {schema} already exists (concurrent create); continuing", flush=True)


def main() -> None:
    schema = _require("DBT_TARGET_SCHEMA")
    table = _require("TABLE_NAME")

    try:
        raw_sql = load_candidate_sql()
    except Exception as exc:
        uri = os.environ.get("CANDIDATE_SQL_URI", "")
        print(
            f"validation_runner: ERROR fetching candidate SQL from {uri!r}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
    if not raw_sql:
        # Seed or empty node — nothing to validate via CTAS.
        print(
            "validation_runner: no CANDIDATE_SQL_URI (seed/empty node); nothing to validate",
            flush=True,
        )
        return

    # Strip any trailing terminator so it embeds cleanly inside CREATE TABLE AS (...).
    candidate_sql = raw_sql.strip().rstrip(";").strip()

    import psycopg2
    from psycopg2 import sql as pg_sql

    conn = psycopg2.connect(
        host=_require("DBT_POSTGRES_HOST"),
        port=os.environ.get("DBT_POSTGRES_PORT", "5432"),
        dbname=_require("DBT_POSTGRES_DB"),
        user=_require("DBT_POSTGRES_USER"),
        password=os.environ.get("DBT_POSTGRES_PASSWORD", ""),
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            _ensure_schema(cur, schema)
            # The candidate SQL is the compiled model SELECT fetched from S3, with
            # all schema-qualified references already rewritten to the candidate
            # schema. WITH NO DATA validates the SELECT against the empty upstream
            # tables built earlier in dependency order, without loading any rows.
            # Every node is materialized as a table here regardless of its real
            # materialization type (view, incremental, etc.) — validation checks
            # the SELECT, not the final materialization behaviour.
            statements = (
                pg_sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    pg_sql.Identifier(schema), pg_sql.Identifier(table)
                ),
                pg_sql.SQL("CREATE TABLE {}.{} AS ({}) WITH NO DATA").format(
                    pg_sql.Identifier(schema),
                    pg_sql.Identifier(table),
                    pg_sql.SQL(candidate_sql),
                ),
            )
            for stmt in statements:
                print(f"-- executing:\n{stmt.as_string(conn)}", flush=True)
                cur.execute(stmt)
        print(f"validation_runner: built {schema}.{table} (empty)", flush=True)
    except Exception as exc:  # surface any DB/exec error as the per-node validation log
        print(f"validation_runner: ERROR building {schema}.{table}: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
