#!/usr/bin/env python3
"""Build a single node as an empty table in the candidate schema (blue/green validation).

The executor passes ``CANDIDATE_SQL_URI`` — an ``s3://bucket/key`` reference to the
node's compiled SQL with every schema-qualified reference already rewritten to the
candidate schema. We fetch the SQL at runtime, then materialize it ``WITH NO DATA``
so the SQL is validated against the (empty) upstream tables built earlier in
dependency order, without touching production. stdout is captured as the per-node
validation log; a non-zero exit marks the node failed.

Seeds run ``dbt seed --empty`` via a separate code path and never invoke this
runner (the executor only uses it as the model/snapshot validation command). A
missing or empty ``CANDIDATE_SQL_URI`` therefore means the producer never uploaded
this node's compiled SQL — that is a validation error, not a no-op: the node fails
rather than being silently reported as validated.
"""
import os
import sys

import boto3

try:
    from base import validation_result  # repo/test context (pythonpath=".")
except ModuleNotFoundError:  # pragma: no cover - flat layout inside the image
    import validation_result

try:
    from base.warehouse import adapter_from_env
except ModuleNotFoundError:  # pragma: no cover - flat layout inside the image
    from warehouse import adapter_from_env


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


def main() -> None:
    schema = _require("DBT_TARGET_SCHEMA")
    table = _require("TABLE_NAME")
    op = os.environ.get("VALIDATION_OP", "build_from_sql")
    unique_id = f"model.{table}"

    # Gather op-specific inputs BEFORE connecting, surfacing input errors as a
    # structured block (preserves the prior contract + exit codes).
    candidate_sql = None
    prod_schema = None
    if op == "build_from_sql":
        try:
            raw_sql = load_candidate_sql()
        except Exception as exc:
            uri = os.environ.get("CANDIDATE_SQL_URI", "")
            print(f"validation_runner: ERROR fetching candidate SQL from {uri!r}: {exc}", file=sys.stderr)
            print(validation_result.result_block("error", str(exc), unique_id=unique_id), flush=True)
            sys.exit(1)
        if not raw_sql:
            print("validation_runner: CANDIDATE_SQL_URI is missing or empty for a "
                  "build_from_sql node; cannot validate", file=sys.stderr)
            print(validation_result.result_block("error", "CANDIDATE_SQL_URI is missing or empty",
                                                 unique_id=unique_id), flush=True)
            sys.exit(2)
        candidate_sql = raw_sql
    elif op == "clone_from_prod":
        prod_schema = _require("PROD_SCHEMA")
    else:
        print(f"validation_runner: unknown VALIDATION_OP {op!r}", file=sys.stderr)
        print(validation_result.result_block("error", f"unknown VALIDATION_OP {op!r}",
                                             unique_id=unique_id), flush=True)
        sys.exit(2)

    adapter = None
    try:
        adapter = adapter_from_env()
        adapter.ensure_schema(schema)
        if op == "build_from_sql":
            adapter.build_empty_from_sql(schema, table, candidate_sql)
        else:
            adapter.clone_empty_from_prod(schema, prod_schema, table)
        print(f"validation_runner: built {schema}.{table} (empty, op={op})", flush=True)
        print(validation_result.result_block("success", unique_id=unique_id), flush=True)
    except Exception as exc:
        print(f"validation_runner: ERROR building {schema}.{table}: {exc}", file=sys.stderr)
        print(validation_result.result_block("error", str(exc), unique_id=unique_id), flush=True)
        sys.exit(1)
    finally:
        if adapter is not None:
            adapter.close()


if __name__ == "__main__":
    main()
