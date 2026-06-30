#!/usr/bin/env python3
"""Download a node's compiled candidate SQL from S3 (the validation Job's init container).

The validation main container (dbt-base, no S3 access) reads the SQL from a shared
emptyDir file instead of calling S3 itself. This init container reads CANDIDATE_SQL_URI
(s3://bucket/key, the rewritten compiled SQL for the changed-closure node) and writes the
object body to CANDIDATE_SQL_PATH inside the shared emptyDir. Keeping S3 access here keeps
boto3 out of every dbt image. A nonzero exit fails the validation Job.
"""
import os
import sys

import boto3


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"invalid S3 URI (must start with s3://): {uri!r}")
    bucket, _, key = uri[len("s3://"):].partition("/")
    if not bucket or not key:
        raise ValueError(f"invalid S3 URI (missing bucket or key): {uri!r}")
    return bucket, key


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"candidate_fetcher: missing required env var {name}", file=sys.stderr)
        sys.exit(2)
    return value


def main() -> None:
    dest = _require("CANDIDATE_SQL_PATH")
    try:
        bucket, key = _parse_s3_uri(_require("CANDIDATE_SQL_URI"))
    except ValueError as exc:
        print(f"candidate_fetcher: invalid CANDIDATE_SQL_URI: {exc}", file=sys.stderr)
        sys.exit(2)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION"),
    )
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as exc:
        print(f"candidate_fetcher: S3 download s3://{bucket}/{key} failed: {exc}", file=sys.stderr)
        sys.exit(4)
    try:
        with open(dest, "wb") as f:
            f.write(body)
    except OSError as exc:
        print(f"candidate_fetcher: cannot write {dest}: {exc}", file=sys.stderr)
        sys.exit(3)
    print(f"candidate_fetcher: downloaded s3://{bucket}/{key} -> {dest} ({len(body)} bytes)")


if __name__ == "__main__":
    main()
