#!/usr/bin/env python3
"""Download a node's compiled candidate SQL from S3 (the validation Job's init container).

The validation main container (dbt-base, no S3 access) reads the SQL from a shared
emptyDir file instead of calling S3 itself. This init container reads CANDIDATE_SQL_URI
(s3://bucket/key, the rewritten compiled SQL for the changed-closure node) and writes the
object body to CANDIDATE_SQL_PATH inside the shared emptyDir. Keeping S3 access here keeps
boto3 out of every dbt image. A nonzero exit fails the validation Job.
"""
import sys

import s3_common


def main() -> None:
    dest = s3_common.require_env("CANDIDATE_SQL_PATH", caller="candidate_fetcher")
    try:
        bucket, key = s3_common.parse_s3_uri(
            s3_common.require_env("CANDIDATE_SQL_URI", caller="candidate_fetcher")
        )
    except ValueError as exc:
        print(f"candidate_fetcher: invalid CANDIDATE_SQL_URI: {exc}", file=sys.stderr)
        sys.exit(2)
    s3 = s3_common.make_s3_client()
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
