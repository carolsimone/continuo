#!/usr/bin/env python3
"""Upload a locally-compiled dbt manifest to S3 (the compile Job's main container).

The init container (the team image) runs `dbt compile` and copies
target/manifest.json into a shared emptyDir; this main container reads that file
(COMPILE_MANIFEST_PATH) and put_objects it to MANIFEST_S3_URI (s3://bucket/key,
continuo's canonical <service>/<release_id>/manifest.json). The team image has no
S3 access; this continuo-owned container does the upload. A nonzero exit fails
the compile Job, which release-controller treats as compile_failed.
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
        print(f"compile_uploader: missing required env var {name}", file=sys.stderr)
        sys.exit(2)
    return value


def main() -> None:
    path = _require("COMPILE_MANIFEST_PATH")
    bucket, key = _parse_s3_uri(_require("MANIFEST_S3_URI"))
    try:
        with open(path, "rb") as f:
            body = f.read()
    except OSError as exc:
        print(f"compile_uploader: cannot read manifest {path}: {exc}", file=sys.stderr)
        sys.exit(3)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION"),
    )
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    print(f"compile_uploader: uploaded {path} -> s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
