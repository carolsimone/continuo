#!/usr/bin/env python3
"""Upload a locally-compiled dbt manifest to S3 (the compile Job's main container).

The init container (the team image) runs `dbt compile` and copies
target/manifest.json into a shared emptyDir; this main container reads that file
(COMPILE_MANIFEST_PATH) and put_objects it to MANIFEST_S3_URI (s3://bucket/key,
continuo's canonical <service>/<release_id>/manifest.json). The team image has no
S3 access; this continuo-owned container does the upload. A nonzero exit fails
the compile Job, which release-controller treats as compile_failed.
"""
import sys

import s3_common


def main() -> None:
    path = s3_common.require_env("COMPILE_MANIFEST_PATH", caller="compile_uploader")
    try:
        bucket, key = s3_common.parse_s3_uri(
            s3_common.require_env("MANIFEST_S3_URI", caller="compile_uploader")
        )
    except ValueError as exc:
        print(f"compile_uploader: invalid MANIFEST_S3_URI: {exc}", file=sys.stderr)
        sys.exit(2)
    try:
        with open(path, "rb") as f:
            body = f.read()
    except OSError as exc:
        print(f"compile_uploader: cannot read manifest {path}: {exc}", file=sys.stderr)
        sys.exit(3)
    s3 = s3_common.make_s3_client()
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body)
    except Exception as exc:
        print(f"compile_uploader: S3 upload to s3://{bucket}/{key} failed: {exc}", file=sys.stderr)
        sys.exit(4)
    print(f"compile_uploader: uploaded {path} -> s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
