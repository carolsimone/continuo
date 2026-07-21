#!/usr/bin/env python3
"""Upload a locally-compiled dbt manifest to S3 (the compile Job's main container).

The init container (the team image) runs `dbt compile` and copies
target/manifest.json into a shared emptyDir; this main container reads that file
(COMPILE_MANIFEST_PATH) and put_objects it to MANIFEST_S3_URI (s3://bucket/key,
continuo's canonical <service>/<release_id>/manifest.json). The team image has no
S3 access; this continuo-owned container does the upload. A nonzero exit fails
the compile Job, which release-controller treats as compile_failed.

After the manifest upload, it also uploads up to two partial-parse msgpack
artifacts produced by the same compile: PARSE_PROD_LOCAL_PATH/PARSE_PROD_S3_URI
(the release-proven parse cache, published under the service's parse-cache
prefix for later per-node hydration) and PARSE_CANDIDATE_LOCAL_PATH/
PARSE_CANDIDATE_S3_URI (the candidate release's own parse artifact, stored
alongside its manifest). Each pair is independent and optional: an absent URI
env means that artifact is not produced in this context and is skipped
silently; a URI present without a readable local file is a hard error, since
it means the compile leg claimed to export the artifact but didn't.
"""
import os
import sys

import s3_common


def _upload_optional(s3, local_env: str, uri_env: str) -> None:
    """Upload one optional parse-cache artifact. Both envs present -> upload
    (missing local file is a hard error: the compile pod claimed it exported).
    URI env absent -> skip silently (feature not active for this context)."""
    uri = os.environ.get(uri_env, "")
    if not uri:
        return
    local = os.environ.get(local_env, "")
    try:
        bucket, key = s3_common.parse_s3_uri(uri)
    except ValueError as exc:
        print(f"compile_uploader: invalid {uri_env}: {exc}", file=sys.stderr)
        sys.exit(2)
    try:
        with open(local, "rb") as f:
            body = f.read()
    except OSError as exc:
        print(f"compile_uploader: cannot read parse artifact {local}: {exc}", file=sys.stderr)
        sys.exit(3)
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body)
    except Exception as exc:  # noqa: BLE001 - any boto3 failure is terminal here
        print(f"compile_uploader: S3 upload to s3://{bucket}/{key} failed: {exc}", file=sys.stderr)
        sys.exit(4)
    print(f"compile_uploader: uploaded {local} -> s3://{bucket}/{key}")


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

    _upload_optional(s3, "PARSE_PROD_LOCAL_PATH", "PARSE_PROD_S3_URI")
    _upload_optional(s3, "PARSE_CANDIDATE_LOCAL_PATH", "PARSE_CANDIDATE_S3_URI")


if __name__ == "__main__":
    main()
