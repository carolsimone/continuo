#!/usr/bin/env python3
"""Hydrate a per-node dbt Job with the release-proven partial-parse cache.

Runs as the `hydrate-parse-cache` initContainer (s3-sidecar image). Downloads
PARSE_CACHE_S3_URI into PARSE_CACHE_DEST (an emptyDir the team container mounts
over its dbt target dir). NEVER fails the Job: any problem logs a loud
`degraded:<reason>` line, writes the same to the container termination message,
and exits 0 — the node then runs with a full parse (correct but slow). Success
writes `hydrated` to the termination message.
"""
import os
import sys

import s3_common

TERMINATION_LOG = os.environ.get("TERMINATION_LOG_PATH", "/dev/termination-log")


def _terminate(message: str) -> None:
    try:
        with open(TERMINATION_LOG, "w") as f:
            f.write(message[:4000])
    except OSError as exc:
        print(f"parse_cache_fetcher: cannot write termination message: {exc}", file=sys.stderr)


def _degrade(reason: str) -> None:
    print(f"parse_cache_fetcher: degraded:{reason} — node will run WITHOUT the parse cache (full parse)", file=sys.stderr)
    _terminate(f"degraded:{reason}")
    sys.exit(0)


def main() -> None:
    uri = os.environ.get("PARSE_CACHE_S3_URI", "")
    dest = os.environ.get("PARSE_CACHE_DEST", "")
    if not uri or not dest:
        _degrade("missing PARSE_CACHE_S3_URI/PARSE_CACHE_DEST configuration")
    try:
        bucket, key = s3_common.parse_s3_uri(uri)
    except ValueError as exc:
        _degrade(f"invalid PARSE_CACHE_S3_URI: {exc}")
    try:
        s3 = s3_common.make_s3_client()
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as exc:  # noqa: BLE001 - every fetch failure degrades
        _degrade(f"fetch s3://{bucket}/{key} failed: {exc}")
    try:
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "wb") as f:
            f.write(body)
    except OSError as exc:
        try:
            os.unlink(dest)
        except OSError:
            pass  # best-effort cleanup of a partially-written dest file
        _degrade(f"write {dest} failed: {exc}")
    print(f"parse_cache_fetcher: hydrated {dest} from s3://{bucket}/{key} ({len(body)} bytes)")
    _terminate("hydrated")


if __name__ == "__main__":
    main()
