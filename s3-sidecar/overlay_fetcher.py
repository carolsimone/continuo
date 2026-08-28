#!/usr/bin/env python3
"""Lay a shadow release's proposed source files over the compile Job's project.

Runs as the `overlay` initContainer (s3-sidecar image) of a compile Job for a
shadow release. Downloads SOURCE_OVERLAY_URI — a gzip tarball whose member
names are project-relative paths (e.g. models/orders.sql) — and extracts it
into OVERLAY_DEST; the compile container then copies that tree over its
project before running `dbt compile`, so the release compiles and validates
the proposed fix rather than the committed source.

Every failure exits nonzero and fails the Job. A shadow release exists only to
judge a fix; compiling the committed source instead would judge the wrong
thing, so there is deliberately no degraded path here.
"""
import io
import logging
import os
import posixpath
import sys
import tarfile

import s3_common

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="overlay_fetcher: %(message)s")
log = logging.getLogger(__name__)

EXIT_CONFIG = 2
EXIT_FETCH = 3
EXIT_UNSAFE = 4


def _safe_relative(name: str) -> str:
    """Return name normalised as a path inside the overlay, or raise ValueError
    when it is absolute or climbs out of the destination."""
    if name.startswith("/"):
        raise ValueError(f"absolute member path {name!r}")
    norm = posixpath.normpath(name)
    if norm == "." or norm.startswith("../") or norm == ".." or "/../" in f"/{norm}/":
        raise ValueError(f"member path escapes the overlay: {name!r}")
    return norm


def main() -> None:
    uri = os.environ.get("SOURCE_OVERLAY_URI", "")
    dest = os.environ.get("OVERLAY_DEST", "")
    if not uri or not dest:
        log.error("SOURCE_OVERLAY_URI and OVERLAY_DEST are required")
        sys.exit(EXIT_CONFIG)
    try:
        bucket, key = s3_common.parse_s3_uri(uri)
    except ValueError as exc:
        log.error("invalid SOURCE_OVERLAY_URI: %s", exc)
        sys.exit(EXIT_CONFIG)

    try:
        body = s3_common.make_s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as exc:  # noqa: BLE001 - any fetch failure fails the Job
        log.error("fetch s3://%s/%s failed: %s", bucket, key, exc)
        sys.exit(EXIT_FETCH)

    written = 0
    try:
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as tf:
            for member in tf.getmembers():
                rel = _safe_relative(member.name)
                target = os.path.join(dest, rel)
                if member.isdir():
                    os.makedirs(target, exist_ok=True)
                    os.chmod(target, 0o755)
                    continue
                if not member.isfile():
                    raise ValueError(f"unsupported member type for {member.name!r} (only files and directories)")
                os.makedirs(os.path.dirname(target), exist_ok=True)
                extracted = tf.extractfile(member)
                if extracted is None:
                    raise ValueError(f"unreadable member {member.name!r}")
                with open(target, "wb") as f:
                    f.write(extracted.read())
                os.chmod(target, 0o644)
                written += 1
        # Directories created implicitly for nested files also need the team
        # image's uid to traverse them.
        for root, dirs, _files in os.walk(dest):
            for d in dirs:
                os.chmod(os.path.join(root, d), 0o755)
    except (tarfile.TarError, ValueError, OSError) as exc:
        log.error("overlay from s3://%s/%s not applied: %s", bucket, key, exc)
        sys.exit(EXIT_UNSAFE)

    log.info("applied %d file(s) from s3://%s/%s into %s", written, bucket, key, dest)


if __name__ == "__main__":
    main()
