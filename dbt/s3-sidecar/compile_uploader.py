#!/usr/bin/env python3
"""Upload a locally-compiled dbt release's artifacts to S3 (the compile Job's main container).

The init container (the team image) runs `dbt compile` and copies its outputs
into a shared emptyDir; this main container reads them and put_objects them to
sibling keys under continuo's canonical <service>/<release_id>/ prefix. The team
image has no S3 access; this continuo-owned container does the upload. A nonzero
exit fails the compile Job, which release-controller treats as compile_failed.

manifest.json (COMPILE_MANIFEST_PATH -> MANIFEST_S3_URI) always uploads. The
runtime manifest artifacts (COMPILE_PARTIAL_PARSE_PATH and
COMPILE_RUNTIME_DESCRIPTOR_PATH) are a set: both or neither. Neither means the
team image predates the runtime exporter, which is a supported manifest-only
release. Exactly one means the exporter ran and failed partway, so nothing is
uploaded and the Job fails rather than publishing an artifact set no worker can
trust.
"""
import hashlib
import json
import os
import sys
from pathlib import Path

import s3_common


def _existing_path(name: str) -> Path | None:
    """Return the path in env var name, or None if it is unset or absent on disk."""
    value = os.environ.get(name)
    if not value:
        return None
    path = Path(value)
    return path if path.is_file() else None


def _runtime_objects(prefix: str) -> list[tuple[Path, str]]:
    """Resolve the runtime artifact set into (path, key) pairs, or [] if absent."""
    partial_parse = _existing_path("COMPILE_PARTIAL_PARSE_PATH")
    descriptor_path = _existing_path("COMPILE_RUNTIME_DESCRIPTOR_PATH")

    if partial_parse is None and descriptor_path is None:
        print("compile_uploader: runtime artifact unavailable; manifest-only compatibility upload")
        return []
    if partial_parse is None or descriptor_path is None:
        missing = "partial_parse.msgpack" if partial_parse is None else "runtime-manifest.json"
        raise RuntimeError(f"incomplete runtime artifact set: {missing} is missing")

    descriptor = json.loads(descriptor_path.read_text())
    packed = partial_parse.read_bytes()
    if hashlib.sha256(packed).hexdigest() != descriptor["sha256"]:
        raise RuntimeError("partial_parse.msgpack SHA does not match descriptor")

    return [
        (partial_parse, f"{prefix}/partial_parse.msgpack"),
        (descriptor_path, f"{prefix}/runtime-manifest.json"),
    ]


def main() -> None:
    path = s3_common.require_env("COMPILE_MANIFEST_PATH", caller="compile_uploader")
    try:
        bucket, manifest_key = s3_common.parse_s3_uri(
            s3_common.require_env("MANIFEST_S3_URI", caller="compile_uploader")
        )
    except ValueError as exc:
        print(f"compile_uploader: invalid MANIFEST_S3_URI: {exc}", file=sys.stderr)
        sys.exit(2)

    if not Path(path).is_file():
        print(f"compile_uploader: cannot read manifest {path}: not a file", file=sys.stderr)
        sys.exit(3)

    # The whole set is resolved and verified before anything is uploaded, so a
    # rejected set leaves no partial artifacts behind in S3.
    objects = [(Path(path), manifest_key)]
    try:
        objects.extend(_runtime_objects(manifest_key.rsplit("/", 1)[0]))
    except Exception as exc:
        print(f"compile_uploader: {exc}", file=sys.stderr)
        sys.exit(5)

    try:
        bodies = [(key, local.read_bytes()) for local, key in objects]
    except OSError as exc:
        print(f"compile_uploader: cannot read artifact: {exc}", file=sys.stderr)
        sys.exit(3)

    s3 = s3_common.make_s3_client()
    for key, body in bodies:
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body)
        except Exception as exc:
            print(
                f"compile_uploader: S3 upload to s3://{bucket}/{key} failed: {exc}",
                file=sys.stderr,
            )
            sys.exit(4)
        print(f"compile_uploader: uploaded -> s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
