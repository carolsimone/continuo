#!/usr/bin/env python3
"""Lay a fix's proposed source files over a Job's dbt project.

Runs as the `overlay` initContainer (s3-sidecar image) of the compile Job and
the seed-build Job of a fix's release check. Downloads SOURCE_OVERLAY_URI — a
gzip tarball whose member names are project-relative paths (e.g. models/orders.sql) — and
extracts it into OVERLAY_DEST; each team-image container of the Job then
copies its project into a writable workdir, copies that tree over the copy and
runs dbt there, so the run builds and validates the proposed fix rather than
the committed source.

A fix may also remove files: renaming a dbt model removes its old file. The
tarball then carries the reserved member `.continuo/delete-paths`, UTF-8 text
with one project-relative path per line (LF line ends, blank lines ignored),
each a file that must not exist in the staged project. That member is never
extracted. Each listed path is checked like a member name, and a path the
overlay also writes (as a file, or as a directory holding its files) is
refused. The paths, normalised, in the order given and without repeats, are
written one per line to the file OVERLAY_DELETE_LIST names, which lies outside
OVERLAY_DEST; the team containers remove each of them from their copy after
laying the overlay over it. A tarball without the member writes no list and
needs no OVERLAY_DELETE_LIST.

Every failure exits nonzero and fails the Job. A fix's release check exists
only to judge the fix; building the committed source instead would judge the
wrong thing, so there is deliberately no degraded path here.
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

# DELETE_LIST_MEMBER is the reserved tarball member listing the
# project-relative paths the overlay removes from the staged project.
DELETE_LIST_MEMBER = ".continuo/delete-paths"


def _safe_relative(name: str) -> str:
    """Return name normalised as a path inside the overlay, or raise ValueError
    when it is absolute or climbs out of the destination."""
    if name.startswith("/"):
        raise ValueError(f"absolute member path {name!r}")
    norm = posixpath.normpath(name)
    if norm == "." or norm.startswith("../") or norm == ".." or "/../" in f"/{norm}/":
        raise ValueError(f"member path escapes the overlay: {name!r}")
    return norm


def _delete_paths(text: bytes) -> list[str]:
    """Return the paths a delete list names, each normalised by _safe_relative,
    in the order given and without repeats. Blank lines are skipped; an unsafe
    path or text that is not UTF-8 raises ValueError."""
    paths: list[str] = []
    seen: set[str] = set()
    for line in text.decode("utf-8").split("\n"):
        if not line.strip():
            continue
        try:
            rel = _safe_relative(line)
        except ValueError as exc:
            raise ValueError(f"{DELETE_LIST_MEMBER} lists an unsafe path: {exc}") from exc
        if rel not in seen:
            seen.add(rel)
            paths.append(rel)
    return paths


def _read_overlay(tf: tarfile.TarFile) -> tuple[list[tuple[str, tarfile.TarInfo]], list[str] | None]:
    """Check every member of the tarball before anything is written.

    Returns the members to extract, each with its normalised path, and the
    paths the reserved delete list names (None when the tarball carries no
    delete list). Raises ValueError for an unsafe member path, a member that is
    neither a file nor a directory, a second delete list, or a listed path the
    overlay also writes."""
    entries: list[tuple[str, tarfile.TarInfo]] = []
    deletes: list[str] | None = None
    occupied: set[str] = set()
    for member in tf.getmembers():
        rel = _safe_relative(member.name)
        if rel == DELETE_LIST_MEMBER:
            if deletes is not None:
                raise ValueError(f"the overlay carries {DELETE_LIST_MEMBER} more than once")
            if not member.isfile():
                raise ValueError(f"{DELETE_LIST_MEMBER} is not a file")
            extracted = tf.extractfile(member)
            if extracted is None:
                raise ValueError(f"unreadable member {member.name!r}")
            deletes = _delete_paths(extracted.read())
            continue
        if not member.isdir() and not member.isfile():
            raise ValueError(f"unsupported member type for {member.name!r} (only files and directories)")
        entries.append((rel, member))
        occupied.add(rel)
        if member.isfile():
            parent = posixpath.dirname(rel)
            while parent:
                occupied.add(parent)
                parent = posixpath.dirname(parent)
    for rel in deletes or []:
        if rel in occupied:
            raise ValueError(f"{DELETE_LIST_MEMBER} lists {rel!r}, which the overlay also writes")
    return entries, deletes


def main() -> None:
    uri = os.environ.get("SOURCE_OVERLAY_URI", "")
    dest = os.environ.get("OVERLAY_DEST", "")
    delete_list = os.environ.get("OVERLAY_DELETE_LIST", "")
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
    deletes: list[str] | None = None
    try:
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as tf:
            entries, deletes = _read_overlay(tf)
            if deletes is not None and not delete_list:
                log.error(
                    "s3://%s/%s carries %s but OVERLAY_DELETE_LIST is not set, so the listed files would stay",
                    bucket, key, DELETE_LIST_MEMBER,
                )
                sys.exit(EXIT_CONFIG)
            # OVERLAY_DEST is a subdirectory this script creates itself (the
            # pod sets no fsGroup, and the team container that reads it back
            # runs as the team image's own uid), so its mode depends on this
            # process's umask unless set explicitly here — the same reason
            # every extracted file and directory below is chmod'd rather than
            # left at its default.
            os.makedirs(dest, exist_ok=True)
            os.chmod(dest, 0o755)
            for rel, member in entries:
                target = os.path.join(dest, rel)
                if member.isdir():
                    os.makedirs(target, exist_ok=True)
                    os.chmod(target, 0o755)
                    continue
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
        if deletes is not None:
            # The team container reads the list as the team image's uid.
            with open(delete_list, "wb") as f:
                f.write("".join(p + "\n" for p in deletes).encode("utf-8"))
            os.chmod(delete_list, 0o644)
    except (tarfile.TarError, ValueError, OSError) as exc:
        log.error("overlay from s3://%s/%s not applied: %s", bucket, key, exc)
        sys.exit(EXIT_UNSAFE)

    log.info("applied %d file(s) from s3://%s/%s into %s", written, bucket, key, dest)
    if deletes is not None:
        log.info("listed %d path(s) to remove from the staged project in %s", len(deletes), delete_list)


if __name__ == "__main__":
    main()
