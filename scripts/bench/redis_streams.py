"""Snapshot every Redis stream's entries-added counter.

entries-added only grows, so the difference between two snapshots counts the
messages published in between even if a stream was trimmed meanwhile. Redis
before 7.0 has no such counter; the length is used instead and flagged.
The redis-cli command prefix comes from BENCH_REDIS_CMD and the password from
BENCH_REDIS_PASSWORD (both set by lib.sh). The password is written to the
command's stdin, never into its arguments, so it cannot appear in a process
list or an error message.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys

log = logging.getLogger(__name__)


def entries_added(raw_lines: list) -> tuple:
    """Counter from raw `XINFO STREAM` output, as (value, exact)."""
    lines = [line.strip() for line in raw_lines]
    for key, exact in (("entries-added", True), ("length", False)):
        for index, line in enumerate(lines[:-1]):
            if line == key:
                return int(lines[index + 1]), exact
    raise ValueError("XINFO STREAM output has neither entries-added nor length")


def redis_command(args: list) -> list:
    prefix = json.loads(os.environ["BENCH_REDIS_CMD"])
    if not isinstance(prefix, list) or not all(isinstance(part, str) for part in prefix):
        raise ValueError("BENCH_REDIS_CMD must be a JSON list of strings")
    return prefix + list(args)


def _stdin(extra: bytes = b"") -> bytes:
    return os.environ.get("BENCH_REDIS_PASSWORD", "").encode("utf-8") + b"\n" + extra


def _redis(args: list) -> list:
    result = subprocess.run(redis_command(args), input=_stdin(), capture_output=True)
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace") if isinstance(result.stderr, bytes) else str(result.stderr)
        last = detail.strip().splitlines()[-1] if detail.strip() else "no output"
        raise RuntimeError(f"redis-cli {args[0]} exited {result.returncode}: {last}")
    out = result.stdout
    return (out.decode("utf-8") if isinstance(out, bytes) else out).splitlines()


def cli(args: list, stdin: bytes) -> int:
    """redis-cli ARGS with STDIN after the password line (for -x); its output passes through."""
    return subprocess.run(redis_command(args), input=_stdin(stdin)).returncode


def stream_names() -> list:
    """Every stream key, by following the SCAN cursor (SCAN ... TYPE needs Redis 6.0)."""
    names = set()
    cursor = "0"
    while True:
        lines = [line.strip() for line in _redis(["SCAN", cursor, "TYPE", "stream", "COUNT", "1000"])]
        cursor = lines[0]
        names.update(line for line in lines[1:] if line)
        if cursor == "0":
            return sorted(names)


def snapshot() -> dict:
    streams = stream_names()
    counts = {}
    inexact = 0
    for stream in streams:
        value, exact = entries_added(_redis(["XINFO", "STREAM", stream]))
        counts[stream] = value
        inexact += 0 if exact else 1
    if inexact:
        log.warning("no entries-added counter on %d streams; using length, which undercounts after trimming", inexact)
    return counts


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
    if not argv or argv[0] not in ("snapshot", "cli") or "BENCH_REDIS_CMD" not in os.environ:
        log.error("usage: BENCH_REDIS_CMD='[...]' BENCH_REDIS_PASSWORD=... redis_streams.py snapshot | cli ARGS...")
        return 2
    if argv[0] == "cli":
        return cli(argv[1:], sys.stdin.buffer.read())
    json.dump(snapshot(), sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
