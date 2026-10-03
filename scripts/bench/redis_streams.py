"""Snapshot every Redis stream's entries-added counter.

entries-added only grows, so the difference between two snapshots counts the
messages published in between even if a stream was trimmed meanwhile. Redis
before 7.0 has no such counter; the length is used instead and flagged.
The redis-cli command prefix comes from BENCH_REDIS_CMD (set by lib.sh).
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


def _redis(args: list) -> list:
    return subprocess.run(redis_command(args), check=True, capture_output=True, text=True).stdout.splitlines()


def snapshot() -> dict:
    streams = sorted(line.strip() for line in _redis(["--scan", "--type", "stream"]) if line.strip())
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
    if argv != ["snapshot"] or "BENCH_REDIS_CMD" not in os.environ:
        log.error("usage: BENCH_REDIS_CMD='[...]' redis_streams.py snapshot")
        return 2
    json.dump(snapshot(), sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
