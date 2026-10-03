"""Resolve a Redis stream name from pkg/streams/contract.yaml by its Go constant.

The benchmark never hard-codes a versioned stream name; it reads the contract
file that the stream-name generator also reads.
"""
from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

log = logging.getLogger(__name__)

CONTRACT = Path(__file__).resolve().parents[2] / "pkg" / "streams" / "contract.yaml"
_ENTRY = re.compile(r"^\s*-\s*name:\s*(?P<name>\S+)\s*\n\s*const:\s*(?P<const>\S+)\s*$", re.MULTILINE)


def stream_by_const(const: str, contract: Path = CONTRACT) -> str:
    text = contract.read_text(encoding="utf-8")
    for match in _ENTRY.finditer(text):
        if match.group("const") == const:
            return match.group("name")
    raise KeyError(f"stream constant {const!r} not found in {contract}")


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
    if len(argv) != 1:
        log.error("usage: contract.py <StreamConst>")
        return 2
    try:
        print(stream_by_const(argv[0]))
    except KeyError as err:
        log.error("%s", err)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
