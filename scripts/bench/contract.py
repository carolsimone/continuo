"""Resolve names from pkg/streams/contract.yaml: a Redis stream by its Go
constant, and a vocabulary value by its vocabulary and constant.

The benchmark never hard-codes a versioned stream name or a shared vocabulary
value; it reads the contract file that the generator also reads.
"""
from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

log = logging.getLogger(__name__)

CONTRACT = Path(__file__).resolve().parents[2] / "pkg" / "streams" / "contract.yaml"
_ENTRY = re.compile(r"^\s*-\s*name:\s*(?P<name>\S+)\s*\n\s*const:\s*(?P<const>\S+)\s*$", re.MULTILINE)
_VALUE = re.compile(r"^\s*-\s*value:\s*(?P<value>\S+)\s*\n\s*const:\s*(?P<const>\S+)\s*$", re.MULTILINE)
_VOCABULARIES = "\nvocabularies:"


def _sections(contract: Path) -> tuple:
    """(streams section, vocabularies section) of the contract text."""
    text = contract.read_text(encoding="utf-8")
    streams, _, vocabularies = text.partition(_VOCABULARIES)
    return streams, vocabularies


def stream_by_const(const: str, contract: Path = CONTRACT) -> str:
    for match in _ENTRY.finditer(_sections(contract)[0]):
        if match.group("const") == const:
            return match.group("name")
    raise KeyError(f"stream constant {const!r} not found in {contract}")


def vocabulary_value(vocabulary: str, const: str, contract: Path = CONTRACT) -> str:
    """The value named CONST inside the vocabulary named VOCABULARY."""
    blocks = re.split(r"^  - name:\s*", _sections(contract)[1], flags=re.MULTILINE)
    for block in blocks[1:]:
        if block.split(None, 1)[0] != vocabulary:
            continue
        for match in _VALUE.finditer(block):
            if match.group("const") == const:
                return match.group("value")
    raise KeyError(f"value {const!r} not found in vocabulary {vocabulary!r} of {contract}")


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
