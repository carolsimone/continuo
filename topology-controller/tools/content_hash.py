"""Print the content_hash of every node a dbt manifest yields, through the component's own parser.

    python tools/content_hash.py <directory>

The directory holds the manifest.json, and a compile-extras.json beside it when the
manifest's compile wrote one. The parse is the release's: the same function with a
fresh contract validator and timing record. Stdout is one JSON line, `{unique_id:
content_hash}` of each node the parse yields, sorted by unique_id. Diagnostics go to
stderr. The e2e tests run it in the topology-controller container, because the parser
is the one implementation of the formula and they ask it rather than keep a copy.
"""
import json
import logging
import os
import sys

from adapters.process import MonotonicClock, ResidentMemoryProbe
from service.compile_extras import load_compile_extras
from service.contract_validation import ContractValidator
from service.parser import parse_manifest
from service.release_timings import ReleaseTimings


def content_hashes(directory: str) -> dict[str, str]:
    extras_path = os.path.join(directory, "compile-extras.json")
    extras = load_compile_extras(extras_path, "") if os.path.exists(extras_path) else None
    parsed = parse_manifest(
        os.path.join(directory, "manifest.json"), "", extras,
        validator=ContractValidator(), timings=ReleaseTimings("content-hash", MonotonicClock(), ResidentMemoryProbe()),
    )
    return {node.unique_id: node.content_hash for node in parsed.nodes}


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
    print(json.dumps(content_hashes(sys.argv[1]), sort_keys=True))
