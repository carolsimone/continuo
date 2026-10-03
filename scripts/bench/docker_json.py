"""Read `docker inspect` JSON (stdin) for the benchmark: a container's networks and their aliases."""
from __future__ import annotations

import json
import logging
import sys

log = logging.getLogger(__name__)


def network_aliases(inspect_doc: list) -> list:
    """(network, aliases) for every network the inspected container is attached to, sorted by network."""
    networks = inspect_doc[0].get("NetworkSettings", {}).get("Networks") or {}
    return [(name, list(settings.get("Aliases") or [])) for name, settings in sorted(networks.items())]


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
    if argv != ["network-aliases"]:
        log.error("usage: docker inspect CONTAINER | docker_json.py network-aliases")
        return 2
    for name, aliases in network_aliases(json.load(sys.stdin)):
        print("\t".join([name] + aliases))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
