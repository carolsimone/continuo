#!/usr/bin/env python3
"""The state Deployment must roll with strategy Recreate.

Every state pod consumes task.status.updated:v1 through one consumer group and,
in the status transaction, dispatches the tasks a success makes ready. During a
RollingUpdate the previous release's pod and the new one consume the same group,
so a success of a run the new pod planned can reach the old pod, which records
it without dispatching what it unblocks: the downstream tasks stay pending until
the dispatch watchdog cancels the run. Recreate stops the old pod before the new
one consumes anything.

Usage: assert-state-recreate.py <rendered-manifest.yaml>
"""
import logging
import pathlib
import sys

import yaml

logger = logging.getLogger("assert-state-recreate")


def main() -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    docs = [d for d in yaml.safe_load_all(pathlib.Path(sys.argv[1]).read_text()) if d]
    states = [d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"] == "state"]
    if len(states) != 1:
        logger.error("FAIL: expected exactly one state Deployment, found %d", len(states))
        return 1
    strategy = states[0]["spec"].get("strategy", {}).get("type")
    if strategy != "Recreate":
        logger.error("FAIL: state Deployment strategy is %r, want 'Recreate'", strategy)
        return 1
    logger.info("state rolls with Recreate")
    return 0


if __name__ == "__main__":
    sys.exit(main())
