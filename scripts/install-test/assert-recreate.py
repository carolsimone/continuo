#!/usr/bin/env python3
"""The named Deployments must roll with strategy Recreate.

run-controller: every pod consumes task.status.updated:v1 through one consumer
group and, in the status transaction, dispatches the tasks a success makes
ready. During a RollingUpdate the previous release's pod and the new one
consume the same group, so a success of a run the new pod planned can reach the
old pod, which records it without dispatching what it unblocks: the downstream
tasks stay pending until the run fails as stalled. Recreate stops the old pod
before the new one consumes anything.

agent-remediation: at every start it fails the attempt rows that are in flight
and have no remediation. During a RollingUpdate the previous release's pod can
still write such a row after that sweep (a handler that finishes its model call
upserts `verifying`), and nothing would finish it: the row blocks "Try again"
until the next restart. Recreate stops the old pod before the new one starts.

Usage: assert-recreate.py <rendered-manifest.yaml> <component> [<component>...]
"""
import logging
import pathlib
import sys

import yaml

logger = logging.getLogger("assert-recreate")


def main() -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    if len(sys.argv) < 3:
        logger.error("usage: assert-recreate.py <rendered-manifest.yaml> <component> [<component>...]")
        return 2
    docs = [d for d in yaml.safe_load_all(pathlib.Path(sys.argv[1]).read_text()) if d]
    failed = False
    for component in sys.argv[2:]:
        deployments = [
            d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"] == component
        ]
        if len(deployments) != 1:
            logger.error("FAIL: expected exactly one %s Deployment, found %d", component, len(deployments))
            failed = True
            continue
        strategy = deployments[0]["spec"].get("strategy", {}).get("type")
        if strategy != "Recreate":
            logger.error("FAIL: %s Deployment strategy is %r, want 'Recreate'", component, strategy)
            failed = True
            continue
        logger.info("%s rolls with Recreate", component)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
