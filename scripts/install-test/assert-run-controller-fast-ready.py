#!/usr/bin/env python3
"""run-controller must turn Ready as soon as it can serve.

run-controller is one pod that rolls with Recreate, so from the moment the old
pod stops until the new one is Ready the Service has no endpoint, and every read
of a run fails. The process serves about a second after it starts, so a fixed
readiness delay would only lengthen that gap on every deploy. A startup probe
checks the readiness path every second instead, and no readiness delay follows
it. The startup probe must also outlast the process's own 5-minute wait for
Redis (pkg/redis.WaitForRedis): running out of failures restarts the container,
which a readiness probe never does.

Usage: assert-run-controller-fast-ready.py <rendered-manifest.yaml>
"""
import logging
import pathlib
import sys

import yaml

logger = logging.getLogger("assert-run-controller-fast-ready")

MINIMUM_STARTUP_ALLOWANCE_SECONDS = 360


def main() -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    docs = [d for d in yaml.safe_load_all(pathlib.Path(sys.argv[1]).read_text()) if d]
    deployments = [
        d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"] == "run-controller"
    ]
    if len(deployments) != 1:
        logger.error("FAIL: expected exactly one run-controller Deployment, found %d", len(deployments))
        return 1
    container = next(
        c for c in deployments[0]["spec"]["template"]["spec"]["containers"] if c["name"] == "run-controller"
    )
    readiness = container["readinessProbe"]
    startup = container.get("startupProbe")
    if startup is None:
        logger.error("FAIL: run-controller has no startupProbe")
        return 1
    if startup.get("httpGet") != readiness.get("httpGet"):
        logger.error(
            "FAIL: startupProbe checks %r, want the readiness check %r",
            startup.get("httpGet"),
            readiness.get("httpGet"),
        )
        return 1
    if startup.get("periodSeconds") != 1:
        logger.error("FAIL: startupProbe periodSeconds is %r, want 1", startup.get("periodSeconds"))
        return 1
    allowance = startup.get("periodSeconds", 10) * startup.get("failureThreshold", 3)
    if allowance < MINIMUM_STARTUP_ALLOWANCE_SECONDS:
        logger.error(
            "FAIL: startupProbe allows %ds of startup, want at least %ds",
            allowance,
            MINIMUM_STARTUP_ALLOWANCE_SECONDS,
        )
        return 1
    if readiness.get("initialDelaySeconds", 0) != 0:
        logger.error(
            "FAIL: readinessProbe initialDelaySeconds is %r, want 0",
            readiness.get("initialDelaySeconds"),
        )
        return 1
    logger.info("run-controller turns Ready as soon as it can serve")
    return 0


if __name__ == "__main__":
    sys.exit(main())
