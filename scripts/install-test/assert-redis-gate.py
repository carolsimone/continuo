#!/usr/bin/env python3
"""Every chart-deployed service whose code connects to Redis must carry the
wait-for-redis init container when the chart bundles Redis, and no Deployment
may carry it when Redis is BYO.

A bundled Redis is a StatefulSet with no ordering guarantee against the
Deployments, so a Redis client that starts first logs connection failures, or
crash-loops, before Redis is ready. Which services connect to Redis is taken
from their own source, so the chart cannot drift from the code:
  - Go services:  any non-test .go file importing github.com/redis/go-redis
  - Python:       pyproject.toml declaring the `redis` distribution
  - Node:         package.json depending on `ioredis` or `redis`

A Deployment that has a service directory in the repo but none of those
manifests fails the check rather than being skipped, so a restructured service
cannot silently leave the guard. Deployments with no service directory
(third-party images such as dex) are not governed by repo code and are not
checked.

Usage: assert-redis-gate.py <repo-root> <bundled-manifest.yaml> <byo-manifest.yaml>...
"""
import json
import logging
import pathlib
import re
import sys

import yaml

logger = logging.getLogger("assert-redis-gate")

GATE = "wait-for-redis"
GO_REDIS_IMPORT = re.compile(r'"github\.com/redis/go-redis/v\d+"')
# A quoted PEP 508 requirement whose distribution name is exactly `redis`, as
# it appears in a pyproject.toml dependency array. Matched textually so the
# check runs on Pythons without tomllib.
PY_REDIS_REQ = re.compile(r"""^\s*["']redis\s*(?:[\[<>=!~;@ ]|["'])""", re.IGNORECASE | re.MULTILINE)
NODE_REDIS_PACKAGES = ("ioredis", "redis")
SKIP_DIRS = {"node_modules", ".venv", "vendor", "dist", "build"}


def go_uses_redis(service_dir: pathlib.Path) -> bool:
    for source in service_dir.rglob("*.go"):
        if source.name.endswith("_test.go") or SKIP_DIRS.intersection(source.relative_to(service_dir).parts):
            continue
        if GO_REDIS_IMPORT.search(source.read_text()):
            return True
    return False


def python_uses_redis(pyproject: pathlib.Path) -> bool:
    return PY_REDIS_REQ.search(pyproject.read_text()) is not None


def node_uses_redis(package_json: pathlib.Path) -> bool:
    deps = json.loads(package_json.read_text()).get("dependencies", {})
    return any(name in deps for name in NODE_REDIS_PACKAGES)


def uses_redis(service_dir: pathlib.Path):
    """True/False from the service's own manifests; None if none is recognised."""
    if (service_dir / "go.mod").exists():
        return go_uses_redis(service_dir)
    if (service_dir / "pyproject.toml").exists():
        return python_uses_redis(service_dir / "pyproject.toml")
    if (service_dir / "package.json").exists():
        return node_uses_redis(service_dir / "package.json")
    return None


def gated_deployments(manifest: pathlib.Path) -> dict:
    """Deployment name -> whether its pod template has the Redis gate."""
    out = {}
    for doc in yaml.safe_load_all(manifest.read_text()):
        if not doc or doc.get("kind") != "Deployment":
            continue
        init = doc["spec"]["template"]["spec"].get("initContainers") or []
        out[doc["metadata"]["name"]] = any(c.get("name") == GATE for c in init)
    return out


def main() -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    if len(sys.argv) < 4:
        logger.error("usage: assert-redis-gate.py <repo-root> <bundled-manifest> <byo-manifest>...")
        return 2
    repo = pathlib.Path(sys.argv[1])
    bundled = pathlib.Path(sys.argv[2])
    byo_manifests = [pathlib.Path(p) for p in sys.argv[3:]]
    failures = []

    gated = []
    for name, has_gate in sorted(gated_deployments(bundled).items()):
        service_dir = repo / name
        if not service_dir.is_dir():
            continue
        want = uses_redis(service_dir)
        if want is None:
            failures.append(f"{name}: has a service directory but no go.mod, pyproject.toml or package.json to read its Redis use from")
        elif want and not has_gate:
            failures.append(f"{name}: its code connects to Redis but the bundled render has no {GATE} init container")
        elif has_gate and not want:
            failures.append(f"{name}: carries {GATE} but its code never connects to Redis")
        elif has_gate:
            gated.append(name)
    if not gated and not failures:
        failures.append(f"no Deployment in {bundled} carries {GATE}; expected every Redis-using service to")

    for manifest in byo_manifests:
        for name, has_gate in sorted(gated_deployments(manifest).items()):
            if has_gate:
                failures.append(f"{name}: BYO render {manifest} carries {GATE}; BYO Redis is already running and must not be gated")

    for f in failures:
        logger.error("FAIL: %s", f)
    if not failures:
        logger.info("%s present on every Redis-using service (%s) and absent in %d BYO render(s)",
                    GATE, ", ".join(gated), len(byo_manifests))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
