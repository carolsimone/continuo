#!/usr/bin/env python3
"""Every chart-deployed service must receive its HTTP port under the env name
its code reads, set to the Deployment's `http` containerPort, and must not
receive a port variable it never reads (which only looks like configuration).

The variable a service reads is taken from its own source, so the chart cannot
drift from the code:
  - Go services:        <service>/config/config.go   (HTTPPort / HealthPort field)
  - topology-controller: <service>/config/config.py   (HTTP_PORT = int(os.environ.get(...)))
  - ui:                  <service>/src/server/index.ts (PORT = parseInt(process.env.X))

A Deployment that exposes an `http` port and has a service directory in the
repo, but whose source form is not recognised, fails the check rather than
being skipped, so a refactor of a config file cannot silently take a service
out of the guard. Deployments with no service directory (third-party images
such as dex) are not governed by repo code and are not checked.

Usage: assert-port-env.py <rendered-manifest.yaml> <repo-root>
"""
import logging
import pathlib
import re
import sys

import yaml

logger = logging.getLogger("assert-port-env")

# Each entry: (source file relative to the service dir, regex capturing the env name).
SOURCES = (
    ("config/config.go", re.compile(r'(?:HTTPPort|HealthPort):\s*[\w.]*\(\s*"([A-Z0-9_]+)"')),
    ("config/config.py", re.compile(r'HTTP_PORT\s*=\s*int\(os\.environ\.get\("([A-Z0-9_]+)"')),
    ("src/server/index.ts", re.compile(r'\bPORT\s*=\s*parseInt\(process\.env\.([A-Z0-9_]+)')),
)
# Env names that carry an HTTP/health listen port. gRPC, Redis and other
# *_PORT variables are deliberately not matched.
PORT_NAME_RE = re.compile(r"^(?:(?:[A-Z0-9_]+_)?(?:HTTP|HEALTH)_PORT|PORT)$")


def port_var(repo: pathlib.Path, service: str):
    for relative, pattern in SOURCES:
        source = repo / service / relative
        if source.exists():
            m = pattern.search(source.read_text())
            if m:
                return m.group(1)
    return None


def main() -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    manifest, repo = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
    failures = []
    checked = []
    for doc in yaml.safe_load_all(manifest.read_text()):
        if not doc or doc.get("kind") != "Deployment":
            continue
        name = doc["metadata"]["name"]
        container = doc["spec"]["template"]["spec"]["containers"][0]
        port = next((str(p["containerPort"]) for p in container.get("ports", []) if p.get("name") == "http"), None)
        if port is None:
            continue
        if not (repo / name).is_dir():
            continue
        want = port_var(repo, name)
        if want is None:
            failures.append(f"{name}: exposes an http port but the env var its code reads for it could not be determined")
            continue
        env = {e["name"]: e.get("value") for e in container.get("env", [])}
        if env.get(want) != port:
            failures.append(f"{name}: reads {want}, chart sets {want}={env.get(want)!r}, http containerPort={port}")
        for key in env:
            if PORT_NAME_RE.match(key) and key != want:
                failures.append(f"{name}: chart sets {key}, which the service never reads (it reads {want})")
        checked.append(f"{name}={want}")
    for f in failures:
        logger.error("FAIL: %s", f)
    if not failures:
        logger.info("port env names match code for: %s", ", ".join(checked))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
