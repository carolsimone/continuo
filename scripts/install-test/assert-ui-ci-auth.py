#!/usr/bin/env python3
"""The ui Deployment must receive CI auth from the template itself, not only
from its `services` entry: exactly one CI_AUTH_CONFIG_PATH env var pointing at
the mounted file, exactly one read-only ci-auth mount at /app/ci-auth, and
exactly one ci-auth volume backed by the <fullname>-ci-auth ConfigMap, whose
ci-auth.json parses.

With --default-ci-auth the rendered ci-auth.json must also carry the GitHub
Actions issuer and no bindings (the render for a release that has no ciAuth
values, as after `helm upgrade --reuse-values` from a chart without them).

Usage: assert-ui-ci-auth.py <rendered-manifest.yaml> <fullname> [--default-ci-auth]
"""
import json
import logging
import pathlib
import sys

import yaml

logger = logging.getLogger("assert-ui-ci-auth")

CONFIG_PATH = "/app/ci-auth/ci-auth.json"
GITHUB_ISSUER = "https://token.actions.githubusercontent.com"


def main() -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    manifest, fullname = pathlib.Path(sys.argv[1]), sys.argv[2]
    default_ci_auth = "--default-ci-auth" in sys.argv[3:]
    configmap_name = f"{fullname}-ci-auth"
    docs = [d for d in yaml.safe_load_all(manifest.read_text()) if d]
    failures = []

    uis = [d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"] == "ui"]
    if len(uis) != 1:
        logger.error("FAIL: expected exactly one ui Deployment, found %d", len(uis))
        return 1
    pod = uis[0]["spec"]["template"]["spec"]
    container = next(c for c in pod["containers"] if c["name"] == "ui")

    envs = [e for e in container.get("env", []) if e["name"] == "CI_AUTH_CONFIG_PATH"]
    if [e.get("value") for e in envs] != [CONFIG_PATH]:
        failures.append(f"ui CI_AUTH_CONFIG_PATH entries {envs!r}, want exactly one = {CONFIG_PATH}")
    mounts = [m for m in container.get("volumeMounts", []) if m["name"] == "ci-auth"]
    if len(mounts) != 1 or mounts[0].get("mountPath") != "/app/ci-auth" or mounts[0].get("readOnly") is not True:
        failures.append(f"ui ci-auth volumeMounts {mounts!r}, want exactly one read-only at /app/ci-auth")
    volumes = [v for v in pod.get("volumes", []) if v["name"] == "ci-auth"]
    if len(volumes) != 1 or volumes[0].get("configMap", {}).get("name") != configmap_name:
        failures.append(f"ui ci-auth volumes {volumes!r}, want exactly one from ConfigMap {configmap_name}")

    configmaps = [d for d in docs if d.get("kind") == "ConfigMap" and d["metadata"]["name"] == configmap_name]
    if len(configmaps) != 1:
        failures.append(f"expected exactly one ConfigMap {configmap_name}, found {len(configmaps)}")
    else:
        ci_auth = json.loads(configmaps[0]["data"]["ci-auth.json"])
        if default_ci_auth and (ci_auth.get("issuer") != GITHUB_ISSUER or ci_auth.get("bindings") != {}):
            failures.append(f"ci-auth.json {ci_auth!r}, want issuer {GITHUB_ISSUER} and empty bindings")

    for f in failures:
        logger.error("FAIL: %s", f)
    if not failures:
        logger.info("ui receives CI auth from the template (%s)", manifest.name)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
