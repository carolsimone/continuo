"""Read kubectl JSON (stdin) for the benchmark: env-var sources, Secret and ConfigMap values, and Job
lists slimmed to what the metrics read.

secret-key prints a decoded Secret value on stdout for a shell variable to
capture; nothing in this module logs a value.
"""
from __future__ import annotations

import base64
import json
import logging
import sys

log = logging.getLogger(__name__)


def env_sources(deploy: dict, var: str) -> list:
    """Where a Deployment's env VAR comes from, in lookup order.

    An explicit env entry wins: ("value", v), ("secret", name, key) or ("configmap", name, key).
    Otherwise every envFrom source is a candidate: ("envfrom-configmap", name), ("envfrom-secret", name).
    """
    containers = deploy.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    for container in containers:
        for env in container.get("env") or []:
            if env.get("name") != var:
                continue
            if "value" in env:
                return [("value", env["value"])]
            ref = env.get("valueFrom") or {}
            if "secretKeyRef" in ref:
                return [("secret", ref["secretKeyRef"]["name"], ref["secretKeyRef"]["key"])]
            if "configMapKeyRef" in ref:
                return [("configmap", ref["configMapKeyRef"]["name"], ref["configMapKeyRef"]["key"])]
    candidates = []
    for container in containers:
        for source in container.get("envFrom") or []:
            if "configMapRef" in source:
                candidates.append(("envfrom-configmap", source["configMapRef"]["name"]))
            if "secretRef" in source:
                candidates.append(("envfrom-secret", source["secretRef"]["name"]))
    return candidates


def slim_jobs(jobs_doc: dict) -> dict:
    """A Job list with only what the metrics read: name, creation time, labels, status and TABLE_NAME.

    A task Job's pod spec carries literal credentials (the parse-cache init
    container's S3 keys); none of it is kept, so a saved Job list holds no secret.
    """
    items = []
    for job in jobs_doc.get("items", []):
        metadata = job.get("metadata", {})
        containers = job.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        items.append({
            "metadata": {key: metadata[key] for key in ("name", "creationTimestamp", "labels") if key in metadata},
            "spec": {"template": {"spec": {"containers": [
                {"name": container.get("name"),
                 "env": [env for env in container.get("env") or [] if env.get("name") == "TABLE_NAME"]}
                for container in containers]}}},
            "status": job.get("status") or {},
        })
    return {"items": items}


def data_value(doc: dict, key: str, encoded: bool) -> str:
    value = (doc.get("data") or {}).get(key)
    if value is None:
        raise KeyError(key)
    return base64.b64decode(value).decode("utf-8") if encoded else value


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
    if not argv:
        log.error("usage: k8s_json.py env-source VAR | slim-jobs | secret-key KEY | configmap-key KEY")
        return 2
    doc = json.load(sys.stdin)
    command = argv[0]
    if command == "env-source" and len(argv) == 2:
        for source in env_sources(doc, argv[1]):
            print("\t".join(source))
        return 0
    if command == "slim-jobs" and len(argv) == 1:
        json.dump(slim_jobs(doc), sys.stdout)
        sys.stdout.write("\n")
        return 0
    if command in ("secret-key", "configmap-key") and len(argv) == 2:
        try:
            sys.stdout.write(data_value(doc, argv[1], encoded=command == "secret-key"))
        except KeyError:
            return 1
        return 0
    log.error("unknown command: %s", " ".join(argv))
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
