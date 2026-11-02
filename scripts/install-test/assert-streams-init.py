#!/usr/bin/env python3
"""Assert the rendered streams-init Job: the one deploy step that creates every
Redis stream and consumer group of the release.

  - It is a Job on the dead-letter-controller image that runs /streams-init.
  - With a BYO Redis (--mode hook) it is a Helm hook (pre-install,pre-upgrade,
    weight -4, deleted before the next hook and once it succeeds): the Redis
    already exists, and the streams must exist before the services roll. With
    the bundled Redis (--mode job) it carries no hook annotation, because a
    pre-install hook runs before any release resource, Redis included.
  - REDIS_ADDR is the host:port of the release's Redis and REDIS_PASSWORD comes
    from a secretKeyRef. A hook runs before the release's regular Secrets
    exist, so a hook's secretKeyRef may name a Secret of the render only when
    that Secret is itself a hook ordered before the Job.
  - REDIS_INIT_CREATE_STREAMS carries redisInit.createStreams.

Usage: assert-streams-init.py <rendered-manifest.yaml> --mode hook|job
           --create-streams true|false
"""
import argparse
import logging
import sys

import yaml

logger = logging.getLogger("assert-streams-init")

HOOK = "helm.sh/hook"
WEIGHT = "helm.sh/hook-weight"
DELETE_POLICY = "helm.sh/hook-delete-policy"
JOB_NAME = "streams-init"


def hook_weight(doc):
    return int(((doc["metadata"].get("annotations") or {}).get(WEIGHT)) or 0)


def check_job(docs, mode, create_streams):
    failures = []
    jobs = [d for d in docs if d.get("kind") == "Job"
            and (d["metadata"].get("labels") or {}).get("app.kubernetes.io/name") == JOB_NAME]
    if len(jobs) != 1:
        return [f"want exactly one {JOB_NAME} Job, the render holds {len(jobs)}"]
    job = jobs[0]
    annotations = job["metadata"].get("annotations") or {}
    pod = job["spec"]["template"]["spec"]
    containers = pod["containers"]
    if len(containers) != 1:
        return [f"the {JOB_NAME} pod must hold one container, it holds {len(containers)}"]
    container = containers[0]

    if mode == "hook":
        if annotations.get(HOOK) != "pre-install,pre-upgrade":
            failures.append(f"{HOOK} is {annotations.get(HOOK)!r}, want 'pre-install,pre-upgrade' (BYO Redis)")
        if annotations.get(WEIGHT) != "-4":
            failures.append(f"{WEIGHT} is {annotations.get(WEIGHT)!r}, want '-4'")
        if annotations.get(DELETE_POLICY) != "before-hook-creation,hook-succeeded":
            failures.append(f"{DELETE_POLICY} is {annotations.get(DELETE_POLICY)!r}, "
                            "want 'before-hook-creation,hook-succeeded'")
    else:
        stray = sorted(k for k in annotations if k.startswith("helm.sh/hook"))
        if stray:
            failures.append(f"the bundled-Redis Job must not be a hook, it carries {stray}")
        if not job["metadata"]["name"].rsplit("-", 1)[-1].startswith("r"):
            failures.append(f"the bundled-Redis Job name {job['metadata']['name']!r} lacks a -r<revision> suffix")

    if "continuo-dead-letter-controller" not in container["image"]:
        failures.append(f"image {container['image']!r} is not the dead-letter-controller image")
    if container.get("command") != ["/streams-init"]:
        failures.append(f"command is {container.get('command')!r}, want ['/streams-init']")

    env = {e["name"]: e for e in container.get("env") or []}
    for name in ("REDIS_ADDR", "REDIS_PASSWORD", "REDIS_INIT_CREATE_STREAMS", "LOG_LEVEL"):
        if name not in env:
            failures.append(f"env {name} is missing")
    if "REDIS_INIT_CREATE_STREAMS" in env and env["REDIS_INIT_CREATE_STREAMS"].get("value") != create_streams:
        failures.append(f"REDIS_INIT_CREATE_STREAMS is {env['REDIS_INIT_CREATE_STREAMS'].get('value')!r}, "
                        f"want {create_streams!r}")
    addr = (env.get("REDIS_ADDR") or {}).get("value", "")
    host, _, port = addr.rpartition(":")
    if not host or not port.isdigit():
        failures.append(f"REDIS_ADDR {addr!r} is not host:port")

    ref = (((env.get("REDIS_PASSWORD") or {}).get("valueFrom") or {}).get("secretKeyRef")) or {}
    if not ref.get("name") or not ref.get("key"):
        failures.append("REDIS_PASSWORD is not a secretKeyRef")
    elif mode == "hook":
        secrets = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Secret"}
        secret = secrets.get(ref["name"])
        if secret is not None:
            secret_annotations = secret["metadata"].get("annotations") or {}
            if "pre-install" not in secret_annotations.get(HOOK, ""):
                failures.append(f"the hook reads Secret {ref['name']}, a regular resource Helm creates only "
                                "after pre-install hooks: the pod would never start")
            elif hook_weight(secret) >= hook_weight(job):
                failures.append(f"hook Secret {ref['name']} (weight {hook_weight(secret)}) is not created "
                                f"before the Job (weight {hook_weight(job)})")
            elif ref["key"] not in (secret.get("stringData") or {}) | (secret.get("data") or {}):
                failures.append(f"hook Secret {ref['name']} has no key {ref['key']!r}")

    security = container.get("securityContext") or {}
    if security.get("allowPrivilegeEscalation") is not False:
        failures.append("allowPrivilegeEscalation must be false")
    if security.get("readOnlyRootFilesystem") is not True:
        failures.append("readOnlyRootFilesystem must be true")
    if not container.get("resources", {}).get("limits"):
        failures.append("the container sets no resource limits")
    if pod.get("restartPolicy") != "OnFailure":
        failures.append(f"restartPolicy is {pod.get('restartPolicy')!r}, want 'OnFailure'")
    return failures


def main(argv):
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("manifest")
    parser.add_argument("--mode", choices=("hook", "job"), required=True)
    parser.add_argument("--create-streams", choices=("true", "false"), required=True)
    args = parser.parse_args(argv[1:])
    with open(args.manifest) as handle:
        docs = [d for d in yaml.safe_load_all(handle) if d]
    failures = check_job(docs, args.mode, args.create_streams)
    for failure in failures:
        logger.error("FAIL: %s: %s", args.manifest, failure)
    if failures:
        return 1
    logger.info("streams-init Job OK (%s, createStreams=%s)", args.mode, args.create_streams)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
