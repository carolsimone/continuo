#!/usr/bin/env python3
"""Assert the rendered streams-init Job: the one deploy step that creates every
Redis stream and consumer group of the release.

  - It is a Job on the dead-letter-controller image (the image and pull policy
    of the dead-letter-controller Deployment) that runs /streams-init with no
    arguments, with the container and pod securityContext of that Deployment
    (no privilege escalation, a read-only root filesystem, every capability
    dropped, user and group 65532), backoffLimit 6 and restartPolicy OnFailure.
  - With a BYO Redis (--mode hook) it is a Helm hook (pre-install,pre-upgrade,
    weight -4, deleted before the next hook and once it succeeds, and no other
    annotation), ordered after the migrate Job when that Job is a hook, kept
    for an hour after it finishes, with no init container. With the bundled
    Redis (--mode job) it carries no hook annotation, because a pre-install
    hook runs before any release resource, Redis included: it is a
    revision-named regular Job, kept for a day, whose one init container
    (wait-for-redis, on the bundled Redis image) waits for the Redis the main
    container connects to, signing in with the same password.
  - Its env is exactly REDIS_ADDR (the shared ConfigMap's), REDIS_PASSWORD (a
    secretKeyRef), REDIS_ACL_MODE (--acl-mode), REDIS_INIT_CREATE_STREAMS
    (--create-streams) and LOG_LEVEL (the shared ConfigMap's), plus
    REDIS_USERNAME and REDIS_LOGINS_DIR in perService mode, which also mounts
    the redis-logins volume and nothing else. Which login and which files
    those are is assert-redis-logins.py's to check.
  - A hook runs before the release's regular resources exist, so a hook Job
    reads no ConfigMap of the render, and every Secret of the render it reads
    (an env secretKeyRef or a volume) is itself a pre-install hook with a lower
    weight that holds the key it reads. The one Secret the chart renders for
    streams-init is that hook Secret, and only a shared-mode hook whose
    password the chart holds (no existingSecret) reads it.

Usage: assert-streams-init.py <rendered-manifest.yaml> --mode hook|job
           --create-streams true|false --acl-mode perService|shared
"""
import argparse
import logging
import re
import sys

import yaml

logger = logging.getLogger("assert-streams-init")

HOOK = "helm.sh/hook"
WEIGHT = "helm.sh/hook-weight"
DELETE_POLICY = "helm.sh/hook-delete-policy"
JOB_NAME = "streams-init"
MIGRATE_NAME = "db-init-migrate"
CONFIGMAP = "continuo-config"
HOOK_ANNOTATIONS = {
    HOOK: "pre-install,pre-upgrade",
    WEIGHT: "-4",
    DELETE_POLICY: "before-hook-creation,hook-succeeded",
}
HOOK_REF_ANNOTATIONS = {
    HOOK: "pre-install,pre-upgrade",
    WEIGHT: "-10",
    DELETE_POLICY: "before-hook-creation,hook-succeeded",
}
CONTAINER_SECURITY = {
    "allowPrivilegeEscalation": False,
    "readOnlyRootFilesystem": True,
    "capabilities": {"drop": ["ALL"]},
}
COMMON_ENV = {"REDIS_ADDR", "REDIS_PASSWORD", "REDIS_ACL_MODE", "REDIS_INIT_CREATE_STREAMS", "LOG_LEVEL"}
MAIN_ENV = {
    "shared": COMMON_ENV,
    "perService": COMMON_ENV | {"REDIS_USERNAME", "REDIS_LOGINS_DIR"},
}
WAIT_ENV = {
    "shared": {"REDISCLI_AUTH", "REDIS_HOST", "REDIS_PORT"},
    "perService": {"REDISCLI_AUTH", "REDIS_USERNAME", "REDIS_HOST", "REDIS_PORT"},
}
TTL = {"hook": 3600, "job": 86400}


def annotations_of(doc):
    return doc["metadata"].get("annotations") or {}


def hook_weight(doc):
    return int(annotations_of(doc).get(WEIGHT) or 0)


def helm_annotations(doc):
    return {k: v for k, v in annotations_of(doc).items() if k.startswith("helm.sh/hook")}


def labelled(docs, kind, name):
    return [d for d in docs if d.get("kind") == kind
            and (d["metadata"].get("labels") or {}).get("app.kubernetes.io/name") == name]


def env_list_names(container):
    return [e["name"] for e in container.get("env") or []]


def key_ref(entry):
    return ((entry or {}).get("valueFrom") or {}).get("secretKeyRef")


def check_env_names(failures, where, container, want):
    names = env_list_names(container)
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        failures.append(f"{where}: env sets {dupes} more than once")
    if set(names) != want:
        failures.append(f"{where}: env is {sorted(set(names))}, want exactly {sorted(want)}")
    if container.get("envFrom"):
        failures.append(f"{where}: envFrom is set; the Job's env must be exactly {sorted(want)}")


def key_reads(pod):
    """Every (secret name, key) the pod reads: env secretKeyRefs of every
    container and the items of its secret and projected-secret volumes (key
    None for a whole-Secret volume)."""
    reads = []
    for c in list(pod.get("initContainers") or []) + list(pod.get("containers") or []):
        for e in c.get("env") or []:
            ref = key_ref(e)
            if ref:
                reads.append((ref["name"], ref["key"], f"{c['name']} env {e['name']}"))
    for vol in pod.get("volumes") or []:
        sources = [{"secret": vol["secret"]}] if "secret" in vol else (vol.get("projected") or {}).get("sources") or []
        for src in sources:
            ref_source = src.get("secret")
            if not ref_source:
                continue
            name = ref_source.get("name") or ref_source.get("secretName")
            items = ref_source.get("items") or [{"key": None}]
            for item in items:
                reads.append((name, item["key"], f"volume {vol['name']}"))
    return reads


def configmap_reads(pod):
    reads = []
    for c in list(pod.get("initContainers") or []) + list(pod.get("containers") or []):
        for e in c.get("env") or []:
            ref = ((e.get("valueFrom") or {}).get("configMapKeyRef")) or {}
            if ref:
                reads.append((ref.get("name"), f"{c['name']} env {e['name']}"))
        for src in c.get("envFrom") or []:
            if "configMapRef" in src:
                reads.append((src["configMapRef"].get("name"), f"{c['name']} envFrom"))
    for vol in pod.get("volumes") or []:
        sources = [{"configMap": vol["configMap"]}] if "configMap" in vol else (vol.get("projected") or {}).get("sources") or []
        for src in sources:
            if "configMap" in src:
                reads.append((src["configMap"].get("name"), f"volume {vol['name']}"))
    return reads


def check_job(docs, mode, create_streams, acl_mode):
    failures = []
    jobs = labelled(docs, "Job", JOB_NAME)
    if len(jobs) != 1:
        return [f"want exactly one {JOB_NAME} Job, the render holds {len(jobs)}"]
    job = jobs[0]
    name = job["metadata"]["name"]
    pod = job["spec"]["template"]["spec"]
    containers = pod.get("containers") or []
    if [c["name"] for c in containers] != [JOB_NAME]:
        return [f"the {JOB_NAME} pod must hold one container named {JOB_NAME}, it holds "
                f"{[c['name'] for c in containers]}"]
    container = containers[0]
    ref_docs = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Secret"}
    configmaps = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "ConfigMap"}
    config = (configmaps.get(CONFIGMAP) or {}).get("data") or {}
    dlc = [d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"] == "dead-letter-controller"]
    if len(dlc) != 1:
        return ["the render holds no dead-letter-controller Deployment, whose image the Job runs"]
    dlc_pod = dlc[0]["spec"]["template"]["spec"]
    dlc_main = dlc_pod["containers"][0]

    # Hook annotations and ordering.
    if mode == "hook":
        if not re.fullmatch(r".+-streams-init", name):
            failures.append(f"the hook Job's name {name!r} must end in -streams-init")
        if helm_annotations(job) != HOOK_ANNOTATIONS:
            failures.append(f"the hook's annotations are {helm_annotations(job)}, want exactly {HOOK_ANNOTATIONS}")
        if set(annotations_of(job)) - set(HOOK_ANNOTATIONS):
            failures.append(f"the hook carries other annotations: {sorted(set(annotations_of(job)) - set(HOOK_ANNOTATIONS))}")
        migrates = labelled(docs, "Job", MIGRATE_NAME)
        if len(migrates) != 1:
            failures.append(f"want exactly one {MIGRATE_NAME} Job, the render holds {len(migrates)}")
        elif helm_annotations(migrates[0]):
            migrate = migrates[0]
            if annotations_of(migrate).get(HOOK) != HOOK_ANNOTATIONS[HOOK]:
                failures.append(f"the migrate hook runs on {annotations_of(migrate).get(HOOK)!r}, "
                                f"streams-init on {HOOK_ANNOTATIONS[HOOK]!r}: they must run on the same events")
            if hook_weight(migrate) >= hook_weight(job):
                failures.append(f"the migrate hook's weight {hook_weight(migrate)} is not lower than streams-init's "
                                f"{hook_weight(job)}: the streams would be created before the migrations")
        if pod.get("initContainers"):
            failures.append(f"the hook Job has init containers {[c['name'] for c in pod['initContainers']]}; "
                            "a BYO Redis exists before the release and needs no wait")
    else:
        if helm_annotations(job):
            failures.append(f"the bundled-Redis Job must not be a hook, it carries {sorted(helm_annotations(job))}")
        if not re.fullmatch(r".+-streams-init-r[0-9]+", name):
            failures.append(f"the bundled-Redis Job name {name!r} lacks a -r<revision> suffix")

    # The Job's spec.
    if job["spec"].get("backoffLimit") != 6:
        failures.append(f"backoffLimit is {job['spec'].get('backoffLimit')!r}, want 6")
    if job["spec"].get("ttlSecondsAfterFinished") != TTL[mode]:
        failures.append(f"ttlSecondsAfterFinished is {job['spec'].get('ttlSecondsAfterFinished')!r}, want {TTL[mode]}")
    if pod.get("restartPolicy") != "OnFailure":
        failures.append(f"restartPolicy is {pod.get('restartPolicy')!r}, want 'OnFailure'")
    pod_security = pod.get("securityContext") or {}
    if pod_security != (dlc_pod.get("securityContext") or {}):
        failures.append(f"the pod securityContext {pod_security} differs from dead-letter-controller's "
                        f"{dlc_pod.get('securityContext')}")
    if pod_security.get("runAsNonRoot") is not True or pod_security.get("runAsUser") != 65532 \
            or pod_security.get("runAsGroup") != 65532:
        failures.append(f"the pod must run as non-root user and group 65532, its securityContext is {pod_security}")

    # The main container.
    if container["image"] != dlc_main["image"]:
        failures.append(f"image {container['image']!r} is not the dead-letter-controller image {dlc_main['image']!r}")
    if container.get("imagePullPolicy") != dlc_main.get("imagePullPolicy"):
        failures.append(f"imagePullPolicy {container.get('imagePullPolicy')!r} differs from dead-letter-controller's "
                        f"{dlc_main.get('imagePullPolicy')!r}")
    if container.get("command") != ["/streams-init"]:
        failures.append(f"command is {container.get('command')!r}, want ['/streams-init']")
    if container.get("args"):
        failures.append(f"args is {container['args']!r}, want none")
    if container.get("securityContext") != CONTAINER_SECURITY:
        failures.append(f"the container securityContext is {container.get('securityContext')}, want {CONTAINER_SECURITY}")
    resources = container.get("resources") or {}
    if not resources.get("limits") or not resources.get("requests"):
        failures.append("the container must set resource requests and limits")

    check_env_names(failures, JOB_NAME, container, MAIN_ENV[acl_mode])
    env = {e["name"]: e for e in container.get("env") or []}
    for var, want in (("REDIS_ACL_MODE", acl_mode), ("REDIS_INIT_CREATE_STREAMS", create_streams),
                      ("REDIS_ADDR", config.get("REDIS_ADDR")), ("LOG_LEVEL", config.get("LOG_LEVEL"))):
        got = (env.get(var) or {}).get("value")
        if want is None:
            failures.append(f"the {CONFIGMAP} ConfigMap has no {var} to compare the Job's with")
        elif got != want:
            failures.append(f"{var} is {got!r}, want {want!r}")
    login_ref = key_ref(env.get("REDIS_PASSWORD"))
    if not login_ref or not login_ref.get("name") or not login_ref.get("key"):
        failures.append("REDIS_PASSWORD is not a secretKeyRef")

    mounts = sorted(m["name"] for m in container.get("volumeMounts") or [])
    volumes = sorted(v["name"] for v in pod.get("volumes") or [])
    want_volumes = ["redis-logins"] if acl_mode == "perService" else []
    if mounts != want_volumes or volumes != want_volumes:
        failures.append(f"the Job mounts {mounts} from volumes {volumes}, want {want_volumes}")

    # The bundled-Redis wait.
    if mode == "job":
        inits = pod.get("initContainers") or []
        if [c["name"] for c in inits] != ["wait-for-redis"]:
            failures.append(f"init containers are {[c['name'] for c in inits]}, want ['wait-for-redis']")
        else:
            wait = inits[0]
            check_env_names(failures, "wait-for-redis", wait, WAIT_ENV[acl_mode])
            wenv = {e["name"]: e for e in wait.get("env") or []}
            target = f"{(wenv.get('REDIS_HOST') or {}).get('value')}:{(wenv.get('REDIS_PORT') or {}).get('value')}"
            if target != (env.get("REDIS_ADDR") or {}).get("value"):
                failures.append(f"wait-for-redis waits for {target}, the Job connects to "
                                f"{(env.get('REDIS_ADDR') or {}).get('value')!r}")
            if key_ref(wenv.get("REDISCLI_AUTH")) != login_ref:
                failures.append(f"wait-for-redis signs in with {key_ref(wenv.get('REDISCLI_AUTH'))}, "
                                f"the Job with {login_ref}")
            redis = [d for d in docs if d.get("kind") == "StatefulSet"
                     and (d["metadata"].get("labels") or {}).get("app.kubernetes.io/name") == "redis"]
            if len(redis) != 1:
                failures.append("the bundled-Redis Job's render holds no bundled Redis StatefulSet")
            elif wait["image"] != redis[0]["spec"]["template"]["spec"]["containers"][0]["image"]:
                failures.append(f"wait-for-redis runs {wait['image']!r}, not the bundled Redis image")
            if wait.get("securityContext") != CONTAINER_SECURITY:
                failures.append(f"wait-for-redis securityContext is {wait.get('securityContext')}, "
                                f"want {CONTAINER_SECURITY}")

    # What a hook may read, and the streams-init hook Secret.
    if mode == "hook":
        for cm_name, where in configmap_reads(pod):
            if cm_name in configmaps:
                failures.append(f"the hook reads the ConfigMap {cm_name!r} ({where}), a regular resource Helm "
                                "creates only after pre-install hooks: the pod would never start")
        for ref_name, key, where in key_reads(pod):
            ref_source = ref_docs.get(ref_name)
            if ref_source is None:
                continue
            if "pre-install" not in annotations_of(ref_source).get(HOOK, ""):
                failures.append(f"the hook reads the Secret {ref_name!r} ({where}), a regular resource Helm "
                                "creates only after pre-install hooks: the pod would never start")
            elif hook_weight(ref_source) >= hook_weight(job):
                failures.append(f"the hook's Secret {ref_name!r} ({where}) is not created before the Job: "
                                "its hook weight must be lower")
            elif key is not None and key not in (ref_source.get("stringData") or {}) | (ref_source.get("data") or {}):
                failures.append(f"the hook's Secret {ref_name!r} lacks the key {key!r} ({where})")
    hook_refs = labelled(docs, "Secret", JOB_NAME)
    reads_render_login = login_ref is not None and login_ref.get("name") in ref_docs
    if mode == "hook" and acl_mode == "shared" and reads_render_login:
        if len(hook_refs) != 1 or hook_refs[0]["metadata"]["name"] != login_ref["name"]:
            failures.append(f"the shared-mode hook reads its password from {login_ref['name']!r}, "
                            f"which is not the one streams-init hook Secret {[s['metadata']['name'] for s in hook_refs]}")
        else:
            ref_source = hook_refs[0]
            if annotations_of(ref_source) != HOOK_REF_ANNOTATIONS:
                failures.append(f"the hook Secret's annotations are {annotations_of(ref_source)}, "
                                f"want exactly {HOOK_REF_ANNOTATIONS}")
            if sorted((ref_source.get("stringData") or {}) | (ref_source.get("data") or {})) != ["redis-password"] \
                    or login_ref.get("key") != "redis-password":
                failures.append("the hook Secret must hold exactly redis-password, the key the Job reads")
    elif hook_refs:
        failures.append(f"the render holds the streams-init Secret {[s['metadata']['name'] for s in hook_refs]}, "
                        "which only a shared-mode hook whose password the chart holds reads")
    return failures


def main(argv):
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("manifest")
    parser.add_argument("--mode", choices=("hook", "job"), required=True)
    parser.add_argument("--create-streams", choices=("true", "false"), required=True)
    parser.add_argument("--acl-mode", choices=("perService", "shared"), required=True)
    args = parser.parse_args(argv[1:])
    with open(args.manifest) as handle:
        docs = [d for d in yaml.safe_load_all(handle) if d]
    failures = check_job(docs, args.mode, args.create_streams, args.acl_mode)
    for failure in failures:
        logger.error("FAIL: %s: %s", args.manifest, failure)
    if failures:
        return 1
    logger.info("streams-init Job OK (%s, %s, createStreams=%s)", args.mode, args.acl_mode, args.create_streams)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
