#!/usr/bin/env python3
"""Each service signs in to Redis as its own login.

perService (credentials.redis.mode):
  - every Deployment of a Redis service sets REDIS_USERNAME to its login from
    deploy/continuo/files/redis-acl-rules.json and reads REDIS_PASSWORD from
    that login's Secret; its wait-for-redis init container, when rendered,
    signs in as the same login;
  - the shared ConfigMap says REDIS_ACL_MODE=perService;
  - the streams-init Job signs in as continuo_redis_migrate, checks its own
    login's reach (REDIS_ACL_MODE=perService), and mounts the
    password of every other login it can reach under
    /var/run/secrets/continuo/redis-logins/<username> (REDIS_LOGINS_DIR);
  - bundled: Redis starts from an ACL file (--aclfile, no --requirepass) with
    `user default off` and one line per login of the rules file, holding the
    login's rule and the SHA-256 of the password in <release>-logins.
shared:
  - no container gets REDIS_USERNAME, REDIS_ACL_MODE=shared, and the bundled
    Redis keeps --requirepass.

Usage:
  assert-redis-logins.py <rendered.yaml> perService bundled
  assert-redis-logins.py <rendered.yaml> perService external <secret-name-format>
    <secret-name-format> names each login's Secret, with {service} for the
    service name (streams-init included); its key is `password`.
  assert-redis-logins.py <rendered.yaml> shared
"""
import hashlib
import json
import logging
import pathlib
import sys

import yaml

logger = logging.getLogger("assert-redis-logins")

RELEASE = "continuo"
RULES = pathlib.Path(__file__).resolve().parents[2] / "deploy/continuo/files/redis-acl-rules.json"
LOGINS_DIR = "/var/run/secrets/continuo/redis-logins"
STREAMS_INIT = "streams-init"


def env_of(container: dict) -> dict:
    return {e["name"]: e for e in container.get("env") or []}


def value(env: dict, name: str):
    return (env.get(name) or {}).get("value")


def secret_ref(env: dict, name: str):
    ref = ((env.get(name) or {}).get("valueFrom") or {}).get("secretKeyRef")
    return (ref["name"], ref["key"]) if ref else None


def password_source(pod_spec: dict, container: dict, var: str):
    env = env_of(container)
    if var in env:
        return secret_ref(env, var)
    if f"{var}_FILE" in env:
        item_path = value(env, f"{var}_FILE").rsplit("/", 1)[-1]
        for vol in pod_spec.get("volumes") or []:
            for src in (vol.get("projected") or {}).get("sources") or []:
                secret = src.get("secret")
                for item in (secret or {}).get("items") or []:
                    if item["path"] == item_path:
                        return (secret["name"], item["key"])
    return None


def main() -> int:
    logging.basicConfig(stream=sys.stderr, format="%(message)s", level=logging.INFO)
    args = sys.argv[1:]
    if len(args) < 2 or args[1] not in ("perService", "shared"):
        logger.error(__doc__)
        return 2
    path, mode = args[0], args[1]
    where = args[2] if mode == "perService" else None
    secret_format = args[3] if where == "external" else None
    with open(path) as fh:
        rendered = [d for d in yaml.safe_load_all(fh) if d]
    rules = json.loads(RULES.read_text())
    login_of = {l["service"]: l["username"] for l in rules["logins"]}
    rule_of = {l["username"]: l["rule"] for l in rules["logins"]}
    problems = []

    def want_secret(svc: str):
        if where == "bundled":
            return (f"{RELEASE}-logins", f"redis-{svc}")
        return (secret_format.format(service=svc), "password")

    cm = next(d for d in rendered if d.get("kind") == "ConfigMap" and d["metadata"]["name"] == f"{RELEASE}-config")
    if cm["data"].get("REDIS_ACL_MODE") != mode:
        problems.append(f"ConfigMap REDIS_ACL_MODE is {cm['data'].get('REDIS_ACL_MODE')!r}, want {mode!r}")

    seen = set()
    for d in rendered:
        if d.get("kind") not in ("Deployment", "Job", "StatefulSet"):
            continue
        name = d["metadata"]["name"]
        spec = d["spec"]["template"]["spec"]
        for c in list(spec.get("initContainers") or []) + list(spec.get("containers") or []):
            env = env_of(c)
            if mode == "shared" and "REDIS_USERNAME" in env:
                problems.append(f"{name}/{c['name']}: REDIS_USERNAME in shared mode")
        if mode == "shared" or d.get("kind") != "Deployment" or name not in login_of:
            continue
        seen.add(name)
        login = login_of[name]
        main = spec["containers"][0]
        env = env_of(main)
        if value(env, "REDIS_USERNAME") != login:
            problems.append(f"{name}: REDIS_USERNAME is {value(env, 'REDIS_USERNAME')!r}, want {login!r}")
        got = password_source(spec, main, "REDIS_PASSWORD")
        if got != want_secret(name):
            problems.append(f"{name}: REDIS_PASSWORD comes from {got}, want {want_secret(name)}")
        if sum(1 for e in main.get("env") or [] if e["name"].startswith("REDIS_PASSWORD")) != 1:
            problems.append(f"{name}: REDIS_PASSWORD is set more than once")
        for wait in [c for c in spec.get("initContainers") or [] if c["name"] == "wait-for-redis"]:
            wenv = env_of(wait)
            if value(wenv, "REDIS_USERNAME") != login:
                problems.append(f"{name}: wait-for-redis signs in as {value(wenv, 'REDIS_USERNAME')!r}, want {login!r}")
            got = password_source(spec, wait, "REDISCLI_AUTH")
            if got != want_secret(name):
                problems.append(f"{name}: wait-for-redis reads REDISCLI_AUTH from {got}, want {want_secret(name)}")
    if mode == "perService":
        missing = {s for s in login_of if s != STREAMS_INIT} - seen
        if missing:
            problems.append(f"no Redis Deployment rendered for {sorted(missing)}")

        jobs = [d for d in rendered if d.get("kind") == "Job"
                and d["metadata"].get("labels", {}).get("app.kubernetes.io/name") == "streams-init"]
        if len(jobs) != 1:
            problems.append(f"want one streams-init Job, found {len(jobs)}")
        else:
            spec = jobs[0]["spec"]["template"]["spec"]
            main = next(c for c in spec["containers"] if c["name"] == "streams-init")
            env = env_of(main)
            if value(env, "REDIS_USERNAME") != login_of[STREAMS_INIT]:
                problems.append(f"streams-init signs in as {value(env, 'REDIS_USERNAME')!r}")
            if secret_ref(env, "REDIS_PASSWORD") != want_secret(STREAMS_INIT):
                problems.append(f"streams-init reads REDIS_PASSWORD from {secret_ref(env, 'REDIS_PASSWORD')}")
            if value(env, "REDIS_ACL_MODE") != "perService":
                problems.append(f"streams-init REDIS_ACL_MODE is {value(env, 'REDIS_ACL_MODE')!r}, want 'perService'")
            if value(env, "REDIS_LOGINS_DIR") != LOGINS_DIR:
                problems.append(f"streams-init REDIS_LOGINS_DIR is {value(env, 'REDIS_LOGINS_DIR')!r}")
            mounts = {m["name"]: m["mountPath"] for m in main.get("volumeMounts") or []}
            items = {}
            for vol in spec.get("volumes") or []:
                if mounts.get(vol["name"]) != LOGINS_DIR:
                    continue
                for src in (vol.get("projected") or {}).get("sources") or []:
                    secret = src.get("secret") or {}
                    for item in secret.get("items") or []:
                        items[item["path"]] = (secret["name"], item["key"])
            want_items = {login_of[s]: want_secret(s) for s in login_of if s != STREAMS_INIT}
            if items != want_items:
                problems.append(f"streams-init mounts {items}, want {want_items}")

    sts = next((d for d in rendered if d.get("kind") == "StatefulSet"
                and d["metadata"].get("labels", {}).get("app.kubernetes.io/name") == "redis"), None)
    if sts:
        redis_args = sts["spec"]["template"]["spec"]["containers"][0].get("args") or []
        if mode == "perService":
            if "--aclfile" not in redis_args or "--requirepass" in redis_args:
                problems.append(f"bundled Redis args {redis_args}: want --aclfile and no --requirepass")
            secrets = {d["metadata"]["name"]: d for d in rendered if d.get("kind") == "Secret"}
            acl = secrets.get(f"{RELEASE}-redis-acl")
            passwords = (secrets.get(f"{RELEASE}-logins") or {}).get("stringData") or {}
            if not acl:
                problems.append(f"no {RELEASE}-redis-acl Secret")
            else:
                lines = [l for l in acl["stringData"]["users.acl"].splitlines() if l.strip()]
                if "user default off" not in lines:
                    problems.append("users.acl does not turn the default user off")
                users = {}
                for line in lines:
                    parts = line.split(" ", 4)
                    if parts[1] != "default":
                        users[parts[1]] = parts
                if set(users) != set(rule_of):
                    problems.append(f"users.acl names {sorted(users)}, want {sorted(rule_of)}")
                for svc, login in login_of.items():
                    parts = users.get(login)
                    if not parts:
                        continue
                    pw = passwords.get(f"redis-{svc}", "")
                    want_hash = "#" + hashlib.sha256(pw.encode()).hexdigest()
                    if parts[2] != "on" or parts[3] != want_hash or parts[4] != rule_of[login]:
                        problems.append(f"users.acl line for {login} does not match its rule and password")
        elif "--requirepass" not in redis_args:
            problems.append("shared mode: the bundled Redis must keep --requirepass")

    for p in problems:
        logger.error("FAIL: %s", p)
    if not problems:
        logger.info("ok: Redis logins (%s %s)", mode, where or "")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
