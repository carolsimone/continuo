#!/usr/bin/env python3
"""Each service signs in to Postgres as its own login.

Checks a rendered manifest:
  - every Deployment whose main container has POSTGRES_DB sets POSTGRES_USER
    to that service's login, and reads POSTGRES_PASSWORD (as a secretKeyRef
    variable, or as the file projected from a Secret) from that login's Secret;
  - a bundled render gates the service on its wait-for-migrations init
    container signing in as the same login;
  - the migrate Job's CONTINUO_PG_LOGINS names each database's login once;
  - bundled: the migrate Job creates the logins (CONTINUO_PG_CREATE_LOGINS)
    from the <release>-logins Secret, which the render contains, and hands
    the warehouse to continuo_warehouse; external: it creates none.

Usage:
  assert-postgres-logins.py <rendered.yaml> bundled
  assert-postgres-logins.py <rendered.yaml> external <secret-name-format>
    <secret-name-format> names each login's Secret, with {service} for the
    service name (e.g. continuo-pg-{service}); its key is `password`.
"""
import logging
import sys

import yaml

logger = logging.getLogger("assert-postgres-logins")

RELEASE = "continuo"
LOGINS = {
    "run-controller": ("run_controller", "continuo_run_controller"),
    "execution-controller": ("execution", "continuo_execution_controller"),
    "release-controller": ("release", "continuo_release_controller"),
    "remediation": ("remediation", "continuo_remediation"),
    "agent-remediation": ("agent_remediation", "continuo_agent_remediation"),
    "agent-chat": ("agent_chat", "continuo_agent_chat"),
    "dead-letter-controller": ("dead_letter", "continuo_dead_letter_controller"),
}
WAREHOUSE_LOGIN = "continuo_warehouse"


def docs(path: str):
    with open(path) as fh:
        return [d for d in yaml.safe_load_all(fh) if d]


def env_of(container: dict) -> dict:
    return {e["name"]: e for e in container.get("env") or []}


def password_source(pod_spec: dict, container: dict, var: str):
    """(secret name, key) a container reads credential var from, or None."""
    env = env_of(container)
    if var in env:
        ref = (env[var].get("valueFrom") or {}).get("secretKeyRef")
        return (ref["name"], ref["key"]) if ref else None
    if f"{var}_FILE" in env:
        path = env[f"{var}_FILE"].get("value", "")
        item_path = path.rsplit("/", 1)[-1]
        for vol in pod_spec.get("volumes") or []:
            for src in (vol.get("projected") or {}).get("sources") or []:
                secret = src.get("secret")
                if not secret:
                    continue
                for item in secret.get("items") or []:
                    if item["path"] == item_path:
                        return (secret["name"], item["key"])
    return None


def main() -> int:
    logging.basicConfig(stream=sys.stderr, format="%(message)s", level=logging.INFO)
    if len(sys.argv) < 3 or sys.argv[2] not in ("bundled", "external"):
        logger.error(__doc__)
        return 2
    path, mode = sys.argv[1], sys.argv[2]
    secret_format = sys.argv[3] if mode == "external" else None
    rendered = docs(path)
    problems = []

    seen = set()
    for d in rendered:
        if d.get("kind") != "Deployment":
            continue
        name = d["metadata"]["name"]
        spec = d["spec"]["template"]["spec"]
        main = spec["containers"][0]
        env = env_of(main)
        if "POSTGRES_DB" not in env:
            continue
        seen.add(name)
        if name not in LOGINS:
            problems.append(f"{name}: has POSTGRES_DB but no login is expected for it")
            continue
        login = LOGINS[name][1]
        user = env.get("POSTGRES_USER", {}).get("value")
        if user != login:
            problems.append(f"{name}: POSTGRES_USER is {user!r}, want {login!r}")
        want = (f"{RELEASE}-logins", f"postgres-{name}") if mode == "bundled" \
            else (secret_format.format(service=name), "password")
        got = password_source(spec, main, "POSTGRES_PASSWORD")
        if got != want:
            problems.append(f"{name}: POSTGRES_PASSWORD comes from {got}, want {want}")
        if sum(1 for e in main.get("env") or [] if e["name"].startswith("POSTGRES_PASSWORD")) != 1:
            problems.append(f"{name}: POSTGRES_PASSWORD is set more than once")
        if mode == "bundled":
            waits = [c for c in spec.get("initContainers") or [] if c["name"] == "wait-for-migrations"]
            if not waits:
                problems.append(f"{name}: no wait-for-migrations init container")
            else:
                wait_user = env_of(waits[0]).get("PGUSER", {}).get("value")
                if wait_user != login:
                    problems.append(f"{name}: wait-for-migrations signs in as {wait_user!r}, want {login!r}")
                got = password_source(spec, waits[0], "PGPASSWORD")
                if got != want:
                    problems.append(f"{name}: wait-for-migrations reads PGPASSWORD from {got}, want {want}")
    missing = set(LOGINS) - seen
    if missing:
        problems.append(f"no Postgres Deployment rendered for {sorted(missing)}")

    jobs = [d for d in rendered if d.get("kind") == "Job"
            and d["metadata"].get("labels", {}).get("app.kubernetes.io/name") == "db-init-migrate"]
    if len(jobs) != 1:
        problems.append(f"want one migrate Job, found {len(jobs)}")
    else:
        migrate = next(c for c in jobs[0]["spec"]["template"]["spec"]["containers"] if c["name"] == "migrate")
        menv = env_of(migrate)
        pairs = (menv.get("CONTINUO_PG_LOGINS", {}).get("value") or "").split()
        want_pairs = sorted(f"{db}={login}" for db, login in LOGINS.values())
        if sorted(pairs) != want_pairs:
            problems.append(f"migrate Job CONTINUO_PG_LOGINS is {pairs}, want {want_pairs}")
        create = menv.get("CONTINUO_PG_CREATE_LOGINS", {}).get("value")
        if mode == "bundled":
            if create != "true":
                problems.append(f"bundled migrate Job CONTINUO_PG_CREATE_LOGINS is {create!r}, want 'true'")
            for svc, (db, _) in LOGINS.items():
                var = f"CONTINUO_PG_PASSWORD_{db.upper()}"
                ref = (menv.get(var, {}).get("valueFrom") or {}).get("secretKeyRef")
                if not ref or (ref["name"], ref["key"]) != (f"{RELEASE}-logins", f"postgres-{svc}"):
                    problems.append(f"bundled migrate Job {var} comes from {ref}")
            wh = menv.get("CONTINUO_PG_WAREHOUSE_LOGIN", {}).get("value")
            if wh != WAREHOUSE_LOGIN:
                problems.append(f"bundled migrate Job CONTINUO_PG_WAREHOUSE_LOGIN is {wh!r}")
            if "CONTINUO_PG_WAREHOUSE_PASSWORD" not in menv or "CONTINUO_PG_WAREHOUSE_DB" not in menv:
                problems.append("bundled migrate Job lacks the warehouse password or database")
        elif create or any(k.startswith("CONTINUO_PG_PASSWORD_") or k.startswith("CONTINUO_PG_WAREHOUSE")
                           for k in menv):
            problems.append("external migrate Job must create no login and read no runtime password")

    secrets = {d["metadata"]["name"]: d for d in rendered if d.get("kind") == "Secret"}
    if mode == "bundled":
        logins = secrets.get(f"{RELEASE}-logins")
        if not logins:
            problems.append(f"bundled render has no {RELEASE}-logins Secret")
        else:
            keys = set((logins.get("stringData") or {}) | (logins.get("data") or {}))
            want_keys = {f"postgres-{svc}" for svc in LOGINS}
            if not want_keys <= keys:
                problems.append(f"{RELEASE}-logins lacks {sorted(want_keys - keys)}")
        warehouse = [s for s in secrets.values()
                     if (s.get("stringData") or {}).get("POSTGRES_USER") == WAREHOUSE_LOGIN]
        if len(warehouse) != 1:
            problems.append("bundled render has no warehouse Secret for continuo_warehouse")
        elif "password" in (warehouse[0].get("stringData") or {}):
            problems.append("the warehouse Secret carries another password besides POSTGRES_PASSWORD")
    elif f"{RELEASE}-logins" in secrets and any(
            k.startswith("postgres-") for k in (secrets[f"{RELEASE}-logins"].get("stringData") or {})):
        problems.append("external render must not generate Postgres login passwords")

    for p in problems:
        logger.error("FAIL: %s", p)
    if not problems:
        logger.info("ok: every Postgres service signs in as its own login (%s)", mode)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
