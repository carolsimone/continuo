#!/usr/bin/env python3
"""How the chart hands credentials to the pods of its `services` Deployments.

Modes:
  env      every Deployment runs under a ServiceAccount named after it.
  file     the same, and no container of a service Deployment has a
           secretKeyRef: every credential is a `<VAR>_FILE` variable naming a
           file under /var/run/secrets/continuo, projected from its Secret into
           the pod's `continuo-credentials` volume and mounted read-only there.
           With a baseline render (the same values in `env` delivery), every
           credential a container gets as a secretKeyRef there has its
           `<VAR>_FILE` twin here.
  company  the same ServiceAccount rule, and agent-chat reads LLM_API_KEY from
           the file the company provides (/vault/secrets/llm), with no Secret
           behind it: no LLM_API_KEY variable and no projected item for it.
           agent-chat's pod carries the Vault Agent injector annotation and its
           ServiceAccount the fixture's role annotation, and
           execution-controller mounts the fixture's Secrets Store CSI volume.

Deployments the chart renders from their own templates (dex, egress-proxy) are
not services entries and are not checked.

Usage: assert-credential-delivery.py <rendered.yaml> <env|file|company> [<baseline.yaml>]
"""
import logging
import sys

import yaml

logger = logging.getLogger("assert-credential-delivery")

MOUNT_DIR = "/var/run/secrets/continuo"
VOLUME = "continuo-credentials"
NOT_SERVICES = {"dex", "egress-proxy"}
COMPANY_SERVICE, COMPANY_VAR, COMPANY_PATH = "agent-chat", "LLM_API_KEY", "/vault/secrets/llm"
VAULT_ANNOTATION = "vault.hashicorp.com/agent-inject"
ROLE_ANNOTATION = "eks.amazonaws.com/role-arn"
CSI_SERVICE, CSI_DRIVER = "execution-controller", "secrets-store.csi.k8s.io"


def rendered(path: str, kind: str) -> dict:
    found = {}
    with open(path) as fh:
        for doc in yaml.safe_load_all(fh):
            if not doc or doc.get("kind") != kind:
                continue
            if doc["metadata"].get("labels", {}).get("app.kubernetes.io/name") in NOT_SERVICES:
                continue
            found[doc["metadata"]["name"]] = doc
    return found


def service_deployments(path: str) -> dict:
    return rendered(path, "Deployment")


def containers(deployment: dict):
    spec = deployment["spec"]["template"]["spec"]
    return list(spec.get("initContainers") or []) + list(spec.get("containers") or [])


def projected_items(deployment: dict) -> dict:
    """Projected path -> (secret name, key) of the continuo-credentials volume."""
    items = {}
    for volume in deployment["spec"]["template"]["spec"].get("volumes") or []:
        if volume.get("name") != VOLUME:
            continue
        for source in volume.get("projected", {}).get("sources") or []:
            secret = source.get("secret") or {}
            for item in secret.get("items") or []:
                items[item["path"]] = (secret.get("name"), item["key"])
    return items


def secret_ref_vars(container: dict) -> set:
    return {e["name"] for e in container.get("env") or [] if "secretKeyRef" in (e.get("valueFrom") or {})}


def check_service_accounts(deployments: dict, failures: list) -> None:
    for name, deployment in deployments.items():
        account = deployment["spec"]["template"]["spec"].get("serviceAccountName")
        if account != name:
            failures.append(f"{name}: serviceAccountName is {account!r}, want {name!r}")


def check_file(deployments: dict, baseline: dict, failures: list) -> None:
    for name, deployment in deployments.items():
        items = projected_items(deployment)
        for container in containers(deployment):
            where = f"{name}/{container['name']}"
            for var in sorted(secret_ref_vars(container)):
                failures.append(f"{where}: {var} is a secretKeyRef; file delivery puts no credential in the environment")
            env = {e["name"]: e for e in container.get("env") or []}
            file_vars = {k: e.get("value") for k, e in env.items() if k.endswith("_FILE")}
            for var, value in file_vars.items():
                if not (value or "").startswith(MOUNT_DIR + "/"):
                    failures.append(f"{where}: {var} does not name a file under {MOUNT_DIR}")
                    continue
                if value[len(MOUNT_DIR) + 1:] not in items:
                    failures.append(f"{where}: {var} names no projected item of the {VOLUME} volume")
            if file_vars:
                mounts = [m for m in container.get("volumeMounts") or [] if m.get("name") == VOLUME]
                if not any(m.get("mountPath") == MOUNT_DIR and m.get("readOnly") for m in mounts):
                    failures.append(f"{where}: reads credential files but does not mount {VOLUME} read-only at {MOUNT_DIR}")
        main = deployment["spec"]["template"]["spec"]["containers"][0]
        base = baseline.get(name)
        if base is None:
            continue
        have = {e["name"] for e in main.get("env") or []}
        for var in sorted(secret_ref_vars(base["spec"]["template"]["spec"]["containers"][0])):
            if f"{var}_FILE" not in have:
                failures.append(f"{name}: gets {var} as a secretKeyRef with env delivery but has no {var}_FILE with file delivery")


def check_company(deployments: dict, service_accounts: dict, failures: list) -> None:
    deployment = deployments.get(COMPANY_SERVICE)
    if deployment is None:
        failures.append(f"no {COMPANY_SERVICE} Deployment rendered")
        return
    main = deployment["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e for e in main.get("env") or []}
    got = (env.get(f"{COMPANY_VAR}_FILE") or {}).get("value")
    if got != COMPANY_PATH:
        failures.append(f"{COMPANY_SERVICE}: {COMPANY_VAR}_FILE does not name {COMPANY_PATH}")
    if COMPANY_VAR in env:
        failures.append(f"{COMPANY_SERVICE}: {COMPANY_VAR} is set; a company-provided file has no Secret behind it")
    if COMPANY_VAR in projected_items(deployment):
        failures.append(f"{COMPANY_SERVICE}: the {VOLUME} volume projects {COMPANY_VAR} from a Secret")
    annotations = deployment["spec"]["template"]["metadata"].get("annotations") or {}
    if annotations.get(VAULT_ANNOTATION) != "true":
        failures.append(f"{COMPANY_SERVICE}: pod annotation {VAULT_ANNOTATION} is {annotations.get(VAULT_ANNOTATION)!r}, want 'true'")
    account = service_accounts.get(COMPANY_SERVICE)
    if account is None:
        failures.append(f"{COMPANY_SERVICE}: no ServiceAccount rendered")
    elif not (account["metadata"].get("annotations") or {}).get(ROLE_ANNOTATION):
        failures.append(f"{COMPANY_SERVICE}: ServiceAccount carries no {ROLE_ANNOTATION} annotation")
    executor = deployments.get(CSI_SERVICE)
    volumes = executor["spec"]["template"]["spec"].get("volumes") or [] if executor else []
    if not any((v.get("csi") or {}).get("driver") == CSI_DRIVER for v in volumes):
        failures.append(f"{CSI_SERVICE}: no {CSI_DRIVER} volume rendered")


def main() -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    if len(sys.argv) not in (3, 4) or sys.argv[2] not in ("env", "file", "company"):
        logger.error("usage: assert-credential-delivery.py <rendered.yaml> <env|file|company> [<baseline.yaml>]")
        return 2
    rendered_path, mode = sys.argv[1], sys.argv[2]
    deployments = service_deployments(rendered_path)
    failures = []
    if not deployments:
        failures.append(f"no service Deployment in {rendered_path}")
    check_service_accounts(deployments, failures)
    if mode == "file":
        baseline = service_deployments(sys.argv[3]) if len(sys.argv) == 4 else {}
        check_file(deployments, baseline, failures)
    elif mode == "company":
        check_company(deployments, rendered(rendered_path, "ServiceAccount"), failures)
    for failure in failures:
        logger.error("FAIL: %s", failure)
    if not failures:
        logger.info("credential delivery (%s) OK for: %s", mode, ", ".join(sorted(deployments)))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
