#!/usr/bin/env python3
"""A `helm upgrade` that changes a chart-managed ConfigMap must roll the pods
that read it only at startup, and no others.

Kubernetes rolls a Deployment only when its pod template changes. A ConfigMap
read through envFrom, or mounted and read once at boot, reaches a running pod
only if the pod template carries a digest of it; without one, `helm upgrade`
updates the ConfigMap, renders an identical Deployment, and the service keeps
its previous configuration with no error.

For each chart-managed ConfigMap, the check changes that ConfigMap's source (a
file under files/ or a values key) in a copy of the chart, renders before and
after, and compares the pod template of every Deployment and StatefulSet:
  - every workload that consumes the ConfigMap must change, except one listed
    in the case's `reread_by` (it re-reads the file on every use);
  - every other workload must stay identical, so an upgrade restarts only what
    it has to.
The same cases then run against a render whose service volumes name
operator-owned ConfigMaps (configMap.name set): no workload consumes the
chart's copies there, so a change to them must roll nothing.

It also checks that:
  - two identical renders give identical pod templates, so an upgrade that
    changes nothing rolls nothing;
  - each `reread_by` consumer mounts the ConfigMap as a directory, with no
    subPath: the kubelet refreshes a directory mount in place but never a
    subPath one, so a per-request read sees an edit only through the former;
  - every chart-managed ConfigMap a workload consumes has a case here, so a new
    one cannot ship without a decision about how its consumers see a change.

Usage: assert-config-rollout.py <chart-dir> <kube-version>
"""
import json
import logging
import pathlib
import shutil
import subprocess
import sys
import tempfile

import yaml

logger = logging.getLogger("assert-config-rollout")

RELEASE = "continuo"
ROLLING_KINDS = ("Deployment", "StatefulSet")


def append_yaml_comment(text: str) -> str:
    return text.rstrip("\n") + "\n# config-rollout probe\n"


def add_cancellation_reason(text: str) -> str:
    doc = json.loads(text)
    doc["cancellation_reasons"].append("config-rollout probe")
    return json.dumps(doc, indent=2) + "\n"


# Each case changes one chart-managed ConfigMap, through a file under the
# chart's files/ directory (`file` + `edit`) or through values (`args`).
# `reread_by` names consumers that re-read the file on every use and so must
# not roll: the ui reads cancel-config.json per request
# (ui/src/server/routes/config.ts).
CASES = [
    {"name": "shared config (global.logLevel)", "configmap": "config",
     "args": ["--set", "global.logLevel=DEBUG"]},
    {"name": "files/schedules.yaml", "configmap": "schedules",
     "file": "files/schedules.yaml", "edit": append_yaml_comment},
    {"name": "files/dbt-commands.yaml", "configmap": "dbt-commands",
     "file": "files/dbt-commands.yaml", "edit": append_yaml_comment},
    {"name": "files/service_repos.yaml", "configmap": "service-repos",
     "file": "files/service_repos.yaml", "edit": append_yaml_comment},
    {"name": "serviceRepos value", "configmap": "service-repos",
     "args": ["--set", "serviceRepos.config-rollout-probe=probe"]},
    {"name": "ciAuth.bindings value", "configmap": "ci-auth",
     "args": ["--set-string", "ciAuth.bindings.core[0].repositoryId=812345678"]},
    {"name": "files/cancel-config.json", "configmap": "cancel-config",
     "file": "files/cancel-config.json", "edit": add_cancellation_reason,
     "reread_by": {"ui"}},
]


class Renderer:
    def __init__(self, chart: pathlib.Path, kube_version: str):
        self.chart = chart
        self.kube_version = kube_version

    def render(self, *args: str) -> list:
        proc = subprocess.run(
            ["helm", "template", RELEASE, str(self.chart), "--kube-version", self.kube_version, *args],
            capture_output=True, text=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"helm template {' '.join(args)} failed:\n{proc.stderr}")
        return [d for d in yaml.safe_load_all(proc.stdout) if d]

    def render_case(self, case: dict, base_args: list) -> list:
        if "file" not in case:
            return self.render(*base_args, *case["args"])
        path = self.chart / case["file"]
        original = path.read_text()
        path.write_text(case["edit"](original))
        try:
            return self.render(*base_args)
        finally:
            path.write_text(original)


def pod_templates(docs: list) -> dict:
    return {d["metadata"]["name"]: d["spec"]["template"] for d in docs if d.get("kind") in ROLLING_KINDS}


def chart_configmaps(docs: list) -> dict:
    return {d["metadata"]["name"]: d.get("data") for d in docs if d.get("kind") == "ConfigMap"}


def consumers(docs: list) -> dict:
    """ConfigMap name -> {workload name: [volume mounts of it, by container]}."""
    out = {}
    for name, tpl in pod_templates(docs).items():
        spec = tpl["spec"]
        volumes = {v["name"]: v["configMap"]["name"] for v in spec.get("volumes") or [] if "configMap" in v}
        for cm in volumes.values():
            out.setdefault(cm, {}).setdefault(name, [])
        for container in (spec.get("initContainers") or []) + spec["containers"]:
            for source in container.get("envFrom") or []:
                if "configMapRef" in source:
                    out.setdefault(source["configMapRef"]["name"], {}).setdefault(name, [])
            for env in container.get("env") or []:
                ref = (env.get("valueFrom") or {}).get("configMapKeyRef")
                if ref:
                    out.setdefault(ref["name"], {}).setdefault(name, [])
            for mount in container.get("volumeMounts") or []:
                if mount["name"] in volumes:
                    out.setdefault(volumes[mount["name"]], {}).setdefault(name, []).append(mount)
    return out


def operator_owned_values(chart: pathlib.Path, workdir: pathlib.Path) -> pathlib.Path:
    """A values file whose service volumes all name operator-owned ConfigMaps."""
    values = yaml.safe_load((chart / "values.yaml").read_text())
    for svc in values["services"]:
        for volume in svc.get("volumes") or []:
            if "configMap" in volume:
                volume["configMap"]["name"] = f"operator-{volume['name']}"
    path = workdir / "values-operator-configmaps.yaml"
    path.write_text(yaml.safe_dump({"services": values["services"]}))
    return path


def check_topology(label: str, renderer: Renderer, base_args: list, failures: list) -> dict:
    base = renderer.render(*base_args)
    base_tpl = pod_templates(base)
    if pod_templates(renderer.render(*base_args)) != base_tpl:
        failures.append(f"[{label}] two identical renders differ in a pod template; every upgrade would roll it")
    base_cms = chart_configmaps(base)
    users = consumers(base)

    for case in CASES:
        cm = f"{RELEASE}-{case['configmap']}"
        changed = renderer.render_case(case, base_args)
        if chart_configmaps(changed).get(cm) == base_cms.get(cm):
            failures.append(f"[{label}] {case['name']}: ConfigMap {cm} is unchanged, so the case tests nothing")
            continue
        reread = case.get("reread_by", set())
        want = set(users.get(cm, {})) - reread
        changed_tpl = pod_templates(changed)
        rolled = {n for n, t in changed_tpl.items() if base_tpl.get(n) != t}
        for name in sorted(want - rolled):
            failures.append(f"[{label}] {case['name']}: {name} consumes {cm} but its pod template is unchanged, "
                            f"so `helm upgrade` would leave it on the previous {case['configmap']}")
        for name in sorted(rolled - want):
            failures.append(f"[{label}] {case['name']}: {name} does not need {cm} at startup but its pod template changed, "
                            f"so the upgrade would restart it needlessly")
        if want and want <= rolled and not (rolled - want):
            logger.info("[%s] %s rolls %s", label, case["name"], ", ".join(sorted(want)))
        elif not want and not rolled:
            logger.info("[%s] %s rolls nothing", label, case["name"])
        for name in sorted(reread & set(users.get(cm, {}))):
            for mount in users[cm][name]:
                if mount.get("subPath"):
                    failures.append(f"[{label}] {name} mounts {cm} with subPath {mount['subPath']}; the kubelet never "
                                    f"refreshes a subPath mount, so its per-request read would keep the old file")
    return users


def check(source_chart: pathlib.Path, kube_version: str, failures: list) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        workdir = pathlib.Path(tmp)
        chart = workdir / source_chart.name
        shutil.copytree(source_chart, chart)
        renderer = Renderer(chart, kube_version)

        users = check_topology("chart ConfigMaps", renderer, [], failures)
        covered = {f"{RELEASE}-{c['configmap']}" for c in CASES}
        chart_cms = set(chart_configmaps(renderer.render()))
        for cm in sorted((set(users) & chart_cms) - covered):
            failures.append(f"chart ConfigMap {cm} is consumed by {', '.join(sorted(users[cm]))} but has no case in "
                            f"{pathlib.Path(__file__).name}; decide whether its consumers must roll when it changes")

        operator_args = ["-f", str(operator_owned_values(source_chart, workdir))]
        operator_users = check_topology("operator-owned ConfigMaps", renderer, operator_args, failures)
        for case in CASES:
            if "file" in case and f"{RELEASE}-{case['configmap']}" in operator_users:
                failures.append(f"operator-owned render still mounts {RELEASE}-{case['configmap']}; "
                                f"the values override did not take effect")


def main() -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    if len(sys.argv) != 3:
        logger.error("usage: assert-config-rollout.py <chart-dir> <kube-version>")
        return 2
    failures = []
    try:
        check(pathlib.Path(sys.argv[1]), sys.argv[2], failures)
    except RuntimeError as err:
        failures.append(str(err))
    for f in failures:
        logger.error("FAIL: %s", f)
    if not failures:
        logger.info("every chart ConfigMap change rolls exactly the pods that read it at startup")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
