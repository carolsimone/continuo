#!/usr/bin/env python3
"""Every chart-deployed service whose Go source imports pkg/metrics exposes the
metrics port the shared ConfigMap names, with prometheus.io annotations, and
no other Deployment exposes it. The scrape NetworkPolicy is absent by default
and, when rendered, admits exactly those services on that port only.

Which services serve /metrics is read from their source (an import of
github.com/carolsimone/continuo/pkg/metrics in a non-test .go file), so the
chart's list cannot drift from the code.

Usage: assert-metrics.py <repo-root> <defaults.yaml> <scrape-rule.yaml>
"""
import logging
import pathlib
import re
import sys

import yaml

logger = logging.getLogger("assert-metrics")

IMPORT = re.compile(r'"github\.com/carolsimone/continuo/pkg/metrics"')


def serving_services(root: pathlib.Path) -> set[str]:
    found = set()
    for main in root.glob("*/main.go"):
        service_dir = main.parent
        for source in service_dir.rglob("*.go"):
            if not source.name.endswith("_test.go") and IMPORT.search(source.read_text()):
                found.add(service_dir.name)
                break
    return found


def load(path: str) -> list[dict]:
    with open(path) as f:
        return [d for d in yaml.safe_load_all(f) if d]


def configmap_port(docs: list[dict]) -> str:
    for d in docs:
        if d["kind"] == "ConfigMap" and "METRICS_PORT" in d.get("data", {}):
            return d["data"]["METRICS_PORT"]
    raise SystemExit("FAIL: no ConfigMap carries METRICS_PORT")


def check_defaults(docs: list[dict], serving: set[str], port: str) -> list[str]:
    problems = []
    exposed = set()
    for d in docs:
        if d["kind"] != "Deployment":
            continue
        name = d["metadata"]["name"]
        pod = d["spec"]["template"]
        ports = [p for c in pod["spec"]["containers"] for p in c.get("ports", []) if p.get("name") == "metrics"]
        if not ports:
            continue
        exposed.add(name)
        if any(str(p["containerPort"]) != port for p in ports):
            problems.append(f"{name}: metrics containerPort differs from METRICS_PORT {port}")
        annotations = pod["metadata"].get("annotations", {})
        if annotations.get("prometheus.io/port") != port or annotations.get("prometheus.io/scrape") != "true":
            problems.append(f"{name}: missing or wrong prometheus.io annotations")
    if exposed != serving:
        problems.append(f"metrics port on {sorted(exposed)}, but pkg/metrics is imported by {sorted(serving)}")
    if any(d["kind"] == "NetworkPolicy" and d["metadata"]["name"].endswith("-allow-metrics-scrape") for d in docs):
        problems.append("the default render must not admit any scraper")
    return problems


def check_scrape_rule(docs: list[dict], serving: set[str], port: str) -> list[str]:
    rules = [d for d in docs if d["kind"] == "NetworkPolicy" and d["metadata"]["name"].endswith("-allow-metrics-scrape")]
    if len(rules) != 1:
        return [f"expected one allow-metrics-scrape policy, found {len(rules)}"]
    spec = rules[0]["spec"]
    problems = []
    selected = set(spec["podSelector"]["matchExpressions"][0]["values"])
    if selected != serving:
        problems.append(f"scrape rule selects {sorted(selected)}, want {sorted(serving)}")
    ingress = spec["ingress"]
    if [p["port"] for rule in ingress for p in rule["ports"]] != [int(port)]:
        problems.append("scrape rule must admit the metrics port and nothing else")
    if not all("namespaceSelector" in peer for rule in ingress for peer in rule["from"]):
        problems.append("scrape rule must admit namespaces only")
    return problems


def main(argv: list[str]) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    root, defaults_path, scrape_path = pathlib.Path(argv[1]), argv[2], argv[3]
    serving = serving_services(root)
    defaults = load(defaults_path)
    port = configmap_port(defaults)
    problems = check_defaults(defaults, serving, port) + check_scrape_rule(load(scrape_path), serving, port)
    for p in problems:
        logger.error("FAIL: %s", p)
    if not problems:
        logger.info("metrics: port %s on %s; scrape rule admits only that port", port, ", ".join(sorted(serving)))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
