#!/usr/bin/env python3
"""Every chart-deployed service that serves /metrics (a Go service whose source
imports pkg/metrics, or a Python service whose source imports prometheus_client)
exposes the metrics port the shared ConfigMap names, with prometheus.io
annotations, and no other Deployment exposes it. The scrape NetworkPolicy is
absent by default and, when rendered, admits exactly those services on that
port only.

Which services serve /metrics is read from their source: an import of
github.com/carolsimone/continuo/pkg/metrics in a non-test .go file of a
directory holding main.go, or an import of prometheus_client in a non-test .py
file of a directory holding main.py, so the chart's list cannot drift from the
code. The metric table of deploy/OBSERVABILITY.md lists exactly the continuo_*
metrics the source registers: the Name: and NewDesc( literals of the Go source
and the Counter, Gauge, Histogram and Summary names of the Python source.

Usage: assert-metrics.py <repo-root> <defaults.yaml> <scrape-rule.yaml>
"""
import logging
import pathlib
import re
import sys

import yaml

logger = logging.getLogger("assert-metrics")

IMPORT = re.compile(r'"github\.com/carolsimone/continuo/pkg/metrics"')
METRIC_NAME = re.compile(r'(?:Name:\s*|NewDesc\(\s*)"(continuo_[a-z0-9_]+)"')
PY_IMPORT = re.compile(r"^\s*(?:import prometheus_client|from prometheus_client\b)", re.MULTILINE)
PY_METRIC_NAME = re.compile(r'(?:Counter|Gauge|Histogram|Summary)\(\s*"(continuo_[a-z0-9_]+)"')
PY_SKIPPED_DIRS = {"tests", "bench"}
DOCUMENTED_METRIC = re.compile(r'^\| `(continuo_[a-z0-9_]+)`', re.MULTILINE)


def python_sources(service_dir: pathlib.Path):
    """Yield the non-test .py files under a directory holding main.py.

    A path part that starts with a dot (a virtualenv, a cache), a tests or bench
    directory and a test_*.py file are skipped.
    """
    for source in service_dir.rglob("*.py"):
        relative = source.relative_to(service_dir).parts
        if any(p.startswith(".") for p in relative) or PY_SKIPPED_DIRS.intersection(relative[:-1]):
            continue
        if source.name.startswith("test_"):
            continue
        yield source


def serving_services(root: pathlib.Path) -> set[str]:
    found = set()
    for main in root.glob("*/main.go"):
        service_dir = main.parent
        for source in service_dir.rglob("*.go"):
            if not source.name.endswith("_test.go") and IMPORT.search(source.read_text()):
                found.add(service_dir.name)
                break
    for main in root.glob("*/main.py"):
        service_dir = main.parent
        if any(PY_IMPORT.search(source.read_text(encoding="utf-8")) for source in python_sources(service_dir)):
            found.add(service_dir.name)
    return found


def registered_metrics(root: pathlib.Path) -> set[str]:
    found = set()
    for source in root.rglob("*.go"):
        relative = source.relative_to(root).parts
        if source.name.endswith("_test.go") or any(p.startswith(".") for p in relative):
            continue
        found.update(METRIC_NAME.findall(source.read_text()))
    for main in root.glob("*/main.py"):
        for source in python_sources(main.parent):
            found.update(PY_METRIC_NAME.findall(source.read_text(encoding="utf-8")))
    return found


def check_documented_metrics(root: pathlib.Path) -> list[str]:
    registered = registered_metrics(root)
    documented = set(DOCUMENTED_METRIC.findall((root / "deploy" / "OBSERVABILITY.md").read_text()))
    problems = []
    if not registered:
        problems.append("found no registered continuo_* metric in the source")
    if missing := registered - documented:
        problems.append(f"deploy/OBSERVABILITY.md does not list {sorted(missing)}")
    if stale := documented - registered:
        problems.append(f"deploy/OBSERVABILITY.md lists {sorted(stale)}, which the source does not register")
    return problems


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
        problems.append(f"metrics port on {sorted(exposed)}, but pkg/metrics or prometheus_client is imported by {sorted(serving)}")
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
    problems = check_defaults(defaults, serving, port) + check_scrape_rule(load(scrape_path), serving, port) + check_documented_metrics(root)
    for p in problems:
        logger.error("FAIL: %s", p)
    if not problems:
        logger.info("metrics: port %s on %s; scrape rule admits only that port; the guide lists every metric", port, ", ".join(sorted(serving)))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
