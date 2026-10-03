"""Live-topology export, union with a bench DAG, and comparison with the live graphs.

On a shared install the benchmark never publishes a topology that omits a live
node: a promotion retires missing nodes and deletes those no recent run used,
cutting their code-version and failure-history links.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

log = logging.getLogger(__name__)

STRING_FIELDS = ("unique_id", "schema_name", "table_name", "service_name", "node_type",
                 "content_hash", "image_tag", "schedule", "original_file_path")


def snapshot_to_wire(node: dict) -> dict:
    """A release-controller current_prod node as a release-promotion node, marked unchanged."""
    wire = {field: node.get(field) or "" for field in STRING_FIELDS}
    wire["test_count"] = int(node.get("test_count") or 0)
    wire["upstream_unique_ids"] = list(node.get("upstream_unique_ids") or [])
    wire["changed"] = False
    if node.get("secret_ref"):
        wire["secret_ref"] = node["secret_ref"]
    return wire


def image_tags(nodes: list) -> dict:
    tags = {}
    for node in nodes:
        service, tag = node.get("service_name"), node.get("image_tag")
        if service and tag and service not in tags:
            tags[service] = tag
    return tags


def restore_payload(current: dict) -> dict:
    """The payload that re-announces the exported release unchanged."""
    if not current.get("release_id") or not isinstance(current.get("topology"), list):
        raise ValueError("the current_prod export needs a release_id and a topology list")
    nodes = [snapshot_to_wire(node) for node in current["topology"]]
    return {"release_id": current["release_id"], "topology": nodes, "image_tags": image_tags(nodes)}


def union(base: dict, bench: dict, release_id: str) -> dict:
    base_nodes = base["topology"]
    bench_nodes = bench["topology"]
    base_ids = {node["unique_id"] for node in base_nodes}
    clashes = sorted(node["unique_id"] for node in bench_nodes if node["unique_id"] in base_ids)
    if clashes:
        raise ValueError(f"bench nodes clash with live nodes: {clashes[:5]}")
    base_schedules = {node["schedule"] for node in base_nodes if node.get("schedule")}
    shared = sorted({node["schedule"] for node in bench_nodes} & base_schedules)
    if shared:
        raise ValueError(f"bench schedule already used by the live topology: {shared}")
    nodes = [dict(node, changed=False) for node in base_nodes] + list(bench_nodes)
    return {"release_id": release_id, "topology": nodes, "image_tags": image_tags(nodes)}


def graph_ids(graph: dict) -> set:
    return {f"{node['schema_name']}.{node['table_name']}".lower() for node in graph.get("nodes", [])}


def compare(payload: dict, graphs: list) -> list:
    """Problems between the export and the live schedule graphs; empty when they match."""
    exported = {node["unique_id"].lower() for node in payload["topology"]}
    scheduled = {node["unique_id"].lower() for node in payload["topology"] if node.get("schedule")}
    live = set()
    for graph in graphs:
        live |= graph_ids(graph)
    problems = []
    missing_live = sorted(scheduled - live)
    if missing_live:
        problems.append(f"{len(missing_live)} scheduled export nodes missing from the live graphs: {missing_live[:5]}")
    missing_export = sorted(live - exported)
    if missing_export:
        problems.append(f"{len(missing_export)} live graph nodes missing from the export: {missing_export[:5]}")
    return problems


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("restore-payload")
    u = sub.add_parser("union")
    u.add_argument("--base", required=True)
    u.add_argument("--bench", required=True)
    u.add_argument("--release-id", required=True)
    c = sub.add_parser("compare")
    c.add_argument("--payload", required=True)
    c.add_argument("--graphs-dir", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "restore-payload":
            payload = restore_payload(json.load(sys.stdin))
            json.dump(payload, sys.stdout)
            sys.stdout.write("\n")
            log.info("restore payload: release %s, %d nodes", payload["release_id"], len(payload["topology"]))
            return 0
        if args.command == "union":
            base = json.loads(Path(args.base).read_text(encoding="utf-8"))
            bench = json.loads(Path(args.bench).read_text(encoding="utf-8"))
            json.dump(union(base, bench, args.release_id), sys.stdout)
            sys.stdout.write("\n")
            return 0
        payload = json.loads(Path(args.payload).read_text(encoding="utf-8"))
        graphs = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(Path(args.graphs_dir).glob("*.json"))]
        problems = compare(payload, graphs)
    except (ValueError, KeyError) as err:
        log.error("%s", err)
        return 2
    for problem in problems:
        log.error("%s", problem)
    if not problems:
        log.info("export matches the live graphs (%d nodes)", len(payload["topology"]))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
