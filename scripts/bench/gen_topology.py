"""Generate a synthetic layered DAG as a release-promotion payload for the benchmark.

Level k holds about nodes/levels dbt-model nodes named nKK_IIII; each node
depends on `fan_in` nodes of level k-1. With --fail-root, level 0 is a single
node named fail_root, which the benchmark image fails on purpose.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time

log = logging.getLogger(__name__)

LABEL_SAFE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
MAX_JOB_NAME_PREFIX = 54  # pkg/domain/naming.go: 63 - "-" - 8-character run-id suffix
MAX_SCHEDULE_NAME = 50  # scheduler_tracker.schedule_name varchar(50)
FAIL_ROOT = "fail_root"


def level_sizes(nodes: int, levels: int, fail_root: bool) -> list:
    if nodes < 1 or levels < 1:
        raise ValueError("nodes and levels must be at least 1")
    if levels > nodes:
        raise ValueError(f"levels ({levels}) cannot exceed nodes ({nodes})")
    if fail_root:
        if levels < 2:
            raise ValueError("--fail-root needs at least 2 levels")
        return [1] + level_sizes(nodes - 1, levels - 1, False)
    base, extra = divmod(nodes, levels)
    return [base + (1 if index < extra else 0) for index in range(levels)]


def node_name(level: int, index: int) -> str:
    return f"n{level:02d}_{index:04d}"


def build_topology(*, nodes: int, levels: int, fan_in: int, schedule: str, service: str, schema: str,
                   image_tag: str, fail_root: bool) -> list:
    if fan_in < 1:
        raise ValueError("fan_in must be at least 1")
    names = []
    for level, size in enumerate(level_sizes(nodes, levels, fail_root)):
        if level == 0 and fail_root:
            names.append([FAIL_ROOT])
        else:
            names.append([node_name(level, index) for index in range(size)])
    topology = []
    for level, row in enumerate(names):
        for index, table in enumerate(row):
            upstream = []
            if level > 0:
                previous = names[level - 1]
                for offset in range(min(fan_in, len(previous))):
                    unique_id = f"{schema}.{previous[(index + offset) % len(previous)]}"
                    if unique_id not in upstream:
                        upstream.append(unique_id)
            topology.append({
                "unique_id": f"{schema}.{table}",
                "schema_name": schema,
                "table_name": table,
                "service_name": service,
                "node_type": "dbt-model",
                "content_hash": "",
                "image_tag": image_tag,
                "schedule": schedule,
                "original_file_path": "",
                "upstream_unique_ids": upstream,
                "test_count": 1,
                "changed": False,
            })
    return topology


def job_name_prefix(service: str, schema: str, table: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", f"{service}-{schema}-{table}".lower()).strip("-")


def validate(topology: list, schedule: str) -> None:
    if len(schedule) > MAX_SCHEDULE_NAME or not LABEL_SAFE.match(schedule):
        raise ValueError(f"schedule {schedule!r} must be a label-safe name of at most {MAX_SCHEDULE_NAME} characters")
    seen = set()
    for node in topology:
        prefix = job_name_prefix(node["service_name"], node["schema_name"], node["table_name"])
        if len(prefix) > MAX_JOB_NAME_PREFIX:
            raise ValueError(f"job name prefix {prefix!r} exceeds {MAX_JOB_NAME_PREFIX} characters")
        if prefix in seen:
            raise ValueError(f"two nodes share the job name prefix {prefix!r}")
        seen.add(prefix)


def build_payload(topology: list, release_id: str, service: str, image_tag: str) -> dict:
    return {"release_id": release_id, "topology": topology, "image_tags": {service: image_tag}}


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, required=True)
    parser.add_argument("--levels", type=int, required=True)
    parser.add_argument("--fan-in", type=int, default=2)
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--service", default="bench")
    parser.add_argument("--schema", default="bench")
    parser.add_argument("--image-tag", default="v1")
    parser.add_argument("--fail-root", action="store_true")
    parser.add_argument("--release-id", default="")
    args = parser.parse_args(argv)
    try:
        topology = build_topology(nodes=args.nodes, levels=args.levels, fan_in=args.fan_in,
                                  schedule=args.schedule, service=args.service, schema=args.schema,
                                  image_tag=args.image_tag, fail_root=args.fail_root)
        validate(topology, args.schedule)
    except ValueError as err:
        log.error("%s", err)
        return 2
    release_id = args.release_id or f"bench-{args.schedule}-{int(time.time())}"
    json.dump(build_payload(topology, release_id, args.service, args.image_tag), sys.stdout)
    sys.stdout.write("\n")
    log.info("generated %d nodes in %d levels for schedule %s", len(topology), args.levels, args.schedule)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
