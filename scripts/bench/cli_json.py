"""Read the continuo CLI's JSON output (stdin) for the benchmark shell scripts."""
from __future__ import annotations

import json
import logging
import sys

log = logging.getLogger(__name__)


def has_schedule(doc: dict, name: str) -> bool:
    return any(entry.get("schedule_name") == name for entry in doc.get("schedules", []))


def run_state(doc: dict) -> tuple:
    return doc.get("run_id", ""), bool(doc.get("is_running")), doc.get("last_run_status", "")


def schedule_names(doc: dict) -> list:
    return sorted(entry["schedule_name"] for entry in doc.get("schedules", []) if entry.get("schedule_name"))


def same_schedules(a: dict, b: dict) -> bool:
    return schedule_names(a) == schedule_names(b)


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
    if not argv:
        log.error("usage: cli_json.py has-schedule NAME | run-state | field NAME | schedule-names | same-schedules FILE")
        return 2
    doc = json.load(sys.stdin)
    command = argv[0]
    if command == "has-schedule" and len(argv) == 2:
        return 0 if has_schedule(doc, argv[1]) else 1
    if command == "run-state" and len(argv) == 1:
        run_id, running, status = run_state(doc)
        print(f"{run_id} {'true' if running else 'false'} {status}")
        return 0
    if command == "field" and len(argv) == 2:
        value = doc.get(argv[1])
        if value is None:
            log.error("field %s missing from CLI output", argv[1])
            return 1
        print(value)
        return 0
    if command == "schedule-names" and len(argv) == 1:
        for name in schedule_names(doc):
            print(name)
        return 0
    if command == "same-schedules" and len(argv) == 2:
        with open(argv[1], encoding="utf-8") as handle:
            return 0 if same_schedules(doc, json.load(handle)) else 1
    log.error("unknown command: %s", " ".join(argv))
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
