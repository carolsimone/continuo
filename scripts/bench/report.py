"""Aggregate benchmark result folders (one per scenario, rep*.json inside) into markdown."""
from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path
from typing import Optional

import metrics as m

log = logging.getLogger(__name__)

MAX_SAMPLE_PAUSE_S = 60.0  # the sampler ticks every 5 s; a longer gap means the host slept or stalled

SCALARS = ["wall_s", "busy_s", "idle_s", "first_start_s", "finalize_s", "observed_done_s",
           "handoff_p50_s", "handoff_p95_s", "observe_p50_s", "round_trip_p50_s", "to_dispatch_p50_s",
           "to_queue_p50_s", "admit_p50_s", "messages_per_task", "messages_per_attempt", "attempts",
           "failed_attempts", "retries", "retry_turnaround_p50_s", "overlapping_attempts",
           "jobs_active_at_cancel", "overlap_s", "messages_dropped"]

SWEEP = re.compile(r"^sweep-(\d+)$")

# The chart's default `execution.maxConcurrentJobs`. A level wider than this waits for admission by design, so
# the hand-off targets read only the sweep widths up to it.
ADMISSION_WIDTH = 50


def aggregate(reps: list) -> dict:
    out = {}
    for key in SCALARS:
        values = [float(r[key]) for r in reps if r.get(key) is not None]
        if values:
            out[key] = {"p50": m.percentile(values, 0.5), "p95": m.percentile(values, 0.95), "n": len(values)}
    usage = {}
    for r in reps:
        tasks = max(r.get("nodes_executed", 0), 1)
        for service, u in (r.get("usage_run") or {}).items():
            entry = usage.setdefault(service, {"cpu": [], "mem": []})
            entry["cpu"].append(u["cpu_seconds"] / tasks * 1000)
            entry["mem"].append(u["mem_max_mib"])
    out["usage"] = {service: {"cpu_s_per_1k_tasks_p50": m.percentile(v["cpu"], 0.5),
                              "mem_peak_mib_p50": m.percentile(v["mem"], 0.5)}
                    for service, v in usage.items()}
    idle = next((r["usage_idle"] for r in reps if r.get("usage_idle")), {})
    out["idle"] = {service: {"cpu_avg_m": u["cpu_avg_m"], "mem_avg_mib": u["mem_avg_mib"]}
                   for service, u in idle.items()}
    out["final_status"] = [r["final_status"] for r in reps if r.get("final_status")]
    return out


def p50_of(scenarios: dict, name: str, key: str) -> Optional[float]:
    """The p50 over a scenario's reps of one scalar, or None when no rep has it."""
    values = [float(r[key]) for r in scenarios.get(name, []) if r.get(key) is not None]
    return m.percentile(values, 0.5) if values else None


def sweep_slope(scenarios: dict) -> Optional[float]:
    """Least-squares slope of the hand-off p50 against the level width over the sweep-<width> scenarios
    up to ADMISSION_WIDTH, in seconds per task of width; None with fewer than two widths measured."""
    points = []
    for name in scenarios:
        match = SWEEP.match(name)
        if not match or int(match.group(1)) > ADMISSION_WIDTH:
            continue
        value = p50_of(scenarios, name, "handoff_p50_s")
        if value is not None:
            points.append((int(match.group(1)), value))
    if len(points) < 2:
        return None
    mean_x = sum(x for x, _ in points) / len(points)
    mean_y = sum(y for _, y in points) / len(points)
    spread = sum((x - mean_x) ** 2 for x, _ in points)
    return sum((x - mean_x) * (y - mean_y) for x, y in points) / spread


def largest(scenarios: dict, key: str) -> Optional[float]:
    values = [float(r[key]) for reps in scenarios.values() for r in reps if r.get(key) is not None]
    return max(values) if values else None


# The run-lifecycle targets: (measure, target, how it is read from the results).
TARGETS = [
    (f"hand-off p50 at the admission width (sweep-{ADMISSION_WIDTH})", "≤ 5 s",
     lambda s: p50_of(s, f"sweep-{ADMISSION_WIDTH}", "handoff_p50_s")),
    (f"hand-off growth per task of level width (sweep-* up to {ADMISSION_WIDTH})", "< 0.1 s", sweep_slope),
    ("retry turnaround p50 (fail-once)", "≤ 5 s", lambda s: p50_of(s, "fail-once", "retry_turnaround_p50_s")),
    ("messages per attempt (sweep-100)", "4", lambda s: p50_of(s, "sweep-100", "messages_per_attempt")),
    ("overlapping attempts, largest of any scenario", "0", lambda s: largest(s, "overlapping_attempts")),
]


def target_values(scenarios: dict) -> list:
    """(measure, target, value) for every target; value is None when the results hold no measurement."""
    return [(measure, target, read(scenarios)) for measure, target, read in TARGETS]


def render(scenarios: dict) -> str:
    lines = ["# Benchmark report", ""]
    for name in sorted(scenarios):
        agg = aggregate(scenarios[name])
        lines += [f"## {name}", "", f"Reps: {len(scenarios[name])}; final statuses: {agg['final_status']}", ""]
        paused = [r.get("rep") for r in scenarios[name] if (r.get("max_sample_gap_s") or 0) > MAX_SAMPLE_PAUSE_S]
        if paused:
            lines += [f"Reps with a sampling pause over {MAX_SAMPLE_PAUSE_S:.0f} s "
                      f"(host asleep or overloaded; exclude them): {paused}", ""]
        lines += ["| metric | p50 | p95 | n |", "|---|---|---|---|"]
        for key in SCALARS:
            if key in agg:
                lines.append(f"| {key} | {agg[key]['p50']:.2f} | {agg[key]['p95']:.2f} | {agg[key]['n']} |")
        if agg["usage"]:
            lines += ["", "| service | CPU-s per 1,000 tasks (p50) | peak memory MiB (p50) | idle CPU m | idle memory MiB |",
                      "|---|---|---|---|---|"]
            for service in sorted(agg["usage"]):
                u = agg["usage"][service]
                i = agg["idle"].get(service, {})
                lines.append(f"| {service} | {u['cpu_s_per_1k_tasks_p50']:.1f} | {u['mem_peak_mib_p50']:.0f} | "
                             f"{i.get('cpu_avg_m', float('nan')):.0f} | {i.get('mem_avg_mib', float('nan')):.0f} |")
        lines.append("")
    values = target_values(scenarios)
    if any(value is not None for _, _, value in values):
        lines += ["## Targets", "", "| measure | target | value |", "|---|---|---|"]
        for measure, target, value in values:
            lines.append(f"| {measure} | {target} | {'—' if value is None else f'{value:.2f}'} |")
        lines.append("")
    return "\n".join(lines)


def load(folder: str) -> dict:
    """Read every <scenario>/rep*.json under folder, grouped by scenario name."""
    scenarios = {}
    for rep in sorted(Path(folder).glob("*/rep*.json")):
        scenarios.setdefault(rep.parent.name, []).append(json.loads(rep.read_text(encoding="utf-8")))
    return scenarios


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
    if len(argv) != 1:
        log.error("usage: report.py RESULTS_DIR")
        return 2
    scenarios = load(argv[0])
    if not scenarios:
        log.error("no rep*.json files under %s", argv[0])
        return 1
    sys.stdout.write(render(scenarios))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
