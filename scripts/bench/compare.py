"""Compare two benchmark result folders scenario by scenario, as markdown.

Each side is summarised as report.py summarises one folder (p50 over the reps),
after leaving out the reps whose sampling paused for longer than
report.MAX_SAMPLE_PAUSE_S, since their timings span a host sleep or stall.
"""
from __future__ import annotations

import argparse
import logging
import sys

import report

log = logging.getLogger(__name__)

DASH = "—"


def fmt(value, digits: int) -> str:
    return DASH if value is None else f"{value:.{digits}f}"


def change(before, after, digits: int) -> str:
    """after − before, with the relative change against the size of before when it is non-zero."""
    if before is None or after is None:
        return DASH
    delta = f"{after - before:+.{digits}f}"
    if before == 0:
        return delta
    return f"{delta} ({(after - before) / abs(before):+.0%})"


def split_paused(reps: list) -> tuple:
    kept, paused = [], []
    for r in reps:
        if (r.get("max_sample_gap_s") or 0) > report.MAX_SAMPLE_PAUSE_S:
            paused.append(r.get("rep"))
        else:
            kept.append(r)
    return kept, paused


def scenario_section(reps_a: list, reps_b: list, label_a: str, label_b: str) -> list:
    kept_a, paused_a = split_paused(reps_a)
    kept_b, paused_b = split_paused(reps_b)
    agg_a, agg_b = report.aggregate(kept_a), report.aggregate(kept_b)
    lines = [f"Reps: {label_a} {len(kept_a)}, {label_b} {len(kept_b)}.", ""]
    left_out = [f"{label} rep {paused}" for label, paused in ((label_a, paused_a), (label_b, paused_b)) if paused]
    if left_out:
        lines += [f"Left out, sampling pause over {report.MAX_SAMPLE_PAUSE_S:.0f} s: {'; '.join(left_out)}", ""]
    statuses_a, statuses_b = sorted(set(agg_a["final_status"])), sorted(set(agg_b["final_status"]))
    if statuses_a != statuses_b:
        lines += [f"Outcomes differ ({label_a}: {', '.join(statuses_a) or DASH}; "
                  f"{label_b}: {', '.join(statuses_b) or DASH}), so the timings below do not measure the same work.",
                  ""]

    lines += [f"| metric (p50) | {label_a} | {label_b} | change |", "|---|---|---|---|"]
    for key in report.SCALARS:
        a, b = agg_a.get(key, {}).get("p50"), agg_b.get(key, {}).get("p50")
        if a is not None or b is not None:
            lines.append(f"| {key} | {fmt(a, 2)} | {fmt(b, 2)} | {change(a, b, 2)} |")

    services = sorted(set(agg_a["usage"]) | set(agg_b["usage"]))
    if services:
        lines += ["", f"| service | CPU-s per 1,000 tasks {label_a} | {label_b} | change "
                      f"| peak memory MiB {label_a} | {label_b} | change |",
                  "|---|---|---|---|---|---|---|"]
        for service in services:
            ua, ub = agg_a["usage"].get(service, {}), agg_b["usage"].get(service, {})
            cpu_a, cpu_b = ua.get("cpu_s_per_1k_tasks_p50"), ub.get("cpu_s_per_1k_tasks_p50")
            mem_a, mem_b = ua.get("mem_peak_mib_p50"), ub.get("mem_peak_mib_p50")
            lines.append(f"| {service} | {fmt(cpu_a, 1)} | {fmt(cpu_b, 1)} | {change(cpu_a, cpu_b, 1)} "
                         f"| {fmt(mem_a, 0)} | {fmt(mem_b, 0)} | {change(mem_a, mem_b, 0)} |")

    idle_services = sorted(set(agg_a["idle"]) | set(agg_b["idle"]))
    if idle_services:
        lines += ["", f"| service | idle CPU m {label_a} | {label_b} | change "
                      f"| idle memory MiB {label_a} | {label_b} | change |",
                  "|---|---|---|---|---|---|---|"]
        for service in idle_services:
            ia, ib = agg_a["idle"].get(service, {}), agg_b["idle"].get(service, {})
            cpu_a, cpu_b = ia.get("cpu_avg_m"), ib.get("cpu_avg_m")
            mem_a, mem_b = ia.get("mem_avg_mib"), ib.get("mem_avg_mib")
            lines.append(f"| {service} | {fmt(cpu_a, 0)} | {fmt(cpu_b, 0)} | {change(cpu_a, cpu_b, 0)} "
                         f"| {fmt(mem_a, 0)} | {fmt(mem_b, 0)} | {change(mem_a, mem_b, 0)} |")
    lines.append("")
    return lines


def compare(before: dict, after: dict, label_a: str, label_b: str) -> str:
    lines = [f"# Benchmark comparison: {label_a} → {label_b}", ""]
    for name in sorted(set(before) | set(after)):
        lines += [f"## {name}", ""]
        if name not in after:
            lines += [f"Only in {label_a}.", ""]
        elif name not in before:
            lines += [f"Only in {label_b}.", ""]
        else:
            lines += scenario_section(before[name], after[name], label_a, label_b)
    return "\n".join(lines)


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Compare two benchmark result folders as markdown.")
    parser.add_argument("before", help="results folder of the earlier run (one sub-folder per scenario)")
    parser.add_argument("after", help="results folder of the later run")
    parser.add_argument("--labels", default="before,after", help="names of the two sides, comma-separated")
    args = parser.parse_args(argv)
    labels = args.labels.split(",")
    if len(labels) != 2:
        log.error("--labels takes exactly two names, comma-separated")
        return 2
    sides = []
    for folder in (args.before, args.after):
        scenarios = report.load(folder)
        if not scenarios:
            log.error("no rep*.json files under %s", folder)
            return 1
        sides.append(scenarios)
    sys.stdout.write(compare(sides[0], sides[1], labels[0], labels[1]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
