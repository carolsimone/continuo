"""Aggregate benchmark result folders (one per scenario, rep*.json inside) into markdown."""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import metrics as m

log = logging.getLogger(__name__)

MAX_SAMPLE_PAUSE_S = 60.0  # the sampler ticks every 5 s; a longer gap means the host slept or stalled

SCALARS = ["wall_s", "busy_s", "idle_s", "first_start_s", "finalize_s", "observed_done_s",
           "handoff_p50_s", "handoff_p95_s", "messages_per_task", "attempts", "failed_attempts",
           "jobs_active_at_cancel", "overlap_s", "messages_dropped"]


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
    return "\n".join(lines)


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
    if len(argv) != 1:
        log.error("usage: report.py RESULTS_DIR")
        return 2
    scenarios = {}
    for rep in sorted(Path(argv[0]).glob("*/rep*.json")):
        scenarios.setdefault(rep.parent.name, []).append(json.loads(rep.read_text(encoding="utf-8")))
    if not scenarios:
        log.error("no rep*.json files under %s", argv[0])
        return 1
    sys.stdout.write(render(scenarios))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
