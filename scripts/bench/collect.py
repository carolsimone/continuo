"""Turn one benchmark rep's raw captures into a result JSON."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import metrics as m

log = logging.getLogger(__name__)


def cancel_overlap(jobs_doc: dict, run_id: str, cancel_ts: datetime, next_run_id: Optional[str],
                   trigger_ts: datetime) -> dict:
    old = m.attempts_for_run(jobs_doc, run_id)
    active = [a for a in old if a.created <= cancel_ts and (a.finished is None or a.finished > cancel_ts)]
    out = {
        "cancel_s": (cancel_ts - trigger_ts).total_seconds(),
        "jobs_active_at_cancel": len(active),
        "jobs_never_finished": sum(1 for a in active if a.finished is None),
        "overlap_s": None,
    }
    last_old = max((a.finished for a in active if a.finished), default=None)
    if next_run_id and last_old:
        new = m.attempts_for_run(jobs_doc, next_run_id)
        if new:
            first_new = min(a.created for a in new)
            out["overlap_s"] = max(0.0, (last_old - first_new).total_seconds())
    return out


def build_result(*, scenario: str, rep: int, operation: str, run_id: str, payload: dict, jobs_doc: dict,
                 trigger_ts: datetime, done_ts: datetime, samples: list, idle_window, streams_before: dict,
                 streams_after: dict, extra: Optional[dict] = None, cancel_ts: Optional[datetime] = None,
                 next_run_id: Optional[str] = None) -> dict:
    topology = payload["topology"]
    attempts = m.attempts_for_run(jobs_doc, run_id)
    if not attempts:
        raise ValueError(f"no Jobs found for run {run_id}")
    starts, finishes = m.node_windows(attempts)
    finished = [(a.created, a.finished) for a in attempts if a.finished]
    busy, gaps = m.busy_and_gaps(finished)
    first_start = min(a.created for a in attempts)
    last_finish = max((f for _, f in finished), default=first_start)
    wall = (last_finish - trigger_ts).total_seconds()
    handoffs = m.handoff_latencies(topology, starts, finishes) if operation == "run" else []
    deltas = m.messages_delta(streams_before, streams_after)
    total = sum(deltas.values())
    result = {
        "scenario": scenario, "rep": rep, "operation": operation, "run_id": run_id,
        "nodes": len(topology), "nodes_executed": len(starts), "attempts": len(attempts),
        "failed_attempts": sum(1 for a in attempts if a.failed),
        "wall_s": wall, "busy_s": busy, "idle_s": wall - busy, "gaps_s": gaps,
        "first_start_s": (first_start - trigger_ts).total_seconds(),
        "observed_done_s": (done_ts - trigger_ts).total_seconds(),
        "finalize_s": (done_ts - last_finish).total_seconds(),
        "handoff_p50_s": m.percentile(handoffs, 0.5) if handoffs else None,
        "handoff_p95_s": m.percentile(handoffs, 0.95) if handoffs else None,
        "handoff_max_s": max(handoffs) if handoffs else None,
        "messages_total": total,
        "messages_per_task": total / max(len(starts), 1),
        "messages_by_stream": deltas,
        "usage_run": m.usage(samples, trigger_ts, done_ts),
        "usage_idle": m.usage(samples, idle_window[0], idle_window[1]) if idle_window else {},
    }
    if cancel_ts is not None:
        result.update(cancel_overlap(jobs_doc, run_id, cancel_ts, next_run_id, trigger_ts))
    result.update(extra or {})
    return result


def _load(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8")) if path else {}


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--scenario", required=True)
    p.add_argument("--rep", type=int, required=True)
    p.add_argument("--operation", choices=["run", "test"], required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--payload", required=True)
    p.add_argument("--jobs", required=True)
    p.add_argument("--trigger-ts", required=True)
    p.add_argument("--done-ts", required=True)
    p.add_argument("--samples", default="")
    p.add_argument("--idle-start", default="")
    p.add_argument("--idle-end", default="")
    p.add_argument("--streams-before", default="")
    p.add_argument("--streams-after", default="")
    p.add_argument("--cancel-ts", default="")
    p.add_argument("--next-run-id", default="")
    p.add_argument("--extra", action="append", default=[], help="key=value added to the result")
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    samples = m.read_samples(Path(a.samples).read_text(encoding="utf-8").splitlines()) if a.samples else []
    idle = (m.parse_ts(a.idle_start), m.parse_ts(a.idle_end)) if a.idle_start and a.idle_end else None
    extra = dict(item.split("=", 1) for item in a.extra)
    try:
        result = build_result(
            scenario=a.scenario, rep=a.rep, operation=a.operation, run_id=a.run_id,
            payload=_load(a.payload), jobs_doc=_load(a.jobs),
            trigger_ts=m.parse_ts(a.trigger_ts), done_ts=m.parse_ts(a.done_ts),
            samples=samples, idle_window=idle,
            streams_before=_load(a.streams_before), streams_after=_load(a.streams_after),
            extra=extra, cancel_ts=m.parse_ts(a.cancel_ts) if a.cancel_ts else None,
            next_run_id=a.next_run_id or None)
    except ValueError as err:
        log.error("%s", err)
        return 1
    Path(a.out).write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    log.info("%s rep %d: wall %.1fs, idle %.1fs, %d messages", a.scenario, a.rep,
             result["wall_s"], result["idle_s"], result["messages_total"])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
