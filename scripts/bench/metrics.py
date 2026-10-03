"""Pure timing and resource computations for the run-lifecycle benchmark."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

MAX_SAMPLE_GAP_S = 15.0
_TS = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:\d{2})$")


def parse_ts(value: str) -> datetime:
    match = _TS.match(value.strip())
    if not match:
        raise ValueError(f"unparseable timestamp: {value!r}")
    base = datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%S")
    if match.group(2):
        base += timedelta(microseconds=round(float(match.group(2)) * 1_000_000))
    zone = match.group(3)
    if zone == "Z":
        return base.replace(tzinfo=timezone.utc)
    sign = 1 if zone[0] == "+" else -1
    offset = timedelta(hours=int(zone[1:3]), minutes=int(zone[4:6]))
    return (base - sign * offset).replace(tzinfo=timezone.utc)


def percentile(values: list, q: float) -> float:
    if not values:
        raise ValueError("percentile of an empty list")
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


@dataclass(frozen=True)
class Attempt:
    table: str
    created: datetime
    finished: Optional[datetime]
    failed: bool


def _env(job: dict, name: str) -> Optional[str]:
    for container in job.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
        for env in container.get("env") or []:
            if env.get("name") == name:
                return env.get("value")
    return None


def attempts_for_run(jobs_doc: dict, run_id: str) -> list:
    """One Attempt per Kubernetes Job of the run; a failed Job ends at its Failed condition."""
    attempts = []
    for job in jobs_doc.get("items", []):
        labels = job.get("metadata", {}).get("labels") or {}
        if labels.get("schedule-id") != run_id:
            continue
        table = _env(job, "TABLE_NAME")
        if table is None:
            continue
        status = job.get("status") or {}
        finished = status.get("completionTime")
        if finished is None:
            for condition in status.get("conditions") or []:
                if condition.get("type") == "Failed" and condition.get("status") == "True":
                    finished = condition.get("lastTransitionTime")
        attempts.append(Attempt(
            table=table,
            created=parse_ts(job["metadata"]["creationTimestamp"]),
            finished=parse_ts(finished) if finished else None,
            failed=bool(status.get("failed")),
        ))
    return attempts


def busy_and_gaps(intervals: list) -> tuple:
    """Union length of [start, end] intervals and the gaps between merged islands, in seconds."""
    ordered = sorted(intervals)
    if not ordered:
        return 0.0, []
    busy = 0.0
    gaps = []
    start, end = ordered[0]
    for next_start, next_end in ordered[1:]:
        if next_start > end:
            busy += (end - start).total_seconds()
            gaps.append((next_start - end).total_seconds())
            start, end = next_start, next_end
        else:
            end = max(end, next_end)
    busy += (end - start).total_seconds()
    return busy, gaps


def node_windows(attempts: list) -> tuple:
    """First Job creation and last Job finish per table across all attempts."""
    starts = {}
    finishes = {}
    for attempt in attempts:
        if attempt.table not in starts or attempt.created < starts[attempt.table]:
            starts[attempt.table] = attempt.created
        if attempt.finished and (attempt.table not in finishes or attempt.finished > finishes[attempt.table]):
            finishes[attempt.table] = attempt.finished
    return starts, finishes


def handoff_latencies(topology: list, starts: dict, finishes: dict) -> list:
    """Per node with upstreams: its first Job's creation minus its last upstream's finish."""
    table_of = {node["unique_id"]: node["table_name"] for node in topology}
    latencies = []
    for node in topology:
        upstream = [table_of[u] for u in node.get("upstream_unique_ids", []) if u in table_of]
        if not upstream or node["table_name"] not in starts:
            continue
        if any(u not in finishes for u in upstream):
            continue
        ready = max(finishes[u] for u in upstream)
        latencies.append((starts[node["table_name"]] - ready).total_seconds())
    return latencies


_MEM_UNITS = {
    "B": 1 / 1048576, "KiB": 1 / 1024, "Ki": 1 / 1024, "kB": 1000 / 1048576, "KB": 1000 / 1048576,
    "MiB": 1.0, "Mi": 1.0, "MB": 1e6 / 1048576, "GiB": 1024.0, "Gi": 1024.0, "GB": 1e9 / 1048576,
}
_NUM_UNIT = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]*)\s*$")


def parse_cpu_millicores(value: str) -> float:
    text = value.strip()
    if text.endswith("%"):
        return float(text[:-1]) * 10.0  # docker stats: 100% is one core
    if text.endswith("m"):
        return float(text[:-1])
    if text.endswith("n"):
        return float(text[:-1]) / 1e6
    return float(text) * 1000.0


def parse_mem_mib(value: str) -> float:
    match = _NUM_UNIT.match(value)
    if not match:
        raise ValueError(f"unparseable memory value: {value!r}")
    unit = match.group(2) or "B"
    if unit not in _MEM_UNITS:
        raise ValueError(f"unknown memory unit {unit!r} in {value!r}")
    return float(match.group(1)) * _MEM_UNITS[unit]


# StatefulSet pods of a datastore Helm release: <release>-redis-master-0, <release>-postgresql-0, ...
_INFRA_POD = re.compile(r"^.+-(?P<svc>neo4j|postgresql|redis)(-master)?-\d+$")
_COMPOSE_REPLICA = re.compile(r"^.+-(?P<svc>postgres|redis|neo4j|minio)-\d+$")
_DEPLOYMENT_POD = re.compile(r"^(?P<svc>.+)-[0-9a-f]{8,10}-[a-z0-9]{5}$")


def service_of(name: str) -> str:
    for pattern in (_INFRA_POD, _COMPOSE_REPLICA, _DEPLOYMENT_POD):
        match = pattern.match(name)
        if match:
            service = match.group("svc")
            return "postgres" if service == "postgresql" else service
    return name


@dataclass(frozen=True)
class Sample:
    ts: datetime
    service: str
    cpu_m: float
    mem_mib: float


def read_samples(lines: Iterable) -> list:
    """Parse sampler TSV (ts, name, cpu, mem); malformed lines are skipped."""
    samples = []
    for line in lines:
        parts = line.rstrip("\n").split("\t")
        if len(parts) != 4:
            continue
        try:
            samples.append(Sample(parse_ts(parts[0]), service_of(parts[1]),
                                  parse_cpu_millicores(parts[2]), parse_mem_mib(parts[3])))
        except ValueError:
            continue
    return samples


def usage(samples: list, start: datetime, end: datetime) -> dict:
    """Per service within [start, end]: replicas summed per instant, CPU integrated over time."""
    per_instant = {}
    for sample in samples:
        if start <= sample.ts <= end:
            total = per_instant.setdefault((sample.service, sample.ts), [0.0, 0.0])
            total[0] += sample.cpu_m
            total[1] += sample.mem_mib
    by_service = {}
    for (service, ts), (cpu, mem) in per_instant.items():
        by_service.setdefault(service, []).append((ts, cpu, mem))
    result = {}
    for service, points in by_service.items():
        points.sort()
        cpu_seconds = 0.0
        for previous, current in zip(points, points[1:]):
            gap = min((current[0] - previous[0]).total_seconds(), MAX_SAMPLE_GAP_S)
            cpu_seconds += current[1] / 1000.0 * gap
        result[service] = {
            "cpu_avg_m": sum(p[1] for p in points) / len(points),
            "cpu_max_m": max(p[1] for p in points),
            "mem_avg_mib": sum(p[2] for p in points) / len(points),
            "mem_max_mib": max(p[2] for p in points),
            "cpu_seconds": cpu_seconds,
            "samples": len(points),
        }
    return result


def max_sample_gap(samples: list, start: datetime, end: datetime) -> Optional[float]:
    """Longest stretch of [start, end] without a sampling instant, in seconds; None without samples.

    The window's edges count, so a sampler that started late or stopped early
    shows as a gap. The sampler ticks every few seconds; a much longer gap means
    the host slept, stalled or lost the sampler, and the rep's figures span it.
    """
    if not samples:
        return None
    points = [start] + sorted({sample.ts for sample in samples if start <= sample.ts <= end}) + [end]
    return max((b - a).total_seconds() for a, b in zip(points, points[1:]))


def messages_delta(before: dict, after: dict) -> dict:
    return {stream: count - before.get(stream, 0) for stream, count in after.items()
            if count - before.get(stream, 0) > 0}
