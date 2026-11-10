"""Runs the real release parse over a synthetic estate and writes one JSON line
per size, pass and repetition.

The parent process starts one child per size and repetition, because a new
process makes the child's first pass cold: no sqlglot or YAML state is warm.
The child builds the estate and one in-memory S3, then parses the release
twice, `cold` and `warm`, with the handler `main.py` builds, and writes one
JSON line to stdout per pass. The parent adds the facts about the machine and
the checkout to each line. Diagnostics go to stderr through logging, with the
same format `main.py` uses.
"""
import argparse
import json
import logging
import platform
import resource
import subprocess
import sys
import time
from collections.abc import Sequence
from importlib import metadata
from pathlib import Path

import yaml

from adapters.candidate_spec_uploader import CandidateSpecUploader
from adapters.candidate_sql_uploader import CandidateSqlUploader
from adapters.code_bundle_uploader import CodeBundleUploader
from adapters.process import MonotonicClock, ResidentMemoryProbe
from adapters.sources.s3 import S3Source
from adapters.topology_artifact_writer import TopologyArtifactWriter
from bench.estate import Estate, generate_estate
from bench.memory_s3 import MemoryS3
from domain.contract_vocabulary import ParseFailureKind
from domain.model import FailedNode, Runtime, TopologyArtifactRef
from service.candidate_artifacts import DbtSqlArtifactBuilder, PythonSpecArtifactBuilder
from service.candidate_manifest_handler import CandidateManifestHandler
from service.ports import ClockPort, MemoryProbePort

logger = logging.getLogger(__name__)

BUCKET = "bench"
# The version of the result line's fields, documented in README.md.
RESULT_SCHEMA = 1
_ENV = "bench"
_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
_SERVICE_DIR = Path(__file__).resolve().parent.parent
_PACKAGES = ("sqlglot", "PyYAML", "jsonschema", "continuo-contracts", "prometheus-client")
_STDERR_TAIL_LINES = 50
_DIALECTS = ("postgres", "trino")


class RecordingPublisher:
    """A CandidatePublisherPort that keeps the outcome of the last publish."""

    def __init__(self) -> None:
        self.ok = False
        self.failure = ""
        self.node_count = 0

    def publish_ok(self, *, release_id: str, artifact: TopologyArtifactRef, code_bundle_uri: str) -> None:
        self.ok = True
        self.node_count = artifact.node_count

    def publish_failed(
        self,
        release_id: str,
        failure_kind: ParseFailureKind,
        detail: str,
        failed_nodes: Sequence[FailedNode] = (),
    ) -> None:
        self.ok = False
        self.failure = f"{failure_kind}: {detail}"


def build_handler(
    estate: Estate,
    store: MemoryS3,
    *,
    clock: ClockPort,
    memory: MemoryProbePort,
    dialect: str = "postgres",
) -> tuple[CandidateManifestHandler, RecordingPublisher]:
    """The handler `main.py` builds, over `store` instead of S3, with a
    publisher that records the outcome instead of writing to Redis."""
    publisher = RecordingPublisher()
    handler = CandidateManifestHandler(
        source=S3Source(bucket=BUCKET, env=_ENV, s3_client=store, keys=estate.requests),
        publisher=publisher,
        bundle_uploader=CodeBundleUploader(store, BUCKET),
        artifact_writer=TopologyArtifactWriter(store, BUCKET),
        artifact_builders={
            Runtime.DBT: DbtSqlArtifactBuilder(CandidateSqlUploader(store, BUCKET)),
            Runtime.PYTHON: PythonSpecArtifactBuilder(CandidateSpecUploader(store, BUCKET)),
        },
        dialect=dialect,
        image_tags=estate.image_tags,
        is_infrastructure_error=lambda exc: False,
        clock=clock,
        memory=memory,
    )
    return handler, publisher


def _max_rss_bytes() -> int:
    """The process's peak resident set: getrusage reports kilobytes on Linux
    and bytes on macOS."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak * 1024 if sys.platform.startswith("linux") else peak


def _run_child(args: argparse.Namespace) -> int:
    """Parse the estate twice and write one JSON line per pass to stdout."""
    estate = generate_estate(args.size, columns=args.columns)
    store = MemoryS3(estate.objects, put_latency_ms=args.put_latency_ms)
    for pass_name in ("cold", "warm"):
        store.reset_counts()
        handler, publisher = build_handler(
            estate, store, clock=MonotonicClock(), memory=ResidentMemoryProbe(), dialect=args.dialect,
        )
        started = time.perf_counter()
        timings = handler.handle(estate.release_id)
        handler_seconds = time.perf_counter() - started
        if not publisher.ok:
            logger.error("the %s pass of size %d did not publish: %s", pass_name, args.size, publisher.failure)
            return 1
        line = {
            "dialect": args.dialect,
            "size": args.size,
            "columns": args.columns,
            "put_latency_ms": args.put_latency_ms,
            "pass": pass_name,
            "rep": args.rep,
            "nodes": {"dbt": estate.dbt_nodes, "python": estate.python_nodes},
            "puts": store.puts,
            "put_bytes": store.put_bytes,
            "handler_seconds": handler_seconds,
            "stages": {
                stage.value: {"seconds": record.seconds, "runs": record.runs, "counts": dict(record.counts)}
                for stage, record in timings.stages.items()
            },
            "peak_rss_bytes": timings.peak_rss_bytes,
            "max_rss_bytes": _max_rss_bytes(),
        }
        # The result lines are this process's stdout protocol: the parent reads
        # them, and everything else goes to stderr through logging.
        sys.stdout.write(json.dumps(line) + "\n")
        sys.stdout.flush()
    return 0


def _command_output(command: list[str]) -> str | None:
    """The stdout of a command run in the service directory, or None when the
    command is missing or fails."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False, cwd=_SERVICE_DIR)
    except OSError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _git_state() -> tuple[str | None, bool | None]:
    """The checkout's commit and whether it has uncommitted changes; both None
    where there is no git or no checkout (the dev image has neither)."""
    commit = _command_output(["git", "rev-parse", "HEAD"])
    status = _command_output(["git", "status", "--porcelain"])
    if not commit or status is None:
        return None, None
    return commit, bool(status)


def _package_versions() -> dict[str, str]:
    versions = {}
    for name in _PACKAGES:
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            continue
    return versions


def _cpu_model() -> str | None:
    """The CPU's name from sysctl (macOS), else the `model name` line of
    /proc/cpuinfo; an arm64 Linux kernel has no such line, so None."""
    brand = _command_output(["sysctl", "-n", "machdep.cpu.brand_string"])
    if brand:
        return brand
    try:
        cpuinfo = Path("/proc/cpuinfo").read_text()
    except OSError:
        return None
    for line in cpuinfo.splitlines():
        if line.startswith("model name"):
            return line.partition(":")[2].strip()
    return None


def _cpu_limit() -> float | None:
    """The cgroup CPU quota in CPUs (quota divided by period), None when the
    quota is `max` or there is no cgroup file."""
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
    except (OSError, ValueError):
        return None
    if quota == "max":
        return None
    return int(quota) / int(period)


def _environment() -> dict:
    commit, dirty = _git_state()
    return {
        "commit": commit,
        "dirty": dirty,
        "python": platform.python_version(),
        "packages": _package_versions(),
        "libyaml": yaml.__with_libyaml__,
        "cpu": _cpu_model(),
        "cpu_limit": _cpu_limit(),
    }


def _child_command(args: argparse.Namespace, size: int, rep: int) -> list[str]:
    return [
        sys.executable, "-m", "bench.run", "--child",
        "--size", str(size), "--rep", str(rep),
        "--columns", str(args.columns),
        "--put-latency-ms", str(args.put_latency_ms),
        "--dialect", args.dialect,
    ]


def _run_parent(args: argparse.Namespace) -> int:
    """Run one child per size and repetition and write their result lines,
    each with the environment fields, to args.out."""
    environment = _environment()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as results:
        for size in (int(part) for part in args.sizes.split(",")):
            for rep in range(1, args.reps + 1):
                logger.info("running size %d, repetition %d of %d", size, rep, args.reps)
                child = subprocess.run(
                    _child_command(args, size, rep),
                    cwd=_SERVICE_DIR, capture_output=True, text=True, check=False,
                )
                if child.returncode != 0:
                    tail = "\n".join(child.stderr.splitlines()[-_STDERR_TAIL_LINES:])
                    logger.error(
                        "the child of size %d, repetition %d exited %d, the last %d lines of its stderr:\n%s",
                        size, rep, child.returncode, _STDERR_TAIL_LINES, tail,
                    )
                    return 1
                for text in child.stdout.splitlines():
                    line = {"schema": RESULT_SCHEMA, **environment, **json.loads(text)}
                    results.write(json.dumps(line) + "\n")
                results.flush()
    logger.info("wrote %s", out)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m bench.run",
        description="Time the release parse on synthetic estates, one JSON line per size, pass and repetition.",
    )
    parser.add_argument("--sizes", help="comma-separated model counts, for example 100,1000,10000")
    parser.add_argument("--reps", type=int, default=3, help="repetitions of each size, each in a new process")
    parser.add_argument("--columns", type=int, default=20, help="columns of each model")
    parser.add_argument("--put-latency-ms", type=float, default=0.0,
                        help="milliseconds each S3 put and download sleeps, to stand in for a round trip")
    parser.add_argument("--dialect", choices=_DIALECTS, default="postgres", help="the warehouse's SQL dialect")
    parser.add_argument("--out", help="the JSON-lines file to write, for example bench/results/baseline.jsonl")
    parser.add_argument("--child", action="store_true", help="internal: run one size and repetition")
    parser.add_argument("--size", type=int, help="internal: the model count of the child's run")
    parser.add_argument("--rep", type=int, help="internal: the repetition number of the child's run")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format=_LOG_FORMAT)
    if args.child:
        if args.size is None or args.rep is None:
            parser.error("--child needs --size and --rep")
        return _run_child(args)
    if not args.sizes or not args.out:
        parser.error("--sizes and --out are required")
    return _run_parent(args)


if __name__ == "__main__":
    sys.exit(main())
