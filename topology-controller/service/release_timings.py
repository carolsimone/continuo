"""Where the time of one release's parse goes, and what the release held.

The handler wraps each stage of the parse in `ReleaseTimings.stage`, which adds
the stage's elapsed seconds, its run count and its counts to one `StageRecord`
per stage. A stage that runs several times (once per manifest, once per node)
adds up in the same record. The record also keeps the largest resident set seen
at a stage boundary, how many nodes and declared columns each runtime
contributed, how many nodes the parse skipped and why, how many dormant nodes
each service holds, and how the release ended. `log_fields` renders all of it as
the fields of the release's log line.
"""
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from enum import StrEnum
from types import TracebackType

from domain.contract_vocabulary import ParseFailureKind
from domain.dormant import is_dormant
from domain.model import ManifestNode, Runtime, SkippedNode, SkipReason
from service.ports import ClockPort, MemoryProbePort


class Stage(StrEnum):
    """The stages of a release's parse, in pipeline order; a value is the key
    `ReleaseTimings.stages_ms` reports the stage under."""
    DOWNLOAD = "download"
    DBT_PARSE = "dbt_parse"
    PYTHON_PARSE = "python_parse"
    REGISTRY = "registry"
    RESOLVE = "resolve"
    REWRITE = "rewrite"
    CANDIDATE_UPLOAD = "candidate_upload"
    CODE_BUNDLE = "code_bundle"
    TOPOLOGY_ARTIFACT = "topology_artifact"
    PUBLISH = "publish"


class ReleaseOutcome(StrEnum):
    """How a release's parse ended: published to release-controller, or
    reported to it as failed."""
    OK = "ok"
    FAILED = "failed"


@dataclass
class StageRecord:
    """What one stage cost over the release: seconds summed over its runs, how
    many times it ran, and the named counts of what it handled."""
    seconds: float = 0.0
    runs: int = 0
    counts: dict[str, int] = field(default_factory=dict)

    def add(self, **counts: int) -> None:
        """Sum each count into the record; a count the stage only learns while
        it runs (the nodes a parse returned) is added from inside the stage."""
        for name, value in counts.items():
            self.counts[name] = self.counts.get(name, 0) + value


class _StageTimer:
    """The context manager `ReleaseTimings.stage` returns. It is a class rather
    than a generator-based context manager because a stage runs twice per node,
    and a use then costs little more than its two clock reads."""

    __slots__ = ("_timings", "_now", "_record", "_sample_memory", "_counts", "_started")

    def __init__(self, timings: "ReleaseTimings", now: Callable[[], float], record: StageRecord,
                 sample_memory: bool, counts: dict[str, int]) -> None:
        self._timings = timings
        self._now = now
        self._record = record
        self._sample_memory = sample_memory
        self._counts = counts
        self._started = 0.0

    def __enter__(self) -> StageRecord:
        # The memory reading comes before the clock's on entry and after it on
        # exit, so reading memory is not counted as the stage's time.
        if self._sample_memory:
            self._timings.sample_memory()
        self._started = self._now()
        return self._record

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 tb: TracebackType | None) -> None:
        elapsed = self._now() - self._started
        self._record.seconds += elapsed
        self._record.runs += 1
        self._record.add(**self._counts)
        if self._sample_memory:
            self._timings.sample_memory()


class ReleaseTimings:
    """The timing record of one release's parse.

    `stages` holds one record per `Stage`, in declaration order, from
    construction: a stage that did not run reads zero seconds and zero runs.
    `peak_rss_bytes` is the largest resident set read at the boundary of a stage
    that samples memory, so it is a lower bound of the process's true peak.
    `nodes` and `columns` hold, per runtime, the nodes the release holds (the
    nodes the parses returned and the unknown-external nodes, which no parse returns
    and which count under the `external` runtime) and the columns they
    declare; `skipped` holds the nodes the parses left out, per reason.
    `nodes`, `columns` and `skipped` name every runtime or reason from
    construction. `dormant` holds, per service, the dormant nodes the release
    holds (the nodes a release builds that belong to no schedule); a service
    with none has no entry.
    `outcome` and `failure_kind` stay unset until the release ends.
    """

    def __init__(self, release_id: str, clock: ClockPort, memory: MemoryProbePort) -> None:
        self.release_id = release_id
        self._clock = clock
        self._memory = memory
        self.stages: dict[Stage, StageRecord] = {stage: StageRecord() for stage in Stage}
        self.peak_rss_bytes = 0
        self.outcome: ReleaseOutcome | None = None
        self.failure_kind: ParseFailureKind | None = None
        self.nodes: dict[str, int] = {runtime.value: 0 for runtime in Runtime}
        self.columns: dict[str, int] = {runtime.value: 0 for runtime in Runtime}
        self.skipped: dict[SkipReason, int] = {reason: 0 for reason in SkipReason}
        self.dormant: dict[str, int] = {}

    def stage(self, name: Stage, *, sample_memory: bool = True, **counts: int) -> AbstractContextManager[StageRecord]:
        """Time one run of a stage. On exit, also when the body raised, the
        run's seconds, one run and `counts` are added to the stage's record.
        The context manager yields that record, so a count known only inside
        the stage can be added with `record.add(...)`. A stage that runs per
        node passes `sample_memory=False` and reads no memory."""
        return _StageTimer(self, self._clock.now, self.stages[name], sample_memory, counts)

    def sample_memory(self) -> None:
        """Read the resident set now and keep it if it is the largest so far."""
        self.peak_rss_bytes = max(self.peak_rss_bytes, self._memory.resident_bytes())

    def stages_ms(self) -> dict[str, int]:
        """Every stage's milliseconds by stage name, in pipeline order."""
        return {stage.value: round(record.seconds * 1000) for stage, record in self.stages.items()}

    def add_nodes(self, nodes: Iterable[ManifestNode]) -> None:
        """Count nodes the release holds, a parse's or the release's unknown-external
        nodes, and their declared columns, under each node's runtime, and each
        dormant node under its service."""
        for node in nodes:
            self.nodes[node.runtime] += 1
            self.columns[node.runtime] += node.declared_columns
            if is_dormant(node.node_type, node.schedule_name):
                self.dormant[node.service_name] = self.dormant.get(node.service_name, 0) + 1

    def add_skips(self, skips: Iterable[SkippedNode]) -> None:
        """Count the nodes a parse left out, under each skip's reason."""
        for skip in skips:
            self.skipped[skip.reason] += 1

    def log_fields(self) -> dict[str, object]:
        """The fields of the release's log line. `failure_kind` is present only
        for a failed release."""
        fields: dict[str, object] = {
            "release_id": self.release_id,
            "outcome": self.outcome.value if self.outcome is not None else None,
            "nodes": dict(self.nodes),
            "columns": dict(self.columns),
            "skipped": {reason.value: count for reason, count in self.skipped.items()},
            "dormant": dict(sorted(self.dormant.items())),
            "stages_ms": self.stages_ms(),
            "peak_rss_bytes": self.peak_rss_bytes,
        }
        if self.outcome is ReleaseOutcome.FAILED and self.failure_kind is not None:
            fields["failure_kind"] = self.failure_kind.value
        return fields
