"""The Prometheus series topology-controller exports.

`TopologyMetrics` owns one CollectorRegistry and serves two ports: it is the
release observer of the candidate handler (service.ports.ReleaseObserverPort)
and the observer of the stream consumer (adapters.redis.consumer.ConsumerObserver).

Every series declares `service` as its first label, because a prometheus-client
registry takes no labels of its own and the Go services' series carry it. The
registry holds no process or Python runtime collectors for the same reason: they
cannot carry the label. Each metric name is a literal at its definition, so a
scan of this file's source finds every series the service exports.
"""
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, disable_created_metrics

from domain.contract_vocabulary import ParseFailureKind
from domain.model import SkipReason
from service.release_timings import ReleaseOutcome, ReleaseTimings, Stage

# Seconds a release's stage can take, from a manifest download to the whole
# parse of a large estate.
STAGE_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200, 1800)

# The buckets of pkg/metrics/consumers.go's continuo_consumer_handler_duration_seconds.
CONSUMER_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300)

# The failure_kind of a release that did not fail.
_NO_FAILURE_KIND = "none"


class TopologyMetrics:
    """Turns finished releases and consumer events into the service's series."""

    def __init__(self, service: str) -> None:
        # Prometheus-client appends a `_created` timestamp series to every
        # counter and histogram unless told not to, and the global switch only
        # affects series defined after it.
        disable_created_metrics()
        self.registry = CollectorRegistry()
        self._service = service

        self._stage_duration = Histogram(
            "continuo_topology_stage_duration_seconds",
            "Seconds one stage of a release's parse took, summed over its runs, once per release that ended.",
            ["service", "stage"],
            buckets=STAGE_BUCKETS,
            registry=self.registry,
        )
        self._releases = Counter(
            "continuo_topology_releases_total",
            "Releases whose parse ended, by outcome (ok, failed) and failure kind (none when it did not fail).",
            ["service", "outcome", "failure_kind"],
            registry=self.registry,
        )
        self._release_nodes = Gauge(
            "continuo_topology_release_nodes",
            "Nodes the last release that ended held, by family (dbt, python).",
            ["service", "family"],
            registry=self.registry,
        )
        self._release_columns = Gauge(
            "continuo_topology_release_columns",
            "Columns the nodes of the last release that ended declare, by family (dbt, python).",
            ["service", "family"],
            registry=self.registry,
        )
        self._release_skipped_nodes = Gauge(
            "continuo_topology_release_skipped_nodes",
            "Nodes the parse of the last release that ended left out, by reason.",
            ["service", "reason"],
            registry=self.registry,
        )
        self._release_peak_rss = Gauge(
            "continuo_topology_release_peak_rss_bytes",
            "Largest resident set read at a stage boundary during the last release that ended.",
            ["service"],
            registry=self.registry,
        )

        self._handled = Histogram(
            "continuo_consumer_handler_duration_seconds",
            "Handler invocations by result (ok, permanent, infrastructure, transient) and their duration.",
            ["service", "stream", "group", "result"],
            buckets=CONSUMER_BUCKETS,
            registry=self.registry,
        )
        self._dead_letters = Counter(
            "continuo_consumer_dead_letters_total",
            "Messages written to the consumer dead-letter stream, by kind.",
            ["service", "stream", "group", "kind"],
            registry=self.registry,
        )
        self._pauses = Counter(
            "continuo_consumer_infra_pauses_total",
            "Pauses after an infrastructure error.",
            ["service", "stream", "group"],
            registry=self.registry,
        )

        # Every stage, every pair a release can end under and every skip reason
        # is visible from boot, so a rate or an increase over a series that has
        # not moved yet is 0, not absent (an increase does not count a series'
        # first sample from an implied 0).
        for stage in Stage:
            self._stage_duration.labels(service, stage.value)
        self._releases.labels(service, ReleaseOutcome.OK.value, _NO_FAILURE_KIND)
        for kind in ParseFailureKind:
            self._releases.labels(service, ReleaseOutcome.FAILED.value, kind.value)
        for reason in SkipReason:
            self._release_skipped_nodes.labels(service, reason.value).set(0)

    def release_finished(self, timings: ReleaseTimings) -> None:
        """Count a release that ended and replace the gauges of the last one.

        A stage that never ran observes nothing. The gauges are set for every
        family and every skip reason the record names, so one the release lacked
        drops to 0 instead of keeping the previous release's value.
        """
        for stage, record in timings.stages.items():
            if record.runs > 0:
                self._stage_duration.labels(self._service, stage.value).observe(record.seconds)
        failure_kind = timings.failure_kind.value if timings.failure_kind is not None else _NO_FAILURE_KIND
        self._releases.labels(self._service, timings.outcome.value, failure_kind).inc()
        for family, count in timings.nodes.items():
            self._release_nodes.labels(self._service, family).set(count)
        for family, count in timings.columns.items():
            self._release_columns.labels(self._service, family).set(count)
        for reason in SkipReason:
            self._release_skipped_nodes.labels(self._service, reason.value).set(timings.skipped.get(reason, 0))
        self._release_peak_rss.labels(self._service).set(timings.peak_rss_bytes)

    def handled(self, stream: str, group: str, result: str, seconds: float) -> None:
        self._handled.labels(self._service, stream, group, result).observe(seconds)

    def dead_lettered(self, stream: str, group: str, kind: str) -> None:
        self._dead_letters.labels(self._service, stream, group, kind).inc()

    def paused(self, stream: str, group: str) -> None:
        self._pauses.labels(self._service, stream, group).inc()
