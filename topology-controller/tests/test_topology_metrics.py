from continuo_contracts.ontology_check import RULES
from continuo_contracts.validate import REASONS
from prometheus_client import generate_latest

from adapters.metrics.prometheus import TopologyMetrics
from adapters.redis.consumer import ConsumerObserver
from domain.contract_vocabulary import ParseFailureKind
from domain.model import SkipReason
from service.ports import ReleaseObserverPort
from service.release_timings import (
    CONTRACT_FAILURE_REASONS,
    CONTRACT_FAILURE_STAGES,
    ReleaseOutcome,
    ReleaseTimings,
    Stage,
)
from streams_contract import RELEASE_REQUESTED_V1, TOPOLOGY_CONTROLLER_RELEASE_REQUESTED
from tests.fakes import FakeClock, ScriptedMemoryProbe

COMPONENT = {"component": "topology-controller"}


def _value(metrics, name, **labels):
    return metrics.registry.get_sample_value(name, {**COMPONENT, **labels})


def _finished(nodes, skipped=None, outcome=ReleaseOutcome.OK, kind=None, peak=0, dormant=None):
    timings = ReleaseTimings("rel-1", FakeClock(step=1.0), ScriptedMemoryProbe(peak))
    with timings.stage(Stage.RESOLVE, nodes=3):
        pass
    timings.nodes.update(nodes)
    timings.columns.update({family: count * 10 for family, count in nodes.items()})
    for reason, count in (skipped or {}).items():
        timings.skipped[reason] = count
    timings.dormant.update(dormant or {})
    timings.outcome, timings.failure_kind = outcome, kind
    return timings


def test_every_outcome_and_failure_kind_pair_and_every_skip_reason_is_exported_at_zero():
    metrics = TopologyMetrics("topology-controller")
    assert _value(metrics, "continuo_topology_releases_total", outcome="ok", failure_kind="none") == 0.0
    for kind in ParseFailureKind:
        assert _value(metrics, "continuo_topology_releases_total", outcome="failed", failure_kind=kind.value) == 0.0
    for reason in SkipReason:
        assert _value(metrics, "continuo_topology_release_skipped_nodes", reason=reason.value) == 0.0


def test_no_node_is_skipped_for_want_of_a_tag():
    """A model of the project with an owner and no tag is kept as a dormant node,
    so the skip reasons are the owner gate, the local stub and an untagged node
    of an installed package."""
    assert sorted(reason.value for reason in SkipReason) == ["installed_package", "local_stub", "no_owner"]
    metrics = TopologyMetrics("topology-controller")
    assert _value(metrics, "continuo_topology_release_skipped_nodes", reason="no_tag") is None


def test_the_installed_package_reason_is_exported_at_zero_from_boot_and_set_by_a_release():
    metrics = TopologyMetrics("topology-controller")
    assert _value(metrics, "continuo_topology_release_skipped_nodes", reason="installed_package") == 0.0
    metrics.release_finished(_finished({"dbt": 2}, {SkipReason.INSTALLED_PACKAGE: 4}))
    assert _value(metrics, "continuo_topology_release_skipped_nodes", reason="installed_package") == 4.0
    assert _value(metrics, "continuo_topology_release_skipped_nodes", reason="no_owner") == 0.0


def test_the_dormant_gauge_is_exported_at_zero_from_boot():
    metrics = TopologyMetrics("topology-controller")
    assert _value(metrics, "continuo_topology_release_dormant_nodes") == 0.0


def test_every_stage_and_reason_pair_is_exported_at_boot():
    """Every contract failure stage with every reason the library names (its
    validation's and the rules of its ontology checks) and the two reasons of
    the parse's own (a manifest entry the generator cannot read, a
    seed whose declared columns differ from its CSV header) are exported at 0
    before any release, so a rate over a failure that has not happened yet is 0
    and not absent."""
    metrics = TopologyMetrics("topology-controller")
    assert REASONS
    assert CONTRACT_FAILURE_STAGES == ("generate", "validate", "normalise", "hash", "ontology")
    assert CONTRACT_FAILURE_REASONS == (*REASONS, "manifest_shape", "seed_header", *RULES)
    for stage in CONTRACT_FAILURE_STAGES:
        for reason in CONTRACT_FAILURE_REASONS:
            assert _value(metrics, "continuo_topology_contract_failures_total", stage=stage, reason=reason) == 0.0
    samples = [
        line for line in generate_latest(metrics.registry).decode().splitlines()
        if line.startswith("continuo_topology_contract_failures_total{")
    ]
    assert len(samples) == len(CONTRACT_FAILURE_STAGES) * len(CONTRACT_FAILURE_REASONS)


def test_every_ontology_stage_and_rule_pair_is_exported_at_boot():
    """The ontology stage is a contract failure stage, and each rule of the library's ontology
    checks is a reason under it, visible at 0 before a release has failed one."""
    metrics = TopologyMetrics("topology-controller")
    assert "ontology" in CONTRACT_FAILURE_STAGES
    for rule in RULES:
        assert _value(metrics, "continuo_topology_contract_failures_total", stage="ontology", reason=rule) == 0.0
    assert _value(metrics, "continuo_topology_stage_duration_seconds_count", stage="ontology") == 0.0


def test_an_ontology_failure_feeds_the_contract_counter_and_the_stage_histogram():
    metrics = TopologyMetrics("topology-controller")
    timings = _finished({"python": 1})
    with timings.stage(Stage.ONTOLOGY):
        pass
    timings.add_contract_failure("ontology", "contract")
    timings.add_contract_failure("ontology", "contract")
    timings.add_contract_failure("ontology", "coverage")

    metrics.release_finished(timings)

    assert _value(metrics, "continuo_topology_contract_failures_total", stage="ontology", reason="contract") == 2.0
    assert _value(metrics, "continuo_topology_contract_failures_total", stage="ontology", reason="coverage") == 1.0
    assert _value(metrics, "continuo_topology_stage_duration_seconds_count", stage="ontology") == 1.0


def test_both_validation_cache_results_are_exported_at_boot():
    metrics = TopologyMetrics("topology-controller")
    for result in ("hit", "miss"):
        assert _value(metrics, "continuo_topology_validation_cache_total", result=result) == 0.0


def test_a_release_feeds_the_contract_counters():
    metrics = TopologyMetrics("topology-controller")
    timings = _finished({"python": 3})
    timings.add_contract_failure("validate", "column_type")
    timings.add_contract_failure("validate", "column_type")
    timings.add_contract_failure("validate", "strict_yaml")
    timings.add_contract_failure("normalise", "value")
    timings.add_contract_failure("hash", "value")
    timings.add_contract_failure("generate", "manifest_shape")
    timings.add_contract_failure("validate", "seed_header")
    timings.add_contract_failure("validate", "seed_header")
    for hit in (True, False, False, False):
        timings.add_validation_cache(hit)

    metrics.release_finished(timings)

    failures = "continuo_topology_contract_failures_total"
    assert _value(metrics, failures, stage="validate", reason="column_type") == 2.0
    assert _value(metrics, failures, stage="validate", reason="strict_yaml") == 1.0
    assert _value(metrics, failures, stage="normalise", reason="value") == 1.0
    assert _value(metrics, failures, stage="hash", reason="value") == 1.0
    assert _value(metrics, failures, stage="hash", reason="column_type") == 0.0
    assert _value(metrics, failures, stage="generate", reason="manifest_shape") == 1.0
    assert _value(metrics, failures, stage="validate", reason="seed_header") == 2.0
    assert _value(metrics, "continuo_topology_validation_cache_total", result="hit") == 1.0
    assert _value(metrics, "continuo_topology_validation_cache_total", result="miss") == 3.0


def test_the_contract_counters_add_up_over_releases_and_a_clean_release_adds_nothing():
    metrics = TopologyMetrics("topology-controller")
    first = _finished({"python": 1})
    first.add_contract_failure("validate", "value")
    first.add_validation_cache(False)
    metrics.release_finished(first)
    second = _finished({"python": 1})
    second.add_contract_failure("validate", "value")
    second.add_validation_cache(True)
    metrics.release_finished(second)
    metrics.release_finished(_finished({"dbt": 2}))

    assert _value(metrics, "continuo_topology_contract_failures_total", stage="validate", reason="value") == 2.0
    assert _value(metrics, "continuo_topology_validation_cache_total", result="hit") == 1.0
    assert _value(metrics, "continuo_topology_validation_cache_total", result="miss") == 1.0


def test_every_stage_is_exported_at_zero_from_boot():
    metrics = TopologyMetrics("topology-controller")
    for stage in Stage:
        assert _value(metrics, "continuo_topology_stage_duration_seconds_count", stage=stage.value) == 0.0
        assert _value(metrics, "continuo_topology_stage_duration_seconds_sum", stage=stage.value) == 0.0


def test_a_finished_release_moves_every_release_series():
    metrics = TopologyMetrics("topology-controller")
    metrics.release_finished(_finished({"dbt": 3, "python": 1}, {SkipReason.NO_OWNER: 2},
                                       ReleaseOutcome.FAILED, ParseFailureKind.INVALID_SQL, peak=1000))
    assert _value(metrics, "continuo_topology_releases_total", outcome="failed", failure_kind="invalid_sql") == 1.0
    assert _value(metrics, "continuo_topology_stage_duration_seconds_count", stage="resolve") == 1.0
    assert _value(metrics, "continuo_topology_stage_duration_seconds_sum", stage="resolve") == 1.0
    # The stage did not run in this release, so it is exported but not observed.
    assert _value(metrics, "continuo_topology_stage_duration_seconds_count", stage="download") == 0.0
    assert _value(metrics, "continuo_topology_release_nodes", family="dbt") == 3.0
    assert _value(metrics, "continuo_topology_release_columns", family="python") == 10.0
    assert _value(metrics, "continuo_topology_release_skipped_nodes", reason="no_owner") == 2.0
    assert _value(metrics, "continuo_topology_release_peak_rss_bytes") == 1000.0


def test_a_finished_release_sets_the_dormant_gauge():
    """The gauge holds the release's dormant nodes over every project."""
    metrics = TopologyMetrics("topology-controller")
    metrics.release_finished(_finished({"dbt": 2}, dormant={"shop": 1}))
    assert _value(metrics, "continuo_topology_release_dormant_nodes") == 1.0
    metrics.release_finished(_finished({"dbt": 3}, dormant={"shop": 1, "billing": 2}))
    assert _value(metrics, "continuo_topology_release_dormant_nodes") == 3.0


def test_an_ok_release_counts_under_the_none_failure_kind():
    metrics = TopologyMetrics("topology-controller")
    metrics.release_finished(_finished({"dbt": 1}))
    metrics.release_finished(_finished({"dbt": 1}))
    assert _value(metrics, "continuo_topology_releases_total", outcome="ok", failure_kind="none") == 2.0
    assert _value(metrics, "continuo_topology_stage_duration_seconds_count", stage="resolve") == 2.0


def test_the_last_release_gauges_drop_to_zero_for_what_the_next_release_lacks():
    metrics = TopologyMetrics("topology-controller")
    metrics.release_finished(_finished({"dbt": 3, "python": 1, "external": 2}, {SkipReason.NO_OWNER: 2},
                                       dormant={"shop": 1}))
    metrics.release_finished(_finished({}))
    families = ("dbt", "python", "external")
    assert [_value(metrics, "continuo_topology_release_nodes", family=f) for f in families] == [0.0, 0.0, 0.0]
    assert [_value(metrics, "continuo_topology_release_columns", family=f) for f in families] == [0.0, 0.0, 0.0]
    assert _value(metrics, "continuo_topology_release_skipped_nodes", reason="no_owner") == 0.0
    assert _value(metrics, "continuo_topology_release_dormant_nodes") == 0.0


def test_the_consumer_series_carry_the_go_names_labels_and_buckets():
    metrics = TopologyMetrics("topology-controller")
    labels = {"stream": RELEASE_REQUESTED_V1, "group": TOPOLOGY_CONTROLLER_RELEASE_REQUESTED}
    metrics.handled(RELEASE_REQUESTED_V1, TOPOLOGY_CONTROLLER_RELEASE_REQUESTED, "ok", 0.2)
    metrics.dead_lettered(RELEASE_REQUESTED_V1, TOPOLOGY_CONTROLLER_RELEASE_REQUESTED, "permanent")
    metrics.paused(RELEASE_REQUESTED_V1, TOPOLOGY_CONTROLLER_RELEASE_REQUESTED)
    assert _value(metrics, "continuo_consumer_handler_duration_seconds_bucket", result="ok", le="0.25", **labels) == 1.0
    assert _value(metrics, "continuo_consumer_handler_duration_seconds_bucket", result="ok", le="300.0", **labels) == 1.0
    assert _value(metrics, "continuo_consumer_dead_letters_total", kind="permanent", **labels) == 1.0
    assert _value(metrics, "continuo_consumer_infra_pauses_total", **labels) == 1.0


def test_no_created_series_are_exported():
    metrics = TopologyMetrics("topology-controller")
    metrics.release_finished(_finished({"dbt": 1}))
    assert "_created" not in generate_latest(metrics.registry).decode()


def test_the_registry_holds_only_the_component_series():
    metrics = TopologyMetrics("topology-controller")
    metrics.release_finished(_finished({"dbt": 1}))
    names = {line.split("{")[0].split(" ")[0] for line in generate_latest(metrics.registry).decode().splitlines()
             if line and not line.startswith("#")}
    assert names and all(name.startswith("continuo_") for name in names)


def test_the_metrics_object_serves_both_observer_ports():
    metrics = TopologyMetrics("topology-controller")
    assert isinstance(metrics, ReleaseObserverPort)
    assert isinstance(metrics, ConsumerObserver)
