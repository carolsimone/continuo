import pytest

from domain.contract_vocabulary import NodeType, ParseFailureKind
from domain.model import ManifestNode, Runtime, SkippedNode, SkipReason
from service.release_timings import (
    CONTRACT_FAILURE_REASONS,
    CONTRACT_FAILURE_STAGES,
    REASON_IDENTITY,
    REASON_MANIFEST_SHAPE,
    REASON_SEED_HEADER,
    REASON_STRICT_YAML,
    REASON_VALUE,
    FailureStage,
    ReleaseOutcome,
    ReleaseTimings,
    Stage,
    StageRecord,
)
from tests.fakes import FakeClock, ScriptedMemoryProbe


def _timings(clock=None, memory=None):
    return ReleaseTimings("rel-1", clock or FakeClock(), memory or ScriptedMemoryProbe(0))


def test_a_stage_records_its_seconds_one_run_and_its_counts():
    clock = FakeClock()
    timings = _timings(clock)
    with timings.stage(Stage.RESOLVE, nodes=3):
        clock.t += 2.5
    assert timings.stages[Stage.RESOLVE] == StageRecord(seconds=2.5, runs=1, counts={"nodes": 3})


def test_a_stage_that_runs_twice_adds_up():
    clock = FakeClock()
    timings = _timings(clock)
    for seconds in (0.5, 0.25):
        with timings.stage(Stage.REWRITE, sample_memory=False, nodes=1):
            clock.t += seconds
    assert timings.stages[Stage.REWRITE] == StageRecord(seconds=0.75, runs=2, counts={"nodes": 2})


def test_counts_known_only_inside_a_stage_are_added_to_it():
    timings = _timings()
    with timings.stage(Stage.DOWNLOAD) as record:
        record.add(manifests=2, bytes=1024)
    assert timings.stages[Stage.DOWNLOAD].counts == {"manifests": 2, "bytes": 1024}


def test_a_stage_that_raises_still_records_its_time():
    clock = FakeClock()
    timings = _timings(clock)
    with pytest.raises(RuntimeError):
        with timings.stage(Stage.PUBLISH):
            clock.t += 1.0
            raise RuntimeError("boom")
    assert timings.stages[Stage.PUBLISH].seconds == 1.0


def test_peak_rss_is_the_largest_resident_set_seen_at_a_stage_boundary():
    timings = _timings(memory=ScriptedMemoryProbe(100, 300, 200, 150))
    with timings.stage(Stage.DBT_PARSE):
        pass
    with timings.stage(Stage.REGISTRY):
        pass
    assert timings.peak_rss_bytes == 300


def test_a_per_node_stage_reads_no_memory():
    memory = ScriptedMemoryProbe(100)
    timings = _timings(memory=memory)
    with timings.stage(Stage.CANDIDATE_UPLOAD, sample_memory=False, nodes=1):
        pass
    assert memory.calls == 0


def test_stages_ms_names_every_stage_in_pipeline_order_and_zero_for_one_that_did_not_run():
    clock = FakeClock()
    timings = _timings(clock)
    with timings.stage(Stage.RESOLVE):
        clock.t += 0.6604
    ms = timings.stages_ms()
    assert list(ms) == ["download", "dbt_parse", "python_parse", "registry", "resolve", "rewrite",
                        "candidate_upload", "code_bundle", "contract_generation", "contract_validation",
                        "canonical_hashing", "contract_bundle", "topology_artifact", "publish"]
    assert (ms["resolve"], ms["python_parse"]) == (660, 0)


def _node(runtime: Runtime, declared_columns: int, *, schedule: str = "daily",
          node_type: NodeType = NodeType.DBT_MODEL, service: str = "svc-a") -> ManifestNode:
    return ManifestNode(table_name="t", schema_name="s", service_name=service, owner="o",
                        schedule_name=schedule, criticality="SECONDARY", runtime=runtime,
                        node_type=node_type, declared_columns=declared_columns)


def test_log_fields_carry_the_release_its_counts_and_every_stage():
    timings = _timings(FakeClock())
    timings.add_nodes([_node(Runtime.DBT, 3), _node(Runtime.DBT, 0, schedule=""), _node(Runtime.PYTHON, 2)])
    timings.add_skips([SkippedNode("m", "svc-a", SkipReason.NO_OWNER)])
    timings.outcome = ReleaseOutcome.OK
    assert timings.log_fields() == {
        "release_id": "rel-1",
        "outcome": "ok",
        "nodes": {"dbt": 2, "python": 1, "external": 0},
        "columns": {"dbt": 3, "python": 2, "external": 0},
        "skipped": {"no_owner": 1, "local_stub": 0, "installed_package": 0},
        "dormant": {"svc-a": 1},
        "stages_ms": timings.stages_ms(),
        "peak_rss_bytes": 0,
    }


def test_add_skips_counts_each_skip_under_its_own_reason():
    timings = _timings()
    timings.add_skips([
        SkippedNode("model.evalpkg.a", "evalpkg", SkipReason.INSTALLED_PACKAGE),
        SkippedNode("seed.evalpkg.b", "evalpkg", SkipReason.INSTALLED_PACKAGE),
        SkippedNode("model.svc_a.c", "svc-a", SkipReason.NO_OWNER),
    ])
    assert timings.skipped == {SkipReason.NO_OWNER: 1, SkipReason.LOCAL_STUB: 0, SkipReason.INSTALLED_PACKAGE: 2}


def test_log_fields_name_the_failure_kind_of_a_failed_release():
    timings = _timings()
    timings.outcome = ReleaseOutcome.FAILED
    timings.failure_kind = ParseFailureKind.INVALID_SQL
    fields = timings.log_fields()
    assert (fields["outcome"], fields["failure_kind"]) == ("failed", "invalid_sql")


def test_a_release_with_no_nodes_counts_every_runtime_and_reason_at_zero():
    fields = _timings().log_fields()
    zero = {"dbt": 0, "python": 0, "external": 0}
    assert (fields["nodes"], fields["columns"]) == (zero, zero)
    assert fields["skipped"] == {"no_owner": 0, "local_stub": 0, "installed_package": 0}
    assert fields["dormant"] == {}


def test_add_nodes_counts_a_dormant_node_under_its_service_and_nothing_else_as_dormant():
    """A dormant node is a dbt or python node with no schedule that is not a test.
    A test node and an unknown-external node carry no schedule either and are not."""
    timings = _timings()
    timings.add_nodes([
        _node(Runtime.DBT, 0, schedule="", service="svc-b"),
        _node(Runtime.DBT, 0, schedule="", service="svc-a"),
        _node(Runtime.DBT, 0, schedule="", service="svc-b"),
        _node(Runtime.DBT, 0, schedule="daily", service="svc-a"),
        _node(Runtime.DBT, 0, schedule="", node_type=NodeType.DBT_TEST),
        _node(Runtime.EXTERNAL, 0, schedule="", node_type=NodeType.UNKNOWN_EXTERNAL, service=""),
    ])
    assert timings.dormant == {"svc-b": 2, "svc-a": 1}
    assert list(timings.log_fields()["dormant"]) == ["svc-a", "svc-b"]


def test_the_contract_stages_exist():
    assert (Stage.CONTRACT_GENERATION.value, Stage.CONTRACT_VALIDATION.value, Stage.CANONICAL_HASHING.value,
            Stage.CONTRACT_BUNDLE.value) == (
        "contract_generation", "contract_validation", "canonical_hashing", "contract_bundle")
    assert [stage.value for stage in Stage][-6:] == [
        "contract_generation", "contract_validation", "canonical_hashing", "contract_bundle",
        "topology_artifact", "publish"]


def test_contract_generation_comes_before_contract_validation():
    stages = list(Stage)
    assert stages.index(Stage.CONTRACT_GENERATION) < stages.index(Stage.CONTRACT_VALIDATION)


def test_a_release_that_checked_no_contract_has_no_failures_and_an_empty_cache():
    timings = _timings()
    assert timings.contract_failures == {}
    assert timings.validation_cache == {"hit": 0, "miss": 0}


def test_contract_failures_and_cache_add_up():
    timings = _timings()
    timings.add_contract_failure("validate", "column_type")
    timings.add_contract_failure("validate", "column_type")
    timings.add_contract_failure(FailureStage.HASH, REASON_VALUE)
    timings.add_validation_cache(True)
    timings.add_validation_cache(False)
    timings.add_validation_cache(False)
    assert timings.contract_failures == {("validate", "column_type"): 2, ("hash", "value"): 1}
    assert timings.validation_cache == {"hit": 1, "miss": 2}


def test_a_contract_failure_counts_only_under_a_stage_the_metrics_export():
    timings = _timings()
    with pytest.raises(ValueError, match="not a contract failure stage"):
        timings.add_contract_failure("validation", "value")
    assert timings.contract_failures == {}


def test_a_contract_failure_counts_only_under_a_reason_the_metrics_export():
    timings = _timings()
    with pytest.raises(ValueError, match="'unhashable' is not a contract failure reason"):
        timings.add_contract_failure("hash", "unhashable")
    assert timings.contract_failures == {}


def test_a_stage_given_as_the_enum_is_counted_under_its_plain_name():
    timings = _timings()
    timings.add_contract_failure(FailureStage.NORMALISE, REASON_VALUE)
    (key,) = timings.contract_failures
    assert key == ("normalise", "value") and type(key[0]) is str


@pytest.mark.parametrize("reason", [
    REASON_MANIFEST_SHAPE, REASON_SEED_HEADER, REASON_STRICT_YAML, REASON_IDENTITY, REASON_VALUE,
])
def test_every_reason_this_service_writes_is_one_the_metrics_export(reason):
    assert reason in CONTRACT_FAILURE_REASONS


def test_the_failure_stages_are_the_enums_values_in_order():
    assert CONTRACT_FAILURE_STAGES == ("generate", "validate", "normalise", "hash")


def test_bundle_timings_hold_the_stages_that_ran_so_far():
    clock = FakeClock()
    timings = _timings(clock)
    timings.add_nodes([_node(Runtime.PYTHON, 2), _node(Runtime.PYTHON, 3), _node(Runtime.DBT, 4)])
    with timings.stage(Stage.CONTRACT_VALIDATION):
        clock.t += 0.551
    with timings.stage(Stage.CANONICAL_HASHING):
        clock.t += 0.011
    timings.add_validation_cache(False)
    assert timings.bundle_timings() == {
        "stages_ms": {"contract_validation": 551, "canonical_hashing": 11},
        "nodes": {"dbt": 1, "python": 2, "external": 0},
        "columns": {"dbt": 4, "python": 5, "external": 0},
        "validation_cache": {"hit": 0, "miss": 1},
    }


def test_a_stage_that_ran_in_no_time_is_still_in_the_bundle_timings():
    timings = _timings()
    with timings.stage(Stage.REGISTRY):
        pass
    assert timings.bundle_timings()["stages_ms"] == {"registry": 0}


def test_a_stage_with_no_run_is_left_out_of_the_bundle_timings_but_not_of_the_log_line():
    timings = _timings()
    assert timings.bundle_timings()["stages_ms"] == {}
    assert set(timings.stages_ms()) == {stage.value for stage in Stage}


def test_bundle_timings_are_a_copy_the_record_does_not_share():
    timings = _timings()
    snapshot = timings.bundle_timings()
    timings.add_nodes([_node(Runtime.PYTHON, 1)])
    timings.add_validation_cache(True)
    assert snapshot["nodes"]["python"] == 0
    assert snapshot["validation_cache"] == {"hit": 0, "miss": 0}


def test_contracts_ms_sums_the_four_contract_stages():
    """A contract's generation, its check, its hashing and the bundle that holds
    it are the time a release spends on contracts. A stage that holds them (the
    dbt parse) and every other stage are not part of it."""
    clock = FakeClock()
    timings = _timings(clock)
    with timings.stage(Stage.DBT_PARSE):
        clock.t += 4.0
        with timings.stage(Stage.CONTRACT_GENERATION):
            clock.t += 0.25
    with timings.stage(Stage.CONTRACT_VALIDATION):
        clock.t += 0.5
    with timings.stage(Stage.CANONICAL_HASHING):
        clock.t += 0.125
    with timings.stage(Stage.CONTRACT_BUNDLE):
        clock.t += 0.375
    with timings.stage(Stage.RESOLVE):
        clock.t += 8.0
    assert timings.contracts_ms() == 250 + 500 + 125 + 375


def test_a_release_that_ran_no_contract_stage_spent_no_time_on_contracts():
    assert _timings().contracts_ms() == 0


def test_parse_ms_is_the_time_to_publish_without_the_contract_stages():
    clock = FakeClock()
    timings = _timings(clock)
    with timings.stage(Stage.DBT_PARSE):
        clock.t += 1.0
        with timings.stage(Stage.CONTRACT_GENERATION):
            clock.t += 0.5
    with timings.stage(Stage.CONTRACT_VALIDATION):
        clock.t += 0.25
    clock.t += 2.0
    timings.mark_published()
    assert timings.contracts_ms() == 750
    assert timings.parse_ms() == 3000


def test_parse_ms_counts_from_the_clock_reading_at_construction():
    clock = FakeClock()
    clock.t = 100.0
    timings = _timings(clock)
    clock.t = 102.5
    timings.mark_published()
    assert timings.parse_ms() == 2500


def test_parse_and_contract_milliseconds_add_up_to_the_time_to_publish():
    clock = FakeClock()
    timings = _timings(clock)
    with timings.stage(Stage.CONTRACT_GENERATION):
        clock.t += 0.0004
    with timings.stage(Stage.CONTRACT_VALIDATION):
        clock.t += 0.0004
    clock.t += 0.0014
    timings.mark_published()
    assert timings.contracts_ms() + timings.parse_ms() == round(clock.t * 1000)


def test_parse_ms_is_never_negative():
    clock = FakeClock()
    timings = _timings(clock)
    timings.stages[Stage.CONTRACT_GENERATION].seconds = 5.0
    clock.t += 1.0
    timings.mark_published()
    assert timings.contracts_ms() == 5000
    assert timings.parse_ms() == 0


def test_parse_ms_needs_the_publish_to_be_marked():
    with pytest.raises(RuntimeError, match="mark_published"):
        _timings().parse_ms()
