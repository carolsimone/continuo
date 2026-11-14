import pytest

from domain.contract_vocabulary import NodeType, ParseFailureKind
from domain.model import ManifestNode, Runtime, SkippedNode, SkipReason
from service.release_timings import ReleaseOutcome, ReleaseTimings, Stage, StageRecord
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
                        "candidate_upload", "code_bundle", "topology_artifact", "publish"]
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
