import pytest

from service.release_timings import ReleaseTimings, Stage, StageRecord
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
