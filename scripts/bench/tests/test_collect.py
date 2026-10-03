import collect
from helpers import at, job

PAYLOAD = {"topology": [
    {"unique_id": "bench.n00_0000", "table_name": "n00_0000", "upstream_unique_ids": []},
    {"unique_id": "bench.n01_0000", "table_name": "n01_0000", "upstream_unique_ids": ["bench.n00_0000"]},
]}


def test_build_result_for_a_two_level_run():
    jobs = {"items": [job("r1", "n00_0000", 2, 6), job("r1", "n01_0000", 9, 12)]}
    result = collect.build_result(
        scenario="s", rep=1, operation="run", run_id="r1", payload=PAYLOAD, jobs_doc=jobs,
        trigger_ts=at(0), done_ts=at(15), samples=[], idle_window=None,
        streams_before={"x": 10}, streams_after={"x": 18}, extra={"final_status": "succeeded"})
    assert result["wall_s"] == 12
    assert result["busy_s"] == 7
    assert result["idle_s"] == 5
    assert result["first_start_s"] == 2
    assert result["finalize_s"] == 3
    assert result["handoff_p50_s"] == 3
    assert result["messages_per_task"] == 4
    assert result["final_status"] == "succeeded"


def test_test_operation_reports_no_handoff():
    jobs = {"items": [job("r1", "n00_0000", 2, 6), job("r1", "n01_0000", 2, 6)]}
    result = collect.build_result(
        scenario="s", rep=1, operation="test", run_id="r1", payload=PAYLOAD, jobs_doc=jobs,
        trigger_ts=at(0), done_ts=at(8), samples=[], idle_window=None,
        streams_before={}, streams_after={})
    assert result["handoff_p50_s"] is None


def test_cancel_overlap_counts_old_jobs_still_running():
    jobs = {"items": [job("r1", "a", 0, 100), job("r1", "b", 5), job("r2", "a", 40, 45)]}
    out = collect.cancel_overlap(jobs, "r1", at(30), "r2", trigger_ts=at(0))
    assert out["cancel_s"] == 30
    assert out["jobs_active_at_cancel"] == 2
    assert out["jobs_never_finished"] == 1
    assert out["overlap_s"] == 60
