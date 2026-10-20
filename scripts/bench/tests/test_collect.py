import collect
import metrics as m
from helpers import at, iso, job

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


def test_a_pause_in_sampling_during_the_run_is_recorded():
    lines = [f"{iso(s)}\tstate-aaaaaaaaaa-bbbbb\t1m\t1Mi\n" for s in (0, 5, 400, 405)]
    jobs = {"items": [job("r1", "n00_0000", 2, 6)]}
    result = collect.build_result(
        scenario="s", rep=1, operation="run", run_id="r1", payload=PAYLOAD, jobs_doc=jobs,
        trigger_ts=at(0), done_ts=at(405), samples=m.read_samples(lines), idle_window=None,
        streams_before={}, streams_after={})
    assert result["max_sample_gap_s"] == 395


def test_a_rep_reports_retry_turnaround_overlap_and_messages_per_attempt():
    jobs = {"items": [job("r1", "n00_0000", 2, 6, failed=True, name="j0"), job("r1", "n00_0000", 9, 12, name="j0-r1"),
                      job("r1", "n01_0000", 14, 16)]}
    result = collect.build_result(
        scenario="s", rep=1, operation="run", run_id="r1", payload=PAYLOAD, jobs_doc=jobs,
        trigger_ts=at(0), done_ts=at(20), samples=[], idle_window=None,
        streams_before={"x": 0}, streams_after={"x": 12}, extra={"final_status": "succeeded"})
    assert result["attempts"] == 3
    assert result["retries"] == 1
    assert result["retry_turnaround_p50_s"] == 3
    assert result["overlapping_attempts"] == 0
    assert result["messages_per_attempt"] == 4


def test_a_failed_run_whose_jobs_are_all_gone_still_reports():
    result = collect.build_result(
        scenario="s", rep=1, operation="run", run_id="r1", payload=PAYLOAD, jobs_doc={"items": []},
        trigger_ts=at(0), done_ts=at(150), samples=[], idle_window=None,
        streams_before={}, streams_after={}, extra={"final_status": "failed"})
    assert result["attempts"] == 0
    assert result["wall_s"] is None
    assert result["observed_done_s"] == 150
    assert result["overlapping_attempts"] == 0


def test_a_run_whose_jobs_never_finished_counts_its_retries_and_has_no_wall_time():
    jobs = {"items": [job("r1", "n00_0000", 1, name="j0"), job("r1", "n00_0000", 3, name="j0-r1"),
                      job("r1", "n00_0000", 5, name="j0-r2")]}
    result = collect.build_result(
        scenario="s", rep=1, operation="run", run_id="r1", payload=PAYLOAD, jobs_doc=jobs,
        trigger_ts=at(0), done_ts=at(20), samples=[], idle_window=None,
        streams_before={}, streams_after={}, extra={"final_status": "failed"})
    assert result["retries"] == 2
    assert result["retry_turnaround_p50_s"] is None
    assert result["overlapping_attempts"] == 2
    assert result["wall_s"] is None
    assert result["idle_s"] is None
    assert result["finalize_s"] is None


def test_a_succeeded_run_without_jobs_is_a_harness_error():
    import pytest
    with pytest.raises(ValueError, match="no Jobs found for run r1"):
        collect.build_result(
            scenario="s", rep=1, operation="run", run_id="r1", payload=PAYLOAD, jobs_doc={"items": []},
            trigger_ts=at(0), done_ts=at(5), samples=[], idle_window=None,
            streams_before={}, streams_after={}, extra={"final_status": "succeeded"})
