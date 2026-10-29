from datetime import timedelta

import metrics as m
from helpers import T0, at, iso, job


def test_parse_ts_accepts_z_fraction_and_offset():
    assert m.parse_ts("2026-10-03T09:00:00Z") == T0
    assert m.parse_ts("2026-10-03T09:00:00.123456Z") == T0 + timedelta(microseconds=123456)
    assert m.parse_ts("2026-10-03T11:00:00+02:00") == T0


def test_percentile_interpolates():
    assert m.percentile([1, 2, 3, 4], 0.5) == 2.5


def test_busy_and_gaps_merge_overlaps():
    busy, gaps = m.busy_and_gaps([(at(0), at(10)), (at(5), at(12)), (at(20), at(25))])
    assert busy == 17
    assert gaps == [8]


def test_attempts_include_failed_jobs_without_completion_time():
    doc = {"items": [job("r1", "fail_root", 0, 7, failed=True), job("r1", "n01_0000", 1, 3), job("r2", "x", 0, 1)]}
    attempts = m.attempts_for_run(doc, "r1")
    assert {a.table for a in attempts} == {"fail_root", "n01_0000"}
    assert [a.finished for a in attempts if a.failed] == [at(7)]


def test_node_windows_span_retries():
    doc = {"items": [job("r1", "a", 0, 5, failed=True, name="j"), job("r1", "a", 6, 9, name="j-r1")]}
    starts, finishes = m.node_windows(m.attempts_for_run(doc, "r1"))
    assert starts["a"] == at(0)
    assert finishes["a"] == at(9)


def test_handoff_uses_the_last_upstream_finish():
    topology = [
        {"unique_id": "b.a", "table_name": "a", "upstream_unique_ids": []},
        {"unique_id": "b.b", "table_name": "b", "upstream_unique_ids": []},
        {"unique_id": "b.c", "table_name": "c", "upstream_unique_ids": ["b.a", "b.b"]},
    ]
    starts = {"a": at(0), "b": at(0), "c": at(14)}
    finishes = {"a": at(5), "b": at(10)}
    assert m.handoff_latencies(topology, starts, finishes) == [4.0]


def test_parse_cpu_units():
    assert m.parse_cpu_millicores("12m") == 12
    assert m.parse_cpu_millicores("1.50%") == 15
    assert m.parse_cpu_millicores("2") == 2000


def test_parse_mem_units():
    assert m.parse_mem_mib("13Mi") == 13
    assert m.parse_mem_mib("1.5GiB") == 1536
    assert m.parse_mem_mib("512KiB") == 0.5


def test_service_of_normalises_pod_and_container_names():
    assert m.service_of("execution-controller-677d8fc4c7-r829s") == "execution-controller"
    assert m.service_of("infra-redis-master-0") == "redis"
    assert m.service_of("data-postgresql-0") == "postgres"
    assert m.service_of("continuo-postgres-1") == "postgres"
    assert m.service_of("run-controller") == "run-controller"
    assert m.service_of("NODE") == "NODE"


def test_usage_sums_replicas_at_one_instant_and_integrates_cpu():
    lines = [
        f"{iso(0)}\trun-controller-aaaaaaaaaa-bbbbb\t10m\t10Mi\n",
        f"{iso(0)}\trun-controller-cccccccccc-ddddd\t10m\t5Mi\n",
        f"{iso(5)}\trun-controller-aaaaaaaaaa-bbbbb\t40m\t12Mi\n",
        "garbage\n",
    ]
    usage = m.usage(m.read_samples(lines), at(0), at(5))["run-controller"]
    assert usage["cpu_max_m"] == 40
    assert usage["mem_max_mib"] == 15
    assert abs(usage["cpu_seconds"] - 0.2) < 1e-9


def test_messages_delta_counts_new_streams_from_zero():
    assert m.messages_delta({"a": 5}, {"a": 9, "b": 2}) == {"a": 4, "b": 2}


def test_sampling_that_starts_late_or_stops_early_counts_as_a_gap():
    def samples(seconds):
        return m.read_samples([f"{iso(s)}\trun-controller\t1m\t1Mi\n" for s in seconds])
    assert m.max_sample_gap(samples(range(0, 61, 5)), at(0), at(600)) == 540
    assert m.max_sample_gap(samples(range(100, 601, 5)), at(0), at(600)) == 100
    assert m.max_sample_gap([], at(0), at(600)) is None


def test_retry_turnaround_runs_from_a_failed_job_finishing_to_the_retry_job_being_created():
    doc = {"items": [job("r1", "a", 0, 5, failed=True, name="j-a"), job("r1", "a", 8, 12, name="j-a-r1"),
                     job("r1", "b", 0, 4, name="j-b")]}
    assert m.retry_turnarounds(m.attempts_for_run(doc, "r1")) == [3]


def test_retry_count_is_every_attempt_after_each_tasks_first():
    doc = {"items": [job("r1", "a", 0, name="j-a"), job("r1", "a", 2, name="j-a-r1"), job("r1", "a", 4, 6, name="j-a-r2"),
                     job("r1", "b", 0, 4, name="j-b")]}
    assert m.retry_count(m.attempts_for_run(doc, "r1")) == 2


def test_a_job_that_never_finished_gives_no_turnaround():
    doc = {"items": [job("r1", "a", 0, None, name="j-a"), job("r1", "a", 2, 3, name="j-a-r1")]}
    assert m.retry_turnarounds(m.attempts_for_run(doc, "r1")) == []


def test_an_attempt_created_while_an_earlier_attempt_had_not_finished_overlaps():
    # a: attempt 0 never finished, so attempts 1 and 2 each overlap it.
    # b: attempt 0 finished before attempt 1 was created.
    # c: attempt 0 finished after attempt 1 was created.
    doc = {"items": [
        job("r1", "a", 0, None, name="a0"), job("r1", "a", 10, None, name="a1"), job("r1", "a", 20, None, name="a2"),
        job("r1", "b", 0, 5, failed=True, name="b0"), job("r1", "b", 6, 9, name="b1"),
        job("r1", "c", 0, 9, failed=True, name="c0"), job("r1", "c", 7, 12, name="c1"),
    ]}
    assert m.overlapping_attempts(m.attempts_for_run(doc, "r1")) == 3


def test_breakdown_splits_each_handoff_into_its_steps():
    topology = [
        {"unique_id": "b.a", "table_name": "a", "upstream_unique_ids": []},
        {"unique_id": "b.b", "table_name": "b", "upstream_unique_ids": []},
        {"unique_id": "b.c", "table_name": "c", "upstream_unique_ids": ["b.a", "b.b"]},
    ]
    finishes = {"a": at(6), "b": at(4), "c": at(12)}
    deployments = m.read_deployments([
        {"table": "a", "queued": at(0).isoformat(), "created": at(1).isoformat(), "recorded": at(6.5).isoformat()},
        {"table": "b", "queued": at(0).isoformat(), "created": at(1).isoformat(), "recorded": at(4.5).isoformat()},
        {"table": "c", "queued": at(8).isoformat(), "created": at(9).isoformat(), "recorded": None},
    ])
    dispatched = m.read_dispatches([{"table": "c", "dispatched": at(7.5).isoformat()}, {"table": "a", "dispatched": None}])
    steps = m.handoff_breakdown(topology, finishes, deployments, dispatched)
    assert steps == {"observe": [0.5], "round_trip": [1.5], "to_dispatch": [1.0], "to_queue": [0.5], "admit": [1.0]}


def test_breakdown_takes_the_upstreams_last_attempt_and_the_nodes_first():
    topology = [{"unique_id": "b.a", "table_name": "a", "upstream_unique_ids": []},
                {"unique_id": "b.c", "table_name": "c", "upstream_unique_ids": ["b.a"]}]
    deployments = m.read_deployments([
        {"table": "a", "queued": at(0).isoformat(), "created": at(1).isoformat(), "recorded": at(3).isoformat()},
        {"table": "a", "queued": at(4).isoformat(), "created": at(5).isoformat(), "recorded": at(9).isoformat()},
        {"table": "c", "queued": at(10).isoformat(), "created": at(11).isoformat(), "recorded": None},
        {"table": "c", "queued": at(20).isoformat(), "created": None, "recorded": None},
    ])
    steps = m.handoff_breakdown(topology, {"a": at(8)}, deployments, {})
    assert steps["observe"] == [1.0]
    assert steps["round_trip"] == [1.0]
    assert steps["admit"] == [1.0]
    assert steps["to_dispatch"] == [] and steps["to_queue"] == []
