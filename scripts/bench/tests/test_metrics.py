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
    assert m.service_of("continuo-infra-redis-master-0") == "redis"
    assert m.service_of("continuo-infra-postgresql-0") == "postgres"
    assert m.service_of("continuo-postgres-1") == "postgres"
    assert m.service_of("state") == "state"
    assert m.service_of("NODE") == "NODE"


def test_usage_sums_replicas_at_one_instant_and_integrates_cpu():
    lines = [
        f"{iso(0)}\tstate-aaaaaaaaaa-bbbbb\t10m\t10Mi\n",
        f"{iso(0)}\tstate-cccccccccc-ddddd\t10m\t5Mi\n",
        f"{iso(5)}\tstate-aaaaaaaaaa-bbbbb\t40m\t12Mi\n",
        "garbage\n",
    ]
    state = m.usage(m.read_samples(lines), at(0), at(5))["state"]
    assert state["cpu_max_m"] == 40
    assert state["mem_max_mib"] == 15
    assert abs(state["cpu_seconds"] - 0.2) < 1e-9


def test_messages_delta_counts_new_streams_from_zero():
    assert m.messages_delta({"a": 5}, {"a": 9, "b": 2}) == {"a": 4, "b": 2}
