import os
import subprocess
from pathlib import Path

DBT = Path(__file__).resolve().parents[1] / "image" / "dbt"


def run(table, job_name):
    env = dict(os.environ, TABLE_NAME=table, JOB_NAME=job_name, BENCH_SLEEP_SECONDS="0")
    return subprocess.run(["sh", str(DBT), "run", "--select", table], env=env, capture_output=True, text=True).returncode


def test_an_ordinary_node_succeeds():
    assert run("n00_0000", "bench-bench-n00-0000-7f3e1a2b") == 0


def test_a_fail_node_fails_every_attempt():
    assert run("fail_root", "bench-bench-fail-root-7f3e1a2b") == 1
    assert run("fail_root", "bench-bench-fail-root-7f3e1a2b-r1") == 1


def test_a_fail_once_node_fails_its_first_attempt_and_succeeds_on_a_retry():
    assert run("once_00_0000", "bench-bench-once-00-0000-7f3e1a2b") == 1
    assert run("once_00_0000", "bench-bench-once-00-0000-7f3e1a2b-r1") == 0
    assert run("once_00_0000", "bench-bench-once-00-0000-7f3e1a2b-r1-r2") == 0
    assert run("once_00_0000", "bench-bench-once-00-0000-7f3e1a2b-r10") == 0
