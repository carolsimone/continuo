import json
import subprocess
from pathlib import Path

import fake_env

BENCH = Path(__file__).resolve().parents[1]


def test_a_run_past_the_timeout_is_cancelled_and_the_next_rep_proceeds(tmp_path):
    env = fake_env.make(tmp_path, ["run-1", "run-2"])
    env.update(BENCH_RUN_TIMEOUT_S="1", BENCH_IDLE_S="0", BENCH_SETTLE_S="0")
    out = tmp_path / "out"

    result = subprocess.run(["bash", str(BENCH / "run_scenario.sh"), "s", str(fake_env.write_payload(tmp_path)),
                             "bench-s", "run", "2", str(out)], env=env, capture_output=True, text=True, timeout=120)

    assert result.returncode == 0, result.stderr
    assert [json.loads((out / f"rep{n}.json").read_text())["final_status"] for n in (1, 2)] == ["timeout", "timeout"]
    assert (Path(env["FAKE_DIR"]) / "calls").read_text().splitlines() == ["cancel benchmark timeout"] * 2
    assert fake_env.wait_until_gone(str(out)) == []


def test_a_terminated_scenario_stops_at_once_and_leaves_no_sampler(tmp_path):
    import signal
    import time
    env = fake_env.make(tmp_path, ["run-1"])
    env.update(BENCH_IDLE_S="120", BENCH_SETTLE_S="0")
    out = tmp_path / "out"
    proc = subprocess.Popen(["bash", str(BENCH / "run_scenario.sh"), "s", str(fake_env.write_payload(tmp_path)),
                             "bench-s", "run", "1", str(out)], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 30
    while not fake_env.live_processes_mentioning(f"{out}/samples") and time.time() < deadline:
        time.sleep(0.2)
    assert fake_env.live_processes_mentioning(f"{out}/samples")
    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=10)
    assert fake_env.wait_until_gone(str(out)) == []


def test_each_rep_exports_the_runs_rows_for_the_breakdown(tmp_path):
    run_id = "00000000-0000-4000-8000-000000000001"
    env = fake_env.make(tmp_path, [run_id])
    env.update(FAKE_UUID_RUNS="1", FAKE_RUN_RESULT="succeeded", BENCH_IDLE_S="0", BENCH_SETTLE_S="0")
    fake = Path(env["FAKE_DIR"])
    (fake / "deployments.json").write_text('[{"table": "n00_0000", "queued": "2026-10-03T09:00:00Z"}]\n')
    (fake / "tasks.json").write_text('[{"table": "n00_0000", "dispatched": "2026-10-03T09:00:00Z"}]\n')
    out = tmp_path / "out"

    result = subprocess.run(["bash", str(BENCH / "run_scenario.sh"), "s", str(fake_env.write_payload(tmp_path)),
                             "bench-s", "run", "1", str(out)], env=env, capture_output=True, text=True, timeout=120)

    assert result.returncode == 0, result.stderr
    assert json.loads((out / "deployments-rep1.json").read_text())[0]["table"] == "n00_0000"
    assert json.loads((out / "tasks-rep1.json").read_text())[0]["table"] == "n00_0000"
    psql = [json.loads(line)["argv"] for line in (fake / "docker-calls.jsonl").read_text().splitlines()]
    assert any(f"schedule_id = '{run_id}'" in " ".join(argv) and "FROM deployments" in " ".join(argv) for argv in psql)
    assert json.loads((out / "rep1.json").read_text())["final_status"] == "succeeded"


def test_a_rep_whose_rows_cannot_be_read_keeps_going_without_them(tmp_path):
    env = fake_env.make(tmp_path, ["run-1"])
    env.update(FAKE_RUN_RESULT="succeeded", BENCH_IDLE_S="0", BENCH_SETTLE_S="0")
    out = tmp_path / "out"

    result = subprocess.run(["bash", str(BENCH / "run_scenario.sh"), "s", str(fake_env.write_payload(tmp_path)),
                             "bench-s", "run", "1", str(out)], env=env, capture_output=True, text=True, timeout=120)

    assert result.returncode == 0, result.stderr
    assert "could not read the run's deployments" in result.stderr
    assert "could not read the run's deployments (ERROR run id 'run-1' is not a UUID)" in result.stderr, \
        "the warning carries the read's error"
    assert json.loads((out / "deployments-rep1.json").read_text()) == []
    assert json.loads((out / "rep1.json").read_text())["round_trip_p50_s"] is None
