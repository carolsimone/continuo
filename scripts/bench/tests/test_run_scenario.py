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
