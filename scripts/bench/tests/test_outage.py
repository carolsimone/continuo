import json
import subprocess
from pathlib import Path

import fake_env

BENCH = Path(__file__).resolve().parents[1]
DROP_POISON = "Poison message exceeded max deliveries — ACK-dropping to break the redelivery loop"
DROP_PERMANENT = "Permanent handler error — ACKing to drop from PEL"


def run_outage(tmp_path, env):
    out = tmp_path / "out"
    result = subprocess.run(["bash", str(BENCH / "outage.sh"), "postgres", str(fake_env.write_payload(tmp_path)),
                             "bench-s", str(out), "0", "0"], env=env, capture_output=True, text=True, timeout=120)
    return result, out


def test_outage_counts_only_the_drops_logged_after_its_trigger(tmp_path):
    env = fake_env.make(tmp_path, ["run-1"])
    env["FAKE_RUN_RESULT"] = "succeeded"
    fake = Path(env["FAKE_DIR"])
    (fake / "logs" / "state.log").write_text(f"{DROP_POISON}\nstarted\n")
    (fake / "logs" / "orchestrator.log").write_text("")
    (fake / "logs" / "execution-controller.log").write_text("")
    (fake / "on-trigger" / "state.log").write_text(f"{DROP_POISON}\n{DROP_PERMANENT}\nother\n")
    (fake / "on-trigger" / "orchestrator.log").write_text(f"{DROP_PERMANENT}\n")

    result, out = run_outage(tmp_path, env)

    assert result.returncode == 0, result.stderr
    rep = json.loads((out / "rep1.json").read_text())
    assert rep["final_status"] == "succeeded"
    assert rep["messages_dropped"] == "3"


def test_outage_refuses_to_start_without_service_logs(tmp_path):
    env = fake_env.make(tmp_path, ["run-1"])
    env["FAKE_RUN_RESULT"] = "succeeded"

    result, out = run_outage(tmp_path, env)

    assert result.returncode == 2
    assert "start_local_services.sh" in result.stderr
    assert not (Path(env["FAKE_DIR"]) / "n").exists()


def test_outage_cancels_a_run_that_outlives_its_timeout(tmp_path):
    env = fake_env.make(tmp_path, ["run-1"])
    env["BENCH_RUN_TIMEOUT_S"] = "1"
    fake = Path(env["FAKE_DIR"])
    for service in ("state", "orchestrator", "execution-controller"):
        (fake / "logs" / f"{service}.log").write_text("")

    result, out = run_outage(tmp_path, env)

    assert result.returncode == 0, result.stderr
    assert json.loads((out / "rep1.json").read_text())["final_status"] == "timeout"
    assert (fake / "calls").read_text().splitlines() == ["cancel benchmark outage cleanup"]
