import os
import subprocess
from pathlib import Path

BENCH = Path(__file__).resolve().parents[1]


def in_quiet_window(now, windows=None):
    env = dict(os.environ)
    env.pop("BENCH_QUIET_WINDOWS_UTC", None)
    if windows is not None:
        env["BENCH_QUIET_WINDOWS_UTC"] = windows
    return subprocess.run(["bash", "-c", f'. "{BENCH}/lib.sh"; bench_in_quiet_window {now}'], env=env).returncode == 0


def test_default_window_surrounds_a_23_00_utc_daily_run():
    assert in_quiet_window("2300")
    assert not in_quiet_window("2214")
    assert not in_quiet_window("2345")


def test_windows_are_configurable_and_may_cross_midnight():
    windows = "0415-0545,2330-0030"
    assert in_quiet_window("0415", windows)
    assert in_quiet_window("0010", windows)
    assert in_quiet_window("2345", windows)
    assert not in_quiet_window("0600", windows)
    assert not in_quiet_window("2300", windows)


def test_the_redis_command_never_carries_the_password(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    env["REDIS_PASSWORD"] = "pw-secret-1"
    out = subprocess.run(["bash", "-c", f'. "{BENCH}/lib.sh"; bench_setup_redis; printf "%s" "$BENCH_REDIS_CMD"'],
                         env=env, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "pw-secret-1" not in out.stdout
    assert "read -r REDISCLI_AUTH" in out.stdout


def test_psql_receives_the_password_on_stdin(tmp_path):
    import json
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_TARGET="k8s", BENCH_KUBECONFIG="/dev/null")
    (Path(env["FAKE_DIR"]) / "exec-output").write_text("rel-1\n")
    out = subprocess.run(["bash", "-c", f'. "{BENCH}/lib.sh"; bench_psql pg-0 user db pw-secret-1 "SELECT 1"'],
                         env=env, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert out.stdout == "rel-1\n"
    call = json.loads((Path(env["FAKE_DIR"]) / "exec-calls.jsonl").read_text().splitlines()[0])
    assert "pw-secret-1" not in " ".join(call["argv"])
    assert call["stdin"].splitlines()[0] == "pw-secret-1"
