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


def run_lib(env, script):
    return subprocess.run(["bash", "-c", f'. "{BENCH}/lib.sh"; {script}'], env=env, capture_output=True, text=True,
                          timeout=60)


def test_stopping_a_scenario_ends_its_runner_and_cancels_the_live_run(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    fake = Path(env["FAKE_DIR"])
    (fake / "state").write_text("running")
    (fake / "current").write_text("run-7")
    out = run_lib(env, 'sleep 60 & c=$!; bench_stop_scenario "$c" bench-s; '
                       'if kill -0 "$c" 2>/dev/null; then echo alive; else echo stopped; fi')
    assert out.stdout.strip() == "stopped", out.stderr
    assert (fake / "calls").read_text().splitlines() == ["cancel benchmark interrupted"]
    assert (fake / "state").read_text() == "cancelled"


def test_stopping_a_scenario_leaves_a_finished_run_alone(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    fake = Path(env["FAKE_DIR"])
    (fake / "state").write_text("succeeded")
    (fake / "current").write_text("run-7")
    assert run_lib(env, 'bench_stop_scenario "" bench-s').returncode == 0
    assert not (fake / "calls").exists()


def test_release_check_compares_current_prod_with_the_export(tmp_path):
    import json
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_TARGET="k8s", BENCH_KUBECONFIG="/dev/null", BENCH_PG_POD="pg-0")
    (Path(env["FAKE_DIR"]) / "exec-output").write_text("rel-9\n")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "restore.json").write_text(json.dumps({"release_id": "rel-9", "topology": []}))
    assert run_lib(env, f'bench_check_release "{out_dir}"').returncode == 0
    (out_dir / "restore.json").write_text(json.dumps({"release_id": "rel-8", "topology": []}))
    moved = run_lib(env, f'bench_check_release "{out_dir}"')
    assert moved.returncode == 1
    assert "rel-9" in moved.stderr and "rel-8" in moved.stderr


def test_release_check_fails_closed_when_current_prod_cannot_be_read(tmp_path):
    import json
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_TARGET="k8s", BENCH_KUBECONFIG="/dev/null", BENCH_PG_POD="pg-0", FAKE_EXEC_FAIL="1")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "restore.json").write_text(json.dumps({"release_id": "rel-9", "topology": []}))
    unreadable = run_lib(env, f'bench_check_release "{out_dir}"')
    assert unreadable.returncode == 2
    assert "cannot read current_prod" in unreadable.stderr


def test_restore_never_re_exports_when_current_prod_cannot_be_read(tmp_path):
    import json
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_TARGET="k8s", BENCH_KUBECONFIG="/dev/null", BENCH_PG_POD="pg-0", BENCH_REDIS_POD="redis-0",
               FAKE_EXEC_FAIL="1")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "restore.json").write_text(json.dumps({"release_id": "rel-9", "topology": []}))
    (out_dir / "schedules-before.json").write_text(json.dumps({"schedules": []}))
    result = subprocess.run(["bash", str(BENCH / "restore.sh"), str(out_dir)], env=env, capture_output=True,
                            text=True, timeout=60)
    assert result.returncode != 0
    assert "re-exporting" not in result.stderr
    assert "cannot read current_prod" in result.stderr


def test_scenario_subset_selects_by_name():
    def wanted(name, scenarios=None):
        env = dict(os.environ)
        env.pop("BENCH_SCENARIOS", None)
        if scenarios is not None:
            env["BENCH_SCENARIOS"] = scenarios
        return subprocess.run(["bash", "-c", f'. "{BENCH}/lib.sh"; bench_wanted {name}'], env=env).returncode == 0
    assert wanted("dag-500")
    assert wanted("cascade-2000", "cascade-2000,cancel-500")
    assert wanted("cancel-500", "cascade-2000 cancel-500")
    assert not wanted("dag-500", "cascade-2000,cancel-500")
    assert not wanted("dag-2000", "dag-200")
