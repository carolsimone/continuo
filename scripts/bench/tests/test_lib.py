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


def exec_calls(env):
    import json
    return [json.loads(line) for line in (Path(env["FAKE_DIR"]) / "exec-calls.jsonl").read_text().splitlines()]


def test_announce_runs_inside_the_release_controller_deployment_on_k8s(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_TARGET="k8s", BENCH_KUBECONFIG="/dev/null")
    (Path(env["FAKE_DIR"]) / "announce-output").write_text('{"release_id":"r","promotion_seq":3}\n')
    out = run_lib(env, "printf '[]' | bench_announce --release-id r --topology -")
    assert out.returncode == 0, out.stderr
    assert out.stdout == '{"release_id":"r","promotion_seq":3}\n'
    call = exec_calls(env)[-1]
    assert "exec -i deploy/release-controller -- announce-topology --release-id r --topology -" in " ".join(call["argv"])
    assert call["stdin"] == "[]"


def test_announce_runs_inside_the_release_controller_container_on_compose(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    (Path(env["FAKE_DIR"]) / "announce-output").write_text('{"release_id":"r","promotion_seq":4}\n')
    out = run_lib(env, "bench_announce --reannounce-current < /dev/null")
    assert out.returncode == 0, out.stderr
    assert '"promotion_seq":4' in out.stdout
    assert exec_calls(env)[-1]["argv"][-2:] == ["/app/release-controller/bin/announce-topology", "--reannounce-current"]


def test_restore_treats_no_current_prod_as_nothing_to_restore(tmp_path):
    import json
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_TARGET="k8s", BENCH_KUBECONFIG="/dev/null", BENCH_PG_POD="pg-0", FAKE_ANNOUNCE_RC="3")
    (Path(env["FAKE_DIR"]) / "exec-output").write_text("rel-9\n")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "restore.json").write_text(json.dumps({"release_id": "rel-9", "topology": []}))
    (out_dir / "schedules-before.json").write_text(json.dumps({"schedules": []}))
    result = subprocess.run(["bash", str(BENCH / "restore.sh"), str(out_dir)], env=env, capture_output=True,
                            text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert "nothing to restore" in result.stderr
    assert "--reannounce-current" in " ".join(exec_calls(env)[-1]["argv"])


def test_export_reads_current_prod_through_announce_topology(tmp_path):
    import json
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_TARGET="k8s", BENCH_KUBECONFIG="/dev/null", BENCH_PG_POD="pg-0")
    fake = Path(env["FAKE_DIR"])
    (fake / "exec-output").write_text("rel-9\n")
    (fake / "announce-output").write_text(json.dumps([
        {"unique_id": "a.x", "schema_name": "a", "table_name": "x", "node_type": "dbt-model", "schedule": "daily",
         "upstream_unique_ids": [], "test_count": 1},
        {"unique_id": "a.not_null_x", "node_type": "dbt-test", "upstream_unique_ids": ["a.x"]}]))
    out_dir = tmp_path / "out"
    result = subprocess.run(["bash", str(BENCH / "export_topology.sh"), str(out_dir)], env=env, capture_output=True,
                            text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert any("--print-current" in " ".join(call["argv"]) for call in exec_calls(env))
    current = json.loads((out_dir / "current.json").read_text())
    assert current["release_id"] == "rel-9" and len(current["topology"]) == 2
    restore = json.loads((out_dir / "restore.json").read_text())
    assert restore["release_id"] == "rel-9"
    assert [n["unique_id"] for n in restore["topology"]] == ["a.x"]


def test_export_refuses_when_current_prod_names_no_release(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_TARGET="k8s", BENCH_KUBECONFIG="/dev/null", BENCH_PG_POD="pg-0")
    (Path(env["FAKE_DIR"]) / "exec-output").write_text("\n")
    result = subprocess.run(["bash", str(BENCH / "export_topology.sh"), str(tmp_path / "out")], env=env,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode != 0
    assert "names no release" in result.stderr


def test_sql_on_compose_runs_in_the_postgres_container_as_the_services_user(tmp_path):
    import json
    import fake_env
    env = fake_env.make(tmp_path, [])
    (Path(env["FAKE_DIR"]) / "tasks.json").write_text('[{"table": "a"}]\n')
    out = run_lib(env, 'bench_sql state "SELECT 1 FROM task_tracker"')
    assert out.returncode == 0, out.stderr
    assert out.stdout == '[{"table": "a"}]\n'
    psql = [json.loads(line) for line in (Path(env["FAKE_DIR"]) / "docker-calls.jsonl").read_text().splitlines()]
    assert psql[-1]["argv"][-7:] == ["-U", "fake_postgres_user", "-d", "fake_postgres_db", "-At", "-c",
                                     "SELECT 1 FROM task_tracker"]


def test_sql_on_k8s_runs_in_the_postgres_pod_with_the_services_credentials(tmp_path):
    import json
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_TARGET="k8s", BENCH_KUBECONFIG="/dev/null", BENCH_PG_POD="pg-0")
    (Path(env["FAKE_DIR"]) / "exec-output").write_text("[]\n")
    out = run_lib(env, 'bench_sql execution-controller "SELECT 1"')
    assert out.returncode == 0, out.stderr
    call = json.loads((Path(env["FAKE_DIR"]) / "exec-calls.jsonl").read_text().splitlines()[-1])
    assert "pg-0" in call["argv"]
    assert call["argv"][-3:] == ["fake-postgres_user", "fake-postgres_db", "SELECT 1"]
    assert call["stdin"].splitlines()[0] == "fake-postgres_password"


def test_sql_on_k8s_without_a_postgres_pod_fails_the_read_not_the_script(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_TARGET="k8s", BENCH_KUBECONFIG="/dev/null")
    env.pop("BENCH_PG_POD", None)
    out = run_lib(env, 'if bench_sql state "SELECT 1"; then echo read; else echo "fell back"; fi')
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "fell back"
    assert "BENCH_PG_POD is not set" in out.stderr


def quota_file(tmp_path):
    quota = tmp_path / "quota.yaml"
    quota.write_text("kind: ResourceQuota\n")
    return quota


def start_failure(env, tmp_path, quota):
    """bench_start_failure for schedule bench-sq, as a shell of its own with errexit on."""
    import fake_env
    out = tmp_path / "out"
    proc = subprocess.Popen(
        ["bash", "-c", f'set -euo pipefail; . "{BENCH}/lib.sh"; '
                       f'bench_start_failure start-quota "{fake_env.write_payload(tmp_path)}" bench-sq "{out}" "{quota}"'],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc, out


def interrupt_once_triggered(proc, fake):
    import signal
    import time
    deadline = time.time() + 60
    while not (fake / "current").exists() and time.time() < deadline:
        time.sleep(0.2)
    assert (fake / "current").exists(), "the scenario triggered its run"
    proc.send_signal(signal.SIGTERM)
    _, stderr = proc.communicate(timeout=60)
    return stderr


def after_the_cancel(fake):
    events = (fake / "events").read_text().splitlines()
    assert "cancel benchmark interrupted" in events, events
    return events[events.index("cancel benchmark interrupted") + 1:]


def test_an_interrupted_start_failure_cancels_its_run_then_deletes_its_jobs_and_its_quota(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_IDLE_S="0", BENCH_SETTLE_S="0")
    fake, quota = Path(env["FAKE_DIR"]), quota_file(tmp_path)
    proc, out = start_failure(env, tmp_path, quota)

    stderr = interrupt_once_triggered(proc, fake)

    assert proc.returncode == 143, stderr
    assert fake_env.wait_until_gone(str(out)) == [], "the scenario runner is stopped"
    assert (fake / "state").read_text() == "cancelled"
    after = after_the_cancel(fake)
    assert len(after) == 2, after
    assert "delete jobs -l schedule=bench-sq" in after[0], "the cleanup deletes the Jobs after the cancel"
    assert after[1].endswith(f"delete -f {quota} --ignore-not-found"), "and then the quota"


def test_an_interrupted_start_failure_removes_its_quota_when_its_jobs_cannot_be_deleted(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_IDLE_S="0", BENCH_SETTLE_S="0", FAKE_DELETE_JOBS_FAIL_AFTER="1")
    fake, quota = Path(env["FAKE_DIR"]), quota_file(tmp_path)
    proc, _ = start_failure(env, tmp_path, quota)

    stderr = interrupt_once_triggered(proc, fake)

    assert proc.returncode == 143, "a failed cleanup step keeps the signal's exit status"
    assert "could not delete bench-sq's Jobs" in stderr
    assert after_the_cancel(fake)[-1].endswith(f"delete -f {quota} --ignore-not-found")


def test_a_start_failure_whose_cleanup_fails_fails_once_its_reps_are_done(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_IDLE_S="0", BENCH_SETTLE_S="0", FAKE_RUN_RESULT="failed", FAKE_DELETE_JOBS_FAIL_AFTER="3")
    fake, quota = Path(env["FAKE_DIR"]), quota_file(tmp_path)
    proc, out = start_failure(env, tmp_path, quota)
    _, stderr = proc.communicate(timeout=120)

    assert (out / "rep3.json").exists(), "the runner's own three deletions succeeded and every rep ran"
    assert proc.returncode != 0, "the cleanup's failed deletion fails the scenario"
    assert "could not delete bench-sq's Jobs" in stderr
    events = (fake / "events").read_text().splitlines()
    assert events[-1].endswith(f"delete -f {quota} --ignore-not-found"), "the quota is removed anyway"


def test_stopping_a_scenario_reports_a_run_it_could_not_cancel(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    env["FAKE_CANCEL_FAIL"] = "1"
    fake = Path(env["FAKE_DIR"])
    (fake / "state").write_text("running")
    (fake / "current").write_text("run-7")
    out = run_lib(env, 'if bench_stop_scenario "" bench-s; then echo stopped; else echo failed; fi')
    assert out.stdout.strip() == "failed"
    assert "could not cancel bench-s's run run-7" in out.stderr


def test_stopping_a_scenario_reports_a_run_state_it_could_not_read(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    env["FAKE_STATUS_FAIL"] = "1"
    out = run_lib(env, 'if bench_stop_scenario "" bench-s; then echo stopped; else echo failed; fi')
    assert out.stdout.strip() == "failed"
    assert "could not read bench-s's run state" in out.stderr


def test_the_dev_exit_handler_restores_even_when_stopping_the_scenario_fails(tmp_path):
    source = (BENCH / "run_baseline_dev.sh").read_text()
    start = source.index("restore_on_exit() {")
    handler = source[start:source.index("\n}\n", start) + 2]
    here = tmp_path / "here"
    here.mkdir()
    restore = here / "restore.sh"
    restore.write_text('#!/usr/bin/env bash\necho restored > "$1/restored"\n')
    restore.chmod(0o755)
    script = (f'set -euo pipefail\nhere="{here}"; out="{tmp_path}"; injected=1; child=""; '
              f'current_schedule=bench-s; BENCH_KUBECONFIG=k\nbench_stop_scenario() {{ return 1; }}\n'
              f'{handler}\ntrap restore_on_exit EXIT\n')
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=60)
    assert (tmp_path / "restored").exists(), result.stderr
    assert result.returncode == 1, "a scenario that could not be stopped is reported by the exit status"


def runner_script(tmp_path, then=""):
    """A scenario runner as bash 3.2 behaves when it waits on its run: it ignores TERM and exits once the
    schedule's run is no longer running (giving up after 15 s), then runs THEN."""
    script = tmp_path / "runner.sh"
    script.write_text('trap "" TERM\n: > "$FAKE_DIR/runner-ready"\nfor _ in $(seq 150); do\n'
                      '  [ "$(cat "$FAKE_DIR/state")" = running ] || break\n  sleep 0.1\ndone\n' + then + "\n")
    return script


# Starts the runner and waits until it ignores TERM, so a stop cannot end it before it does.
START_RUNNER = 'bash "{runner}" & c=$!; while [ ! -e "$FAKE_DIR/runner-ready" ]; do sleep 0.05; done; '


def live_run(fake, run_id="run-7"):
    (fake / "state").write_text("running")
    (fake / "current").write_text(run_id)
    (fake / "n").write_text(run_id.split("-")[-1])


def test_stopping_a_scenario_cancels_its_run_before_waiting_for_its_runner(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    fake = Path(env["FAKE_DIR"])
    live_run(fake)
    runner = runner_script(tmp_path, then='echo "runner exits" >> "$FAKE_DIR/events"')
    out = run_lib(env, START_RUNNER.format(runner=runner) +
                  'if bench_stop_scenario "$c" bench-s; then echo stopped; else echo failed; fi; '
                  'if kill -0 "$c" 2>/dev/null; then echo alive; else echo gone; fi')
    assert out.stdout.split() == ["stopped", "gone"], out.stderr
    assert (fake / "state").read_text() == "cancelled"
    assert (fake / "events").read_text().splitlines() == ["cancel benchmark interrupted", "runner exits"]


def test_stopping_a_scenario_reports_a_run_state_it_could_not_read_after_its_runner_exits(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    fake = Path(env["FAKE_DIR"])
    env["FAKE_STATUS_FAIL_WHEN_EXISTS"] = str(fake / "runner-done")
    live_run(fake)
    runner = runner_script(tmp_path, then=': > "$FAKE_DIR/runner-done"')
    out = run_lib(env, START_RUNNER.format(runner=runner) +
                  'if bench_stop_scenario "$c" bench-s; then echo stopped; else echo failed; fi')
    assert out.stdout.strip() == "failed", out.stderr
    assert "could not read bench-s's run state" in out.stderr


def test_a_spawned_runner_runs_in_a_process_group_of_its_own(tmp_path):
    out = subprocess.run(["bash", "-c", f'. "{BENCH}/lib.sh"; bench_spawn sleep 30; c=$!; sleep 0.5; '
                                        'python3 -c "import os, sys; print(os.getpgid(int(sys.argv[1])))" "$c"; '
                                        'echo "$c"; kill "$c"'],
                         capture_output=True, text=True, timeout=60)
    group, pid = out.stdout.split()
    assert group == pid, out.stderr


def test_stopping_a_spawned_runner_ends_every_process_it_started(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    marker = f"bench-grandchild-{tmp_path.name}"
    out = run_lib(env, f'bench_spawn bash -c \'python3 -c "import time; time.sleep(60)" {marker} & '
                       ': > "$FAKE_DIR/grandchild"; wait\'; c=$!; '
                       'while [ ! -e "$FAKE_DIR/grandchild" ]; do sleep 0.05; done; '
                       'bench_stop_scenario "$c" ""; echo stopped')
    assert out.stdout.strip() == "stopped", out.stderr
    assert fake_env.wait_until_gone(marker) == [], "the runner's own child ends with it"


def test_stopping_a_scenario_that_cannot_cancel_its_run_does_not_wait_for_its_runner(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    env["FAKE_CANCEL_FAIL"] = "1"
    fake = Path(env["FAKE_DIR"])
    live_run(fake)
    out = run_lib(env, START_RUNNER.format(runner=runner_script(tmp_path)) + 'start=$SECONDS; '
                  'if bench_stop_scenario "$c" bench-s; then echo stopped; else echo failed; fi; '
                  'echo "took $((SECONDS - start))"; kill -KILL "$c" 2>/dev/null || true')
    assert out.stdout.split()[0] == "failed", out.stderr
    assert int(out.stdout.split()[-1]) < 5, "a stop that could not cancel the run does not wait for the runner"
    assert "could not cancel bench-s's run run-7" in out.stderr


def test_stopping_a_scenario_cancels_a_run_its_runner_triggered_meanwhile(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    fake = Path(env["FAKE_DIR"])
    live_run(fake)
    runner = runner_script(tmp_path, then="continuo schedule trigger bench-s >/dev/null")
    out = run_lib(env, START_RUNNER.format(runner=runner) +
                  'if bench_stop_scenario "$c" bench-s; then echo stopped; else echo failed; fi')
    assert out.stdout.strip() == "stopped", out.stderr
    assert (fake / "current").read_text() == "run-8", "the runner triggered a run while it was being stopped"
    assert (fake / "state").read_text() == "cancelled"
    assert (fake / "events").read_text().splitlines() == ["cancel benchmark interrupted"] * 2


def test_stopping_a_scenario_reports_a_run_still_live_after_the_cancel(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(FAKE_CANCEL_NOOP="1", BENCH_CANCEL_WAIT_S="1")
    live_run(Path(env["FAKE_DIR"]))
    out = run_lib(env, 'if bench_stop_scenario "" bench-s; then echo stopped; else echo failed; fi')
    assert out.stdout.strip() == "failed"
    assert "bench-s's run run-7 was still live 1 s after it was cancelled" in out.stderr


def test_an_interrupted_start_failure_finishes_its_cleanup_despite_a_second_signal(tmp_path):
    import signal
    import time
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_IDLE_S="0", BENCH_SETTLE_S="0", FAKE_DELETE_SLEEP="2")
    fake, quota = Path(env["FAKE_DIR"]), quota_file(tmp_path)
    proc, _ = start_failure(env, tmp_path, quota)
    deadline = time.time() + 60
    while not (fake / "current").exists() and time.time() < deadline:
        time.sleep(0.2)
    proc.send_signal(signal.SIGTERM)
    while time.time() < deadline and "cancel benchmark interrupted" not in (
            (fake / "events").read_text() if (fake / "events").exists() else ""):
        time.sleep(0.1)
    proc.send_signal(signal.SIGTERM)
    _, stderr = proc.communicate(timeout=60)

    assert proc.returncode == 143, stderr
    assert after_the_cancel(fake)[-1].endswith(f"delete -f {quota} --ignore-not-found"), "the quota is still removed"


def test_an_interrupted_start_failure_that_cannot_cancel_its_run_leaves_no_process_behind(tmp_path):
    import signal
    import time
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_IDLE_S="0", BENCH_SETTLE_S="0", FAKE_CANCEL_FAIL="1")
    fake, quota = Path(env["FAKE_DIR"]), quota_file(tmp_path)
    proc, out = start_failure(env, tmp_path, quota)
    deadline = time.time() + 60
    while not (fake / "current").exists() and time.time() < deadline:
        time.sleep(0.2)
    time.sleep(2)  # the runner is waiting on its run
    proc.send_signal(signal.SIGTERM)
    _, stderr = proc.communicate(timeout=60)

    assert proc.returncode == 143, stderr
    assert "could not cancel bench-sq's run" in stderr
    assert fake_env.wait_until_gone(str(out)) == [], "the runner and everything it started are stopped"


def test_the_dev_harness_runs_a_scenario_in_a_process_group_of_its_own(tmp_path):
    source = (BENCH / "run_baseline_dev.sh").read_text()
    start = source.index("scenario() {")
    scenario = source[start:source.index("\n}\n", start) + 2]
    here = tmp_path / "here"
    here.mkdir()
    inject = here / "inject.sh"
    inject.write_text('#!/usr/bin/env bash\nprintf "%s" "${current_schedule}" > "$OUT/during-inject"\n')
    runner = here / "run_scenario.sh"
    runner.write_text('#!/usr/bin/env bash\nprintf "%s" "${current_schedule}" > "$OUT/during-run"\n'
                      'python3 -c "import os, sys; print(os.getpgid(0) == int(sys.argv[1]))" "$$" > "$OUT/own-group"\n')
    for script in (inject, runner):
        script.chmod(0o755)
    script = (f'set -euo pipefail\n. "{BENCH}/lib.sh"\nexport OUT="{tmp_path}"\n'
              f'here="{here}"; p="{tmp_path}"; out="{tmp_path}"; injected=0; child=""\n'
              'current_schedule=""; export current_schedule\nbench_check_release() { :; }\n'
              f'{scenario}\nscenario n n.json bench-s run 1 "{tmp_path}/n"\n'
              'echo "after=${current_schedule} child=${child}"\n')
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "own-group").read_text().strip() == "True"
    assert (tmp_path / "during-inject").read_text() == "", "no run to cancel until the schedule is published"
    assert (tmp_path / "during-run").read_text() == "bench-s"
    assert result.stdout.strip() == "after= child=", "nothing to stop once the scenario has ended"


def test_the_dev_exit_handler_finishes_its_restore_despite_a_second_signal(tmp_path):
    import signal
    import time
    source = (BENCH / "run_baseline_dev.sh").read_text()
    start = source.index("restore_on_exit() {")
    handler = source[start:source.index("\n}\n", start) + 2]
    here = tmp_path / "here"
    here.mkdir()
    restore = here / "restore.sh"
    restore.write_text('#!/usr/bin/env bash\necho restored > "$1/restored"\n')
    restore.chmod(0o755)
    script = (f'set -euo pipefail\nhere="{here}"; out="{tmp_path}"; injected=1; child=""; '
              f'current_schedule=bench-s; BENCH_KUBECONFIG=k\n'
              'bench_stop_scenario() { : > "$out/stopping"; sleep 2 & wait $!; }\n'
              f'{handler}\ntrap restore_on_exit EXIT\ntrap "exit 130" INT\ntrap "exit 143" TERM HUP\n'
              ': > "$out/started"; sleep 30 & wait $!\n')
    proc = subprocess.Popen(["bash", "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.time() + 30
    while not (tmp_path / "started").exists() and time.time() < deadline:
        time.sleep(0.05)
    proc.send_signal(signal.SIGTERM)
    while not (tmp_path / "stopping").exists() and time.time() < deadline:
        time.sleep(0.05)
    proc.send_signal(signal.SIGTERM)
    _, stderr = proc.communicate(timeout=30)

    assert (tmp_path / "restored").exists(), stderr
    assert proc.returncode == 143


def test_a_start_failure_whose_last_stop_fails_fails(tmp_path):
    import fake_env
    env = fake_env.make(tmp_path, [])
    env.update(BENCH_IDLE_S="0", BENCH_SETTLE_S="0", FAKE_RUN_RESULT="failed",
               FAKE_STATUS_FAIL_WHEN_EXISTS=str(tmp_path / "out" / "rep3.json"))
    proc, out = start_failure(env, tmp_path, quota_file(tmp_path))
    _, stderr = proc.communicate(timeout=120)

    assert (out / "rep3.json").exists(), "every rep ran"
    assert proc.returncode != 0, "a cleanup that could not read the run's state fails the scenario"
    assert "could not read bench-sq's run state" in stderr
