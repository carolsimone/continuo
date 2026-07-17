"""Tests for the worker's task environment and its two ways of running a task.

The native path is exercised against a real dbt run on the test warehouse with
the project parser patched to explode, which is the only way to prove the run
came from the hydrated Manifest rather than a quiet re-parse.

The wrapper path is exercised against a real child process, because a wrapper is
a program the worker starts and does not otherwise understand: what it received
can only be established by asking it.
"""
import _thread
import copy
import json
import os
import shutil
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from dbt.contracts.graph.manifest import Manifest

from continuo_dbt_runtime.execution import (
    CACHE_ACCEPTED,
    CACHE_REJECTED,
    EventSink,
    ExecutionResult,
    Lease,
    NativeExecutor,
    TaskRef,
    WrapperExecutor,
    executor_for,
    task_environment,
)
from continuo_dbt_runtime.artifact_store import LoadedArtifact
from continuo_dbt_runtime.worker import task_values

from tests.conftest import SERVICE_NAME, TABLE_NAME, UNIQUE_ID

FIXTURE_WRAPPER = Path(__file__).parent / "fixtures" / "fake_dbt_wrapper.py"


def forbid_full_parse(monkeypatch) -> None:
    """Make a project parse impossible, so only a hydrated Manifest can serve."""
    def forbidden(*args, **kwargs):
        raise AssertionError("ManifestLoader.get_full_manifest must not run")

    monkeypatch.setattr(
        "dbt.parser.manifest.ManifestLoader.get_full_manifest", forbidden
    )


@pytest.fixture
def loaded_artifact(real_manifest_bytes, tmp_path) -> LoadedArtifact:
    canonical = tmp_path / "partial_parse.msgpack"
    canonical.write_bytes(real_manifest_bytes)
    return LoadedArtifact(
        manifest=Manifest.from_msgpack(real_manifest_bytes),
        canonical_path=canonical,
        descriptor={"sha256": "a" * 64},
    )


def native_lease(real_project, unique_id: str = UNIQUE_ID, table: str = TABLE_NAME) -> Lease:
    return Lease(
        lease_id="l-1",
        deployment_id="d-1",
        token="t-1",
        attempt=1,
        execution_path="native",
        argv=[
            "dbt", "run", "--select", table,
            "--project-dir", str(real_project),
            "--profiles-dir", str(real_project),
        ],
        task=TaskRef(
            task_id="task-1",
            schedule_id="sched-1",
            service_name=SERVICE_NAME,
            schema_name="worker_runtime_test",
            table_name=table,
            dbt_unique_id=unique_id,
        ),
    )


# --- task environment -----------------------------------------------------


def test_task_environment_restores_what_it_replaced(monkeypatch):
    monkeypatch.setenv("TASK_ID", "before")
    monkeypatch.delenv("SCHEDULE_ID", raising=False)

    with task_environment({"TASK_ID": "during", "SCHEDULE_ID": "s-1"}):
        assert os.environ["TASK_ID"] == "during"
        assert os.environ["SCHEDULE_ID"] == "s-1"

    assert os.environ["TASK_ID"] == "before"
    assert "SCHEDULE_ID" not in os.environ


def test_task_environment_restores_even_when_the_task_raises(monkeypatch):
    monkeypatch.setenv("TASK_ID", "before")

    with pytest.raises(RuntimeError):
        with task_environment({"TASK_ID": "during"}):
            raise RuntimeError("task blew up")

    assert os.environ["TASK_ID"] == "before"


def test_a_child_process_never_sees_the_pool_credential(monkeypatch):
    """The credential is popped at startup, so nothing dbt spawns can read it.

    Proved by inspecting a real child's environment rather than the parent's:
    the child is what a dbt wrapper would be, and inheritance is the leak.
    """
    monkeypatch.setenv("CONTINUO_POOL_CREDENTIAL", "super-secret-credential")

    from continuo_dbt_runtime.worker import take_credential

    credential = take_credential()

    assert credential == "super-secret-credential"
    assert "CONTINUO_POOL_CREDENTIAL" not in os.environ

    child = subprocess.run(
        [sys.executable, "-c", "import os, json; print(json.dumps(dict(os.environ)))"],
        capture_output=True, text=True, check=True,
    )
    child_env = json.loads(child.stdout)
    assert "CONTINUO_POOL_CREDENTIAL" not in child_env
    assert "super-secret-credential" not in json.dumps(child_env)


def test_the_task_environment_never_carries_the_credential(monkeypatch):
    from continuo_dbt_runtime.worker import task_values

    monkeypatch.setenv("CONTINUO_POOL_CREDENTIAL", "super-secret-credential")

    values = task_values(
        Lease(
            lease_id="l-1", deployment_id="d-1", token="t-1", attempt=1,
            execution_path="native", argv=["dbt", "run"],
            task=TaskRef(
                task_id="task-1", schedule_id="sched-1", service_name=SERVICE_NAME,
                schema_name="analytics", table_name=TABLE_NAME, dbt_unique_id=UNIQUE_ID,
                schedule_name="nightly", job_name="job-1",
            ),
        ),
        task_dir="/tmp/task",
    )

    assert "CONTINUO_POOL_CREDENTIAL" not in values
    assert "super-secret-credential" not in json.dumps(values)
    assert values["TASK_ID"] == "task-1"
    assert values["TABLE_NAME"] == TABLE_NAME


def test_the_task_environment_names_the_schedule_and_job_the_grant_carried():
    """A wrapper reads these names, so they must be the grant's, not stand-ins.

    A worker that had only the schedule's id would have to pass that id here,
    and a wrapper would read a UUID where a per-task Job gave it a name.
    """
    lease = Lease(
        lease_id="l-1", deployment_id="d-1", token="t-1", attempt=1,
        execution_path="wrapper_required", argv=["wise-dbt", "run"],
        task=TaskRef(
            task_id="task-1", schedule_id="sched-1", service_name=SERVICE_NAME,
            schema_name="analytics", table_name=TABLE_NAME, dbt_unique_id=UNIQUE_ID,
            schedule_name="nightly-finance", job_name="dbt-finance-orders-42",
        ),
    )

    values = task_values(lease, task_dir="/tmp/task")

    assert values["SCHEDULE_NAME"] == "nightly-finance"
    assert values["JOB_NAME"] == "dbt-finance-orders-42"
    assert values["SCHEDULE_ID"] == "sched-1"


# --- native execution -----------------------------------------------------


def test_native_run_executes_from_the_hydrated_manifest(
    loaded_artifact, real_project, tmp_path, monkeypatch
):
    """The payoff: a real dbt run with the project parser forbidden."""
    forbid_full_parse(monkeypatch)
    sink = EventSink(tmp_path / "dbt.log")

    result = NativeExecutor(loaded_artifact, sink).execute(
        native_lease(real_project), tmp_path
    )

    assert result.succeeded
    assert (tmp_path / "run_results.json").exists()


def test_native_run_writes_the_run_results_of_the_invocation(
    loaded_artifact, real_project, tmp_path, monkeypatch
):
    forbid_full_parse(monkeypatch)

    NativeExecutor(loaded_artifact, EventSink(tmp_path / "dbt.log")).execute(
        native_lease(real_project), tmp_path
    )

    written = json.loads((tmp_path / "run_results.json").read_text())
    assert [r["unique_id"] for r in written["results"]] == [UNIQUE_ID]


def test_event_sink_captures_the_run_log(
    loaded_artifact, real_project, tmp_path, monkeypatch
):
    forbid_full_parse(monkeypatch)
    log_path = tmp_path / "dbt.log"

    NativeExecutor(loaded_artifact, EventSink(log_path)).execute(
        native_lease(real_project), tmp_path
    )

    assert "table_a" in log_path.read_text()


def test_native_run_rejects_a_dbt_id_absent_from_the_manifest(
    loaded_artifact, real_project, tmp_path
):
    lease = native_lease(real_project, unique_id="model.service_1.ghost")

    result = NativeExecutor(loaded_artifact, EventSink(tmp_path / "l")).execute(
        lease, tmp_path
    )

    assert not result.succeeded
    assert result.error_class == "dbt_unique_id_not_found"
    assert not result.retryable


def test_native_run_rejects_a_selector_that_is_not_unique(
    real_manifest_bytes, real_project, tmp_path
):
    """Two nodes answering one name would make the resolved argv ambiguous."""
    manifest = Manifest.from_msgpack(real_manifest_bytes)
    twin = copy.deepcopy(manifest.nodes[UNIQUE_ID])
    twin.unique_id = "model.service_1.table_a_twin"
    manifest.nodes[twin.unique_id] = twin
    artifact = LoadedArtifact(manifest, tmp_path / "p", {"sha256": "a" * 64})

    result = NativeExecutor(artifact, EventSink(tmp_path / "l")).execute(
        native_lease(real_project), tmp_path
    )

    assert not result.succeeded
    assert result.error_class == "dbt_selector_not_unique"


def test_native_executor_refuses_non_dbt_argv(loaded_artifact, real_project, tmp_path):
    """The native path runs dbt in-process; anything else is a wiring fault."""
    lease = native_lease(real_project)
    wrapper = Lease(
        lease_id=lease.lease_id, deployment_id=lease.deployment_id, token=lease.token,
        attempt=1, execution_path="native", argv=["/bin/custom-wrapper", "run"],
        task=lease.task,
    )

    with pytest.raises(ValueError):
        NativeExecutor(loaded_artifact, EventSink(tmp_path / "l")).execute(
            wrapper, tmp_path
        )


# --- wrapper execution ----------------------------------------------------


@pytest.fixture
def wise_dbt(tmp_path_factory) -> Path:
    """The team's wrapper, on disk under the name their dialect names it."""
    bin_dir = tmp_path_factory.mktemp("team-bin")
    program = bin_dir / "wise-dbt"
    shutil.copyfile(FIXTURE_WRAPPER, program)
    program.chmod(program.stat().st_mode | stat.S_IXUSR)
    return program


def wrapper_lease(argv: list[str], path: str = "wrapper_required") -> Lease:
    return Lease(
        lease_id="l-1",
        deployment_id="d-1",
        token="t-1",
        attempt=1,
        execution_path=path,
        argv=argv,
        task=TaskRef(
            task_id="task-1",
            schedule_id="sched-1",
            schedule_name="nightly-finance",
            service_name=SERVICE_NAME,
            schema_name="analytics",
            table_name=TABLE_NAME,
            dbt_unique_id=UNIQUE_ID,
            job_name="dbt-finance-orders-42",
        ),
    )


def run_wrapper(artifact, lease, task_dir, *, required_cache=True):
    task_dir.mkdir(parents=True, exist_ok=True)
    executor = WrapperExecutor(
        artifact, EventSink(task_dir / "dbt.log"), required_cache=required_cache
    )
    return executor.execute(lease, task_dir)


def test_wrapper_receives_exact_argv_and_no_pool_secret(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch
):
    """The wrapper runs exactly as handed over, minus this worker's credential.

    The recorder is steered through an inherited variable on purpose: it proves
    the child does inherit the environment, so the credential's absence is the
    scrub working rather than an empty environment hiding the question.
    """
    task_dir = tmp_path / "task"
    record = tmp_path / "invocation.json"
    monkeypatch.setenv("FAKE_WRAPPER_RECORD", str(record))
    monkeypatch.setenv("FAKE_WRAPPER_CODES", "I040")
    monkeypatch.setenv("CONTINUO_POOL_CREDENTIAL", "super-secret-credential")

    argv = [str(wise_dbt), "run-model", "orders"]
    result = run_wrapper(loaded_artifact, wrapper_lease(argv), task_dir)

    invocation = json.loads(record.read_text())
    assert result.succeeded
    assert invocation["argv"] == argv
    assert invocation["cwd"] == "/project"
    assert "CONTINUO_POOL_CREDENTIAL" not in invocation["env"]
    assert "super-secret-credential" not in json.dumps(invocation["env"])
    # The scrub is targeted, not a blanket empty environment.
    assert invocation["env"]["FAKE_WRAPPER_RECORD"] == str(record)


def test_wrapper_reads_its_cache_from_a_task_local_copy(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch
):
    """Each task gets its own copy, so a wrapper that writes cannot poison the
    artifact the next task on this pod reads."""
    task_dir = tmp_path / "task"
    record = tmp_path / "invocation.json"
    monkeypatch.setenv("FAKE_WRAPPER_RECORD", str(record))
    monkeypatch.setenv("FAKE_WRAPPER_CODES", "I040")

    run_wrapper(loaded_artifact, wrapper_lease([str(wise_dbt), "run"]), task_dir)

    cache = json.loads(record.read_text())["env"]["DBT_PARTIAL_PARSE_FILE_PATH"]
    assert cache.startswith(str(task_dir))
    assert Path(cache) != loaded_artifact.canonical_path
    assert Path(cache).read_bytes() == loaded_artifact.canonical_path.read_bytes()


def test_wrapper_runs_under_json_debug_logs_so_cache_evidence_is_readable(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch
):
    task_dir = tmp_path / "task"
    record = tmp_path / "invocation.json"
    monkeypatch.setenv("FAKE_WRAPPER_RECORD", str(record))
    monkeypatch.setenv("FAKE_WRAPPER_CODES", "I040")

    run_wrapper(loaded_artifact, wrapper_lease([str(wise_dbt), "run"]), task_dir)

    env = json.loads(record.read_text())["env"]
    assert env["DBT_LOG_FORMAT"] == "json"
    assert env["DBT_LOG_LEVEL"] == "debug"
    assert env["DBT_TARGET_PATH"] == str(task_dir / "target")
    assert env["DBT_LOG_PATH"] == str(task_dir / "logs")


def test_wrapper_captures_both_pipes_into_the_task_log(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch
):
    task_dir = tmp_path / "task"
    monkeypatch.setenv("FAKE_WRAPPER_CODES", "I040")
    monkeypatch.setenv("FAKE_WRAPPER_STDERR", "a warning from the wrapper")

    run_wrapper(loaded_artifact, wrapper_lease([str(wise_dbt), "run"]), task_dir)

    log = (task_dir / "dbt.log").read_text()
    assert "I040" in log
    assert "a warning from the wrapper" in log


def test_wrapper_failure_is_reported_from_its_exit_status(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch
):
    task_dir = tmp_path / "task"
    monkeypatch.setenv("FAKE_WRAPPER_CODES", "I040")
    monkeypatch.setenv("FAKE_WRAPPER_EXIT", "1")

    result = run_wrapper(loaded_artifact, wrapper_lease([str(wise_dbt), "run"]), task_dir)

    assert not result.succeeded
    assert result.error_class == "wrapper_failed"
    assert not result.unsafe_runtime


@pytest.mark.parametrize("code", sorted(CACHE_ACCEPTED))
def test_an_accepted_code_proves_the_wrapper_used_the_promoted_cache(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch, code
):
    """Both accepted codes appear together on a real dbt happy path.

    I017 says a reused manifest needed no reparsing; I040 says a partial parse
    ran off one. Either alone is proof the promoted cache was read.
    """
    task_dir = tmp_path / "task"
    monkeypatch.setenv("FAKE_WRAPPER_CODES", code)

    result = run_wrapper(loaded_artifact, wrapper_lease([str(wise_dbt), "run"]), task_dir)

    assert result.succeeded
    assert result.cache_status == "accepted"


def test_a_real_dbt_happy_path_emits_more_than_one_accepted_code():
    """One dbt invocation reports both accepted codes, so requiring exactly one
    acceptance would reject every wrapper that did the right thing."""
    assert len(CACHE_ACCEPTED) > 1
    assert not CACHE_ACCEPTED & CACHE_REJECTED


@pytest.mark.parametrize("code", sorted(CACHE_REJECTED))
def test_a_rejected_code_stops_a_required_cache_wrapper_immediately(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch, code
):
    """A team that declared its wrapper reuses the cache, and whose wrapper then
    says it did not, is reparsing the project the pool exists to avoid."""
    task_dir = tmp_path / "task"
    monkeypatch.setenv("FAKE_WRAPPER_CODES", code)
    # Without the terminate the wrapper would outlive the test.
    monkeypatch.setenv("FAKE_WRAPPER_HANG", "1")

    result = run_wrapper(loaded_artifact, wrapper_lease([str(wise_dbt), "run"]), task_dir)

    assert not result.succeeded
    assert result.error_class == "runtime_manifest_unverified"
    assert code in result.error_message
    assert result.cache_status == "rejected"
    assert not result.retryable


def test_a_required_cache_wrapper_that_proves_nothing_is_unverified(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch
):
    """Silence is not evidence: the wrapper may have reparsed and said nothing."""
    task_dir = tmp_path / "task"
    monkeypatch.setenv("FAKE_WRAPPER_CODES", "")

    result = run_wrapper(loaded_artifact, wrapper_lease([str(wise_dbt), "run"]), task_dir)

    assert not result.succeeded
    assert result.error_class == "runtime_manifest_unverified"
    assert result.cache_status == "unknown"


def test_an_opaque_wrapper_needs_no_cache_evidence(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch
):
    """A team that claimed nothing is held to nothing."""
    task_dir = tmp_path / "task"
    monkeypatch.setenv("FAKE_WRAPPER_CODES", "")

    result = run_wrapper(
        loaded_artifact,
        wrapper_lease([str(wise_dbt), "run"], path="wrapper_opaque"),
        task_dir,
        required_cache=False,
    )

    assert result.succeeded
    assert result.cache_status == "unknown"


def test_an_opaque_wrapper_records_a_rejection_without_acting_on_it(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch
):
    """Nothing was promised about this wrapper's cache, so a rejection is an
    observation to report, not a reason to kill a run that is doing its job."""
    task_dir = tmp_path / "task"
    monkeypatch.setenv("FAKE_WRAPPER_CODES", "I024")

    result = run_wrapper(
        loaded_artifact,
        wrapper_lease([str(wise_dbt), "run"], path="wrapper_opaque"),
        task_dir,
        required_cache=False,
    )

    assert result.succeeded
    assert result.cache_status == "rejected"


def test_wrapper_output_that_is_not_dbt_json_is_not_evidence(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch
):
    """A wrapper prints its own prose, which says nothing about the cache."""
    task_dir = tmp_path / "task"
    monkeypatch.setenv("FAKE_WRAPPER_STDERR", "I016 is a string I happened to print")

    result = run_wrapper(loaded_artifact, wrapper_lease([str(wise_dbt), "run"]), task_dir)

    assert result.error_class == "runtime_manifest_unverified"
    assert result.cache_status == "unknown"


def test_a_cancelled_wrapper_leaves_no_process_behind(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch
):
    """Cancellation reaches the wrapper's whole process group.

    The worker interrupts its own main thread to stop a task, so a wrapper that
    was not terminated here would keep running against the warehouse after the
    task it belongs to was settled.
    """
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    monkeypatch.setenv("FAKE_WRAPPER_CODES", "I040")
    monkeypatch.setenv("FAKE_WRAPPER_HANG", "1")
    executor = WrapperExecutor(
        loaded_artifact, EventSink(task_dir / "dbt.log"), required_cache=True
    )

    def interrupt_once_running():
        deadline = time.monotonic() + 10
        while executor.child_pid is None and time.monotonic() < deadline:
            time.sleep(0.01)
        _thread.interrupt_main()

    threading.Thread(target=interrupt_once_running, daemon=True).start()

    with pytest.raises(KeyboardInterrupt):
        executor.execute(wrapper_lease([str(wise_dbt), "run"]), task_dir)

    # Reaped, not merely signalled: a worker outlives its tasks, so an
    # unreaped child would accumulate as a zombie for the life of the pod.
    assert executor.child_pid is not None
    with pytest.raises(ProcessLookupError):
        os.kill(executor.child_pid, 0)


def test_wrapper_rejects_a_dbt_id_absent_from_the_manifest(
    loaded_artifact, wise_dbt, tmp_path
):
    """The wrapper path runs the same selector guard as the native path: an argv
    that named more than one node would run something other than the task."""
    lease = wrapper_lease([str(wise_dbt), "run"])
    ghost = TaskRef(
        task_id=lease.task.task_id, schedule_id=lease.task.schedule_id,
        schedule_name=lease.task.schedule_name, service_name=SERVICE_NAME,
        schema_name="analytics", table_name=TABLE_NAME,
        dbt_unique_id="model.service_1.ghost", job_name=lease.task.job_name,
    )
    absent = Lease(
        lease_id="l-1", deployment_id="d-1", token="t-1", attempt=1,
        execution_path="wrapper_required", argv=lease.argv, task=ghost,
    )

    result = run_wrapper(loaded_artifact, absent, tmp_path / "task")

    assert not result.succeeded
    assert result.error_class == "dbt_unique_id_not_found"


def test_wrapper_rejects_a_selector_that_is_not_unique(
    real_manifest_bytes, wise_dbt, tmp_path
):
    manifest = Manifest.from_msgpack(real_manifest_bytes)
    twin = copy.deepcopy(manifest.nodes[UNIQUE_ID])
    twin.unique_id = "model.service_1.table_a_twin"
    manifest.nodes[twin.unique_id] = twin
    canonical = tmp_path / "partial_parse.msgpack"
    canonical.write_bytes(real_manifest_bytes)
    artifact = LoadedArtifact(manifest, canonical, {"sha256": "a" * 64})

    result = run_wrapper(artifact, wrapper_lease([str(wise_dbt), "run"]), tmp_path / "task")

    assert not result.succeeded
    assert result.error_class == "dbt_selector_not_unique"


def test_a_wrapper_is_never_run_through_a_shell(
    loaded_artifact, wise_dbt, tmp_path, monkeypatch
):
    """argv is passed as a list, so a table name is an argument and never a
    fragment of a command line a shell would re-interpret."""
    task_dir = tmp_path / "task"
    record = tmp_path / "invocation.json"
    monkeypatch.setenv("FAKE_WRAPPER_RECORD", str(record))
    monkeypatch.setenv("FAKE_WRAPPER_CODES", "I040")

    argv = [str(wise_dbt), "run-model", "orders; touch /tmp/pwned", "$HOME"]
    run_wrapper(loaded_artifact, wrapper_lease(argv), task_dir)

    assert json.loads(record.read_text())["argv"] == argv


# --- choosing how a task runs ---------------------------------------------


def test_executor_for_reads_the_path_the_lease_pinned(loaded_artifact, tmp_path):
    sink = EventSink(tmp_path / "dbt.log")

    native = executor_for(wrapper_lease(["dbt", "run"], path="native"),
                          loaded_artifact, sink)
    required = executor_for(wrapper_lease(["wise-dbt", "run"]), loaded_artifact, sink)
    opaque = executor_for(wrapper_lease(["wise-dbt", "run"], path="wrapper_opaque"),
                          loaded_artifact, sink)

    assert isinstance(native, NativeExecutor)
    assert isinstance(required, WrapperExecutor) and required.required_cache
    assert isinstance(opaque, WrapperExecutor) and not opaque.required_cache


def test_executor_for_refuses_a_path_it_does_not_know(loaded_artifact, tmp_path):
    """A path this worker cannot honour is a wiring fault, not a task failure."""
    lease = wrapper_lease(["wise-dbt", "run"], path="")

    with pytest.raises(ValueError):
        executor_for(lease, loaded_artifact, EventSink(tmp_path / "dbt.log"))


def test_execution_result_constructors():
    permanent = ExecutionResult.permanent("dbt_unique_id_not_found", "absent")
    unsafe = ExecutionResult.unsafe("dbt_runner_exception", "boom")

    assert not permanent.succeeded and not permanent.retryable
    assert not permanent.unsafe_runtime
    assert not unsafe.succeeded and unsafe.unsafe_runtime
