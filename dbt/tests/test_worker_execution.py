"""Tests for the worker's task environment and its native dbt execution.

The native path is exercised against a real dbt run on the test warehouse with
the project parser patched to explode, which is the only way to prove the run
came from the hydrated Manifest rather than a quiet re-parse.
"""
import copy
import json
import os
import subprocess
import sys

import pytest
from dbt.contracts.graph.manifest import Manifest

from continuo_dbt_runtime.execution import (
    EventSink,
    ExecutionResult,
    Lease,
    NativeExecutor,
    TaskRef,
    task_environment,
)
from continuo_dbt_runtime.artifact_store import LoadedArtifact

from tests.conftest import SERVICE_NAME, TABLE_NAME, UNIQUE_ID


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
            ),
        ),
        job_name="job-1",
        schedule_name="nightly",
        task_dir="/tmp/task",
    )

    assert "CONTINUO_POOL_CREDENTIAL" not in values
    assert "super-secret-credential" not in json.dumps(values)
    assert values["TASK_ID"] == "task-1"
    assert values["TABLE_NAME"] == TABLE_NAME


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


def test_execution_result_constructors():
    permanent = ExecutionResult.permanent("dbt_unique_id_not_found", "absent")
    unsafe = ExecutionResult.unsafe("dbt_runner_exception", "boom")

    assert not permanent.succeeded and not permanent.retryable
    assert not permanent.unsafe_runtime
    assert not unsafe.succeeded and unsafe.unsafe_runtime
