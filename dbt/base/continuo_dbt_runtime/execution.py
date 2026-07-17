"""Running one dbt node against a Manifest the process already holds.

A task names one dbt unique id and one resolved argv. Neither is derived here:
the executor resolved them, and this module runs exactly what it was handed or
refuses. Nothing reconstructs SQL or a materialization from a node.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from dbt.adapters.factory import cleanup_connections, reset_adapters
from dbt.cli.main import dbtRunner
from dbt.contracts.results import RunExecutionResult

from continuo_dbt_runtime.artifact_store import EXECUTABLE_RESOURCE_TYPES, LoadedArtifact


@dataclass(frozen=True)
class TaskRef:
    """The dbt node a lease covers."""
    task_id: str
    schedule_id: str
    service_name: str
    schema_name: str
    table_name: str
    dbt_unique_id: str

    @classmethod
    def from_response(cls, payload: dict) -> "TaskRef":
        return cls(
            task_id=payload.get("task_id", ""),
            schedule_id=payload.get("schedule_id", ""),
            service_name=payload.get("service_name", ""),
            schema_name=payload.get("schema_name", ""),
            table_name=payload.get("table_name", ""),
            dbt_unique_id=payload.get("dbt_unique_id", ""),
        )


@dataclass(frozen=True)
class Lease:
    """A granted hold on one task."""
    lease_id: str
    deployment_id: str
    token: str
    attempt: int
    execution_path: str
    argv: list[str]
    task: TaskRef

    @classmethod
    def from_response(cls, payload: dict) -> "Lease":
        return cls(
            lease_id=payload["lease_id"],
            deployment_id=payload["deployment_id"],
            token=payload["lease_token"],
            attempt=payload.get("attempt", 1),
            execution_path=payload.get("execution_path", ""),
            argv=list(payload.get("argv", [])),
            task=TaskRef.from_response(payload.get("task", {})),
        )


@dataclass(frozen=True)
class ExecutionResult:
    """What one task did, as the worker reports it.

    retryable is a report, not a decision: the executor narrows it against its
    own denylist and the task's retry budget.
    """
    succeeded: bool
    error_class: str = ""
    error_message: str = ""
    retryable: bool = False
    unsafe_runtime: bool = False

    @classmethod
    def permanent(cls, error_class: str, message: str) -> "ExecutionResult":
        """A failure that will read the same however many times it is tried."""
        return cls(succeeded=False, error_class=error_class, error_message=message)

    @classmethod
    def unsafe(cls, error_class: str, message: str) -> "ExecutionResult":
        """A failure that leaves this process untrustworthy.

        The task may run again elsewhere, but not in this process: it is retried
        by the executor and this worker exits so Kubernetes starts a clean one.
        """
        return cls(succeeded=False, error_class=error_class, error_message=message,
                   retryable=True, unsafe_runtime=True)


class EventSink:
    """Writes a run's dbt events to the log the task uploads.

    dbtRunner takes callbacks as plain callables, so this is called, not
    registered as a listener.
    """

    def __init__(self, log_path: Path):
        self._log_path = Path(log_path)
        self._log_path.parent.mkdir(parents=True, exist_ok=True)

    def __call__(self, event) -> None:
        with self._log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"{event.info.level:<8}{event.info.msg}\n")


@contextmanager
def task_environment(values: dict[str, str]):
    """Apply a task's environment for the length of that task only.

    Every value is restored afterwards, so one task's schema or paths cannot
    leak into the next task this long-lived process runs.
    """
    before = {key: os.environ.get(key) for key in values}
    try:
        os.environ.update(values)
        yield
    finally:
        for key, value in before.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class NativeExecutor:
    """Runs dbt in this process, against the Manifest hydrated at startup."""

    def __init__(self, artifact: LoadedArtifact, event_sink: EventSink):
        self._manifest = artifact.manifest
        self._runner = dbtRunner(manifest=self._manifest, callbacks=[event_sink])

    def execute(self, lease: Lease, task_dir: Path) -> ExecutionResult:
        if lease.task.dbt_unique_id not in self._manifest.nodes:
            return ExecutionResult.permanent(
                "dbt_unique_id_not_found",
                f"{lease.task.dbt_unique_id} is absent from the pinned Manifest",
            )
        selected = [
            unique_id for unique_id, node in self._manifest.nodes.items()
            if node.name == lease.task.table_name
            and node.resource_type in EXECUTABLE_RESOURCE_TYPES
        ]
        if selected != [lease.task.dbt_unique_id]:
            # The argv selects by name, so a name that answers to more than one
            # node would run something other than the task's node.
            return ExecutionResult.permanent(
                "dbt_selector_not_unique",
                f"selector name resolves to {selected!r}",
            )
        if Path(lease.argv[0]).name != "dbt":
            raise ValueError("native executor received non-dbt argv")

        result = self._runner.invoke(lease.argv[1:])
        if isinstance(result.result, RunExecutionResult):
            result.result.write(str(Path(task_dir) / "run_results.json"))
        if result.exception is not None:
            return ExecutionResult.unsafe("dbt_runner_exception", str(result.exception))
        return ExecutionResult(succeeded=result.success)


def release_adapters() -> None:
    """Drop the warehouse connections and adapters this task opened.

    Connection reuse across tasks is not assumed: a worker that cannot release
    them completes its lease and exits rather than run the next task on a
    runtime it no longer understands.
    """
    cleanup_connections()
    reset_adapters()
