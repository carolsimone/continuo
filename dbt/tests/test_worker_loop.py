"""Tests for the worker's startup and its one-task-at-a-time loop.

The loop is exercised against a recording client rather than a socket: the HTTP
contract itself is covered in test_worker_api_client, and what matters here is
the order the worker reports things in and what it does when it is told to stop.
"""
import threading

import pytest

from continuo_dbt_runtime.api_client import CancelledError, StaleLeaseError
from continuo_dbt_runtime.artifact_store import InitializationError
from continuo_dbt_runtime.execution import ExecutionResult, Lease
from continuo_dbt_runtime.worker import Heartbeat, Worker, WorkerConfig, bootstrap

from tests.conftest import SERVICE_NAME, TABLE_NAME, UNIQUE_ID

LEASE_PAYLOAD = {
    "lease_id": "l-1",
    "deployment_id": "d-1",
    "lease_token": "t-1",
    "attempt": 1,
    "expires_at": "2026-01-01T00:00:00Z",
    "execution_path": "native",
    "argv": ["dbt", "run", "--select", TABLE_NAME],
    "task": {
        "task_id": "task-1",
        "schedule_id": "sched-1",
        "service_name": SERVICE_NAME,
        "schema_name": "analytics",
        "table_name": TABLE_NAME,
        "dbt_unique_id": UNIQUE_ID,
    },
}


def config_for(tmp_path, **overrides) -> WorkerConfig:
    values = {
        "executor_url": "http://executor",
        "pool_key": "e" * 64,
        "service_name": SERVICE_NAME,
        "image_tag": "sha-1",
        "runtime_manifest_sha256": "a" * 64,
        "controller_context_json": "{}",
        "pod_name": "worker-0",
        "pod_uid": "uid-0",
        "cache_dir": tmp_path / "cache",
        "ready_file": tmp_path / "ready",
    }
    values.update(overrides)
    return WorkerConfig(**values)


class RecordingClient:
    """A stand-in executor that records what the worker told it."""

    def __init__(self, leases=(), heartbeat_error=None):
        self.calls: list[str] = []
        self.initializations: list[dict] = []
        self.completions: list[dict] = []
        self._leases = list(leases)
        self.heartbeat_error = heartbeat_error

    def initialization(self, ok, error_code="", message="", hydration_seconds=0.0):
        self.calls.append("initialization")
        self.initializations.append(
            {"ok": ok, "error_code": error_code, "message": message}
        )

    def claim(self, wait_seconds, owner, pod_name, pod_uid):
        self.calls.append("claim")
        return self._leases.pop(0) if self._leases else None

    def start(self, lease_id, deployment_id, lease_token):
        self.calls.append("start")

    def heartbeat(self, lease_id, deployment_id, lease_token):
        self.calls.append("heartbeat")
        if self.heartbeat_error is not None:
            raise self.heartbeat_error

    def result_urls(self, lease_id, deployment_id, lease_token):
        self.calls.append("result_urls")
        return {
            "log": {"url": "http://s3/log", "s3_uri": "s3://b/log"},
            "run_results": {"url": "http://s3/rr", "s3_uri": "s3://b/rr"},
        }

    def complete(self, lease_id, deployment_id, lease_token, result):
        self.calls.append("complete")
        self.completions.append(result)


class StubStore:
    def __init__(self, error=None, artifact="artifact"):
        self._error = error
        self._artifact = artifact

    def load(self):
        if self._error is not None:
            raise self._error
        return self._artifact


# --- startup --------------------------------------------------------------


def test_bootstrap_reports_ok_and_turns_ready(tmp_path):
    config = config_for(tmp_path)
    client = RecordingClient()

    artifact = bootstrap(config, client, store=StubStore())

    assert artifact == "artifact"
    assert client.initializations == [
        {"ok": True, "error_code": "", "message": ""}
    ]
    assert config.ready_file.read_text() == "ready\n"


def test_bootstrap_reports_the_error_class_and_stays_unready(tmp_path):
    """An unloadable artifact leaves the pod unready rather than crash-looping."""
    config = config_for(tmp_path)
    client = RecordingClient()
    store = StubStore(error=InitializationError("runtime_manifest_rejected", "bad sha"))
    waited = []

    with pytest.raises(SystemExit) as caught:
        bootstrap(config, client, store=store, wait_unready=lambda: waited.append(True))

    assert caught.value.code == 0
    assert client.initializations[0]["ok"] is False
    assert client.initializations[0]["error_code"] == "runtime_manifest_rejected"
    assert not config.ready_file.exists()
    assert waited == [True]


# --- the loop -------------------------------------------------------------


class StubExecutor:
    """Stands in for a dbt invocation, leaving behind what one leaves behind."""

    def __init__(self, result=None, writes=("dbt.log", "run_results.json")):
        self._result = result or ExecutionResult(succeeded=True)
        self._writes = writes

    def execute(self, lease, task_dir):
        for name in self._writes:
            (task_dir / name).write_text("{}")
        return self._result


def worker_for(tmp_path, client, executor, **kwargs):
    return Worker(
        config_for(tmp_path),
        client,
        artifact=None,
        executor_factory=lambda artifact, sink: executor,
        upload=lambda url, data, content_type: None,
        **kwargs,
    )


def test_worker_claims_starts_executes_and_completes_in_order(tmp_path):
    client = RecordingClient(leases=[LEASE_PAYLOAD])
    worker = worker_for(tmp_path, client, StubExecutor())

    assert worker.run_once() is True

    assert client.calls[:2] == ["claim", "start"]
    assert client.calls[-1] == "complete"
    assert "result_urls" in client.calls


def test_worker_reports_the_locations_the_executor_issued(tmp_path):
    client = RecordingClient(leases=[LEASE_PAYLOAD])

    worker_for(tmp_path, client, StubExecutor()).run_once()

    result = client.completions[0]
    assert result["succeeded"] is True
    assert result["log_s3_uri"] == "s3://b/log"
    assert result["run_results_s3_uri"] == "s3://b/rr"


def test_worker_reports_no_location_for_a_result_it_never_wrote(tmp_path):
    """A task that died before writing reports nothing, rather than a location
    the executor would then try to read."""
    client = RecordingClient(leases=[LEASE_PAYLOAD])
    executor = StubExecutor(result=ExecutionResult.permanent("boom", "died"), writes=())

    worker_for(tmp_path, client, executor).run_once()

    result = client.completions[0]
    assert "log_s3_uri" not in result
    assert "run_results_s3_uri" not in result


def test_worker_reports_a_failure_with_its_error_class(tmp_path):
    client = RecordingClient(leases=[LEASE_PAYLOAD])
    failed = ExecutionResult.permanent("dbt_unique_id_not_found", "absent")

    worker_for(tmp_path, client, StubExecutor(result=failed)).run_once()

    result = client.completions[0]
    assert result["succeeded"] is False
    assert result["error_class"] == "dbt_unique_id_not_found"
    assert result["retryable"] is False


def test_worker_does_no_work_when_there_is_nothing_to_claim(tmp_path):
    client = RecordingClient(leases=[])

    assert worker_for(tmp_path, client, StubExecutor()).run_once() is False

    assert client.calls == ["claim"]


def test_worker_stops_claiming_once_it_is_told_to_shut_down(tmp_path):
    client = RecordingClient(leases=[LEASE_PAYLOAD])
    worker = worker_for(tmp_path, client, StubExecutor())

    worker.shutdown()

    assert worker.run_once() is False
    assert client.calls == []


# --- heartbeat and cancellation -------------------------------------------


def lease_object() -> Lease:
    return Lease.from_response(LEASE_PAYLOAD)


def test_lease_is_read_from_the_grant_verbatim():
    lease = lease_object()

    assert lease.argv == ["dbt", "run", "--select", TABLE_NAME]
    assert lease.token == "t-1"
    assert lease.task.dbt_unique_id == UNIQUE_ID
    assert lease.execution_path == "native"


def test_heartbeat_stops_the_task_when_the_executor_says_cancelled():
    """410 on heartbeat is the only way a worker learns a task was cancelled.

    Nothing deletes the Job or fences the pod, so a worker that ignored this
    would keep writing to the warehouse after the cancel.
    """
    client = RecordingClient(heartbeat_error=CancelledError(410, "cancelled", "gone"))
    stopped = threading.Event()
    beat = Heartbeat(client, lease_object(), interval=0.01, on_stop=stopped.set)

    beat.start()
    try:
        assert stopped.wait(timeout=5)
        assert beat.cancelled
    finally:
        beat.stop()


def test_heartbeat_stops_the_task_when_the_lease_went_stale():
    client = RecordingClient(heartbeat_error=StaleLeaseError(409, "stale_lease", "no"))
    stopped = threading.Event()
    beat = Heartbeat(client, lease_object(), interval=0.01, on_stop=stopped.set)

    beat.start()
    try:
        assert stopped.wait(timeout=5)
        assert not beat.cancelled
    finally:
        beat.stop()


def test_heartbeat_keeps_beating_while_the_lease_holds():
    client = RecordingClient()
    beat = Heartbeat(client, lease_object(), interval=0.01, on_stop=lambda: None)

    beat.start()
    try:
        threading.Event().wait(0.1)
    finally:
        beat.stop()

    assert client.calls.count("heartbeat") >= 2
