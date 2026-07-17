"""Tests for the worker's startup and its one-task-at-a-time loop.

The loop is exercised against a recording client rather than a socket: the HTTP
contract itself is covered in test_worker_api_client, and what matters here is
the order the worker reports things in and what it does when it is told to stop.
"""
import threading
import time
from pathlib import Path

import pytest

from continuo_dbt_runtime.api_client import (
    CancelledError,
    PoolMismatchError,
    RequestFailed,
    StaleLeaseError,
)
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
        self.start_error = None
        self.result_urls_error = None
        # Called when a claim finds nothing, which is where a real worker would
        # long-poll. A test that drives run_forever stops the worker here.
        self.on_empty_claim = None

    def initialization(self, ok, error_code="", message="", hydration_seconds=0.0):
        self.calls.append("initialization")
        self.initializations.append(
            {"ok": ok, "error_code": error_code, "message": message}
        )

    def claim(self, wait_seconds, owner, pod_name, pod_uid):
        self.calls.append("claim")
        if self._leases:
            return self._leases.pop(0)
        if self.on_empty_claim is not None:
            self.on_empty_claim()
        return None

    def start(self, lease_id, deployment_id, lease_token):
        self.calls.append("start")
        if self.start_error is not None:
            raise self.start_error

    def heartbeat(self, lease_id, deployment_id, lease_token):
        self.calls.append("heartbeat")
        if self.heartbeat_error is not None:
            raise self.heartbeat_error

    def result_urls(self, lease_id, deployment_id, lease_token):
        self.calls.append("result_urls")
        if self.result_urls_error is not None:
            raise self.result_urls_error
        return {
            "log": {"url": "http://s3/log", "s3_uri": "s3://b/log"},
            "run_results": {"url": "http://s3/rr", "s3_uri": "s3://b/rr"},
        }

    def complete(self, lease_id, deployment_id, lease_token, result):
        self.calls.append("complete")
        self.completions.append(result)


class CountingHeartbeatClient(RecordingClient):
    """Signals once it has been beaten a second time, so a test need not sleep."""

    def __init__(self, on_second_beat):
        super().__init__()
        self._on_second_beat = on_second_beat

    def heartbeat(self, lease_id, deployment_id, lease_token):
        super().heartbeat(lease_id, deployment_id, lease_token)
        if self.calls.count("heartbeat") == 2:
            self._on_second_beat()


class RefusingClient(RecordingClient):
    """An executor that refuses this worker's claims, settled, every time.

    A worker that re-asked would never be answered, so the fake stops the test
    at a bound rather than letting it spin for as long as the pod would.
    """

    def __init__(self, refusal, bound=5):
        super().__init__()
        self._refusal = refusal
        self._bound = bound

    def claim(self, wait_seconds, owner, pod_name, pod_uid):
        self.calls.append("claim")
        if self.calls.count("claim") > self._bound:
            raise AssertionError(
                "the worker kept claiming after a settled refusal"
            )
        raise self._refusal


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

    def __init__(self, result=None, writes=("dbt.log", "run_results.json"),
                 raises=None):
        self._result = result or ExecutionResult(succeeded=True)
        self._writes = writes
        self._raises = raises
        # Where the worker ran this task, as the task itself saw it.
        self.task_dirs: list[Path] = []

    def execute(self, lease, task_dir):
        self.task_dirs.append(Path(task_dir))
        for name in self._writes:
            (task_dir / name).write_text("{}")
        if self._raises is not None:
            raise self._raises
        return self._result


class CancelledExecutor:
    """Runs until the heartbeat interrupts the main thread, as dbt would.

    Writes a partial log first, so a test can tell whether a cancelled task's
    directory is cleaned up.
    """

    def __init__(self):
        self.task_dirs: list[Path] = []

    def execute(self, lease, task_dir):
        self.task_dirs.append(Path(task_dir))
        (task_dir / "dbt.log").write_text("partial")
        while True:
            time.sleep(0.01)


def worker_for(tmp_path, client, executor, config=None, **kwargs):
    kwargs.setdefault("upload", lambda url, data, content_type: None)
    return Worker(
        config if config is not None else config_for(tmp_path),
        client,
        artifact=None,
        executor_factory=lambda artifact, sink: executor,
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


# --- a refused claim ------------------------------------------------------


def test_a_settled_claim_refusal_stops_the_worker(tmp_path):
    """A pool this pod does not belong to will never answer it a task.

    Re-asking would be a full-speed claim loop against the fence for the life of
    the pod, which is the thing the client's terminal-status rule exists to stop.
    """
    config = config_for(tmp_path)
    config.ready_file.write_text("ready\n")
    refusal = PoolMismatchError(403, "pool_mismatch", "not your pool")
    client = RefusingClient(refusal)
    worker = worker_for(tmp_path, client, StubExecutor(), config=config)

    worker.run_forever()

    assert client.calls == ["claim"]
    assert worker.claim_refusal is refusal
    assert not config.ready_file.exists()


def test_a_settled_lease_refusal_does_not_stop_the_worker(tmp_path):
    """A lease refused under this worker is settled, but the pod is still good.

    This is the case run_forever's continue is for, and it must stay distinct
    from a refusal of the claim itself.
    """
    client = RecordingClient(leases=[LEASE_PAYLOAD])
    client.start_error = StaleLeaseError(409, "stale_lease", "superseded")
    worker = worker_for(tmp_path, client, StubExecutor())
    client.on_empty_claim = worker.shutdown

    worker.run_forever()

    assert worker.claim_refusal is None
    assert client.calls.count("claim") == 2


# --- what a task leaves behind --------------------------------------------


def test_a_finished_task_leaves_no_directory_behind(tmp_path):
    """A worker outlives every task it runs, so nothing a task wrote may stay."""
    client = RecordingClient(leases=[LEASE_PAYLOAD])
    executor = StubExecutor()

    worker_for(tmp_path, client, executor).run_once()

    assert executor.task_dirs
    assert not executor.task_dirs[0].exists()


def test_a_task_that_raised_leaves_no_directory_behind(tmp_path):
    client = RecordingClient(leases=[LEASE_PAYLOAD])
    executor = StubExecutor(raises=RuntimeError("boom"))

    worker_for(tmp_path, client, executor).run_once()

    assert executor.task_dirs
    assert not executor.task_dirs[0].exists()


def test_a_cancelled_task_leaves_no_directory_behind(tmp_path):
    """A cancelled task reports nothing, so its partial log goes with it."""
    client = RecordingClient(
        leases=[LEASE_PAYLOAD],
        heartbeat_error=CancelledError(410, "cancelled", "gone"),
    )
    executor = CancelledExecutor()
    config = config_for(tmp_path, heartbeat_seconds=0.01)

    worker_for(tmp_path, client, executor, config=config).run_once()

    assert client.completions == []
    assert executor.task_dirs
    assert not executor.task_dirs[0].exists()


# --- reporting when the artifacts do not land ------------------------------


def test_an_upload_failure_still_completes_the_lease(tmp_path):
    """dbt already touched the warehouse, so the lease must still be settled.

    Failing to write a log is not a reason to re-run a model, and a lease left
    unsettled would be exactly that once it expires.
    """
    client = RecordingClient(leases=[LEASE_PAYLOAD])

    def failing_upload(url, data, content_type):
        raise OSError("s3 is having a moment")

    worker_for(tmp_path, client, StubExecutor(), upload=failing_upload).run_once()

    assert client.calls[-1] == "complete"
    result = client.completions[0]
    assert result["succeeded"] is True
    assert "log_s3_uri" not in result
    assert "run_results_s3_uri" not in result


def test_locations_that_did_upload_are_still_reported(tmp_path):
    """One object failing does not hide another that landed."""
    client = RecordingClient(leases=[LEASE_PAYLOAD])

    def upload_log_only(url, data, content_type):
        if url != "http://s3/log":
            raise OSError("s3 is having a moment")

    worker_for(tmp_path, client, StubExecutor(), upload=upload_log_only).run_once()

    result = client.completions[0]
    assert result["log_s3_uri"] == "s3://b/log"
    assert "run_results_s3_uri" not in result


def test_a_result_urls_failure_still_completes_the_lease(tmp_path):
    """No locations to upload to is not a reason to lose the warehouse work."""
    client = RecordingClient(leases=[LEASE_PAYLOAD])
    client.result_urls_error = RequestFailed(0, "unreachable", "no route")

    worker_for(tmp_path, client, StubExecutor()).run_once()

    assert client.calls[-1] == "complete"
    assert client.completions[0]["succeeded"] is True


def test_a_result_urls_failure_reports_only_the_exception_class(tmp_path, capsys):
    """The diagnostic never repeats what the exception says, only what it is.

    An executor error's message can itself echo back request details, so the
    stderr line must carry the exception's class and nothing from its text.
    """
    client = RecordingClient(leases=[LEASE_PAYLOAD])
    client.result_urls_error = RequestFailed(
        0, "unreachable", "no route to http://s3/log?sig=redacted-me"
    )

    worker_for(tmp_path, client, StubExecutor()).run_once()

    captured = capsys.readouterr()
    assert "RequestFailed" in captured.err
    assert "http://s3/log" not in captured.err


def test_an_upload_failure_reports_only_the_field_and_exception_class(tmp_path, capsys):
    """A signed URL is a capability token, so it must never reach stderr.

    The exception a real HTTP client raises for a failed PUT routinely embeds
    the URL it was given, which is why only the field name and the exception's
    class name are safe to print.
    """
    client = RecordingClient(leases=[LEASE_PAYLOAD])

    def failing_upload(url, data, content_type):
        raise OSError(f"connection reset while PUTting to {url}")

    worker_for(tmp_path, client, StubExecutor(), upload=failing_upload).run_once()

    captured = capsys.readouterr()
    assert "OSError" in captured.err
    assert "log_s3_uri" in captured.err
    assert "http://s3/log" not in captured.err
    assert "http://s3/rr" not in captured.err


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
    """A lease that still holds is beaten again, rather than beaten once."""
    beat_twice = threading.Event()
    client = CountingHeartbeatClient(on_second_beat=beat_twice.set)
    beat = Heartbeat(client, lease_object(), interval=0.01, on_stop=lambda: None)

    beat.start()
    try:
        assert beat_twice.wait(timeout=5)
    finally:
        beat.stop()

    assert client.calls.count("heartbeat") >= 2
