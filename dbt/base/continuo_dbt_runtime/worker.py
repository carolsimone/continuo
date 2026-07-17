"""The long-lived worker process: hydrate once, then run tasks one at a time.

The pool credential enters through the environment and is popped into memory
before dbt or anything dbt spawns can read it, so a child process inherits an
environment that does not contain it.

A worker that cannot hydrate its artifact reports why and stays unready rather
than crash-looping: the pod is running and answering, it simply never becomes a
claim target.
"""
from __future__ import annotations

import _thread
import os
import shutil
import signal
import sys
import tempfile
import threading
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from continuo_dbt_runtime.api_client import (
    CancelledError,
    ExecutorClient,
    ExecutorError,
    TerminalError,
)
from continuo_dbt_runtime.artifact_store import ArtifactStore, InitializationError
from continuo_dbt_runtime.execution import (
    CREDENTIAL_ENV,
    EventSink,
    ExecutionResult,
    Lease,
    executor_for,
    release_adapters,
    task_environment,
)

UPLOAD_TIMEOUT_SECONDS = 120.0


@dataclass(frozen=True)
class WorkerConfig:
    """Everything this process needs, other than its credential."""
    executor_url: str
    pool_key: str
    service_name: str
    image_tag: str
    # The artifact digest this pool is pinned to. It is what stops a worker
    # hydrating a valid artifact that belongs to another release.
    runtime_manifest_sha256: str
    controller_context_json: str
    pod_name: str
    pod_uid: str
    cache_dir: Path
    ready_file: Path
    claim_wait_seconds: int = 30
    heartbeat_seconds: float = 10.0

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        return cls(
            executor_url=_required("CONTINUO_EXECUTOR_URL"),
            pool_key=_required("CONTINUO_POOL_KEY"),
            service_name=_required("CONTINUO_SERVICE_NAME"),
            image_tag=_required("CONTINUO_IMAGE_TAG"),
            runtime_manifest_sha256=_required("CONTINUO_RUNTIME_MANIFEST_SHA256"),
            controller_context_json=_required("CONTINUO_RUNTIME_CONTEXT_JSON"),
            pod_name=os.environ.get("CONTINUO_POD_NAME", ""),
            pod_uid=os.environ.get("CONTINUO_POD_UID", ""),
            cache_dir=Path(os.environ.get("CONTINUO_CACHE_DIR", "/tmp/continuo-runtime")),
            ready_file=Path(
                os.environ.get("CONTINUO_READY_FILE", "/tmp/continuo-worker-ready")
            ),
        )


def _required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def take_credential() -> str:
    """Move the pool credential out of the environment and into memory.

    Called before dbt is invoked, so neither dbt nor any process it starts
    inherits an environment carrying the credential.
    """
    try:
        return os.environ.pop(CREDENTIAL_ENV)
    except KeyError:
        raise RuntimeError(f"{CREDENTIAL_ENV} is required") from None


def task_values(lease: Lease, task_dir: str | Path) -> dict[str, str]:
    """The environment one task runs under.

    These name the task a team's dbt project is running, plus the paths that
    keep one task's dbt output out of the next task's. The pool credential is
    never among them.

    Every name here is the one a per-task Job would have set, and carries the
    same value: a team's wrapper reading SCHEDULE_NAME or JOB_NAME reads what it
    has always read, so running on a pool does not change what it sees.
    """
    return {
        "TASK_ID": lease.task.task_id,
        "SCHEDULE_ID": lease.task.schedule_id,
        "SCHEDULE_NAME": lease.task.schedule_name,
        "SERVICE_NAME": lease.task.service_name,
        "SCHEMA": lease.task.schema_name,
        "TABLE_NAME": lease.task.table_name,
        "JOB_NAME": lease.task.job_name,
        "DBT_TARGET_PATH": str(Path(task_dir) / "target"),
        "DBT_LOG_PATH": str(Path(task_dir) / "logs"),
    }


def wait_unready_until_signal() -> None:
    """Stay up, and unready, until Kubernetes replaces this pod."""
    stop = threading.Event()
    for received in (signal.SIGTERM, signal.SIGINT):
        signal.signal(received, lambda *_args: stop.set())
    stop.wait()


def bootstrap(config: WorkerConfig, client, *, store=None,
              wait_unready=wait_unready_until_signal):
    """Hydrate the artifact and report the outcome, once.

    A worker that cannot hydrate never turns ready, so it is never handed a
    task. It does not fall back to parsing the project.
    """
    store = store if store is not None else ArtifactStore(config, client)
    started = time.monotonic()
    try:
        artifact = store.load()
    except InitializationError as exc:
        client.initialization(ok=False, error_code=exc.code, message=str(exc))
        wait_unready()
        raise SystemExit(0)
    client.initialization(ok=True, hydration_seconds=time.monotonic() - started)
    config.ready_file.write_text("ready\n")
    return artifact


class Heartbeat:
    """Holds a lease while its task runs, and stops the task when it cannot.

    A 410 is the only way a worker learns its task was cancelled: cancelling
    neither deletes the Job nor fences the pod, so this is what stops dbt.
    """

    def __init__(self, client, lease: Lease, interval: float, on_stop):
        self._client = client
        self._lease = lease
        self._interval = interval
        self._on_stop = on_stop
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.cancelled = False

    def start(self) -> None:
        self._thread = threading.Thread(target=self._beat, daemon=True)
        self._thread.start()

    def _beat(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._client.heartbeat(
                    self._lease.lease_id, self._lease.deployment_id, self._lease.token
                )
            except CancelledError:
                self.cancelled = True
                self._stop.set()
                self._on_stop()
                return
            except TerminalError:
                # The hold is gone: superseded, or moved to another pool. Either
                # way this worker must stop, and must not ask again.
                self._stop.set()
                self._on_stop()
                return
            except ExecutorError:
                # The executor could not answer. The lease is still ours until it
                # expires, so keep trying rather than abandon a running task.
                continue

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)


def upload_bytes(url: str, data: bytes, content_type: str) -> None:
    """Write one object through a URL the executor signed for exactly that key."""
    request = urllib.request.Request(
        url, data=data, headers={"Content-Type": content_type}, method="PUT"
    )
    with urllib.request.urlopen(request, timeout=UPLOAD_TIMEOUT_SECONDS):
        pass


class Worker:
    """Claims one task at a time and runs it to a terminal report."""

    def __init__(self, config: WorkerConfig, client, artifact, *,
                 executor_factory=None, upload=None):
        self._config = config
        self._client = client
        self._artifact = artifact
        self._executor_factory = executor_factory or executor_for
        self._upload = upload or upload_bytes
        self._stop = threading.Event()
        # Set when a task leaves the process in a state it cannot trust.
        self.unsafe = False
        # The settled refusal that stopped this worker claiming, if one did.
        self.claim_refusal: TerminalError | None = None

    def shutdown(self) -> None:
        """Stop claiming new work."""
        self._stop.set()

    def run_forever(self) -> None:
        while not self._stop.is_set() and not self.unsafe:
            try:
                self.run_once()
            except TerminalError:
                # A lease-scoped call was refused: the hold this worker had is
                # settled, so there is nothing left to report against it. Claim
                # the next task. A refusal of the claim itself does not reach
                # here; run_once stops the worker on those.
                continue

    def run_once(self) -> bool:
        """Claim and run one task. False when there was nothing to do."""
        if self._stop.is_set():
            return False
        try:
            payload = self._client.claim(
                wait_seconds=self._config.claim_wait_seconds,
                owner=self._config.pod_name or self._config.pool_key,
                pod_name=self._config.pod_name,
                pod_uid=self._config.pod_uid,
            )
        except TerminalError as refusal:
            # The claim itself was refused, which is settled: this pod does not
            # belong to this pool, so no later claim can be answered either.
            # Stop and go unready rather than re-ask at full speed forever.
            self._refuse_claims(refusal)
            return False
        if payload is None:
            return False
        self._run_task(Lease.from_response(payload))
        return True

    def _refuse_claims(self, refusal: TerminalError) -> None:
        """Stop claiming, and stop answering ready, for the life of this pod."""
        self.claim_refusal = refusal
        self._stop.set()
        self._config.ready_file.unlink(missing_ok=True)

    def _run_task(self, lease: Lease) -> None:
        task_dir = Path(tempfile.mkdtemp(prefix="continuo-task-"))
        try:
            self._execute_and_report(lease, task_dir)
        finally:
            # Each task leaves a dbt log, run results, and a target tree behind.
            # A worker runs tasks for the life of its pod, so what one task wrote
            # goes before the next one starts.
            shutil.rmtree(task_dir, ignore_errors=True)

    def _execute_and_report(self, lease: Lease, task_dir: Path) -> None:
        log_path = task_dir / "dbt.log"
        executor = self._executor_factory(lease, self._artifact, EventSink(log_path))

        self._client.start(lease.lease_id, lease.deployment_id, lease.token)
        beat = Heartbeat(self._client, lease, self._config.heartbeat_seconds,
                         on_stop=_thread.interrupt_main)
        beat.start()
        started = time.monotonic()
        try:
            with task_environment(task_values(lease, task_dir)):
                result = executor.execute(lease, task_dir)
        except KeyboardInterrupt:
            # The heartbeat stopped this task; the executor already knows why.
            result = ExecutionResult.permanent("cancelled", "the task was cancelled")
        except Exception as exc:
            result = ExecutionResult.unsafe("worker_exception", str(exc))
        finally:
            beat.stop()
        execution_seconds = time.monotonic() - started

        if not self._release_runtime():
            result = ExecutionResult.unsafe(
                "adapter_cleanup_failed", "warehouse connections could not be released"
            )
        self.unsafe = self.unsafe or result.unsafe_runtime

        if beat.cancelled:
            # The task is already settled, so nothing is reported and nothing is
            # uploaded: the partial dbt log this task wrote is discarded with the
            # task directory. Reporting it would only be refused.
            return
        self._report(lease, result, log_path, task_dir, execution_seconds)

    def _release_runtime(self) -> bool:
        try:
            release_adapters()
            return True
        except Exception:
            return False

    def _report(self, lease: Lease, result: ExecutionResult, log_path: Path,
                task_dir: Path, execution_seconds: float) -> None:
        """Settle the lease, whatever the artifacts did.

        complete is the call that must land: dbt has already touched the
        warehouse, so a lease left unsettled is re-run somewhere else. Artifacts
        are best effort, and a task whose result is SUCCEEDED stays SUCCEEDED
        even when nothing could be uploaded to describe it.
        """
        payload = {
            "succeeded": result.succeeded,
            "retryable": result.retryable,
            "error_class": result.error_class,
            "error_message": result.error_message,
            "execution_seconds": execution_seconds,
            "unsafe_runtime": result.unsafe_runtime,
        }
        if result.cache_status:
            payload["cache_status"] = result.cache_status
        upload_started = time.monotonic()
        payload.update(
            self._upload_results(self._result_urls(lease), log_path, task_dir)
        )
        payload["upload_seconds"] = time.monotonic() - upload_started
        self._client.complete(lease.lease_id, lease.deployment_id, lease.token, payload)

    def _result_urls(self, lease: Lease) -> dict:
        """The locations to upload to, or none when the executor issued none.

        A terminal refusal is raised: the lease is settled, so completing it is
        refused too and there is nothing to upload against.
        """
        try:
            return self._client.result_urls(
                lease.lease_id, lease.deployment_id, lease.token
            )
        except TerminalError:
            raise
        except Exception as exc:
            # The exception from a signed-URL request routinely carries the URL
            # itself, which is a capability token, so only its class reaches
            # stderr. A pod's stderr is the diagnostic surface here; without
            # this a systematic failure to fetch result URLs looks, from the
            # executor's side, identical to a worker that simply wrote nothing.
            print(f"result_urls failed: {exc.__class__.__name__}", file=sys.stderr)
            return {}

    def _upload_results(self, urls: dict, log_path: Path, task_dir: Path) -> dict:
        """Upload what the task produced, reporting only what landed.

        A task that failed before writing a file reports no location for it,
        which is how the executor tells "nothing to read" from "read this". An
        upload that fails reports no location for the same reason: the object is
        not there to be read, and the task's own outcome does not change.
        """
        objects = {
            "log_s3_uri": (urls.get("log"), log_path, "text/plain"),
            "run_results_s3_uri": (
                urls.get("run_results"), task_dir / "run_results.json", "application/json"
            ),
        }
        reported = {}
        for field, (signed, path, content_type) in objects.items():
            if not signed or not signed.get("url") or not path.exists():
                continue
            try:
                self._upload(signed["url"], path.read_bytes(), content_type)
            except Exception as exc:
                # Same redaction as above: an HTTP client's exception text
                # routinely embeds the signed URL it was given, so only the
                # field that failed to upload and the exception class name are
                # safe to put on stderr.
                print(f"upload failed for {field}: {exc.__class__.__name__}", file=sys.stderr)
                continue
            reported[field] = signed["s3_uri"]
        return reported


def main() -> int:
    credential = take_credential()
    config = WorkerConfig.from_env()
    client = ExecutorClient(config.executor_url, config.pool_key, credential)
    artifact = bootstrap(config, client)

    worker = Worker(config, client, artifact)
    for received in (signal.SIGTERM, signal.SIGINT):
        signal.signal(received, lambda *_args: worker.shutdown())
    worker.run_forever()
    return 0
