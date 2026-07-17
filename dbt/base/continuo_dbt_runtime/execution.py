"""Running one dbt node against a Manifest the process already holds.

A task names one dbt unique id and one resolved argv. Neither is derived here:
the executor resolved them, and this module runs exactly what it was handed or
refuses. Nothing reconstructs SQL or a materialization from a node.

There are two ways to run a task, and the lease says which. Plain dbt runs in
this process against the hydrated Manifest. Anything else is a team's own
wrapper program, which runs as an unchanged child process: it is the same
program, with the same argv, in the same working directory it would have under a
per-task Job, so moving a team onto a worker pool does not change what their
wrapper does.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from dbt.adapters.factory import cleanup_connections, reset_adapters
from dbt.cli.main import dbtRunner
from dbt.contracts.results import RunExecutionResult

from continuo_dbt_runtime.artifact_store import EXECUTABLE_RESOURCE_TYPES, LoadedArtifact

# The environment value the pool credential arrives in. It is popped at worker
# startup and never read again.
CREDENTIAL_ENV = "CONTINUO_POOL_CREDENTIAL"  # noqa: S105 - a variable name

# What is removed from a child's environment before it starts. A wrapper is a
# team's own program running arbitrary code, and nothing it runs has any business
# speaking to the executor for this pool. The pool key is deliberately not here:
# it names the pool a caller claims to be, and the credential is what proves it.
POOL_SECRET_ENV_KEYS: tuple[str, ...] = (CREDENTIAL_ENV,)

# Where a wrapper runs. It is the team image's working directory, so a wrapper
# resolves relative paths exactly as it does under a per-task Job.
CHILD_CWD = "/project"

# The paths a lease can pin, matching executor-controller's ExecutionPath.
EXECUTION_PATH_NATIVE = "native"
EXECUTION_PATH_WRAPPER_REQUIRED = "wrapper_required"
EXECUTION_PATH_WRAPPER_OPAQUE = "wrapper_opaque"

# dbt event codes that prove a run read the promoted parse cache. I017
# (PartialParsingSkipParsing) says a reused manifest needed no reparsing; I040
# (PartialParsingEnabled) says a partial parse ran off one. A single dbt
# invocation that reuses a cache emits both, so acceptance is "at least one of
# these", never a count.
CACHE_ACCEPTED = frozenset({"I017", "I040"})

# dbt event codes that say the promoted parse cache was not used: partial parsing
# failed (I016 PartialParsingError), could not be attempted (I024
# UnableToPartialParse), or was switched off (I028 PartialParsingNotEnabled).
CACHE_REJECTED = frozenset({"I016", "I024", "I028"})

# How long a wrapper is given to exit on SIGTERM before it is killed.
TERMINATE_GRACE_SECONDS = 5.0

# How long each step of the wait for a wrapper lasts. It bounds how late a
# cancellation reaches a running wrapper, so it is short.
WAIT_POLL_SECONDS = 0.1


@dataclass(frozen=True)
class TaskRef:
    """The dbt node a lease covers."""
    task_id: str
    schedule_id: str
    service_name: str
    schema_name: str
    table_name: str
    dbt_unique_id: str
    # The human-readable names a team's dbt project may read out of its
    # environment. They name the same schedule and job a per-task Job would.
    schedule_name: str = ""
    job_name: str = ""

    @classmethod
    def from_response(cls, payload: dict) -> "TaskRef":
        return cls(
            task_id=payload.get("task_id", ""),
            schedule_id=payload.get("schedule_id", ""),
            service_name=payload.get("service_name", ""),
            schema_name=payload.get("schema_name", ""),
            table_name=payload.get("table_name", ""),
            dbt_unique_id=payload.get("dbt_unique_id", ""),
            schedule_name=payload.get("schedule_name", ""),
            job_name=payload.get("job_name", ""),
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
    # What this task observed about the promoted parse cache, when it was in a
    # position to observe anything: "accepted", "rejected", or "unknown".
    cache_status: str = ""

    @classmethod
    def permanent(cls, error_class: str, message: str,
                  cache_status: str = "") -> "ExecutionResult":
        """A failure that will read the same however many times it is tried."""
        return cls(succeeded=False, error_class=error_class, error_message=message,
                   cache_status=cache_status)

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

    @property
    def log_path(self) -> Path:
        """The log this task's output lands in, however the task was run."""
        return self._log_path

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


def selector_refusal(manifest, task: TaskRef) -> ExecutionResult | None:
    """Refuse a task whose argv would not select exactly the node it names.

    Both ways of running a task select by name, and neither can see what the
    other's selection resolved to, so both ask this before running anything.
    """
    if task.dbt_unique_id not in manifest.nodes:
        return ExecutionResult.permanent(
            "dbt_unique_id_not_found",
            f"{task.dbt_unique_id} is absent from the pinned Manifest",
        )
    selected = [
        unique_id for unique_id, node in manifest.nodes.items()
        if node.name == task.table_name
        and node.resource_type in EXECUTABLE_RESOURCE_TYPES
    ]
    if selected != [task.dbt_unique_id]:
        # The argv selects by name, so a name that answers to more than one
        # node would run something other than the task's node.
        return ExecutionResult.permanent(
            "dbt_selector_not_unique",
            f"selector name resolves to {selected!r}",
        )
    return None


class NativeExecutor:
    """Runs dbt in this process, against the Manifest hydrated at startup."""

    def __init__(self, artifact: LoadedArtifact, event_sink: EventSink):
        self._manifest = artifact.manifest
        self._runner = dbtRunner(manifest=self._manifest, callbacks=[event_sink])

    def execute(self, lease: Lease, task_dir: Path) -> ExecutionResult:
        refusal = selector_refusal(self._manifest, lease.task)
        if refusal is not None:
            return refusal
        if Path(lease.argv[0]).name != "dbt":
            raise ValueError("native executor received non-dbt argv")

        result = self._runner.invoke(lease.argv[1:])
        if isinstance(result.result, RunExecutionResult):
            result.result.write(str(Path(task_dir) / "run_results.json"))
        if result.exception is not None:
            return ExecutionResult.unsafe("dbt_runner_exception", str(result.exception))
        return ExecutionResult(succeeded=result.success)


def _event_code(line: str) -> str:
    """The dbt event code one line of a wrapper's output carries, if any.

    A wrapper writes its own output alongside dbt's, and prose that merely
    mentions a code is not the same as dbt reporting one, so only a well-formed
    dbt JSON event counts.
    """
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return ""
    if not isinstance(event, dict):
        return ""
    info = event.get("info")
    return info.get("code", "") if isinstance(info, dict) else ""


class CacheEvidence:
    """What a wrapper's dbt events said about the promoted parse cache."""

    def __init__(self):
        self.accepted: list[str] = []
        self.rejected_code = ""

    def observe(self, line: str) -> bool:
        """Record one line of output. True when it rejects the cache."""
        code = _event_code(line)
        if code in CACHE_ACCEPTED:
            self.accepted.append(code)
        elif code in CACHE_REJECTED:
            # The first rejection is the one that explains the run; a terminated
            # wrapper may emit more on its way out.
            self.rejected_code = self.rejected_code or code
            return True
        return False

    def status(self) -> str:
        """What the run proved. A rejection outranks any earlier acceptance."""
        if self.rejected_code:
            return "rejected"
        return "accepted" if self.accepted else "unknown"

    def explain(self) -> str:
        if self.rejected_code:
            return (f"the wrapper's dbt reported {self.rejected_code}: it did not "
                    f"use the promoted parse cache")
        return "the wrapper's dbt never reported using the promoted parse cache"


class WrapperExecutor:
    """Runs a team's dbt wrapper as an unchanged child process.

    The wrapper is started exactly as the executor resolved it: argv is passed as
    a list and never through a shell, so an argument is an argument and never a
    fragment of a command line something else re-interprets. What the worker adds
    is a task-local copy of the promoted parse cache and the removal of its own
    credential; what it takes away is nothing.

    required_cache is the team's own claim that their wrapper reuses dbt's parse
    cache. When they claim it, the run must show it: a wrapper that reparses the
    project is doing the work the pool exists to avoid, and is stopped rather
    than left to finish. When they claim nothing, what dbt says is recorded and
    nothing is required of it.
    """

    def __init__(self, artifact: LoadedArtifact, event_sink: EventSink, *,
                 required_cache: bool):
        self._artifact = artifact
        self._sink = event_sink
        self.required_cache = required_cache
        self._write_lock = threading.Lock()
        # The child's pid while it runs, so cancellation can reach its group.
        self.child_pid: int | None = None

    def execute(self, lease: Lease, task_dir: Path) -> ExecutionResult:
        refusal = selector_refusal(self._artifact.manifest, lease.task)
        if refusal is not None:
            return refusal

        task_dir = Path(task_dir)
        # A task-local copy: a wrapper may write to the cache it is given, and
        # the artifact this pod hydrated from must outlive every task that runs.
        task_cache = task_dir / "partial_parse.msgpack"
        shutil.copyfile(self._artifact.canonical_path, task_cache)

        evidence = CacheEvidence()
        process = subprocess.Popen(  # noqa: S603 - a resolved argv, never a shell
            lease.argv,
            cwd=CHILD_CWD,
            env=self._child_env(task_cache, task_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            # Its own process group, so terminating the wrapper terminates the
            # dbt it shelled out to rather than orphaning it.
            start_new_session=True,
        )
        self.child_pid = process.pid
        pumps = [
            threading.Thread(target=self._pump, args=(process.stdout, process, evidence),
                             daemon=True),
            threading.Thread(target=self._pump, args=(process.stderr, process, None),
                             daemon=True),
        ]
        for pump in pumps:
            pump.start()
        try:
            exit_code = self._wait_interruptibly(process)
        except KeyboardInterrupt:
            # The heartbeat stops a task by interrupting the main thread. The
            # wrapper does not know that happened, so it is stopped here.
            self._terminate(process)
            raise
        for pump in pumps:
            pump.join(timeout=TERMINATE_GRACE_SECONDS)
        return self._result(exit_code, evidence)

    def _wait_interruptibly(self, process) -> int:
        """Wait for the wrapper, staying reachable by an interrupt while it runs.

        The heartbeat stops a task by raising KeyboardInterrupt in the main
        thread, which Python only raises between bytecode instructions. A plain
        process.wait() blocks in waitpid instead, so the interrupt would not be
        seen until the wrapper exited on its own — the one case cancellation
        exists for. Waiting in short steps runs bytecode between each one, so a
        cancelled wrapper is stopped while it is still running.
        """
        while True:
            try:
                return process.wait(timeout=WAIT_POLL_SECONDS)
            except subprocess.TimeoutExpired:
                continue

    def _child_env(self, task_cache: Path, task_dir: Path) -> dict[str, str]:
        """The environment the wrapper runs under.

        It is this process's environment, so the wrapper reads everything a
        per-task Job gave it, plus the cache and paths that keep one task's dbt
        output out of the next task's, minus what speaks for this pool.

        The log settings below are the one part a wrapper can tell apart from
        the environment a per-task Job gave it: the dbt the wrapper spawns logs
        JSON at debug level, so a wrapper that reads dbt's stdout reads JSON
        here. They are set because dbt's structured log is the only channel that
        carries the cache-evidence codes, and a wrapper declared as reusing the
        promoted cache is held to showing that it did.
        """
        env = os.environ.copy()
        env.update({
            "DBT_PARTIAL_PARSE_FILE_PATH": str(task_cache),
            "DBT_LOG_FORMAT": "json",
            "DBT_LOG_LEVEL": "debug",
            "DBT_TARGET_PATH": str(task_dir / "target"),
            "DBT_LOG_PATH": str(task_dir / "logs"),
        })
        for key in POOL_SECRET_ENV_KEYS:
            env.pop(key, None)
        return env

    def _pump(self, stream, process, evidence: CacheEvidence | None) -> None:
        """Copy one of the wrapper's pipes into the task log as it is written.

        The bytes the wrapper wrote are the bytes recorded: reading them decides
        nothing about what the wrapper is handed.
        """
        with stream:
            for line in stream:
                self._write(line)
                if evidence is None:
                    continue
                if evidence.observe(line) and self.required_cache:
                    self._terminate(process)

    def _write(self, text: str) -> None:
        with self._write_lock, self._sink.log_path.open("a", encoding="utf-8") as log:
            log.write(text)

    def _terminate(self, process) -> None:
        """Stop the wrapper and everything it started, then reap it.

        A worker outlives every task it runs, so a child that was signalled but
        never waited on would sit as a zombie for the life of the pod.
        """
        try:
            group = os.getpgid(process.pid)
        except ProcessLookupError:
            return
        for number in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(group, number)
            except ProcessLookupError:
                break
            try:
                process.wait(timeout=TERMINATE_GRACE_SECONDS)
                return
            except subprocess.TimeoutExpired:
                continue
        process.wait()

    def _result(self, exit_code: int, evidence: CacheEvidence) -> ExecutionResult:
        status = evidence.status()
        if self.required_cache and status == "rejected":
            # The team said their wrapper reuses the cache and the wrapper said
            # outright that it did not, so what it did to the warehouse cannot be
            # attributed to the pinned manifest. That reading stands whatever the
            # exit code says, because the run is stopped the moment a rejection
            # is seen and the code it exits with is that stop. No retry changes
            # it.
            return ExecutionResult.permanent(
                "runtime_manifest_unverified", evidence.explain(), cache_status=status
            )
        if exit_code != 0:
            # A wrapper that exited non-zero without saying anything about the
            # cache failed; it did not withhold evidence. Reading the silence as
            # unverified first would point an operator at a cache the wrapper
            # never got as far as using.
            return ExecutionResult.permanent(
                "wrapper_failed", f"the wrapper exited {exit_code}", cache_status=status
            )
        if self.required_cache and status != "accepted":
            # The wrapper ran to a clean exit and still never showed it read the
            # cache. Silence is not evidence: it may have reparsed the project
            # the pool exists to parse once.
            return ExecutionResult.permanent(
                "runtime_manifest_unverified", evidence.explain(), cache_status=status
            )
        return ExecutionResult(succeeded=True, cache_status=status)


def executor_for(lease: Lease, artifact: LoadedArtifact, event_sink: EventSink):
    """The executor the lease's pinned execution path names.

    The path was decided when the task was claimed and persisted with it, so it
    is read here rather than re-derived from the argv.
    """
    if lease.execution_path == EXECUTION_PATH_NATIVE:
        return NativeExecutor(artifact, event_sink)
    if lease.execution_path in (EXECUTION_PATH_WRAPPER_REQUIRED,
                                EXECUTION_PATH_WRAPPER_OPAQUE):
        return WrapperExecutor(
            artifact, event_sink,
            required_cache=lease.execution_path == EXECUTION_PATH_WRAPPER_REQUIRED,
        )
    raise ValueError(f"unknown execution path {lease.execution_path!r}")


def release_adapters() -> None:
    """Drop the warehouse connections and adapters this task opened.

    Connection reuse across tasks is not assumed: a worker that cannot release
    them completes its lease and exits rather than run the next task on a
    runtime it no longer understands.
    """
    cleanup_connections()
    reset_adapters()
