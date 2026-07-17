"""The worker's client for the executor's internal API.

Only the standard library is used, so the client adds no dependency to any team
image that inherits the dbt base image.

The one rule that matters here is which answers may be retried. A worker that
has been superseded, moved to another pool, or cancelled is told so with a
settled status, and retrying any of them would put a worker in a loop against
the fence for as long as its pod lives. Only an answer that says the executor
could not reach a verdict is tried again.
"""
import json
import random
import time
import urllib.error
import urllib.request

# Endpoints the worker calls. The lease-scoped ones name their lease in the path.
RUNTIME_PATH = "/internal/v1/worker/runtime"
CLAIM_PATH = "/internal/v1/workers/claim"
INITIALIZATION_PATH = "/internal/v1/workers/initialization"

# The header the raw lease token travels in. It is a second credential, distinct
# from the pool credential that says which pool the caller is, and rides its own
# header so neither is mistaken for the other.
LEASE_TOKEN_HEADER = "X-Continuo-Lease-Token"  # noqa: S105 - a header name
POOL_KEY_HEADER = "X-Continuo-Pool-Key"

# Statuses worth trying again: the executor did not reach a verdict, so the same
# request may still be answered differently.
RETRIABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class ExecutorError(Exception):
    """A request the worker cannot complete."""

    def __init__(self, status: int, code: str, message: str):
        self.status = status
        self.code = code
        # The message is composed from the executor's stable error envelope, so
        # it carries neither credential nor signed URL.
        super().__init__(f"executor answered {status} {code}: {message}")


class TerminalError(ExecutorError):
    """An answer that is settled. Retrying it can never change the outcome."""


class StaleLeaseError(TerminalError):
    """The lease is no longer current, or names a deployment that is not there.

    The executor answers both with 409 on purpose: telling them apart would say
    which deployments exist. Either way this worker's hold is over.
    """


class PoolMismatchError(TerminalError):
    """The task belongs to another pool."""


class CancelledError(TerminalError):
    """The task was cancelled.

    This is the only way a worker learns to stop: cancelling neither deletes the
    Kubernetes Job nor fences the pod, so the worker itself must stop dbt.
    """


class RequestFailed(ExecutorError):
    """The request could not be completed, after any retries it was owed."""


def _error_for(status: int, body: dict | None) -> ExecutorError:
    detail = (body or {}).get("error") or {}
    code = detail.get("code", "")
    message = detail.get("message", "")
    if status == 409:
        return StaleLeaseError(status, code or "stale_lease", message)
    if status == 403:
        return PoolMismatchError(status, code or "pool_mismatch", message)
    if status == 410:
        return CancelledError(status, code or "cancelled", message)
    return RequestFailed(status, code, message)


class ExecutorClient:
    """The executor's worker API, as the worker sees it."""

    def __init__(self, base_url: str, pool_key: str, credential: str,
                 timeout_seconds: float = 30.0, max_attempts: int = 5,
                 backoff_seconds: float = 0.5, sleep=time.sleep):
        self._base_url = base_url.rstrip("/")
        self._pool_key = pool_key
        self._credential = credential
        self._timeout = timeout_seconds
        self._max_attempts = max_attempts
        self._backoff = backoff_seconds
        self._sleep = sleep

    def request(self, method: str, path: str, body: dict | None = None,
                lease_token: str | None = None) -> tuple[int, dict | None]:
        """Make one request, without retrying anything."""
        headers = {
            "Authorization": f"Bearer {self._credential}",
            POOL_KEY_HEADER: self._pool_key,
            "Content-Type": "application/json",
        }
        if lease_token is not None:
            headers[LEASE_TOKEN_HEADER] = lease_token
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            f"{self._base_url}{path}", data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                raw = response.read()
                return response.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                parsed = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                # A body that is not the executor's error envelope tells the
                # worker nothing; the status is what it acts on.
                parsed = None
            return exc.code, parsed

    def call(self, method: str, path: str, body: dict | None = None,
             lease_token: str | None = None) -> tuple[int, dict | None]:
        """Make a request, trying again only while the answer is unsettled.

        A terminal answer is raised on the attempt that produced it, so a
        superseded worker stops on its first refusal rather than backing off
        against a fence that will never move.
        """
        for attempt in range(1, self._max_attempts + 1):
            try:
                status, body_out = self.request(method, path, body, lease_token)
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                # The executor was not reached, so it holds no opinion yet.
                if attempt == self._max_attempts:
                    raise RequestFailed(0, "unreachable", str(exc)) from exc
                self._sleep(self._delay(attempt))
                continue

            if status < 400:
                return status, body_out
            error = _error_for(status, body_out)
            if isinstance(error, TerminalError) or status not in RETRIABLE_STATUSES:
                raise error
            if attempt == self._max_attempts:
                raise error
            self._sleep(self._delay(attempt))
        raise RequestFailed(0, "unreachable", "retries exhausted")

    def _delay(self, attempt: int) -> float:
        """Bounded exponential backoff, jittered so a pool does not sync up."""
        return min(self._backoff * (2 ** (attempt - 1)), 30.0) * (
            0.5 + random.random() / 2  # noqa: S311 - jitter, not cryptography
        )

    # --- typed calls ------------------------------------------------------

    def runtime(self) -> dict:
        """The signed reads of the artifact this pool executes against."""
        _, body = self.call("GET", RUNTIME_PATH)
        return body or {}

    def claim(self, wait_seconds: int, owner: str, pod_name: str,
              pod_uid: str) -> dict | None:
        """One task from this worker's pool, or None if nothing is due."""
        status, body = self.call("POST", CLAIM_PATH, {
            "wait_seconds": wait_seconds,
            "owner": owner,
            "pod_name": pod_name,
            "pod_uid": pod_uid,
        })
        return None if status == 204 else body

    def start(self, lease_id: str, deployment_id: str, lease_token: str) -> None:
        self.call("POST", f"/internal/v1/leases/{lease_id}/start",
                  {"deployment_id": deployment_id}, lease_token)

    def heartbeat(self, lease_id: str, deployment_id: str, lease_token: str) -> None:
        self.call("POST", f"/internal/v1/leases/{lease_id}/heartbeat",
                  {"deployment_id": deployment_id}, lease_token)

    def result_urls(self, lease_id: str, deployment_id: str, lease_token: str) -> dict:
        _, body = self.call("POST", f"/internal/v1/leases/{lease_id}/result-urls",
                            {"deployment_id": deployment_id}, lease_token)
        return body or {}

    def complete(self, lease_id: str, deployment_id: str, lease_token: str,
                 result: dict) -> None:
        self.call("POST", f"/internal/v1/leases/{lease_id}/complete",
                  {"deployment_id": deployment_id, "result": result}, lease_token)

    def initialization(self, ok: bool, error_code: str = "", message: str = "",
                       hydration_seconds: float = 0.0) -> None:
        """Report whether this worker hydrated its artifact."""
        self.call("POST", INITIALIZATION_PATH, {
            "ok": ok,
            "error_code": error_code,
            "message": message,
            "hydration_seconds": hydration_seconds,
        })
