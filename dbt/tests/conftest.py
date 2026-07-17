"""Shared fixtures for the worker runtime shipped in the dbt base image.

The manifest fixtures parse a real dbt project once per session and reuse the
partial parse dbt itself wrote, so hydration is exercised against bytes a real
compile produced rather than a hand-built stand-in. The fake executor is served
over a real socket, so the worker's HTTP client is exercised as written rather
than mocked away.
"""
import json
import subprocess
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

# The fixture project's dbt name. A node's fqn[0] is its project name, which the
# worker maps onto a service name by swapping underscores for hyphens.
PROJECT_NAME = "service_1"
SERVICE_NAME = "service-1"
TABLE_NAME = "table_a"
UNIQUE_ID = f"model.{PROJECT_NAME}.{TABLE_NAME}"

DBT_PROJECT_YML = f"""\
name: {PROJECT_NAME}
version: "1.0"
profile: {PROJECT_NAME}
model-paths: ["models"]
"""

# Mirrors the connection shape of the real per-service profiles, so the fixture
# reaches the same warehouse the compile container is wired to.
PROFILES_YML = f"""\
{PROJECT_NAME}:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{{{ env_var('DBT_POSTGRES_HOST', 'postgres') }}}}"
      port: "{{{{ env_var('DBT_POSTGRES_PORT', '5432') | int }}}}"
      dbname: "{{{{ env_var('DBT_POSTGRES_DB', 'continuo_dbt') }}}}"
      user: "{{{{ env_var('DBT_POSTGRES_USER', 'continuo_svc') }}}}"
      password: "{{{{ env_var('DBT_POSTGRES_PASSWORD', 'continuo') }}}}"
      schema: worker_runtime_test
      threads: 1
"""


@pytest.fixture(scope="session")
def real_project(tmp_path_factory) -> Path:
    """A parsed dbt project holding exactly one model, model.service_1.table_a."""
    root = tmp_path_factory.mktemp("dbt") / PROJECT_NAME
    (root / "models").mkdir(parents=True)
    (root / "dbt_project.yml").write_text(DBT_PROJECT_YML)
    (root / "profiles.yml").write_text(PROFILES_YML)
    (root / "models" / f"{TABLE_NAME}.sql").write_text("select 1 as id\n")
    subprocess.run(
        ["dbt", "parse", "--project-dir", str(root), "--profiles-dir", str(root)],
        check=True,
        capture_output=True,
    )
    return root


@pytest.fixture(scope="session")
def real_manifest_bytes(real_project) -> bytes:
    """The partial parse dbt wrote for the fixture project."""
    return (real_project / "target" / "partial_parse.msgpack").read_bytes()


@dataclass(frozen=True)
class RecordedRequest:
    method: str
    path: str
    headers: dict
    body: dict | None


class FakeExecutor:
    """The executor's worker API, served over a real socket.

    Responses are queued per path. A path whose queue is exhausted keeps
    answering with its last queued response, so a test that cares only about the
    steady-state answer does not have to count calls.
    """

    def __init__(self):
        self._queues: dict[str, list] = {}
        self._lock = threading.Lock()
        self.requests: list[RecordedRequest] = []
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(self))
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def queue(self, path: str, *responses) -> None:
        """Queue (status, body) JSON answers for path."""
        with self._lock:
            self._queues.setdefault(path, []).extend(
                (status, "application/json", None if body is None else json.dumps(body).encode())
                for status, body in responses
            )

    def queue_raw(self, path: str, status: int, data: bytes, content_type: str) -> None:
        """Queue one verbatim-bytes answer for path, for artifact downloads."""
        with self._lock:
            self._queues.setdefault(path, []).append((status, content_type, data))

    def take(self, path: str):
        with self._lock:
            queue = self._queues.get(path)
            if not queue:
                return (404, "application/json", json.dumps(
                    {"error": {"code": "not_found", "message": path}}).encode())
            # The last queued response stands in for every later call.
            return queue.pop(0) if len(queue) > 1 else queue[0]

    def record(self, request: RecordedRequest) -> None:
        with self._lock:
            self.requests.append(request)

    def count(self, path: str) -> int:
        with self._lock:
            return sum(1 for r in self.requests if r.path == path)

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def _make_handler(executor: "FakeExecutor"):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _handle(self):
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            body = json.loads(raw) if raw else None
            executor.record(RecordedRequest(
                method=self.command,
                path=self.path,
                headers=dict(self.headers),
                body=body,
            ))
            status, content_type, data = executor.take(self.path)
            self.send_response(status)
            if data is None:
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = _handle
        do_POST = _handle

        def log_message(self, *args):
            """Silence the stderr access log the stdlib handler writes by default."""

    return Handler


@pytest.fixture
def fake_executor():
    executor = FakeExecutor()
    try:
        yield executor
    finally:
        executor.stop()
