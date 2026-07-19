import http.client
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

from adapters.health.server import HealthServer


def _get(server: HealthServer, path: str) -> tuple[int, str]:
    conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=2)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        return resp.status, resp.read().decode()
    finally:
        conn.close()


def _serving(server: HealthServer) -> HealthServer:
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _alive_thread() -> MagicMock:
    thread = MagicMock()
    thread.is_alive.return_value = True
    return thread


def test_health_and_ready_ok_when_consumer_is_fresh():
    consumer = SimpleNamespace(last_heartbeat=time.monotonic())
    server = _serving(HealthServer(0, consumer, _alive_thread(), max_staleness_seconds=60))
    try:
        for path in ("/health", "/ready"):
            status, body = _get(server, path)
            assert status == 200
            assert body == "OK"
    finally:
        server.shutdown()
        server.server_close()


def test_unhealthy_when_consumer_thread_is_not_alive():
    consumer = SimpleNamespace(last_heartbeat=time.monotonic())
    dead_thread = MagicMock()
    dead_thread.is_alive.return_value = False
    server = _serving(HealthServer(0, consumer, dead_thread, max_staleness_seconds=60))
    try:
        status, body = _get(server, "/health")
        assert status == 503
        assert "not running" in body
    finally:
        server.shutdown()
        server.server_close()


def test_unhealthy_when_heartbeat_is_stale():
    """A thread that's technically alive but stuck (never returns from a
    call, never raises) must still fail the check once its last successful
    pass falls outside the staleness window — this is the actual backstop
    for 'the loop stopped making progress'."""
    consumer = SimpleNamespace(last_heartbeat=time.monotonic() - 120)
    server = _serving(HealthServer(0, consumer, _alive_thread(), max_staleness_seconds=60))
    try:
        status, body = _get(server, "/ready")
        assert status == 503
        assert "stale" in body
    finally:
        server.shutdown()
        server.server_close()


def test_fresh_heartbeat_within_window_is_healthy():
    consumer = SimpleNamespace(last_heartbeat=time.monotonic() - 5)
    server = _serving(HealthServer(0, consumer, _alive_thread(), max_staleness_seconds=60))
    try:
        status, _ = _get(server, "/ready")
        assert status == 200
    finally:
        server.shutdown()
        server.server_close()


def test_unknown_path_is_404():
    consumer = SimpleNamespace(last_heartbeat=time.monotonic())
    server = _serving(HealthServer(0, consumer, _alive_thread(), max_staleness_seconds=60))
    try:
        status, _ = _get(server, "/other")
        assert status == 404
    finally:
        server.shutdown()
        server.server_close()
