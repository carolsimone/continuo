import socket
import urllib.error
import urllib.request

import pytest
from prometheus_client import CollectorRegistry, Counter

from adapters.metrics.server import MetricsServer, start_metrics_server


def _get(port, path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as resp:
            return resp.status, resp.headers["Content-Type"], resp.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, None, ""


@pytest.fixture
def server():
    registry = CollectorRegistry()
    Counter("continuo_test_total", "A counter for the listener test.", registry=registry).inc()
    srv = start_metrics_server(0, registry)
    yield srv
    srv.shutdown()
    srv.server_close()


def test_get_metrics_answers_in_the_prometheus_text_format(server):
    status, content_type, body = _get(server.port, "/metrics?debug=1")
    assert (status, content_type) == (200, "text/plain; version=0.0.4; charset=utf-8")
    assert "continuo_test_total 1.0" in body


def test_every_other_path_answers_404(server):
    assert [_get(server.port, path)[0] for path in ("/", "/health", "/metrics/x")] == [404, 404, 404]


def test_a_taken_port_refuses_to_start():
    with socket.socket() as held:
        held.bind(("0.0.0.0", 0))
        held.listen()
        with pytest.raises(OSError):
            MetricsServer(held.getsockname()[1], CollectorRegistry())
