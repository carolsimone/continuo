import logging
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_PLAIN_0_0_4, CollectorRegistry, generate_latest

logger = logging.getLogger(__name__)

METRICS_PATH = "/metrics"


class _Handler(BaseHTTPRequestHandler):
    # BaseHTTPRequestHandler writes a plain-text line to stderr for every
    # request by default; the process logs one JSON object per line, and a
    # scraper polls this path every few seconds, so leaving this on would
    # drown out real log lines with scrape traffic.
    def log_message(self, format: str, *args) -> None:  # noqa: A002
        pass

    def do_GET(self) -> None:
        # The query string is ignored: a scraper may append parameters such as
        # ?debug=1 or ?name[]=..., and only the path names the resource.
        if urllib.parse.urlsplit(self.path).path != METRICS_PATH:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body = generate_latest(self.server.registry)  # type: ignore[attr-defined]
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE_PLAIN_0_0_4)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class MetricsServer(ThreadingHTTPServer):
    """Serves one CollectorRegistry as GET /metrics in the Prometheus text format.

    The socket is bound in the constructor, so a port another process holds
    raises OSError there and the caller can refuse to start. Every other path
    answers 404, and the health probes stay on their own port (HealthServer).
    """

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, port: int, registry: CollectorRegistry) -> None:
        super().__init__(("0.0.0.0", port), _Handler)
        self.registry = registry

    @property
    def port(self) -> int:
        """The bound port, which is the one the OS picked when 0 was asked for."""
        return self.server_address[1]


def start_metrics_server(port: int, registry: CollectorRegistry) -> MetricsServer:
    """Bind the port, serve it on a daemon thread and return the server (for tests).

    Raises OSError when the port is taken.
    """
    server = MetricsServer(port, registry)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="metrics-server")
    thread.start()
    logger.info("Metrics listening", extra={"port": server.port})
    return server
