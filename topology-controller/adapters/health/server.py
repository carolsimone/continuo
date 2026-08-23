import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logger = logging.getLogger(__name__)

# How stale Consumer.last_heartbeat may get before the health server reports
# unhealthy. The loop's own retry cadence during an outage is a 3s sleep per
# failed pass (adapters/redis/consumer.py), so this leaves a wide margin over
# any single slow pass while still catching a genuinely stuck/dead consumer
# well within a Kubernetes probe's failureThreshold window.
DEFAULT_MAX_STALENESS_SECONDS = 60.0


class _Handler(BaseHTTPRequestHandler):
    # BaseHTTPRequestHandler logs every request to stderr by default; the
    # process log format is plain `%(message)s`, and Kubernetes polls these
    # paths every few seconds, so leaving this on would drown out real
    # consumer log lines with probe traffic.
    def log_message(self, format: str, *args) -> None:  # noqa: A002
        pass

    def _respond(self) -> None:
        healthy, detail = self.server.health_check()  # type: ignore[attr-defined]
        body = detail.encode()
        self.send_response(200 if healthy else 503)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path in ("/health", "/ready"):
            self._respond()
        else:
            self.send_response(404)
            self.end_headers()


class HealthServer(ThreadingHTTPServer):
    """Liveness/readiness HTTP server backed by a Consumer's heartbeat.

    Both `/health` and `/ready` report the same check here: is the consumer
    thread alive, and has it completed a loop pass recently? This differs
    deliberately from the Go services' split model (a liveness endpoint that
    is hardcoded 200, plus a heartbeat/registry-aware readiness endpoint):
    topology-controller serves no application traffic through this port, so
    there's no reason to keep liveness blind to a stuck consumer. The whole
    point of adding this server is for Kubernetes to *restart* the pod when
    the consumer stops making progress, and only a failing livenessProbe
    triggers a restart — a failing readinessProbe alone would just pull an
    (unused) Service endpoint, leaving the same silently-dead pod running.
    """

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        port: int,
        consumer,
        consumer_thread: threading.Thread,
        max_staleness_seconds: float = DEFAULT_MAX_STALENESS_SECONDS,
    ) -> None:
        super().__init__(("0.0.0.0", port), _Handler)
        self._consumer = consumer
        self._consumer_thread = consumer_thread
        self._max_staleness = max_staleness_seconds

    def health_check(self) -> tuple[bool, str]:
        if not self._consumer_thread.is_alive():
            return False, "NOT READY: consumer thread is not running"
        age = time.monotonic() - self._consumer.last_heartbeat
        if age > self._max_staleness:
            return False, f"NOT READY: consumer heartbeat stale ({age:.1f}s)"
        return True, "OK"


def start_health_server(
    port: int,
    consumer,
    consumer_thread: threading.Thread,
    max_staleness_seconds: float = DEFAULT_MAX_STALENESS_SECONDS,
) -> HealthServer:
    """Start the health server on a daemon thread and return it (for tests)."""
    server = HealthServer(port, consumer, consumer_thread, max_staleness_seconds)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="health-server")
    thread.start()
    logger.info("Health server starting", extra={"port": port})
    return server
