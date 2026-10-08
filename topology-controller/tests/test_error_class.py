import http.server
import socket
import threading

import boto3
import boto3.exceptions
import botocore.exceptions
import pytest
import redis.exceptions
import s3transfer.exceptions
from botocore.config import Config

from adapters.redis.error_class import ErrorClass, classify
from service.errors import PermanentMessageError


def _client_error(status):
    return botocore.exceptions.ClientError(
        {"Error": {"Code": "X"}, "ResponseMetadata": {"HTTPStatusCode": status}}, "GetObject")


@pytest.mark.parametrize("exc, want", [
    (PermanentMessageError("no payload"), ErrorClass.PERMANENT),
    (redis.exceptions.ConnectionError("refused"), ErrorClass.INFRASTRUCTURE),
    (redis.exceptions.TimeoutError("slow"), ErrorClass.INFRASTRUCTURE),
    (botocore.exceptions.EndpointConnectionError(endpoint_url="http://minio:9000"), ErrorClass.INFRASTRUCTURE),
    (ConnectionRefusedError(), ErrorClass.INFRASTRUCTURE),
    (socket.gaierror(), ErrorClass.INFRASTRUCTURE),
    (_client_error(503), ErrorClass.INFRASTRUCTURE),
    (botocore.exceptions.IncompleteReadError(actual_bytes=10, expected_bytes=100), ErrorClass.INFRASTRUCTURE),
    (botocore.exceptions.ResponseStreamingError(error="Connection broken"), ErrorClass.INFRASTRUCTURE),
    (_client_error(404), ErrorClass.TRANSIENT),
    (ValueError("library bug"), ErrorClass.TRANSIENT),
    (RuntimeError("boom"), ErrorClass.TRANSIENT),
    # An OSError is not a lost connection: only its ConnectionError and
    # TimeoutError subclasses are.
    (OSError("disk full"), ErrorClass.TRANSIENT),
    (FileNotFoundError("/tmp/manifest.json"), ErrorClass.TRANSIENT),
])
def test_classify(exc, want):
    assert classify(exc) is want


_ENDPOINT = "http://minio:9000"

# S3Source.list_manifests calls s3_client.download_file, which boto3 runs through
# S3Transfer.download_file; that raises boto3's RetriesExceededError once the
# transfer's own retries are spent, with the failure of the last attempt in
# last_exception. s3transfer's own wrapper is what the transfer manager raises
# underneath it.
_TRANSFER_WRAPPERS = [boto3.exceptions.RetriesExceededError, s3transfer.exceptions.RetriesExceededError]


@pytest.mark.parametrize("wrapper", _TRANSFER_WRAPPERS, ids=["boto3", "s3transfer"])
@pytest.mark.parametrize("underlying, want", [
    (botocore.exceptions.ConnectionClosedError(endpoint_url=_ENDPOINT), ErrorClass.INFRASTRUCTURE),
    (botocore.exceptions.ReadTimeoutError(endpoint_url=_ENDPOINT), ErrorClass.INFRASTRUCTURE),
    (botocore.exceptions.EndpointConnectionError(endpoint_url=_ENDPOINT), ErrorClass.INFRASTRUCTURE),
    (botocore.exceptions.IncompleteReadError(actual_bytes=10, expected_bytes=100), ErrorClass.INFRASTRUCTURE),
    (botocore.exceptions.ResponseStreamingError(error="Connection broken"), ErrorClass.INFRASTRUCTURE),
    (ConnectionResetError(), ErrorClass.INFRASTRUCTURE),
    (socket.timeout(), ErrorClass.INFRASTRUCTURE),
    (_client_error(503), ErrorClass.INFRASTRUCTURE),
    (_client_error(404), ErrorClass.TRANSIENT),
    (ValueError("library bug"), ErrorClass.TRANSIENT),
    (FileNotFoundError("/tmp/manifest.json"), ErrorClass.TRANSIENT),
    (None, ErrorClass.TRANSIENT),
], ids=["connection-closed", "read-timeout", "endpoint-unreachable", "incomplete-read", "response-streaming",
        "connection-reset", "socket-timeout", "http-503", "http-404", "value-error", "file-not-found",
        "no-last-exception"])
def test_classify_looks_through_a_transfer_retry_wrapper(wrapper, underlying, want):
    """The wrapper only says the transfer's retries ran out; the failure it
    carries decides whether S3 was unreachable."""
    assert classify(wrapper(underlying)) is want


def test_classify_looks_through_nested_transfer_retry_wrappers():
    inner = botocore.exceptions.ConnectionClosedError(endpoint_url=_ENDPOINT)
    nested = boto3.exceptions.RetriesExceededError(s3transfer.exceptions.RetriesExceededError(inner))
    assert classify(nested) is ErrorClass.INFRASTRUCTURE


def test_classify_gives_up_unwrapping_a_wrapper_that_wraps_itself():
    loop = boto3.exceptions.RetriesExceededError(None)
    loop.last_exception = loop
    assert classify(loop) is ErrorClass.TRANSIENT


def test_classify_does_not_follow_the_cause_of_an_ordinary_exception():
    """Only the transfer wrappers are unwrapped: a RuntimeError raised from a
    connection failure is still a RuntimeError."""
    try:
        try:
            raise botocore.exceptions.ConnectionClosedError(endpoint_url=_ENDPOINT)
        except botocore.exceptions.ConnectionClosedError as cause:
            raise RuntimeError("download failed") from cause
    except RuntimeError as exc:
        assert classify(exc) is ErrorClass.TRANSIENT


class _TruncatingS3(http.server.BaseHTTPRequestHandler):
    """Answers HeadObject normally and every GetObject with a Content-Length
    larger than the body it sends before it closes the connection."""

    protocol_version = "HTTP/1.1"
    object_size = 100
    sent_bytes = 10

    def do_HEAD(self):  # noqa: N802 — the http.server handler API
        self.send_response(200)
        self.send_header("Content-Length", str(self.object_size))
        self.send_header("ETag", '"abc"')
        self.end_headers()

    def do_GET(self):  # noqa: N802
        self.send_response(200)
        self.send_header("Content-Length", str(self.object_size))
        self.send_header("ETag", '"abc"')
        self.end_headers()
        self.wfile.write(b"x" * self.sent_bytes)
        self.wfile.flush()
        self.close_connection = True

    def log_message(self, *_args):
        pass


def test_a_download_whose_stream_is_cut_short_is_infrastructure(tmp_path, monkeypatch):
    """The real path of S3Source.list_manifests: boto3's download_file against an
    endpoint that closes the connection before the body is complete. The
    transfer retries, runs out of attempts and raises its RetriesExceededError;
    the lost connection inside it makes the failure infrastructure."""
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _TruncatingS3)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    try:
        s3 = boto3.client(
            "s3", endpoint_url=f"http://127.0.0.1:{server.server_port}", region_name="us-east-1",
            aws_access_key_id="test", aws_secret_access_key="test",
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 0}, read_timeout=5),
        )
        with pytest.raises(boto3.exceptions.RetriesExceededError) as raised:
            s3.download_file("bucket", "manifest.json", str(tmp_path / "manifest.json"))
    finally:
        server.shutdown()
        server.server_close()
    assert isinstance(raised.value.last_exception, (
        botocore.exceptions.IncompleteReadError, botocore.exceptions.ResponseStreamingError,
    )), raised.value.last_exception
    assert classify(raised.value) is ErrorClass.INFRASTRUCTURE


@pytest.mark.parametrize("exc, want", [
    (botocore.exceptions.EndpointConnectionError(endpoint_url="http://minio:9000"), True),
    (_client_error(503), True),
    (_client_error(403), False),
    (RuntimeError("boom"), False),
    (PermanentMessageError("no payload"), False),
])
def test_is_infrastructure_is_classify_says_infrastructure(exc, want):
    """The candidate handler re-raises a write failure is_infrastructure accepts,
    so the consumer waits the outage out instead of the release being rejected."""
    from adapters.redis.error_class import is_infrastructure
    assert is_infrastructure(exc) is want
