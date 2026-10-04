import socket

import boto3.exceptions
import botocore.exceptions
import pytest
import redis.exceptions
import s3transfer.exceptions

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
    (_client_error(404), ErrorClass.TRANSIENT),
    (ValueError("library bug"), ErrorClass.TRANSIENT),
    (RuntimeError("boom"), ErrorClass.TRANSIENT),
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
    (_client_error(503), ErrorClass.INFRASTRUCTURE),
    (_client_error(404), ErrorClass.TRANSIENT),
    (ValueError("library bug"), ErrorClass.TRANSIENT),
    (None, ErrorClass.TRANSIENT),
], ids=["connection-closed", "read-timeout", "endpoint-unreachable", "http-503", "http-404",
        "value-error", "no-last-exception"])
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
