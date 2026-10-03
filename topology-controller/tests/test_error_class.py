import socket

import botocore.exceptions
import pytest
import redis.exceptions

from adapters.redis.error_class import ErrorClass, PermanentMessageError, classify


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
