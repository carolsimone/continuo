"""How the release.requested consumer treats an exception from its handler."""

import enum
import socket

import botocore.exceptions
import redis.exceptions


class PermanentMessageError(ValueError):
    """A message no redelivery can process, such as a missing or malformed
    payload. The consumer dead-letters it at once."""


class ErrorClass(enum.Enum):
    PERMANENT = "permanent"
    INFRASTRUCTURE = "infrastructure"
    TRANSIENT = "transient"


# Failures that mean Redis or S3 could not be reached. They never count toward
# the delivery limit: the consumer waits and retries the same message.
_INFRASTRUCTURE = (
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
    botocore.exceptions.EndpointConnectionError,
    botocore.exceptions.ConnectTimeoutError,
    botocore.exceptions.ReadTimeoutError,
    botocore.exceptions.ConnectionClosedError,
    ConnectionError,
    TimeoutError,
    socket.gaierror,
)


def classify(exc: BaseException) -> ErrorClass:
    if isinstance(exc, PermanentMessageError):
        return ErrorClass.PERMANENT
    if isinstance(exc, _INFRASTRUCTURE):
        return ErrorClass.INFRASTRUCTURE
    if isinstance(exc, botocore.exceptions.ClientError):
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        if status >= 500:
            return ErrorClass.INFRASTRUCTURE
    return ErrorClass.TRANSIENT
