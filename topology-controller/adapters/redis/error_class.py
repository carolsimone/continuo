"""How the release.requested consumer treats an exception from its handler."""

import enum
import socket

import boto3.exceptions
import botocore.exceptions
import redis.exceptions
import s3transfer.exceptions

from service.errors import PermanentMessageError


class ErrorClass(enum.Enum):
    PERMANENT = "permanent"
    INFRASTRUCTURE = "infrastructure"
    TRANSIENT = "transient"


# Failures that mean Redis or S3 could not be reached, or that the connection
# to S3 was lost or timed out part-way through a response. They never count
# toward the delivery limit: the consumer waits and retries the same message.
# The botocore pair IncompleteReadError (the body ended before its
# Content-Length) and ResponseStreamingError (the connection broke while the
# body was being read) is how a download cut short by S3 surfaces, with
# ReadTimeoutError and the built-in connection and timeout errors: together they
# are the failures an S3 transfer retries (s3transfer's
# S3_RETRYABLE_DOWNLOAD_ERRORS). A bare OSError is not here: a failed local file
# write is one too.
_INFRASTRUCTURE = (
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
    botocore.exceptions.EndpointConnectionError,
    botocore.exceptions.ConnectTimeoutError,
    botocore.exceptions.ReadTimeoutError,
    botocore.exceptions.ConnectionClosedError,
    botocore.exceptions.IncompleteReadError,
    botocore.exceptions.ResponseStreamingError,
    ConnectionError,
    TimeoutError,
    socket.gaierror,
)


# Raised by an S3 managed transfer (download_file) once its own retries are
# spent, with the failure of the last attempt in last_exception. boto3 raises its
# own wrapper over the one s3transfer raises underneath it.
_TRANSFER_RETRY_WRAPPERS = (
    boto3.exceptions.RetriesExceededError,
    s3transfer.exceptions.RetriesExceededError,
)

# How many wrappers deep classify looks for the failure inside them. A transfer
# raises one or two; the bound keeps a wrapper that carries itself from looping.
_MAX_UNWRAP_DEPTH = 8


def _underlying_failure(exc: BaseException) -> BaseException:
    """The failure a transfer-retry wrapper carries, looking through wrappers
    around wrappers. A wrapper that carries no exception, or one still nested
    past _MAX_UNWRAP_DEPTH, is returned as it is. Only these wrappers are opened;
    the __cause__ and __context__ of any exception are not followed."""
    for _ in range(_MAX_UNWRAP_DEPTH):
        if not isinstance(exc, _TRANSFER_RETRY_WRAPPERS):
            break
        carried = exc.last_exception
        if not isinstance(carried, BaseException):
            break
        exc = carried
    return exc


def classify(exc: BaseException) -> ErrorClass:
    """The class of a handler failure. A transfer-retry wrapper takes the class
    of the failure it carries: S3 dropping a download's connection is an outage
    to wait out, however many times the transfer already retried it."""
    exc = _underlying_failure(exc)
    if isinstance(exc, PermanentMessageError):
        return ErrorClass.PERMANENT
    if isinstance(exc, _INFRASTRUCTURE):
        return ErrorClass.INFRASTRUCTURE
    if isinstance(exc, botocore.exceptions.ClientError):
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        if status >= 500:
            return ErrorClass.INFRASTRUCTURE
    return ErrorClass.TRANSIENT


def is_infrastructure(exc: BaseException) -> bool:
    """Whether exc is an outage the consumer waits out. The candidate handler
    re-raises an S3 write failure this accepts instead of rejecting the
    release, so the consumer pauses and redelivers the same message."""
    return classify(exc) is ErrorClass.INFRASTRUCTURE
