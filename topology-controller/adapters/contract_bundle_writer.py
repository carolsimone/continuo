"""Stores a release's contract bundle in S3 (see service/contract_bundle.py).

The object is the gzip stream of the canonical document, written with
Content-Type application/gzip and no Content-Encoding, so no HTTP client
decompresses it in transit: the bytes a reader downloads are the bytes whose
SHA-256 the parse result carries. The gzip header holds no timestamp and no
file name.

A release's bundle is written once. The document records how long each stage of
the parse took, so a second parse of the same release (a redelivered request, or
a delivery another replica reclaimed while its first handler still runs) would
produce other bytes, and an event already published carries the checksum of the
first. The writer reads the key first and, when the object exists, keeps it and
returns the reference of the bytes it holds. Otherwise it creates the object with
a conditional put (If-None-Match: *), which the store applies only while no
object is at the key. When another writer created it first, the store answers
412 PreconditionFailed, or 409 ConditionalRequestConflict while that writer's
own conditional put is still in progress; the writer then reads the object again
and returns the reference of the bytes stored there, trying the put again (after
a short wait) only while no object is there yet. Every writer of a release
therefore returns the checksum of the one object the store holds. Conflicts that
never settle, and every other failure of the put, reach the caller unchanged.
"""
import gzip
import time
from collections.abc import Callable, Mapping
from typing import Any

from botocore.exceptions import ClientError
from continuo_contracts.canonical import canonical_json
from continuo_contracts.hashing import sha256_hex

from domain.model import ContractBundleRef
from service.contract_bundle import contract_bundle_key

_ABSENT_CODES = {"404", "NoSuchKey", "NotFound"}

# The answers of a conditional put that another writer's object (stored, or
# being stored) refused: S3's 412 PreconditionFailed when the object exists, and
# 409 ConditionalRequestConflict when a concurrent conditional write to the key
# is in progress.
_TAKEN_CODES = {"PreconditionFailed", "ConditionalRequestConflict"}
_TAKEN_STATUSES = {409, 412}

# How many conditional puts one write sends, and the wait before the second
# (doubling after each refused put whose object is still not readable).
_MAX_PUTS = 5
_FIRST_WAIT_SECONDS = 0.2


class ContractBundleWriter:
    def __init__(self, s3_client, bucket: str, *, sleep: Callable[[float], None] = time.sleep) -> None:
        self._s3 = s3_client
        self._bucket = bucket
        self._sleep = sleep

    def write(self, *, tenant_id: str, release_id: str, document: Mapping[str, Any]) -> ContractBundleRef:
        key = contract_bundle_key(tenant_id, release_id)
        body = self._stored(key)
        if body is None:
            body = self._create(key, gzip.compress(canonical_json(document), mtime=0))
        return ContractBundleRef(uri=f"s3://{self._bucket}/{key}", sha256=sha256_hex(body))

    def _create(self, key: str, body: bytes) -> bytes:
        """Create the object at key with body unless another writer has, and
        return the bytes the store holds at key."""
        wait = _FIRST_WAIT_SECONDS
        puts = 0
        while True:
            puts += 1
            try:
                self._s3.put_object(
                    Bucket=self._bucket, Key=key, Body=body, ContentType="application/gzip", IfNoneMatch="*")
                return body
            except ClientError as exc:
                if not _taken(exc):
                    raise
                stored = self._stored(key)
                if stored is not None:
                    return stored
                if puts == _MAX_PUTS:
                    raise
            self._sleep(wait)
            wait *= 2

    def _stored(self, key: str) -> bytes | None:
        """The bytes of the object at key, or None when there is none. Any other
        failure of the read reaches the caller, which decides whether it is an
        outage to wait out."""
        try:
            return self._s3.get_object(Bucket=self._bucket, Key=key)["Body"].read()
        except ClientError as exc:
            if str(exc.response.get("Error", {}).get("Code", "")) in _ABSENT_CODES:
                return None
            raise


def _taken(exc: ClientError) -> bool:
    """Whether a conditional put was refused because another writer's object is
    at the key, or is being written there."""
    error = exc.response.get("Error", {})
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return str(error.get("Code", "")) in _TAKEN_CODES or status in _TAKEN_STATUSES
