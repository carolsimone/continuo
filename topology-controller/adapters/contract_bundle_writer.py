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
never settle, and every other failure of the put, reach the caller unchanged: the
consumer waits out an unsettled 409 like an outage (adapters/redis/error_class.py),
so the redelivered message's read finds the other writer's object.

The reference also carries how many contracts the stored bundle holds by family:
the document's own `summary.by_family` when the store holds this writer's bytes,
and otherwise the `summary.by_family` of the bytes already stored, which another
parse of the release wrote (it may be another version of topology-controller, with
other contracts). Those bytes are already in memory from the read; they are
inflated a piece at a time and only their end is kept, since keys sort in canonical
JSON and `summary` comes after the large `contracts` (only `tenant_id` and `timings`
follow it). The last `"summary":` key there is decoded, and accepted only when what
follows it closes the document. Any other layout is read as a whole. A stored bundle
that is not gzip, whose JSON cannot be decoded, or that holds no `summary.by_family`
of counts raises UnreadableContractBundleError.
"""
import gzip
import io
import json
import time
import zlib
from collections.abc import Callable, Mapping
from typing import Any

from botocore.exceptions import ClientError
from continuo_contracts.canonical import canonical_json
from continuo_contracts.hashing import sha256_hex

from domain.model import ContractBundleRef
from service.contract_bundle import contract_bundle_key
from service.errors import UnreadableContractBundleError

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

# How much of a stored bundle's JSON is inflated at a time, and how much of its
# end is kept to find its summary in: far more than the summary, `tenant_id` and
# `timings` that end the document.
_PIECE_BYTES = 1 << 20
_END_BYTES = 1 << 20
_SUMMARY_KEY = b'"summary":'
_DECODER = json.JSONDecoder()


class ContractBundleWriter:
    def __init__(self, s3_client, bucket: str, *, sleep: Callable[[float], None] = time.sleep) -> None:
        self._s3 = s3_client
        self._bucket = bucket
        self._sleep = sleep

    def write(self, *, tenant_id: str, release_id: str, document: Mapping[str, Any]) -> ContractBundleRef:
        key = contract_bundle_key(tenant_id, release_id)
        uri = f"s3://{self._bucket}/{key}"
        stored = self._stored(key)
        if stored is None:
            body = gzip.compress(canonical_json(document), mtime=0)
            stored = self._create(key, body)
            if stored == body:
                by_family = dict(document["summary"]["by_family"])
                return ContractBundleRef(uri=uri, sha256=sha256_hex(body), by_family=by_family)
        return ContractBundleRef(uri=uri, sha256=sha256_hex(stored), by_family=_stored_by_family(uri, stored))

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


def _stored_by_family(uri: str, stored: bytes) -> dict[str, int]:
    """The `summary.by_family` of the bundle whose gzip bytes are `stored`."""
    try:
        summary = _summary_at_the_end(_inflated_end(stored))
        if summary is None:
            document = json.loads(gzip.decompress(stored))
            summary = document.get("summary") if isinstance(document, dict) else None
    except (OSError, EOFError, zlib.error, ValueError, RecursionError) as exc:
        raise UnreadableContractBundleError(f"the contract bundle {uri} is stored and cannot be read: {exc}") from exc
    by_family = summary.get("by_family") if isinstance(summary, dict) else None
    if not isinstance(by_family, dict) or not all(
            isinstance(family, str) and type(count) is int and count >= 0 for family, count in by_family.items()):
        raise UnreadableContractBundleError(
            f"the contract bundle {uri} is stored and holds no summary.by_family of counts")
    return dict(by_family)


def _inflated_end(stored: bytes) -> bytes:
    """The last _END_BYTES of what the gzip bytes `stored` inflate to, inflated a
    piece at a time so the whole JSON is never held."""
    end = b""
    with gzip.GzipFile(fileobj=io.BytesIO(stored)) as inflated:
        while piece := inflated.read(_PIECE_BYTES):
            end = (end + piece)[-_END_BYTES:]
    return end


def _summary_at_the_end(end: bytes) -> object | None:
    """The value of the top-level `summary` key of a canonical bundle whose last
    bytes are `end`, or None when they do not end the document that way.

    In canonical JSON a key is written `"<key>":` with no space, and a quote inside
    a string is escaped, so `"summary":` after a `{` or a `,` is a key named
    `summary`. The last one is the top-level key when the members after it (the
    document's `tenant_id` and `timings`) close the document: a key nested in a
    value is followed by the bracket that closes its own container first."""
    at = end.rfind(_SUMMARY_KEY)
    if at < 1 or end[at - 1:at] not in (b"{", b","):
        return None
    try:
        text = end[at + len(_SUMMARY_KEY):].decode("utf-8")
        summary, position = _DECODER.raw_decode(text)
        while text[position:position + 1] == ",":
            key, position = _DECODER.raw_decode(text, position + 1)
            if not isinstance(key, str) or text[position:position + 1] != ":":
                return None
            _, position = _DECODER.raw_decode(text, position + 1)
    except (UnicodeDecodeError, ValueError, RecursionError):
        return None
    return summary if text[position:] == "}" else None


def _taken(exc: ClientError) -> bool:
    """Whether a conditional put was refused because another writer's object is
    at the key, or is being written there."""
    error = exc.response.get("Error", {})
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return str(error.get("Code", "")) in _TAKEN_CODES or status in _TAKEN_STATUSES
