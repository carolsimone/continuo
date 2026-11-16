"""Stores a release's contract bundle in S3 (see service/contract_bundle.py).

The object is the gzip stream of the canonical document, written with
Content-Type application/gzip and no Content-Encoding, so no HTTP client
decompresses it in transit: the bytes a reader downloads are the bytes whose
SHA-256 the parse result carries. The gzip header holds no timestamp and no
file name.

A release's bundle is written once. The document records how long each stage of
the parse took, so a second parse of the same release (a redelivered request)
would produce other bytes, and an event already published carries the checksum
of the first. When the object exists, the writer keeps it and returns the
reference of the bytes it holds.
"""
import gzip
from collections.abc import Mapping
from typing import Any

from botocore.exceptions import ClientError
from continuo_contracts.canonical import canonical_json
from continuo_contracts.hashing import sha256_hex

from domain.model import ContractBundleRef
from service.contract_bundle import contract_bundle_key

_ABSENT_CODES = {"404", "NoSuchKey", "NotFound"}


class ContractBundleWriter:
    def __init__(self, s3_client, bucket: str) -> None:
        self._s3 = s3_client
        self._bucket = bucket

    def write(self, *, tenant_id: str, release_id: str, document: Mapping[str, Any]) -> ContractBundleRef:
        key = contract_bundle_key(tenant_id, release_id)
        body = self._stored(key)
        if body is None:
            body = gzip.compress(canonical_json(document), mtime=0)
            self._s3.put_object(Bucket=self._bucket, Key=key, Body=body, ContentType="application/gzip")
        return ContractBundleRef(uri=f"s3://{self._bucket}/{key}", sha256=sha256_hex(body))

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
