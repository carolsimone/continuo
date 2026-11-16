"""The contract bundle writer: one gzipped canonical document per release, written once."""
import gzip
import hashlib
import io

import botocore.exceptions
import pytest
from continuo_contracts.canonical import canonical_json

from adapters.contract_bundle_writer import ContractBundleWriter
from domain.model import ContractBundleRef
from service.contract_bundle import contract_bundle_key

KEY = contract_bundle_key("default", "r1")


def _client_error(code: str, status: int, operation: str) -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, operation)


class FakeS3:
    """The two calls the writer makes, over a dict. A key that is not there answers
    the way S3 does: a ClientError whose code is NoSuchKey."""

    def __init__(self, *, get_error: Exception | None = None, put_error: Exception | None = None) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.puts: list[dict] = []
        self.gets: list[dict] = []
        self._get_error = get_error
        self._put_error = put_error

    def get(self, bucket: str, key: str) -> bytes:
        return self.objects[(bucket, key)]

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        self.gets.append({"Bucket": Bucket, "Key": Key})
        if self._get_error is not None:
            raise self._get_error
        if (Bucket, Key) not in self.objects:
            raise _client_error("NoSuchKey", 404, "GetObject")
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **kwargs) -> dict:
        if self._put_error is not None:
            raise self._put_error
        self.puts.append({"Bucket": Bucket, "Key": Key, "Body": Body, **kwargs})
        self.objects[(Bucket, Key)] = Body
        return {}


@pytest.fixture
def fake_s3() -> FakeS3:
    return FakeS3()


def test_the_writer_stores_the_gzipped_canonical_document_and_returns_its_reference(fake_s3):
    document = {"b": [1, 2], "a": {"z": None, "y": "é"}}

    ref = ContractBundleWriter(fake_s3, "b").write(tenant_id="default", release_id="r1", document=document)

    [put] = fake_s3.puts
    assert (put["Bucket"], put["Key"], put["ContentType"]) == ("b", KEY, "application/gzip")
    assert "ContentEncoding" not in put, "a Content-Encoding header would let an HTTP client gunzip it in transit"
    assert put["Body"] == gzip.compress(canonical_json(document), mtime=0)
    assert gzip.decompress(put["Body"]) == canonical_json(document)
    assert ref == ContractBundleRef(
        uri=f"s3://b/{KEY}", sha256=hashlib.sha256(put["Body"]).hexdigest())


def test_the_gzip_header_carries_no_timestamp_and_no_file_name(fake_s3):
    ContractBundleWriter(fake_s3, "b").write(tenant_id="default", release_id="r1", document={})

    body = fake_s3.puts[0]["Body"]
    assert body[:2] == b"\x1f\x8b"
    assert body[4:8] == b"\x00\x00\x00\x00", "MTIME must be zero"
    assert body[3] & 0x08 == 0, "FNAME must not be set"


def test_a_second_write_of_the_same_release_keeps_the_first_bytes(fake_s3):
    w = ContractBundleWriter(fake_s3, "b")
    first = w.write(tenant_id="default", release_id="r1", document={"a": 1})
    second = w.write(tenant_id="default", release_id="r1", document={"a": 2})
    assert second == first and gzip.decompress(fake_s3.get("b", contract_bundle_key("default", "r1"))) == b'{"a":1}'
    assert len(fake_s3.puts) == 1


def test_the_reference_of_an_existing_object_carries_the_checksum_of_the_stored_bytes(fake_s3):
    stored = gzip.compress(b'{"written":"by another pod"}', mtime=5)
    fake_s3.objects[("b", KEY)] = stored

    ref = ContractBundleWriter(fake_s3, "b").write(tenant_id="default", release_id="r1", document={"a": 1})

    assert ref == ContractBundleRef(uri=f"s3://b/{KEY}", sha256=hashlib.sha256(stored).hexdigest())
    assert fake_s3.puts == []


def test_another_release_is_written_even_when_one_exists(fake_s3):
    w = ContractBundleWriter(fake_s3, "b")
    w.write(tenant_id="default", release_id="r1", document={"a": 1})

    other = w.write(tenant_id="default", release_id="r2", document={"a": 2})

    assert other.uri == f"s3://b/{contract_bundle_key('default', 'r2')}"
    assert len(fake_s3.puts) == 2


@pytest.mark.parametrize(
    ("code", "status"), [("NoSuchKey", 404), ("404", 404), ("NotFound", 404)], ids=["no-such-key", "404", "not-found"])
def test_every_answer_for_a_missing_object_means_write_it(code, status):
    s3 = FakeS3(get_error=_client_error(code, status, "GetObject"))

    ContractBundleWriter(s3, "b").write(tenant_id="default", release_id="r1", document={})

    assert len(s3.puts) == 1


@pytest.mark.parametrize(
    "error",
    [
        botocore.exceptions.EndpointConnectionError(endpoint_url="http://minio:9000"),
        _client_error("SlowDown", 503, "GetObject"),
        _client_error("AccessDenied", 403, "GetObject"),
    ],
    ids=["unreachable", "503", "403"],
)
def test_a_read_failure_other_than_a_missing_object_reaches_the_caller_and_writes_nothing(error):
    s3 = FakeS3(get_error=error)

    with pytest.raises(type(error)) as raised:
        ContractBundleWriter(s3, "b").write(tenant_id="default", release_id="r1", document={})

    assert raised.value is error
    assert s3.puts == []


def test_a_put_failure_reaches_the_caller():
    s3 = FakeS3(put_error=RuntimeError("bucket gone"))

    with pytest.raises(RuntimeError, match="bucket gone"):
        ContractBundleWriter(s3, "b").write(tenant_id="default", release_id="r1", document={})
