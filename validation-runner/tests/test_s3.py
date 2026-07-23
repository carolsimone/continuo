"""Unit tests for the s3 module — no boto3 network calls."""
import pytest

from continuo_validation_runner import s3


def test_parse_s3_uri_splits_bucket_and_key():
    """A well-formed s3:// URI splits into its bucket and key parts."""
    assert s3.parse_s3_uri("s3://bucket/a/b.sql") == ("bucket", "a/b.sql")


@pytest.mark.parametrize("uri", ["http://x/y", "s3://bucket-only", "s3://bucket/", "s3://"])
def test_parse_s3_uri_rejects_invalid(uri):
    """Non-s3 schemes, and URIs missing a bucket or key, raise ValueError."""
    with pytest.raises(ValueError):
        s3.parse_s3_uri(uri)


def test_make_s3_client_passes_only_endpoint_when_set(monkeypatch):
    """Only S3_ENDPOINT_URL is forwarded; credentials are left to boto3's chain."""
    captured = {}

    def _fake_client(service, **kwargs):
        captured["service"] = service
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(s3.boto3, "client", _fake_client)
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    s3.make_s3_client()
    assert captured["service"] == "s3"
    assert captured["kwargs"] == {"endpoint_url": "http://minio:9000"}


def test_make_s3_client_no_endpoint_passes_no_kwargs(monkeypatch):
    """With no S3_ENDPOINT_URL, boto3.client is called with no extra kwargs."""
    captured = {}
    monkeypatch.setattr(s3.boto3, "client", lambda service, **kw: captured.update(kw) or object())
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    s3.make_s3_client()
    assert captured == {}
