import pytest
from adapters.sources.s3_uri import parse_s3_uri


def test_parse_s3_uri_basic():
    assert parse_s3_uri("s3://continuo/releases/abc/manifests/") == ("continuo", "releases/abc/manifests/")

def test_parse_s3_uri_appends_trailing_slash():
    assert parse_s3_uri("s3://continuo/releases/abc/manifests") == ("continuo", "releases/abc/manifests/")

def test_parse_s3_uri_root_prefix():
    assert parse_s3_uri("s3://continuo/") == ("continuo", "")

def test_parse_s3_uri_rejects_non_s3_scheme():
    with pytest.raises(ValueError, match="must start with s3://"):
        parse_s3_uri("https://example.com/foo")

def test_parse_s3_uri_rejects_missing_bucket():
    with pytest.raises(ValueError, match="missing bucket"):
        parse_s3_uri("s3://")
