"""Unit tests for compile_uploader.main.

No S3 access required — boto3 is patched with a MagicMock.
"""
import os
from unittest import mock

import pytest

import compile_uploader  # pythonpath="s3-sidecar" in pyproject
import s3_common


def test_uploads_file_to_parsed_key(tmp_path, monkeypatch):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"nodes":{}}')
    monkeypatch.setenv("COMPILE_MANIFEST_PATH", str(manifest))
    monkeypatch.setenv("MANIFEST_S3_URI", "s3://continuo/svc/rel-1/manifest.json")
    fake = mock.MagicMock()
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)

    compile_uploader.main()

    fake.put_object.assert_called_once()
    _, kwargs = fake.put_object.call_args
    assert kwargs["Bucket"] == "continuo"
    assert kwargs["Key"] == "svc/rel-1/manifest.json"
    assert kwargs["Body"] == b'{"nodes":{}}'


def test_missing_manifest_file_exits_nonzero(monkeypatch):
    monkeypatch.setenv("COMPILE_MANIFEST_PATH", "/nope/manifest.json")
    monkeypatch.setenv("MANIFEST_S3_URI", "s3://continuo/svc/rel-1/manifest.json")
    with pytest.raises(SystemExit) as e:
        compile_uploader.main()
    assert e.value.code != 0


def test_missing_env_exits_nonzero(monkeypatch):
    monkeypatch.delenv("COMPILE_MANIFEST_PATH", raising=False)
    with pytest.raises(SystemExit) as e:
        compile_uploader.main()
    assert e.value.code != 0


def test_malformed_manifest_s3_uri_exits_nonzero(tmp_path, monkeypatch):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"nodes":{}}')
    monkeypatch.setenv("COMPILE_MANIFEST_PATH", str(manifest))
    monkeypatch.setenv("MANIFEST_S3_URI", "not-an-s3-uri")
    with pytest.raises(SystemExit) as e:
        compile_uploader.main()
    assert e.value.code != 0


def test_put_object_failure_exits_nonzero(tmp_path, monkeypatch):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"nodes":{}}')
    monkeypatch.setenv("COMPILE_MANIFEST_PATH", str(manifest))
    monkeypatch.setenv("MANIFEST_S3_URI", "s3://continuo/svc/rel-1/manifest.json")
    fake = mock.MagicMock()
    fake.put_object.side_effect = RuntimeError("connection refused")
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)

    with pytest.raises(SystemExit) as e:
        compile_uploader.main()
    assert e.value.code != 0
