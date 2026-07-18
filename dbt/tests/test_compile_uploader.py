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


def test_uploads_parse_artifacts_when_envs_set(tmp_path, monkeypatch):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"nodes":{}}')
    prod = tmp_path / "pp_prod"
    prod.write_bytes(b"PROD")
    cand = tmp_path / "pp_cand"
    cand.write_bytes(b"CAND")
    monkeypatch.setenv("COMPILE_MANIFEST_PATH", str(manifest))
    monkeypatch.setenv("MANIFEST_S3_URI", "s3://continuo/svc/rel-1/manifest.json")
    monkeypatch.setenv("PARSE_PROD_LOCAL_PATH", str(prod))
    monkeypatch.setenv("PARSE_PROD_S3_URI", "s3://continuo/svc/parse-cache/team-x/partial_parse.msgpack")
    monkeypatch.setenv("PARSE_CANDIDATE_LOCAL_PATH", str(cand))
    monkeypatch.setenv("PARSE_CANDIDATE_S3_URI", "s3://continuo/svc/rel-1/partial_parse.candidate.msgpack")
    fake = mock.MagicMock()
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)

    compile_uploader.main()

    assert fake.put_object.call_count == 3
    _, manifest_kwargs = fake.put_object.call_args_list[0]
    assert manifest_kwargs["Key"] == "svc/rel-1/manifest.json"
    _, prod_kwargs = fake.put_object.call_args_list[1]
    assert prod_kwargs["Bucket"] == "continuo"
    assert prod_kwargs["Key"] == "svc/parse-cache/team-x/partial_parse.msgpack"
    assert prod_kwargs["Body"] == b"PROD"
    _, cand_kwargs = fake.put_object.call_args_list[2]
    assert cand_kwargs["Key"] == "svc/rel-1/partial_parse.candidate.msgpack"
    assert cand_kwargs["Body"] == b"CAND"


def test_skips_parse_artifacts_when_envs_absent(tmp_path, monkeypatch):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"nodes":{}}')
    monkeypatch.setenv("COMPILE_MANIFEST_PATH", str(manifest))
    monkeypatch.setenv("MANIFEST_S3_URI", "s3://continuo/svc/rel-1/manifest.json")
    monkeypatch.delenv("PARSE_PROD_S3_URI", raising=False)
    monkeypatch.delenv("PARSE_CANDIDATE_S3_URI", raising=False)
    fake = mock.MagicMock()
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)

    compile_uploader.main()

    assert fake.put_object.call_count == 1


def test_parse_artifact_missing_local_file_exits_3(tmp_path, monkeypatch):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"nodes":{}}')
    monkeypatch.setenv("COMPILE_MANIFEST_PATH", str(manifest))
    monkeypatch.setenv("MANIFEST_S3_URI", "s3://continuo/svc/rel-1/manifest.json")
    monkeypatch.setenv("PARSE_PROD_LOCAL_PATH", str(tmp_path / "does-not-exist"))
    monkeypatch.setenv("PARSE_PROD_S3_URI", "s3://continuo/svc/parse-cache/team-x/partial_parse.msgpack")
    fake = mock.MagicMock()
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)

    with pytest.raises(SystemExit) as e:
        compile_uploader.main()
    assert e.value.code == 3
