"""Unit tests for compile_uploader.main.

No S3 access required — boto3 is patched with a MagicMock.
"""
import hashlib
import json
import os
from unittest import mock

import pytest

import compile_uploader  # pythonpath="s3-sidecar" in pyproject
import s3_common


@pytest.fixture
def fake_s3(monkeypatch):
    """Patch boto3 so no test reaches S3, and expose the client for assertions."""
    fake = mock.MagicMock()
    monkeypatch.setattr(s3_common.boto3, "client", lambda *a, **k: fake)
    return fake


def uploaded(fake) -> dict[str, bytes]:
    """Map every uploaded S3 key to the bytes put under it."""
    return {
        call.kwargs["Key"]: call.kwargs["Body"] for call in fake.put_object.call_args_list
    }


def write_runtime_artifacts(tmp_path, monkeypatch, *, sha256: str | None = None):
    """Write a msgpack + descriptor pair and point the uploader's env at them.

    sha256 overrides the descriptor's digest so a mismatch can be forced.
    """
    packed = b"\x81\xa4fake-partial-parse"
    partial_parse = tmp_path / "partial_parse.msgpack"
    partial_parse.write_bytes(packed)
    descriptor = tmp_path / "runtime-manifest.json"
    descriptor.write_text(
        json.dumps({"sha256": sha256 or hashlib.sha256(packed).hexdigest()})
    )
    monkeypatch.setenv("COMPILE_PARTIAL_PARSE_PATH", str(partial_parse))
    monkeypatch.setenv("COMPILE_RUNTIME_DESCRIPTOR_PATH", str(descriptor))
    return partial_parse, descriptor


@pytest.fixture
def manifest_env(tmp_path, monkeypatch):
    """The always-present half of the uploader contract: manifest + target URI."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"nodes":{}}')
    monkeypatch.setenv("COMPILE_MANIFEST_PATH", str(manifest))
    monkeypatch.setenv("MANIFEST_S3_URI", "s3://continuo/svc/rel-1/manifest.json")
    monkeypatch.delenv("COMPILE_PARTIAL_PARSE_PATH", raising=False)
    monkeypatch.delenv("COMPILE_RUNTIME_DESCRIPTOR_PATH", raising=False)
    return manifest


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


# --- runtime artifact set -------------------------------------------------


def test_uploads_all_three_artifacts_to_sibling_keys(
    tmp_path, monkeypatch, manifest_env, fake_s3
):
    partial_parse, descriptor = write_runtime_artifacts(tmp_path, monkeypatch)

    compile_uploader.main()

    assert uploaded(fake_s3) == {
        "svc/rel-1/manifest.json": b'{"nodes":{}}',
        "svc/rel-1/partial_parse.msgpack": partial_parse.read_bytes(),
        "svc/rel-1/runtime-manifest.json": descriptor.read_bytes(),
    }
    for call in fake_s3.put_object.call_args_list:
        assert call.kwargs["Bucket"] == "continuo"


def test_old_image_without_exporter_uploads_manifest_only(
    tmp_path, monkeypatch, manifest_env, fake_s3, capsys
):
    """A team image predating the exporter still produces a working release."""
    monkeypatch.setenv("COMPILE_PARTIAL_PARSE_PATH", str(tmp_path / "absent.msgpack"))
    monkeypatch.setenv("COMPILE_RUNTIME_DESCRIPTOR_PATH", str(tmp_path / "absent.json"))

    compile_uploader.main()

    assert uploaded(fake_s3) == {"svc/rel-1/manifest.json": b'{"nodes":{}}'}
    assert "runtime artifact unavailable; manifest-only compatibility upload" in (
        capsys.readouterr().out
    )


def test_unset_runtime_env_uploads_manifest_only(manifest_env, fake_s3):
    compile_uploader.main()

    assert uploaded(fake_s3) == {"svc/rel-1/manifest.json": b'{"nodes":{}}'}


def test_descriptor_without_msgpack_exits_nonzero(
    tmp_path, monkeypatch, manifest_env, fake_s3
):
    partial_parse, _ = write_runtime_artifacts(tmp_path, monkeypatch)
    partial_parse.unlink()

    with pytest.raises(SystemExit) as e:
        compile_uploader.main()
    assert e.value.code != 0
    assert fake_s3.put_object.call_count == 0, "a partial artifact set uploads nothing"


def test_msgpack_without_descriptor_exits_nonzero(
    tmp_path, monkeypatch, manifest_env, fake_s3
):
    _, descriptor = write_runtime_artifacts(tmp_path, monkeypatch)
    descriptor.unlink()

    with pytest.raises(SystemExit) as e:
        compile_uploader.main()
    assert e.value.code != 0
    assert fake_s3.put_object.call_count == 0, "a partial artifact set uploads nothing"


def test_sha_mismatch_exits_nonzero_and_uploads_nothing(
    tmp_path, monkeypatch, manifest_env, fake_s3
):
    write_runtime_artifacts(tmp_path, monkeypatch, sha256="c" * 64)

    with pytest.raises(SystemExit) as e:
        compile_uploader.main()
    assert e.value.code != 0
    assert fake_s3.put_object.call_count == 0, "a mismatched set uploads nothing"
