"""
Tests for dbt manifest upload logic.

Integration tests (requiring minio) are marked with @pytest.mark.integration
and skipped by default. Run them inside the dbt-compile-and-load container:
  docker exec -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \
    -e AWS_DEFAULT_REGION=us-east-1 \
    -e S3_ENDPOINT_URL=http://minio:9000 -e S3_BUCKET=continuo -e S3_ENV=local \
    -e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 \
    -e POSTGRES_DB=continuo_dbt -e POSTGRES_USER=continuo_svc \
    -e POSTGRES_PASSWORD=continuo \
    dbt-compile-and-load uv run --with pytest pytest tests/test_upload.py -v -m integration
"""
import json
import os
import subprocess
import sys

import boto3
import pytest

from dbt_upload.upload import upload_manifest

SERVICES_DIR = "/app/services"
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "continuo")
S3_ENV = os.getenv("S3_ENV", "local")


@pytest.fixture
def s3():
    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    try:
        client.create_bucket(Bucket=S3_BUCKET)
    except client.exceptions.BucketAlreadyOwnedByYou:
        pass
    except Exception:
        pass  # already exists / created by infra setup
    return client


# ---------------------------------------------------------------------------
# Unit tests (no minio required)
# ---------------------------------------------------------------------------


def test_upload_manifest_canonical_key(tmp_path):
    """Cross-language drift guard for the manifest S3 key.

    The authoritative owner of the manifest layout is the Go function
    CanonicalManifestKey in
    release-controller/service/handlers/manifest_layout.go, which builds
    s3://<bucket>/<service>/<release_id>/manifest.json. release-controller
    derives the key it reads from (bucket, service, release_id), so the Python
    writer must produce the exact same <service>/<release_id>/manifest.json key
    object (the s3:// scheme + bucket are supplied to boto3 separately as the
    Bucket argument). If either side changes the layout, this test fails before
    the two can silently diverge.
    """
    from unittest.mock import MagicMock

    service = "service-2"
    release_id = "rel-abc"
    bucket = "continuo"

    service_dir = tmp_path / service
    (service_dir / "target").mkdir(parents=True)
    (service_dir / "target" / "manifest.json").write_text('{"nodes": {}}')

    mock_s3 = MagicMock()

    result = upload_manifest(mock_s3, str(service_dir), "local", bucket, release_id=release_id)

    assert result is True
    # boto3 upload_file(filename, bucket, key) — assert the exact (bucket, key) pair.
    args, _ = mock_s3.upload_file.call_args
    _filename, called_bucket, called_key = args
    assert called_bucket == bucket
    assert called_key == f"{service}/{release_id}/manifest.json"
    # Exactly one upload — no sidecar.
    assert mock_s3.upload_file.call_count == 1


def test_upload_manifest_empty_release_id_raises():
    """upload_manifest raises ValueError when release_id is empty."""
    from unittest.mock import MagicMock

    mock_s3 = MagicMock()

    with pytest.raises(ValueError, match="release_id is required"):
        upload_manifest(mock_s3, "/some/service-dir", "local", "continuo", release_id="")


def test_upload_manifest_release_mode_no_version_lookup(tmp_path):
    """upload_manifest never calls get_paginator — there is no legacy version lookup."""
    from unittest.mock import MagicMock

    service_dir = tmp_path / "service-1"
    (service_dir / "target").mkdir(parents=True)
    (service_dir / "target" / "manifest.json").write_text('{"nodes": {}}')

    mock_s3 = MagicMock()

    upload_manifest(mock_s3, str(service_dir), "local", "continuo", release_id="rel-abc")

    mock_s3.get_paginator.assert_not_called()


def test_upload_manifest_no_sidecar(tmp_path):
    """upload_manifest never uploads a service_metadata.json sidecar."""
    from unittest.mock import MagicMock

    service_dir = tmp_path / "service-1"
    (service_dir / "target").mkdir(parents=True)
    (service_dir / "target" / "manifest.json").write_text('{"nodes": {}}')

    mock_s3 = MagicMock()

    upload_manifest(mock_s3, str(service_dir), "local", "continuo", release_id="rel-abc")

    upload_calls = [str(c) for c in mock_s3.upload_file.call_args_list]
    assert not any("service_metadata.json" in c for c in upload_calls), \
        f"service_metadata.json must NOT be uploaded; calls={upload_calls}"


def test_upload_manifest_missing_manifest_returns_false(tmp_path):
    """upload_manifest returns False and does not call S3 when manifest.json is absent."""
    from unittest.mock import MagicMock

    service_dir = tmp_path / "service-1"
    service_dir.mkdir(parents=True)

    mock_s3 = MagicMock()

    result = upload_manifest(mock_s3, str(service_dir), "local", "continuo", release_id="rel-abc")

    assert result is False
    mock_s3.upload_file.assert_not_called()


def test_upload_services_release_mode_no_image_tag_required(tmp_path, monkeypatch):
    """With release_id set and IMAGE_TAG_PER_SERVICE empty, every service still
    uploads to <service>/<release_id>/manifest.json with no sidecar."""
    from unittest.mock import MagicMock, patch
    from dbt_upload.upload import upload_services

    services = ["service-1", "service-2"]
    service_dirs = []
    for name in services:
        d = tmp_path / name
        (d / "target").mkdir(parents=True)
        (d / "target" / "manifest.json").write_text(
            '{"nodes": {"model.x.y": {"resource_type": "model", "name": "y", "tags": []}}}'
        )
        service_dirs.append(str(d))

    monkeypatch.setenv("IMAGE_TAG_PER_SERVICE", "")

    target_config = {
        "endpoint_url": "http://minio:9000",
        "access_key_id": "test",
        "secret_access_key": "test",
        "region": "us-east-1",
        "env": "local",
        "bucket": "continuo",
    }

    mock_s3 = MagicMock()
    with patch("dbt_upload.upload.boto3.client", return_value=mock_s3):
        succeeded, failed = upload_services(
            service_dirs, target_config, release_id="rel-xyz"
        )

    assert failed == []
    assert succeeded == service_dirs

    upload_calls = [str(c) for c in mock_s3.upload_file.call_args_list]
    for name in services:
        assert any(
            f"{name}/rel-xyz/manifest.json" in c for c in upload_calls
        ), f"missing canonical manifest for {name}; calls={upload_calls}"
    assert not any("service_metadata.json" in c for c in upload_calls), \
        f"no sidecar on the release path; calls={upload_calls}"


# ---------------------------------------------------------------------------
# Integration tests (require minio + compiled dbt services)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_dbt_compile_service1_succeeds():
    """dbt compile runs without error for service-1."""
    service_dir = os.path.join(SERVICES_DIR, "service-1")
    result = subprocess.run(
        ["dbt", "compile", "--profiles-dir", "."],
        cwd=service_dir,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"dbt compile failed:\n{result.stderr}"
    manifest = os.path.join(service_dir, "target", "manifest.json")
    assert os.path.exists(manifest), "target/manifest.json not created"


@pytest.mark.integration
def test_upload_and_read_back_canonical(s3):
    """compile + upload produces a readable manifest.json at the canonical key."""
    from dbt_upload.compile import compile_service

    service_dir = os.path.join(SERVICES_DIR, "service-1")
    assert compile_service(service_dir), "compile_service returned False"
    assert upload_manifest(s3, service_dir, S3_ENV, S3_BUCKET, release_id="test-rel-001"), \
        "upload_manifest returned False"

    key = "service-1/test-rel-001/manifest.json"
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = json.loads(response["Body"].read())

    assert "nodes" in content
    node_names = [n["name"] for n in content["nodes"].values()]
    assert "table_a" in node_names


@pytest.mark.integration
def test_all_valid_services_upload_canonical(s3):
    """service-1, service-2, service-3 all compile and upload at canonical keys."""
    from dbt_upload.compile import compile_service

    valid = ["service-1", "service-2", "service-3"]
    release_id = "test-rel-all"
    for name in valid:
        service_dir = os.path.join(SERVICES_DIR, name)
        assert compile_service(service_dir), f"{name} failed to compile"
        assert upload_manifest(s3, service_dir, S3_ENV, S3_BUCKET, release_id=release_id), \
            f"{name} failed to upload"

    for name in valid:
        key = f"{name}/{release_id}/manifest.json"
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content = json.loads(response["Body"].read())
        assert "nodes" in content, f"manifest missing nodes for {name}"


@pytest.mark.integration
def test_compile_uploader_lands_manifest_at_canonical_key(s3):
    """The DEPLOYED uploader: real `dbt compile` + /compile_uploader.py -> manifest at the
    canonical S3 key, readable and parseable. Proves the actual adapter-sidecar main container."""
    service_dir = os.path.join(SERVICES_DIR, "service-1")
    # 1) real dbt compile (init-container step)
    compiled = subprocess.run(
        ["dbt", "compile", "--profiles-dir", "."],
        cwd=service_dir, capture_output=True, text=True,
    )
    assert compiled.returncode == 0, f"dbt compile failed:\n{compiled.stderr}"
    manifest_path = os.path.join(service_dir, "target", "manifest.json")
    assert os.path.exists(manifest_path)

    # 2) run the DEPLOYED uploader exactly as the pod's main container does
    release_id = "test-rel-compile-uploader"
    key = f"service-1/{release_id}/manifest.json"
    env = {
        **os.environ,
        "COMPILE_MANIFEST_PATH": manifest_path,
        "MANIFEST_S3_URI": f"s3://{S3_BUCKET}/{key}",
        "S3_ENDPOINT_URL": S3_ENDPOINT,
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    }
    uploaded = subprocess.run(
        [sys.executable, "/compile_uploader.py"], capture_output=True, text=True, env=env,
    )
    assert uploaded.returncode == 0, f"compile_uploader failed:\n{uploaded.stderr}"

    # 3) the object exists at the canonical key and is a parseable manifest
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = json.loads(response["Body"].read())
    assert "nodes" in content
    node_names = [n["name"] for n in content["nodes"].values()]
    assert "table_a" in node_names
