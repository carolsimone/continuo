"""
Integration tests for dbt compile+upload pipeline.
Requires localstack running at S3_ENDPOINT_URL (default: http://localstack:4566).
Run against the running dbt-compile-and-load container:
  docker exec -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
    -e AWS_DEFAULT_REGION=us-east-1 \
    -e S3_ENDPOINT_URL=http://localstack:4566 -e S3_BUCKET=continuo -e S3_ENV=local \
    -e DBT_POSTGRES_HOST=postgres -e DBT_POSTGRES_PORT=5432 \
    -e DBT_POSTGRES_DB=continuo_dbt -e DBT_POSTGRES_USER=continuo_svc \
    -e DBT_POSTGRES_PASSWORD=continuo \
    dbt-compile-and-load uv run --with pytest pytest tests/test_upload.py -v
"""
import json
import os
import subprocess

import boto3
import pytest

from dbt_upload.upload import next_version, upload_manifest

SERVICES_DIR = "/app/services"
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "continuo")
S3_ENV = os.getenv("S3_ENV", "local")


@pytest.fixture
def s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


@pytest.fixture
def s3_prefix(s3, request):
    """Yield a unique S3 prefix for a test and delete all its objects on teardown."""
    service = request.node.name.replace("[", "-").replace("]", "")
    prefix = f"{S3_ENV}/manifest/{service}/"
    yield prefix
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])


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


def test_upload_and_read_back(s3):
    """compile + upload produces a readable manifest_v1.json in S3."""
    from dbt_upload.compile import compile_service

    service_dir = os.path.join(SERVICES_DIR, "service-1")
    assert compile_service(service_dir), "compile_service returned False"
    assert upload_manifest(s3, service_dir, S3_ENV, S3_BUCKET), "upload_manifest returned False"

    key = f"{S3_ENV}/manifest/service-1/manifest_v1.json"
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = json.loads(response["Body"].read())

    assert "nodes" in content
    node_names = [n["name"] for n in content["nodes"].values()]
    assert "table_a" in node_names


def test_all_valid_services_upload(s3):
    """service-1, service-2, service-3 all compile and upload."""
    from dbt_upload.compile import compile_service

    valid = ["service-1", "service-2", "service-3"]
    for name in valid:
        service_dir = os.path.join(SERVICES_DIR, name)
        assert compile_service(service_dir), f"{name} failed to compile"
        assert upload_manifest(s3, service_dir, S3_ENV, S3_BUCKET), f"{name} failed to upload"

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{S3_ENV}/manifest/")
    keys = {obj["Key"] for obj in response.get("Contents", [])}
    for name in valid:
        versioned_keys = {k for k in keys if k.startswith(f"{S3_ENV}/manifest/{name}/manifest_v")}
        assert versioned_keys, f"No versioned manifest found in S3 for {name}"


# Versioning edge-case integration tests


def test_next_version_returns_1_when_prefix_empty(s3, s3_prefix):
    """next_version returns 1 when no files exist under the S3 prefix."""
    assert next_version(s3, S3_BUCKET, s3_prefix) == 1


def test_next_version_returns_8_when_v7_exists(s3, s3_prefix):
    """next_version returns 8 when manifest_v7.json is already in S3."""
    s3.put_object(Bucket=S3_BUCKET, Key=f"{s3_prefix}manifest_v7.json", Body=b"{}")
    assert next_version(s3, S3_BUCKET, s3_prefix) == 8


def test_upload_manifest_first_upload_creates_v1(s3, s3_prefix, tmp_path):
    """upload_manifest uploads to manifest_v1.json when the S3 prefix is empty."""
    service_name = s3_prefix.rstrip("/").rsplit("/", 1)[-1]
    service_dir = tmp_path / service_name
    (service_dir / "target").mkdir(parents=True)
    (service_dir / "target" / "manifest.json").write_text('{"nodes": {}}')

    assert upload_manifest(s3, str(service_dir), S3_ENV, S3_BUCKET)

    response = s3.get_object(Bucket=S3_BUCKET, Key=f"{s3_prefix}manifest_v1.json")
    assert json.loads(response["Body"].read()) == {"nodes": {}}


def test_upload_manifest_increments_from_v7_to_v8(s3, s3_prefix, tmp_path):
    """upload_manifest uploads to manifest_v8.json when manifest_v7.json already exists in S3."""
    s3.put_object(Bucket=S3_BUCKET, Key=f"{s3_prefix}manifest_v7.json", Body=b'{"nodes": {}}')

    service_name = s3_prefix.rstrip("/").rsplit("/", 1)[-1]
    service_dir = tmp_path / service_name
    (service_dir / "target").mkdir(parents=True)
    (service_dir / "target" / "manifest.json").write_text(
        '{"nodes": {"model.x.y": {"resource_type": "model", "name": "y", "tags": []}}}'
    )

    assert upload_manifest(s3, str(service_dir), S3_ENV, S3_BUCKET)

    response = s3.get_object(Bucket=S3_BUCKET, Key=f"{s3_prefix}manifest_v8.json")
    assert "nodes" in json.loads(response["Body"].read())


# Per-release upload layout (no localstack — MagicMock S3 client)


def test_upload_manifest_release_mode_writes_v1_under_release_prefix(tmp_path):
    """release_id uploads to releases/<id>/manifests/<service>/manifest_v1.json,
    always v1 (fresh prefix), with no next_version lookup and no sidecar."""
    from unittest.mock import MagicMock
    from dbt_upload.upload import upload_manifest

    service_dir = tmp_path / "service-1"
    (service_dir / "target").mkdir(parents=True)
    (service_dir / "target" / "manifest.json").write_text('{"nodes": {}}')

    mock_s3 = MagicMock()

    result = upload_manifest(
        mock_s3, str(service_dir), "local", "continuo", release_id="rel-abc"
    )

    assert result is True
    # next_version is never consulted on the per-release path.
    mock_s3.get_paginator.assert_not_called()

    upload_calls = [str(c) for c in mock_s3.upload_file.call_args_list]
    assert any(
        "releases/rel-abc/manifests/service-1/manifest_v1.json" in c
        for c in upload_calls
    ), f"per-release manifest_v1.json not uploaded; calls={upload_calls}"
    # No sidecar on the per-release path.
    assert not any("service_metadata.json" in c for c in upload_calls), \
        f"service_metadata.json must NOT be uploaded on the release path; calls={upload_calls}"


def test_upload_services_release_mode_no_image_tag_required(tmp_path, monkeypatch):
    """With release_id set and IMAGE_TAG_PER_SERVICE empty, every service still
    uploads to releases/<id>/manifests/<service>/manifest_v1.json with no sidecar."""
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
        "endpoint_url": "http://localstack:4566",
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
            f"releases/rel-xyz/manifests/{name}/manifest_v1.json" in c
            for c in upload_calls
        ), f"missing per-release manifest for {name}; calls={upload_calls}"
    assert not any("service_metadata.json" in c for c in upload_calls), \
        f"no sidecar on the release path; calls={upload_calls}"
