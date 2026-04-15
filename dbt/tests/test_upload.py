"""
Integration tests for dbt compile+upload pipeline.
Requires localstack running at S3_ENDPOINT_URL (default: http://localstack:4566).
Run from the repo root:
  docker run --rm --network continuo_default --workdir /app \
    -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
    -e AWS_DEFAULT_REGION=us-east-1 \
    -v "$(pwd)/dbt/services:/app/services" \
    dbt-compile-and-load:latest uv run pytest tests/ -v
"""
import json
import os
import subprocess

import boto3
import pytest

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
    """compile + upload produces a readable manifest.json in S3."""
    from dbt_upload.compile import compile_service
    from dbt_upload.upload import upload_manifest

    service_dir = os.path.join(SERVICES_DIR, "service-1")
    assert compile_service(service_dir), "compile_service returned False"
    assert upload_manifest(s3, service_dir, S3_ENV, S3_BUCKET), "upload_manifest returned False"

    key = f"{S3_ENV}/manifest/service-1/manifest.json"
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = json.loads(response["Body"].read())

    assert "nodes" in content
    node_names = [n["name"] for n in content["nodes"].values()]
    assert "table_a" in node_names


def test_all_valid_services_upload(s3):
    """service-1, service-2, service-3 all compile and upload; service-3-broken is skipped."""
    from dbt_upload.compile import compile_service
    from dbt_upload.upload import upload_manifest

    valid = ["service-1", "service-2", "service-3"]
    for name in valid:
        service_dir = os.path.join(SERVICES_DIR, name)
        assert compile_service(service_dir), f"{name} failed to compile"
        assert upload_manifest(s3, service_dir, S3_ENV, S3_BUCKET), f"{name} failed to upload"

    # verify keys exist in S3
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{S3_ENV}/manifest/")
    keys = {obj["Key"] for obj in response.get("Contents", [])}
    for name in valid:
        assert f"{S3_ENV}/manifest/{name}/manifest.json" in keys


def test_service3_broken_compile_fails():
    """service-3-broken fails dbt compile — compile_service returns False."""
    from dbt_upload.compile import compile_service

    service_dir = os.path.join(SERVICES_DIR, "service-3-broken")
    assert not compile_service(service_dir), "Expected compile to fail for broken service"
