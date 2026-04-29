import json
from pathlib import Path

import pytest

from dbt_upload.service_metadata import (
    parse_image_tag_env,
    write_service_metadata_json,
    MissingImageTagError,
)


def test_parse_image_tag_env_basic():
    raw = "service-1=abc123,service-2=def456,service-3=ghi789"
    assert parse_image_tag_env(raw) == {
        "service-1": "abc123",
        "service-2": "def456",
        "service-3": "ghi789",
    }


def test_parse_image_tag_env_strips_whitespace():
    raw = " service-1 = abc123 , service-2 = def456 "
    assert parse_image_tag_env(raw) == {
        "service-1": "abc123",
        "service-2": "def456",
    }


def test_parse_image_tag_env_empty_string_returns_empty_map():
    assert parse_image_tag_env("") == {}


def test_parse_image_tag_env_malformed_raises():
    with pytest.raises(ValueError, match="malformed entry"):
        parse_image_tag_env("service-1abc123,service-2=def456")


def test_write_service_metadata_json_creates_file(tmp_path: Path):
    write_service_metadata_json(
        out_dir=tmp_path,
        service_name="service-1",
        manifest_version="v3",
        image_tag="abc123-1714300000",
    )
    written = tmp_path / "service_metadata.json"
    assert written.exists()
    data = json.loads(written.read_text())
    assert data == {
        "manifest_version": "v3",
        "image_tag": "abc123-1714300000",
    }


def test_write_service_metadata_json_raises_on_empty_image_tag(tmp_path: Path):
    with pytest.raises(MissingImageTagError, match="service-2"):
        write_service_metadata_json(
            out_dir=tmp_path,
            service_name="service-2",
            manifest_version="v3",
            image_tag="",
        )


def test_upload_manifest_writes_sidecar_when_image_tag_provided(tmp_path: Path):
    """upload_manifest uploads service_metadata.json sidecar when image_tag is set."""
    from unittest.mock import MagicMock
    from dbt_upload.upload import upload_manifest

    # Create a fake manifest.json
    service_dir = tmp_path / "service-1"
    (service_dir / "target").mkdir(parents=True)
    (service_dir / "target" / "manifest.json").write_text('{"nodes": {}}')

    mock_s3 = MagicMock()
    mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

    result = upload_manifest(mock_s3, str(service_dir), "local", "continuo", image_tag="abc123-1714300000")

    assert result is True
    upload_calls = [str(c) for c in mock_s3.upload_file.call_args_list]
    assert any("service_metadata.json" in c for c in upload_calls), \
        f"service_metadata.json not uploaded; calls={upload_calls}"
    # Verify the S3 key for the sidecar
    sidecar_call = next(c for c in mock_s3.upload_file.call_args_list if "service_metadata" in str(c))
    assert "local/manifest/service-1/service_metadata.json" in str(sidecar_call)


def test_upload_services_rejects_service_missing_image_tag(tmp_path: Path, monkeypatch):
    """upload_services must mark services missing from IMAGE_TAG_PER_SERVICE as failed
    before touching S3 — otherwise a manifest is uploaded without a sidecar, the
    snapshot propagates image_tag='', and the run fails at deployment instead of
    at upload time."""
    from unittest.mock import patch
    from dbt_upload.upload import upload_services

    service_dir = tmp_path / "service-1"
    (service_dir / "target").mkdir(parents=True)
    (service_dir / "target" / "manifest.json").write_text('{"nodes": {}}')

    monkeypatch.setenv("IMAGE_TAG_PER_SERVICE", "")

    target_config = {
        "endpoint_url": "http://localstack:4566",
        "access_key_id": "test",
        "secret_access_key": "test",
        "region": "us-east-1",
        "env": "local",
        "bucket": "continuo",
    }

    with patch("dbt_upload.upload.boto3.client"), \
         patch("dbt_upload.upload.upload_manifest") as mock_upload:
        succeeded, failed = upload_services([str(service_dir)], target_config)

    assert succeeded == []
    assert failed == [str(service_dir)]
    mock_upload.assert_not_called()
