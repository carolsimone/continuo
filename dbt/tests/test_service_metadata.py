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


