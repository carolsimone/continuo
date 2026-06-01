import json
import os
from botocore.exceptions import ClientError
from unittest.mock import MagicMock
from adapters.sources.s3 import S3Source


def _make_s3_source(keys=None, file_content='{"nodes": {}}'):
    mock_s3 = MagicMock()
    contents = [{"Key": k} for k in (keys or [])]
    mock_s3.list_objects_v2.return_value = (
        {"Contents": contents} if contents else {}
    )

    def fake_download(bucket, key, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            f.write(file_content)

    mock_s3.download_file.side_effect = fake_download

    # Default: no service_metadata.json sidecar (backward compat)
    mock_s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "GetObject"
    )

    return S3Source(bucket="continuo", env="local", s3_client=mock_s3)


def test_s3_source_returns_highest_version_per_service():
    keys = [
        "local/manifest/service-1/manifest_v1.json",
        "local/manifest/service-1/manifest_v3.json",
    ]
    source = _make_s3_source(keys=keys)
    try:
        result = source.list_manifests()
        assert len(result) == 1
        assert result[0].version == "v3"
    finally:
        source.cleanup()


def test_s3_source_multiple_services_sorted():
    keys = [
        "local/manifest/service-b/manifest_v2.json",
        "local/manifest/service-a/manifest_v1.json",
    ]
    source = _make_s3_source(keys=keys)
    try:
        result = source.list_manifests()
        assert len(result) == 2
        assert result[0].version == "v1"   # service-a first (sorted)
        assert result[1].version == "v2"   # service-b second
    finally:
        source.cleanup()


def test_s3_source_returns_empty_when_no_objects():
    source = _make_s3_source(keys=[])
    try:
        assert source.list_manifests() == []
    finally:
        source.cleanup()


def test_s3_source_skips_service_with_no_versioned_manifest():
    keys = ["local/manifest/service-1/manifest.json"]
    source = _make_s3_source(keys=keys)
    try:
        assert source.list_manifests() == []
    finally:
        source.cleanup()


def test_s3_source_version_from_key_not_temp_path():
    """Version is extracted from S3 key before download — temp filename is irrelevant."""
    keys = ["local/manifest/service-1/manifest_v5.json"]
    source = _make_s3_source(keys=keys)
    try:
        result = source.list_manifests()
        assert result[0].version == "v5"
        assert os.path.exists(result[0].path)
    finally:
        source.cleanup()


def test_s3_source_downloads_content_to_temp_file():
    content = json.dumps({"nodes": {"n1": {"name": "table_a"}}})
    source = _make_s3_source(
        keys=["local/manifest/service-1/manifest_v2.json"],
        file_content=content,
    )
    try:
        result = source.list_manifests()
        assert len(result) == 1
        with open(result[0].path) as f:
            assert json.load(f) == {"nodes": {"n1": {"name": "table_a"}}}
    finally:
        source.cleanup()


def test_s3_source_cleanup_removes_temp_dir():
    source = _make_s3_source(keys=[])
    tmpdir_name = source._tmpdir.name
    assert os.path.isdir(tmpdir_name)
    source.cleanup()
    assert not os.path.exists(tmpdir_name)


def test_s3_source_attaches_image_tag_from_sidecar(tmp_path):
    """S3Source populates ManifestFile.image_tag from service_metadata.json sidecar."""
    mock_s3 = MagicMock()
    keys = ["local/manifest/service-1/manifest_v3.json"]
    mock_s3.list_objects_v2.return_value = {"Contents": [{"Key": k} for k in keys]}

    def fake_download(bucket, key, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            f.write('{"nodes": {}}')
    mock_s3.download_file.side_effect = fake_download

    # Sidecar returns the metadata
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps({
            "manifest_version": "v3",
            "image_tag": "abc123-1714300000",
        }).encode())
    }

    source = S3Source(bucket="continuo", env="local", s3_client=mock_s3)
    try:
        result = source.list_manifests()
        assert len(result) == 1
        assert result[0].image_tag == "abc123-1714300000"
    finally:
        source.cleanup()


def test_s3_source_image_tag_empty_when_sidecar_missing(tmp_path):
    """S3Source returns image_tag='' when service_metadata.json is absent (backward compat)."""
    mock_s3 = MagicMock()
    keys = ["local/manifest/service-1/manifest_v3.json"]
    mock_s3.list_objects_v2.return_value = {"Contents": [{"Key": k} for k in keys]}

    def fake_download(bucket, key, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            f.write('{"nodes": {}}')
    mock_s3.download_file.side_effect = fake_download

    # Sidecar is missing
    mock_s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "GetObject"
    )

    source = S3Source(bucket="continuo", env="local", s3_client=mock_s3)
    try:
        result = source.list_manifests()
        assert len(result) == 1
        assert result[0].image_tag == ""
    finally:
        source.cleanup()


def test_s3_source_uses_explicit_prefix_when_provided():
    """When prefix= is set, S3Source ignores the env-derived prefix."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": [
        {"Key": "releases/abc/manifests/service-1/manifest_v1.json"},
    ]}
    def fake_download(bucket, key, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            f.write('{"nodes": {}}')
    mock_s3.download_file.side_effect = fake_download
    mock_s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "GetObject"
    )

    source = S3Source(
        bucket="continuo",
        env="local",
        s3_client=mock_s3,
        prefix="releases/abc/manifests/",
    )
    try:
        result = source.list_manifests()
        assert len(result) == 1
        assert result[0].version == "v1"
        assert mock_s3.list_objects_v2.call_args.kwargs["Prefix"] == "releases/abc/manifests/"
    finally:
        source.cleanup()


def test_s3_source_defaults_to_env_derived_prefix():
    """Backward compat: omitting prefix= uses f'{env}/manifest/'."""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"Contents": []}
    source = S3Source(bucket="continuo", env="local", s3_client=mock_s3)
    try:
        source.list_manifests()
    finally:
        source.cleanup()
    assert mock_s3.list_objects_v2.call_args.kwargs["Prefix"] == "local/manifest/"
