import json
import os
from unittest.mock import MagicMock
from adapters.sources.s3 import S3Source
from domain.model import ManifestKind, ManifestRequest


def _make_s3_source(keys=None, file_content='{"nodes": {}}'):
    """Build an S3Source with a fake S3 client.

    keys is a list of ManifestRequest, matching S3Source's expected signature.
    """
    mock_s3 = MagicMock()

    def fake_download(bucket, key, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            f.write(file_content)

    mock_s3.download_file.side_effect = fake_download
    return S3Source(bucket="continuo", env="local", s3_client=mock_s3, keys=keys or [])


def test_s3_source_returns_one_file_per_key():
    """list_manifests() returns exactly one ManifestFile per supplied key."""
    keys = [
        ManifestRequest(service="service-1", key="service-1/rel-99/manifest.json"),
        ManifestRequest(service="service-2", key="service-2/rel-99/manifest.json"),
    ]
    source = _make_s3_source(keys=keys)
    try:
        result = source.list_manifests()
        assert len(result) == 2
    finally:
        source.cleanup()


def test_s3_source_image_tag_always_empty():
    """image_tag is always empty; release-controller joins tags downstream."""
    source = _make_s3_source(keys=[ManifestRequest(service="svc", key="svc/rel/manifest.json")])
    try:
        result = source.list_manifests()
        assert result[0].image_tag == ""
    finally:
        source.cleanup()


def test_s3_source_returns_empty_when_no_keys():
    source = _make_s3_source(keys=[])
    try:
        assert source.list_manifests() == []
    finally:
        source.cleanup()


def test_s3_source_no_list_objects_call():
    """No S3 listing is performed in the explicit-keys path."""
    mock_s3 = MagicMock()

    def fake_download(bucket, key, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            f.write('{"nodes": {}}')

    mock_s3.download_file.side_effect = fake_download
    source = S3Source(
        bucket="b", env="e", s3_client=mock_s3,
        keys=[ManifestRequest(service="svc", key="k/manifest.json")],
    )
    try:
        source.list_manifests()
        mock_s3.list_objects_v2.assert_not_called()
    finally:
        source.cleanup()


def test_s3_source_downloads_correct_keys():
    """Each provided key is fetched from the correct bucket."""
    mock_s3 = MagicMock()
    downloaded = []

    def fake_download(bucket, key, filename):
        downloaded.append((bucket, key))
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            f.write('{"nodes": {}}')

    mock_s3.download_file.side_effect = fake_download

    keys = [
        ManifestRequest(service="svc-a", key="svc-a/r1/manifest.json"),
        ManifestRequest(service="svc-b", key="svc-b/r1/manifest.json"),
    ]
    source = S3Source(bucket="my-bucket", env="local", s3_client=mock_s3, keys=keys)
    try:
        source.list_manifests()
    finally:
        source.cleanup()

    assert downloaded == [("my-bucket", "svc-a/r1/manifest.json"),
                          ("my-bucket", "svc-b/r1/manifest.json")]


def test_s3_source_temp_files_exist_after_list():
    """Downloaded files are present under the temp dir until cleanup()."""
    source = _make_s3_source(keys=[ManifestRequest(service="svc", key="svc/r/manifest.json")])
    try:
        result = source.list_manifests()
        assert os.path.exists(result[0].path)
    finally:
        source.cleanup()


def test_s3_source_downloads_content_to_temp_file():
    content = json.dumps({"nodes": {"n1": {"name": "table_a"}}})
    source = _make_s3_source(
        keys=[ManifestRequest(service="service-1", key="service-1/rel/manifest.json")],
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


def test_s3_source_propagates_declared_service():
    """declared_service from each ManifestRequest appears on the ManifestFile."""
    keys = [
        ManifestRequest(service="service-a", key="service-a/rel-1/manifest.json"),
        ManifestRequest(service="service-b", key="service-b/rel-1/manifest.json"),
    ]
    source = _make_s3_source(keys=keys)
    try:
        result = source.list_manifests()
        assert result[0].declared_service == "service-a"
        assert result[1].declared_service == "service-b"
    finally:
        source.cleanup()


def test_s3_source_empty_declared_service_when_no_service_supplied():
    """An empty declared_service propagates to the ManifestFile unchanged."""
    source = _make_s3_source(keys=[ManifestRequest(service="", key="svc/rel/manifest.json")])
    try:
        result = source.list_manifests()
        assert result[0].declared_service == ""
    finally:
        source.cleanup()


def test_source_carries_the_declared_kind_onto_each_manifest_file():
    """kind travels with the key from the release.requested payload; the
    handler is what decides how to parse it."""
    s3 = MagicMock()
    source = S3Source(
        bucket="continuo", env="local", s3_client=s3,
        keys=[
            ManifestRequest(service="service-1", key="service-1/rel-1/manifest.json"),
            ManifestRequest(service="marketing-py", key="marketing-py/rel-1/contract.yaml",
                            kind=ManifestKind.PYTHON),
        ],
    )

    files = source.list_manifests()

    assert [f.kind for f in files] == ["dbt", "python"]
    assert [f.declared_service for f in files] == ["service-1", "marketing-py"]
