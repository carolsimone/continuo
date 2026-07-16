import io
import json
import os
from pathlib import Path
from unittest.mock import MagicMock
import pytest
from botocore.exceptions import ClientError
from adapters.sources.s3 import S3Source
from domain.exceptions import MalformedRuntimeManifestError

FIXTURES = Path(__file__).parent / "fixtures"

DESCRIPTOR = json.loads((FIXTURES / "runtime-manifest.json").read_text())


def _client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "GetObject")


def _fake_s3_client(file_content='{"nodes": {}}', objects=None):
    """Build a fake S3 client backed by an in-memory object map.

    download_file writes file_content to the requested local path. get_object
    serves objects[key], raising a NoSuchKey ClientError for anything else —
    the same signal real S3 gives for a key that was never uploaded.
    """
    mock_s3 = MagicMock()
    stored = objects or {}

    def fake_download(bucket, key, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            f.write(file_content)

    def fake_get_object(Bucket, Key):
        if Key not in stored:
            raise _client_error("NoSuchKey")
        return {"Body": io.BytesIO(stored[Key])}

    mock_s3.download_file.side_effect = fake_download
    mock_s3.get_object.side_effect = fake_get_object
    return mock_s3


def _make_s3_source(keys=None, file_content='{"nodes": {}}', objects=None):
    """Build an S3Source with a fake S3 client.

    keys is a list of (declared_service, object_key) pairs, matching S3Source's
    expected signature. Callers may also pass a list of plain strings for
    convenience — each is wrapped as ("", key) to represent a legacy/untagged key.
    objects maps object key -> body bytes for the descriptor fetch.
    """
    mock_s3 = _fake_s3_client(file_content=file_content, objects=objects)
    raw = keys or []
    # Normalise plain strings to ("", key) pairs so helper callers don't break.
    normalised = [k if isinstance(k, tuple) else ("", k) for k in raw]
    return S3Source(bucket="continuo", env="local", s3_client=mock_s3, keys=normalised)


def test_s3_source_returns_one_file_per_key():
    """list_manifests() returns exactly one ManifestFile per supplied key."""
    keys = [
        ("service-1", "service-1/rel-99/manifest.json"),
        ("service-2", "service-2/rel-99/manifest.json"),
    ]
    source = _make_s3_source(keys=keys)
    try:
        result = source.list_manifests()
        assert len(result) == 2
    finally:
        source.cleanup()


def test_s3_source_image_tag_always_empty():
    """image_tag is always empty; release-controller joins tags downstream."""
    source = _make_s3_source(keys=[("svc", "svc/rel/manifest.json")])
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
    mock_s3 = _fake_s3_client()
    source = S3Source(bucket="b", env="e", s3_client=mock_s3, keys=[("svc", "k/manifest.json")])
    try:
        source.list_manifests()
        mock_s3.list_objects_v2.assert_not_called()
    finally:
        source.cleanup()


def test_s3_source_downloads_correct_keys():
    """Each provided key is fetched from the correct bucket."""
    mock_s3 = _fake_s3_client()
    keys = [("svc-a", "svc-a/r1/manifest.json"), ("svc-b", "svc-b/r1/manifest.json")]
    source = S3Source(bucket="my-bucket", env="local", s3_client=mock_s3, keys=keys)
    try:
        source.list_manifests()
    finally:
        source.cleanup()

    downloaded = [(c.args[0], c.args[1]) for c in mock_s3.download_file.call_args_list]
    assert downloaded == [("my-bucket", "svc-a/r1/manifest.json"),
                          ("my-bucket", "svc-b/r1/manifest.json")]


def test_s3_source_temp_files_exist_after_list():
    """Downloaded files are present under the temp dir until cleanup()."""
    source = _make_s3_source(keys=[("svc", "svc/r/manifest.json")])
    try:
        result = source.list_manifests()
        assert os.path.exists(result[0].path)
    finally:
        source.cleanup()


def test_s3_source_downloads_content_to_temp_file():
    content = json.dumps({"nodes": {"n1": {"name": "table_a"}}})
    source = _make_s3_source(
        keys=[("service-1", "service-1/rel/manifest.json")],
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
    """declared_service from each (service, key) pair appears on the ManifestFile."""
    keys = [
        ("service-a", "service-a/rel-1/manifest.json"),
        ("service-b", "service-b/rel-1/manifest.json"),
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
    source = _make_s3_source(keys=[("", "svc/rel/manifest.json")])
    try:
        result = source.list_manifests()
        assert result[0].declared_service == ""
    finally:
        source.cleanup()


# ---------------------------------------------------------------------------
# runtime manifest descriptor: sibling fetch, legacy absence, and validation
# ---------------------------------------------------------------------------

MANIFEST_KEY = "service-1/rel-1/manifest.json"
DESCRIPTOR_KEY = "service-1/rel-1/runtime-manifest.json"


def _source_with_descriptor(body):
    return _make_s3_source(
        keys=[("service-1", MANIFEST_KEY)],
        objects={DESCRIPTOR_KEY: body},
    )


def test_s3_source_reads_sibling_runtime_descriptor():
    """The descriptor is read from the manifest's own prefix and narrowed to a ref."""
    source = _source_with_descriptor(json.dumps(DESCRIPTOR).encode())
    try:
        result = source.list_manifests()
        ref = result[0].runtime_manifest
        assert ref is not None
        assert ref.uri == "s3://continuo/service-1/rel-1/partial_parse.msgpack"
        assert ref.sha256 == "a" * 64
        assert ref.dbt_version == "1.12.0b1"
        assert ref.parse_context_sha256 == "b" * 64
        source._s3.get_object.assert_called_once_with(
            Bucket="continuo", Key=DESCRIPTOR_KEY,
        )
    finally:
        source.cleanup()


def test_s3_source_descriptor_key_derived_without_listing():
    """The descriptor key is derived from the manifest key, never discovered."""
    source = _make_s3_source(keys=[("svc-a", "svc-a/r7/manifest.json")])
    try:
        source.list_manifests()
        source._s3.get_object.assert_called_once_with(
            Bucket="continuo", Key="svc-a/r7/runtime-manifest.json",
        )
        source._s3.list_objects_v2.assert_not_called()
    finally:
        source.cleanup()


@pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
def test_s3_source_runtime_manifest_none_when_descriptor_absent(code):
    """A release compiled before the runtime exporter has no descriptor; that is
    a supported manifest-only release, not an error."""
    source = _make_s3_source(keys=[("service-1", MANIFEST_KEY)])
    source._s3.get_object.side_effect = _client_error(code)
    try:
        result = source.list_manifests()
        assert result[0].runtime_manifest is None
    finally:
        source.cleanup()


@pytest.mark.parametrize("code", ["AccessDenied", "InvalidAccessKeyId", "SlowDown"])
def test_s3_source_propagates_non_absence_s3_errors(code):
    """An auth or throttling failure is retryable and must never be mistaken for
    a legacy release with no descriptor."""
    source = _make_s3_source(keys=[("service-1", MANIFEST_KEY)])
    source._s3.get_object.side_effect = _client_error(code)
    try:
        with pytest.raises(ClientError):
            source.list_manifests()
    finally:
        source.cleanup()


def test_s3_source_propagates_transport_error():
    """A non-ClientError transport failure escapes unchanged for retry."""
    source = _make_s3_source(keys=[("service-1", MANIFEST_KEY)])
    source._s3.get_object.side_effect = ConnectionError("s3 unreachable")
    try:
        with pytest.raises(ConnectionError):
            source.list_manifests()
    finally:
        source.cleanup()


def test_s3_source_rejects_descriptor_that_is_not_json():
    source = _source_with_descriptor(b"not json {{{")
    try:
        with pytest.raises(MalformedRuntimeManifestError):
            source.list_manifests()
    finally:
        source.cleanup()


def test_s3_source_rejects_descriptor_for_another_service():
    source = _make_s3_source(
        keys=[("service-2", "service-2/rel-1/manifest.json")],
        objects={"service-2/rel-1/runtime-manifest.json": json.dumps(DESCRIPTOR).encode()},
    )
    try:
        with pytest.raises(MalformedRuntimeManifestError, match="service_name"):
            source.list_manifests()
    finally:
        source.cleanup()


def test_s3_source_rejects_descriptor_pointing_at_foreign_artifact():
    """The descriptor must describe the artifact sitting beside this manifest."""
    foreign = dict(DESCRIPTOR, artifact_uri="s3://continuo/service-1/rel-9/partial_parse.msgpack")
    source = _source_with_descriptor(json.dumps(foreign).encode())
    try:
        with pytest.raises(MalformedRuntimeManifestError, match="artifact_uri"):
            source.list_manifests()
    finally:
        source.cleanup()
