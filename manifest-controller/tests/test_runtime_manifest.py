import json
from pathlib import Path
import pytest
from domain.exceptions import MalformedRuntimeManifestError
from service.runtime_manifest import parse_descriptor

FIXTURES = Path(__file__).parent / "fixtures"

ARTIFACT_URI = "s3://continuo/service-1/rel-1/partial_parse.msgpack"


def _descriptor(**overrides):
    descriptor = json.loads((FIXTURES / "runtime-manifest.json").read_text())
    descriptor.update(overrides)
    return descriptor


def _parse(descriptor, expected_service="service-1", expected_artifact_uri=ARTIFACT_URI):
    return parse_descriptor(
        descriptor,
        expected_service=expected_service,
        expected_artifact_uri=expected_artifact_uri,
    )


def test_parse_descriptor_returns_ref():
    ref = _parse(_descriptor())
    assert ref.uri == ARTIFACT_URI
    assert ref.sha256 == "a" * 64
    assert ref.dbt_version == "1.12.0b1"
    assert ref.parse_context_sha256 == "b" * 64


def test_parse_descriptor_rejects_non_object():
    with pytest.raises(MalformedRuntimeManifestError):
        _parse([])


def test_parse_descriptor_rejects_unknown_format():
    with pytest.raises(MalformedRuntimeManifestError, match="format"):
        _parse(_descriptor(format="dbt-partial-parse-msgpack-v2"))


@pytest.mark.parametrize("field", [
    "format", "service_name", "release_id", "image_tag", "artifact_uri",
    "sha256", "dbt_core_version", "adapter_type", "parse_context_sha256",
])
def test_parse_descriptor_rejects_missing_field(field):
    descriptor = _descriptor()
    del descriptor[field]
    with pytest.raises(MalformedRuntimeManifestError, match=field):
        _parse(descriptor)


@pytest.mark.parametrize("field", [
    "format", "service_name", "release_id", "image_tag", "artifact_uri",
    "sha256", "dbt_core_version", "adapter_type", "parse_context_sha256",
])
def test_parse_descriptor_rejects_empty_field(field):
    with pytest.raises(MalformedRuntimeManifestError, match=field):
        _parse(_descriptor(**{field: ""}))


def test_parse_descriptor_rejects_non_string_field():
    """A non-string field is rejected as malformed, never allowed to surface as a
    type error that the consumer would mistake for a transient fault."""
    with pytest.raises(MalformedRuntimeManifestError, match="sha256"):
        _parse(_descriptor(sha256=12345))


def test_parse_descriptor_rejects_service_mismatch():
    """A descriptor for another service must never be attributed to this one."""
    with pytest.raises(MalformedRuntimeManifestError, match="service_name"):
        _parse(_descriptor(), expected_service="service-2")


def test_parse_descriptor_rejects_foreign_artifact_uri():
    """The artifact must be the sibling of the manifest the descriptor came with."""
    with pytest.raises(MalformedRuntimeManifestError, match="artifact_uri"):
        _parse(_descriptor(artifact_uri="s3://continuo/other/r9/partial_parse.msgpack"))


@pytest.mark.parametrize("field", ["sha256", "parse_context_sha256"])
@pytest.mark.parametrize("value", [
    "A" * 64, "a" * 63, "a" * 65, "z" * 64, "sha256:" + "a" * 64,
    "a" * 64 + "\n", "a" * 64 + " ",
])
def test_parse_descriptor_rejects_malformed_digest(field, value):
    with pytest.raises(MalformedRuntimeManifestError, match=field):
        _parse(_descriptor(**{field: value}))


def test_parse_descriptor_rejects_foreign_adapter():
    with pytest.raises(MalformedRuntimeManifestError, match="adapter_type"):
        _parse(_descriptor(adapter_type="snowflake"))


def test_malformed_runtime_manifest_error_is_value_error():
    assert issubclass(MalformedRuntimeManifestError, ValueError)
