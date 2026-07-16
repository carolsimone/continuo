"""Unit tests for the runtime-manifest exporter shipped in the dbt base image.

Hydration is exercised against a real dbt Manifest msgpack round trip, so the
descriptor these tests accept is one a worker can actually load. The parse
context is exercised against a small fake, because its payload only reads
attribute shapes and hashing it must not depend on a real parse.
"""
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import msgpack.exceptions
import pytest
from dbt.contracts.graph.manifest import Manifest

from continuo_dbt_runtime.descriptor import FORMAT, validate_descriptor
from continuo_dbt_runtime.export_artifacts import export_runtime_artifacts
from continuo_dbt_runtime.parse_context import (
    PARSE_CONTEXT_ENV_KEYS,
    _parse_context_payload,
    parse_context_sha256,
)

CONTROLLER_CONTEXT = '{"command_dialect_sha256":"abc"}'


def make_manifest(adapter_type: str = "postgres") -> Manifest:
    """Build a real Manifest that survives a to_msgpack/from_msgpack round trip."""
    manifest = Manifest()
    manifest.metadata.adapter_type = adapter_type
    return manifest


@dataclass(frozen=True)
class FakeFileHash:
    name: str
    checksum: str


@dataclass(frozen=True)
class FakeStateCheck:
    vars_hash: FakeFileHash
    project_env_vars_hash: FakeFileHash
    profile_env_vars_hash: FakeFileHash
    profile_hash: FakeFileHash
    project_hashes: dict


@dataclass(frozen=True)
class FakeManifest:
    state_check: FakeStateCheck


def fake_manifest(vars_checksum: str = "v1") -> FakeManifest:
    return FakeManifest(
        state_check=FakeStateCheck(
            vars_hash=FakeFileHash("vars", vars_checksum),
            project_env_vars_hash=FakeFileHash("project_env", "pe1"),
            profile_env_vars_hash=FakeFileHash("profile_env", "fe1"),
            profile_hash=FakeFileHash("profile", "p1"),
            project_hashes={
                "proj_b": FakeFileHash("b", "hb"),
                "proj_a": FakeFileHash("a", "ha"),
            },
        )
    )


def write_sources(tmp_path: Path, manifest: Manifest) -> tuple[Path, Path]:
    """Write the two files a team's compile leg leaves behind."""
    partial_parse = tmp_path / "partial_parse.msgpack"
    partial_parse.write_bytes(manifest.to_msgpack())
    manifest_json = tmp_path / "manifest.json"
    manifest_json.write_text('{"metadata": {"dbt_version": "1.12.0b1"}}')
    return manifest_json, partial_parse


# --- export ---------------------------------------------------------------


def test_export_writes_three_bound_files(tmp_path):
    manifest = make_manifest()
    manifest_json, partial_parse = write_sources(tmp_path, manifest)

    descriptor = export_runtime_artifacts(
        manifest_path=manifest_json,
        partial_parse_path=partial_parse,
        output_dir=tmp_path / "shared",
        service_name="finance",
        release_id="r1",
        image_tag="sha-1",
        artifact_uri="s3://continuo/finance/r1/partial_parse.msgpack",
        controller_context=CONTROLLER_CONTEXT,
    )

    assert descriptor["format"] == "dbt-partial-parse-msgpack-v1"
    assert descriptor["sha256"] == hashlib.sha256(partial_parse.read_bytes()).hexdigest()
    assert descriptor["dbt_core_version"] == "1.12.0b1"
    assert descriptor["adapter_type"] == "postgres"
    assert descriptor["service_name"] == "finance"
    assert descriptor["release_id"] == "r1"
    assert descriptor["image_tag"] == "sha-1"
    assert descriptor["artifact_uri"] == "s3://continuo/finance/r1/partial_parse.msgpack"

    shared = tmp_path / "shared"
    assert (shared / "runtime-manifest.json").exists()
    assert (shared / "manifest.json").read_text() == manifest_json.read_text()
    assert (shared / "partial_parse.msgpack").read_bytes() == partial_parse.read_bytes()


def test_exported_msgpack_hydrates_back_into_a_manifest(tmp_path):
    """The exported artifact is loadable, not merely byte-identical."""
    manifest_json, partial_parse = write_sources(tmp_path, make_manifest())

    export_runtime_artifacts(
        manifest_path=manifest_json,
        partial_parse_path=partial_parse,
        output_dir=tmp_path / "shared",
        service_name="finance",
        release_id="r1",
        image_tag="sha-1",
        artifact_uri="s3://continuo/finance/r1/partial_parse.msgpack",
        controller_context=CONTROLLER_CONTEXT,
    )

    hydrated = Manifest.from_msgpack(
        (tmp_path / "shared" / "partial_parse.msgpack").read_bytes()
    )
    assert hydrated.metadata.adapter_type == "postgres"
    assert hydrated.metadata.dbt_version == "1.12.0b1"


def test_exported_descriptor_is_canonical_json_with_trailing_newline(tmp_path):
    manifest_json, partial_parse = write_sources(tmp_path, make_manifest())

    descriptor = export_runtime_artifacts(
        manifest_path=manifest_json,
        partial_parse_path=partial_parse,
        output_dir=tmp_path / "shared",
        service_name="finance",
        release_id="r1",
        image_tag="sha-1",
        artifact_uri="s3://continuo/finance/r1/partial_parse.msgpack",
        controller_context=CONTROLLER_CONTEXT,
    )

    raw = (tmp_path / "shared" / "runtime-manifest.json").read_text()
    assert raw.endswith("\n")
    assert raw == json.dumps(descriptor, sort_keys=True, separators=(",", ":")) + "\n"


def test_export_missing_manifest_raises(tmp_path):
    _, partial_parse = write_sources(tmp_path, make_manifest())

    with pytest.raises(RuntimeError, match="manifest missing"):
        export_runtime_artifacts(
            manifest_path=tmp_path / "absent.json",
            partial_parse_path=partial_parse,
            output_dir=tmp_path / "shared",
            service_name="finance",
            release_id="r1",
            image_tag="sha-1",
            artifact_uri="s3://continuo/finance/r1/partial_parse.msgpack",
            controller_context=CONTROLLER_CONTEXT,
        )


def test_export_rejects_unhydratable_partial_parse(tmp_path):
    manifest_json, partial_parse = write_sources(tmp_path, make_manifest())
    partial_parse.write_bytes(b"not msgpack")

    with pytest.raises(msgpack.exceptions.ExtraData):
        export_runtime_artifacts(
            manifest_path=manifest_json,
            partial_parse_path=partial_parse,
            output_dir=tmp_path / "shared",
            service_name="finance",
            release_id="r1",
            image_tag="sha-1",
            artifact_uri="s3://continuo/finance/r1/partial_parse.msgpack",
            controller_context=CONTROLLER_CONTEXT,
        )


def test_export_rejects_non_s3_artifact_uri(tmp_path):
    manifest_json, partial_parse = write_sources(tmp_path, make_manifest())

    with pytest.raises(RuntimeError, match="artifact_uri"):
        export_runtime_artifacts(
            manifest_path=manifest_json,
            partial_parse_path=partial_parse,
            output_dir=tmp_path / "shared",
            service_name="finance",
            release_id="r1",
            image_tag="sha-1",
            artifact_uri="/local/partial_parse.msgpack",
            controller_context=CONTROLLER_CONTEXT,
        )


# --- parse context --------------------------------------------------------


def test_parse_context_is_stable_for_identical_inputs(monkeypatch):
    for key in PARSE_CONTEXT_ENV_KEYS:
        monkeypatch.setenv(key, "same")
    manifest = fake_manifest()

    first = parse_context_sha256(manifest, CONTROLLER_CONTEXT)
    second = parse_context_sha256(fake_manifest(), CONTROLLER_CONTEXT)

    assert first == second
    assert len(first) == 64


def test_parse_context_changes_with_state_check(monkeypatch):
    for key in PARSE_CONTEXT_ENV_KEYS:
        monkeypatch.setenv(key, "same")

    assert parse_context_sha256(fake_manifest("v1"), CONTROLLER_CONTEXT) != (
        parse_context_sha256(fake_manifest("v2"), CONTROLLER_CONTEXT)
    )


def test_parse_context_changes_with_controller_context(monkeypatch):
    for key in PARSE_CONTEXT_ENV_KEYS:
        monkeypatch.setenv(key, "same")
    manifest = fake_manifest()

    assert parse_context_sha256(manifest, '{"command_dialect_sha256":"abc"}') != (
        parse_context_sha256(manifest, '{"command_dialect_sha256":"def"}')
    )


def test_parse_context_changes_with_an_allowlisted_env_var(monkeypatch):
    for key in PARSE_CONTEXT_ENV_KEYS:
        monkeypatch.setenv(key, "same")
    manifest = fake_manifest()
    before = parse_context_sha256(manifest, CONTROLLER_CONTEXT)

    monkeypatch.setenv(PARSE_CONTEXT_ENV_KEYS[0], "different")

    assert parse_context_sha256(manifest, CONTROLLER_CONTEXT) != before


def test_parse_context_ignores_the_candidate_target_schema(monkeypatch):
    """A compile pod's candidate schema must not fork the digest.

    The digest is recomputed in the production worker, which runs against the
    prod schema and hard-fails on mismatch. Hashing DBT_TARGET_SCHEMA would
    make every pool permanently unready.
    """
    for key in PARSE_CONTEXT_ENV_KEYS:
        monkeypatch.setenv(key, "same")
    manifest = fake_manifest()

    monkeypatch.setenv("DBT_TARGET_SCHEMA", "_candidate_rel1")
    candidate = parse_context_sha256(manifest, CONTROLLER_CONTEXT)
    monkeypatch.setenv("DBT_TARGET_SCHEMA", "analytics")
    production = parse_context_sha256(manifest, CONTROLLER_CONTEXT)

    assert candidate == production


def test_parse_context_allowlist_excludes_schema_and_credentials():
    assert "DBT_TARGET_SCHEMA" not in PARSE_CONTEXT_ENV_KEYS
    assert "DBT_POSTGRES_PASSWORD" not in PARSE_CONTEXT_ENV_KEYS


def test_parse_context_payload_never_carries_plaintext_env_values(monkeypatch):
    """Allowlisted env values reach the payload only as hashes.

    Asserted against the payload rather than the digest: a digest is hex, so it
    cannot contain a plaintext value under any implementation and asserting
    against it would pass even if the payload embedded the value verbatim.
    """
    for key in PARSE_CONTEXT_ENV_KEYS:
        monkeypatch.setenv(key, "super-secret-value")
    manifest = fake_manifest()

    payload = _parse_context_payload(manifest, CONTROLLER_CONTEXT)

    assert "super-secret-value" not in json.dumps(payload)
    for key in PARSE_CONTEXT_ENV_KEYS:
        assert payload["parse_env_sha256"][key] == (
            hashlib.sha256(b"super-secret-value").hexdigest()
        )


def test_parse_context_digest_is_the_hash_of_the_canonical_payload(monkeypatch):
    """Binds the digest to the payload the plaintext test inspects."""
    for key in PARSE_CONTEXT_ENV_KEYS:
        monkeypatch.setenv(key, "same")
    manifest = fake_manifest()

    payload = _parse_context_payload(manifest, CONTROLLER_CONTEXT)
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()

    assert parse_context_sha256(manifest, CONTROLLER_CONTEXT) == (
        hashlib.sha256(canonical).hexdigest()
    )


def test_parse_context_treats_unset_env_as_empty(monkeypatch):
    for key in PARSE_CONTEXT_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    manifest = fake_manifest()

    unset = parse_context_sha256(manifest, CONTROLLER_CONTEXT)

    for key in PARSE_CONTEXT_ENV_KEYS:
        monkeypatch.setenv(key, "")
    assert parse_context_sha256(manifest, CONTROLLER_CONTEXT) == unset


# --- descriptor validation ------------------------------------------------


def valid_descriptor() -> dict:
    return {
        "format": FORMAT,
        "service_name": "finance",
        "release_id": "r1",
        "image_tag": "sha-1",
        "artifact_uri": "s3://continuo/finance/r1/partial_parse.msgpack",
        "sha256": "a" * 64,
        "dbt_core_version": "1.12.0b1",
        "adapter_type": "postgres",
        "parse_context_sha256": "b" * 64,
    }


def test_validate_descriptor_accepts_a_complete_descriptor():
    validate_descriptor(valid_descriptor())


def test_validate_descriptor_rejects_an_unknown_format():
    descriptor = valid_descriptor()
    descriptor["format"] = "dbt-partial-parse-msgpack-v2"
    with pytest.raises(RuntimeError, match="format"):
        validate_descriptor(descriptor)


@pytest.mark.parametrize("field", sorted(valid_descriptor().keys()))
def test_validate_descriptor_rejects_a_missing_field(field):
    descriptor = valid_descriptor()
    del descriptor[field]
    with pytest.raises(RuntimeError, match=field):
        validate_descriptor(descriptor)


@pytest.mark.parametrize("field", sorted(valid_descriptor().keys()))
def test_validate_descriptor_rejects_an_empty_field(field):
    descriptor = valid_descriptor()
    descriptor[field] = ""
    with pytest.raises(RuntimeError, match=field):
        validate_descriptor(descriptor)


@pytest.mark.parametrize("field", ["sha256", "parse_context_sha256"])
@pytest.mark.parametrize("bad", ["A" * 64, "a" * 63, "z" * 64])
def test_validate_descriptor_rejects_a_malformed_digest(field, bad):
    descriptor = valid_descriptor()
    descriptor[field] = bad
    with pytest.raises(RuntimeError, match=field):
        validate_descriptor(descriptor)


def test_validate_descriptor_rejects_a_non_s3_artifact_uri():
    descriptor = valid_descriptor()
    descriptor["artifact_uri"] = "https://continuo/finance/r1/partial_parse.msgpack"
    with pytest.raises(RuntimeError, match="artifact_uri"):
        validate_descriptor(descriptor)
