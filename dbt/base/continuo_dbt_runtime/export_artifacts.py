"""Export a compiled dbt project's runtime artifacts into one shared directory.

A team's compile leg leaves a manifest.json and a partial_parse.msgpack behind.
This module copies both into a directory the release's uploader reads, and
writes the descriptor that binds them to the release, the image, and the parse
context they were produced under.
"""
import hashlib
import json
import shutil
from pathlib import Path

from dbt.contracts.graph.manifest import Manifest

from continuo_dbt_runtime.descriptor import FORMAT, validate_descriptor
from continuo_dbt_runtime.parse_context import parse_context_sha256


def export_runtime_artifacts(
    *,
    manifest_path: Path,
    partial_parse_path: Path,
    output_dir: Path,
    service_name: str,
    release_id: str,
    image_tag: str,
    artifact_uri: str,
    controller_context: str,
) -> dict:
    """Write manifest.json, partial_parse.msgpack, and their descriptor.

    The partial parse is hydrated before it is described, so an artifact that no
    worker could load never reaches the output directory. Returns the descriptor.
    """
    manifest_path = Path(manifest_path)
    partial_parse_path = Path(partial_parse_path)
    output_dir = Path(output_dir)

    if not manifest_path.is_file():
        raise RuntimeError(f"manifest missing: {manifest_path}")
    if not partial_parse_path.is_file():
        raise RuntimeError(f"partial parse missing: {partial_parse_path}")

    packed = partial_parse_path.read_bytes()
    manifest = Manifest.from_msgpack(packed)
    descriptor = {
        "format": FORMAT,
        "service_name": service_name,
        "release_id": release_id,
        "image_tag": image_tag,
        "artifact_uri": artifact_uri,
        "sha256": hashlib.sha256(packed).hexdigest(),
        "dbt_core_version": manifest.metadata.dbt_version,
        "adapter_type": manifest.metadata.adapter_type,
        "parse_context_sha256": parse_context_sha256(manifest, controller_context),
    }
    validate_descriptor(descriptor)

    output_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(manifest_path, output_dir / "manifest.json")
    (output_dir / "partial_parse.msgpack").write_bytes(packed)
    (output_dir / "runtime-manifest.json").write_text(
        json.dumps(descriptor, sort_keys=True, separators=(",", ":")) + "\n"
    )
    return descriptor
