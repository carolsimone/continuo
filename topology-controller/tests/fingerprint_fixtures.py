"""Read the compiled variants of the fpshop dbt fixture project.

Each variant under tests/fixtures/dbt_fingerprints/manifests/<variant>/ holds the
manifest.json.gz and compile-extras.json that real dbt wrote for the fixture project
with one edit applied (tests/fixtures/dbt_fingerprints/variants.py names the edits,
generate.py compiles them). These helpers parse a variant with the topology-controller
parser and compare its content hashes with another variant's.
"""
from __future__ import annotations

import gzip
import os
import shutil
import tempfile
from pathlib import Path

from domain.model import ParsedArtifact

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "dbt_fingerprints"
MANIFESTS_DIR = FIXTURE_DIR / "manifests"

VARIANTS: tuple[str, ...] = tuple(sorted(p.name for p in MANIFESTS_DIR.iterdir() if p.is_dir()))


def variant_paths(name: str, tmp_dir: str) -> tuple[str, str]:
    """The variant's manifest, gunzipped into tmp_dir, and its compile-extras.json path
    ("" when the variant has none)."""
    variant_dir = MANIFESTS_DIR / name
    manifest_path = os.path.join(tmp_dir, f"{name}.manifest.json")
    with gzip.open(variant_dir / "manifest.json.gz", "rb") as src, open(manifest_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    extras = variant_dir / "compile-extras.json"
    return manifest_path, str(extras) if extras.is_file() else ""


def parse_variant(name: str, *, extras: bool = True) -> ParsedArtifact:
    """The parsed artifact of a variant, with its compile extras unless extras is False."""
    from service.compile_extras import load_compile_extras
    from service.parser import parse_manifest

    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest_path, extras_path = variant_paths(name, tmp_dir)
        loaded = load_compile_extras(extras_path, "") if extras and extras_path else None
        return parse_manifest(manifest_path, "", loaded)


def hashes(name: str, *, extras: bool = True) -> dict[str, str]:
    """Each node's unique id mapped to its content_hash in the variant."""
    return {node.unique_id: node.content_hash for node in parse_variant(name, extras=extras).nodes}


def moved(base: str, variant: str, *, extras: bool = True) -> set[str]:
    """The unique ids whose content_hash differs between the two variants, plus the ids
    present in only one of them."""
    before = hashes(base, extras=extras)
    after = hashes(variant, extras=extras)
    return {uid for uid in before.keys() | after.keys() if before.get(uid) != after.get(uid)}
