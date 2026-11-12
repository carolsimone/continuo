"""Read the compiled variants of the fpshop dbt fixture project.

Each variant under tests/fixtures/dbt_fingerprints/manifests/<variant>/ holds the
manifest.json.gz and compile-extras.json that real dbt wrote for the fixture project
with one edit applied (tests/fixtures/dbt_fingerprints/variants.py names the edits,
generate.py compiles them). These helpers parse a variant with the topology-controller
parser and compare its content hashes with another variant's.
"""
from __future__ import annotations

import atexit
import gzip
import json
import os
import shutil
import tempfile
from functools import cache
from pathlib import Path

from domain.model import CompileExtras, ManifestNode, NodeRegistry, ParsedArtifact

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


def node_of(parsed: ParsedArtifact, unique_id: str) -> ManifestNode:
    """The node of the parsed artifact whose unique id is unique_id."""
    return next(node for node in parsed.nodes if node.unique_id == unique_id)


def parse_and_resolve(name: str) -> ParsedArtifact:
    """The parsed artifact of a variant with each node's upstream_deps resolved
    against the registry of its non-test nodes, as the candidate handler builds it."""
    from service.resolver import resolve_upstream_deps
    from service.table_refs import referenced_tables

    parsed = parse_variant(name)
    registry = NodeRegistry.of_nodes(parsed.nodes)
    lookup = registry.to_lookup()
    ephemeral_ctes = registry.ephemeral_ctes()
    for node in parsed.nodes:
        ctes: set[str] = set()
        refs = referenced_tables(node, dialect="postgres", ctes=ctes)
        node.upstream_deps = resolve_upstream_deps(
            refs, lookup, service_name=node.service_name, ctes=ctes, ephemeral_ctes=ephemeral_ctes,
        )
    return parsed


def candidate_sql_of(name: str, unique_id: str) -> str:
    """The candidate SQL the dbt builder uploads for one node of a variant, for a
    release whose id is rel-1, so the candidate schema is _candidate_rel_1."""
    from config.config import _SYSTEM_RELATIONS
    from service.candidate_artifacts import DbtSqlArtifactBuilder, RewriteContext
    from service.release_timings import ReleaseTimings
    from service.rewriter import candidate_schema_name
    from tests.fakes import FakeClock, ScriptedMemoryProbe

    parsed = parse_variant(name)
    uploaded: dict[str, str] = {}

    class _RecordingUploader:
        def upload(self, release_id: str, unique_id: str, sql: str) -> str:
            uploaded[unique_id] = sql
            return ""

    ctx = RewriteContext(
        release_id="rel-1",
        registry=NodeRegistry.of_nodes(parsed.nodes).to_lookup(),
        candidate_schema=candidate_schema_name("rel-1"),
        dialect="postgres",
        system_relations=_SYSTEM_RELATIONS["postgres"],
        timings=ReleaseTimings("rel-1", FakeClock(), ScriptedMemoryProbe(0)),
    )
    DbtSqlArtifactBuilder(_RecordingUploader()).build(node_of(parsed, unique_id), ctx)
    return uploaded[unique_id]


@cache
def base_manifest_path() -> str:
    """The base variant's manifest, gunzipped once into a directory that is removed
    when the test process exits."""
    tmp_dir = tempfile.mkdtemp(prefix="fpshop-base-")
    atexit.register(shutil.rmtree, tmp_dir, True)
    manifest_path, _ = variant_paths("base", tmp_dir)
    return manifest_path


def base_extras() -> CompileExtras:
    """The base variant's compile extras, loaded."""
    from service.compile_extras import load_compile_extras

    return load_compile_extras(str(MANIFESTS_DIR / "base" / "compile-extras.json"), "")


def manifest_checksum(dbt_unique_id: str) -> str:
    """dbt's own checksum of a node of the base variant, as the manifest writes it."""
    with gzip.open(MANIFESTS_DIR / "base" / "manifest.json.gz", "rt", encoding="utf-8") as f:
        manifest = json.load(f)
    return manifest["nodes"][dbt_unique_id]["checksum"]["checksum"]
