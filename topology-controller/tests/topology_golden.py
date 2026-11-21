"""Captures the topology and the code bundle that CandidateManifestHandler builds for each fixture release.

`release_outputs(name)` runs the handler, with the recording fakes of tests/fakes.py and a clock
that never moves, over one release made of fixture manifests and returns the nodes of the topology
artifact it writes and the code bundle it uploads. `GOLDEN` names the releases: each compiled
variant of the fpshop dbt project (tests/fixtures/dbt_fingerprints) and the releases the
tests/fixtures/manifest_*.json files make, among them the ones with seeds, snapshots, tests, an
installed dbt package, dormant nodes, a seed without tags and a table no node builds.

The committed files under tests/fixtures/topology_golden/ hold these outputs, one file per release.
`contract_bundle_digest(name)` captures the contract bundle the same handler writes for the release
with the ontology switch off, as one `sha256:<hex>` digest; tests/fixtures/contract_bundle_digests.json
holds them all. The prefix keeps a secret scanner from reading a bare 64-character hex value next to a
key such as `access` as a credential.

    uv run --frozen python -m tests.topology_golden --write

rewrites the golden files and the digests from the code as it stands.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from continuo_contracts.canonical import canonical_json

from domain.model import ManifestFile
from tests.fakes import (
    FakeBundleUploader,
    RecordingArtifactWriter,
    RecordingContractBundleWriter,
    build_handler,
    make_uploader,
    source_of,
)
from tests.fingerprint_fixtures import VARIANTS, variant_paths

logger = logging.getLogger(__name__)

FIXTURES = Path(__file__).parent / "fixtures"
GOLDEN_DIR = FIXTURES / "topology_golden"
DIGESTS_PATH = FIXTURES / "contract_bundle_digests.json"

RELEASE_ID = "rel-1"

# The image of each service a release names, so that a node's image_tag is part of what is captured.
IMAGE_TAGS = {
    "fpshop": "registry.example/fpshop:1",
    "service-1": "registry.example/service-1:1",
    "service-2": "registry.example/service-2:1",
    "service-4": "registry.example/service-4:1",
    "shop-core": "registry.example/shop-core:1",
}

# The service the fpshop variants declare, which their compile extras name too.
FPSHOP_SERVICE = "fpshop"

# Each release the fixture manifests make, as the (file, declared service) pairs of its manifests. A
# release request declares the service of every manifest, so each pair does, except
# manifest_missing_owner.json: the parse leaves no node of it, and a manifest that declares a service
# and holds no node is refused.
MANIFEST_RELEASES: dict[str, tuple[tuple[str, str], ...]] = {
    "manifest_valid": (("manifest_valid.json", "service-1"),),
    "manifest_missing_owner": (("manifest_missing_owner.json", ""),),
    "manifest_missing_tags": (("manifest_missing_tags.json", "service-1"),),
    "manifest_seed_no_tags": (("manifest_seed_no_tags.json", "service-1"),),
    "manifest_installed_package": (("manifest_installed_package.json", "shop-core"),),
    "manifest_with_test": (("manifest_with_test.json", "service-1"),),
    "manifest_service1": (("manifest_service1.json", "service-1"),),
    "manifest_service2": (("manifest_service2.json", "service-2"),),
    "manifest_service4": (("manifest_service4.json", "service-4"),),
    # service-2 reads a table service-1 builds, and service-4 one no node of the release builds.
    "manifests_service1_service2_service4": (
        ("manifest_service1.json", "service-1"),
        ("manifest_service2.json", "service-2"),
        ("manifest_service4.json", "service-4"),
    ),
}

# The base variant parsed without the compile extras it ships with, as a service whose compile wrote
# none is: its seeds keep dbt's path checksum.
_FPSHOP_WITHOUT_EXTRAS = "fpshop_base_without_extras"

GOLDEN: tuple[str, ...] = (
    *MANIFEST_RELEASES,
    *(f"fpshop_{variant}" for variant in VARIANTS),
    _FPSHOP_WITHOUT_EXTRAS,
)


def release_outputs(name: str) -> dict[str, Any]:
    """The `topology_nodes` the handler writes to the topology artifact and the `code_bundle` it
    uploads for release `name`, as plain JSON values.

    The topology artifact is a set of nodes: its writer sorts the nodes by unique_id and each node's
    upstream_unique_ids, so `topology_nodes` is sorted the same way and the order the handler
    happened to produce them in is not part of the capture. Every field of each node and of the
    bundle is kept, and neither document holds a time, a path or an id that differs from one run to
    the next."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        manifests = _manifest_files(name, tmp_dir)
        publisher, artifact_writer, bundle_uploader = MagicMock(), RecordingArtifactWriter(), FakeBundleUploader()
        build_handler(
            source_of(*manifests), publisher, make_uploader(),
            bundle_uploader=bundle_uploader, artifact_writer=artifact_writer, image_tags=IMAGE_TAGS,
        ).handle(release_id=RELEASE_ID)
    if publisher.publish_ok.call_count != 1:
        raise RuntimeError(f"release {name!r} was not published ok: {publisher.publish_failed.call_args_list}")
    ((_, code_bundle),) = bundle_uploader.uploads
    return _as_json({
        "topology_nodes": [_sorted_upstreams(node) for node in sorted(artifact_writer.nodes, key=_unique_id)],
        "code_bundle": code_bundle,
    })


def contract_bundle_digest(name: str, *, ontology_enabled: bool = False) -> str:
    """The digest of the canonical JSON of the contract bundle the handler writes for release `name`,
    as `sha256:<hex>` (the form every content hash takes), without the bundle's `continuo_contracts`
    and `timings` keys: the version of the library does not show, so a new version that stores every
    contract as before leaves the digest where it was, and the validation cache's counts, which
    depend on the releases the process handled before, do not either."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        publisher, bundle_writer = MagicMock(), RecordingContractBundleWriter()
        build_handler(
            source_of(*_manifest_files(name, tmp_dir)), publisher, make_uploader(),
            bundle_uploader=FakeBundleUploader(), contract_bundle_writer=bundle_writer, image_tags=IMAGE_TAGS,
            ontology_enabled=ontology_enabled,
        ).handle(release_id=RELEASE_ID)
    if publisher.publish_ok.call_count != 1:
        raise RuntimeError(f"release {name!r} was not published ok: {publisher.publish_failed.call_args_list}")
    left_out = {"continuo_contracts", "timings"}
    document = {key: value for key, value in bundle_writer.document.items() if key not in left_out}
    return f"sha256:{hashlib.sha256(canonical_json(document)).hexdigest()}"


def golden_path(name: str) -> Path:
    """The committed file that holds the outputs of release `name`."""
    return GOLDEN_DIR / f"{name}.json"


def _manifest_files(name: str, tmp_dir: str) -> list[ManifestFile]:
    """The manifests of release `name`, with the variants' gunzipped into tmp_dir."""
    if name in MANIFEST_RELEASES:
        return [
            ManifestFile(path=str(FIXTURES / file), image_tag="", declared_service=service)
            for file, service in MANIFEST_RELEASES[name]
        ]
    if name == _FPSHOP_WITHOUT_EXTRAS:
        manifest_path, _ = variant_paths("base", tmp_dir)
        return [ManifestFile(path=manifest_path, image_tag="", declared_service=FPSHOP_SERVICE)]
    variant = name.removeprefix("fpshop_")
    if name.startswith("fpshop_") and variant in VARIANTS:
        manifest_path, extras_path = variant_paths(variant, tmp_dir)
        return [ManifestFile(
            path=manifest_path, image_tag="", declared_service=FPSHOP_SERVICE, extras_path=extras_path,
        )]
    raise KeyError(f"no fixture release named {name!r}")


def _unique_id(node: Mapping[str, Any]) -> str:
    return node["unique_id"]


def _sorted_upstreams(node: Mapping[str, Any]) -> dict[str, Any]:
    return {**node, "upstream_unique_ids": sorted(node["upstream_unique_ids"])}


def _as_json(document: Mapping[str, Any]) -> dict[str, Any]:
    """The document as a JSON round trip returns it: enum members as their values, tuples as lists."""
    return json.loads(golden_text(document))


def golden_text(document: Mapping[str, Any]) -> str:
    """The text of a golden file: sorted keys, so that the order a dict was filled in does not show."""
    return json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def write_golden_files() -> None:
    """Write the outputs of every release in GOLDEN to its file, and remove a file no release owns."""
    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    for stale in sorted(set(GOLDEN_DIR.glob("*.json")) - {golden_path(name) for name in GOLDEN}):
        stale.unlink()
        logger.info("removed %s", stale.name)
    for name in GOLDEN:
        golden_path(name).write_text(golden_text(release_outputs(name)), encoding="utf-8")
        logger.info("wrote %s", golden_path(name).name)
    digests = {name: contract_bundle_digest(name) for name in GOLDEN}
    DIGESTS_PATH.write_text(json.dumps(digests, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    logger.info("wrote %s", DIGESTS_PATH.name)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--write", action="store_true", required=True, help="rewrite the golden files")
    parser.parse_args(argv)
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
    logger.setLevel(logging.INFO)
    write_golden_files()


if __name__ == "__main__":
    main()
