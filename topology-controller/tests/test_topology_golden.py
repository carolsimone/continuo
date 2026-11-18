import json

import pytest

from tests.fingerprint_fixtures import VARIANTS
from tests.topology_golden import FIXTURES, GOLDEN, GOLDEN_DIR, MANIFEST_RELEASES, golden_path, release_outputs


@pytest.mark.parametrize("name", GOLDEN)
def test_the_topology_and_code_bundle_are_unchanged(name):
    committed = json.loads(golden_path(name).read_text(encoding="utf-8"))
    assert release_outputs(name) == committed, (
        f"the topology or code bundle of release {name!r} differs from {golden_path(name)}"
    )


def test_every_compiled_variant_and_every_fixture_manifest_is_in_a_golden_release():
    assert {f"fpshop_{variant}" for variant in VARIANTS} <= set(GOLDEN)
    manifests = {
        path.name for path in FIXTURES.glob("manifest_*.json") if "nodes" in json.loads(path.read_text("utf-8"))
    }
    used = {file for release in MANIFEST_RELEASES.values() for file, _ in release}
    assert manifests == used


def test_the_golden_directory_holds_one_file_per_release():
    assert sorted(path.name for path in GOLDEN_DIR.glob("*")) == sorted(f"{name}.json" for name in GOLDEN)
