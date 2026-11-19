import json

import pytest

from tests.fingerprint_fixtures import MANIFESTS_DIR
from tests.topology_golden import FIXTURES, GOLDEN, GOLDEN_DIR, MANIFEST_RELEASES, golden_path, golden_text, release_outputs


def _first_difference(actual: str, committed: str) -> str:
    """Where two texts first differ: the line number and both lines."""
    actual_lines, committed_lines = actual.splitlines(), committed.splitlines()
    for number, (got, want) in enumerate(zip(actual_lines, committed_lines), start=1):
        if got != want:
            return f"line {number}: got {got.strip()!r}, committed {want.strip()!r}"
    return f"the text has {len(actual_lines)} lines, the committed file {len(committed_lines)}"


@pytest.mark.parametrize("name", GOLDEN)
def test_the_topology_and_code_bundle_are_unchanged(name):
    """The text is compared, not the parsed JSON, which would call True and 1, or 1 and 1.0, equal."""
    actual = golden_text(release_outputs(name))
    committed = golden_path(name).read_text(encoding="utf-8")
    if actual != committed:
        pytest.fail(f"release {name!r} differs from {golden_path(name).name}, first at {_first_difference(actual, committed)}")


def test_every_compiled_variant_has_the_manifest_and_compile_extras_its_release_is_built_from():
    """A variant with no compile-extras.json would be captured as a release with none."""
    variants = [path for path in MANIFESTS_DIR.iterdir() if path.is_dir()]
    assert variants
    for variant in variants:
        assert (variant / "manifest.json.gz").is_file(), variant.name
        assert (variant / "compile-extras.json").is_file(), variant.name
        assert f"fpshop_{variant.name}" in GOLDEN, variant.name


def test_every_fixture_manifest_is_in_a_golden_release():
    manifests = {
        path.name for path in FIXTURES.glob("manifest_*.json") if "nodes" in json.loads(path.read_text("utf-8"))
    }
    used = {file for release in MANIFEST_RELEASES.values() for file, _ in release}
    assert manifests == used


def test_the_golden_directory_holds_one_file_per_release():
    assert sorted(path.name for path in GOLDEN_DIR.glob("*")) == sorted(f"{name}.json" for name in GOLDEN)
