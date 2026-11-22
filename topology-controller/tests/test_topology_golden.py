import json
import re

import pytest

from tests.fingerprint_fixtures import MANIFESTS_DIR
from tests.topology_golden import (
    DIGESTS_PATH,
    FIXTURES,
    GOLDEN,
    GOLDEN_DIR,
    MANIFEST_RELEASES,
    TRINO_RELEASES,
    contract_bundle_digest,
    golden_path,
    golden_text,
    release_outputs,
)


def _first_difference(actual: str, committed: str) -> str:
    """Where two texts first differ: the line number and both lines."""
    actual_lines, committed_lines = actual.splitlines(), committed.splitlines()
    for number, (got, want) in enumerate(zip(actual_lines, committed_lines), start=1):
        if got != want:
            return f"line {number}: got {got.strip()!r}, committed {want.strip()!r}"
    return f"the text has {len(actual_lines)} lines, the committed file {len(committed_lines)}"


@pytest.mark.parametrize("name", GOLDEN)
def test_the_topology_code_bundle_and_candidate_sql_are_unchanged(name):
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
    used = {file for release in (*MANIFEST_RELEASES.values(), *TRINO_RELEASES.values()) for file, _ in release}
    assert manifests == used


def test_the_golden_directory_holds_one_file_per_release():
    assert sorted(path.name for path in GOLDEN_DIR.glob("*")) == sorted(f"{name}.json" for name in GOLDEN)


# --- what a Trino release pins, said in words: the golden files hold the same facts as text ---

def _trino_nodes() -> dict:
    return {node["unique_id"]: node for node in release_outputs("trino_manifest_trino")["topology_nodes"]}


def test_a_trino_read_under_a_catalog_of_a_table_no_node_builds_is_an_unknown_external_node_by_schema_and_table():
    nodes = _trino_nodes()

    assert nodes["raw.events"]["node_type"] == "unknown-external"
    assert nodes["test_schema.trino_events"]["upstream_unique_ids"] == ["raw.events"]


def test_a_trino_read_of_the_engines_own_catalogs_is_neither_an_edge_nor_a_node():
    nodes = _trino_nodes()

    assert nodes["test_schema.trino_health"]["upstream_unique_ids"] == []
    assert sorted(nodes) == [
        "raw.events", "test_schema.trino_daily", "test_schema.trino_events",
        "test_schema.trino_health", "test_schema.trino_report",
    ]


def test_a_trino_read_that_writes_the_warehouses_catalog_is_an_edge_and_is_redirected_to_the_candidate_schema():
    outputs = release_outputs("trino_manifest_trino")
    nodes = {node["unique_id"]: node for node in outputs["topology_nodes"]}

    assert nodes["test_schema.trino_report"]["upstream_unique_ids"] == ["test_schema.trino_daily"]
    assert outputs["candidate_sql"]["test_schema.trino_report"] == (
        'SELECT d.day, d.n FROM warehouse."_candidate_rel_1".trino_daily AS d')


def test_the_candidate_sql_of_a_trino_release_keeps_a_cte_a_read_the_engine_owns_and_an_external_read_as_they_are():
    sql = release_outputs("trino_manifest_trino")["candidate_sql"]

    assert "latest AS (SELECT * FROM \"_candidate_rel_1\".trino_events)" in sql["test_schema.trino_daily"]
    assert "FROM system.runtime.nodes AS n CROSS JOIN information_schema.tables AS t" in sql["test_schema.trino_health"]
    assert "FROM hive.raw.events AS e" in sql["test_schema.trino_events"]


def test_the_contract_bundle_digests_name_every_release():
    assert sorted(json.loads(DIGESTS_PATH.read_text(encoding="utf-8"))) == sorted(GOLDEN)


def test_every_contract_bundle_digest_is_a_prefixed_sha256_value():
    """A bare 64-character hex value beside a key such as `access` reads as a credential to a secret
    scanner, so each digest is stored as `sha256:<hex>`."""
    committed = json.loads(DIGESTS_PATH.read_text(encoding="utf-8"))
    assert {name: value for name, value in committed.items() if not re.fullmatch(r"sha256:[0-9a-f]{64}", value)} == {}


@pytest.mark.parametrize("name", GOLDEN)
def test_the_switch_off_keeps_every_golden(name):
    """With the ontology switch off, the contract bundle of each fixture release is the one it was
    before an ontology could be released: its stored contracts are hashed with their ontology part
    taken away, and its `ontology` value is null. The digest committed for the release is the one
    the bundle had before, with the version of the library left out."""
    committed = json.loads(DIGESTS_PATH.read_text(encoding="utf-8"))
    assert contract_bundle_digest(name, ontology_enabled=False) == committed[name]
