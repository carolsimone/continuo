"""Holds every input the content hashing reads to continuo-contracts' fingerprints.yaml.

A name the hashing reads that fingerprints.yaml does not classify would change a node's
content_hash without the contract saying whether that change is a code change or a
contract change. These tests collect the input names of every dbt fixture variant, with
and without compile extras, and the keys of the python code projection, and require each
to be classified for its family.

The same holds for the contract side: a field of a stored dbt contract that no entry of
fingerprints.yaml classifies as moving the contract hash would change contract_hash with
no declaration saying so. The stored contracts of every variant are the ones the handler
writes to the contract bundle, and their fields are named the way the file names them.
"""
import json
import tempfile
from collections.abc import Iterator
from functools import cache
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import continuo_contracts
import pytest
from continuo_contracts import fingerprints, ontology
from continuo_contracts.fingerprints import unclassified
from continuo_contracts.python import PythonColumn, PythonNode, code_projection
from continuo_contracts.vocabulary import continuo_values

from domain.model import ManifestFile
from service.dbt_hashing import hash_dbt_node
from service.ontology_binding import ReleaseOntology, bind_contract
from tests.fakes import (
    FakeBundleUploader,
    RecordingArtifactWriter,
    RecordingContractBundleWriter,
    build_handler,
    make_uploader,
    source_of,
)
from tests.fingerprint_fixtures import (
    VARIANTS,
    column_tests,
    enforced_dbt_node,
    manifest_and_context,
    stored_dbt_contracts,
    variant_paths,
)

FAMILY = {"model": "dbt-model", "seed": "dbt-seed", "snapshot": "dbt-snapshot", "test": "dbt-test"}


def test_every_input_the_dbt_hashing_reads_is_classified():
    seen: dict[str, set[str]] = {}
    for variant in VARIANTS:
        for extras in (True, False):
            manifest, ctx = manifest_and_context(variant, extras=extras)
            for node in manifest["nodes"].values():
                family = FAMILY.get(node["resource_type"])
                if family:
                    seen.setdefault(family, set()).update(hash_dbt_node(node, ctx).inputs)
    assert set(seen) == set(FAMILY.values())
    for family, names in seen.items():
        assert unclassified(family, names) == [], family


def test_every_python_projection_key_is_classified():
    node = PythonNode(
        schema="analytics",
        table="t",
        script="scripts/t.py",
        reads={"r": "select 1 as id"},
        output_columns=[PythonColumn("id", "INTEGER")],
    )
    keys = {f"projection.{k}" for k in code_projection(node)} | {"ci.source_hash", "ci.shared_code_hash"}
    for family in ("python-node", "python-csv", "python-api"):
        assert unclassified(family, keys) == [], family


def test_the_run_end_hook_input_is_classified_for_every_dbt_family_that_folds_hooks():
    for family in ("dbt-model", "dbt-seed", "dbt-snapshot"):
        assert unclassified(family, {"on_run_end"}) == [], family


# The keys whose value is free-form: the field is named, and its content is not walked.
_FREE_FORM = frozenset(
    {"examples", "implementation", "arguments", "mustBe", "mustNotBe", "value", "valueExt", "logicalTypeOptions"}
)

# The project the fpshop variants declare, which their compile extras name too.
_FPSHOP_PROJECT = "fpshop"

_DBT_FAMILIES = ("dbt-model", "dbt-seed", "dbt-snapshot")


# The walk below mirrors the one in continuo-contracts' tests/test_fingerprints_guard.py, so a
# field gets here the name that file's inputs lists give it.
def _holds_objects(value: Any) -> bool:
    return isinstance(value, dict) or (isinstance(value, list) and any(isinstance(item, dict) for item in value))


def _walk(node: Any, path: str, names: set[str]) -> None:
    """Add to names the `odcs.<path>` of each field under node, array indexes dropped.

    A `continuo` custom property is `<path>.custom.<property>`, another vendor's is
    `<path>.customProperties`, and the walk stops at a free-form value."""
    if isinstance(node, list):
        for item in node:
            _walk(item, path, names)
        return
    for key, value in node.items():
        if key == "customProperties":
            for item in value:
                is_continuo = item["vendor"] == "continuo"
                names.add(f"{path}.custom.{item['property']}" if is_continuo else f"{path}.customProperties")
        elif key not in _FREE_FORM and _holds_objects(value):
            _walk(value, f"{path}.{key}", names)
        else:
            names.add(f"{path}.{key}")


def _odcs_inputs(stored: dict) -> set[str]:
    """The `odcs.<path>` of every field of a stored contract that contract_hash hashes, named as
    the inputs of fingerprints.yaml are: the root keys the hash leaves out are not fields."""
    left_out = fingerprints.contract_hash_exclusions()
    names: set[str] = set()
    _walk({key: value for key, value in stored.items() if key not in left_out}, "odcs", names)
    return names


@cache
def _stored_dbt_contracts(variant: str) -> tuple[tuple[str, str, dict], ...]:
    """The (family, unique_id, stored form) of each dbt contract the handler writes to the
    contract bundle of the release one variant makes, with its compile extras. The family is
    the node's kind (`dbt-model`, `dbt-seed` or `dbt-snapshot`)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest_path, extras_path = variant_paths(variant, tmp_dir)
        manifest = ManifestFile(
            path=manifest_path, image_tag="", declared_project=_FPSHOP_PROJECT, extras_path=extras_path
        )
        publisher, bundle_writer = MagicMock(), RecordingContractBundleWriter()
        build_handler(
            source_of(manifest), publisher, make_uploader(), bundle_uploader=FakeBundleUploader(),
            artifact_writer=RecordingArtifactWriter(), contract_bundle_writer=bundle_writer,
        ).handle(release_id="rel-1")
    assert publisher.publish_ok.call_count == 1, (variant, publisher.publish_failed.call_args_list)
    return tuple(
        (continuo_values(entry["document"]["customProperties"])["kind"], unique_id, entry["document"])
        for unique_id, entry in sorted(bundle_writer.document["contracts"].items())
        if entry["family"] == "dbt"
    )


def _all_stored_dbt_contracts() -> Iterator[tuple[str, str, str, dict]]:
    for variant in VARIANTS:
        for family, unique_id, stored in _stored_dbt_contracts(variant):
            yield variant, family, unique_id, stored


def test_the_odcs_inputs_of_a_contract_are_named_as_fingerprints_yaml_names_them():
    stored = {
        "apiVersion": "v3.1.0", "kind": "DataContract", "status": "active", "version": "1.0.0",
        "id": "analytics.t", "name": "t", "tags": ["daily"], "team": {"name": "shop"},
        "customProperties": [
            {"property": "kind", "value": "dbt-model", "vendor": "continuo"},
            {"property": "elsewhere", "value": 1, "vendor": "other"},
        ],
        "schema": [{
            "name": "t", "physicalType": "table",
            "customProperties": [{"property": "reads", "value": ["raw.t"], "vendor": "continuo"}],
            "properties": [
                {"name": "id", "physicalType": "integer", "logicalTypeOptions": {"minimum": 1}, "examples": [1, 2],
                 "quality": [{"metric": "nullValues", "mustBe": 0, "customProperties": [
                     {"property": "failCalc", "value": "count(*)", "vendor": "continuo"}]}]},
                {"name": "amount", "physicalType": "numeric"},
            ],
        }],
    }
    assert _odcs_inputs(stored) == {
        "odcs.id", "odcs.name", "odcs.tags", "odcs.team.name", "odcs.custom.kind", "odcs.customProperties",
        "odcs.schema.name", "odcs.schema.physicalType", "odcs.schema.custom.reads",
        "odcs.schema.properties.name", "odcs.schema.properties.physicalType",
        "odcs.schema.properties.logicalTypeOptions", "odcs.schema.properties.examples",
        "odcs.schema.properties.quality.metric", "odcs.schema.properties.quality.mustBe",
        "odcs.schema.properties.quality.custom.failCalc",
    }


def test_the_stored_dbt_contracts_cover_every_dbt_family_and_the_fields_a_dbt_node_holds():
    """The guard below is not vacuous: the variants' stored contracts hold a model, a seed and a
    snapshot, and between them the fields of each that the fixture project's nodes set. The
    fixture seeds declare no columns, so a seed's property fields are checked by the test of
    declared columns below, and a field no fixture sets is checked by continuo-contracts' own
    guard."""
    names: dict[str, set[str]] = {family: set() for family in _DBT_FAMILIES}
    for _, family, _, stored in _all_stored_dbt_contracts():
        names[family] |= _odcs_inputs(stored)
    every_family = {
        "odcs.id", "odcs.name", "odcs.schema.name", "odcs.schema.physicalName", "odcs.schema.physicalType",
        "odcs.team.name", "odcs.tags", "odcs.custom.kind", "odcs.custom.project", "odcs.custom.schedule",
        "odcs.custom.filePath", "odcs.custom.materialization", "odcs.custom.criticality",
    }
    only = {
        "dbt-model": {
            "odcs.schema.properties.name", "odcs.schema.properties.physicalType",
            "odcs.schema.properties.logicalType", "odcs.schema.properties.required",
            "odcs.schema.properties.quality.metric", "odcs.schema.custom.reads", "odcs.custom.contractEnforced",
            "odcs.custom.access", "odcs.custom.group", "odcs.custom.freshnessBuildAfter", "odcs.description.purpose",
        },
        "dbt-seed": set(),  # the fixture seeds declare no columns
        "dbt-snapshot": {"odcs.custom.snapshot", "odcs.schema.properties.name"},
    }
    floor = {"dbt-model": 25, "dbt-seed": 12, "dbt-snapshot": 15}
    for family in _DBT_FAMILIES:
        expected = every_family | only[family]
        assert expected <= names[family], (family, sorted(expected - names[family]))
        assert len(names[family]) >= floor[family], (family, len(names[family]))


@pytest.mark.parametrize("variant", VARIANTS)
def test_every_field_of_a_stored_dbt_contract_is_classified(variant):
    contracts = _stored_dbt_contracts(variant)
    assert {family for family, _, _ in contracts} >= {"dbt-model"}, variant
    missing: dict[tuple[str, str], str] = {}
    for family, unique_id, stored in contracts:
        for name in _odcs_inputs(stored):
            entry = fingerprints.classify(family, name)
            if entry is None or entry.fingerprint not in ("contract", "both"):
                missing[(family, name)] = unique_id
    assert not missing, (variant, missing)


@pytest.mark.parametrize("resource_type", ["seed", "snapshot"])
def test_every_field_of_a_seed_and_a_snapshot_with_declared_columns_is_classified(tmp_path, resource_type):
    """A seed or a snapshot that declares described and tagged columns with data types, with
    not_null and unique tests, stored as the bundle stores it, holds the property fields the
    fixture seeds lack, and each is classified for its family as moving the contract. The seed's
    contract is enforced, which dbt ignores for a seed; a snapshot's is not, because the contracts
    library refuses an enforced contract on a snapshot."""
    family = f"dbt-{resource_type}"
    node = enforced_dbt_node(resource_type, described=True)
    if resource_type == "snapshot":
        node["config"]["contract"] = {"enforced": False}
    _, (stored,) = stored_dbt_contracts(str(tmp_path), node, *column_tests(node))
    assert continuo_values(stored.document["customProperties"])["kind"] == family

    names = _odcs_inputs(stored.document)

    expected = {
        "odcs.description.purpose", "odcs.schema.description",
        "odcs.schema.properties.name", "odcs.schema.properties.physicalType", "odcs.schema.properties.logicalType",
        "odcs.schema.properties.description", "odcs.schema.properties.tags",
        "odcs.schema.properties.required", "odcs.schema.properties.unique",
    }
    if resource_type == "seed":
        expected.add("odcs.custom.contractEnforced")
    assert expected <= names
    missing = {}
    for name in sorted(names):
        entry = fingerprints.classify(family, name)
        if entry is None or entry.fingerprint not in ("contract", "both"):
            missing[name] = None if entry is None else entry.fingerprint
    assert not missing, (family, missing)


# The library's vectors whose ontology fills something in: the one of a project the ontology does
# not declare resolves to its stored form.
_ONTOLOGY_VECTORS = [
    vector
    for vector in json.loads(
        (Path(continuo_contracts.__file__).parent / "vectors" / "ontology.json").read_text(encoding="utf-8"))
    if vector["resolved"] != vector["stored"]
]


@pytest.mark.parametrize("vector", _ONTOLOGY_VECTORS, ids=lambda vector: vector["name"])
def test_every_field_of_a_resolved_contract_is_classified(vector):
    """A contract the ontology fills in (a domain, links, bound logical types, semantic types, a
    key, a classification) holds fields no unresolved contract has, and each moves the contract
    hash: fingerprints.yaml classifies it for the family of the contract, a dbt model's, a dbt
    seed's or a python node's."""
    loaded = ontology.load(vector["ontology"])
    release_ontology = ReleaseOntology(
        project="company-ontology", release_id="rel-1", sha256=loaded.sha256, ontology=loaded, findings=())
    resolved = bind_contract(vector["stored"], enabled=True, release_ontology=release_ontology)
    family = continuo_values(resolved["customProperties"])["kind"]
    assert family in ("python-node", "dbt-model", "dbt-seed")

    assert _odcs_inputs(resolved) - _odcs_inputs(vector["stored"]), "the ontology fills in a new field"
    missing = {}
    for name in sorted(_odcs_inputs(resolved)):
        entry = fingerprints.classify(family, name)
        if entry is None or entry.fingerprint not in ("contract", "both"):
            missing[name] = None if entry is None else entry.fingerprint
    assert not missing, (family, missing)
