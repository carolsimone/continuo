"""Holds every input the content hashing reads to continuo-contracts' fingerprints.yaml.

A name the hashing reads that fingerprints.yaml does not classify would change a node's
content_hash without the contract saying whether that change is a code change or a
contract change. These tests collect the input names of every dbt fixture variant, with
and without compile extras, and the keys of the python code projection, and require each
to be classified for its family.
"""
from continuo_contracts.fingerprints import unclassified
from continuo_contracts.python import PythonColumn, PythonNode, code_projection

from service.dbt_hashing import hash_dbt_node
from tests.fingerprint_fixtures import VARIANTS, manifest_and_context

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
