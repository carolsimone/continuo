import json
from pathlib import Path

import pytest

from domain.exceptions import InvalidCompileExtrasError, MalformedContractError
from domain.model import ManifestFile, ManifestKind, ParsedArtifact, SkippedNode, SkipReason
from service.manifest_parsers import parser_for

FIXTURES = Path(__file__).parent / "fixtures"


def test_each_kind_resolves_to_its_own_failure_vocabulary():
    dbt = parser_for(ManifestKind.DBT)
    python = parser_for(ManifestKind.PYTHON)

    assert json.JSONDecodeError in dbt.permanent_errors
    assert InvalidCompileExtrasError in dbt.permanent_errors
    assert python.permanent_errors == (MalformedContractError,)


def test_the_dbt_entry_parses_a_manifest_file_into_a_parsed_artifact():
    parsed = parser_for(ManifestKind.DBT).parse(
        ManifestFile(path=str(FIXTURES / "manifest_service1.json"), image_tag="img:1"))

    assert isinstance(parsed, ParsedArtifact)
    assert parsed.nodes and all(node.image_tag == "img:1" for node in parsed.nodes)


def test_the_dbt_entry_loads_the_extras_beside_the_manifest(tmp_path):
    extras = tmp_path / "compile-extras.json"
    extras.write_text(json.dumps({"version": 2}))
    mf = ManifestFile(path=str(FIXTURES / "manifest_service1.json"), extras_path=str(extras))

    with pytest.raises(InvalidCompileExtrasError):
        parser_for(ManifestKind.DBT).parse(mf)


def test_the_dbt_entry_holds_the_extras_to_the_declared_service(tmp_path):
    extras = tmp_path / "compile-extras.json"
    extras.write_text(json.dumps({"version": 1, "service": "other", "release_id": "rel-1",
                                  "seeds": {}, "project_flags": {}, "packages": {}}))
    mf = ManifestFile(path=str(FIXTURES / "manifest_service1.json"), declared_service="service-1",
                      extras_path=str(extras))

    with pytest.raises(InvalidCompileExtrasError, match="other"):
        parser_for(ManifestKind.DBT).parse(mf)


def test_the_dbt_entry_reads_a_manifest_that_names_no_project_against_the_declared_service(tmp_path):
    """The declared service stands for the project the manifest was compiled
    from, so an untagged node of an installed package is left out."""
    manifest = json.loads((FIXTURES / "manifest_installed_package.json").read_text())
    del manifest["metadata"]
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    skipped: list[SkippedNode] = []

    parsed = parser_for(ManifestKind.DBT).parse(ManifestFile(path=str(path), declared_service="shop-core"),
                                                skipped=skipped)

    assert sorted(n.table_name for n in parsed.nodes) == ["countries", "monthly_report", "orders"]
    assert [(s.node_id, s.reason) for s in skipped] == [
        ("model.evalpkg.pkg_model", SkipReason.INSTALLED_PACKAGE),
        ("seed.evalpkg.pkg_seed", SkipReason.INSTALLED_PACKAGE),
        ("model.evalpkg.pkg_no_owner", SkipReason.NO_OWNER),
    ]


def test_a_plain_string_resolves_the_same_as_the_enum_member():
    """main.py passes the raw wire value through, so lookups must work on str."""
    assert parser_for("python") is parser_for(ManifestKind.PYTHON)


def test_an_unrecognized_kind_resolves_to_nothing():
    assert parser_for("spark") is None
    assert parser_for("") is None
