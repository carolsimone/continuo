import json
from pathlib import Path

import pytest

from domain.exceptions import InvalidCompileExtrasError, InvalidOdcsBundleError, MalformedContractError
from domain.model import ManifestFile, ManifestKind, ParsedArtifact, SkippedNode, SkipReason
from service.manifest_parsers import parser_for
from tests.fakes import python_contract, python_entry
from tests.odcs_fixtures import bundle_file, demo_nodes, release_timings

FIXTURES = Path(__file__).parent / "fixtures"


def _parse(kind, mf, **kwargs):
    """A kind's parse function as the handler calls it: with the progress callback and the
    timing record of the release."""
    return parser_for(kind).parse(mf, progress=lambda: None, timings=release_timings(), **kwargs)


def test_each_kind_resolves_to_its_own_failure_vocabulary():
    dbt = parser_for(ManifestKind.DBT)
    python = parser_for(ManifestKind.PYTHON)
    odcs = parser_for(ManifestKind.PYTHON_ODCS)

    assert json.JSONDecodeError in dbt.permanent_errors
    assert InvalidCompileExtrasError in dbt.permanent_errors
    assert python.permanent_errors == (MalformedContractError,)
    assert odcs.permanent_errors == (InvalidOdcsBundleError,)
    assert odcs.empty_detail == "bundle declares no nodes"


def test_the_dbt_entry_parses_a_manifest_file_into_a_parsed_artifact():
    parsed = _parse(ManifestKind.DBT, ManifestFile(path=str(FIXTURES / "manifest_service1.json"), image_tag="img:1"))

    assert isinstance(parsed, ParsedArtifact)
    assert parsed.nodes and all(node.image_tag == "img:1" for node in parsed.nodes)


def test_the_dbt_entry_loads_the_extras_beside_the_manifest(tmp_path):
    extras = tmp_path / "compile-extras.json"
    extras.write_text(json.dumps({"version": 2}))
    mf = ManifestFile(path=str(FIXTURES / "manifest_service1.json"), extras_path=str(extras))

    with pytest.raises(InvalidCompileExtrasError):
        _parse(ManifestKind.DBT, mf)


def test_the_dbt_entry_holds_the_extras_to_the_declared_service(tmp_path):
    extras = tmp_path / "compile-extras.json"
    extras.write_text(json.dumps({"version": 1, "service": "other", "release_id": "rel-1",
                                  "seeds": {}, "project_flags": {}, "packages": {}}))
    mf = ManifestFile(path=str(FIXTURES / "manifest_service1.json"), declared_service="service-1",
                      extras_path=str(extras))

    with pytest.raises(InvalidCompileExtrasError, match="other"):
        _parse(ManifestKind.DBT, mf)


def test_the_dbt_entry_reads_a_manifest_that_names_no_project_against_the_declared_service(tmp_path):
    """The declared service stands for the project the manifest was compiled
    from, so an untagged node of an installed package is left out."""
    manifest = json.loads((FIXTURES / "manifest_installed_package.json").read_text())
    del manifest["metadata"]
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    skipped: list[SkippedNode] = []

    parsed = _parse(ManifestKind.DBT, ManifestFile(path=str(path), declared_service="shop-core"), skipped=skipped)

    assert sorted(n.table_name for n in parsed.nodes) == ["countries", "monthly_report", "orders"]
    assert [(s.node_id, s.reason) for s in skipped] == [
        ("model.evalpkg.pkg_model", SkipReason.INSTALLED_PACKAGE),
        ("seed.evalpkg.pkg_seed", SkipReason.INSTALLED_PACKAGE),
        ("model.evalpkg.pkg_no_owner", SkipReason.NO_OWNER),
    ]


def test_the_wire_python_entry_ignores_the_progress_callback_and_the_timings(tmp_path):
    mf = ManifestFile(path=python_contract(tmp_path, python_entry()), image_tag="img:1", declared_service="service-py")

    parsed = _parse(ManifestKind.PYTHON, mf)

    assert [n.unique_id for n in parsed.nodes] == ["test_schema.py_metrics"]
    assert parsed.failed == () and all(n.contract is None for n in parsed.nodes)


def test_the_odcs_entry_parses_a_bundle_with_the_library_this_install_runs_on(tmp_path):
    timings = release_timings()
    calls = []

    parsed = parser_for(ManifestKind.PYTHON_ODCS).parse(
        bundle_file(tmp_path, demo_nodes()), progress=lambda: calls.append(1), timings=timings)

    assert [n.unique_id for n in parsed.nodes] == [
        "analytics.demo_orders_csv", "analytics.fx_eur_usd_rates", "analytics.py_daily_kpis"]
    assert len(calls) == 3
    assert timings.validation_cache["hit"] + timings.validation_cache["miss"] == 3


def test_a_plain_string_resolves_the_same_as_the_enum_member():
    """main.py passes the raw wire value through, so lookups must work on str."""
    assert parser_for("python") is parser_for(ManifestKind.PYTHON)
    assert parser_for("python-odcs") is parser_for(ManifestKind.PYTHON_ODCS)


def test_an_unrecognized_kind_resolves_to_nothing():
    assert parser_for("spark") is None
    assert parser_for("") is None
