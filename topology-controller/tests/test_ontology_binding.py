"""The release's ontology: the manifest of kind `ontology` is read once, before any node
manifest, when the install's switch is on, and ignored when it is off."""
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from continuo_contracts.ontology import Ontology

from domain.contract_vocabulary import ParseFailureKind
from domain.exceptions import InvalidOntologyError
from domain.model import ManifestFile, ManifestKind
from service.ontology_binding import ReleaseOntology, load_release_ontology
from service.release_timings import Stage
from tests.fakes import RecordingContractBundleWriter, build_handler, make_uploader, source_of

FIXTURES = Path(__file__).parent / "fixtures"

ONTOLOGY_TEXT = """\
principals:
  domains:
    sales:
      owner_team: sales-team
  projects:
    company-reports:
      domain: sales
      kind: dbt
  teams:
    sales-team:
      idp_group: sales
entities:
  Order:
    owner: sales
    key:
      order_id: int
    properties:
      amount:
        type: money
"""

# Two things whose owner is a team, not a declared domain: two errors of the `ontology` rule.
TWO_RULE_ERRORS = ONTOLOGY_TEXT.replace(
    "entities:\n", "entities:\n  Invoice:\n    owner: sales-team\n    key:\n      invoice_id: int\n"
).replace("  Order:\n    owner: sales\n", "  Order:\n    owner: finance\n")


def ontology_file(tmp_path, text=ONTOLOGY_TEXT, *, project="company-ontology", release_id="rel-onto-4", name="ontology.yaml"):
    path = tmp_path / name
    if isinstance(text, bytes):
        path.write_bytes(text)
    else:
        path.write_text(text, encoding="utf-8")
    return ManifestFile(
        path=str(path), declared_project=project, kind=ManifestKind.ONTOLOGY, release_id=release_id,
    )


def dbt_file():
    return ManifestFile(path=str(FIXTURES / "manifest_service1.json"), image_tag="")


def handle(files, *, enabled, writer=None):
    publisher = MagicMock()
    bundle_writer = writer if writer is not None else RecordingContractBundleWriter()
    handler = build_handler(
        source_of(*files), publisher, make_uploader(), contract_bundle_writer=bundle_writer,
        ontology_enabled=enabled,
    )
    timings = handler.handle(release_id="rel-1")
    return publisher, timings, bundle_writer


def failed_kwargs(publisher):
    publisher.publish_failed.assert_called_once()
    publisher.publish_ok.assert_not_called()
    return publisher.publish_failed.call_args.kwargs


# --- load_release_ontology -------------------------------------------------------------

def test_a_valid_ontology_is_loaded_with_the_identity_of_its_release(tmp_path):
    loaded = load_release_ontology(ontology_file(tmp_path))

    assert isinstance(loaded, ReleaseOntology)
    assert loaded.project == "company-ontology"
    assert loaded.release_id == "rel-onto-4"
    assert isinstance(loaded.ontology, Ontology)
    assert loaded.ontology.projects() == ("company-reports",)
    assert loaded.sha256 == loaded.ontology.sha256
    assert len(loaded.sha256) == 64
    assert loaded.findings == ()


def test_an_ontology_that_does_not_load_names_its_project_and_the_librarys_reason(tmp_path):
    with pytest.raises(InvalidOntologyError) as raised:
        load_release_ontology(ontology_file(tmp_path, "principals: 1\n"))

    assert str(raised.value) == "company-ontology: principals: must be a mapping, got a number"
    assert raised.value.rule_errors == ()


def test_a_file_that_is_not_utf8_text_is_an_invalid_ontology(tmp_path):
    with pytest.raises(InvalidOntologyError, match=r"^company-ontology: the file is not UTF-8 text"):
        load_release_ontology(ontology_file(tmp_path, b"principals:\n  \xff\n"))


def test_every_error_of_the_ontology_rule_is_named_and_counted_by_its_rule(tmp_path):
    with pytest.raises(InvalidOntologyError) as raised:
        load_release_ontology(ontology_file(tmp_path, TWO_RULE_ERRORS))

    assert str(raised.value) == (
        "company-ontology: Invoice: owner 'sales-team' is not a declared domain; "
        "Order: owner 'finance' is not a declared domain"
    )
    assert raised.value.rule_errors == ("ontology", "ontology")


def test_an_ontology_whose_release_cannot_be_read_from_its_key_is_refused(tmp_path):
    with pytest.raises(InvalidOntologyError, match="^company-ontology: .*release id"):
        load_release_ontology(ontology_file(tmp_path, release_id=""))


# --- the handler -----------------------------------------------------------------------

@pytest.mark.parametrize("enabled", [True, False])
def test_the_ontology_manifest_is_not_a_node_manifest(tmp_path, enabled):
    """It is taken out before the parse loop, so it is neither an unknown kind nor an
    empty manifest, and the release's nodes are those of its dbt manifest alone."""
    publisher, timings, _ = handle([dbt_file(), ontology_file(tmp_path)], enabled=enabled)

    publisher.publish_ok.assert_called_once()
    publisher.publish_failed.assert_not_called()
    assert timings.nodes["dbt"] == 1


def test_an_ontology_alone_is_a_release_with_no_node(tmp_path):
    publisher, _, _ = handle([ontology_file(tmp_path)], enabled=True)

    publisher.publish_ok.assert_called_once()
    publisher.publish_failed.assert_not_called()


def test_with_the_switch_on_the_ontology_is_read_and_then_checked_in_its_own_stage(tmp_path):
    _, timings, _ = handle([dbt_file(), ontology_file(tmp_path)], enabled=True)

    assert timings.stages[Stage.ONTOLOGY].runs == 2


def test_with_the_switch_off_an_ontology_is_logged_and_ignored(tmp_path, caplog):
    """An ontology file that would not load is ignored too: nothing reads it."""
    broken = ontology_file(tmp_path, "principals: [")
    with caplog.at_level(logging.INFO, logger="service.candidate_manifest_handler"):
        publisher, timings, writer = handle([dbt_file(), broken], enabled=False)

    publisher.publish_ok.assert_called_once()
    (record,) = [r for r in caplog.records if r.getMessage() == "candidate: the ontology switch is off, so the ontology is ignored"]
    assert record.levelno == logging.INFO
    assert (record.release_id, record.project) == ("rel-1", "company-ontology")
    assert timings.stages[Stage.ONTOLOGY].runs == 0
    # A release that ran no ontology stage leaves the stage out of the bundle's timings.
    assert "ontology" not in writer.document["timings"]["stages_ms"]


def test_with_the_switch_off_and_no_ontology_nothing_is_logged(tmp_path, caplog):
    with caplog.at_level(logging.INFO, logger="service.candidate_manifest_handler"):
        handle([dbt_file()], enabled=False)

    assert not [r for r in caplog.records if "ontology" in r.getMessage()]


def test_an_ontology_that_does_not_load_is_an_invalid_artifact_naming_its_project(tmp_path):
    publisher, timings, _ = handle([dbt_file(), ontology_file(tmp_path, "principals: 1\n")], enabled=True)

    kwargs = failed_kwargs(publisher)
    assert kwargs["failure_kind"] is ParseFailureKind.INVALID_ARTIFACT
    assert kwargs["detail"] == "company-ontology: principals: must be a mapping, got a number"
    assert kwargs["failed_nodes"] == []
    assert timings.contract_failures == {}


def test_an_ontology_rule_error_is_an_invalid_artifact(tmp_path):
    """The owner of a thing must be a declared domain, not a team."""
    publisher, timings, _ = handle([dbt_file(), ontology_file(tmp_path, TWO_RULE_ERRORS)], enabled=True)

    kwargs = failed_kwargs(publisher)
    assert kwargs["failure_kind"] is ParseFailureKind.INVALID_ARTIFACT
    assert kwargs["detail"] == (
        "company-ontology: Invoice: owner 'sales-team' is not a declared domain; "
        "Order: owner 'finance' is not a declared domain"
    )
    assert kwargs["failed_nodes"] == []
    assert timings.contract_failures == {("ontology", "ontology"): 2}
    assert timings.stages[Stage.ONTOLOGY].runs == 1


@pytest.mark.parametrize("enabled", [True, False])
def test_two_ontology_manifests_are_an_invalid_artifact(tmp_path, enabled):
    first = ontology_file(tmp_path, project="ontology-a", name="a.yaml")
    second = ontology_file(tmp_path, project="ontology-b", name="b.yaml")

    publisher, _, _ = handle([dbt_file(), first, second], enabled=enabled)

    kwargs = failed_kwargs(publisher)
    assert kwargs["failure_kind"] is ParseFailureKind.INVALID_ARTIFACT
    assert kwargs["detail"] == "an install has one ontology: the release carries one from ontology-a and one from ontology-b"
