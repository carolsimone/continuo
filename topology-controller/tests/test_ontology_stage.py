"""The ontology stage: once every node of a release has resolved and its contracts are stored, the
contracts are checked against the release's ontology. An error rejects the release naming the
contract's node, and a warning or an information line goes to the contract bundle."""
import json
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from continuo_contracts import ontology_check
from continuo_contracts.ontology_check import RELEASE_RULES, Finding

from adapters.redis.candidate_publisher import CandidateManifestPublisher
from domain.contract_vocabulary import NodeType, ParseFailureKind
from domain.exceptions import InvalidCompiledSqlError
from domain.model import FailedNode, ManifestFile, ManifestKind
from service.release_timings import ReleaseTimings, Stage
from streams_contract import MANIFEST_LOADED_CANDIDATE_V2
from tests.fakes import FakeClock, RecordingContractBundleWriter, ScriptedMemoryProbe, dbt_node, python_handler, source_of
from tests.odcs_fixtures import bound_kpis_text, bundle_file, bundle_node, ontology_vector

PATH = "contracts/py_daily_kpis.odcs.yaml"
BROKEN_BINDING = (
    "ontology contract: column total_amount -> Transaction.amount_eur: "
    "Transaction.amount_eur is not a property or key part of Transaction"
)


def valid_ontology() -> str:
    return ontology_vector("python_node_with_aggregates")["ontology"]


def ontology_without_amount() -> str:
    """The ontology as a new release of it would leave it after dropping the property that the
    contracts of service-py sum."""
    text = valid_ontology().replace("      amount_eur:     {type: money, currency: EUR}\n", "")
    assert text != valid_ontology()
    return text


def ontology_manifest(tmp_path, text: str) -> ManifestFile:
    path = Path(tmp_path) / "ontology.yaml"
    path.write_text(text, encoding="utf-8")
    return ManifestFile(
        path=str(path), declared_service="company-ontology", kind=ManifestKind.ONTOLOGY, release_id="rel-onto-4")


def release(tmp_path, *, ontology_text: str | None, tables=("py_daily_kpis",), extra=(), enabled=True,
            publisher=None, progress=None, clock=None, writer=None):
    """Release the bound contracts of service-py (one per table) with the ontology `ontology_text`,
    when there is one, through `writer` (a recording contract bundle writer unless given one).
    Returns the publisher, the contract bundle writer and the timing record."""
    Path(tmp_path).mkdir(parents=True, exist_ok=True)
    files = [bundle_file(tmp_path, [
        bundle_node(f"contracts/{table}.odcs.yaml", bound_kpis_text(table)) for table in tables])]
    if ontology_text is not None:
        files.append(ontology_manifest(tmp_path, ontology_text))
    files.extend(extra)
    publisher = publisher if publisher is not None else MagicMock()
    writer = writer if writer is not None else RecordingContractBundleWriter()
    timings = python_handler(
        source_of(*files), publisher, contract_bundle_writer=writer, ontology_enabled=enabled,
        progress=progress, clock=clock,
    ).handle(release_id="rel-1")
    return publisher, writer, timings


def failed_kwargs(publisher) -> dict:
    publisher.publish_failed.assert_called_once()
    publisher.publish_ok.assert_not_called()
    return publisher.publish_failed.call_args.kwargs


def failing_check(findings):
    """A replacement for the library's check that finds `findings` and runs no rule."""
    def check(ontology, contracts, *, rules, progress):
        return list(findings)
    return check


# --- an error rejects the release --------------------------------------------------------

def test_a_checker_error_rejects_naming_the_node(tmp_path):
    publisher, writer, timings = release(tmp_path, ontology_text=ontology_without_amount())

    kwargs = failed_kwargs(publisher)
    assert kwargs["failure_kind"] is ParseFailureKind.INVALID_CONTRACT
    assert kwargs["detail"] == "1 node failed to parse: analytics.py_daily_kpis"
    assert kwargs["failed_nodes"] == [FailedNode(
        node_id="analytics.py_daily_kpis", kind=ParseFailureKind.INVALID_CONTRACT, service="service-py",
        file_path=PATH, node_type=NodeType.PYTHON_NODE, detail=BROKEN_BINDING,
    )]
    assert timings.contract_failures == {("ontology", "contract"): 1}
    assert writer.writes == []


def test_an_ontology_that_breaks_a_binding_rejects_naming_the_nodes(tmp_path):
    """The release that carries the ontology is the rejected one, and each failed node is a node of
    the other service whose binding the new ontology breaks: no node names the ontology service."""
    publisher, _, timings = release(
        tmp_path, ontology_text=ontology_without_amount(), tables=("py_weekly_kpis", "py_daily_kpis"))

    kwargs = failed_kwargs(publisher)
    assert kwargs["failure_kind"] is ParseFailureKind.INVALID_CONTRACT
    assert kwargs["detail"] == "2 nodes failed to parse: analytics.py_daily_kpis, analytics.py_weekly_kpis"
    assert [(n.node_id, n.service, n.kind) for n in kwargs["failed_nodes"]] == [
        ("analytics.py_daily_kpis", "service-py", ParseFailureKind.INVALID_CONTRACT),
        ("analytics.py_weekly_kpis", "service-py", ParseFailureKind.INVALID_CONTRACT),
    ]
    assert {n.service for n in kwargs["failed_nodes"]}.isdisjoint({"company-ontology"})
    assert timings.contract_failures == {("ontology", "contract"): 2}


def test_the_errors_of_one_node_are_joined_and_each_is_counted_by_its_rule(tmp_path, monkeypatch):
    monkeypatch.setattr(ontology_check, "check", failing_check([
        Finding("error", "contract", "analytics.py_daily_kpis", "first", "analytics.py_daily_kpis"),
        Finding("error", "coverage", "analytics.py_daily_kpis", "second", "analytics.py_daily_kpis"),
        Finding("warning", "access", "analytics.py_daily_kpis", "left to the bundle", "analytics.py_daily_kpis"),
    ]))

    publisher, _, timings = release(tmp_path, ontology_text=valid_ontology())

    (node,) = failed_kwargs(publisher)["failed_nodes"]
    assert node.detail == "ontology contract: first; ontology coverage: second"
    assert timings.contract_failures == {("ontology", "contract"): 1, ("ontology", "coverage"): 1}


def test_an_error_that_names_no_contract_is_an_invalid_artifact_naming_the_ontology_service(tmp_path, monkeypatch):
    monkeypatch.setattr(ontology_check, "check", failing_check([
        Finding("error", "order", "copy graph", "a cycle", None),
        Finding("error", "contract", "analytics.py_daily_kpis", "a node's own", "analytics.py_daily_kpis"),
    ]))

    publisher, _, timings = release(tmp_path, ontology_text=valid_ontology())

    kwargs = failed_kwargs(publisher)
    assert kwargs["failure_kind"] is ParseFailureKind.INVALID_ARTIFACT
    assert kwargs["detail"] == "company-ontology: ontology order: copy graph: a cycle"
    assert kwargs["failed_nodes"] == []
    assert timings.contract_failures == {("ontology", "order"): 1, ("ontology", "contract"): 1}


def test_a_finding_that_names_a_contract_the_release_does_not_hold_is_internal(tmp_path, monkeypatch):
    monkeypatch.setattr(ontology_check, "check", failing_check([
        Finding("error", "contract", "analytics.ghost", "who", "analytics.ghost")]))

    publisher, _, _ = release(tmp_path, ontology_text=valid_ontology())

    kwargs = failed_kwargs(publisher)
    assert kwargs["failure_kind"] is ParseFailureKind.INTERNAL
    assert "analytics.ghost" in kwargs["detail"]


def test_the_checker_runs_the_release_rules_over_every_stored_contract(tmp_path, monkeypatch):
    seen = {}

    def check(ontology, contracts, *, rules, progress):
        seen.update(rules=tuple(rules), ids=[c["id"] for c in contracts], services=ontology.services())
        return []

    monkeypatch.setattr(ontology_check, "check", check)

    release(tmp_path, ontology_text=valid_ontology(), tables=("py_daily_kpis", "py_weekly_kpis"))

    assert seen == {
        "rules": RELEASE_RULES, "ids": ["analytics.py_daily_kpis", "analytics.py_weekly_kpis"],
        "services": ("core", "finance", "service-py"),
    }


# --- what does not reject --------------------------------------------------------------

def test_warnings_never_reject_and_reach_the_bundle(tmp_path):
    publisher, writer, _ = release(tmp_path, ontology_text=valid_ontology())

    publisher.publish_ok.assert_called_once()
    publisher.publish_failed.assert_not_called()
    assert writer.document["ontology"]["findings"] == [
        {"severity": "warning", "rule": "access", "subject": "analytics.py_daily_kpis",
         "message": "reads analytics.daily_transactions, which no checked contract binds: its owner and grants are unknown",
         "contract_id": "analytics.py_daily_kpis"},
        {"severity": "info", "rule": "order", "subject": "copy graph", "message": "1: analytics.py_daily_kpis",
         "contract_id": None},
    ]


def test_the_findings_about_an_undeclared_service_are_written_by_the_checker_and_kept(tmp_path):
    """The checker writes one information line for a service the ontology does not declare and a
    warning for each of its contracts that binds: continuo adds none of its own."""
    undeclared = ontology_vector("service_not_declared")["ontology"]

    publisher, writer, _ = release(tmp_path, ontology_text=undeclared)

    publisher.publish_ok.assert_called_once()
    assert [(f["severity"], f["rule"], f["subject"]) for f in writer.document["ontology"]["findings"]] == [
        ("warning", "contract", "analytics.py_daily_kpis"), ("info", "contract", "service-py")]


def test_a_release_with_the_switch_on_and_no_ontology_runs_no_check(tmp_path, monkeypatch):
    monkeypatch.setattr(ontology_check, "check", failing_check([
        Finding("error", "contract", "analytics.py_daily_kpis", "never run", "analytics.py_daily_kpis")]))

    publisher, writer, timings = release(tmp_path, ontology_text=None)

    publisher.publish_ok.assert_called_once()
    assert timings.stages[Stage.ONTOLOGY].runs == 0
    assert "ontology" not in writer.document["timings"]["stages_ms"]


def test_a_release_with_the_switch_off_runs_no_check(tmp_path, monkeypatch):
    monkeypatch.setattr(ontology_check, "check", failing_check([
        Finding("error", "contract", "analytics.py_daily_kpis", "never run", "analytics.py_daily_kpis")]))

    publisher, _, timings = release(tmp_path, ontology_text=ontology_without_amount(), enabled=False)

    publisher.publish_ok.assert_called_once()
    assert timings.stages[Stage.ONTOLOGY].runs == 0


def test_the_check_runs_only_when_every_node_resolved(tmp_path, monkeypatch):
    """A node whose SQL cannot be read is published as its own failure, and the contracts of the
    other nodes are not stored, so they are not checked."""
    dbt_path = Path(tmp_path) / "service-1.json"
    dbt_path.parent.mkdir(parents=True, exist_ok=True)
    dbt_path.write_text(json.dumps({"nodes": {"model.service-1.broken": dbt_node("broken")}}))
    dbt_manifest = ManifestFile(path=str(dbt_path), declared_service="service-1")
    monkeypatch.setattr(ontology_check, "check", failing_check(
        [Finding("error", "contract", "analytics.py_daily_kpis", "never run", "analytics.py_daily_kpis")]))

    def unreadable(node, *, dialect, **kwargs):
        if node.service_name == "service-1":
            raise InvalidCompiledSqlError(node_table_name=node.table_name, detail="broken")
        return []

    monkeypatch.setattr("service.candidate_manifest_handler.referenced_tables", unreadable)

    publisher, writer, timings = release(tmp_path, ontology_text=valid_ontology(), extra=[dbt_manifest])

    kwargs = failed_kwargs(publisher)
    assert kwargs["failure_kind"] is ParseFailureKind.INVALID_SQL
    assert [n.node_id for n in kwargs["failed_nodes"]] == ["test_schema.broken"]
    assert timings.contract_failures == {}
    assert writer.writes == []


# --- text UTF-8 cannot encode --------------------------------------------------------------

def _publishing(redis) -> CandidateManifestPublisher:
    """The real publisher over a recording redis client."""
    return CandidateManifestPublisher(
        redis, MANIFEST_LOADED_CANDIDATE_V2, producer="topology-controller", continuo_contracts="0.4.1")


def test_an_unencodable_failure_is_an_invalid_artifact(tmp_path, monkeypatch):
    """A finding can quote text of a contract that holds a lone surrogate. The event that reports
    the rejection is encoded as UTF-8, so a rejection that held it could never be published and the
    release would be retried for ever: it is an invalid artifact that names the release."""
    monkeypatch.setattr(ontology_check, "check", failing_check([
        Finding("error", "contract", "analytics.py_daily_kpis", "column x\ud800 is unbound", "analytics.py_daily_kpis")]))
    redis = MagicMock()

    release(tmp_path, ontology_text=valid_ontology(), publisher=_publishing(redis))

    redis.xadd.assert_called_once()
    _, fields = redis.xadd.call_args.args
    for value in fields.values():
        value.encode("utf-8")
    payload = json.loads(fields["payload"])
    assert (payload["status"], payload["failure_kind"]) == ("failed", "invalid_artifact")
    assert payload["detail"].startswith("release rel-1: the failure holds text UTF-8 cannot encode")
    assert "x\\ud800" in payload["detail"]
    assert payload["failed_nodes"] == []


def test_a_failure_that_can_be_encoded_is_published_as_it_is(tmp_path):
    redis = MagicMock()

    release(tmp_path, ontology_text=ontology_without_amount(), publisher=_publishing(redis))

    _, fields = redis.xadd.call_args.args
    payload = json.loads(fields["payload"])
    assert payload["failure_kind"] == "invalid_contract"
    assert [n["node_id"] for n in payload["failed_nodes"]] == ["analytics.py_daily_kpis"]


# --- timing and progress ---------------------------------------------------------------------

def test_the_ontology_stage_counts_in_contracts_ms():
    clock = FakeClock()
    timings = ReleaseTimings("rel-1", clock, ScriptedMemoryProbe(0))
    with timings.stage(Stage.ONTOLOGY):
        clock.t += 0.25
    with timings.stage(Stage.CANONICAL_HASHING):
        clock.t += 0.5
    with timings.stage(Stage.RESOLVE):
        clock.t += 1.0
    timings.mark_published()

    assert timings.contracts_ms() == 750
    assert timings.parse_ms() == 1000


def test_the_ontology_stage_is_timed_and_counts_in_contracts_ms(tmp_path):
    """It runs twice for a release with an ontology: the file is read, and the contracts are checked."""
    _, writer, timings = release(tmp_path, ontology_text=valid_ontology(), clock=FakeClock(step=0.25))

    assert timings.stages[Stage.ONTOLOGY].runs == 2
    assert timings.stages[Stage.ONTOLOGY].seconds > 0
    assert writer.document["timings"]["stages_ms"]["ontology"] == round(timings.stages[Stage.ONTOLOGY].seconds * 1000)
    assert timings.contracts_ms() >= round(timings.stages[Stage.ONTOLOGY].seconds * 1000)


def test_progress_is_called_after_each_contract_of_the_ontology_stage(tmp_path, monkeypatch):
    """A cold release of a large estate spends its time here, so the checker is given the handler's
    progress callback, and the real checker calls it after each contract of each rule it runs."""
    calls = []
    during_check = []
    real_check = ontology_check.check

    def spying_check(ontology, contracts, *, rules, progress):
        before = len(calls)
        findings = real_check(ontology, contracts, rules=rules, progress=progress)
        during_check.append((len(contracts), len(calls) - before))
        return findings

    monkeypatch.setattr(ontology_check, "check", spying_check)

    release(
        tmp_path, ontology_text=valid_ontology(), tables=("py_a_kpis", "py_b_kpis", "py_c_kpis"),
        progress=lambda: calls.append(1))

    ((contracts, progress_calls),) = during_check
    assert contracts == 3
    assert progress_calls >= contracts
    assert len(calls) > progress_calls, "progress is called while the contracts are stored too"


# --- the release line ----------------------------------------------------------------------

def _release_line(caplog):
    """The one log line the release ends with."""
    (line,) = [record for record in caplog.records if record.getMessage() == "release parsed"]
    return line


def _logged_release(tmp_path, caplog, **kwargs):
    with caplog.at_level(logging.INFO, logger="service.candidate_manifest_handler"):
        release(tmp_path, **kwargs)
    return _release_line(caplog)


def test_the_release_line_counts_the_warnings_and_information_the_check_found(tmp_path, caplog, monkeypatch):
    """A line per finding would be one per contract of an undeclared service: the release line
    carries one count per severity, and the findings themselves are in the contract bundle."""
    monkeypatch.setattr(ontology_check, "check", failing_check([
        Finding("warning", "contract", "analytics.a", "one", "analytics.a"),
        Finding("warning", "contract", "analytics.b", "two", "analytics.b"),
        Finding("warning", "access", "analytics.b", "three", "analytics.b"),
        Finding("info", "order", "copy graph", "layers", None),
        Finding("info", "contract", "service-py", "not declared", None),
    ]))

    line = _logged_release(tmp_path, caplog, ontology_text=valid_ontology())

    assert (line.ontology_warnings, line.ontology_information) == (3, 2)
    assert not any(record.getMessage() == "candidate: a finding of the ontology check" for record in caplog.records)


def test_the_release_line_names_only_the_severities_the_check_found(tmp_path, caplog, monkeypatch):
    monkeypatch.setattr(ontology_check, "check", failing_check([
        Finding("info", "order", "copy graph", "layers", None)]))

    line = _logged_release(tmp_path, caplog, ontology_text=valid_ontology())

    assert line.ontology_information == 1
    assert not hasattr(line, "ontology_warnings")


def test_the_release_line_has_no_ontology_field_when_the_check_found_nothing(tmp_path, caplog, monkeypatch):
    monkeypatch.setattr(ontology_check, "check", failing_check([]))

    line = _logged_release(tmp_path, caplog, ontology_text=valid_ontology())

    assert not [name for name in vars(line) if name.startswith("ontology")]


def test_the_release_line_of_the_real_check_counts_what_the_bundle_lists(tmp_path, caplog):
    with caplog.at_level(logging.INFO, logger="service.candidate_manifest_handler"):
        _, writer, _ = release(tmp_path, ontology_text=valid_ontology())
    line = _release_line(caplog)

    severities = [finding["severity"] for finding in writer.document["ontology"]["findings"]]
    assert (line.ontology_warnings, line.ontology_information) == (severities.count("warning"), severities.count("info"))
    assert (line.ontology_warnings, line.ontology_information) == (1, 1)


def test_the_release_line_of_a_switch_on_release_with_no_ontology_counts_its_one_warning(tmp_path, caplog):
    line = _logged_release(tmp_path, caplog, ontology_text=None)

    assert line.ontology_warnings == 1
    assert not hasattr(line, "ontology_information")


def test_a_release_that_fails_to_write_has_no_ontology_field_on_its_line(tmp_path, caplog, monkeypatch):
    """The counts are of a published release's findings: a release whose contract bundle cannot be
    written ends failed, and its line carries no ontology field, though the check found some."""
    monkeypatch.setattr(ontology_check, "check", failing_check([
        Finding("warning", "contract", "analytics.a", "one", "analytics.a"),
        Finding("info", "order", "copy graph", "layers", None),
    ]))
    writer = RecordingContractBundleWriter(fail=RuntimeError("the bucket refused the write"))

    with caplog.at_level(logging.INFO, logger="service.candidate_manifest_handler"):
        publisher, _, timings = release(tmp_path, ontology_text=valid_ontology(), writer=writer)

    assert failed_kwargs(publisher)["failure_kind"] is ParseFailureKind.INTERNAL
    line = _release_line(caplog)
    assert line.outcome == "failed"
    assert not [name for name in vars(line) if name.startswith("ontology")]
    assert timings.ontology_findings == {}


def test_the_release_line_has_no_ontology_field_with_the_switch_off(tmp_path, caplog):
    """The line carries no ontology field, whether or not the release carries an ontology."""
    for ontology_text in (None, valid_ontology()):
        caplog.clear()
        line = _logged_release(tmp_path / str(ontology_text is None), caplog, ontology_text=ontology_text, enabled=False)

        assert not [name for name in vars(line) if name.startswith("ontology")]
