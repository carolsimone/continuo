"""The handler's treatment of invalid python-odcs contracts: every manifest's parse reports its
invalid contracts, the release goes on through resolve with the valid nodes, and one
`publish_failed` names every failed node."""
import itertools
import json
from unittest.mock import MagicMock

import pytest

from domain.contract_vocabulary import ParseFailureKind
from domain.model import FailedNode, ManifestFile
from service.contracts_library import LIBRARY_VERSION, version_triple
from service.release_timings import ReleaseOutcome
from tests.fakes import (
    RecordingArtifactWriter,
    dbt_node,
    python_handler,
    source_of,
)
from tests.odcs_fixtures import bundle_doc, bundle_file, bundle_node, demo_node, demo_nodes, demo_text, write_bundle

KPIS_PATH = "contracts/py_daily_kpis.odcs.yaml"
CSV_PATH = "contracts/demo_orders_csv.odcs.yaml"

KPIS_WITH_NUMERIC_12 = bundle_node(KPIS_PATH, demo_text("py_daily_kpis").replace("NUMERIC(12,2)", "NUMERIC(12)"))
KPIS_ISSUE = "schema[0].properties[2].physicalType: 'NUMERIC(12)' is not a type continuo accepts"
CSV_WITH_SERVERS = bundle_node(
    CSV_PATH,
    demo_text("demo_orders_csv")
    + "servers:\n  - {server: warehouse, type: postgres, host: db, port: 5432, database: d, schema: analytics}\n",
)


def _failed_kwargs(publisher):
    publisher.publish_failed.assert_called_once()
    publisher.publish_ok.assert_not_called()
    return publisher.publish_failed.call_args.kwargs


def _broken_sql_node(name):
    return {**dbt_node(name), "compiled_code": "SELECT FROM WHERE((("}


def _dbt_manifest(tmp_path, *nodes, declared_project="service-1"):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": {n["unique_id"]: n for n in nodes}}))
    return ManifestFile(path=str(path), image_tag="", declared_project=declared_project)


def test_an_invalid_contract_rejects_the_release_with_invalid_contract(tmp_path):
    publisher, writer = MagicMock(), RecordingArtifactWriter()
    source = source_of(bundle_file(
        tmp_path, [demo_node("demo_orders_csv"), KPIS_WITH_NUMERIC_12, demo_node("fx_eur_usd_rates")]))

    timings = python_handler(source, publisher, artifact_writer=writer).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_CONTRACT
    assert kwargs["detail"] == "1 node failed to parse: analytics.py_daily_kpis"
    assert kwargs["failed_nodes"] == [FailedNode(
        node_id="analytics.py_daily_kpis",
        kind=ParseFailureKind.INVALID_CONTRACT,
        project="service-py",
        file_path=KPIS_PATH,
        node_type="python-node",
        detail=KPIS_ISSUE,
    )]
    assert writer.writes == []
    assert (timings.outcome, timings.failure_kind) == (ReleaseOutcome.FAILED, ParseFailureKind.INVALID_CONTRACT)


def test_every_invalid_contract_of_a_manifest_is_named_on_the_one_publish(tmp_path):
    publisher = MagicMock()
    source = source_of(bundle_file(tmp_path, [CSV_WITH_SERVERS, KPIS_WITH_NUMERIC_12, demo_node("fx_eur_usd_rates")]))

    python_handler(source, publisher).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_CONTRACT
    assert kwargs["detail"] == "2 nodes failed to parse: analytics.demo_orders_csv, analytics.py_daily_kpis"
    assert [n.node_id for n in kwargs["failed_nodes"]] == ["analytics.demo_orders_csv", "analytics.py_daily_kpis"]


def test_the_invalid_contracts_of_two_manifests_are_named_together(tmp_path):
    publisher = MagicMock()
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    source = source_of(
        bundle_file(first, [KPIS_WITH_NUMERIC_12], declared_project="service-a", project="service-a"),
        bundle_file(second, [CSV_WITH_SERVERS], declared_project="service-b", project="service-b"),
    )

    python_handler(source, publisher).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["detail"] == "2 nodes failed to parse: analytics.demo_orders_csv, analytics.py_daily_kpis"
    assert {(n.node_id, n.project) for n in kwargs["failed_nodes"]} == {
        ("analytics.py_daily_kpis", "service-a"), ("analytics.demo_orders_csv", "service-b")}


def test_a_bundle_whose_every_contract_is_invalid_names_them_and_is_not_empty(tmp_path):
    publisher = MagicMock()
    source = source_of(bundle_file(tmp_path, [KPIS_WITH_NUMERIC_12]))

    python_handler(source, publisher).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_CONTRACT
    assert kwargs["detail"] == "1 node failed to parse: analytics.py_daily_kpis"
    assert [n.node_id for n in kwargs["failed_nodes"]] == ["analytics.py_daily_kpis"]


def test_an_invalid_contract_and_an_invalid_sql_report_both_and_the_sql_wins(tmp_path):
    publisher = MagicMock()
    broken_sql = {**dbt_node("broken"), "compiled_code": "SELECT FROM WHERE ((("}
    source = source_of(
        _dbt_manifest(tmp_path, broken_sql),
        bundle_file(tmp_path, [KPIS_WITH_NUMERIC_12, demo_node("fx_eur_usd_rates")]),
    )

    python_handler(source, publisher).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_SQL
    assert kwargs["detail"] == "2 nodes failed to parse: analytics.py_daily_kpis, test_schema.broken"
    by_id = {n.node_id: n for n in kwargs["failed_nodes"]}
    assert by_id["analytics.py_daily_kpis"].kind == ParseFailureKind.INVALID_CONTRACT
    assert by_id["test_schema.broken"].kind == ParseFailureKind.INVALID_SQL


def test_the_valid_nodes_of_a_rejected_release_are_still_resolved(tmp_path):
    """Contract failures do not stop the resolve: a valid node's own SQL error is reported with them."""
    publisher = MagicMock()
    broken_sql = {**dbt_node("broken"), "compiled_code": "SELECT FROM WHERE ((("}
    source = source_of(
        bundle_file(tmp_path, [KPIS_WITH_NUMERIC_12]),
        _dbt_manifest(tmp_path, broken_sql),
    )

    python_handler(source, publisher).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert sorted(n.node_id for n in kwargs["failed_nodes"]) == ["analytics.py_daily_kpis", "test_schema.broken"]


def test_a_failed_release_counts_each_issue(tmp_path):
    publisher = MagicMock()
    source = source_of(bundle_file(tmp_path, [CSV_WITH_SERVERS, KPIS_WITH_NUMERIC_12, demo_node("fx_eur_usd_rates")]))

    timings = python_handler(source, publisher).handle(release_id="rel-1")

    assert timings.contract_failures == {("validate", "column_type"): 1, ("validate", "refused_field"): 1}


def test_a_bundle_the_install_cannot_read_reports_its_message_plain(tmp_path):
    publisher = MagicMock()
    major, minor, _ = version_triple(LIBRARY_VERSION)
    newer = f"{major}.{minor + 1}.0"
    source = source_of(bundle_file(tmp_path, demo_nodes(), producer_contracts=newer))

    python_handler(source, publisher).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    assert kwargs["detail"] == (
        f"service-py: the bundle was written by continuo-contracts {newer} and this install runs {LIBRARY_VERSION}:"
        " upgrade continuo, or pin an older runtime")
    assert kwargs["failed_nodes"] == []


def test_any_other_unreadable_manifest_is_named_by_its_path_and_its_error(tmp_path):
    """A KeyError says nothing but the key, so the detail keeps the error's type and its file."""
    publisher = MagicMock()
    bad = tmp_path / "no_nodes.json"
    bad.write_text('{"metadata": {}}')

    python_handler(source_of(ManifestFile(path=str(bad), image_tag="")), publisher).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    assert kwargs["detail"] == f"{bad}: KeyError('nodes')"


def _failed_node_ids(publisher):
    return [(n.node_id, n.project) for n in _failed_kwargs(publisher)["failed_nodes"]]


def test_the_failed_nodes_do_not_depend_on_the_order_of_the_manifests_or_of_the_nodes(tmp_path):
    """Three manifests, one holding two broken nodes written in reverse order: whatever the
    order, the publish lists the same failed nodes, by node id."""
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    (tmp_path / "c").mkdir()
    manifests = [
        bundle_file(tmp_path / "a", [KPIS_WITH_NUMERIC_12], declared_project="service-a", project="service-a"),
        bundle_file(tmp_path / "b", [CSV_WITH_SERVERS], declared_project="service-b", project="service-b"),
        _dbt_manifest(tmp_path / "c", _broken_sql_node("zeta"), _broken_sql_node("alpha")),
    ]
    expected = [
        ("analytics.demo_orders_csv", "service-b"),
        ("analytics.py_daily_kpis", "service-a"),
        ("test_schema.alpha", "service-1"),
        ("test_schema.zeta", "service-1"),
    ]

    for order in itertools.permutations(manifests):
        publisher = MagicMock()
        python_handler(source_of(*order), publisher).handle(release_id="rel-1")
        assert _failed_node_ids(publisher) == expected, [m.declared_project for m in order]


def test_failed_nodes_that_share_an_id_and_a_path_are_ordered_by_what_else_they_carry(tmp_path):
    """The same contract id at the same path in two projects is two failed nodes: their order
    is theirs, not the order the manifests came in."""
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    manifests = [
        bundle_file(tmp_path / "a", [KPIS_WITH_NUMERIC_12], declared_project="service-a", project="service-a"),
        bundle_file(tmp_path / "b", [KPIS_WITH_NUMERIC_12], declared_project="service-b", project="service-b"),
    ]

    for order in (manifests, manifests[::-1]):
        publisher = MagicMock()
        python_handler(source_of(*order), publisher).handle(release_id="rel-1")
        assert _failed_node_ids(publisher) == [
            ("analytics.py_daily_kpis", "service-a"), ("analytics.py_daily_kpis", "service-b")]


def _unreadable_bundle(directory):
    return write_bundle(
        directory, {**bundle_doc(demo_nodes(), project="service-b"), "bundle_version": 2}, declared_project="service-b")


def _empty_bundle(directory):
    return bundle_file(directory, [], declared_project="service-b", project="service-b")


def _foreign_nodes_manifest(directory):
    return _dbt_manifest(directory, dbt_node("foreign"), declared_project="service-b")


def _unknown_kind(directory):
    return ManifestFile(path=str(directory / "odcs-bundle.json"), declared_project="service-b", kind="spark")


@pytest.mark.parametrize("artifact_error_first", [False, True])
@pytest.mark.parametrize("artifact_error, detail", [
    (_unreadable_bundle, "service-b: bundle_version 2 is newer than this install reads"),
    (_empty_bundle, "service-b: bundle declares no nodes"),
    (_foreign_nodes_manifest, "service-b: manifest contains nodes for ['service-1']"),
    (_unknown_kind, "service-b: unknown manifest kind 'spark'"),
])
def test_an_artifact_level_error_fails_the_release_with_no_failed_nodes(
        tmp_path, artifact_error, detail, artifact_error_first):
    """An unreadable, empty, wrong-project or unknown-kind manifest ends the release at once
    as invalid_artifact, whether an earlier or a later bundle holds invalid contracts."""
    (tmp_path / "invalid").mkdir()
    (tmp_path / "bad").mkdir()
    invalid = bundle_file(tmp_path / "invalid", [KPIS_WITH_NUMERIC_12])
    bad = artifact_error(tmp_path / "bad")
    publisher = MagicMock()

    python_handler(source_of(*((bad, invalid) if artifact_error_first else (invalid, bad))), publisher).handle(
        release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    assert kwargs["detail"].startswith(detail)
    assert kwargs["failed_nodes"] == []


def test_a_python_odcs_node_whose_query_fails_to_resolve_is_named_by_its_contract_file(tmp_path):
    """The SQL of a python-odcs node lives in its contract, so a resolve failure names the
    contract file, not the script."""
    publisher = MagicMock()
    unqualified = demo_text("py_daily_kpis").replace(
        "from analytics.daily_transactions", "from daily_transactions")
    source = source_of(bundle_file(tmp_path, [bundle_node(KPIS_PATH, unqualified)]))

    python_handler(source, publisher).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    [failed] = kwargs["failed_nodes"]
    assert (failed.node_id, failed.kind, failed.file_path) == (
        "analytics.py_daily_kpis", ParseFailureKind.UNQUALIFIED_REFERENCE, KPIS_PATH)
