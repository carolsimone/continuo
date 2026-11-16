"""The handler's contract bundle: every release that publishes ok writes one, after the
code bundle and before the topology artifact, holding the stored form of each python-odcs
contract of the release."""
import json
from pathlib import Path
from unittest.mock import MagicMock, create_autospec

import botocore.exceptions
import continuo_contracts
import pytest

from adapters.redis.error_class import is_infrastructure
from domain.contract_vocabulary import ParseFailureKind
from domain.model import ManifestFile, ManifestKind
from service.ports import ManifestSourcePort
from service.release_timings import ReleaseOutcome, Stage
from tests.fakes import (
    FakeBundleUploader,
    FakeClock,
    RecordingArtifactWriter,
    RecordingContractBundleWriter,
    build_handler,
    dbt_node,
    make_uploader,
    python_contract,
    python_entry,
    python_handler,
    source_of,
)
from tests.odcs_fixtures import bundle_file, bundle_node, demo_node, demo_nodes, demo_text

FIXTURES = Path(__file__).parent / "fixtures"
_VECTORS = json.loads((Path(continuo_contracts.__file__).parent / "vectors" / "contracts.json").read_text())
KPIS_PATH = "contracts/py_daily_kpis.odcs.yaml"


def _vector(name: str) -> dict:
    return next(v for v in _VECTORS if v["name"] == name)


def _dbt_source(*names: str):
    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [ManifestFile(path=str(FIXTURES / name), image_tag="") for name in names]
    source.manifest_count.return_value = len(names)
    return source


def _dbt_handler(log, *, fail=None, publisher=None, names=("manifest_service1.json",)):
    publisher = publisher if publisher is not None else MagicMock()
    bundle_writer = RecordingContractBundleWriter(fail=fail, log=log)
    topology_writer = RecordingArtifactWriter(log=log)
    handler = build_handler(
        _dbt_source(*names), publisher, make_uploader(), bundle_uploader=FakeBundleUploader(log=log),
        artifact_writer=topology_writer, contract_bundle_writer=bundle_writer)
    return handler, publisher, bundle_writer, topology_writer


def _failed_kwargs(publisher):
    publisher.publish_failed.assert_called_once()
    publisher.publish_ok.assert_not_called()
    return publisher.publish_failed.call_args.kwargs


# --- every ok release writes one bundle ----------------------------------------

def test_every_ok_release_writes_one_bundle_before_the_topology_artifact():
    log: list[str] = []
    handler, publisher, bundle_writer, topology_writer = _dbt_handler(log)

    handler.handle(release_id="rel-1")

    assert log == ["code_bundle", "contract_bundle", "topology_artifact"]
    [write] = bundle_writer.writes
    assert (write["tenant_id"], write["release_id"]) == ("default", "rel-1")
    assert write["document"]["contracts"] == {}, "a dbt-only release has no contract"
    assert write["document"]["summary"] == {"contracts": 0, "by_family": {}, "invalid": 0, "skipped": 0}
    publisher.publish_ok.assert_called_once()
    kwargs = publisher.publish_ok.call_args.kwargs
    assert kwargs["contract_bundle"] == bundle_writer.refs[0]
    assert kwargs["artifact"] == topology_writer.refs[0]


def test_a_release_with_no_manifest_writes_an_empty_bundle_too():
    log: list[str] = []
    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = []
    publisher = MagicMock()
    bundle_writer = RecordingContractBundleWriter(log=log)
    handler = build_handler(
        source, publisher, make_uploader(), artifact_writer=RecordingArtifactWriter(log=log),
        contract_bundle_writer=bundle_writer)

    handler.handle(release_id="rel-empty")

    assert log == ["contract_bundle", "topology_artifact"]
    assert bundle_writer.document["contracts"] == {}
    assert bundle_writer.document["provenance"] == {}
    assert bundle_writer.document["release_id"] == "rel-empty"
    publisher.publish_ok.assert_called_once()
    assert publisher.publish_ok.call_args.kwargs["contract_bundle"] == bundle_writer.refs[0]


def test_the_bundle_holds_the_stored_form_of_each_python_odcs_contract(tmp_path):
    bundle_writer = RecordingContractBundleWriter()
    source = source_of(bundle_file(tmp_path, demo_nodes()))

    python_handler(source, MagicMock(), contract_bundle_writer=bundle_writer).handle(release_id="rel-1")

    doc = bundle_writer.document
    assert sorted(doc["contracts"]) == ["analytics.demo_orders_csv", "analytics.fx_eur_usd_rates", "analytics.py_daily_kpis"]
    kpis = doc["contracts"]["analytics.py_daily_kpis"]
    assert kpis["family"] == "python"
    assert kpis["service"] == "service-py"
    assert kpis["contract_hash"] == _vector("demo_py_daily_kpis")["contract_hash"]
    assert kpis["document"] == _vector("demo_py_daily_kpis")["stored"], "the read query names analytics.daily_transactions"
    assert kpis["source"]["path"] == KPIS_PATH
    assert kpis["content_hash"].startswith("sha256:")
    assert doc["summary"] == {"contracts": 3, "by_family": {"python": 3}, "invalid": 0, "skipped": 0}
    assert doc["continuo_contracts"] == continuo_contracts.__version__
    assert doc["provenance"] == {
        "service-py": {"continuo_python_runtime": "0.13.0", "image_packages": {"pyarrow": "26.0.0"}}}


def test_the_content_hash_of_a_contract_is_the_nodes_content_hash(tmp_path):
    bundle_writer, topology_writer = RecordingContractBundleWriter(), RecordingArtifactWriter()
    source = source_of(bundle_file(tmp_path, demo_nodes()))

    python_handler(
        source, MagicMock(), contract_bundle_writer=bundle_writer, artifact_writer=topology_writer,
    ).handle(release_id="rel-1")

    from_topology = {n["unique_id"]: n["content_hash"] for n in topology_writer.nodes if n["node_type"] != "unknown-external"}
    from_bundle = {uid: c["content_hash"] for uid, c in bundle_writer.document["contracts"].items()}
    assert from_bundle == from_topology


def test_a_contract_reads_the_tables_its_queries_name_and_no_system_relation(tmp_path):
    """The reads of the stored form come from the resolve step's tables: a system relation of
    the install's engine is left out, a catalog-qualified table is named by schema and table."""
    query = "select created_at, amount_eur from analytics.daily_transactions"
    text = demo_text("py_daily_kpis").replace(
        query,
        "select d.created_at, d.amount_eur from warehouse.analytics.daily_transactions d, pg_catalog.pg_class c")
    assert text != demo_text("py_daily_kpis")
    bundle_writer = RecordingContractBundleWriter()
    source = source_of(bundle_file(tmp_path, [bundle_node(KPIS_PATH, text)]))

    python_handler(source, MagicMock(), contract_bundle_writer=bundle_writer).handle(release_id="rel-1")

    [table] = bundle_writer.document["contracts"]["analytics.py_daily_kpis"]["document"]["schema"]
    assert table["customProperties"] == [
        {"property": "reads", "value": ["analytics.daily_transactions"], "vendor": "continuo"}]


def test_a_wire_python_contract_has_no_entry_in_the_bundle(tmp_path):
    bundle_writer = RecordingContractBundleWriter()
    wire_dir, odcs_dir = tmp_path / "wire", tmp_path / "odcs"
    wire_dir.mkdir()
    odcs_dir.mkdir()
    source = source_of(
        ManifestFile(
            path=python_contract(wire_dir, python_entry(), service="service-wire"),
            declared_service="service-wire", kind=ManifestKind.PYTHON),
        bundle_file(odcs_dir, demo_nodes()),
    )

    python_handler(source, MagicMock(), contract_bundle_writer=bundle_writer).handle(release_id="rel-1")

    doc = bundle_writer.document
    assert sorted(doc["contracts"]) == ["analytics.demo_orders_csv", "analytics.fx_eur_usd_rates", "analytics.py_daily_kpis"]
    assert list(doc["provenance"]) == ["service-py"]


def test_each_python_odcs_service_has_its_own_provenance(tmp_path):
    bundle_writer = RecordingContractBundleWriter()
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    source = source_of(
        bundle_file(first, [demo_node("py_daily_kpis")], declared_service="service-a", service="service-a",
                    producer_runtime="0.13.0", image_packages={"pyarrow": "26.0.0"}),
        bundle_file(second, [demo_node("demo_orders_csv")], declared_service="service-b", service="service-b",
                    producer_runtime="0.12.0", image_packages={}),
    )

    python_handler(source, MagicMock(), contract_bundle_writer=bundle_writer).handle(release_id="rel-1")

    assert bundle_writer.document["provenance"] == {
        "service-a": {"continuo_python_runtime": "0.13.0", "image_packages": {"pyarrow": "26.0.0"}},
        "service-b": {"continuo_python_runtime": "0.12.0", "image_packages": {}},
    }


# --- the bundle's timings and the stages ------------------------------------------

def test_the_bundle_timings_hold_every_stage_before_its_own_write(tmp_path):
    bundle_writer = RecordingContractBundleWriter()
    source = source_of(bundle_file(tmp_path, demo_nodes()))

    python_handler(
        source, MagicMock(), contract_bundle_writer=bundle_writer, clock=FakeClock(step=0.001),
    ).handle(release_id="rel-1")

    stages = bundle_writer.document["timings"]["stages_ms"]
    assert {"download", "python_parse", "registry", "resolve", "code_bundle", "contract_validation",
            "canonical_hashing"} <= set(stages)
    assert not {"contract_bundle", "topology_artifact", "publish"} & set(stages)
    assert "dbt_parse" not in stages, "a stage that did not run has no record"
    assert stages["resolve"] >= 1 and stages["code_bundle"] >= 1, "every stage that ran reports its milliseconds"


def test_the_bundle_timings_count_the_nodes_the_release_holds(tmp_path):
    bundle_writer = RecordingContractBundleWriter()
    source = source_of(bundle_file(tmp_path, demo_nodes()))

    python_handler(source, MagicMock(), contract_bundle_writer=bundle_writer).handle(release_id="rel-1")

    timings = bundle_writer.document["timings"]
    assert timings["nodes"]["python"] == 3
    assert timings["nodes"]["external"] == 1, "analytics.daily_transactions is read and built by no node"
    assert timings["columns"]["python"] > 0
    assert set(timings["validation_cache"]) == {"hit", "miss"}
    assert sum(timings["validation_cache"].values()) == 3


def test_canonical_hashing_is_timed(tmp_path):
    source = source_of(bundle_file(tmp_path, demo_nodes()))

    timings = python_handler(source, MagicMock()).handle(release_id="rel-1")

    record = timings.stages[Stage.CANONICAL_HASHING]
    assert (record.runs, record.counts) == (1, {"contracts": 3})
    assert timings.stages[Stage.CONTRACT_BUNDLE].runs == 1


def test_canonical_hashing_of_a_release_with_no_contract_is_timed_with_none():
    log: list[str] = []
    handler, _publisher, _bundle_writer, _topology_writer = _dbt_handler(log)

    timings = handler.handle(release_id="rel-1")

    record = timings.stages[Stage.CANONICAL_HASHING]
    assert (record.runs, record.counts) == (1, {"contracts": 0})


# --- a failed release writes no contract bundle ----------------------------------

def _invalid_contract_release(tmp_path):
    return source_of(bundle_file(tmp_path, [bundle_node(
        KPIS_PATH, demo_text("py_daily_kpis").replace("NUMERIC(12,2)", "NUMERIC(12)")), demo_node("demo_orders_csv")]))


def _invalid_sql_release(tmp_path):
    path = tmp_path / "manifest.json"
    broken = {**dbt_node("broken"), "compiled_code": "SELECT FROM WHERE((("}
    path.write_text(json.dumps({"nodes": {broken["unique_id"]: broken}}))
    return source_of(ManifestFile(path=str(path), image_tag="", declared_service="service-1"))


def _unknown_kind_release(tmp_path):
    return source_of(ManifestFile(path=str(tmp_path / "x"), image_tag="", declared_service="s", kind="spark"))


@pytest.mark.parametrize(
    "release", [_invalid_contract_release, _invalid_sql_release, _unknown_kind_release],
    ids=["invalid-contract", "invalid-sql", "unknown-kind"])
def test_no_contract_bundle_is_written_for_a_rejected_release(tmp_path, release):
    publisher, bundle_writer = MagicMock(), RecordingContractBundleWriter()

    timings = python_handler(release(tmp_path), publisher, contract_bundle_writer=bundle_writer).handle(release_id="rel-1")

    publisher.publish_failed.assert_called_once()
    publisher.publish_ok.assert_not_called()
    assert bundle_writer.writes == []
    assert timings.outcome is ReleaseOutcome.FAILED
    assert timings.stages[Stage.CANONICAL_HASHING].runs == 0
    assert timings.stages[Stage.CONTRACT_BUNDLE].runs == 0


# --- a library error is a bug: the release fails as internal ---------------------

def test_a_table_the_library_refuses_fails_the_release_as_internal_and_counts_it(tmp_path):
    """A read query naming a table with a space in its name passes every check of the
    contract and no check of the SQL; the stored form refuses it. It is reported as a
    failure of the release, counted, and nothing is written."""
    text = demo_text("py_daily_kpis").replace(
        "from analytics.daily_transactions", 'from analytics."daily transactions"')
    publisher, bundle_writer, topology_writer = MagicMock(), RecordingContractBundleWriter(), RecordingArtifactWriter()
    source = source_of(bundle_file(tmp_path, [bundle_node(KPIS_PATH, text)]))

    timings = python_handler(
        source, publisher, contract_bundle_writer=bundle_writer, artifact_writer=topology_writer,
    ).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INTERNAL
    assert kwargs["failed_nodes"] == []
    assert "analytics.daily transactions" in kwargs["detail"]
    assert timings.contract_failures == {("normalise", "value"): 1}
    assert bundle_writer.writes == [] and topology_writer.writes == []


# --- a failed bundle write follows the outage rule -------------------------------

_OUTAGES = [
    botocore.exceptions.EndpointConnectionError(endpoint_url="http://minio:9000"),
    botocore.exceptions.ClientError(
        {"Error": {"Code": "SlowDown", "Message": "reduce your request rate"},
         "ResponseMetadata": {"HTTPStatusCode": 503}}, "PutObject"),
]


@pytest.mark.parametrize("outage", _OUTAGES, ids=["s3-unreachable", "s3-503"])
def test_a_failed_bundle_write_follows_the_outage_rule(outage):
    """An outage reaches the consumer to wait out, publishes nothing and leaves the topology
    artifact unwritten, so the release waits in parsing."""
    log: list[str] = []
    handler, publisher, _bundle_writer, topology_writer = _dbt_handler(log, fail=outage)

    with pytest.raises(type(outage)) as raised:
        handler.handle(release_id="rel-1")

    assert is_infrastructure(raised.value)
    publisher.publish_ok.assert_not_called()
    publisher.publish_failed.assert_not_called()
    assert topology_writer.writes == []


def test_a_refused_bundle_write_fails_the_release_as_internal():
    denied = botocore.exceptions.ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}, "ResponseMetadata": {"HTTPStatusCode": 403}},
        "PutObject")
    log: list[str] = []
    handler, publisher, _bundle_writer, topology_writer = _dbt_handler(log, fail=denied)

    timings = handler.handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INTERNAL
    assert kwargs["failed_nodes"] == []
    assert "AccessDenied" in kwargs["detail"]
    assert topology_writer.writes == []
    assert (timings.outcome, timings.failure_kind) == (ReleaseOutcome.FAILED, ParseFailureKind.INTERNAL)


def test_the_bundle_write_is_timed_as_its_own_stage_even_when_it_fails():
    denied = botocore.exceptions.ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}, "ResponseMetadata": {"HTTPStatusCode": 403}},
        "PutObject")
    handler, _publisher, _bundle_writer, _topology_writer = _dbt_handler([], fail=denied)

    timings = handler.handle(release_id="rel-1")

    assert timings.stages[Stage.CONTRACT_BUNDLE].runs == 1
    assert timings.stages[Stage.TOPOLOGY_ARTIFACT].runs == 0
