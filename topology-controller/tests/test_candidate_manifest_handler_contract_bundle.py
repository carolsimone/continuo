"""The handler's contract bundle: every release that publishes ok writes one, after the
code bundle and before the topology artifact, holding the stored form of each python-odcs
contract of the release."""
import json
from pathlib import Path
from unittest.mock import MagicMock, create_autospec

import botocore.exceptions
import continuo_contracts
import pytest
from continuo_contracts.errors import CanonicalJsonError

import service.contract_bundle as contract_bundle_module
from adapters.redis.error_class import is_infrastructure
from domain.contract_vocabulary import ParseFailureKind
from domain.model import FailedNode, ManifestFile
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
    [(unique_id, contract)] = write["document"]["contracts"].items()
    assert (unique_id, contract["family"], contract["service"], contract["source"]["path"]) == (
        "test_schema.users", "dbt", "service-1", "models/users.sql")
    assert write["document"]["summary"] == {"contracts": 1, "by_family": {"dbt": 1}, "invalid": 0, "skipped": 0}
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


@pytest.mark.parametrize("first_service", ["service-a", "service-b"])
def test_an_id_two_python_odcs_services_declare_is_left_out_of_the_bundle_and_the_release_publishes(
    tmp_path, first_service, caplog,
):
    """Two services that each declare analytics.py_daily_kpis are release-controller's
    duplicate_table to reject (it names both services): the parse publishes ok, the topology
    holds both nodes, and the bundle holds neither contract of the shared id, whichever
    service comes first. Each node keeps the tables its own SQL reads."""
    bundle_writer, topology_writer, publisher = RecordingContractBundleWriter(), RecordingArtifactWriter(), MagicMock()
    other_reads = demo_text("py_daily_kpis").replace(
        "from analytics.daily_transactions", "from raw.other_transactions")
    files = {}
    for service, nodes in (
        ("service-a", [demo_node("py_daily_kpis"), demo_node("demo_orders_csv")]),
        ("service-b", [bundle_node(KPIS_PATH, other_reads)]),
    ):
        folder = tmp_path / service
        folder.mkdir()
        files[service] = bundle_file(folder, nodes, declared_service=service, service=service)
    second_service = "service-b" if first_service == "service-a" else "service-a"

    with caplog.at_level("WARNING"):
        python_handler(
            source_of(files[first_service], files[second_service]), publisher,
            contract_bundle_writer=bundle_writer, artifact_writer=topology_writer,
        ).handle(release_id="rel-1")

    publisher.publish_failed.assert_not_called()
    publisher.publish_ok.assert_called_once()
    assert sorted(bundle_writer.document["contracts"]) == ["analytics.demo_orders_csv"]
    assert bundle_writer.document["summary"]["contracts"] == 1
    kpis = sorted(
        (n["service_name"], sorted(n["upstream_unique_ids"]))
        for n in topology_writer.nodes if n["unique_id"] == "analytics.py_daily_kpis")
    assert kpis == [
        ("service-a", ["analytics.daily_transactions"]),
        ("service-b", ["raw.other_transactions"]),
    ]
    [record] = [r for r in caplog.records if "more than one node" in r.getMessage()]
    assert record.unique_id == "analytics.py_daily_kpis"
    assert record.services == ["service-a", "service-b"]


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


def test_canonical_hashing_of_a_dbt_release_counts_its_dbt_contracts():
    log: list[str] = []
    handler, _publisher, _bundle_writer, _topology_writer = _dbt_handler(
        log, names=("manifest_service1.json", "manifest_service2.json"))

    timings = handler.handle(release_id="rel-1")

    record = timings.stages[Stage.CANONICAL_HASHING]
    assert (record.runs, record.counts) == (1, {"contracts": 2})


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


# --- a table the contract cannot list is the author's mistake ---------------------

def _kpis_reading(table: str, *, name: str = "py_daily_kpis") -> dict:
    """The demo kpis contract (or a copy of it under another name) with its read query on `table`."""
    text = demo_text("py_daily_kpis").replace("py_daily_kpis", name).replace(
        "from analytics.daily_transactions", f"from {table}")
    return bundle_node(f"contracts/{name}.odcs.yaml", text)


@pytest.mark.parametrize(
    ("table", "written", "reason"),
    [
        ('analytics."daily transactions"', 'analytics."daily transactions"', "a name holds whitespace"),
        ('analytics."daily.transactions"', 'analytics."daily.transactions"', "a name holds a dot"),
        ('"my schema".daily_transactions', '"my schema".daily_transactions', "a name holds whitespace"),
    ],
    ids=["whitespace", "dot", "schema-whitespace"])
def test_a_table_the_contract_cannot_list_rejects_the_release_as_an_invalid_contract(tmp_path, table, written, reason):
    publisher, bundle_writer, topology_writer = MagicMock(), RecordingContractBundleWriter(), RecordingArtifactWriter()
    log: list[str] = []
    source = source_of(bundle_file(tmp_path, [_kpis_reading(table), demo_node("demo_orders_csv")]))

    timings = python_handler(
        source, publisher, contract_bundle_writer=bundle_writer, artifact_writer=topology_writer,
        bundle_uploader=FakeBundleUploader(log=log),
    ).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_CONTRACT
    assert kwargs["detail"] == "1 node failed to parse: analytics.py_daily_kpis"
    assert kwargs["failed_nodes"] == [FailedNode(
        node_id="analytics.py_daily_kpis",
        kind=ParseFailureKind.INVALID_CONTRACT,
        service="service-py",
        file_path=KPIS_PATH,
        node_type="python-node",
        detail=f"readQueries: table {written} cannot be listed as <schema>.<table>: {reason}",
    )]
    assert timings.contract_failures == {("normalise", "value"): 1}
    assert (timings.outcome, timings.failure_kind) == (ReleaseOutcome.FAILED, ParseFailureKind.INVALID_CONTRACT)
    assert bundle_writer.writes == [] and topology_writer.writes == [] and log == []
    assert timings.stages[Stage.CANONICAL_HASHING].runs == 0


def test_every_contract_with_such_a_table_is_named_on_the_one_publish_with_the_other_failures(tmp_path):
    """Each failing contract is one failed node, whatever the order of the bundle, and a broken
    SQL node of another service is reported in the same publish."""
    publisher = MagicMock()
    broken_sql = {**dbt_node("broken"), "compiled_code": "SELECT FROM WHERE((("}
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"nodes": {broken_sql["unique_id"]: broken_sql}}))
    source = source_of(
        bundle_file(tmp_path, [
            _kpis_reading('analytics."a b"', name="py_other_kpis"),
            _kpis_reading('analytics."c.d"'),
            _kpis_reading('analytics."e f"', name="py_third_kpis"),
            demo_node("demo_orders_csv"),
        ]),
        ManifestFile(path=str(manifest), image_tag="", declared_service="service-1"),
    )

    timings = python_handler(source, publisher).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_SQL
    assert [(n.node_id, n.kind) for n in kwargs["failed_nodes"]] == [
        ("analytics.py_daily_kpis", ParseFailureKind.INVALID_CONTRACT),
        ("analytics.py_other_kpis", ParseFailureKind.INVALID_CONTRACT),
        ("analytics.py_third_kpis", ParseFailureKind.INVALID_CONTRACT),
        ("test_schema.broken", ParseFailureKind.INVALID_SQL),
    ]
    assert timings.contract_failures == {("normalise", "value"): 3}


def test_a_contract_reading_two_such_tables_names_both_in_one_node(tmp_path):
    publisher = MagicMock()
    text = demo_text("py_daily_kpis").replace(
        "from analytics.daily_transactions", 'from analytics."b c" join analytics."a b" on true')
    source = source_of(bundle_file(tmp_path, [bundle_node(KPIS_PATH, text)]))

    timings = python_handler(source, publisher).handle(release_id="rel-1")

    [failure] = _failed_kwargs(publisher)["failed_nodes"]
    assert failure.detail == (
        'readQueries: table analytics."a b" cannot be listed as <schema>.<table>: a name holds whitespace; '
        'readQueries: table analytics."b c" cannot be listed as <schema>.<table>: a name holds whitespace')
    assert timings.contract_failures == {("normalise", "value"): 2}


def test_a_system_relation_with_an_odd_name_is_not_a_table_the_contract_lists(tmp_path):
    publisher, bundle_writer = MagicMock(), RecordingContractBundleWriter()
    source = source_of(bundle_file(tmp_path, [_kpis_reading('pg_catalog."odd name"')]))

    python_handler(source, publisher, contract_bundle_writer=bundle_writer).handle(release_id="rel-1")

    publisher.publish_ok.assert_called_once()
    [table] = bundle_writer.document["contracts"]["analytics.py_daily_kpis"]["document"]["schema"]
    assert [p for p in table.get("customProperties", []) if p["property"] == "reads"] == []


@pytest.mark.parametrize("failing", ["normalise_python", "contract_hash"])
def test_a_genuine_library_error_still_fails_the_release_as_internal_and_counts_it(tmp_path, monkeypatch, failing):
    """A contract that passed every check and whose tables can all be listed is refused by the
    library only through a defect: internal, counted by stage, and nothing is written."""
    def refuse(*_args, **_kwargs):
        raise CanonicalJsonError("$: not writable")

    monkeypatch.setattr(contract_bundle_module, failing, refuse)
    publisher, bundle_writer, topology_writer = MagicMock(), RecordingContractBundleWriter(), RecordingArtifactWriter()
    source = source_of(bundle_file(tmp_path, demo_nodes()))

    timings = python_handler(
        source, publisher, contract_bundle_writer=bundle_writer, artifact_writer=topology_writer,
    ).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INTERNAL
    assert kwargs["failed_nodes"] == []
    assert "not writable" in kwargs["detail"]
    assert timings.contract_failures == {("normalise" if failing == "normalise_python" else "hash", "value"): 1}
    assert bundle_writer.writes == [] and topology_writer.writes == []


# --- a failed bundle write follows the outage rule -------------------------------

_OUTAGES = [
    botocore.exceptions.EndpointConnectionError(endpoint_url="http://minio:9000"),
    botocore.exceptions.ClientError(
        {"Error": {"Code": "SlowDown", "Message": "reduce your request rate"},
         "ResponseMetadata": {"HTTPStatusCode": 503}}, "PutObject"),
    # What the writer raises when its last conditional put still met another
    # writer's put in progress: the redelivery reads the object that writer stored.
    botocore.exceptions.ClientError(
        {"Error": {"Code": "ConditionalRequestConflict", "Message": "a conflicting conditional write is in progress"},
         "ResponseMetadata": {"HTTPStatusCode": 409}}, "PutObject"),
]


@pytest.mark.parametrize("outage", _OUTAGES, ids=["s3-unreachable", "s3-503", "unsettled-conditional-write"])
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
