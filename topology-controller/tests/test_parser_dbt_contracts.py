"""The dbt parse builds each node the gates keep from its generated ODCS contract: the contract is
generated and checked per node, an invalid one is a failed node of the release, and the node's run
facts are read back from the contract."""
import dataclasses
import json
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from continuo_contracts import dbt
from continuo_contracts.errors import CanonicalJsonError

from domain.contract_vocabulary import NodeType, ParseFailureKind
from domain.dormant import is_dormant
from domain.exceptions import ContractGenerationError, InvalidNodeSettingError
from domain.model import ContractFamily, FailedNode, ManifestFile
from service.contract_validation import ContractValidator
from service.parser import parse_manifest
from service.release_timings import ReleaseTimings, Stage
from tests.fakes import (
    FakeClock,
    RecordingArtifactWriter,
    RecordingContractBundleWriter,
    ScriptedMemoryProbe,
    build_handler,
    dbt_node,
    make_uploader,
    python_handler,
    source_of,
)
from tests.odcs_fixtures import bundle_file, bundle_node, demo_node, demo_text

_MATERIALIZED = {"model": "table", "seed": "seed", "snapshot": "snapshot"}
_FILE_PATH = {"model": "models/{}.sql", "seed": "seeds/{}.csv", "snapshot": "snapshots/{}.sql"}


def _node(table, *, resource_type="model", tags=("daily",), meta=None, **over):
    """A node of the project `shop`, as dbt writes it; `over` replaces any field."""
    node = {
        "unique_id": f"{resource_type}.shop.{table}",
        "resource_type": resource_type,
        "name": table,
        "schema": "analytics",
        "fqn": ["shop", table],
        "tags": list(tags),
        "original_file_path": _FILE_PATH[resource_type].format(table),
        "config": {"materialized": _MATERIALIZED[resource_type], "meta": {"owner": "team-a", **(meta or {})}},
        "checksum": {"name": "sha256", "checksum": "ab" * 32},
        "raw_code": "select 1 as id" if resource_type != "seed" else "",
        "compiled_code": "select 1 as id" if resource_type != "seed" else "",
        "depends_on": {"macros": [], "nodes": []},
    }
    node.update(over)
    return node


def _not_null_test(name, target):
    return {
        "unique_id": f"test.shop.not_null_{name}_id",
        "resource_type": "test",
        "name": f"not_null_{name}_id",
        "schema": "analytics_dbt_test__audit",
        "fqn": ["shop", f"not_null_{name}_id"],
        "original_file_path": "models/schema.yml",
        "attached_node": target,
        "column_name": "id",
        "test_metadata": {"name": "not_null", "kwargs": {"column_name": "id", "model": "{{ get_where_subquery(ref('x')) }}"}},
        "depends_on": {"macros": ["macro.dbt.test_not_null"], "nodes": [target]},
        "config": {"severity": "ERROR"},
        "compiled_code": f"select id from analytics.{name} where id is null",
    }


def _manifest(tmp_path, *nodes, name="manifest.json", metadata=None) -> str:
    manifest = {"metadata": metadata if metadata is not None else {"project_name": "shop"},
                "macros": {}, "nodes": {n["unique_id"]: n for n in nodes}}
    path = Path(tmp_path) / name
    path.write_text(json.dumps(manifest))
    return str(path)


def _timings() -> ReleaseTimings:
    return ReleaseTimings("rel-1", FakeClock(), ScriptedMemoryProbe(0))


def _failed_kwargs(publisher):
    publisher.publish_failed.assert_called_once()
    publisher.publish_ok.assert_not_called()
    return publisher.publish_failed.call_args.kwargs


# --- the contract of each kept node ---------------------------------------------

def test_each_kept_node_carries_its_generated_contract(tmp_path):
    orders = _node("orders", columns={"id": {"name": "id", "data_type": "integer"}})
    countries = _node("countries", resource_type="seed")
    test = _not_null_test("orders", "model.shop.orders")
    path = _manifest(tmp_path, orders, countries, test)
    manifest = json.loads(Path(path).read_text())

    parsed = parse_manifest(path)

    by_id = {node.unique_id: node for node in parsed.nodes}
    assert parsed.failed == ()
    for unique_id, manifest_id in (("analytics.orders", "model.shop.orders"),
                                   ("analytics.countries", "seed.shop.countries")):
        node = manifest["nodes"][manifest_id]
        generated = dbt.generate(node, dbt.tests_by_node(manifest).get(manifest_id, []), manifest=manifest)
        contract = by_id[unique_id].contract
        assert contract.family is ContractFamily.DBT
        assert contract.document == generated.document
        assert contract.path == node["original_file_path"]
        assert contract.source_sha256 == generated.source_sha256
    # The test is turned into a quality rule of the model's contract, and is no contract itself.
    assert by_id["analytics.orders"].contract.document["schema"][0]["properties"][0]["required"] is True
    assert by_id["test.shop.not_null_orders_id"].contract is None


def test_run_facts_come_from_the_contract(tmp_path, monkeypatch):
    """Every run fact of the node is what `dbt.run_facts` reads from the contract, not
    what the manifest says."""
    real = dbt.run_facts

    def from_contract(document):
        return dataclasses.replace(
            real(document), schema="from_contract_schema", table="from_contract_table",
            physical_name="from_contract_relation", service="from-contract-service", kind="dbt-snapshot",
            schedule="from_contract", criticality="REGULATORY", timeout_minutes=7,
            file_path="from/contract.sql", owner="from-contract-team", materialized="from_contract",
        )

    monkeypatch.setattr(dbt, "run_facts", from_contract)
    (node,) = parse_manifest(_manifest(tmp_path, _node("orders"))).nodes

    assert (node.schema_name, node.table_name, node.resolved_relation) == (
        "from_contract_schema", "from_contract_table", "from_contract_relation")
    assert (node.service_name, node.node_type, node.schedule_name, node.criticality) == (
        "from-contract-service", NodeType.DBT_SNAPSHOT, "from_contract", "REGULATORY")
    assert (node.timeout_seconds, node.original_file_path, node.owner, node.materialized) == (
        420, "from/contract.sql", "from-contract-team", "from_contract")
    # The contract stands for the node's file as the manifest names it.
    assert node.contract.path == "models/orders.sql"


def test_the_facts_a_well_formed_node_gets_from_its_contract_are_the_manifests(tmp_path):
    node = _node("orders_v2", alias="orders", meta={"criticality": "CORE", "timeout_minutes": 30},
                 config={"materialized": "incremental",
                         "meta": {"owner": "team-a", "criticality": "CORE", "timeout_minutes": 30}})
    (parsed,) = parse_manifest(_manifest(tmp_path, node)).nodes
    assert (parsed.schema_name, parsed.table_name, parsed.resolved_relation, parsed.materialized) == (
        "analytics", "orders_v2", "orders", "incremental")
    assert (parsed.service_name, parsed.node_type, parsed.schedule_name, parsed.criticality, parsed.owner) == (
        "shop", NodeType.DBT_MODEL, "daily", "CORE", "team-a")
    assert (parsed.timeout_seconds, parsed.original_file_path) == (1800, "models/orders_v2.sql")


def test_the_dormant_node_has_an_empty_schedule_from_its_contract(tmp_path):
    (node,) = parse_manifest(_manifest(tmp_path, _node("report", tags=()))).nodes
    assert "schedule" not in {item["property"] for item in node.contract.document["customProperties"]}
    assert node.schedule_name == ""
    assert is_dormant(node.node_type, node.schedule_name)


def test_a_schema_declared_with_capitals_is_lower_case(tmp_path):
    (node,) = parse_manifest(_manifest(tmp_path, _node("orders", schema="Analytics"))).nodes
    assert (node.schema_name, node.unique_id, node.contract.document["id"]) == (
        "analytics", "analytics.orders", "analytics.orders")


# --- an invalid contract is a failed node -----------------------------------------

def test_a_bad_timeout_and_a_bad_criticality_are_invalid_contracts(tmp_path):
    timings = _timings()
    parsed = parse_manifest(
        _manifest(tmp_path, _node("orders", meta={"timeout_minutes": 0, "criticality": "high"})), timings=timings)

    assert parsed.nodes == []
    assert parsed.failed == (FailedNode(
        node_id="analytics.orders",
        kind=ParseFailureKind.INVALID_CONTRACT,
        service="shop",
        file_path="models/orders.sql",
        node_type=NodeType.DBT_MODEL,
        detail="customProperties[3].value: `criticality` is 'high', it must be one of REGULATORY, CORE, SECONDARY; "
               "customProperties[4].value: `timeoutMinutes` is 0, it must be an integer number of minutes from 1"
               " to 10080",
    ),)
    assert timings.contract_failures == {("validate", "value"): 2}


def test_every_invalid_contract_of_a_manifest_is_named_and_the_valid_nodes_are_kept(tmp_path):
    parsed = parse_manifest(_manifest(
        tmp_path,
        _node("orders", meta={"criticality": "high"}),
        _node("customers"),
        _node("payments", meta={"timeout_minutes": 99999}),
    ))
    assert [node.unique_id for node in parsed.nodes] == ["analytics.customers"]
    assert [failed.node_id for failed in parsed.failed] == ["analytics.orders", "analytics.payments"]


@pytest.mark.parametrize("node, name, message", [
    # skip_reason cannot read the node's tags.
    ({**_node("orders"), "tags": "daily"}, "analytics.orders",
     "model.shop.orders: tags must be a list of strings, got str"),
    # generate cannot read the node's owner.
    (_node("orders", meta={"owner": 5}), "analytics.orders",
     "model.shop.orders: config.meta.owner must be a non-empty string, got int"),
    # generate cannot read the node's name, so the node is named by its manifest id.
    (_node("orders", name=""), "model.shop.orders",
     "model.shop.orders: name must be a non-empty string, got an empty string"),
])
def test_a_node_shape_error_is_an_invalid_contract_naming_the_node(tmp_path, node, name, message):
    timings = _timings()
    parsed = parse_manifest(_manifest(tmp_path, node, _node("customers")), timings=timings)

    assert [n.unique_id for n in parsed.nodes] == ["analytics.customers"]
    assert parsed.failed == (FailedNode(
        node_id=name, kind=ParseFailureKind.INVALID_CONTRACT, service="shop", file_path="models/orders.sql",
        node_type=NodeType.DBT_MODEL, detail=message,
    ),)
    assert timings.contract_failures == {("generate", "manifest_shape"): 1}


def test_a_node_shape_error_reads_the_service_and_file_path_tolerantly(tmp_path):
    node = _node("orders", fqn="shop", original_file_path=7)
    (failed,) = parse_manifest(_manifest(tmp_path, node)).failed
    assert (failed.node_id, failed.service, failed.file_path, failed.detail) == (
        "analytics.orders", "", "", "model.shop.orders: fqn must be a list, got str")


@pytest.mark.parametrize("metadata, extra, message", [
    ({"project_name": 7}, None, "manifest: metadata.project_name must be a string, got int"),
    (None, {**_not_null_test("orders", "model.shop.orders"), "depends_on": {"nodes": [5], "macros": []}},
     "test.shop.not_null_orders_id: depends_on.nodes[0] must be a string, got int"),
])
def test_a_manifest_shape_error_is_an_invalid_artifact(tmp_path, metadata, extra, message):
    """A field no one node's contract is to blame for (the project's name, a test's
    links to the nodes it tests) refuses the manifest as a whole."""
    nodes = [_node("orders")] + ([extra] if extra is not None else [])
    timings = _timings()
    with pytest.raises(InvalidNodeSettingError) as raised:
        parse_manifest(_manifest(tmp_path, *nodes, metadata=metadata), timings=timings)
    assert str(raised.value) == message
    assert timings.contract_failures == {("generate", "manifest_shape"): 1}


def test_a_kept_node_with_no_fqn_entry_is_still_an_invalid_artifact(tmp_path):
    with pytest.raises(InvalidNodeSettingError, match="model.shop.orders: fqn must be a non-empty list of strings"):
        parse_manifest(_manifest(tmp_path, _node("orders", fqn=[]), metadata={}))


def test_tests_attach_only_to_nodes_that_became_manifest_nodes(tmp_path):
    parsed = parse_manifest(_manifest(
        tmp_path,
        _node("orders", meta={"criticality": "high"}),
        _not_null_test("orders", "model.shop.orders"),
        _node("customers"),
        _not_null_test("customers", "model.shop.customers"),
    ))
    by_id = {node.unique_id: node for node in parsed.nodes}
    assert sorted(by_id) == ["analytics.customers", "test.shop.not_null_customers_id"]
    assert by_id["analytics.customers"].test_count == 1


# --- a defect of the library ---------------------------------------------------

def _raise(error):
    def call(*args, **kwargs):
        raise error
    return call


@pytest.mark.parametrize("target, name, error", [
    (dbt, "generate", KeyError("materialization")),
    (dbt, "run_facts", KeyError("filePath")),
    (dbt, "skip_reason", TypeError("boom")),
    (ContractValidator, "check_dbt", CanonicalJsonError("$.schema: a value canonical JSON cannot write")),
])
def test_a_library_defect_raises_a_contract_generation_error(tmp_path, monkeypatch, target, name, error):
    monkeypatch.setattr(target, name, _raise(error))
    with pytest.raises(ContractGenerationError, match="model.shop.orders: the contracts library failed") as raised:
        parse_manifest(_manifest(tmp_path, _node("orders")))
    assert raised.value.__cause__ is error


def test_a_library_defect_in_a_manifest_call_raises_a_contract_generation_error(tmp_path, monkeypatch):
    monkeypatch.setattr(dbt, "tests_by_node", _raise(AttributeError("nodes")))
    with pytest.raises(ContractGenerationError, match="manifest: the contracts library failed"):
        parse_manifest(_manifest(tmp_path, _node("orders")))


def test_a_library_defect_is_internal(tmp_path, monkeypatch, caplog):
    """A KeyError from `generate` is a defect of continuo, never the team's artifact:
    the release fails as internal, with no failed node."""
    monkeypatch.setattr(dbt, "generate", _raise(KeyError("materialization")))
    publisher = MagicMock()
    source = source_of(ManifestFile(path=_manifest(tmp_path, _node("orders")), declared_service="shop"))

    with caplog.at_level(logging.ERROR, logger="service.candidate_manifest_handler"):
        timings = build_handler(source, publisher, make_uploader()).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INTERNAL
    assert kwargs["detail"] == "shop: model.shop.orders: the contracts library failed: KeyError('materialization')"
    assert kwargs["failed_nodes"] == []
    assert timings.failure_kind == ParseFailureKind.INTERNAL
    (record,) = [r for r in caplog.records if r.getMessage() == "candidate: the contracts library failed on a manifest"]
    assert record.exc_info is not None


# --- progress and timings -----------------------------------------------------

def test_progress_is_called_after_each_dbt_contract(tmp_path, monkeypatch):
    events: list[str] = []
    real_generate = dbt.generate
    monkeypatch.setattr(dbt, "generate", lambda *a, **kw: events.append("generate") or real_generate(*a, **kw))
    path = _manifest(
        tmp_path,
        _node("orders"),
        _node("bad", meta={"criticality": "high"}),
        _node("stub", tags=["local_stub"]),
        {**_node("broken"), "tags": "daily"},
        _node("countries", resource_type="seed"),
        _not_null_test("orders", "model.shop.orders"),
    )

    parse_manifest(path, progress=lambda: events.append("progress"))

    # A failed contract is followed by a call too; a skipped node and a test get none.
    assert events == ["generate", "progress", "generate", "progress", "progress", "generate", "progress"]


def test_each_contract_is_timed_and_its_check_counted_on_the_cache(tmp_path):
    timings, validator = _timings(), ContractValidator()
    path = _manifest(tmp_path, _node("orders"), _node("countries", resource_type="seed"))

    parse_manifest(path, validator=validator, timings=timings)
    parse_manifest(path, validator=validator, timings=timings)

    for stage in (Stage.CONTRACT_GENERATION, Stage.CONTRACT_VALIDATION):
        assert (timings.stages[stage].runs, timings.stages[stage].counts) == (4, {"nodes": 4})
    assert timings.validation_cache == {"hit": 2, "miss": 2}


# --- through the handler ---------------------------------------------------------

def _dbt_file(tmp_path, name, service, *nodes):
    path = Path(tmp_path) / name
    path.write_text(json.dumps({"nodes": {n["unique_id"]: n for n in nodes}}))
    return ManifestFile(path=str(path), image_tag="", declared_service=service)


def _service_2_node(name, **over):
    node = dbt_node(name) | {"unique_id": f"model.service-2.{name}", "fqn": ["service-2", name]}
    node.update(over)
    return node


def test_every_invalid_contract_and_sql_error_is_named_once(tmp_path):
    """Invalid dbt contracts of two services, an invalid Python contract and a dbt node
    whose SQL does not parse end in one rejection naming every failed node once. The
    kind is the first of the parse_failure_kind vocabulary's order, so the healable
    invalid_sql wins over invalid_contract."""
    publisher = MagicMock()
    bad_meta = {"materialized": "table", "meta": {"owner": "team-a", "criticality": "high"}}
    source = source_of(
        _dbt_file(tmp_path, "service-1.json", "service-1",
                  dbt_node("orders") | {"config": bad_meta},
                  dbt_node("broken") | {"compiled_code": "SELECT FROM WHERE((("},
                  dbt_node("fine")),
        bundle_file(tmp_path, [
            bundle_node("contracts/py_daily_kpis.odcs.yaml",
                        demo_text("py_daily_kpis").replace("NUMERIC(12,2)", "NUMERIC(12)")),
            demo_node("fx_eur_usd_rates"),
        ]),
        _dbt_file(tmp_path, "service-2.json", "service-2", _service_2_node("payments", tags="daily")),
    )

    python_handler(source, publisher).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_SQL
    assert kwargs["detail"] == (
        "4 nodes failed to parse: analytics.py_daily_kpis, test_schema.broken, test_schema.orders, test_schema.payments")
    assert [(n.node_id, n.kind, n.service, n.file_path) for n in kwargs["failed_nodes"]] == [
        ("analytics.py_daily_kpis", ParseFailureKind.INVALID_CONTRACT, "service-py", "contracts/py_daily_kpis.odcs.yaml"),
        ("test_schema.broken", ParseFailureKind.INVALID_SQL, "service-1", "models/broken.sql"),
        ("test_schema.orders", ParseFailureKind.INVALID_CONTRACT, "service-1", "models/orders.sql"),
        ("test_schema.payments", ParseFailureKind.INVALID_CONTRACT, "service-2", "models/payments.sql"),
    ]


@pytest.mark.parametrize("two_services", [False, True], ids=["one-service", "two-services"])
def test_two_dbt_nodes_with_one_contract_id_publish_with_neither_contract_in_the_bundle(tmp_path, caplog, two_services):
    """release-controller rejects such a release as duplicate_table, naming every
    service: topology-controller publishes it, and the bundle, which keys a contract by
    id, holds neither contract of that id."""
    publisher, writer, bundle_writer = MagicMock(), RecordingArtifactWriter(), RecordingContractBundleWriter()
    model = dbt_node("orders")
    if two_services:
        files = [_dbt_file(tmp_path, "service-1.json", "service-1", model, dbt_node("fine")),
                 _dbt_file(tmp_path, "service-2.json", "service-2", _service_2_node("orders"))]
    else:
        seed = model | {"unique_id": "seed.service-1.orders", "resource_type": "seed", "compiled_code": "",
                        "original_file_path": "seeds/orders.csv",
                        "config": {"materialized": "seed", "meta": {"owner": "team-a"}}}
        files = [_dbt_file(tmp_path, "service-1.json", "service-1", model, seed, dbt_node("fine"))]

    with caplog.at_level(logging.WARNING, logger="service.candidate_manifest_handler"):
        build_handler(source_of(*files), publisher, make_uploader(), artifact_writer=writer,
                      contract_bundle_writer=bundle_writer).handle(release_id="rel-1")

    publisher.publish_failed.assert_not_called()
    publisher.publish_ok.assert_called_once()
    assert sorted(n["unique_id"] for n in writer.nodes) == ["test_schema.fine", "test_schema.orders", "test_schema.orders"]
    assert sorted(bundle_writer.document["contracts"]) == ["test_schema.fine"]
    (warning,) = [r for r in caplog.records if r.getMessage().startswith("candidate: a unique id more than one node")]
    assert warning.unique_id == "test_schema.orders"
