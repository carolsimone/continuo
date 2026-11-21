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

from adapters.redis.candidate_publisher import CandidateManifestPublisher
from domain.contract_vocabulary import NodeType, ParseFailureKind
from domain.dormant import is_dormant
from domain.exceptions import InvalidNodeSettingError
from domain.model import CompileExtras, ContractFamily, FailedNode, ManifestFile, SeedExtras
from service.contract_validation import ContractValidator
from service.errors import ContractGenerationError
from service import parser
from tests.fakes import parse_manifest
from service.release_timings import (
    CONTRACT_FAILURE_REASONS,
    CONTRACT_FAILURE_STAGES,
    ReleaseTimings,
    Stage,
)
from streams_contract import MANIFEST_LOADED_CANDIDATE_V2
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
from tests.fingerprint_fixtures import enforced_dbt_node
from tests.odcs_fixtures import bundle_file, bundle_node, demo_node, demo_text

_MATERIALIZED = {"model": "table", "seed": "seed", "snapshot": "snapshot"}
_NODE_TYPE = {"model": NodeType.DBT_MODEL, "seed": NodeType.DBT_SEED, "snapshot": NodeType.DBT_SNAPSHOT}
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
    what the manifest says, but the schema: it keeps the spelling the manifest declares,
    which the contract's id holds in lower case."""
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
        "analytics", "from_contract_table", "from_contract_relation")
    assert (node.service_name, node.node_type, node.schedule_name, node.criticality) == (
        "from-contract-service", NodeType.DBT_SNAPSHOT, "from_contract", "REGULATORY")
    assert (node.timeout_seconds, node.original_file_path, node.owner, node.materialized) == (
        420, "from/contract.sql", "from-contract-team", "from_contract")
    # The contract stands for the node's file as its run facts name it, as the node does.
    assert node.contract.path == "from/contract.sql"


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


@pytest.mark.parametrize("criticality", ["", None])
def test_an_empty_criticality_reads_as_secondary(tmp_path, criticality):
    """The contract leaves out a field with no value, so an empty criticality is the
    default, SECONDARY, and the contract is valid."""
    parsed = parse_manifest(_manifest(tmp_path, _node("orders", meta={"criticality": criticality})))
    assert parsed.failed == ()
    (node,) = parsed.nodes
    assert node.criticality == "SECONDARY"


def test_a_schema_declared_with_capitals_keeps_its_spelling_while_the_ids_are_lower_case(tmp_path):
    """dbt builds the relation under the declared spelling, so the node's schema keeps it,
    while its unique_id and its contract's id are lower case."""
    (node,) = parse_manifest(_manifest(tmp_path, _node("orders", schema="Analytics"))).nodes
    assert (node.schema_name, node.unique_id, node.contract.document["id"]) == (
        "Analytics", "analytics.orders", "analytics.orders")
    # The relation the node builds is addressed with the declared schema; its id is lower case.
    assert (f"{node.schema_name}.{node.relation}", node.resolved_relation_id) == (
        "Analytics.orders", "analytics.orders")


_FINAL_SIGMA, _SIGMA = "ς", "σ"


@pytest.mark.parametrize("schema, table, contract_id, node_key", [
    # A schema that ends in a capital Σ before a table that starts with a cased letter.
    ("DATAΣ", "Orders", f"data{_SIGMA}.orders", f"data{_FINAL_SIGMA}.orders"),
    # A table that starts with a capital Σ and no cased letter after it.
    ("x", "Σ_1", f"x.{_FINAL_SIGMA}_1", f"x.{_SIGMA}_1"),
], ids=["schema-ends-in-sigma", "table-starts-with-sigma"])
def test_a_contract_whose_id_is_not_the_node_key_is_an_invalid_contract(tmp_path, schema, table, contract_id, node_key):
    """The library lowers `<schema>.<table>` as one string, continuo each part on its own. Where
    the two differ, the bundle would key the contract by a unique_id its document does not carry,
    and catalog refuses such a bundle whole, so the node is an invalid contract naming both ids."""
    timings = _timings()
    parsed = parse_manifest(
        _manifest(tmp_path, _node(table, schema=schema), _node("customers")), timings=timings)

    assert [node.unique_id for node in parsed.nodes] == ["analytics.customers"]
    assert parsed.failed == (FailedNode(
        node_id=contract_id,
        kind=ParseFailureKind.INVALID_CONTRACT,
        service="shop",
        file_path=f"models/{table}.sql",
        node_type=NodeType.DBT_MODEL,
        detail=f"id: the contract's id is {contract_id!r}, and continuo names its node {node_key!r}: the schema"
               f" {schema!r} and the table {table!r} lower-case differently apart than joined, so rename the"
               " schema or the table",
    ),)
    assert timings.contract_failures == {("validate", "identity"): 1}


def test_an_id_that_is_not_the_node_key_is_named_with_the_contracts_other_issues(tmp_path):
    timings = _timings()
    (failed,) = parse_manifest(
        _manifest(tmp_path, _node("Orders", schema="DATAΣ", meta={"criticality": "high"})), timings=timings).failed

    assert failed.detail.startswith("customProperties[3].value: `criticality` is 'high'")
    assert "; id: the contract's id is " in failed.detail
    assert timings.contract_failures == {("validate", "value"): 1, ("validate", "identity"): 1}


@pytest.mark.parametrize("schema, table", [
    ("analytics", "orders"), ("Analytics", "Orders"), ("ΣΧΗΜΑ", "orders"), ("DATAΣ", "_orders"),
], ids=["lower-case", "ascii-capitals", "sigma-that-lowers-alike", "final-sigma-before-no-cased-letter"])
def test_a_contract_whose_id_is_the_node_key_is_valid(tmp_path, schema, table):
    (node,) = parse_manifest(_manifest(tmp_path, _node(table, schema=schema))).nodes
    assert node.unique_id == node.contract.document["id"] == f"{schema}.{table}".lower()


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


@pytest.mark.parametrize("resource_type", ["model", "seed", "snapshot"])
def test_the_parsers_copies_of_the_librarys_kind_and_service_rules_agree_with_it(tmp_path, resource_type):
    """A failed node takes its type from the resource type and its service from the
    fqn without the library, which may have refused the node: for a well-formed node
    both are what the library gives."""
    node = _node("orders_daily", resource_type=resource_type, fqn=["shop_core", "orders_daily"])
    manifest = json.loads(Path(_manifest(tmp_path, node)).read_text())
    generated = dbt.generate(node, [], manifest=manifest)

    assert parser._RESOURCE_TYPE_TO_NODE_TYPE[resource_type] == dbt.run_facts(generated.document).kind
    assert parser._tolerant_service(node) == dbt.node_service(node) == "shop-core"


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


# --- a seed's declared columns and its CSV header ---------------------------------

def _seed(table, declares, **over):
    """A seed of the project `shop` that declares the columns `declares`, in that order."""
    return _node(table, resource_type="seed", columns={name: {"name": name} for name in declares}, **over)


def _extras(**headers):
    """The compile extras of `shop`, naming each seed `<table>=<header columns>`."""
    seeds = {f"seed.shop.{table}": SeedExtras(sha256="ab" * 32, header=tuple(header), column_types={})
             for table, header in headers.items()}
    return CompileExtras(service="shop", release_id="r", seeds=seeds, project_flags={}, packages={})


def test_a_header_column_the_seed_does_not_declare_is_an_invalid_contract(tmp_path):
    timings = _timings()
    parsed = parse_manifest(_manifest(tmp_path, _seed("countries", ["id"])),
                            extras=_extras(countries=["id", "name"]), timings=timings)

    assert parsed.nodes == []
    assert parsed.failed == (FailedNode(
        node_id="analytics.countries",
        kind=ParseFailureKind.INVALID_CONTRACT,
        service="shop",
        file_path="seeds/countries.csv",
        node_type=NodeType.DBT_SEED,
        detail="seed.shop.countries: name is in the CSV header but not declared",
    ),)
    assert timings.contract_failures == {("validate", "seed_header"): 1}
    # The pair lands on a series the metrics export from boot.
    assert all(stage in CONTRACT_FAILURE_STAGES and reason in CONTRACT_FAILURE_REASONS
               for stage, reason in timings.contract_failures)


def test_a_declared_column_the_header_lacks_is_an_invalid_contract(tmp_path):
    parsed = parse_manifest(_manifest(tmp_path, _seed("countries", ["id", "legacy"])),
                            extras=_extras(countries=["id"]))

    assert parsed.nodes == []
    (failed,) = parsed.failed
    assert failed.kind == ParseFailureKind.INVALID_CONTRACT
    assert failed.detail == "seed.shop.countries: legacy is declared but not in the CSV header"


def test_every_column_the_two_lists_disagree_on_is_named_in_the_librarys_order(tmp_path):
    """The header's undeclared columns come first, in the header's order, then the declared columns
    the header lacks, in the declared order. Names are compared exactly, and each failure is counted."""
    timings = _timings()
    parsed = parse_manifest(
        _manifest(tmp_path, _seed("countries", ["c", "x", "a", "ID", "w"])),
        extras=_extras(countries=["a", "id", "z", "b", "c"]), timings=timings)

    (failed,) = parsed.failed
    assert failed.detail == (
        "seed.shop.countries: id is in the CSV header but not declared; "
        "z is in the CSV header but not declared; "
        "b is in the CSV header but not declared; "
        "x is declared but not in the CSV header; "
        "ID is declared but not in the CSV header; "
        "w is declared but not in the CSV header")
    assert timings.contract_failures == {("validate", "seed_header"): 6}


def test_a_seed_whose_header_and_declared_columns_agree_in_any_order_is_valid(tmp_path):
    parsed = parse_manifest(_manifest(tmp_path, _seed("countries", ["name", "id"])),
                            extras=_extras(countries=["id", "name"]))

    assert parsed.failed == ()
    (node,) = parsed.nodes
    assert (node.unique_id, node.seed_header) == ("analytics.countries", ("id", "name"))


@pytest.mark.parametrize("extras", [None, _extras(other=["id"])], ids=["no-extras", "extras-name-another-seed"])
def test_a_seed_the_extras_do_not_name_is_not_checked(tmp_path, extras):
    """With no header known there is nothing to compare: the seed is kept, and the parse reports it
    as unchecked."""
    timings = _timings()
    parsed = parse_manifest(_manifest(tmp_path, _seed("countries", ["id"]), _seed("other", ["id"])),
                            extras=extras, timings=timings)

    assert parsed.failed == ()
    assert [node.unique_id for node in parsed.nodes] == ["analytics.countries", "analytics.other"]
    assert parsed.unchecked_seeds == (("analytics.countries", "analytics.other") if extras is None
                                      else ("analytics.countries",))
    assert timings.contract_failures == {}


def test_a_seed_the_extras_name_is_not_reported_as_unchecked(tmp_path):
    parsed = parse_manifest(_manifest(tmp_path, _seed("countries", ["id"]), _node("orders")),
                            extras=_extras(countries=["id"]))
    assert parsed.unchecked_seeds == ()


def test_every_mismatched_seed_is_named_once_among_the_invalid_contracts(tmp_path):
    parsed = parse_manifest(
        _manifest(
            tmp_path,
            _seed("countries", ["id"]),
            _node("orders", meta={"criticality": "high"}),
            _seed("currencies", ["code", "name"]),
            _seed("regions", ["id"]),
            _node("customers"),
        ),
        extras=_extras(countries=["id", "iso"], currencies=["code"], regions=["id"]),
    )

    assert [node.unique_id for node in parsed.nodes] == ["analytics.regions", "analytics.customers"]
    assert [(failed.node_id, failed.node_type) for failed in parsed.failed] == [
        ("analytics.countries", NodeType.DBT_SEED),
        ("analytics.orders", NodeType.DBT_MODEL),
        ("analytics.currencies", NodeType.DBT_SEED),
    ]


def test_a_seed_with_an_invalid_contract_and_a_differing_header_is_one_failed_node(tmp_path):
    """The lite check reports a seed's contract issues and then its header lines, so the release names
    both of them at once, in one failed node: the contract's issues first, then the header's."""
    timings = _timings()
    parsed = parse_manifest(
        _manifest(tmp_path, _seed("countries", ["id"], meta={"criticality": "high"})),
        extras=_extras(countries=["id", "name"]), timings=timings)

    (failed,) = parsed.failed
    assert failed.detail == (
        "seed.shop.countries: customProperties[3].value: `criticality` is 'high', it must be one of "
        "REGULATORY, CORE, SECONDARY; name is in the CSV header but not declared")
    assert timings.contract_failures == {("validate", "value"): 1, ("validate", "seed_header"): 1}


def test_the_tests_of_a_rejected_seed_are_not_attached_to_a_node(tmp_path):
    parsed = parse_manifest(
        _manifest(tmp_path, _seed("countries", ["id"]), _not_null_test("countries", "seed.shop.countries")),
        extras=_extras(countries=["id", "name"]))

    assert [node.unique_id for node in parsed.nodes] == []
    assert [failed.node_id for failed in parsed.failed] == ["analytics.countries"]


def test_progress_is_called_after_a_rejected_seed(tmp_path):
    events: list[str] = []

    parse_manifest(_manifest(tmp_path, _seed("countries", ["id"]), _seed("regions", ["id"])),
                   extras=_extras(countries=["id", "name"], regions=["id"]),
                   progress=lambda: events.append("progress"))

    assert events == ["progress", "progress"]


def test_the_header_check_is_the_librarys(tmp_path, monkeypatch):
    """The words of a mismatch are `dbt.seed_header_issues`', so a producer's CI and the release say
    the same thing."""
    calls = []

    def library(declared, header):
        calls.append((list(declared), list(header)))
        return ["the library says so"]

    monkeypatch.setattr(dbt, "seed_header_issues", library)
    (failed,) = parse_manifest(_manifest(tmp_path, _seed("countries", ["id", "name"])),
                               extras=_extras(countries=["name", "id"])).failed

    assert calls == [(["id", "name"], ["name", "id"])]
    assert failed.detail == "seed.shop.countries: the library says so"


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


@pytest.mark.parametrize("fact", [{"kind": "dbt-widget"}, {"timeout_minutes": 30.0}], ids=["kind", "timeout"])
def test_a_run_fact_the_topology_cannot_take_raises_a_contract_generation_error(tmp_path, monkeypatch, fact):
    """A valid contract's run facts name a kind of the node type vocabulary and a whole
    timeout: any other value is a defect of the library, never the team's artifact."""
    real = dbt.run_facts
    monkeypatch.setattr(dbt, "run_facts", lambda document: dataclasses.replace(real(document), **fact))
    with pytest.raises(ContractGenerationError, match="model.shop.orders: the contracts library failed: ValueError") as raised:
        parse_manifest(_manifest(tmp_path, _node("orders")))
    assert isinstance(raised.value.__cause__, ValueError)


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


# --- a relationships test whose target the manifest does not hold ---------------------

_STUB_ID = "model.shop.stub_customers"


def _relationships_test(attached, target, *, generic="relationships", namespace=None):
    """A strict relationships test on the column `id` of `attached`, pointing at `target`, in the
    shape dbt 1.12 writes it. `generic` and `namespace` name another generic test of that shape."""
    return {
        "unique_id": f"test.shop.{generic}_orders_id__id__{target.rsplit('.', 1)[-1]}",
        "resource_type": "test",
        "name": f"{generic}_orders_id__id",
        "schema": "analytics_dbt_test__audit",
        "fqn": ["shop", f"{generic}_orders_id__id"],
        "original_file_path": "models/schema.yml",
        "attached_node": attached,
        "column_name": "id",
        "refs": [{"name": "stub_customers", "package": None, "version": None},
                 {"name": "orders", "package": None, "version": None}],
        "test_metadata": {"name": generic, "namespace": namespace, "kwargs": {
            "column_name": "id", "field": "id", "to": "ref('stub_customers')",
            "model": "{{ get_where_subquery(ref('orders')) }}"}},
        "depends_on": {"macros": [f"macro.dbt.test_{generic}", "macro.dbt.get_where_subquery"],
                       "nodes": [target, attached]},
        "config": {"severity": "ERROR"},
        "compiled_code": "select 1",
    }


def _orders():
    return _node("orders", columns={"id": {"name": "id", "data_type": "integer"}})


def _without_local_stubs(*nodes):
    """The nodes the baseline uploader keeps (`dbt_upload.upload.filter_manifest`): every node but a
    `local_stub` one, while a test that points at one keeps naming it."""
    return [node for node in nodes if "local_stub" not in node.get("tags", [])]


def _id_property(node):
    (column,) = [c for c in node.contract.document["schema"][0]["properties"] if c["name"] == "id"]
    return column


def test_a_relationships_test_on_a_local_stub_the_manifest_holds_stays_a_relationship(tmp_path):
    stub = _node("stub_customers", tags=("local_stub",))
    parsed = parse_manifest(_manifest(tmp_path, _orders(), stub, _relationships_test("model.shop.orders", _STUB_ID)))

    assert parsed.failed == ()
    (orders,) = [node for node in parsed.nodes if node.unique_id == "analytics.orders"]
    assert _id_property(orders)["relationships"] == [{"to": "analytics.stub_customers.id"}]
    assert parsed.relationship_tests_without_target == 0


def test_a_relationships_test_whose_target_the_uploader_left_out_is_a_custom_rule(tmp_path):
    """The library's own rule for a target the manifest does not hold: the reader is valid, and its
    contract carries the test as a custom rule that keeps `to` and `field`."""
    stub = _node("stub_customers", tags=("local_stub",))
    test = _relationships_test("model.shop.orders", _STUB_ID)
    path = _manifest(tmp_path, *_without_local_stubs(_orders(), stub, test))

    parsed = parse_manifest(path)

    assert parsed.failed == ()
    (orders,) = [node for node in parsed.nodes if node.unique_id == "analytics.orders"]
    column = _id_property(orders)
    assert "relationships" not in column
    (rule,) = column["quality"]
    assert (rule["type"], rule["implementation"]) == (
        "custom", {"test": "relationships", "arguments": {"field": "id", "to": "ref('stub_customers')"}})
    no_target = {**test, "depends_on": {**test["depends_on"], "nodes": ["model.shop.orders"]}}
    manifest = json.loads(Path(path).read_text())
    assert orders.contract.document == dbt.generate(
        manifest["nodes"]["model.shop.orders"], [no_target], manifest=manifest).document
    assert parsed.relationship_tests_without_target == 1


@pytest.mark.parametrize("sources", [{}, {"source.shop.raw.other": {"schema": "raw", "name": "other"}}],
                         ids=["no-sources", "other-sources"])
def test_a_relationships_target_that_is_neither_a_node_nor_a_source_is_left_out(tmp_path, sources):
    test = _relationships_test("model.shop.orders", "source.shop.raw.events")
    path = _manifest(tmp_path, _orders(), test)
    manifest = json.loads(Path(path).read_text())
    Path(path).write_text(json.dumps({**manifest, "sources": sources}))

    parsed = parse_manifest(path)

    assert parsed.failed == ()
    (orders,) = [node for node in parsed.nodes if node.unique_id == "analytics.orders"]
    assert "relationships" not in _id_property(orders)
    assert parsed.relationship_tests_without_target == 1


def test_a_relationships_target_that_is_a_source_stays_a_relationship(tmp_path):
    test = _relationships_test("model.shop.orders", "source.shop.raw.events")
    path = _manifest(tmp_path, _orders(), test)
    manifest = json.loads(Path(path).read_text())
    Path(path).write_text(json.dumps(
        {**manifest, "sources": {"source.shop.raw.events": {"schema": "raw", "name": "events"}}}))

    parsed = parse_manifest(path)

    (orders,) = [node for node in parsed.nodes if node.unique_id == "analytics.orders"]
    assert _id_property(orders)["relationships"] == [{"to": "raw.events.id"}]
    assert parsed.relationship_tests_without_target == 0


def test_only_a_relationships_test_loses_a_target_the_manifest_does_not_hold(tmp_path, monkeypatch):
    """`generate` resolves the target of no other test, so another generic test that names the
    same missing node reaches it unchanged."""
    other = _relationships_test("model.shop.orders", _STUB_ID, generic="equal_rowcount", namespace="dbt_utils")
    given = []
    real = dbt.generate
    monkeypatch.setattr(dbt, "generate", lambda node, tests, *, manifest: given.extend(tests) or real(
        node, tests, manifest=manifest))

    parsed = parse_manifest(_manifest(tmp_path, _orders(), other))

    assert parsed.failed == ()
    assert given[0]["depends_on"]["nodes"] == [_STUB_ID, "model.shop.orders"]
    assert parsed.relationship_tests_without_target == 0


def test_the_manifest_keeps_the_target_it_names(tmp_path, monkeypatch):
    """The test is given to `generate` as a copy: the manifest, which the hashes and the parse's own
    test pass read, still names the missing target."""
    test = _relationships_test("model.shop.orders", _STUB_ID)
    path = _manifest(tmp_path, _orders(), _node("customers"), test)
    original = json.loads(Path(path).read_text())
    seen = []
    real = dbt.generate

    def recording(node, tests, *, manifest):
        seen.append((manifest, list(tests)))
        return real(node, tests, manifest=manifest)

    monkeypatch.setattr(dbt, "generate", recording)

    parsed = parse_manifest(path)

    assert parsed.failed == ()
    (manifest, _), *_ = seen
    assert manifest == original
    given = [t for _, tests in seen for t in tests]
    assert all(_STUB_ID not in t["depends_on"]["nodes"] for t in given)
    assert manifest["nodes"][test["unique_id"]]["depends_on"]["nodes"][0] == _STUB_ID
    assert parsed.relationship_tests_without_target == 1


def test_a_test_given_twice_is_one_copy_and_counts_once():
    test = _relationships_test("model.shop.orders", _STUB_ID)
    held = parser._HeldTargets({"nodes": {"model.shop.orders": _orders()}})

    (first,) = held.of([test])
    (second,) = held.of([test])

    assert first is second and first is not test
    assert first["depends_on"]["nodes"] == ["model.shop.orders"]
    assert held.count == 1


def _in_the_projects_own_macro(test):
    """The test as dbt writes it when the project defines a macro `test_relationships` of its own: the
    first macro dbt resolved is the project's, not dbt's."""
    return {**test, "depends_on": {**test["depends_on"],
                                   "macros": ["macro.shop.test_relationships", "macro.dbt.get_where_subquery"]}}


def _with_no_attached_node(test):
    """The test as an older dbt or a hand-written manifest has it: it names no tested node."""
    return {**test, "attached_node": None}


def _in_another_namespace(test):
    return {**test, "test_metadata": {**test["test_metadata"], "namespace": "shop_utils"}}


def _with_no_resolved_macro(test):
    return {**test, "depends_on": {**test["depends_on"], "macros": []}}


_ALREADY_CUSTOM = [_in_the_projects_own_macro, _with_no_attached_node, _in_another_namespace,
                   _with_no_resolved_macro]
_ALREADY_CUSTOM_IDS = ["projects-own-macro", "no-attached-node", "another-namespace", "no-resolved-macro"]


@pytest.mark.parametrize("variant", _ALREADY_CUSTOM, ids=_ALREADY_CUSTOM_IDS)
def test_a_test_the_library_already_writes_as_custom_is_given_as_the_manifest_holds_it(variant):
    test = variant(_relationships_test("model.shop.orders", _STUB_ID))
    held = parser._HeldTargets({"nodes": {"model.shop.orders": _orders()}})

    (given,) = held.of([test])

    assert given is test
    assert held.count == 0


@pytest.mark.parametrize("variant", [_in_the_projects_own_macro, _with_no_attached_node],
                         ids=["projects-own-macro", "no-attached-node"])
def test_a_custom_relationships_test_keeps_the_hash_of_the_manifest_and_is_not_counted(tmp_path, variant):
    """The two cases the library writes as `custom` whatever the target is: the node's `source_sha256`
    covers the test's `depends_on.nodes` as the manifest holds it (the stub included), and the test is
    not counted as one given without a target."""
    test = variant(_relationships_test("model.shop.orders", _STUB_ID))
    path = _manifest(tmp_path, _orders(), test)
    manifest = json.loads(Path(path).read_text())
    node = manifest["nodes"]["model.shop.orders"]

    parsed = parse_manifest(path)

    assert parsed.failed == ()
    (orders,) = [n for n in parsed.nodes if n.unique_id == "analytics.orders"]
    held = dbt.generate(node, [manifest["nodes"][test["unique_id"]]], manifest=manifest)
    assert orders.contract.source_sha256 == held.source_sha256
    without = {**test, "depends_on": {**test["depends_on"], "nodes": ["model.shop.orders"]}}
    assert held.source_sha256 != dbt.generate(node, [without], manifest=manifest).source_sha256
    assert parsed.relationship_tests_without_target == 0


def test_a_relationships_test_in_the_dbt_namespace_is_given_without_its_target_whatever_its_macros(tmp_path):
    """In the namespace `dbt` the library reads the test as dbt's own and resolves its target, so a
    target the manifest does not hold is left out of it, and the test is counted."""
    test = _in_the_projects_own_macro(_relationships_test("model.shop.orders", _STUB_ID, namespace="dbt"))

    parsed = parse_manifest(_manifest(tmp_path, _orders(), test))

    assert parsed.failed == ()
    (orders,) = [n for n in parsed.nodes if n.unique_id == "analytics.orders"]
    assert "relationships" not in _id_property(orders)
    assert parsed.relationship_tests_without_target == 1


# --- an enforced contract on a snapshot ------------------------------------------

def test_an_enforced_contract_on_a_snapshot_is_an_invalid_contract(tmp_path):
    """dbt enforces the contract of a snapshot on its first run and fails every later run, so it does
    not support one. The validation refuses the flag, as the lite check does, so the team hears it
    when it releases and not when the snapshot's second run fails."""
    timings = _timings()

    parsed = parse_manifest(_manifest(tmp_path, enforced_dbt_node("snapshot")), timings=timings)

    assert parsed.nodes == []
    (failed,) = parsed.failed
    assert (failed.node_id, failed.kind, failed.node_type) == (
        "analytics.enforced_snapshot", ParseFailureKind.INVALID_CONTRACT, NodeType.DBT_SNAPSHOT)
    assert "dbt does not support contracts on snapshots" in failed.detail
    assert timings.contract_failures == {("validate", "kind_rule"): 1}


@pytest.mark.parametrize("resource_type, enforced", [("snapshot", False), ("model", True), ("seed", True)])
def test_a_snapshot_without_the_flag_and_an_enforced_model_or_seed_are_not_refused(
        tmp_path, resource_type, enforced):
    parsed = parse_manifest(_manifest(tmp_path, enforced_dbt_node(resource_type, enforced=enforced)))

    assert parsed.failed == ()
    assert [node.node_type for node in parsed.nodes] == [_NODE_TYPE[resource_type]]


# --- through the handler ---------------------------------------------------------

def _dbt_file(tmp_path, name, service, *nodes, headers=None):
    """The manifest file `name` of `service`. `headers` maps a seed's unique id to its CSV header,
    and writes the compile extras that name those seeds beside the manifest."""
    path = Path(tmp_path) / name
    path.write_text(json.dumps({"nodes": {n["unique_id"]: n for n in nodes}}))
    extras_path = ""
    if headers is not None:
        extras = Path(tmp_path) / f"{name}.extras.json"
        extras.write_text(json.dumps({
            "version": 1, "service": service, "release_id": "rel-1", "project_flags": {}, "packages": {},
            "seeds": {seed_id: {"sha256": "ab" * 32, "header": list(header), "column_types": {}}
                      for seed_id, header in headers.items()},
        }))
        extras_path = str(extras)
    return ManifestFile(path=str(path), image_tag="", declared_service=service, extras_path=extras_path)


def _handler_seed(service, name, declares):
    """A seed of `service` as the handler tests release it, declaring the columns `declares`."""
    return dbt_node(name) | {
        "unique_id": f"seed.{service}.{name}", "resource_type": "seed", "compiled_code": "",
        "fqn": [service, name], "original_file_path": f"seeds/{name}.csv",
        "config": {"materialized": "seed", "meta": {"owner": "team-a"}},
        "columns": {column: {"name": column} for column in declares},
    }


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


def test_a_schema_with_capitals_keeps_its_spelling_in_the_topology_under_a_lower_case_id(tmp_path):
    """A model declared in schema `Analytics` is the node `analytics.orders` whose schema is
    `Analytics`, the spelling dbt builds its relation under, so validation copies it from there.
    The compiled SQL of its readers, which dbt writes with the capitals, still names it: the
    reader's edge resolves to it, its candidate SQL is redirected to the candidate schema like the
    read of any known node, and the model's own read of its relation is left on production, as
    dbt wrote it. Its contract's id and the reader's declared reads are lower case."""
    publisher, uploader = MagicMock(), make_uploader()
    writer, bundle_writer = RecordingArtifactWriter(), RecordingContractBundleWriter()
    orders = dbt_node("orders") | {
        "schema": "Analytics", "compiled_code": 'SELECT MAX(id) AS id FROM "warehouse"."Analytics"."orders"'}
    daily_orders = dbt_node("daily_orders") | {
        "schema": "reporting", "compiled_code": 'SELECT id FROM "warehouse"."Analytics"."orders"'}
    source = source_of(_dbt_file(tmp_path, "service-1.json", "service-1", orders, daily_orders))

    build_handler(source, publisher, uploader, artifact_writer=writer,
                  contract_bundle_writer=bundle_writer).handle(release_id="rel-1")

    publisher.publish_failed.assert_not_called()
    publisher.publish_ok.assert_called_once()
    nodes = {node["unique_id"]: node for node in writer.nodes}
    assert sorted(nodes) == ["analytics.orders", "reporting.daily_orders"]
    assert (nodes["analytics.orders"]["schema_name"], nodes["analytics.orders"]["resolved_relation_id"]) == (
        "Analytics", "analytics.orders")
    assert nodes["analytics.orders"]["upstream_unique_ids"] == []
    assert nodes["reporting.daily_orders"]["upstream_unique_ids"] == ["analytics.orders"]
    candidate_sql = {call.kwargs["unique_id"]: call.kwargs["sql"] for call in uploader.upload.call_args_list}
    assert candidate_sql == {
        "analytics.orders": 'SELECT MAX(id) AS id FROM "warehouse"."Analytics"."orders"',
        "reporting.daily_orders": 'SELECT id FROM "warehouse"."_candidate_rel_1"."orders"',
    }
    contracts = bundle_writer.document["contracts"]
    assert sorted(contracts) == ["analytics.orders", "reporting.daily_orders"]
    assert contracts["analytics.orders"]["document"]["id"] == "analytics.orders"
    (reads,) = [prop["value"] for prop in contracts["reporting.daily_orders"]["document"]["schema"][0]["customProperties"]
                if prop["property"] == "reads"]
    assert reads == ["analytics.orders"]


_RELATIONSHIPS_LINE = "candidate: relationships tests whose target the manifest does not hold are custom rules"


def test_a_release_whose_relationships_test_names_a_left_out_stub_publishes_and_logs_it_once(tmp_path, caplog):
    """A baseline manifest the uploader filtered still names the stub its reader's relationships test
    points at: the release publishes, and one line per release counts such tests by service."""
    publisher, bundle_writer = MagicMock(), RecordingContractBundleWriter()
    orders = dbt_node("orders") | {"columns": {"id": {"name": "id", "data_type": "integer"}}}
    test = _relationships_test("model.service-1.orders", "model.service-1.stub_customers") | {
        "fqn": ["service-1", "relationships_orders_id__id"]}
    source = source_of(_dbt_file(tmp_path, "service-1.json", "service-1", orders, test))

    with caplog.at_level(logging.WARNING, logger="service.candidate_manifest_handler"):
        build_handler(source, publisher, make_uploader(), contract_bundle_writer=bundle_writer).handle(
            release_id="rel-1")

    publisher.publish_failed.assert_not_called()
    publisher.publish_ok.assert_called_once()
    (column,) = bundle_writer.document["contracts"]["test_schema.orders"]["document"]["schema"][0]["properties"]
    assert "relationships" not in column and column["quality"][0]["type"] == "custom"
    (record,) = [r for r in caplog.records if r.getMessage() == _RELATIONSHIPS_LINE]
    assert (record.levelno, record.release_id, record.tests, record.services) == (
        logging.WARNING, "rel-1", 1, {"service-1": 1})


def test_a_release_whose_relationships_targets_are_all_held_logs_no_such_line(tmp_path, caplog):
    source = source_of(_dbt_file(tmp_path, "service-1.json", "service-1", dbt_node("fine")))

    with caplog.at_level(logging.WARNING, logger="service.candidate_manifest_handler"):
        build_handler(source, MagicMock(), make_uploader()).handle(release_id="rel-1")

    assert [r for r in caplog.records if r.getMessage() == _RELATIONSHIPS_LINE] == []


def test_a_release_with_a_contract_whose_id_is_not_the_node_key_is_rejected_and_stores_nothing(tmp_path):
    """No contract bundle reaches catalog keyed by a unique_id its document does not carry: the
    release is rejected as invalid_contract before the code bundle, the contract bundle and the
    topology artifact are written."""
    publisher, writer, bundle_writer = MagicMock(), RecordingArtifactWriter(), RecordingContractBundleWriter()
    code_bundle_uploader = MagicMock()
    source = source_of(_dbt_file(
        tmp_path, "service-1.json", "service-1", dbt_node("Orders") | {"schema": "DATAΣ"}, dbt_node("fine")))

    timings = build_handler(source, publisher, make_uploader(), bundle_uploader=code_bundle_uploader,
                            artifact_writer=writer, contract_bundle_writer=bundle_writer).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_CONTRACT
    assert kwargs["detail"] == f"1 node failed to parse: data{_SIGMA}.orders"
    (failed,) = kwargs["failed_nodes"]
    assert (failed.node_id, failed.kind, failed.service, failed.file_path) == (
        f"data{_SIGMA}.orders", ParseFailureKind.INVALID_CONTRACT, "service-1", "models/Orders.sql")
    assert f"continuo names its node 'data{_FINAL_SIGMA}.orders'" in failed.detail
    assert (code_bundle_uploader.upload.call_args_list, bundle_writer.writes, writer.writes) == ([], [], [])
    assert timings.contract_failures == {("validate", "identity"): 1}


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


# --- a seed's declared columns and its CSV header, through the handler -------------

_UNCHECKED_SEEDS_LINE = "candidate: seeds with no known CSV header are not checked against their declared columns"


def test_a_release_with_a_seed_whose_header_differs_from_its_declared_columns_is_rejected(tmp_path):
    publisher = MagicMock()
    source = source_of(_dbt_file(
        tmp_path, "service-1.json", "service-1",
        _handler_seed("service-1", "seed_countries", ["code"]),
        _handler_seed("service-1", "seed_regions", ["id"]),
        dbt_node("orders"),
        headers={"seed.service-1.seed_countries": ["code", "name"], "seed.service-1.seed_regions": ["id"]}))

    timings = build_handler(source, publisher, make_uploader()).handle(release_id="rel-1")

    kwargs = _failed_kwargs(publisher)
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_CONTRACT
    assert kwargs["detail"] == "1 node failed to parse: test_schema.seed_countries"
    (failed,) = kwargs["failed_nodes"]
    assert (failed.node_id, failed.kind, failed.service, failed.file_path, failed.node_type) == (
        "test_schema.seed_countries", ParseFailureKind.INVALID_CONTRACT, "service-1",
        "seeds/seed_countries.csv", NodeType.DBT_SEED)
    assert failed.detail == "seed.service-1.seed_countries: name is in the CSV header but not declared"
    assert timings.failure_kind == ParseFailureKind.INVALID_CONTRACT
    assert timings.contract_failures == {("validate", "seed_header"): 1}


def test_a_release_whose_seeds_have_no_known_header_publishes_and_logs_their_count_once(tmp_path, caplog):
    """No compile extras, or extras that do not name a seed, leave no header to compare: the release
    publishes, and one line per release counts the seeds that were not checked, across its services."""
    publisher = MagicMock()
    source = source_of(
        _dbt_file(tmp_path, "service-1.json", "service-1",
                  _handler_seed("service-1", "seed_a", ["x"]), _handler_seed("service-1", "seed_b", ["x"]),
                  _handler_seed("service-1", "seed_named", ["x"]), dbt_node("fine"),
                  headers={"seed.service-1.seed_named": ["x"]}),
        _dbt_file(tmp_path, "service-2.json", "service-2",
                  _handler_seed("service-2", "seed_c", ["x"])),
    )

    with caplog.at_level(logging.WARNING, logger="service.candidate_manifest_handler"):
        build_handler(source, publisher, make_uploader()).handle(release_id="rel-1")

    publisher.publish_failed.assert_not_called()
    publisher.publish_ok.assert_called_once()
    (record,) = [r for r in caplog.records if r.getMessage() == _UNCHECKED_SEEDS_LINE]
    assert (record.levelno, record.release_id, record.seeds, record.services) == (
        logging.WARNING, "rel-1", 3, {"service-1": 2, "service-2": 1})


def test_a_release_whose_seeds_all_have_a_known_header_logs_no_unchecked_seeds(tmp_path, caplog):
    source = source_of(_dbt_file(
        tmp_path, "service-1.json", "service-1", _handler_seed("service-1", "seed_a", ["x"]), dbt_node("fine"),
        headers={"seed.service-1.seed_a": ["x"]}))

    with caplog.at_level(logging.WARNING, logger="service.candidate_manifest_handler"):
        build_handler(source, MagicMock(), make_uploader()).handle(release_id="rel-1")

    assert [r for r in caplog.records if r.getMessage() == _UNCHECKED_SEEDS_LINE] == []


# --- text UTF-8 cannot encode never reaches a published rejection --------------------

def _publishing(redis):
    """The real publisher over a recording redis client."""
    return CandidateManifestPublisher(
        redis, MANIFEST_LOADED_CANDIDATE_V2, producer="topology-controller", continuo_contracts="0.3.1")


def _published_rejection(redis) -> dict:
    """The one entry the release published, as redis-py sends it: every field encoded as UTF-8, which
    raises for text that holds a lone surrogate. Returns its payload."""
    redis.xadd.assert_called_once()
    _, fields = redis.xadd.call_args.args
    for value in fields.values():
        value.encode("utf-8")
    return json.loads(fields["payload"])


@pytest.mark.parametrize("declares, header, column_types", [
    ([], ["code\ud800"], {}),
    (["code"], ["code", "x\ud800"], {}),
    (["code"], ["code"], {"code": "varchar\ud800"}),
], ids=["header-column", "header-column-beside-a-declared-one", "column-type"])
def test_a_seed_whose_extras_hold_text_utf8_cannot_encode_is_rejected_as_an_invalid_artifact(
        tmp_path, declares, header, column_types):
    """The header's text is refused before it is compared with the declared columns, so no rejection
    repeats it: the release publishes `failed` and the event can be encoded."""
    redis = MagicMock()
    seed = _handler_seed("service-1", "seed_countries", declares)
    source = source_of(_dbt_file(tmp_path, "service-1.json", "service-1", seed, dbt_node("fine"),
                                 headers={"seed.service-1.seed_countries": header}))
    extras = Path(tmp_path) / "service-1.json.extras.json"
    document = json.loads(extras.read_text())
    document["seeds"]["seed.service-1.seed_countries"]["column_types"] = column_types
    extras.write_text(json.dumps(document))

    build_handler(source, _publishing(redis), make_uploader()).handle(release_id="rel-1")

    payload = _published_rejection(redis)
    assert (payload["status"], payload["failure_kind"]) == ("failed", "invalid_artifact")
    assert "test_schema.seed_countries" in payload["detail"]
    assert "lone surrogate" in payload["detail"]
    assert payload["failed_nodes"] == []


@pytest.mark.parametrize("field, over", [
    ("name", {"name": "seed_countries\ud800"}),
    ("schema", {"schema": "test\ud800"}),
    ("fqn", {"fqn": ["service-1\ud800", "seed_countries"]}),
    ("original_file_path", {"original_file_path": "seeds/seed_countries\ud800.csv"}),
])
def test_a_node_whose_manifest_text_utf8_cannot_encode_is_rejected_as_an_invalid_artifact(tmp_path, field, over):
    """A failed node names its node id, service and file as the manifest writes them, and a rejection
    that held text UTF-8 cannot encode could not be published."""
    redis = MagicMock()
    seed = _handler_seed("service-1", "seed_countries", ["code"]) | over
    source = source_of(_dbt_file(tmp_path, "service-1.json", "service-1", seed, dbt_node("fine"),
                                 headers={"seed.service-1.seed_countries": ["code", "extra"]}))

    build_handler(source, _publishing(redis), make_uploader()).handle(release_id="rel-1")

    payload = _published_rejection(redis)
    assert (payload["status"], payload["failure_kind"]) == ("failed", "invalid_artifact")
    assert "UTF-8 cannot encode" in payload["detail"]
    assert payload["failed_nodes"] == []


def test_the_unchecked_seeds_of_two_manifests_of_one_service_are_added(tmp_path, caplog):
    source = source_of(
        _dbt_file(tmp_path, "a.json", "service-1", _handler_seed("service-1", "seed_a", ["x"]), dbt_node("fine")),
        _dbt_file(tmp_path, "b.json", "service-1", _handler_seed("service-1", "seed_b", ["x"]), dbt_node("also_fine")),
    )

    with caplog.at_level(logging.WARNING, logger="service.candidate_manifest_handler"):
        build_handler(source, MagicMock(), make_uploader()).handle(release_id="rel-1")

    (record,) = [r for r in caplog.records if r.getMessage() == _UNCHECKED_SEEDS_LINE]
    assert (record.seeds, record.services) == (2, {"service-1": 2})
