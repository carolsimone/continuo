"""The stored form of each python contract and the release's contract bundle."""
import json
from pathlib import Path

import continuo_contracts
import pytest
from continuo_contracts import yaml_strict
from continuo_contracts.canonical import canonical_json
from continuo_contracts.errors import CanonicalJsonError, HashInputError
from continuo_contracts.hashing import sha256_hex
from continuo_contracts.schema import SCHEMA_SHA256

import service.contract_bundle as contract_bundle_module
from config.config import _SYSTEM_RELATIONS
from domain.model import AuthoredContract, ManifestNode, NodeType, Runtime, TableRef
from service.contract_bundle import StoredContract, build_contract_bundle, contract_bundle_key, store_contracts
from tests.odcs_fixtures import demo_text, release_timings

FIXTURES = Path(__file__).parent / "fixtures"
POSTGRES_SYSTEM = _SYSTEM_RELATIONS["postgres"]
_VECTORS = json.loads((Path(continuo_contracts.__file__).parent / "vectors" / "contracts.json").read_text())


def _vector(name: str) -> dict:
    return next(v for v in _VECTORS if v["name"] == name)


def _node(name: str, text: str, *, path: str | None = None, service: str = "service-py") -> ManifestNode:
    """The node the python-odcs parse builds from `text`, with the fields the stored form reads."""
    document = yaml_strict.load(text)
    schema, table = document["id"].split(".")
    return ManifestNode(
        table_name=table,
        schema_name=schema,
        service_name=service,
        owner="team-py",
        schedule_name="daily",
        criticality="SECONDARY",
        node_type=NodeType.PYTHON_NODE,
        runtime=Runtime.PYTHON,
        content_hash="sha256:" + sha256_hex(name.encode("utf-8")),
        contract=AuthoredContract(
            document=document,
            path=path or f"contracts/{name}.odcs.yaml",
            source_sha256=sha256_hex(text.encode("utf-8")),
            service=service,
        ),
    )


def _kpis_node() -> ManifestNode:
    return _node("py_daily_kpis", demo_text("py_daily_kpis"))


def _reads(document: dict) -> list:
    [table] = document["schema"]
    return [p["value"] for p in table.get("customProperties", []) if p["property"] == "reads"]


def _inputs(given: dict) -> dict:
    """The keyword arguments of build_contract_bundle for the fixture's `input`."""
    return {
        "tenant_id": given["tenant_id"],
        "release_id": given["release_id"],
        "library_version": given["library_version"],
        "contracts": [StoredContract(**c) for c in given["contracts"]],
        "provenance": given["provenance"],
        "timings": given["timings"],
    }


# --- the shared fixture ---------------------------------------------------------

def test_the_bundle_matches_the_shared_fixture():
    fx = json.loads((FIXTURES / "contracts_v1.json").read_text())

    doc = build_contract_bundle(**_inputs(fx["input"]))

    assert canonical_json(doc).decode() == fx["canonical"]


def test_the_fixture_document_is_the_stored_form_the_library_computes():
    """The fixture's one contract is the demo contract's stored form: this builder
    and the library agree on the document and its hash."""
    fx = json.loads((FIXTURES / "contracts_v1.json").read_text())
    [contract] = fx["input"]["contracts"]

    [stored] = store_contracts(
        [_kpis_node()], {"analytics.py_daily_kpis": [TableRef("analytics", "daily_transactions", "")]},
        POSTGRES_SYSTEM)

    assert stored.document == contract["document"]
    assert stored.contract_hash == contract["contract_hash"]
    assert stored.source_sha256 == contract["source_sha256"]
    assert stored.source_path == contract["source_path"]


# --- the stored form ------------------------------------------------------------

def test_the_stored_form_carries_the_tables_the_queries_name():
    [kpis] = store_contracts(
        [_kpis_node()],
        {"analytics.py_daily_kpis": [
            TableRef("analytics", "daily_transactions", ""), TableRef("pg_catalog", "pg_class", "")]},
        POSTGRES_SYSTEM)

    assert kpis.document["schema"][0]["customProperties"] == [
        {"property": "reads", "value": ["analytics.daily_transactions"], "vendor": "continuo"}]
    assert kpis.contract_hash == _vector("demo_py_daily_kpis")["contract_hash"]
    assert kpis.document == _vector("demo_py_daily_kpis")["stored"]


def test_a_catalog_qualified_table_is_named_by_its_schema_and_table():
    [kpis] = store_contracts(
        [_kpis_node()],
        {"analytics.py_daily_kpis": [TableRef("analytics", "daily_transactions", "warehouse")]},
        POSTGRES_SYSTEM)

    assert _reads(kpis.document) == [["analytics.daily_transactions"]]


def test_a_table_two_queries_name_through_two_catalogs_is_listed_once_in_lower_case():
    [kpis] = store_contracts(
        [_kpis_node()],
        {"analytics.py_daily_kpis": [
            TableRef("Analytics", "Orders", "a"), TableRef("analytics", "orders", "b"),
            TableRef("analytics", "fx", "")]},
        POSTGRES_SYSTEM)

    assert _reads(kpis.document) == [["analytics.fx", "analytics.orders"]]


def test_a_contract_that_reads_nothing_has_no_reads_property():
    [csv] = store_contracts([_node("demo_orders_csv", demo_text("demo_orders_csv"))], {}, POSTGRES_SYSTEM)

    assert _reads(csv.document) == []
    assert csv.contract_hash == _vector("demo_orders_csv")["contract_hash"]
    assert csv.document == _vector("demo_orders_csv")["stored"]


def test_only_system_relations_read_means_no_reads_property():
    [kpis] = store_contracts(
        [_kpis_node()], {"analytics.py_daily_kpis": [TableRef("pg_catalog", "pg_class", "")]}, POSTGRES_SYSTEM)

    assert _reads(kpis.document) == []


def test_a_stored_contract_names_its_node_service_source_and_content_hash():
    node = _kpis_node()

    [kpis] = store_contracts([node], {}, POSTGRES_SYSTEM)

    assert (kpis.unique_id, kpis.service, kpis.content_hash) == ("analytics.py_daily_kpis", "service-py", node.content_hash)
    assert (kpis.source_path, kpis.source_sha256) == (
        "contracts/py_daily_kpis.odcs.yaml", sha256_hex(demo_text("py_daily_kpis").encode("utf-8")))


def test_a_node_with_no_contract_stores_nothing():
    dbt_like = ManifestNode(
        table_name="users", schema_name="test_schema", service_name="service-1", owner="t", schedule_name="daily",
        criticality="CORE")

    assert store_contracts([dbt_like], {}, POSTGRES_SYSTEM) == []


def test_the_authored_document_is_not_changed():
    node = _kpis_node()
    before = json.dumps(node.contract.document, sort_keys=True)

    store_contracts([node], {"analytics.py_daily_kpis": [TableRef("analytics", "daily_transactions", "")]}, POSTGRES_SYSTEM)

    assert json.dumps(node.contract.document, sort_keys=True) == before


@pytest.mark.parametrize(
    "name", ["demo_py_daily_kpis", "demo_orders_csv", "demo_fx_eur_usd_rates", "full_python_node", "full_python_csv",
             "full_python_api", "messy_python_api", "messy_column_types"])
def test_every_valid_library_vector_stores_to_its_stored_form_and_hash(name):
    vector = _vector(name)
    node = _node(name, vector["text"], path=vector["path"], service=vector["service"])
    schema, table = node.schema_name, node.table_name
    refs = [TableRef(*ref.lower().split("."), "") for ref in vector["reads"]]

    [stored] = store_contracts([node], {f"{schema}.{table}": refs}, POSTGRES_SYSTEM)

    assert stored.document == vector["stored"]
    assert stored.contract_hash == vector["contract_hash"]


# --- library errors are bugs: counted and re-raised ----------------------------

def test_a_table_the_library_refuses_is_counted_as_a_normalise_failure_and_raised():
    timings = release_timings()

    with pytest.raises(HashInputError):
        store_contracts(
            [_kpis_node()], {"analytics.py_daily_kpis": [TableRef("analytics", "a b", "")]}, POSTGRES_SYSTEM,
            timings=timings)

    assert timings.contract_failures == {("normalise", "value"): 1}


def test_a_hash_the_library_refuses_is_counted_as_a_hash_failure_and_raised(monkeypatch):
    def refuse(_stored):
        raise CanonicalJsonError("$: not writable")

    monkeypatch.setattr(contract_bundle_module, "contract_hash", refuse)
    timings = release_timings()

    with pytest.raises(CanonicalJsonError):
        store_contracts([_kpis_node()], {}, POSTGRES_SYSTEM, timings=timings)

    assert timings.contract_failures == {("hash", "value"): 1}


def test_progress_is_reported_after_each_contract():
    calls = []

    store_contracts(
        [_kpis_node(), _node("demo_orders_csv", demo_text("demo_orders_csv"))], {}, POSTGRES_SYSTEM,
        progress=lambda: calls.append(1))

    assert len(calls) == 2


# --- the bundle -----------------------------------------------------------------

def _stored(unique_id: str, *, service: str = "service-py") -> StoredContract:
    return StoredContract(
        unique_id=unique_id, service=service, contract_hash="sha256:" + "a" * 64, content_hash="sha256:" + "b" * 64,
        source_path=f"contracts/{unique_id.split('.')[1]}.odcs.yaml", source_sha256="c" * 64,
        document={"id": unique_id})


def _bundle(contracts, **overrides):
    args = {
        "tenant_id": "default", "release_id": "rel-1", "library_version": "0.2.0", "contracts": contracts,
        "provenance": {}, "timings": {"stages_ms": {}, "nodes": {}, "columns": {}, "validation_cache": {}},
    }
    args.update(overrides)
    return build_contract_bundle(**args)


def test_a_release_with_no_python_contract_has_an_empty_bundle():
    doc = _bundle([])

    assert doc["contracts"] == {}
    assert doc["summary"] == {"contracts": 0, "by_family": {}, "invalid": 0, "skipped": 0}
    assert doc["provenance"] == {}
    assert (doc["failures"], doc["ontology"], doc["schema_version"]) == ([], None, 1)


def test_the_bundle_keys_its_contracts_by_unique_id_and_counts_them():
    doc = _bundle([_stored("analytics.b"), _stored("analytics.a")])

    assert sorted(doc["contracts"]) == ["analytics.a", "analytics.b"]
    assert doc["summary"] == {"contracts": 2, "by_family": {"python": 2}, "invalid": 0, "skipped": 0}
    assert doc["contracts"]["analytics.a"] == {
        "family": "python", "service": "service-py", "contract_hash": "sha256:" + "a" * 64,
        "content_hash": "sha256:" + "b" * 64,
        "source": {"path": "contracts/a.odcs.yaml", "sha256": "c" * 64}, "document": {"id": "analytics.a"}}


def test_the_bundle_names_the_library_and_the_schema_it_was_checked_against():
    doc = _bundle([], library_version="0.2.0")

    assert doc["continuo_contracts"] == "0.2.0"
    assert doc["odcs_schema_sha256"] == SCHEMA_SHA256
    assert (doc["tenant_id"], doc["release_id"]) == ("default", "rel-1")


def test_the_bundle_is_the_same_whatever_the_order_of_the_contracts():
    one = _bundle([_stored("analytics.a"), _stored("analytics.b")])
    other = _bundle([_stored("analytics.b"), _stored("analytics.a")])

    assert canonical_json(one) == canonical_json(other)


def test_two_contracts_of_one_unique_id_are_a_bug_not_an_overwrite():
    with pytest.raises(ValueError, match="analytics.a"):
        _bundle([_stored("analytics.a", service="s1"), _stored("analytics.a", service="s2")])


def test_the_bundle_does_not_share_its_inputs():
    provenance = {"service-py": {"continuo_python_runtime": "0.13.0", "image_packages": {"pyarrow": "26.0.0"}}}
    timings = {"stages_ms": {"resolve": 1}, "nodes": {}, "columns": {}, "validation_cache": {}}
    doc = _bundle([], provenance=provenance, timings=timings)

    provenance["service-py"]["image_packages"]["pyarrow"] = "0"
    timings["stages_ms"]["resolve"] = 2

    assert doc["provenance"]["service-py"]["image_packages"] == {"pyarrow": "26.0.0"}
    assert doc["timings"]["stages_ms"] == {"resolve": 1}


def test_the_bundle_key_is_under_the_tenant_prefix():
    assert contract_bundle_key("default", "rel-1") == "tenants/default/topologies/rel-1/contracts.json.gz"
