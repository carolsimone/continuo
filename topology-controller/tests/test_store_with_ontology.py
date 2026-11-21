"""How each contract is stored once the ontology switch and the release's ontology are known: the
ontology part taken away with the switch off, kept as written with the switch on and no ontology,
or filled in from the ontology, and the `ontology` value of the contract bundle that says which.

The contracts come from the continuo-contracts library's ontology vectors: each holds a stored
form that writes bindings, the ontology it was resolved against, the resolved form and its hash."""
import copy
import json
from pathlib import Path
from unittest.mock import MagicMock

import continuo_contracts
import pytest
from continuo_contracts import ontology as ontology_file
from continuo_contracts.canonical import canonical_json
from continuo_contracts.contract import contract_hash
from continuo_contracts.ontology import BINDING_PROPERTIES, LINK_PREFIX
from continuo_contracts.ontology_check import RULES, Finding

from config.config import _SYSTEM_RELATIONS
from domain.model import ContractFamily, ManifestFile, ManifestKind, ManifestNode, NodeContract, NodeType, Runtime, TableRef
from service.contract_bundle import StoredContract, build_contract_bundle, store_contracts
from service.ontology_binding import ReleaseOntology, bind_contract, load_release_ontology, ontology_envelope
from tests.fakes import RecordingContractBundleWriter, python_handler, source_of
from tests.odcs_fixtures import bundle_file, bundle_node, demo_text

FIXTURES = Path(__file__).parent / "fixtures"
POSTGRES_SYSTEM = _SYSTEM_RELATIONS["postgres"]
_VECTORS = {
    vector["name"]: vector
    for vector in json.loads((Path(continuo_contracts.__file__).parent / "vectors" / "ontology.json").read_text())
}
NO_ONTOLOGY_WARNING = {
    "severity": "warning",
    "rule": "contract",
    "subject": "ontology",
    "message": "the ontology switch is on and no ontology is released: bindings are kept as written and not checked",
    "contract_id": None,
}


def _vector(name: str) -> dict:
    return copy.deepcopy(_VECTORS[name])


def _release_ontology(tmp_path, text: str, *, service="company-ontology", release_id="rel-onto-4") -> ReleaseOntology:
    path = tmp_path / "ontology.yaml"
    path.write_text(text, encoding="utf-8")
    return load_release_ontology(ManifestFile(
        path=str(path), declared_service=service, kind=ManifestKind.ONTOLOGY, release_id=release_id))


def _reads(stored: dict) -> list[TableRef]:
    tables = [
        entry for prop in stored["schema"][0].get("customProperties", []) if prop["property"] == "reads"
        for entry in prop["value"]
    ]
    return [TableRef(*table.split("."), "") for table in tables]


def _node(stored: dict) -> ManifestNode:
    """The node whose contract is `stored`, which the library's stored forms are also valid
    authored documents of: storing it again gives the same form."""
    values = {p["property"]: p["value"] for p in stored["customProperties"] if p["vendor"] == "continuo"}
    schema, table = stored["id"].split(".")
    python = values["kind"].startswith("python")
    return ManifestNode(
        table_name=table, schema_name=schema, service_name=values["service"], owner="team", schedule_name="daily",
        criticality="SECONDARY", node_type=NodeType(values["kind"]),
        runtime=Runtime.PYTHON if python else Runtime.DBT,
        content_hash="sha256:" + "b" * 64,
        contract=NodeContract(
            family=ContractFamily.PYTHON if python else ContractFamily.DBT,
            document=copy.deepcopy(stored), path=f"contracts/{table}.odcs.yaml", source_sha256="c" * 64),
    )


def _stored(stored: dict, **bind_args) -> StoredContract:
    node = _node(stored)
    (contract,) = store_contracts(
        [node], {node.node_key: _reads(stored)}, POSTGRES_SYSTEM,
        bind=lambda form, _node: bind_contract(form, **bind_args),
    )
    return contract


def _without_bindings(stored: dict) -> dict:
    """The stored form with the binding properties taken out by hand, as the contract of a team
    that writes none would be."""
    plain = copy.deepcopy(stored)
    for table in plain["schema"]:
        for item in (table, *table.get("properties", [])):
            kept = [p for p in item.get("customProperties", []) if p["property"] not in BINDING_PROPERTIES]
            if kept:
                item["customProperties"] = kept
            else:
                item.pop("customProperties", None)
    return plain


# --- the switch off ------------------------------------------------------------------

@pytest.mark.parametrize("name", ["python_node_with_aggregates", "dbt_model_with_a_filter_and_a_join"])
def test_a_bound_contract_with_the_switch_off_hashes_as_an_unbound_one(name):
    bound = _vector(name)["stored"]
    assert ontology_file.has_bindings(bound)

    with_switch_off = _stored(bound, enabled=False, release_ontology=None)
    unbound = _stored(_without_bindings(bound), enabled=False, release_ontology=None)

    assert not ontology_file.has_bindings(with_switch_off.document)
    assert with_switch_off.document == unbound.document
    assert with_switch_off.contract_hash == unbound.contract_hash == contract_hash(unbound.document)
    # The bindings move the hash of the form as written, which is why the switch takes them away.
    assert contract_hash(bound) != with_switch_off.contract_hash


def test_a_bound_python_contract_with_the_switch_off_hashes_as_an_unbound_one():
    bound = _vector("python_node_with_aggregates")["stored"]

    with_switch_off = _stored(bound, enabled=False, release_ontology=None)

    assert with_switch_off.contract_hash == _stored(_without_bindings(bound), enabled=False, release_ontology=None).contract_hash


def test_the_switch_off_leaves_a_contract_with_no_ontology_part_as_it_is():
    """A contract that writes no binding and holds no link stores as it would without the switch,
    the hash of every contract of every fixture release being the proof."""
    plain = _without_bindings(_vector("python_node_with_aggregates")["stored"])

    assert _stored(plain, enabled=False, release_ontology=None).document == plain


def test_the_switch_off_takes_the_links_away_too():
    resolved = _vector("dbt_model_with_a_filter_and_a_join")["resolved"]
    assert LINK_PREFIX in json.dumps(resolved)

    stored = _stored(resolved, enabled=False, release_ontology=None).document

    assert LINK_PREFIX not in json.dumps(stored)
    assert not ontology_file.has_bindings(stored)


# --- the switch on --------------------------------------------------------------------

def test_a_declared_service_is_stored_resolved(tmp_path):
    vector = _vector("python_node_with_aggregates")
    ontology = _release_ontology(tmp_path, vector["ontology"])

    stored = _stored(vector["stored"], enabled=True, release_ontology=ontology)

    assert stored.document == vector["resolved"]
    assert stored.contract_hash == vector["contract_hash"]
    assert stored.document["domain"] == "analytics"
    assert {p["name"]: p["semanticType"] for p in stored.document["schema"][0]["properties"]} == {
        "day": "dimension", "tx_count": "measure", "total_amount": "measure"}
    assert stored.contract_hash != contract_hash(vector["stored"])


def test_a_declared_entity_gets_its_link_and_the_key_of_its_columns(tmp_path):
    vector = _vector("dbt_model_with_a_filter_and_a_join")
    ontology = _release_ontology(tmp_path, vector["ontology"])

    document = _stored(vector["stored"], enabled=True, release_ontology=ontology).document

    assert {"type": "ontology", "url": f"{LINK_PREFIX}Transaction"} in document["schema"][0]["authoritativeDefinitions"]
    assert document["domain"] == "finance"


def test_a_declared_dbt_contract_is_stored_resolved(tmp_path):
    vector = _vector("dbt_model_with_a_filter_and_a_join")
    ontology = _release_ontology(tmp_path, vector["ontology"])

    stored = _stored(vector["stored"], enabled=True, release_ontology=ontology)

    assert stored.document == vector["resolved"]
    assert stored.contract_hash == vector["contract_hash"]


def test_an_undeclared_service_keeps_its_bindings_as_written(tmp_path):
    vector = _vector("service_not_declared")
    ontology = _release_ontology(tmp_path, vector["ontology"])
    assert not ontology.ontology.declares("core")

    stored = _stored(vector["stored"], enabled=True, release_ontology=ontology)

    assert stored.document == vector["stored"]
    assert ontology_file.has_bindings(stored.document)
    assert stored.contract_hash == contract_hash(vector["stored"])


def test_the_switch_on_with_no_ontology_keeps_bindings_and_warns():
    bound = _vector("python_node_with_aggregates")["stored"]

    stored = _stored(bound, enabled=True, release_ontology=None)
    envelope = ontology_envelope(enabled=True, release_ontology=None, findings=())

    assert stored.document == bound
    assert stored.contract_hash == contract_hash(bound)
    assert envelope == {
        "enabled": True, "service": None, "release_id": None, "sha256": None, "declared_services": [],
        "findings": [NO_ONTOLOGY_WARNING],
    }


def test_a_failure_of_the_library_while_binding_is_counted_and_raised():
    bound = _vector("python_node_with_aggregates")["stored"]
    node = _node(bound)
    timings = MagicMock()

    def refuse(form, _node):
        raise ValueError("the library refuses it")

    with pytest.raises(ValueError, match="refuses"):
        store_contracts([node], {node.node_key: _reads(bound)}, POSTGRES_SYSTEM, timings=timings, bind=refuse)

    timings.add_contract_failure.assert_called_once_with("normalise", "value")


# --- the envelope -----------------------------------------------------------------------

def test_the_envelope_is_none_with_the_switch_off(tmp_path):
    ontology = _release_ontology(tmp_path, _vector("python_node_with_aggregates")["ontology"])

    assert ontology_envelope(enabled=False, release_ontology=ontology, findings=()) is None
    assert ontology_envelope(enabled=False, release_ontology=None, findings=()) is None


def test_the_envelope_records_the_ontology_the_release_used(tmp_path):
    text = _vector("python_node_with_aggregates")["ontology"]
    ontology = _release_ontology(tmp_path, text, service="company-ontology", release_id="rel-9f8e7d6-14")

    envelope = ontology_envelope(enabled=True, release_ontology=ontology, findings=())

    assert envelope == {
        "enabled": True,
        "service": "company-ontology",
        "release_id": "rel-9f8e7d6-14",
        "sha256": ontology.ontology.sha256,
        "declared_services": ["core", "finance", "service-py"],
        "findings": [],
    }
    assert len(envelope["sha256"]) == 64


def test_the_envelope_lists_every_finding_in_the_order_the_library_sorts_them(tmp_path):
    ontology = _release_ontology(tmp_path, _vector("python_node_with_aggregates")["ontology"])
    info = Finding("info", "order", "copy graph", "layers", None)
    late = Finding("warning", "drift", "User.name", "no copy holds it", None)
    early = Finding("warning", "contract", "analytics.b", "a message", "analytics.b")

    envelope = ontology_envelope(enabled=True, release_ontology=ontology, findings=[info, late, early])

    assert [(f["severity"], f["rule"], f["subject"]) for f in envelope["findings"]] == [
        ("warning", "contract", "analytics.b"), ("warning", "drift", "User.name"), ("info", "order", "copy graph")]
    assert envelope["findings"][0] == {
        "severity": "warning", "rule": "contract", "subject": "analytics.b", "message": "a message",
        "contract_id": "analytics.b"}


def test_the_warning_of_a_release_with_no_ontology_is_one_of_the_librarys_rules():
    assert NO_ONTOLOGY_WARNING["rule"] in RULES


# --- the bundle --------------------------------------------------------------------------

def _fixture_input() -> dict:
    return json.loads((FIXTURES / "contracts_v1_ontology.json").read_text())["input"]


def _bundle_inputs(given: dict) -> dict:
    return {
        "tenant_id": given["tenant_id"],
        "release_id": given["release_id"],
        "library_version": given["library_version"],
        "contracts": [StoredContract(**c) for c in given["contracts"]],
        "provenance": given["provenance"],
        "timings": given["timings"],
        "ontology": given["ontology"],
    }


def test_the_bundle_with_an_ontology_matches_the_go_fixture():
    fixture = json.loads((FIXTURES / "contracts_v1_ontology.json").read_text())

    document = build_contract_bundle(**_bundle_inputs(fixture["input"]))

    assert canonical_json(document).decode() == fixture["canonical"]


def test_the_fixture_contracts_are_what_the_ontology_resolves_the_library_vectors_to(tmp_path):
    """The fixture is no hand-written document: its two contracts are the resolved forms of the
    library's python and dbt vectors, with the hashes the library gives them."""
    given = _fixture_input()
    ontology = _release_ontology(
        tmp_path, _vector("python_node_with_aggregates")["ontology"],
        service=given["ontology"]["service"], release_id=given["ontology"]["release_id"])
    by_id = {c["unique_id"]: c for c in given["contracts"]}
    assert sorted(by_id) == ["analytics.fx_transactions_eur", "analytics.py_daily_kpis"]

    for name in ("python_node_with_aggregates", "dbt_model_with_a_filter_and_a_join"):
        vector = _vector(name)
        stored = _stored(vector["stored"], enabled=True, release_ontology=ontology)
        recorded = by_id[stored.unique_id]
        assert recorded["document"] == stored.document, name
        assert recorded["contract_hash"] == stored.contract_hash == vector["contract_hash"], name
    assert given["ontology"] == ontology_envelope(enabled=True, release_ontology=ontology, findings=[
        Finding(**finding) for finding in given["ontology"]["findings"]])


def test_the_bundle_of_a_release_with_the_switch_off_holds_a_null_ontology():
    document = build_contract_bundle(**{**_bundle_inputs(_fixture_input()), "ontology": None})

    assert document["ontology"] is None


def test_the_bundle_does_not_share_its_ontology_with_its_caller():
    given = _bundle_inputs(_fixture_input())

    document = build_contract_bundle(**given)
    document["ontology"]["findings"].clear()

    assert given["ontology"]["findings"] != []


# --- through the handler -------------------------------------------------------------------

AUTHORED = """\
apiVersion: v3.2.0
kind: DataContract
id: analytics.py_daily_kpis
name: py_daily_kpis
version: "0"
description:
  purpose: Daily transaction count and total amount in EUR, rolled up from analytics.daily_transactions.
team:
  name: service-py
customProperties:
  - {vendor: continuo, property: schedule, value: daily}
  - {vendor: continuo, property: criticality, value: SECONDARY}
  - {vendor: continuo, property: script, value: scripts/py_daily_kpis.py}
  - vendor: continuo
    property: readQueries
    value:
      daily: select created_at, amount_eur from analytics.daily_transactions
schema:
  - name: py_daily_kpis
    physicalType: table
    customProperties:
      - {vendor: continuo, property: grain, value: [day]}
    properties:
      - name: day
        physicalType: DATE
        required: true
        customProperties:
          - {vendor: continuo, property: aggregate, value: {from: Transaction.created_at, period: day}}
      - name: tx_count
        physicalType: INTEGER
        required: true
        customProperties:
          - {vendor: continuo, property: aggregate, value: {from: Transaction, agg: count}}
      - name: total_amount
        physicalType: "NUMERIC(12,2)"
        required: true
        customProperties:
          - {vendor: continuo, property: aggregate, value: {from: Transaction.amount_eur, agg: sum}}
"""


def _release(tmp_path, *, enabled: bool, authored: str = AUTHORED, ontology_text: str | None = None):
    """The contract bundle document of a release of service-py, which carries the ontology file
    `ontology_text` when there is one."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    files = [bundle_file(tmp_path, [bundle_node("contracts/py_daily_kpis.odcs.yaml", authored)])]
    if ontology_text is not None:
        path = tmp_path / "ontology.yaml"
        path.write_text(ontology_text, encoding="utf-8")
        files.append(ManifestFile(
            path=str(path), declared_service="company-ontology", kind=ManifestKind.ONTOLOGY, release_id="rel-onto-4"))
    publisher, writer = MagicMock(), RecordingContractBundleWriter()
    python_handler(
        source_of(*files), publisher, contract_bundle_writer=writer, ontology_enabled=enabled,
    ).handle(release_id="rel-1")
    publisher.publish_ok.assert_called_once()
    return writer.document


def _the_contract(bundle: dict) -> dict:
    (contract,) = bundle["contracts"].values()
    return contract


def test_a_release_with_the_switch_off_stores_a_bound_contract_as_an_unbound_one(tmp_path):
    bound = _release(tmp_path / "bound", enabled=False)
    plain = _release(tmp_path / "plain", enabled=False, authored=demo_text("py_daily_kpis"))

    assert _the_contract(bound)["contract_hash"] == _the_contract(plain)["contract_hash"]
    assert _the_contract(bound)["document"] == _the_contract(plain)["document"]
    assert bound["ontology"] is None and plain["ontology"] is None


def test_a_release_with_the_switch_off_ignores_the_ontology_it_carries(tmp_path):
    with_ontology = _release(
        tmp_path / "with", enabled=False, ontology_text=_vector("python_node_with_aggregates")["ontology"])
    without = _release(tmp_path / "without", enabled=False)

    assert _the_contract(with_ontology) == _the_contract(without)
    assert with_ontology["ontology"] is None


def test_a_release_with_the_switch_on_stores_the_contracts_of_a_declared_service_resolved(tmp_path):
    ontology_text = _vector("python_node_with_aggregates")["ontology"]

    bundle = _release(tmp_path, enabled=True, ontology_text=ontology_text)

    contract = _the_contract(bundle)
    assert contract["document"]["domain"] == "analytics"
    assert contract["document"]["schema"][0]["properties"][1]["semanticType"] == "measure"
    assert contract["contract_hash"] == contract_hash(contract["document"])
    assert bundle["ontology"] == {
        "enabled": True,
        "service": "company-ontology",
        "release_id": "rel-onto-4",
        "sha256": ontology_file.load(ontology_text).sha256,
        "declared_services": ["core", "finance", "service-py"],
        "findings": [],
    }


def test_a_release_with_the_switch_on_and_no_ontology_keeps_the_bindings_and_warns(tmp_path):
    bundle = _release(tmp_path, enabled=True)

    document = _the_contract(bundle)["document"]
    assert ontology_file.has_bindings(document)
    assert "domain" not in document
    assert bundle["ontology"]["service"] is None
    assert bundle["ontology"]["findings"] == [NO_ONTOLOGY_WARNING]


def test_a_release_with_the_switch_on_keeps_the_bindings_of_a_service_the_ontology_does_not_declare(tmp_path):
    ontology_text = _vector("service_not_declared")["ontology"]
    assert "service-py" not in ontology_file.load(ontology_text).services()

    bundle = _release(tmp_path, enabled=True, ontology_text=ontology_text)

    document = _the_contract(bundle)["document"]
    assert ontology_file.has_bindings(document)
    assert "domain" not in document and LINK_PREFIX not in json.dumps(document)
    assert bundle["ontology"]["service"] == "company-ontology"
