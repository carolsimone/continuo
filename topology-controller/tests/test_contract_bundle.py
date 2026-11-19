"""The stored form of each contract, Python and dbt, and the release's contract bundle."""
import copy
import json
from pathlib import Path

import continuo_contracts
import pytest
from continuo_contracts import dbt, validate, yaml_strict
from continuo_contracts.canonical import canonical_json
from continuo_contracts.contract import normalise_dbt, normalise_python
from continuo_contracts.errors import CanonicalJsonError, HashInputError
from continuo_contracts.hashing import sha256_hex
from continuo_contracts.schema import SCHEMA_SHA256

import service.contract_bundle as contract_bundle_module
from config.config import _SYSTEM_RELATIONS
from domain.contract_vocabulary import ParseFailureKind
from domain.model import ContractFamily, FailedNode, ManifestNode, NodeContract, NodeType, Runtime, TableRef
from service.contract_bundle import (
    StoredContract,
    build_contract_bundle,
    contract_bundle_key,
    contracts_by_family,
    invalid_contract_reads,
    shared_unique_ids,
    store_contracts,
)
from tests.odcs_fixtures import demo_text, release_timings

FIXTURES = Path(__file__).parent / "fixtures"
POSTGRES_SYSTEM = _SYSTEM_RELATIONS["postgres"]
_VECTOR_DIR = Path(continuo_contracts.__file__).parent / "vectors"
_VECTORS = json.loads((_VECTOR_DIR / "contracts.json").read_text())
_DBT_VECTORS = {case["name"]: case for case in json.loads((_VECTOR_DIR / "dbt_contracts.json").read_text())}
# The dbt vectors whose node has a valid contract (the others name the issues of an invalid one).
_DBT_VALID_VECTORS = [name for name, case in _DBT_VECTORS.items() if "generated" in case]


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
        contract=NodeContract(
            family=ContractFamily.PYTHON,
            document=document,
            path=path or f"contracts/{name}.odcs.yaml",
            source_sha256=sha256_hex(text.encode("utf-8")),
        ),
    )


def _kpis_node() -> ManifestNode:
    return _node("py_daily_kpis", demo_text("py_daily_kpis"))


def _dbt_node(name: str, *, edit=None) -> ManifestNode:
    """The node a dbt parse builds for the library's dbt vector `name`: its contract is what
    `dbt.generate` writes for the vector's manifest node (after `edit`, a change to a copy of
    that node), and its run facts are read back from that contract."""
    case = _DBT_VECTORS[name]
    manifest_node = copy.deepcopy(case["node"])
    if edit is not None:
        edit(manifest_node)
    generated = dbt.generate(manifest_node, case["tests"], manifest=case["manifest"])
    facts = dbt.run_facts(generated.document)
    return ManifestNode(
        table_name=facts.table,
        schema_name=facts.schema,
        service_name=facts.service,
        owner=facts.owner,
        schedule_name=facts.schedule,
        criticality=facts.criticality,
        node_type=NodeType(facts.kind),
        runtime=Runtime.DBT,
        content_hash="sha256:" + sha256_hex(name.encode("utf-8")),
        original_file_path=facts.file_path,
        contract=NodeContract(
            family=ContractFamily.DBT,
            document=generated.document,
            path=facts.file_path,
            source_sha256=generated.source_sha256,
        ),
    )


def _with_declared_reads(*entries: str) -> ManifestNode:
    """The library's governed model with `entries` as the tables its meta declares it reads."""
    return _dbt_node("governed", edit=lambda node: node["config"]["meta"]["continuo"].__setitem__("reads", list(entries)))


def _table_refs(tables: list[str]) -> list[TableRef]:
    return [TableRef(*table.split("."), "") for table in tables]


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


def test_the_fixture_documents_are_the_stored_forms_the_library_computes():
    """The fixture's contracts are the demo Python contract's and the library's governed dbt
    model's stored forms: this builder and the library agree on each document and its hash."""
    fx = json.loads((FIXTURES / "contracts_v1.json").read_text())
    by_id = {c["unique_id"]: c for c in fx["input"]["contracts"]}
    assert sorted(by_id) == ["analytics.governed", "analytics.py_daily_kpis"]
    governed = _dbt_node("governed")

    stored = {
        c.unique_id: c
        for c in store_contracts(
            [_kpis_node(), governed],
            {
                ("service-py", "analytics.py_daily_kpis"): [TableRef("analytics", "daily_transactions", "")],
                governed.node_key: _table_refs(_DBT_VECTORS["sql_reads"]["reads"]),
            },
            POSTGRES_SYSTEM)
    }

    for unique_id, contract in by_id.items():
        assert stored[unique_id].family == contract["family"], unique_id
        assert stored[unique_id].document == contract["document"], unique_id
        assert stored[unique_id].contract_hash == contract["contract_hash"], unique_id
        assert stored[unique_id].source_sha256 == contract["source_sha256"], unique_id
        assert stored[unique_id].source_path == contract["source_path"], unique_id


# --- the stored form ------------------------------------------------------------

def test_the_stored_form_carries_the_tables_the_queries_name():
    [kpis] = store_contracts(
        [_kpis_node()],
        {("service-py", "analytics.py_daily_kpis"): [
            TableRef("analytics", "daily_transactions", ""), TableRef("pg_catalog", "pg_class", "")]},
        POSTGRES_SYSTEM)

    assert kpis.document["schema"][0]["customProperties"] == [
        {"property": "reads", "value": ["analytics.daily_transactions"], "vendor": "continuo"}]
    assert kpis.contract_hash == _vector("demo_py_daily_kpis")["contract_hash"]
    assert kpis.document == _vector("demo_py_daily_kpis")["stored"]


def test_a_catalog_qualified_table_is_named_by_its_schema_and_table():
    [kpis] = store_contracts(
        [_kpis_node()],
        {("service-py", "analytics.py_daily_kpis"): [TableRef("analytics", "daily_transactions", "warehouse")]},
        POSTGRES_SYSTEM)

    assert _reads(kpis.document) == [["analytics.daily_transactions"]]


def test_a_table_two_queries_name_through_two_catalogs_is_listed_once_in_lower_case():
    [kpis] = store_contracts(
        [_kpis_node()],
        {("service-py", "analytics.py_daily_kpis"): [
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
        [_kpis_node()], {("service-py", "analytics.py_daily_kpis"): [TableRef("pg_catalog", "pg_class", "")]}, POSTGRES_SYSTEM)

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

    store_contracts([node], {("service-py", "analytics.py_daily_kpis"): [TableRef("analytics", "daily_transactions", "")]}, POSTGRES_SYSTEM)

    assert json.dumps(node.contract.document, sort_keys=True) == before


@pytest.mark.parametrize(
    "name", ["demo_py_daily_kpis", "demo_orders_csv", "demo_fx_eur_usd_rates", "full_python_node", "full_python_csv",
             "full_python_api", "messy_python_api", "messy_column_types"])
def test_every_valid_library_vector_stores_to_its_stored_form_and_hash(name):
    vector = _vector(name)
    node = _node(name, vector["text"], path=vector["path"], service=vector["service"])
    schema, table = node.schema_name, node.table_name
    refs = [TableRef(*ref.lower().split("."), "") for ref in vector["reads"]]

    [stored] = store_contracts([node], {node.node_key: refs}, POSTGRES_SYSTEM)

    assert stored.document == vector["stored"]
    assert stored.contract_hash == vector["contract_hash"]


# --- library errors are bugs: counted and re-raised ----------------------------

def test_a_normalise_the_library_refuses_is_counted_and_raised(monkeypatch):
    def refuse(*_args, **_kwargs):
        raise HashInputError("service '': blank")

    monkeypatch.setattr(contract_bundle_module, "normalise_python", refuse)
    timings = release_timings()

    with pytest.raises(HashInputError):
        store_contracts([_kpis_node()], {}, POSTGRES_SYSTEM, timings=timings)

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


# --- tables a contract cannot list -----------------------------------------------

_KPIS_ID = "analytics.py_daily_kpis"


# Each (schema, table) pair a contract may be asked to list: some the library lists, some it refuses.
_TABLE_CASES = [
    ("analytics", "orders"), ("Analytics", "Orders"), ("é", "t"), ("a-b", "c_d"), ("a1", "2b"),
    ("a.b", "c"), ("a", "b.c"), ("a", "b.c.d"), ("", "t"), ("s", ""),
    ("my schema", "t"), ("s", "my table"), (" s", "t"), ("s", "t "), ("s", "t\tu"), ("s", "t\nu"),
    ("s", "t\u00a0u"), ("s", "t\u2003u"), ("s", "t\u0085u"), ("s", "t\x1fu"), ("s", "t\x00u"),
    ("s", "t\x7fu"), ("s", "t\x9fu"), ("s", "t\u200bu"),
]


def _library_lists(call, tables: list[str]) -> bool:
    """Whether the library's normalisation accepts `tables` as the tables a node reads."""
    try:
        call(tables)
    except HashInputError:
        return False
    return True


def test_every_table_the_library_lists_passes_the_check_and_every_one_it_refuses_fails_it():
    """The check is the library's rule for a listed table: it agrees with normalise_python on
    each of these, so a table the check passes never reaches a HashInputError."""
    document = yaml_strict.load(demo_text("py_daily_kpis"))
    node = _kpis_node()
    for schema, table in _TABLE_CASES:
        library_accepts = _library_lists(
            lambda tables: normalise_python(document, service="service-py", reads=tables), [f"{schema}.{table}"])
        failure = invalid_contract_reads(node, [TableRef(schema, table, "")], POSTGRES_SYSTEM)
        assert (failure is None) == library_accepts, (schema, table)


def test_the_failed_node_of_an_unlistable_table_is_named_like_any_invalid_contract():
    timings = release_timings()

    failure = invalid_contract_reads(
        _kpis_node(), [TableRef("analytics", "daily_transactions", ""), TableRef("analytics", "a b", "")],
        POSTGRES_SYSTEM, timings=timings)

    assert failure == FailedNode(
        node_id=_KPIS_ID, kind=ParseFailureKind.INVALID_CONTRACT, service="service-py",
        file_path="contracts/py_daily_kpis.odcs.yaml", node_type=NodeType.PYTHON_NODE,
        detail='readQueries: table analytics."a b" cannot be listed as <schema>.<table>: a name holds whitespace')
    assert timings.contract_failures == {("normalise", "value"): 1}


@pytest.mark.parametrize(
    ("ref", "shown", "reason"),
    [
        (TableRef("analytics", "a.b", ""), 'analytics."a.b"', "a name holds a dot"),
        (TableRef("a.b", "c", ""), '"a.b".c', "a name holds a dot"),
        (TableRef("analytics", "t\x00u", ""), 'analytics."t\\u0000u"', "a name holds a control character"),
        (TableRef("analytics", "t\u00a0u", ""), 'analytics."t\u00a0u"', "a name holds whitespace"),
        (TableRef("", "t", ""), '"".t', "a name is empty"),
    ],
    ids=["table-dot", "schema-dot", "control", "no-break-space", "empty"])
def test_the_detail_shows_the_name_and_says_why(ref, shown, reason):
    failure = invalid_contract_reads(_kpis_node(), [ref], POSTGRES_SYSTEM)

    assert failure.detail == f"readQueries: table {shown} cannot be listed as <schema>.<table>: {reason}"


def test_a_table_read_through_two_catalogs_is_one_problem():
    timings = release_timings()

    failure = invalid_contract_reads(
        _kpis_node(), [TableRef("analytics", "a b", "c1"), TableRef("analytics", "a b", "c2")], POSTGRES_SYSTEM,
        timings=timings)

    assert failure.detail.count("cannot be listed") == 1
    assert timings.contract_failures == {("normalise", "value"): 1}


def test_a_system_relation_is_not_checked_and_a_node_without_a_contract_has_nothing_to_list():
    timings = release_timings()
    dbt_like = ManifestNode(
        table_name="users", schema_name="test_schema", service_name="service-1", owner="t", schedule_name="daily",
        criticality="CORE")

    assert invalid_contract_reads(_kpis_node(), [TableRef("pg_catalog", "a b", "")], POSTGRES_SYSTEM, timings=timings) is None
    assert invalid_contract_reads(dbt_like, [TableRef("analytics", "a b", "")], POSTGRES_SYSTEM, timings=timings) is None
    assert timings.contract_failures == {}


def test_the_details_of_one_node_come_in_table_order_whatever_the_order_of_the_queries():
    one = invalid_contract_reads(_kpis_node(), [TableRef("analytics", "b c", ""), TableRef("analytics", "a b", "")], POSTGRES_SYSTEM)
    other = invalid_contract_reads(_kpis_node(), [TableRef("analytics", "a b", ""), TableRef("analytics", "b c", "")], POSTGRES_SYSTEM)

    assert one == other and one.detail.index('"a b"') < one.detail.index('"b c"')


# --- dbt contracts ---------------------------------------------------------------

def test_a_dbt_contract_lists_the_tables_its_sql_names():
    """The stored form of the governed model joins the tables its compiled SQL names with the one its
    meta declares: system relations left out, names in lower case, each table once."""
    node = _dbt_node("governed")
    vector = _DBT_VECTORS["sql_reads"]
    refs = [
        TableRef("Analytics", "Orders", "wh"), TableRef("analytics", "orders", ""), TableRef("raw", "events", ""),
        TableRef("pg_catalog", "pg_class", "")]

    [stored] = store_contracts([node], {node.node_key: refs}, POSTGRES_SYSTEM)

    assert _reads(stored.document) == [["analytics.orders", "analytics.raw_events", "raw.events"]]
    assert stored.document == vector["stored"]
    assert stored.contract_hash == vector["contract_hash"]
    assert (stored.family, stored.unique_id, stored.service, stored.content_hash) == (
        "dbt", "analytics.governed", "cases", node.content_hash)
    assert (stored.source_path, stored.source_sha256) == ("models/tagged/governed.sql", vector["source_sha256"])


def test_a_dbt_node_that_reads_nothing_it_declares_has_no_reads_property():
    [stored] = store_contracts([_dbt_node("dormant_model")], {}, POSTGRES_SYSTEM)

    assert _reads(stored.document) == []
    assert stored.document == _DBT_VECTORS["dormant_model"]["stored"]


@pytest.mark.parametrize("name", _DBT_VALID_VECTORS)
def test_the_library_vector_through_topology_controller(name):
    """Every valid case of the library's dbt vector stores to the stored form and hash the vector
    gives, from the contract `dbt.generate` writes and the tables the vector's SQL names, and
    none of them names a table the check refuses."""
    vector = _DBT_VECTORS[name]
    node = _dbt_node(name)
    refs = _table_refs(vector["reads"])

    assert invalid_contract_reads(node, refs, POSTGRES_SYSTEM) is None
    [stored] = store_contracts([node], {node.node_key: refs}, POSTGRES_SYSTEM)

    assert stored.document == vector["stored"]
    assert stored.contract_hash == vector["contract_hash"]
    assert stored.source_sha256 == vector["source_sha256"]
    assert stored.unique_id == vector["generated"]["id"]


def test_the_generated_dbt_document_is_not_changed():
    node = _dbt_node("governed")
    before = json.dumps(node.contract.document, sort_keys=True)

    store_contracts([node], {node.node_key: [TableRef("analytics", "orders", "")]}, POSTGRES_SYSTEM)

    assert json.dumps(node.contract.document, sort_keys=True) == before


def test_a_normalise_the_library_refuses_for_a_dbt_contract_is_counted_and_raised(monkeypatch):
    def refuse(*_args, **_kwargs):
        raise HashInputError("reads entry 'x': a table is written <schema>.<table>")

    monkeypatch.setattr(contract_bundle_module, "normalise_dbt", refuse)
    timings = release_timings()

    with pytest.raises(HashInputError):
        store_contracts([_dbt_node("governed")], {}, POSTGRES_SYSTEM, timings=timings)

    assert timings.contract_failures == {("normalise", "value"): 1}


def test_a_dbt_contract_is_not_normalised_as_a_python_one(monkeypatch):
    def refuse(*_args, **_kwargs):
        raise AssertionError("a dbt contract must not reach normalise_python")

    monkeypatch.setattr(contract_bundle_module, "normalise_python", refuse)

    store_contracts([_dbt_node("governed")], {}, POSTGRES_SYSTEM)


def test_a_dbt_table_that_cannot_be_listed_is_an_invalid_contract():
    timings = release_timings()

    failure = invalid_contract_reads(
        _dbt_node("governed"), [TableRef("analytics", "orders", ""), TableRef("analytics", "a b", "")],
        POSTGRES_SYSTEM, timings=timings)

    assert failure == FailedNode(
        node_id="analytics.governed", kind=ParseFailureKind.INVALID_CONTRACT, service="cases",
        file_path="models/tagged/governed.sql", node_type=NodeType.DBT_MODEL,
        detail='compiled SQL: table analytics."a b" cannot be listed as <schema>.<table>: a name holds whitespace')
    assert timings.contract_failures == {("normalise", "value"): 1}


@pytest.mark.parametrize(
    ("ref", "shown", "reason"),
    [
        (TableRef("analytics", "a.b", ""), 'analytics."a.b"', "a name holds a dot"),
        (TableRef("", "t", ""), '"".t', "a name is empty"),
        (TableRef("analytics", "t\x00u", ""), 'analytics."t\\u0000u"', "a name holds a control character"),
    ],
    ids=["table-dot", "empty", "control"])
def test_the_detail_of_a_dbt_table_shows_the_name_and_says_why(ref, shown, reason):
    failure = invalid_contract_reads(_dbt_node("governed"), [ref], POSTGRES_SYSTEM)

    assert failure.detail == f"compiled SQL: table {shown} cannot be listed as <schema>.<table>: {reason}"


def test_every_sql_table_the_library_lists_passes_the_dbt_check_and_every_one_it_refuses_fails_it():
    """The check agrees with normalise_dbt on the tables the SQL names, as it does with
    normalise_python for a Python node."""
    node = _dbt_node("dormant_model")
    for schema, table in _TABLE_CASES:
        library_accepts = _library_lists(
            lambda tables: normalise_dbt(node.contract.document, reads=tables), [f"{schema}.{table}"])
        failure = invalid_contract_reads(node, [TableRef(schema, table, "")], POSTGRES_SYSTEM)
        assert (failure is None) == library_accepts, (schema, table)


def test_a_system_relation_a_dbt_node_names_is_not_checked():
    timings = release_timings()

    assert invalid_contract_reads(
        _dbt_node("governed"), [TableRef("pg_catalog", "a b", "")], POSTGRES_SYSTEM, timings=timings) is None
    assert timings.contract_failures == {}


def test_a_declared_read_that_cannot_be_listed_is_an_invalid_contract():
    """The library's validation accepts a contract whose meta declares a read it cannot list and its
    normalisation refuses it: that is the team's mistake, named with the entry, not a defect."""
    node = _with_declared_reads("analytics.fine", "orders", "analytics.a table", "a.b.c", "orders")
    assert validate.validate_dbt_contract(node.contract.document) == []
    with pytest.raises(HashInputError):
        normalise_dbt(node.contract.document, reads=[])
    timings = release_timings()

    failure = invalid_contract_reads(node, [], POSTGRES_SYSTEM, timings=timings)

    assert failure == FailedNode(
        node_id="analytics.governed", kind=ParseFailureKind.INVALID_CONTRACT, service="cases",
        file_path="models/tagged/governed.sql", node_type=NodeType.DBT_MODEL,
        detail=(
            'reads: "a.b.c" cannot be listed as <schema>.<table>: a name holds a dot; '
            'reads: "analytics.a table" cannot be listed as <schema>.<table>: a name holds whitespace; '
            'reads: "orders" cannot be listed as <schema>.<table>: the entry has no dot'))
    assert timings.contract_failures == {("normalise", "value"): 3}


def test_a_declared_read_is_checked_even_when_it_names_a_system_relation():
    """normalise_dbt lists every declared read, system relation or not, so the check does too."""
    failure = invalid_contract_reads(_with_declared_reads("pg_catalog.a b"), [], POSTGRES_SYSTEM)

    assert failure.detail == 'reads: "pg_catalog.a b" cannot be listed as <schema>.<table>: a name holds whitespace'


def test_the_unlistable_sql_tables_of_a_dbt_node_come_before_its_unlistable_declared_reads():
    timings = release_timings()

    failure = invalid_contract_reads(
        _with_declared_reads("a b.c"), [TableRef("analytics", "d e", "")], POSTGRES_SYSTEM, timings=timings)

    assert failure.detail == (
        'compiled SQL: table analytics."d e" cannot be listed as <schema>.<table>: a name holds whitespace; '
        'reads: "a b.c" cannot be listed as <schema>.<table>: a name holds whitespace')
    assert timings.contract_failures == {("normalise", "value"): 2}


def test_a_node_with_listable_declared_reads_and_tables_has_no_failure():
    assert invalid_contract_reads(
        _with_declared_reads("analytics.orders", "Raw.Events"), [TableRef("analytics", "orders", "")],
        POSTGRES_SYSTEM) is None


@pytest.mark.parametrize(
    "entry",
    list(dict.fromkeys(
        [f"{schema}.{table}" for schema, table in _TABLE_CASES]
        + ["orders", "", ".", "..", "a..b", ".t", "s.", "a.b.c.d", "Analytics.Orders", "a b", "a.b\u2028", "x\x85.y"])),
    ids=repr)
def test_a_declared_read_passes_the_check_exactly_when_the_library_lists_it(entry):
    """The check is the library's rule for a listed table, applied to the entry as written: it
    refuses an entry exactly when normalise_dbt does, so an entry it passes never reaches a
    HashInputError and an entry it refuses is one the library refuses."""
    node = _with_declared_reads(entry)

    library_accepts = _library_lists(lambda tables: normalise_dbt(node.contract.document, reads=tables), [])
    failure = invalid_contract_reads(node, [], POSTGRES_SYSTEM)

    assert (failure is None) == library_accepts
    if failure is not None:
        assert failure.detail.startswith("reads: ")


# --- the bundle -----------------------------------------------------------------

def _stored(unique_id: str, *, service: str = "service-py", family: str = "python") -> StoredContract:
    return StoredContract(
        unique_id=unique_id, family=family, service=service, contract_hash="sha256:" + "a" * 64, content_hash="sha256:" + "b" * 64,
        source_path=f"contracts/{unique_id.split('.')[1]}.odcs.yaml", source_sha256="c" * 64,
        document={"id": unique_id})


def _bundle(contracts, **overrides):
    args = {
        "tenant_id": "default", "release_id": "rel-1", "library_version": "0.3.1", "contracts": contracts,
        "provenance": {}, "timings": {"stages_ms": {}, "nodes": {}, "columns": {}, "validation_cache": {}},
    }
    args.update(overrides)
    return build_contract_bundle(**args)


@pytest.mark.parametrize("families, want", [
    ([], {}),
    (["python", "python"], {"python": 2}),
    (["python", "dbt", "dbt"], {"dbt": 2, "python": 1}),
    (["dbt", "python", "dbt"], {"dbt": 2, "python": 1}),
], ids=["none", "one-family", "both", "both-other-order"])
def test_contracts_by_family_counts_each_family_and_leaves_out_one_with_none(families, want):
    contracts = [_stored(f"analytics.t{i}", family=family) for i, family in enumerate(families)]

    got = contracts_by_family(contracts)

    assert got == want
    assert list(got) == sorted(got)
    assert _bundle(contracts)["summary"]["by_family"] == got


def test_a_release_with_no_contract_has_an_empty_bundle():
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
    doc = _bundle([], library_version="0.3.1")

    assert doc["continuo_contracts"] == "0.3.1"
    assert doc["odcs_schema_sha256"] == SCHEMA_SHA256
    assert (doc["tenant_id"], doc["release_id"]) == ("default", "rel-1")


def test_the_bundle_is_the_same_whatever_the_order_of_the_contracts():
    one = _bundle([_stored("analytics.a"), _stored("analytics.b")])
    other = _bundle([_stored("analytics.b"), _stored("analytics.a")])

    assert canonical_json(one) == canonical_json(other)


def test_two_contracts_of_one_unique_id_reaching_the_builder_are_a_bug_not_an_overwrite():
    """The handler leaves out every contract of an id shared_unique_ids names, so two
    contracts of one id never reach the builder; if they did, it refuses them."""
    with pytest.raises(ValueError, match="analytics.a"):
        _bundle([_stored("analytics.a", service="s1"), _stored("analytics.a", service="s2")])


def test_the_bundle_counts_both_families():
    governed = _dbt_node("governed")
    kpis = _kpis_node()
    contracts = store_contracts([kpis, governed], {}, POSTGRES_SYSTEM)

    doc = _bundle(contracts)

    assert doc["summary"] == {"contracts": 2, "by_family": {"dbt": 1, "python": 1}, "invalid": 0, "skipped": 0}
    [dbt_contract] = [c for c in contracts if c.family == "dbt"]
    assert doc["contracts"]["analytics.governed"] == {
        "family": "dbt", "service": "cases", "contract_hash": dbt_contract.contract_hash,
        "content_hash": governed.content_hash,
        "source": {"path": "models/tagged/governed.sql", "sha256": governed.contract.source_sha256},
        "document": dbt_contract.document}
    assert doc["contracts"]["analytics.py_daily_kpis"]["family"] == "python"
    assert doc["contracts"]["analytics.py_daily_kpis"]["source"]["path"] == "contracts/py_daily_kpis.odcs.yaml"


def test_a_bundle_of_one_family_counts_only_that_family():
    dbt_only = _bundle(store_contracts([_dbt_node("governed"), _dbt_node("dormant_model")], {}, POSTGRES_SYSTEM))
    python_only = _bundle(store_contracts([_kpis_node()], {}, POSTGRES_SYSTEM))

    assert dbt_only["summary"]["by_family"] == {"dbt": 2}
    assert python_only["summary"]["by_family"] == {"python": 1}


def test_a_contract_of_an_unknown_family_does_not_reach_the_bundle():
    with pytest.raises(ValueError, match="plsql"):
        _bundle([_stored("analytics.a", family="plsql")])


@pytest.mark.parametrize("other_service", ["cases", "billing"], ids=["one-service", "two-services"])
def test_two_dbt_nodes_with_one_contract_id_leave_the_bundle(other_service):
    """An id two dbt nodes declare, in one service or two, is release-controller's duplicate_table to
    reject: shared_unique_ids names it, the caller leaves both contracts out and the bundle holds only
    the other nodes'. A builder given both contracts refuses them."""
    first = _dbt_node("governed")
    second = _dbt_node("governed", edit=lambda node: node.__setitem__("fqn", [other_service, "tagged", "governed"]))
    unrelated = _dbt_node("dormant_model")
    nodes = [first, second, unrelated]

    shared = shared_unique_ids(nodes)

    assert shared == {"analytics.governed": sorted(["cases", other_service])}
    bundle = _bundle(store_contracts([node for node in nodes if node.unique_id not in shared], {}, POSTGRES_SYSTEM))
    assert sorted(bundle["contracts"]) == ["analytics.dormant"]
    assert bundle["summary"] == {"contracts": 1, "by_family": {"dbt": 1}, "invalid": 0, "skipped": 0}
    with pytest.raises(ValueError, match="analytics.governed"):
        _bundle(store_contracts([first, second], {}, POSTGRES_SYSTEM))


def test_shared_unique_ids_names_each_id_two_services_declare_with_its_services_in_order():
    def node(service, table, node_type=NodeType.PYTHON_NODE):
        return ManifestNode(
            table_name=table, schema_name="analytics", service_name=service, owner="t", schedule_name="daily",
            criticality="CORE", node_type=node_type)

    nodes = [node("s2", "a"), node("s1", "a"), node("s1", "b"), node("s3", "A")]

    assert shared_unique_ids(nodes) == {"analytics.a": ["s1", "s2", "s3"]}
    assert shared_unique_ids(list(reversed(nodes))) == {"analytics.a": ["s1", "s2", "s3"]}
    assert shared_unique_ids([node("s1", "b")]) == {}


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
