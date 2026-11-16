import collections
import dataclasses
import hashlib
import importlib.resources
import json
from pathlib import Path

import pytest
import yaml
from continuo_contracts import yaml_strict
from continuo_contracts.contract import contract_hash, normalise_python

from domain.contract_vocabulary import NodeType, ParseFailureKind
from domain.exceptions import InvalidOdcsBundleError, MalformedContractError
from domain.model import FailedNode, Runtime
from service import odcs_bundle_parser
from service.contract_validation import ContractValidator
from service.odcs_bundle_parser import parse_odcs_bundle
from service.python_contract_parser import parse_python_contract
from service.release_timings import Stage
from tests.fakes import python_contract
from tests.odcs_fixtures import (
    DEMO_DIR,
    DEMO_NAMES,
    IMAGE_TAG,
    SERVICE,
    bundle_doc,
    bundle_file,
    bundle_node,
    demo_node,
    demo_nodes,
    demo_text,
    release_timings,
    write_bundle,
)

KPIS_PATH = "contracts/py_daily_kpis.odcs.yaml"
KPIS_UNDER_SCHEMA_NAME_PATH = "contracts/analytics.py_daily_kpis.odcs.yaml"
CSV_PATH = "contracts/demo_orders_csv.odcs.yaml"
FX_PATH = "contracts/fx_eur_usd_rates.odcs.yaml"

KPIS_WITH_NUMERIC_12 = bundle_node(KPIS_PATH, demo_text("py_daily_kpis").replace("NUMERIC(12,2)", "NUMERIC(12)"))
CSV_WITH_SERVERS = bundle_node(
    CSV_PATH,
    demo_text("demo_orders_csv")
    + "servers:\n  - {server: warehouse, type: postgres, host: db, port: 5432, database: d, schema: analytics}\n",
)


def parse(mf, **overrides):
    """parse_odcs_bundle with a fresh validator, the library at 0.2.0, no progress watcher and a
    fresh timing record, each replaced by the keyword of that name."""
    arguments = {
        "validator": ContractValidator(),
        "library_version": "0.2.0",
        "progress": lambda: None,
        "timings": release_timings(),
    }
    arguments.update(overrides)
    return parse_odcs_bundle(mf, **arguments)


def test_the_demo_bundle_parses(tmp_path):
    parsed = parse(bundle_file(tmp_path, demo_nodes()))
    assert [n.unique_id for n in parsed.nodes] == ["analytics.demo_orders_csv", "analytics.fx_eur_usd_rates", "analytics.py_daily_kpis"]
    kpis = parsed.nodes[2]
    assert kpis.dependency_sqls == ["select created_at, amount_eur from analytics.daily_transactions"]
    assert json.loads(kpis.raw_code)["owner"] == "service-py"
    assert kpis.contract.path == "contracts/py_daily_kpis.odcs.yaml"
    assert parsed.failed == ()
    assert parsed.python_provenance == {"continuo_python_runtime": "0.13.0", "image_packages": {"pyarrow": "26.0.0"}}


def test_a_node_carries_what_its_contract_declares(tmp_path):
    parsed = parse(bundle_file(tmp_path, demo_nodes()))
    csv, fx, kpis = parsed.nodes
    assert [n.node_type for n in parsed.nodes] == [NodeType.PYTHON_CSV, NodeType.PYTHON_API, NodeType.PYTHON_NODE]
    assert {n.runtime for n in parsed.nodes} == {Runtime.PYTHON}
    assert {(n.service_name, n.image_tag, n.owner, n.schedule_name, n.criticality) for n in parsed.nodes} == {
        (SERVICE, IMAGE_TAG, "service-py", "daily", "SECONDARY")
    }
    assert csv.csv_source == "s3://continuo-dev/static-files/demo/orders.csv"
    assert csv.dependency_sqls == [] and csv.original_file_path == ""
    assert fx.secret_ref == "continuo-api-demo-fx" and fx.original_file_path == "scripts/fx_eur_usd_rates.py"
    assert [c["name"] for c in kpis.output_columns] == ["day", "tx_count", "total_amount"]
    assert json.loads(kpis.raw_code)["description"].startswith("Daily transaction count")


def test_a_node_keeps_its_authored_contract(tmp_path):
    text = demo_text("py_daily_kpis")
    parsed = parse(bundle_file(tmp_path, demo_nodes()))
    kpis = parsed.nodes[2]
    assert kpis.contract.document == yaml_strict.load(text)
    assert kpis.contract.service == SERVICE
    assert kpis.contract.source_sha256 == hashlib.sha256(text.encode("utf-8")).hexdigest()
    assert parsed.contracts == tuple(node.contract for node in parsed.nodes)


def test_the_demo_content_hash_equals_the_wire_parsers(tmp_path):
    odcs = {n.unique_id: n for n in parse(bundle_file(tmp_path, demo_nodes())).nodes}
    wire = {n.unique_id: n for n in _wire_nodes(tmp_path)}
    assert {uid: n.content_hash for uid, n in odcs.items()} == {uid: n.content_hash for uid, n in wire.items()}
    # Nothing else about a converted node moves either, so it writes no new code version.
    assert {uid: dataclasses.replace(n, contract=None) for uid, n in odcs.items()} == wire


def _wire_nodes(tmp_path):
    """The old entries of the demo through the wire parser, with the CI hash parts the bundle's nodes carry.
    The runtime's merge wrote `script` and `reads` on every wire entry, empty when the kind has none."""
    entries = []
    for name in DEMO_NAMES:
        (entry,) = yaml.safe_load((DEMO_DIR / "old" / f"{name}.yml").read_text())["nodes"]
        parts = demo_node(name)
        entry.setdefault("script", "")
        entry.setdefault("reads", {})
        entry.update(
            source_hash=parts["source_hash"], shared_code_hash=parts["shared_code_hash"],
            config_hash="ci-placeholder", content_hash="ci-placeholder",
        )
        entries.append(entry)
    return parse_python_contract(python_contract(tmp_path, *entries), IMAGE_TAG).nodes


def test_every_invalid_node_is_named(tmp_path):
    timings = release_timings()
    parsed = parse(bundle_file(tmp_path, [KPIS_WITH_NUMERIC_12, CSV_WITH_SERVERS, demo_node("fx_eur_usd_rates")]), timings=timings)
    assert [n.unique_id for n in parsed.nodes] == ["analytics.fx_eur_usd_rates"]
    assert [(f.node_id, f.kind, f.file_path) for f in parsed.failed] == [
        ("analytics.demo_orders_csv", ParseFailureKind.INVALID_CONTRACT, "contracts/demo_orders_csv.odcs.yaml"),
        ("analytics.py_daily_kpis", ParseFailureKind.INVALID_CONTRACT, "contracts/py_daily_kpis.odcs.yaml")]
    assert parsed.failed[1].detail == "schema[0].properties[2].physicalType: 'NUMERIC(12)' is not a type continuo accepts"
    assert all(isinstance(f, FailedNode) and f.service == SERVICE for f in parsed.failed)
    assert [f.node_type for f in parsed.failed] == ["python-csv", "python-node"]
    assert timings.contract_failures == {("validate", "column_type"): 1, ("validate", "refused_field"): 1}


def test_a_failed_node_joins_every_issue_of_its_contract(tmp_path):
    text = demo_text("py_daily_kpis").replace("NUMERIC(12,2)", "NUMERIC(12)").replace("value: daily\n", "value: daily weekly\n")
    text = text.replace("{vendor: continuo, property: schedule, value: daily}", "{vendor: continuo, property: schedule, value: bad schedule}")
    timings = release_timings()
    (failed,) = parse(bundle_file(tmp_path, [bundle_node(KPIS_PATH, text)]), timings=timings).failed
    issues = failed.detail.split("; ")
    assert len(issues) == 2
    assert issues[0].startswith("customProperties[0]: ") and issues[1].startswith("schema[0].properties[2].physicalType: ")
    assert timings.contract_failures == {("validate", "value"): 1, ("validate", "column_type"): 1}


@pytest.mark.parametrize("id_line", ["", "id: 42\n", "id: \" \"\n", "id: [a, b]\n"])
def test_a_failed_node_whose_id_cannot_be_read_is_named_by_its_path(tmp_path, id_line):
    text = demo_text("py_daily_kpis").replace("id: analytics.py_daily_kpis\n", id_line)
    (failed,) = parse(bundle_file(tmp_path, [bundle_node(KPIS_PATH, text)])).failed
    assert failed.node_id == KPIS_PATH


def test_a_failed_node_with_a_kind_no_python_node_has_is_a_python_node(tmp_path):
    odd_kind = demo_text("demo_orders_csv").replace("value: python-csv", "value: spark")
    (failed,) = parse(bundle_file(tmp_path, [bundle_node(CSV_PATH, odd_kind)])).failed
    assert (failed.node_id, failed.node_type) == ("analytics.demo_orders_csv", "python-node")


def test_a_text_that_is_not_a_mapping_is_named_by_its_path(tmp_path):
    (failed,) = parse(bundle_file(tmp_path, [bundle_node(KPIS_PATH, "- a\n- list\n")])).failed
    assert (failed.node_id, failed.node_type, failed.kind) == (KPIS_PATH, "python-node", ParseFailureKind.INVALID_CONTRACT)
    assert failed.detail.startswith("$: ")


def test_a_strict_yaml_error_is_one_issue(tmp_path):
    text = demo_text("py_daily_kpis").replace("name: py_daily_kpis\n", "name: py_daily_kpis\nname: other\n", 1)
    timings = release_timings()
    (failed,) = parse(bundle_file(tmp_path, [bundle_node(KPIS_PATH, text)]), timings=timings).failed
    assert failed.node_id == KPIS_PATH
    assert failed.detail == "$: line 5, column 1: duplicate key 'name'"
    assert timings.contract_failures == {("validate", "strict_yaml"): 1}
    assert timings.validation_cache == {"hit": 0, "miss": 0}


@pytest.mark.parametrize("producer, library", [
    ("0.1.9", "0.2.0"),
    ("0.2.0", "0.2.0"),
    ("0.9.0", "0.10.0"),
    ("0.99.99", "1.0.0"),
])
def test_an_older_or_the_same_library_is_accepted(tmp_path, producer, library):
    parsed = parse(bundle_file(tmp_path, demo_nodes(), producer_contracts=producer), library_version=library)
    assert len(parsed.nodes) == 3


def test_a_newer_library_is_invalid_artifact(tmp_path):
    with pytest.raises(InvalidOdcsBundleError, match="the bundle was written by continuo-contracts 0.3.0 and this install runs 0.2.0: upgrade continuo, or pin an older runtime"):
        parse(bundle_file(tmp_path, demo_nodes(), producer_contracts="0.3.0"))


@pytest.mark.parametrize("producer, library", [("0.10.0", "0.9.0"), ("1.0.0", "0.99.99"), ("0.2.1", "0.2.0")])
def test_versions_are_compared_as_numbers_not_as_text(tmp_path, producer, library):
    with pytest.raises(InvalidOdcsBundleError, match="the bundle was written by continuo-contracts"):
        parse(bundle_file(tmp_path, demo_nodes(), producer_contracts=producer), library_version=library)


@pytest.mark.parametrize("version", ["0.3", "0.3.0rc1", "v0.3.0", "0.3.0.1", "0.-3.0", "٠.٢.٠", "1234567890.0.0"])
def test_a_producer_version_that_is_not_x_y_z_is_refused(tmp_path, version):
    with pytest.raises(InvalidOdcsBundleError, match="is not a version"):
        parse(bundle_file(tmp_path, demo_nodes(), producer_contracts=version))


def _raw(tmp_path, data: bytes):
    """A bundle file that holds exactly `data`."""
    mf = write_bundle(tmp_path, {})
    Path(mf.path).write_bytes(data)
    return mf


def _doc(tmp_path, **changes):
    doc = bundle_doc(demo_nodes())
    for key, value in changes.items():
        if value is KeyError:
            del doc[key]
        else:
            doc[key] = value
    return write_bundle(tmp_path, doc)


def _nodes_doc(tmp_path, nodes):
    return write_bundle(tmp_path, bundle_doc(nodes))


def _node_without(key):
    node = demo_node("py_daily_kpis")
    del node[key]
    return node


@pytest.mark.parametrize("build, message", [
    (lambda p: _raw(p, b"{not json"), "the bundle is not valid JSON"),
    (lambda p: _raw(p, b""), "the bundle is not valid JSON"),
    (lambda p: _raw(p, b"\xff\xfe\x00"), "the bundle is not UTF-8 text"),
    (lambda p: _raw(p, b'["a"]'), "the bundle must be a JSON object, got list"),
    (lambda p: _raw(p, b"null"), "the bundle must be a JSON object, got NoneType"),
    (lambda p: _doc(p, bundle_version=2), "bundle_version 2 is newer than this install reads"),
    (lambda p: _doc(p, bundle_version=0), "unsupported bundle_version 0"),
    (lambda p: _doc(p, bundle_version="1"), "unsupported bundle_version '1'"),
    (lambda p: _doc(p, bundle_version=True), "unsupported bundle_version True"),
    (lambda p: _doc(p, bundle_version=KeyError), "the bundle is missing ['bundle_version']"),
    (lambda p: _doc(p, service="other-service"), "the bundle is for service 'other-service', not 'service-py'"),
    (lambda p: _doc(p, service=""), "service must be a non-empty string"),
    (lambda p: _doc(p, service=7), "service must be a non-empty string"),
    (lambda p: _doc(p, producer=KeyError), "the bundle is missing ['producer']"),
    (lambda p: _doc(p, producer="0.2.0"), "producer must be an object"),
    (lambda p: _doc(p, producer={"continuo_python_runtime": "0.13.0"}), "producer is missing ['continuo_contracts']"),
    (lambda p: _doc(p, producer={"continuo_python_runtime": "0.13.0", "continuo_contracts": 2}), "producer.continuo_contracts must be a non-empty string"),
    (lambda p: _doc(p, producer={"continuo_python_runtime": "", "continuo_contracts": "0.2.0"}), "producer.continuo_python_runtime must be a non-empty string"),
    (lambda p: _doc(p, producer={"continuo_python_runtime": "0.13.0", "continuo_contracts": ""}), "producer.continuo_contracts must be a non-empty string"),
    (lambda p: _doc(p, producer={"continuo_python_runtime": "0.13.0", "continuo_contracts": "0.2.0", "x": 1}), "producer has unknown keys ['x']"),
    (lambda p: _doc(p, image_packages=KeyError), "the bundle is missing ['image_packages']"),
    (lambda p: _doc(p, image_packages=["a"]), "image_packages must be an object of package name to version string"),
    (lambda p: _doc(p, image_packages={"a": 1}), "image_packages must be an object of package name to version string"),
    (lambda p: _doc(p, nodes=KeyError), "the bundle is missing ['nodes']"),
    (lambda p: _doc(p, nodes={"a": 1}), "nodes must be a list"),
    (lambda p: _doc(p, extra=1), "the bundle has unknown keys ['extra']"),
    (lambda p: _nodes_doc(p, ["text"]), "nodes[0] must be an object"),
    (lambda p: _nodes_doc(p, [_node_without("text")]), "nodes[0] is missing ['text']"),
    (lambda p: _nodes_doc(p, [{**demo_node("py_daily_kpis"), "extra": 1}]), "nodes[0] has unknown keys ['extra']"),
    (lambda p: _nodes_doc(p, [{**demo_node("py_daily_kpis"), "text": 5}]), "nodes[0].text must be a string"),
    (lambda p: _nodes_doc(p, [{**demo_node("py_daily_kpis"), "source_hash": ""}]), "nodes[0].source_hash must be a non-empty string"),
    (lambda p: _nodes_doc(p, [{**demo_node("py_daily_kpis"), "shared_code_hash": None}]), "nodes[0].shared_code_hash must be a string"),
    (lambda p: _nodes_doc(p, [demo_node("py_daily_kpis"), demo_node("py_daily_kpis")]), "two nodes have the path contracts/py_daily_kpis.odcs.yaml"),
    (lambda p: _nodes_doc(p, [demo_node("py_daily_kpis"), bundle_node("contracts/other.yml", "a: b")]),
     "nodes[1].path 'contracts/other.yml' does not end in .odcs.yaml"),
    (lambda p: _nodes_doc(p, [bundle_node("contracts/other.yaml", "a: b")]),
     "nodes[0].path 'contracts/other.yaml' does not end in .odcs.yaml"),
])
def test_an_unreadable_bundle_is_invalid_artifact(tmp_path, build, message):
    with pytest.raises(InvalidOdcsBundleError) as excinfo:
        parse(build(tmp_path))
    assert message in str(excinfo.value)
    assert message in excinfo.value.detail


def test_a_lone_surrogate_in_a_hash_part_is_refused(tmp_path):
    # JSON can spell one as an escape, which no UTF-8 text can hold
    doc = bundle_doc([{**demo_node("py_daily_kpis"), "source_hash": "\ud800"}])
    mf = _raw(tmp_path, json.dumps(doc).encode("ascii"))
    with pytest.raises(InvalidOdcsBundleError, match=r"nodes\[0\]\.source_hash must be text UTF-8 can encode"):
        parse(mf)


def test_a_bundle_that_names_no_service_is_read_as_the_service_it_names(tmp_path):
    parsed = parse(bundle_file(tmp_path, demo_nodes(), declared_service=""))
    assert {n.service_name for n in parsed.nodes} == {SERVICE}


def test_an_empty_node_list_is_an_empty_artifact(tmp_path):
    parsed = parse(bundle_file(tmp_path, []))
    assert (parsed.nodes, parsed.failed, parsed.contracts, parsed.shared_code) == ([], (), (), {})


def test_two_contracts_with_one_id_are_invalid_artifact(tmp_path):
    # py_daily_kpis.odcs.yaml and analytics.py_daily_kpis.odcs.yaml, both id analytics.py_daily_kpis and both valid
    kpis, under_schema_name = demo_node("py_daily_kpis"), bundle_node(KPIS_UNDER_SCHEMA_NAME_PATH, demo_text("py_daily_kpis"))
    with pytest.raises(InvalidOdcsBundleError, match=r"two contracts declare analytics\.py_daily_kpis: contracts/analytics\.py_daily_kpis\.odcs\.yaml and contracts/py_daily_kpis\.odcs\.yaml"):
        parse(bundle_file(tmp_path, [kpis, under_schema_name]))


def test_every_id_two_contracts_share_is_named_in_id_order(tmp_path):
    nodes = [
        demo_node("py_daily_kpis"), bundle_node(KPIS_UNDER_SCHEMA_NAME_PATH, demo_text("py_daily_kpis")),
        demo_node("demo_orders_csv"), bundle_node("contracts/analytics.demo_orders_csv.odcs.yaml", demo_text("demo_orders_csv")),
        demo_node("fx_eur_usd_rates"),
    ]
    with pytest.raises(InvalidOdcsBundleError) as excinfo:
        parse(bundle_file(tmp_path, nodes))
    assert excinfo.value.detail == (
        "two contracts declare analytics.demo_orders_csv: contracts/analytics.demo_orders_csv.odcs.yaml"
        " and contracts/demo_orders_csv.odcs.yaml; "
        "two contracts declare analytics.py_daily_kpis: contracts/analytics.py_daily_kpis.odcs.yaml"
        " and contracts/py_daily_kpis.odcs.yaml")


def test_three_contracts_with_one_id_are_all_named(tmp_path):
    paths = [KPIS_PATH, KPIS_UNDER_SCHEMA_NAME_PATH]
    third = bundle_node("contracts/z/py_daily_kpis.odcs.yaml", demo_text("py_daily_kpis"))
    nodes = [bundle_node(path, demo_text("py_daily_kpis")) for path in paths] + [third]
    with pytest.raises(InvalidOdcsBundleError) as excinfo:
        parse(bundle_file(tmp_path, nodes))
    assert excinfo.value.detail == (
        "3 contracts declare analytics.py_daily_kpis: contracts/analytics.py_daily_kpis.odcs.yaml,"
        " contracts/py_daily_kpis.odcs.yaml and contracts/z/py_daily_kpis.odcs.yaml")


def test_a_valid_contract_and_an_invalid_one_with_one_id_are_not_a_bundle_error(tmp_path):
    parsed = parse(bundle_file(tmp_path, [demo_node("py_daily_kpis"), bundle_node("contracts/kpis.odcs.yaml", demo_text("py_daily_kpis"))]))
    assert [n.unique_id for n in parsed.nodes] == ["analytics.py_daily_kpis"]
    assert [(f.node_id, f.file_path) for f in parsed.failed] == [("analytics.py_daily_kpis", "contracts/kpis.odcs.yaml")]


def test_the_ids_of_two_contracts_are_compared_in_lower_case(tmp_path):
    # The library lowers only ASCII letters, so `É` and `é` make two valid ids that name one node.
    upper, lower = ("contracts/%s.odcs.yaml" % table for table in ("Éa", "éa"))
    texts = [demo_text("py_daily_kpis").replace("py_daily_kpis", table) for table in ("Éa", "éa")]
    with pytest.raises(InvalidOdcsBundleError) as excinfo:
        parse(bundle_file(tmp_path, [bundle_node(upper, texts[0]), bundle_node(lower, texts[1])]))
    assert excinfo.value.detail == f"two contracts declare analytics.éa: {upper} and {lower}"


def test_the_order_of_the_bundle_does_not_change_what_the_parse_returns(tmp_path):
    nodes = [KPIS_WITH_NUMERIC_12, CSV_WITH_SERVERS, demo_node("fx_eur_usd_rates")]
    one = parse(bundle_file(tmp_path, nodes))
    other = parse(bundle_file(tmp_path, list(reversed(nodes))))
    assert (one.nodes, one.failed, one.contracts) == (other.nodes, other.failed, other.contracts)
    valid = demo_nodes()
    assert parse(bundle_file(tmp_path, valid)).nodes == parse(bundle_file(tmp_path, list(reversed(valid)))).nodes


def test_progress_runs_once_per_node(tmp_path):
    calls = []
    parse(bundle_file(tmp_path, demo_nodes()), progress=lambda: calls.append(1))
    assert len(calls) == 3


def test_progress_runs_after_an_invalid_node_too(tmp_path):
    calls = []
    parsed = parse(bundle_file(tmp_path, [KPIS_WITH_NUMERIC_12, CSV_WITH_SERVERS, demo_node("fx_eur_usd_rates")]),
                   progress=lambda: calls.append(1))
    assert len(calls) == 3 and len(parsed.failed) == 2


def test_contract_validation_is_timed_inside_the_parse(tmp_path):
    timings = release_timings()
    parse(bundle_file(tmp_path, demo_nodes()), timings=timings)
    record = timings.stages[Stage.CONTRACT_VALIDATION]
    assert (record.runs, record.counts) == (1, {"nodes": 3})
    assert timings.validation_cache == {"hit": 0, "miss": 3}


def test_a_second_parse_of_the_same_contracts_is_answered_from_the_cache(tmp_path):
    validator, second = ContractValidator(), release_timings()
    parse(bundle_file(tmp_path, demo_nodes()), validator=validator)
    parse(bundle_file(tmp_path, demo_nodes()), validator=validator, timings=second)
    assert second.validation_cache == {"hit": 3, "miss": 0}


def test_a_contract_the_node_builder_refuses_after_the_checks_is_one_failed_node(tmp_path, monkeypatch):
    def refuse(*args, **kwargs):
        raise MalformedContractError("the projection refused the node")

    monkeypatch.setattr(odcs_bundle_parser, "build_python_node", refuse)
    timings = release_timings()
    parsed = parse(bundle_file(tmp_path, [demo_node("py_daily_kpis")]), timings=timings)
    assert parsed.nodes == []
    assert [(f.node_id, f.detail) for f in parsed.failed] == [
        ("analytics.py_daily_kpis", "$: Malformed python contract: the projection refused the node")]
    assert timings.contract_failures == {("validate", "value"): 1}


def _vector_node(tmp_path, name):
    case = next(case for case in _vector_cases() if case["name"] == name)
    mf = bundle_file(tmp_path, [bundle_node(case["path"], case["text"])], service=case["service"], declared_service=case["service"])
    (node,) = parse(mf).nodes
    return node


def test_an_upsert_node_keeps_its_key_in_the_order_of_its_positions_and_its_timeout(tmp_path):
    node = _vector_node(tmp_path, "full_python_node")
    entry = json.loads(node.raw_code)
    assert (entry["mode_type"], entry["primary_key"], entry["assets"]) == (
        "upsert", ["order_id", "line_no"], ["assets/currencies.csv", "assets/rounding.json"])
    assert entry["config"] == {"batch_size": 5000, "compression": "zstd", "tags": ["a", "b"]}
    assert (entry["extra_columns"], entry["criticality"], node.timeout_seconds) == ("warn", "CORE", 120 * 60)
    # the upstream queries come in the order of the read names, `orders` then `rates`
    assert node.dependency_sqls == [
        "select order_id, line_no, customer_email, amount, seen_at from analytics.orders",
        "select currency, rate_to_eur from analytics.fx_rates",
    ]


def test_a_node_that_does_not_upsert_writes_no_key_though_its_columns_are_marked(tmp_path):
    node = _vector_node(tmp_path, "full_python_api")
    entry = json.loads(node.raw_code)
    assert entry["mode_type"] == "append" and "primary_key" not in entry
    assert json.loads(node.projection)["primary_key"] == []
    assert (node.secret_ref, node.timeout_seconds) == ("continuo-api-rates", 60 * 60)


def test_a_csv_node_reads_its_file_and_runs_no_script(tmp_path):
    node = _vector_node(tmp_path, "full_python_csv")
    assert (node.csv_source, node.dependency_sqls, node.original_file_path, node.timeout_seconds) == (
        "s3://continuo-dev/static-files/full/orders.csv", [], "", 30 * 60)
    assert "script" not in json.loads(node.raw_code)


def _vector_cases():
    text = importlib.resources.files("continuo_contracts").joinpath("vectors", "contracts.json").read_text(encoding="utf-8")
    return json.loads(text)


@pytest.mark.parametrize("case", _vector_cases(), ids=lambda case: case["name"])
def test_the_library_vectors_pass_through_the_parser(tmp_path, case):
    # every case of continuo_contracts/vectors/contracts.json, packed as one bundle node of its own:
    # a valid case parses and keeps its document, an invalid case fails with the vector's reasons
    timings = release_timings()
    mf = bundle_file(tmp_path, [bundle_node(case["path"], case["text"])], service=case["service"], declared_service=case["service"])
    parsed = parse(mf, timings=timings)
    if "issues" in case:
        assert parsed.nodes == [] and len(parsed.failed) == 1
        (failed,) = parsed.failed
        assert failed.kind == ParseFailureKind.INVALID_CONTRACT and failed.file_path == case["path"]
        reasons = collections.Counter(issue["reason"] for issue in case["issues"])
        assert timings.contract_failures == {("validate", reason): count for reason, count in reasons.items()}
        for issue in case["issues"]:
            assert f"{issue['path']}: " in failed.detail
        return
    assert parsed.failed == ()
    (node,) = parsed.nodes
    assert node.unique_id == case["stored"]["id"]
    assert node.contract.document == yaml_strict.load(case["text"])
    assert node.contract.source_sha256 == hashlib.sha256(case["text"].encode("utf-8")).hexdigest()
    assert contract_hash(normalise_python(node.contract.document, service=case["service"], reads=case["reads"])) == case["contract_hash"]
