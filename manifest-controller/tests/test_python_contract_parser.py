import datetime
import hashlib
import json

import pytest
import yaml

from domain.exceptions import MalformedContractError
from domain.model import NodeType, Runtime
from service.content_hash import content_hash_fold
from service.python_contract_parser import parse_python_contract


def make_entry(remove=(), **overrides):
    """A valid wire entry; overrides mutate it, remove drops keys entirely.

    content_hash defaults to the correct fold of the (possibly overridden)
    parts, so part mutations stay internally consistent unless a test
    overrides content_hash itself.
    """
    entry = {
        "schema": "analytics",
        "table": "table_test",
        "owner": "marketing",
        "schedule": "daily",
        "criticality": "SECONDARY",
        "script": "scripts/table_test.py",
        "reads": {
            "joined": "select a from analytics.table_a",
            "ids": "select id from analytics.table_b",
        },
        "output_columns": [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "amount", "type": "NUMERIC(10,2)"},
        ],
        "source_hash": "aaa111",
        "shared_code_hash": "bbb222",
        "config_hash": "ccc333",
    }
    entry.update(overrides)
    entry.setdefault(
        "content_hash",
        content_hash_fold(
            entry.get("source_hash", ""),
            entry.get("shared_code_hash", ""),
            entry.get("config_hash", ""),
        ),
    )
    for key in remove:
        entry.pop(key, None)
    return entry


def make_csv_entry(remove=(), **overrides):
    """A valid python-csv wire entry; overrides mutate it, remove drops keys.

    A csv node has no script and its sole read is {"csv": <uri>}. Its hash
    parts mirror what the runtime's merge computes for a csv node: source_hash
    over the uri bytes, shared_code_hash empty (no in-repo imports), and an
    opaque config_hash — MC verifies only that content_hash equals the fold of
    these three parts, never recomputing config_hash from the entry itself, so
    the fixture does not need to reproduce the runtime's canonical_entry.
    """
    uri = "s3://drops/orders.csv"
    entry = {
        "schema": "analytics",
        "table": "orders_csv",
        "owner": "team",
        "schedule": "daily",
        "criticality": "SECONDARY",
        "kind": "python-csv",
        "reads": {"csv": uri},
        "output_columns": [{"name": "order_id", "type": "INTEGER", "nullable": False}],
        "source_hash": hashlib.sha256(uri.encode()).hexdigest(),
        "shared_code_hash": "",
        "config_hash": "csvcfg000",
    }
    entry.update(overrides)
    entry.setdefault(
        "content_hash",
        content_hash_fold(
            entry.get("source_hash", ""),
            entry.get("shared_code_hash", ""),
            entry.get("config_hash", ""),
        ),
    )
    for key in remove:
        entry.pop(key, None)
    return entry


def write_contract(tmp_path, *entries, **doc_overrides):
    doc = {"contract_version": 1, "service": "marketing-py", "nodes": list(entries)}
    doc.update(doc_overrides)
    path = tmp_path / "contract.yaml"
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return str(path)


def test_happy_path_maps_every_manifest_node_field(tmp_path):
    entry = make_entry(
        config={"indexes": [{"columns": ["id"], "unique": True}]},
        description="test data",
        extra_columns="warn",
    )
    nodes, shared = parse_python_contract(write_contract(tmp_path, entry), "v1", "img:1")

    assert shared == {}
    (node,) = nodes
    assert node.table_name == "table_test"
    assert node.schema_name == "analytics"
    # A python contract has no alias concept: the resolved relation is always
    # the declared table.
    assert node.resolved_relation == "table_test"
    assert node.resolved_relation_id == "analytics.table_test"
    assert node.service_name == "marketing-py"
    assert node.owner == "marketing"
    assert node.schedule_name == "daily"
    assert node.criticality == "SECONDARY"
    assert node.node_type == "python-model"
    assert node.runtime == "python"
    assert node.candidate_sql == ""
    # dependency_sqls follow SORTED read names ("ids" < "joined") for determinism
    assert node.dependency_sqls == [
        "select id from analytics.table_b",
        "select a from analytics.table_a",
    ]
    assert node.output_columns == [
        {"name": "id", "type": "INTEGER", "nullable": False},
        {"name": "amount", "type": "NUMERIC(10,2)", "nullable": True},
    ]
    assert node.config == {"indexes": [{"columns": ["id"], "unique": True}]}
    assert node.original_file_path == "scripts/table_test.py"
    assert node.manifest_version == "v1"
    assert node.image_tag == "img:1"
    assert node.test_count == 0
    assert node.code_unit_ids == []
    assert node.upstream_deps == []
    assert node.source_hash == "aaa111"
    assert node.shared_code_hash == "bbb222"
    assert node.config_hash == "ccc333"
    assert node.content_hash == content_hash_fold("aaa111", "bbb222", "ccc333")


def test_raw_code_is_deterministic_pretty_json_without_hash_fields(tmp_path):
    nodes, _ = parse_python_contract(write_contract(tmp_path, make_entry()), "v1")
    raw = json.loads(nodes[0].raw_code)
    assert raw["reads"] == make_entry()["reads"]
    assert raw["output_columns"] == [
        {"name": "id", "type": "INTEGER", "nullable": False},
        {"name": "amount", "type": "NUMERIC(10,2)", "nullable": True},
    ]
    assert raw["config"] == {}
    assert raw["description"] == ""
    assert raw["extra_columns"] == "raise"
    assert not {"source_hash", "shared_code_hash", "config_hash", "content_hash"} & set(raw)
    assert nodes[0].raw_code == json.dumps(raw, sort_keys=True, indent=2)


def test_empty_reads_yield_no_dependency_sqls(tmp_path):
    nodes, _ = parse_python_contract(write_contract(tmp_path, make_entry(reads={})), "v1")
    assert nodes[0].dependency_sqls == []


def test_empty_nodes_list_yields_no_nodes(tmp_path):
    nodes, shared = parse_python_contract(write_contract(tmp_path), "v1")
    assert nodes == []
    assert shared == {}


def test_image_tag_defaults_to_empty(tmp_path):
    nodes, _ = parse_python_contract(write_contract(tmp_path, make_entry()), "v1")
    assert nodes[0].image_tag == ""


@pytest.mark.parametrize(
    "mutation, match",
    [
        ({"owner": ""}, "owner"),
        ({"schedule": "   "}, "schedule"),
        ({"criticality": "HIGH"}, "criticality"),
        ({"extra_columns": "ignore"}, "extra_columns"),
        ({"description": 7}, "description"),
        ({"reads": {"joined": "  "}}, "reads"),
        ({"reads": ["select 1"]}, "reads"),
        ({"output_columns": []}, "output_columns"),
        ({"output_columns": [{"name": "id"}]}, "missing"),
        ({"output_columns": [{"name": "id", "type": "INT", "nullable": "yes"}]}, "nullable"),
        ({"output_columns": [{"name": "id", "type": "INT", "sortkey": True}]}, "unknown"),
        ({"surprise_field": 1}, "unknown fields"),
        ({"config": "btree"}, "config"),
        ({"source_hash": ""}, "source_hash"),
        ({"shared_code_hash": None}, "shared_code_hash"),
        ({"content_hash": "sha256:doesnotmatchparts"}, "fold"),
    ],
)
def test_malformed_entries_fail_the_whole_artifact(tmp_path, mutation, match):
    entry = make_entry(**mutation)
    with pytest.raises(MalformedContractError, match=match):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


@pytest.mark.parametrize(
    "field",
    [
        "schema", "table", "owner", "schedule", "criticality", "script",
        "reads", "output_columns",
        "source_hash", "shared_code_hash", "config_hash", "content_hash",
    ],
)
def test_missing_required_field_fails(tmp_path, field):
    entry = make_entry(remove=(field,))
    with pytest.raises(MalformedContractError, match="missing"):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


def test_wrong_contract_version_fails(tmp_path):
    path = write_contract(tmp_path, make_entry(), contract_version=2)
    with pytest.raises(MalformedContractError, match="contract_version"):
        parse_python_contract(path, "v1")


def test_unknown_top_level_field_fails(tmp_path):
    path = write_contract(tmp_path, make_entry(), compiled_at="2026-08-07")
    with pytest.raises(MalformedContractError, match="unknown top-level"):
        parse_python_contract(path, "v1")


def test_empty_service_fails(tmp_path):
    path = write_contract(tmp_path, make_entry(), service="")
    with pytest.raises(MalformedContractError, match="service"):
        parse_python_contract(path, "v1")


def test_nodes_not_a_list_fails(tmp_path):
    path = write_contract(tmp_path, nodes="nope")
    with pytest.raises(MalformedContractError, match="nodes"):
        parse_python_contract(path, "v1")


def test_non_mapping_document_fails(tmp_path):
    path = tmp_path / "contract.yaml"
    path.write_text("- just\n- a\n- list\n")
    with pytest.raises(MalformedContractError, match="mapping"):
        parse_python_contract(str(path), "v1")


def test_duplicate_relation_fails(tmp_path):
    first = make_entry()
    second = make_entry(owner="finance")
    with pytest.raises(MalformedContractError, match="duplicate"):
        parse_python_contract(write_contract(tmp_path, first, second), "v1")


def test_invalid_yaml_fails_as_malformed_contract(tmp_path):
    path = tmp_path / "contract.yaml"
    path.write_text("nodes: [unclosed")
    with pytest.raises(MalformedContractError, match="yaml"):
        parse_python_contract(str(path), "v1")


def test_non_serializable_config_value_fails(tmp_path):
    entry = make_entry(config={"refresh_after": datetime.date(2026, 8, 7)})
    with pytest.raises(MalformedContractError, match="JSON-serializable"):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


def test_boolean_contract_version_fails(tmp_path):
    path = write_contract(tmp_path, make_entry(), contract_version=True)
    with pytest.raises(MalformedContractError, match="contract_version"):
        parse_python_contract(path, "v1")


def test_future_version_with_new_top_level_field_reports_the_version(tmp_path):
    path = write_contract(tmp_path, make_entry(), contract_version=2, compiled_at="x")
    with pytest.raises(MalformedContractError, match="contract_version"):
        parse_python_contract(path, "v1")


def test_non_mapping_node_entry_fails(tmp_path):
    path = write_contract(tmp_path, "oops")
    with pytest.raises(MalformedContractError, match="mapping"):
        parse_python_contract(path, "v1")


def test_non_mapping_column_fails(tmp_path):
    entry = make_entry(output_columns=["id"])
    with pytest.raises(MalformedContractError, match="mapping"):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


def test_duplicate_column_name_fails(tmp_path):
    entry = make_entry(output_columns=[
        {"name": "id", "type": "INTEGER"},
        {"name": "ID", "type": "TEXT"},
    ])
    with pytest.raises(MalformedContractError, match="duplicate column"):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


def test_composite_criticality_fails_as_contract_error(tmp_path):
    entry = make_entry(criticality=[])
    with pytest.raises(MalformedContractError, match="criticality"):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


def test_composite_extra_columns_fails_as_contract_error(tmp_path):
    entry = make_entry(extra_columns={})
    with pytest.raises(MalformedContractError, match="extra_columns"):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


def test_mixed_type_unknown_entry_keys_fail_as_contract_error(tmp_path):
    entry = make_entry()
    entry[1] = "x"
    entry["surprise"] = 2
    with pytest.raises(MalformedContractError, match="unknown fields"):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


def test_csv_entry_parses(tmp_path):
    nodes, _ = parse_python_contract(write_contract(tmp_path, make_csv_entry()), "v1")
    (node,) = nodes
    assert node.node_type == NodeType.PYTHON_CSV
    assert node.runtime == Runtime.PYTHON
    assert node.dependency_sqls == []
    assert node.csv_source == "s3://drops/orders.csv"
    assert node.original_file_path == ""


def test_csv_entry_hash_fields_verify(tmp_path):
    uri = "s3://drops/orders.csv"
    entry = make_csv_entry()
    nodes, _ = parse_python_contract(write_contract(tmp_path, entry), "v1")
    (node,) = nodes
    assert node.source_hash == hashlib.sha256(uri.encode()).hexdigest()
    assert node.shared_code_hash == ""
    assert node.content_hash == content_hash_fold(
        node.source_hash, node.shared_code_hash, entry["config_hash"]
    )


def test_csv_entry_with_script_rejected(tmp_path):
    entry = make_csv_entry(script="scripts/x.py")
    with pytest.raises(MalformedContractError, match="script"):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


def test_csv_entry_reads_must_be_csv_only(tmp_path):
    entry = make_csv_entry(reads={"csv": "s3://b/k", "o": "select 1"})
    with pytest.raises(MalformedContractError, match="exactly"):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


def test_csv_entry_reads_value_must_be_a_valid_uri(tmp_path):
    entry = make_csv_entry(reads={"csv": "ftp://bad"})
    with pytest.raises(MalformedContractError, match="csv"):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


def test_unknown_kind_rejected(tmp_path):
    entry = make_csv_entry(kind="python-parquet")
    with pytest.raises(MalformedContractError, match="kind"):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


def test_non_string_kind_rejected_cleanly(tmp_path):
    entry = make_csv_entry(kind=["python-csv"])
    with pytest.raises(MalformedContractError, match="kind"):
        parse_python_contract(write_contract(tmp_path, entry), "v1")


def test_model_entry_without_kind_unchanged(tmp_path):
    nodes, _ = parse_python_contract(write_contract(tmp_path, make_entry()), "v1")
    (node,) = nodes
    assert node.node_type == NodeType.PYTHON_MODEL
    assert node.csv_source == ""


def test_model_entry_with_explicit_kind_parses_and_hash_verifies(tmp_path):
    entry = make_entry(kind="python-model")
    nodes, _ = parse_python_contract(write_contract(tmp_path, entry), "v1")
    (node,) = nodes
    assert node.node_type == NodeType.PYTHON_MODEL
    assert node.content_hash == content_hash_fold("aaa111", "bbb222", "ccc333")
