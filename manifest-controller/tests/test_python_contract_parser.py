import json

import pytest
import yaml

from domain.exceptions import MalformedContractError
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
