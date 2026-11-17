import json

import pytest
from continuo_contracts.hashing import fold3
from continuo_contracts.python import PythonNode, projection_hash

from domain.contract_vocabulary import NodeType
from domain.exceptions import MalformedContractError
from domain.model import Runtime
from service.python_contract_parser import parse_python_contract
from service.python_node_builder import PythonNodeFields, build_python_node
from tests.test_python_contract_parser import make_csv_entry, make_entry, write_contract

_SOURCE_HASH = "aaa111"
_SHARED_CODE_HASH = "bbb222"


def _fields(**over) -> PythonNodeFields:
    base = dict(
        schema="analytics", table="table_test", owner="marketing", schedule="daily",
        criticality="SECONDARY", kind=NodeType.PYTHON_NODE, description="",
        script="scripts/table_test.py",
        reads={"joined": "select a from analytics.table_a", "ids": "select id from analytics.table_b"},
        dependency_sqls=["select id from analytics.table_b", "select a from analytics.table_a"],
        csv_source="",
        output_columns=[{"name": "id", "type": "INTEGER", "nullable": False},
                        {"name": "amount", "type": "NUMERIC(10,2)", "nullable": True}],
        extra_columns="raise", config={}, secret_ref="", mode_type="replace",
        primary_key=[], assets=[], timeout_seconds=None,
    )
    base.update(over)
    return PythonNodeFields(**base)


def _build(fields: PythonNodeFields, *, service: str = "marketing-py", image_tag: str = "img:1"):
    return build_python_node(
        fields, service=service, image_tag=image_tag,
        source_hash=_SOURCE_HASH, shared_code_hash=_SHARED_CODE_HASH,
    )


def _wire(tmp_path, entry: dict):
    (node,) = parse_python_contract(write_contract(tmp_path, entry), "img:1").nodes
    return node


_API_READS: dict = {}
_UPSERT_COLUMNS = [{"name": "id", "type": "INTEGER", "nullable": False},
                   {"name": "amount", "type": "NUMERIC(10,2)"}]

# One wire entry and the builder fields that say the same thing, per shape the parser handles.
_CASES = {
    "python-node": (
        lambda: make_entry(source_hash=_SOURCE_HASH, shared_code_hash=_SHARED_CODE_HASH),
        lambda: _fields(),
    ),
    "python-node with every option": (
        lambda: make_entry(
            source_hash=_SOURCE_HASH, shared_code_hash=_SHARED_CODE_HASH, description="test data",
            extra_columns="warn", config={"indexes": [{"columns": ["id"], "unique": True}]},
            mode_type="upsert", primary_key=["id"], assets=["lib/ref.json"], timeout_minutes=90,
            output_columns=_UPSERT_COLUMNS,
        ),
        lambda: _fields(
            description="test data", extra_columns="warn",
            config={"indexes": [{"columns": ["id"], "unique": True}]},
            mode_type="upsert", primary_key=["id"], assets=["lib/ref.json"], timeout_seconds=5400,
            output_columns=[{"name": "id", "type": "INTEGER", "nullable": False},
                            {"name": "amount", "type": "NUMERIC(10,2)", "nullable": True}],
        ),
    ),
    "python-csv": (
        lambda: make_csv_entry(source_hash=_SOURCE_HASH, shared_code_hash=_SHARED_CODE_HASH),
        lambda: _fields(
            table="orders_csv", owner="team", kind=NodeType.PYTHON_CSV, script="",
            reads={"csv": "s3://drops/orders.csv"}, dependency_sqls=[], csv_source="s3://drops/orders.csv",
            output_columns=[{"name": "order_id", "type": "INTEGER", "nullable": False}],
        ),
    ),
    "python-api": (
        lambda: make_entry(
            source_hash=_SOURCE_HASH, shared_code_hash=_SHARED_CODE_HASH, kind="python-api",
            reads=_API_READS, secret_ref="continuo-api-fx",
        ),
        lambda: _fields(
            kind=NodeType.PYTHON_API, reads={}, dependency_sqls=[], secret_ref="continuo-api-fx",
        ),
    ),
}


@pytest.mark.parametrize("case", sorted(_CASES))
def test_build_python_node_matches_the_wire_parser(tmp_path, case):
    entry, fields = _CASES[case]
    assert _build(fields()) == _wire(tmp_path, entry())


def test_the_content_hash_folds_the_two_producer_parts_with_the_projection_hash():
    node = _build(_fields())
    config_hash = projection_hash(PythonNode.from_mapping({
        "schema": "analytics", "table": "table_test", "kind": "python-node", "script": "scripts/table_test.py",
        "reads": {"joined": "select a from analytics.table_a", "ids": "select id from analytics.table_b"},
        "output_columns": [{"name": "id", "type": "INTEGER", "nullable": False},
                           {"name": "amount", "type": "NUMERIC(10,2)", "nullable": True}],
        "extra_columns": "raise", "config": {}, "secret_ref": "", "mode_type": "replace",
        "primary_key": [], "assets": [],
    }))
    assert node.config_hash == config_hash
    assert node.content_hash == fold3(_SOURCE_HASH, _SHARED_CODE_HASH, config_hash)
    assert (node.source_hash, node.shared_code_hash) == (_SOURCE_HASH, _SHARED_CODE_HASH)


def test_the_node_is_a_python_node_of_the_given_service_and_image():
    node = _build(_fields(), service="other-svc", image_tag="img:9")
    assert (node.runtime, node.service_name, node.image_tag) == (Runtime.PYTHON, "other-svc", "img:9")
    assert (node.resolved_relation, node.candidate_sql, node.code_unit_ids) == ("table_test", "", [])
    assert node.original_file_path == "scripts/table_test.py"
    assert node.declared_columns == 2


def test_the_raw_code_carries_what_the_node_declares():
    raw = json.loads(_build(_CASES["python-node with every option"][1]()).raw_code)
    assert raw == {
        "schema": "analytics", "table": "table_test", "owner": "marketing", "schedule": "daily",
        "criticality": "SECONDARY", "kind": "python-node", "description": "test data", "extra_columns": "warn",
        "reads": {"joined": "select a from analytics.table_a", "ids": "select id from analytics.table_b"},
        "output_columns": [{"name": "id", "type": "INTEGER", "nullable": False},
                           {"name": "amount", "type": "NUMERIC(10,2)", "nullable": True}],
        "config": {"indexes": [{"columns": ["id"], "unique": True}]},
        "script": "scripts/table_test.py", "mode_type": "upsert", "primary_key": ["id"], "assets": ["lib/ref.json"],
    }


def test_the_raw_code_leaves_out_what_does_not_apply():
    raw = json.loads(_build(_fields(kind=NodeType.PYTHON_CSV, script="", reads={"csv": "s3://d/o.csv"},
                                    dependency_sqls=[], csv_source="s3://d/o.csv")).raw_code)
    assert not {"script", "secret_ref", "mode_type", "primary_key", "assets"} & set(raw)
    assert raw["kind"] == "python-csv"


def test_the_raw_code_is_deterministic_pretty_json():
    node = _build(_fields())
    assert node.raw_code == json.dumps(json.loads(node.raw_code), sort_keys=True, indent=2)


def test_a_node_the_library_refuses_is_a_malformed_contract():
    with pytest.raises(MalformedContractError):
        _build(_fields(output_columns=[{"name": "id", "type": "NOT A TYPE (", "nullable": True}]))


def test_the_fields_are_not_changed_by_the_build():
    fields = _fields(primary_key=["id"], mode_type="upsert", assets=["a.json"], config={"k": [1]})
    before = (list(fields.primary_key), list(fields.assets), dict(fields.config), dict(fields.reads),
              list(fields.dependency_sqls), [dict(c) for c in fields.output_columns])
    _build(fields)
    assert before == (list(fields.primary_key), list(fields.assets), dict(fields.config), dict(fields.reads),
                      list(fields.dependency_sqls), [dict(c) for c in fields.output_columns])
