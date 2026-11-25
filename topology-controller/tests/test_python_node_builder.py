import importlib.resources
import json

import pytest
from continuo_contracts.hashing import fold3
from continuo_contracts.python import PythonColumn, PythonNode, code_projection, projection_hash

from domain.contract_vocabulary import NodeType
from domain.exceptions import MalformedContractError
from domain.model import ManifestNode, Runtime
from service.python_kind_rules import RULES
from service.python_node_builder import PythonNodeFields, build_python_node

_SOURCE_HASH = "aaa111"
_SHARED_CODE_HASH = "bbb222"

# What the demo node's raw_code and code projection read, as the code bundle carries them.
_DEMO_RAW_CODE = """{
  "config": {},
  "criticality": "SECONDARY",
  "description": "",
  "extra_columns": "raise",
  "kind": "python-node",
  "output_columns": [
    {
      "name": "id",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "name": "amount",
      "nullable": true,
      "type": "NUMERIC(10,2)"
    }
  ],
  "owner": "marketing",
  "reads": {
    "ids": "select id from analytics.table_b",
    "joined": "select a from analytics.table_a"
  },
  "schedule": "daily",
  "schema": "analytics",
  "script": "scripts/table_test.py",
  "table": "table_test"
}"""
_DEMO_PROJECTION = """{
  "assets": [],
  "config": {},
  "extra_columns": "raise",
  "kind": "python-node",
  "mode_type": "replace",
  "output_columns": [
    {
      "name": "id",
      "nullable": false,
      "type": "INTEGER"
    },
    {
      "name": "amount",
      "nullable": true,
      "type": "NUMERIC(10,2)"
    }
  ],
  "primary_key": [],
  "reads": {
    "ids": "select id from analytics.table_b",
    "joined": "select a from analytics.table_a"
  },
  "schema": "analytics",
  "script": "scripts/table_test.py",
  "secret_ref": "",
  "table": "table_test"
}"""


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


def _build(fields: PythonNodeFields, *, project: str = "marketing-py", image_tag: str = "img:1"):
    return build_python_node(
        fields, project=project, image_tag=image_tag,
        source_hash=_SOURCE_HASH, shared_code_hash=_SHARED_CODE_HASH,
    )


def _every_option_fields() -> PythonNodeFields:
    return _fields(
        description="test data", extra_columns="warn",
        config={"indexes": [{"columns": ["id"], "unique": True}]},
        mode_type="upsert", primary_key=["id"], assets=["lib/ref.json"], timeout_seconds=5400,
        output_columns=[{"name": "id", "type": "INTEGER", "nullable": False},
                        {"name": "amount", "type": "NUMERIC(10,2)", "nullable": True}],
    )


def _csv_fields() -> PythonNodeFields:
    return _fields(
        table="orders_csv", owner="team", kind=NodeType.PYTHON_CSV, script="",
        reads={"csv": "s3://drops/orders.csv"}, dependency_sqls=[], csv_source="s3://drops/orders.csv",
        output_columns=[{"name": "order_id", "type": "INTEGER", "nullable": False}],
    )


def test_python_node_builder_builds_the_demo_node():
    config_hash = projection_hash(PythonNode.from_mapping({
        "schema": "analytics", "table": "table_test", "kind": "python-node", "script": "scripts/table_test.py",
        "reads": {"joined": "select a from analytics.table_a", "ids": "select id from analytics.table_b"},
        "output_columns": [{"name": "id", "type": "INTEGER", "nullable": False},
                           {"name": "amount", "type": "NUMERIC(10,2)", "nullable": True}],
        "extra_columns": "raise", "config": {}, "secret_ref": "", "mode_type": "replace",
        "primary_key": [], "assets": [],
    }))
    assert _build(_fields()) == ManifestNode(
        table_name="table_test",
        schema_name="analytics",
        project_name="marketing-py",
        owner="marketing",
        schedule_name="daily",
        criticality="SECONDARY",
        dependency_sqls=["select id from analytics.table_b", "select a from analytics.table_a"],
        candidate_sql="",
        output_columns=[{"name": "id", "type": "INTEGER", "nullable": False},
                        {"name": "amount", "type": "NUMERIC(10,2)", "nullable": True}],
        node_type=NodeType.PYTHON_NODE,
        content_hash=fold3(_SOURCE_HASH, _SHARED_CODE_HASH, config_hash),
        image_tag="img:1",
        original_file_path="scripts/table_test.py",
        raw_code=_DEMO_RAW_CODE,
        config={},
        source_hash=_SOURCE_HASH,
        shared_code_hash=_SHARED_CODE_HASH,
        config_hash=config_hash,
        code_unit_ids=[],
        runtime=Runtime.PYTHON,
        resolved_relation="table_test",
        declared_columns=2,
        projection=_DEMO_PROJECTION,
    )


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


def test_the_node_is_a_python_node_of_the_given_project_and_image():
    node = _build(_fields(), project="other-svc", image_tag="img:9")
    assert (node.runtime, node.project_name, node.image_tag) == (Runtime.PYTHON, "other-svc", "img:9")
    assert (node.resolved_relation, node.candidate_sql, node.code_unit_ids) == ("table_test", "", [])
    assert node.original_file_path == "scripts/table_test.py"
    assert node.declared_columns == 2


def test_the_raw_code_carries_what_the_node_declares():
    raw = json.loads(_build(_every_option_fields()).raw_code)
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
    raw = json.loads(_build(_csv_fields()).raw_code)
    assert not {"script", "secret_ref", "mode_type", "primary_key", "assets"} & set(raw)
    assert raw["kind"] == "python-csv"


def test_the_raw_code_is_deterministic_pretty_json():
    node = _build(_fields())
    assert node.raw_code == json.dumps(json.loads(node.raw_code), sort_keys=True, indent=2)


def test_a_python_api_node_carries_its_secret_ref_and_reads_nothing():
    node = _build(_fields(kind=NodeType.PYTHON_API, reads={}, dependency_sqls=[], secret_ref="continuo-api-fx"))
    raw = json.loads(node.raw_code)
    assert (node.node_type, node.dependency_sqls, node.secret_ref) == (NodeType.PYTHON_API, [], "continuo-api-fx")
    assert (raw["reads"], raw["secret_ref"], raw["script"]) == ({}, "continuo-api-fx", "scripts/table_test.py")
    assert json.loads(node.projection)["secret_ref"] == "continuo-api-fx"


def test_a_python_api_node_without_a_secret_ref_carries_none():
    node = _build(_fields(kind=NodeType.PYTHON_API, reads={}, dependency_sqls=[]))
    assert node.secret_ref == "" and "secret_ref" not in json.loads(node.raw_code)


def test_a_csv_node_reads_its_file_and_its_hashes_fold_the_csv_projection():
    node = _build(_csv_fields())
    config_hash = projection_hash(PythonNode(
        schema="analytics", table="orders_csv", kind="python-csv", reads={"csv": "s3://drops/orders.csv"},
        output_columns=[PythonColumn("order_id", "INTEGER", False)],
    ))
    assert (node.node_type, node.runtime) == (NodeType.PYTHON_CSV, Runtime.PYTHON)
    assert (node.csv_source, node.dependency_sqls, node.original_file_path) == ("s3://drops/orders.csv", [], "")
    assert node.config_hash == config_hash
    assert node.content_hash == fold3(_SOURCE_HASH, _SHARED_CODE_HASH, config_hash)


def test_the_task_timeout_rides_on_the_node_and_not_in_the_raw_code():
    node = _build(_every_option_fields())
    assert node.timeout_seconds == 5400
    assert "timeout_seconds" not in json.loads(node.raw_code)
    assert _build(_fields()).timeout_seconds is None


def test_the_write_mode_primary_key_and_assets_join_the_projection():
    node = _build(_every_option_fields())
    expected = projection_hash(PythonNode(
        schema="analytics", table="table_test", kind="python-node", script="scripts/table_test.py",
        reads={"joined": "select a from analytics.table_a", "ids": "select id from analytics.table_b"},
        output_columns=[PythonColumn("id", "INTEGER", False), PythonColumn("amount", "NUMERIC(10,2)", True)],
        extra_columns="warn", config={"indexes": [{"columns": ["id"], "unique": True}]},
        mode_type="upsert", primary_key=["id"], assets=["lib/ref.json"],
    ))
    assert node.config_hash == expected


def test_owner_schedule_criticality_and_description_do_not_move_the_hash():
    changed = _build(_fields(owner="other", schedule="hourly", criticality="CORE", description="x"))
    assert changed.content_hash == _build(_fields()).content_hash


def test_a_column_type_and_its_canonical_spelling_hash_alike():
    spelled = [{"name": "id", "type": "INT", "nullable": False}, {"name": "amount", "type": "NUMERIC(10, 2)"}]
    assert _build(_fields(output_columns=spelled)).content_hash == _build(_fields()).content_hash


def test_a_read_edited_after_the_producer_hashed_the_source_moves_the_hash():
    edited = _fields(reads={"joined": "select a from analytics.other", "ids": "select id from analytics.table_b"})
    assert _build(edited).content_hash != _build(_fields()).content_hash


def test_the_projection_is_the_code_projection_and_the_raw_code_keeps_the_owner():
    node = _build(_fields())
    projection = code_projection(PythonNode(
        schema="analytics", table="table_test", kind="python-node", script="scripts/table_test.py",
        reads={"joined": "select a from analytics.table_a", "ids": "select id from analytics.table_b"},
        output_columns=[PythonColumn("id", "INTEGER", False), PythonColumn("amount", "NUMERIC(10,2)", True)],
    ))
    assert json.loads(node.projection) == projection
    assert node.projection == json.dumps(projection, sort_keys=True, indent=2)
    assert "owner" not in json.loads(node.projection)
    assert json.loads(node.raw_code)["owner"] == "marketing"


def _projection_vectors() -> list[dict]:
    text = importlib.resources.files("continuo_contracts").joinpath("vectors", "projection.json").read_text()
    return json.loads(text)


@pytest.mark.parametrize("vector", _projection_vectors(), ids=lambda vector: vector["name"])
def test_the_library_projection_vectors(vector):
    declared = vector["node"]
    kind = NodeType(declared["kind"])
    parsed = RULES[kind].reads.parse(declared["reads"], declared["table"])
    node = _build(_fields(
        schema=declared["schema"], table=declared["table"], kind=kind, script=declared.get("script", ""),
        reads=parsed.reads, dependency_sqls=parsed.dependency_sqls, csv_source=parsed.csv_source,
        output_columns=[
            {"name": c["name"], "type": c["type"], "nullable": c.get("nullable", True)}
            for c in declared["output_columns"]],
        extra_columns=declared.get("extra_columns", "raise"), config=declared.get("config", {}),
        secret_ref=declared.get("secret_ref", ""), mode_type=declared.get("mode_type", "replace"),
        primary_key=declared.get("primary_key", []), assets=declared.get("assets", []),
    ))
    assert node.config_hash == vector["projection_hash"]
    assert json.loads(node.projection) == vector["projection"]


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
