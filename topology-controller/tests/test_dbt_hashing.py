import hashlib
import json

import pytest
from continuo_contracts.canonical import canonical_json
from continuo_contracts.fingerprints import hashed_config_denylist
from continuo_contracts.hashing import fold4

from domain.exceptions import InvalidNodeSettingError
from service.dbt_hashing import (
    ManifestContext,
    RunMask,
    compiled_part,
    drop_empty,
    hashed_config,
    mask_text,
    mask_value,
    source_part,
)
from service.parser import parse_manifest

RUN = RunMask(invocation_id="9ef3e1e9-a285-49db-8ad9-7eef23da70ef", run_started_at="2026-06-16T04:48:59.438570+00:00")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def test_mask_replaces_the_invocation_id_and_both_run_started_at_forms():
    text = ("select '9ef3e1e9-a285-49db-8ad9-7eef23da70ef', "
            "'2026-06-16T04:48:59.438570+00:00', '2026-06-16 04:48:59.438570+00:00'")
    assert mask_text(text, RUN, "") == ("select '__CONTINUO_INVOCATION_ID__', "
                                        "'__CONTINUO_RUN_STARTED_AT__', '__CONTINUO_RUN_STARTED_AT__'")


def test_mask_replaces_only_whole_identifiers():
    sql = "select * from \"warehouse\".\"warehouse_stage\".t join warehouse.x.y on c = 'my_warehouse'"
    assert mask_text(sql, RunMask("", ""), "warehouse") == (
        "select * from \"__CONTINUO_DATABASE__\".\"warehouse_stage\".t join __CONTINUO_DATABASE__.x.y on c = 'my_warehouse'")


def test_mask_value_masks_every_string_inside_dicts_and_lists():
    value = {"a": ["x 2026-06-16 04:48:59.438570+00:00", 1, None], "b": {"c": "warehouse.t"}, "d": True}
    assert mask_value(value, RUN, "warehouse") == {
        "a": ["x __CONTINUO_RUN_STARTED_AT__", 1, None], "b": {"c": "__CONTINUO_DATABASE__.t"}, "d": True}


def test_drop_empty_keeps_false_zero_and_the_empty_string():
    value = {"a": None, "b": {}, "c": [], "d": False, "e": 0, "f": "", "g": {"h": {"i": None}}, "j": [None, {}, 1]}
    assert drop_empty(value) == {"d": False, "e": 0, "f": "", "j": [1]}


def test_run_mask_reads_the_manifest_metadata():
    assert RunMask.from_metadata({"invocation_id": "i", "run_started_at": "r"}) == RunMask("i", "r")
    assert RunMask.from_metadata({}) == RunMask("", "")


def _ctx(extras=None):
    return ManifestContext(run=RUN, extras=extras, denylist=hashed_config_denylist())


def test_hashed_config_drops_the_denylist_and_masks_rendered_values():
    node = {"resource_type": "test", "schema": "a", "alias": "t", "database": "warehouse", "config": {
        "materialized": "test", "meta": {"o": 1}, "tags": ["x"], "docs": {"show": True}, "grants": {"select": ["r"]},
        "description": "d", "access": "public", "group": "g", "freshness": {"build_after": {"count": 4}},
        "where": "created_at > '2026-06-16 04:48:59.438570+00:00'"}}
    assert hashed_config(node, _ctx()) == {"materialized": "test", "where": "created_at > '__CONTINUO_RUN_STARTED_AT__'",
                                          "relation": {"schema": "a", "alias": "t"}}


def test_hashed_config_drops_nulls_and_empty_containers():
    base = {"resource_type": "model", "schema": "a", "name": "m", "config": {"materialized": "table"}}
    upgraded = {**base, "config": {"materialized": "table", "static_analysis": None, "on_error": None,
                                   "latest_version_pointer": {"enabled": None, "alias": None}}}
    assert hashed_config(upgraded, _ctx()) == hashed_config(base, _ctx())


def test_hashed_config_lowercases_test_severity():
    def node(severity):
        return {"resource_type": "test", "schema": "a", "alias": "t", "config": {"severity": severity}}
    assert hashed_config(node("ERROR"), _ctx()) == hashed_config(node("error"), _ctx())


def test_hashed_config_carries_the_relation_in_its_declared_spelling():
    node = {"resource_type": "model", "schema": "Analytics", "name": "cap_src", "alias": "OrdersCap", "config": {}}
    assert hashed_config(node, _ctx())["relation"] == {"schema": "Analytics", "alias": "OrdersCap"}


def test_hashed_config_carries_enforced_columns_in_order_and_model_constraints():
    node = {"resource_type": "model", "schema": "a", "name": "c", "config": {"contract": {"enforced": True}},
            "columns": {"id": {"name": "id", "data_type": "integer", "constraints": [{"type": "not_null"}], "quote": None},
                        "amount": {"name": "amount", "data_type": "numeric(12,2)", "constraints": [], "quote": True}},
            "constraints": [{"type": "primary_key", "columns": ["id"]}]}
    got = hashed_config(node, _ctx())
    assert got["columns"] == [{"name": "id", "data_type": "integer", "constraints": [{"type": "not_null"}]},
                              {"name": "amount", "data_type": "numeric(12,2)", "quote": True}]
    assert got["constraints"] == [{"type": "primary_key", "columns": ["id"]}]


def test_model_source_is_the_normalised_raw_code():
    def src(code):
        return source_part({"resource_type": "model", "raw_code": code}, _ctx())[0]
    assert src("select 1\r\n") == src("select 1") != src("select  1")


def test_model_source_names_its_input_and_a_model_without_raw_code_hashes_the_empty_text():
    assert source_part({"resource_type": "snapshot", "raw_code": "x"}, _ctx()) == (_sha("x"), "raw_code", False)
    assert source_part({"resource_type": "model"}, _ctx()) == (_sha(""), "raw_code", False)


def test_test_source_is_the_masked_compiled_code():
    node = {"resource_type": "test", "raw_code": "{{ t() }}",
            "compiled_code": "select 1 where at > '2026-06-16 04:48:59.438570+00:00'"}
    assert source_part(node, _ctx()) == (_sha("select 1 where at > '__CONTINUO_RUN_STARTED_AT__'"), "compiled_code", False)
    raw_only = {"resource_type": "test", "raw_code": "select 2"}
    assert source_part(raw_only, _ctx())[0] == _sha("select 2")
    bare = {"resource_type": "test", "name": "t"}
    assert source_part(bare, _ctx())[0] == hashlib.sha256(canonical_json(bare)).hexdigest()


def test_seed_source_without_extras_is_the_dbt_checksum_as_written():
    path_seed = {"resource_type": "seed", "checksum": {"name": "path", "checksum": "seeds/big.csv"}}
    assert source_part(path_seed, _ctx()) == ("seeds/big.csv", "seed_checksum", True)
    sha_seed = {"resource_type": "seed", "checksum": {"name": "sha256", "checksum": "ab" * 32}}
    assert source_part(sha_seed, _ctx()) == ("ab" * 32, "seed_checksum", False)


def test_compiled_part_is_the_masked_compiled_code_of_models_and_snapshots_only():
    sql = "select * from warehouse.a.b"
    assert compiled_part({"resource_type": "model", "database": "warehouse", "compiled_code": sql}, _ctx()) == _sha(
        "select * from __CONTINUO_DATABASE__.a.b")
    assert compiled_part({"resource_type": "snapshot", "compiled_code": "s"}, _ctx()) == _sha("s")
    assert compiled_part({"resource_type": "model"}, _ctx()) == ""
    assert compiled_part({"resource_type": "test", "compiled_code": sql}, _ctx()) == ""
    assert compiled_part({"resource_type": "seed", "compiled_code": sql}, _ctx()) == ""


def _manifest(tmp_path, config: dict, raw_code: str = "select 1") -> str:
    manifest = {"metadata": {}, "nodes": {"model.svc.deep": {
        "resource_type": "model", "name": "deep", "schema": "analytics", "fqn": ["svc", "deep"], "tags": ["daily"],
        "raw_code": raw_code, "compiled_code": "select 1",
        "config": {"meta": {"owner": "o"}, **config}}}}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return str(path)


def _nested(depth: int) -> list:
    value: list = [1]
    for _ in range(depth - 1):
        value = [value]
    return value


def test_a_config_canonical_json_refuses_rejects_the_artifact(tmp_path):
    with pytest.raises(InvalidNodeSettingError, match="analytics.deep"):
        parse_manifest(_manifest(tmp_path, {"deep": _nested(101)}))
    with pytest.raises(InvalidNodeSettingError, match="analytics.deep"):
        parse_manifest(_manifest(tmp_path, {"batch": 9007199254740993}))


def test_a_code_text_utf8_cannot_encode_rejects_the_artifact(tmp_path):
    with pytest.raises(InvalidNodeSettingError, match="analytics.deep"):
        parse_manifest(_manifest(tmp_path, {}, raw_code="select '\ud800'"))


def test_parse_folds_the_four_parts_of_a_model(tmp_path):
    (node,) = parse_manifest(_manifest(tmp_path, {"materialized": "table"})).nodes
    assert node.source_hash == _sha("select 1")
    assert node.compiled_hash == _sha("select 1")
    assert node.hashed_config == {"materialized": "table", "relation": {"schema": "analytics", "alias": "deep"}}
    assert node.config_hash == hashlib.sha256(canonical_json(node.hashed_config)).hexdigest()
    assert node.content_hash == fold4(node.source_hash, node.shared_code_hash, node.config_hash, node.compiled_hash)
