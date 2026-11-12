import hashlib
import json

import pytest
from continuo_contracts.canonical import canonical_json
from continuo_contracts.fingerprints import hashed_config_denylist
from continuo_contracts.hashing import fold4, shared_fold

from domain.exceptions import InvalidNodeSettingError
from domain.model import CompileExtras
from service.dbt_hashing import (
    ManifestContext,
    RunMask,
    compiled_part,
    drop_empty,
    hash_dbt_node,
    hashed_config,
    macro_closure,
    mask_text,
    mask_value,
    materialization_macro,
    resolve_name,
    shared_units,
    source_part,
    static_macro_calls,
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


def _macro(unique_id: str, sql: str = "", deps: tuple[str, ...] = ()) -> dict:
    _, package, name = unique_id.split(".", 2)
    return {"unique_id": unique_id, "name": name, "package_name": package, "macro_sql": sql or f"-- {unique_id}",
            "depends_on": {"macros": list(deps)}}


def _project(macros: list[dict], *, nodes: dict | None = None, unit_tests: dict | None = None,
             functions: dict | None = None, flags: dict | None = None) -> ManifestContext:
    manifest = {"metadata": {"project_name": "fpshop", "adapter_type": "postgres"},
                "macros": {m["unique_id"]: m for m in macros}, "nodes": nodes or {},
                "unit_tests": unit_tests or {}, "functions": functions or {}}
    extras = None
    if flags is not None:
        extras = CompileExtras(service="fpshop", release_id="r", seeds={}, project_flags=flags, packages={})
    return ManifestContext.from_manifest(manifest, extras)


def _node(resource_type: str = "model", **over) -> dict:
    node = {"unique_id": f"{resource_type}.fpshop.n", "resource_type": resource_type, "package_name": "fpshop",
            "name": "n", "schema": "a", "raw_code": "select 1", "config": {},
            "depends_on": {"macros": [], "nodes": []}}
    node.update(over)
    return node


def test_built_in_macros_are_walked_but_not_folded():
    ctx = _project([
        _macro("macro.dbt.materialization_table_default", deps=("macro.dbt.create_table_as",)),
        _macro("macro.dbt.create_table_as", deps=("macro.dbt_postgres.postgres__create_table_as",)),
        _macro("macro.dbt_postgres.postgres__create_table_as"),
        _macro("macro.fpshop.postgres__create_table_as", "{% macro postgres__create_table_as() %}x{% endmacro %}"),
    ])
    assert macro_closure(_node(config={"materialized": "table"}), ctx) == {"macro.fpshop.postgres__create_table_as"}


def test_a_root_macro_replaces_a_global_macro_of_the_same_name():
    ctx = _project([
        _macro("macro.dbt.materialization_seed_default", deps=("macro.dbt.should_full_refresh",)),
        _macro("macro.dbt.should_full_refresh"),
        _macro("macro.fpshop.should_full_refresh"),
    ])
    assert macro_closure(_node("seed"), ctx) == {"macro.fpshop.should_full_refresh"}
    assert macro_closure(_node(depends_on={"macros": ["macro.dbt.should_full_refresh"]}), ctx) == {
        "macro.fpshop.should_full_refresh"}


def test_static_calls_resolve_root_first_and_drop_unknown_names():
    ctx = _project([
        _macro("macro.fpshop.since"), _macro("macro.svc.since"), _macro("macro.svc.cents"),
        _macro("macro.dbt.is_incremental"), _macro("macro.dbt_utils.star"), _macro("macro.fpshop.outside"),
    ])
    raw = ("{{ config(materialized='incremental') }}\nselect outside(), {{ since() }} from {{ ref('x') }}\n"
           "{% if is_incremental() %} {{ dbt_utils.star('a') }} {% endif %}\n"
           "{% for i in range(3) %}{{ adapter.dispatch('f')() }}{% endfor %} {{ cents (1) }}")
    assert static_macro_calls(raw, "svc", ctx) == [
        "macro.fpshop.since", "macro.dbt.is_incremental", "macro.dbt_utils.star", "macro.svc.cents"]
    assert resolve_name("cents", "svc", ctx) == "macro.svc.cents"
    assert resolve_name("ref", "svc", ctx) is None


def test_a_macro_called_only_inside_is_incremental_is_folded():
    ctx = _project([_macro("macro.dbt.is_incremental"), _macro("macro.fpshop.since")])
    node = _node(raw_code="select 1 {% if is_incremental() %} where id > {{ since() }} {% endif %}",
                 depends_on={"macros": ["macro.dbt.is_incremental"], "nodes": []})
    assert macro_closure(node, ctx) == {"macro.fpshop.since"}


def test_an_adapter_specific_materialization_beats_a_root_default():
    ctx = _project([
        _macro("macro.dbt.materialization_table_default"),
        _macro("macro.fpshop.materialization_table_default"),
        _macro("macro.dbt_postgres.materialization_table_postgres"),
    ])
    assert materialization_macro("table", ctx) == "macro.dbt_postgres.materialization_table_postgres"
    assert materialization_macro("view", ctx) is None


def test_a_root_default_materialization_beats_the_built_in_one():
    ctx = _project([_macro("macro.dbt.materialization_table_default"),
                    _macro("macro.fpshop.materialization_table_default")])
    assert materialization_macro("table", ctx) == "macro.fpshop.materialization_table_default"


def test_an_imported_package_overrides_a_built_in_materialization_only_when_the_flag_allows_it():
    macros = [_macro("macro.dbt.materialization_table_default"), _macro("macro.pkg.materialization_table_default"),
              _macro("macro.pkg.materialization_custom_default")]
    assert materialization_macro("table", _project(macros)) == "macro.dbt.materialization_table_default"
    allowed = _project(macros, flags={"require_explicit_package_overrides_for_builtin_materializations": False})
    assert materialization_macro("table", allowed) == "macro.pkg.materialization_table_default"
    assert materialization_macro("custom", _project(macros)) == "macro.pkg.materialization_custom_default"


def test_the_strategy_macro_is_a_root_of_the_closure():
    ctx = _project([
        _macro("macro.dbt.get_incremental_default_sql"), _macro("macro.fpshop.get_incremental_merge_sql"),
        _macro("macro.dbt.snapshot_check_strategy"), _macro("macro.fpshop.snapshot_check_strategy"),
    ])
    merge = _node(config={"materialized": "incremental", "incremental_strategy": "merge"})
    assert macro_closure(merge, ctx) == {"macro.fpshop.get_incremental_merge_sql"}
    assert macro_closure(_node("snapshot", config={"strategy": "check"}), ctx) == {"macro.fpshop.snapshot_check_strategy"}


def test_a_macro_cycle_is_walked_once_and_folds_every_macro_on_it():
    ctx = _project([_macro("macro.fpshop.a", deps=("macro.fpshop.b",)), _macro("macro.fpshop.b", deps=("macro.fpshop.a",))])
    node = _node(depends_on={"macros": ["macro.fpshop.a"], "nodes": []})
    assert macro_closure(node, ctx) == {"macro.fpshop.a", "macro.fpshop.b"}
    assert macro_closure(_node(depends_on={"macros": ["macro.fpshop.b"], "nodes": []}), ctx) == {
        "macro.fpshop.a", "macro.fpshop.b"}


_HOOK = {"unique_id": "operation.fpshop.fpshop-on-run-start-0", "resource_type": "operation",
         "name": "fpshop-on-run-start-0", "tags": ["on-run-start"], "raw_code": "select 1 as hook_start"}


def test_a_test_folds_no_service_unit():
    ctx = _project([_macro("macro.fpshop.generate_schema_name"), _macro("macro.fpshop.generate_alias_name")],
                   nodes={_HOOK["unique_id"]: _HOOK}, flags={"some_flag": True})
    model_units = shared_units(_node(), ctx)
    assert {uid: u.input_name for uid, u in model_units.items()} == {
        "operation.fpshop.fpshop-on-run-start-0": "on_run_start",
        "macro.fpshop.generate_schema_name": "generate_schema_name",
        "macro.fpshop.generate_alias_name": "generate_alias_name",
        "project_flags": "project_flags",
    }
    assert model_units["operation.fpshop.fpshop-on-run-start-0"].digest == _sha("select 1 as hook_start")
    assert model_units["project_flags"].digest == hashlib.sha256(canonical_json({"some_flag": True})).hexdigest()
    assert set(shared_units(_node("seed"), ctx)) == set(model_units)
    assert shared_units(_node("test"), ctx) == {}


def test_an_empty_flags_block_folds_no_unit():
    assert "project_flags" not in shared_units(_node(), _project([], flags={}))
    assert "project_flags" not in shared_units(_node(), _project([]))
    assert "project_flags" in shared_units(_node(), _project([], flags={"f": 1}))


def test_a_model_folds_its_unit_tests_and_the_functions_it_calls():
    unit_test = {"unique_id": "unit_test.fpshop.n.t", "resource_type": "unit_test", "checksum": "ab" * 32,
                 "depends_on": {"macros": [], "nodes": ["model.fpshop.n"]}, "created_at": 1.5}
    no_checksum = {**unit_test, "unique_id": "unit_test.fpshop.n.u", "checksum": ""}
    function = {"unique_id": "function.fpshop.double_it", "resource_type": "function", "raw_code": "x * 2"}
    ctx = _project([], unit_tests={"unit_test.fpshop.n.t": unit_test, "unit_test.fpshop.n.u": no_checksum},
                   functions={"function.fpshop.double_it": function})
    node = _node(depends_on={"macros": [], "nodes": ["function.fpshop.double_it", "model.fpshop.up"]})
    units = shared_units(node, ctx)
    assert units["unit_test.fpshop.n.t"].digest == "ab" * 32
    unchecked = {k: v for k, v in no_checksum.items() if k != "created_at"}
    assert units["unit_test.fpshop.n.u"].digest == hashlib.sha256(canonical_json(unchecked)).hexdigest()
    assert units["function.fpshop.double_it"].digest == hashlib.sha256(canonical_json({"raw_code": "x * 2"})).hexdigest()
    assert {u.input_name for u in units.values()} == {"unit_tests", "functions"}
    assert "unit_test.fpshop.n.t" not in shared_units({**node, "resource_type": "snapshot"}, ctx)


def test_hash_dbt_node_folds_the_units_and_names_its_inputs():
    ctx = _project([_macro("macro.fpshop.m", "select 1")], nodes={_HOOK["unique_id"]: _HOOK})
    node = _node(compiled_code="select 1", depends_on={"macros": ["macro.fpshop.m"], "nodes": []},
                 config={"materialized": "table", "contract": {"enforced": True}},
                 columns={"id": {"name": "id", "data_type": "integer"}})
    parts = hash_dbt_node(node, ctx)
    assert parts.shared_code_hash == shared_fold({"macro.fpshop.m": _sha("select 1"),
                                                  "operation.fpshop.fpshop-on-run-start-0": _sha("select 1 as hook_start")})
    assert parts.content_hash == fold4(parts.source_hash, parts.shared_code_hash, parts.config_hash, parts.compiled_hash)
    assert parts.inputs == {"raw_code", "compiled_code", "config.materialized", "config.contract", "relation.schema",
                            "relation.alias", "contract.columns", "constraints", "macros", "on_run_start"}
    assert parts.path_checksum is False
    assert set(parts.units) == {"macro.fpshop.m", "operation.fpshop.fpshop-on-run-start-0"}


def test_a_contract_foreign_key_to_the_node_database_is_masked():
    node = {"resource_type": "model", "schema": "a", "name": "c", "database": "warehouse",
            "config": {"contract": {"enforced": True}},
            "columns": {"b_id": {"name": "b_id", "data_type": "integer", "constraints": [
                {"type": "foreign_key", "to": "\"warehouse\".\"a\".\"b\"", "to_columns": ["id"]}]}},
            "constraints": [{"type": "foreign_key", "columns": ["b_id"], "to": "\"warehouse\".\"a\".\"b\""}]}
    got = hashed_config(node, _ctx())
    assert got["columns"][0]["constraints"][0]["to"] == "\"__CONTINUO_DATABASE__\".\"a\".\"b\""
    assert got["constraints"][0]["to"] == "\"__CONTINUO_DATABASE__\".\"a\".\"b\""


def test_a_unit_test_canonical_json_refuses_rejects_the_artifact(tmp_path):
    path = _manifest(tmp_path, {"materialized": "table"})
    manifest = json.loads(open(path).read())
    manifest["unit_tests"] = {"unit_test.svc.deep.t": {
        "unique_id": "unit_test.svc.deep.t", "checksum": "", "given": [{"rows": [{"id": 9007199254740993}]}],
        "depends_on": {"macros": [], "nodes": ["model.svc.deep"]}}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(InvalidNodeSettingError, match="manifest.json"):
        parse_manifest(path)


def test_a_unit_test_with_a_checksum_and_a_value_canonical_json_refuses_parses(tmp_path):
    path = _manifest(tmp_path, {"materialized": "table"})
    manifest = json.loads(open(path).read())
    manifest["unit_tests"] = {"unit_test.svc.deep.t": {
        "unique_id": "unit_test.svc.deep.t", "checksum": "ab" * 32, "given": [{"rows": [{"id": 9007199254740993}]}],
        "depends_on": {"macros": [], "nodes": ["model.svc.deep"]}}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    (node,) = parse_manifest(path).nodes
    assert node.shared_code_hash == shared_fold({"unit_test.svc.deep.t": "ab" * 32})


def _hook(project: str, kind: str, raw_code: str = "select 1", macros: tuple[str, ...] = ()) -> dict:
    unique_id = f"operation.{project}.{project}-on-run-{kind}-0"
    return {"unique_id": unique_id, "resource_type": "operation", "package_name": project,
            "name": unique_id.split(".", 2)[2], "tags": [f"on-run-{kind}"], "raw_code": raw_code,
            "depends_on": {"macros": list(macros)}}


def _moved_nodes(before: ManifestContext, after: ManifestContext) -> set[str]:
    nodes = [_node("model"), _node("seed"), _node("snapshot"), _node("test")]
    return {n["resource_type"] for n in nodes if hash_dbt_node(n, before).content_hash != hash_dbt_node(n, after).content_hash}


def test_an_on_run_hook_folds_the_root_macros_it_calls():
    def ctx(body: str, helper: str = "h") -> ManifestContext:
        hook = _hook("fpshop", "start", "{{ grant_usage() }}", macros=("macro.dbt.run_query",))
        return _project([_macro("macro.fpshop.grant_usage", body), _macro("macro.dbt.run_query", deps=("macro.dbt.statement",)),
                         _macro("macro.dbt.statement"), _macro("macro.fpshop.statement", helper)],
                        nodes={hook["unique_id"]: hook})
    assert _moved_nodes(ctx("grant a"), ctx("grant b")) == {"model", "seed", "snapshot"}
    assert _moved_nodes(ctx("grant a"), ctx("grant a", helper="changed")) == {"model", "seed", "snapshot"}
    units = ctx("grant a").service_units
    assert units["macro.fpshop.grant_usage"].input_name == "macros"
    assert units["macro.fpshop.statement"].input_name == "macros"


def test_a_hook_is_classified_by_the_suffix_of_its_id():
    start = _hook("on-run-end-tools", "start")
    end = _hook("on-run-start-tools", "end")
    untagged = {**_hook("on-run-end-tools", "start"), "tags": []}
    ctx = _project([], nodes={start["unique_id"]: start, end["unique_id"]: end})
    assert ctx.service_units[start["unique_id"]].input_name == "on_run_start"
    assert ctx.service_units[end["unique_id"]].input_name == "on_run_end"
    assert _project([], nodes={untagged["unique_id"]: untagged}).service_units[untagged["unique_id"]].input_name == "on_run_start"


def _function(**over) -> dict:
    function = {"unique_id": "function.fpshop.f", "resource_type": "function", "raw_code": "x * 2",
                "arguments": [{"name": "x", "data_type": "integer"}], "returns": {"data_type": "integer"},
                "overloads": [{"arguments": [{"name": "x", "data_type": "text"}], "returns": {"data_type": "text"},
                               "raw_body": "x || x"}],
                "depends_on": {"macros": [], "nodes": []}, "description": "d"}
    function.update(over)
    return function


def _caller_hash(function: dict, macros: list[dict] | None = None) -> str:
    ctx = _project(macros or [], functions={"function.fpshop.f": function})
    return hash_dbt_node(_node(depends_on={"macros": [], "nodes": ["function.fpshop.f"]}), ctx).content_hash


def test_a_function_digest_is_its_body_record():
    function = _function()
    ctx = _project([], functions={"function.fpshop.f": function})
    unit = shared_units(_node(depends_on={"macros": [], "nodes": ["function.fpshop.f"]}), ctx)["function.fpshop.f"]
    body = {"raw_code": "x * 2", "arguments": [{"name": "x", "data_type": "integer"}], "returns": {"data_type": "integer"},
            "overloads": [{"arguments": [{"name": "x", "data_type": "text"}], "returns": {"data_type": "text"},
                           "raw_body": "x || x"}]}
    assert unit.digest == hashlib.sha256(canonical_json(body)).hexdigest()
    bare = {"unique_id": "function.fpshop.f", "resource_type": "function", "raw_code": "x"}
    bare_ctx = _project([], functions={"function.fpshop.f": bare})
    bare_unit = shared_units(_node(depends_on={"macros": [], "nodes": ["function.fpshop.f"]}), bare_ctx)["function.fpshop.f"]
    assert bare_unit.digest == hashlib.sha256(canonical_json({"raw_code": "x"})).hexdigest()


def test_a_function_overload_or_return_type_change_moves_the_caller():
    base = _caller_hash(_function())
    changed_overload = _function(overloads=[{"arguments": [{"name": "x", "data_type": "text"}],
                                             "returns": {"data_type": "text"}, "raw_body": "x || x || x"}])
    assert _caller_hash(changed_overload) != base
    assert _caller_hash(_function(returns={"data_type": "bigint"})) != base
    assert _caller_hash(_function(description="other")) == base


def test_a_function_folds_the_root_macros_it_depends_on():
    function = _function(depends_on={"macros": ["macro.fpshop.helper"], "nodes": []})
    assert _caller_hash(function, [_macro("macro.fpshop.helper", "a")]) != _caller_hash(
        function, [_macro("macro.fpshop.helper", "b")])


def test_a_function_record_canonical_json_refuses_rejects_the_artifact(tmp_path):
    path = _manifest(tmp_path, {"materialized": "table"})
    manifest = json.loads(open(path).read())
    manifest["nodes"]["model.svc.deep"]["depends_on"] = {"macros": [], "nodes": ["function.svc.f"]}
    manifest["functions"] = {"function.svc.f": {"unique_id": "function.svc.f", "raw_code": "x",
                                                "arguments": [{"name": "x", "default_value": 9007199254740993}]}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(InvalidNodeSettingError, match="analytics.deep"):
        parse_manifest(path)


def test_every_macro_a_walk_reaches_is_memoized():
    ctx = _project([_macro("macro.fpshop.a", deps=("macro.fpshop.b",)), _macro("macro.fpshop.b", deps=("macro.fpshop.c",)),
                    _macro("macro.fpshop.c", deps=("macro.fpshop.b", "macro.fpshop.d")), _macro("macro.fpshop.d")])
    macro_closure(_node(depends_on={"macros": ["macro.fpshop.a"], "nodes": []}), ctx)
    assert ctx._closures == {
        "macro.fpshop.a": frozenset({"macro.fpshop.a", "macro.fpshop.b", "macro.fpshop.c", "macro.fpshop.d"}),
        "macro.fpshop.b": frozenset({"macro.fpshop.b", "macro.fpshop.c", "macro.fpshop.d"}),
        "macro.fpshop.c": frozenset({"macro.fpshop.b", "macro.fpshop.c", "macro.fpshop.d"}),
        "macro.fpshop.d": frozenset({"macro.fpshop.d"}),
    }


def test_a_function_description_edit_moves_no_caller_and_a_type_edit_does():
    def described(argument_doc: str, return_doc: str, overload_doc: str, data_type: str = "integer") -> dict:
        return _function(
            arguments=[{"name": "x", "data_type": data_type, "description": argument_doc}],
            returns={"data_type": "integer", "description": return_doc},
            overloads=[{"arguments": [{"name": "x", "data_type": "text", "description": overload_doc}],
                        "returns": {"data_type": "text", "description": overload_doc}, "raw_body": "x || x"}])
    base = _caller_hash(described("a", "b", "c"))
    assert _caller_hash(described("other", "b", "c")) == base
    assert _caller_hash(described("a", "other", "other")) == base
    assert _caller_hash(described("a", "b", "c")) == _caller_hash(_function())
    assert _caller_hash(described("a", "b", "c", data_type="bigint")) != base
