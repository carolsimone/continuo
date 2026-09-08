import json
from domain.model import NodeType
from service.parser import parse_manifest


def _write(tmp_path, manifest):
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(manifest))
    return str(p)


def _model(name):
    return {"resource_type": "model", "name": name, "schema": "analytics", "fqn": ["svc_a", name],
            "tags": ["daily"], "config": {"meta": {"owner": "team@x.com"}},
            "checksum": {"checksum": "abc"}, "compiled_code": f"select 1 as id from raw.{name}"}


def _generic_test(name, attached, compiled):
    return {"resource_type": "test", "name": name, "schema": "analytics_dbt_test__audit", "fqn": ["svc_a", name],
            "attached_node": attached, "depends_on": {"nodes": [attached], "macros": ["macro.dbt.test_not_null"]},
            "original_file_path": "models/schema.yml", "config": {"severity": "ERROR"},
            "checksum": {"name": "none", "checksum": ""}, "compiled_code": compiled}


def test_generic_test_becomes_a_dbt_test_node(tmp_path):
    manifest = {"macros": {"macro.dbt.test_not_null": {"macro_sql": "{% test not_null %}", "depends_on": {"macros": []}}},
                "nodes": {
                    "model.svc_a.orders": _model("orders"),
                    "test.svc_a.not_null_orders_id.9f": _generic_test("not_null_orders_id", "model.svc_a.orders",
                                                                     "select id from analytics.orders where id is null"),
                }}
    parsed, _ = parse_manifest(_write(tmp_path, manifest), "v1")
    by_id = {n.unique_id: n for n in parsed}

    t = by_id["test.svc_a.not_null_orders_id.9f"]
    assert t.node_type == NodeType.DBT_TEST
    assert t.table_name == "not_null_orders_id"
    assert t.schema_name == "analytics_dbt_test__audit"
    assert t.service_name == "svc-a"
    assert t.candidate_sql == "select id from analytics.orders where id is null"
    assert t.dependency_sqls == ["select id from analytics.orders where id is null"]
    assert t.original_file_path == "models/schema.yml"
    assert t.content_hash.startswith("sha256:")
    assert t.owner == "" and t.schedule_name == ""
    assert by_id["analytics.orders"].test_count == 1


def test_singular_test_without_attached_node_is_tracked_through_depends_on(tmp_path):
    manifest = {"macros": {}, "nodes": {
        "model.svc_a.orders": _model("orders"),
        "test.svc_a.assert_orders_positive": {
            "resource_type": "test", "name": "assert_orders_positive", "schema": "analytics_dbt_test__audit",
            "fqn": ["svc_a", "assert_orders_positive"], "depends_on": {"nodes": ["model.svc_a.orders"], "macros": []},
            "original_file_path": "tests/assert_orders_positive.sql", "config": {},
            "checksum": {"checksum": "def"}, "compiled_code": "select * from analytics.orders where amount < 0",
        }}}
    parsed, _ = parse_manifest(_write(tmp_path, manifest), "v1")
    ids = {n.unique_id for n in parsed}
    assert "test.svc_a.assert_orders_positive" in ids


def test_test_on_an_untracked_node_is_skipped(tmp_path):
    untracked = _model("draft"); untracked["config"] = {}  # no owner: the model itself is skipped
    manifest = {"macros": {}, "nodes": {
        "model.svc_a.draft": untracked,
        "test.svc_a.not_null_draft_id.1": _generic_test("not_null_draft_id", "model.svc_a.draft", "select 1"),
    }}
    parsed, _ = parse_manifest(_write(tmp_path, manifest), "v1")
    assert parsed == []


def test_test_content_hash_changes_with_its_compiled_sql(tmp_path):
    base = {"macros": {}, "nodes": {
        "model.svc_a.orders": _model("orders"),
        "test.svc_a.not_null_orders_id.9f": _generic_test("not_null_orders_id", "model.svc_a.orders", "select id from analytics.orders where id is null"),
    }}
    changed = json.loads(json.dumps(base))
    changed["nodes"]["test.svc_a.not_null_orders_id.9f"]["compiled_code"] = "select amount from analytics.orders where amount is null"
    h1 = {n.unique_id: n.content_hash for n in parse_manifest(_write(tmp_path, base), "v1")[0]}
    h2 = {n.unique_id: n.content_hash for n in parse_manifest(_write(tmp_path, changed), "v1")[0]}
    assert h1["test.svc_a.not_null_orders_id.9f"] != h2["test.svc_a.not_null_orders_id.9f"]
    assert h1["analytics.orders"] == h2["analytics.orders"]
