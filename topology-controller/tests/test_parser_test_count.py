import json
from service.parser import parse_manifest


def _write(tmp_path, manifest: dict) -> str:
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(manifest))
    return str(p)


def _model(name):
    return {
        "resource_type": "model", "name": name, "schema": "analytics",
        "fqn": ["svc_a", name], "tags": ["daily"],
        "config": {"meta": {"owner": "team@x.com"}},
        "checksum": {"checksum": "abc"}, "compiled_code": "select 1",
    }


def _test(name, *, attached=None, targets, compiled="select 1"):
    return {
        "resource_type": "test", "name": name, "schema": "analytics_dbt_test__audit",
        "fqn": ["svc_a", name], "attached_node": attached,
        "depends_on": {"nodes": targets, "macros": []},
        "original_file_path": "models/schema.yml", "config": {},
        "checksum": {"checksum": ""}, "compiled_code": compiled,
    }


def test_test_count_counts_generic_and_singular(tmp_path):
    manifest = {"macros": {}, "nodes": {
        "model.svc_a.orders": _model("orders"),
        "model.svc_a.customers": _model("customers"),
        # generic test attached via attached_node
        "test.svc_a.not_null_orders_id": _test(
            "not_null_orders_id", attached="model.svc_a.orders", targets=["model.svc_a.orders"],
        ),
        # singular test attached via depends_on only
        "test.svc_a.assert_orders_positive": _test(
            "assert_orders_positive", targets=["model.svc_a.orders"],
        ),
    }}
    parsed, _ = parse_manifest(_write(tmp_path, manifest), "v1")
    nodes = {n.table_name: n for n in parsed}
    assert nodes["orders"].test_count == 2
    assert nodes["customers"].test_count == 0


def test_relationships_test_counts_once_via_attached_node(tmp_path):
    # A relationships-style test spans two tracked models: it is attached to
    # `orders` but its depends_on lists both `orders` and `customers`. Per the
    # attached_node-priority rule it must count exactly once, toward the
    # attached node, and must NOT also increment the second model.
    manifest = {"macros": {}, "nodes": {
        "model.svc_a.orders": _model("orders"),
        "model.svc_a.customers": _model("customers"),
        "test.svc_a.relationships_orders_customer_id": _test(
            "relationships_orders_customer_id", attached="model.svc_a.orders",
            targets=["model.svc_a.orders", "model.svc_a.customers"],
        ),
    }}
    parsed, _ = parse_manifest(_write(tmp_path, manifest), "v1")
    nodes = {n.table_name: n for n in parsed}
    assert nodes["orders"].test_count == 1
    assert nodes["customers"].test_count == 0
