import json
from service.parser import parse_manifest


def _write(tmp_path, manifest: dict) -> str:
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(manifest))
    return str(p)


def _model(uid, name):
    return {
        "resource_type": "model", "name": name, "schema": "analytics",
        "fqn": ["svc_a", name], "tags": ["daily"],
        "config": {"meta": {"owner": "team@x.com"}},
        "checksum": {"checksum": "abc"}, "compiled_code": "select 1",
    }


def test_test_count_counts_generic_and_singular(tmp_path):
    manifest = {"macros": {}, "nodes": {
        "model.svc_a.orders": _model("model.svc_a.orders", "orders"),
        "model.svc_a.customers": _model("model.svc_a.customers", "customers"),
        # generic test attached via attached_node
        "test.svc_a.not_null_orders_id": {
            "resource_type": "test", "attached_node": "model.svc_a.orders",
            "depends_on": {"nodes": ["model.svc_a.orders"]},
        },
        # singular test attached via depends_on only
        "test.svc_a.assert_orders_positive": {
            "resource_type": "test",
            "depends_on": {"nodes": ["model.svc_a.orders"]},
        },
    }}
    nodes = {n.table_name: n for n in parse_manifest(_write(tmp_path, manifest), "v1")}
    assert nodes["orders"].test_count == 2
    assert nodes["customers"].test_count == 0
