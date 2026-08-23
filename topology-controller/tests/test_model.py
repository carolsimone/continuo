import json

from domain.model import (
    ManifestFile,
    ManifestNode,
    NodeRegistry,
    NodeRegistryEntry,
    NodeType,
    Runtime,
    UpstreamDep,
)
from domain.exceptions import UnqualifiedTableReferenceError


def test_manifest_file_attributes():
    mf = ManifestFile(path="/manifests/service_a/manifest_v3.json", version="v3")
    assert mf.path == "/manifests/service_a/manifest_v3.json"
    assert mf.version == "v3"


def test_manifest_node_manifest_version_defaults_to_empty():
    node = ManifestNode(
        table_name="orders",
        schema_name="public",
        service_name="service-1",
        owner="data-platform",
        schedule_name="daily",
        criticality="SECONDARY",
        dependency_sqls=["SELECT 1"],
        candidate_sql="SELECT 1",
    )
    assert node.manifest_version == ""


def test_unqualified_table_reference_error_attributes():
    err = UnqualifiedTableReferenceError(table_name="users", node_table_name="orders")
    assert err.table_name == "users"
    assert err.node_table_name == "orders"
    assert "users" in str(err)
    assert "orders" in str(err)
    assert isinstance(err, ValueError)


def test_manifest_node_defaults_criticality():
    node = ManifestNode(
        table_name="orders",
        schema_name="public",
        service_name="service-1",
        owner="data-platform",
        schedule_name="daily",
        criticality="SECONDARY",
        dependency_sqls=["SELECT 1"],
        candidate_sql="SELECT 1",
    )
    assert node.criticality == "SECONDARY"
    assert node.upstream_deps == []


def test_manifest_node_carries_dependency_sqls_and_candidate_sql():
    node = ManifestNode(
        table_name="t", schema_name="s", service_name="svc", owner="o",
        schedule_name="daily", criticality="SECONDARY",
        dependency_sqls=["select * from s.up"],
        candidate_sql="select * from s.up",
    )
    assert node.dependency_sqls == ["select * from s.up"]
    assert node.candidate_sql == "select * from s.up"
    assert not hasattr(node, "compiled_sql")


def test_node_registry_entry():
    entry = NodeRegistryEntry(
        table_name="orders",
        schema_name="public",
        service_name="service-1",
        owner="data-platform",
    )
    assert entry.table_name == "orders"


def test_node_registry_to_lookup():
    entries = [
        NodeRegistryEntry("orders", "public", "service-1", "data-platform"),
        NodeRegistryEntry("users", "public", "service-2", "core-team"),
    ]
    registry = NodeRegistry(entries=entries)
    lookup = registry.to_lookup()
    assert ("public", "orders") in lookup
    assert lookup[("public", "users")].service_name == "service-2"


def test_upstream_dep():
    dep = UpstreamDep(table_name="users", schema_name="public", service_name="user_service")
    assert dep.table_name == "users"
    assert dep.service_name == "user_service"


def test_node_type_and_runtime_enums_serialize_as_plain_strings():
    assert json.dumps({"node_type": NodeType.PYTHON_MODEL, "runtime": Runtime.DBT}) == (
        '{"node_type": "python-model", "runtime": "dbt"}'
    )
    assert NodeType.DBT_MODEL == "dbt-model"
    assert Runtime.PYTHON == "python"


def _node(**over):
    fields = dict(
        table_name="orders", schema_name="analytics", service_name="finance",
        owner="data-platform", schedule_name="daily", criticality="SECONDARY",
    )
    fields.update(over)
    return ManifestNode(**fields)


def test_resolved_relation_id_falls_back_to_table_name_when_unset():
    # No resolved_relation set (e.g. a hand-built node, or one this parser
    # never touched) — falls back to table_name, exactly like a node with no
    # dbt alias.
    node = _node(table_name="orders")
    assert node.resolved_relation == ""
    assert node.resolved_relation_id == "analytics.orders"


def test_resolved_relation_id_uses_resolved_relation_when_set():
    # An aliased dbt node: table_name is the declared name (used for
    # unique_id), resolved_relation is what it actually writes.
    node = _node(table_name="orders_v2", resolved_relation="orders")
    assert node.unique_id == "analytics.orders_v2"
    assert node.resolved_relation_id == "analytics.orders"


def test_resolved_relation_id_is_lowercased():
    node = _node(schema_name="Analytics", table_name="Orders", resolved_relation="Orders")
    assert node.resolved_relation_id == "analytics.orders"
