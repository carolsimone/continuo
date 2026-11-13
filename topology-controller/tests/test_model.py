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
    mf = ManifestFile(path="/manifests/service_a/manifest_v3.json")
    assert mf.path == "/manifests/service_a/manifest_v3.json"


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
        relation="orders",
    )
    assert entry.table_name == "orders"


def test_node_registry_to_lookup():
    entries = [
        NodeRegistryEntry("orders", "public", "service-1", "data-platform", relation="orders"),
        NodeRegistryEntry("users", "public", "service-2", "core-team", relation="users"),
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
    assert json.dumps({"node_type": NodeType.PYTHON_NODE, "runtime": Runtime.DBT}) == (
        '{"node_type": "python-node", "runtime": "dbt"}'
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


def test_seed_columns_pairs_each_header_column_with_its_declared_type():
    seed = _node(node_type=NodeType.DBT_SEED, seed_header=("currency", "rate_to_eur"),
                 seed_column_types={"rate_to_eur": "numeric"})
    assert seed.seed_columns == [{"name": "currency", "type": ""}, {"name": "rate_to_eur", "type": "numeric"}]
    assert _node().seed_columns == []


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


def test_relation_name_is_empty_for_a_test_and_a_python_node():
    common = dict(service_name="s", owner="o", schedule_name="daily", criticality="CORE")
    test = ManifestNode(table_name="not_null_x", schema_name="", node_type=NodeType.DBT_TEST, identity="test.s.not_null_x.1", **common)
    py = ManifestNode(table_name="t", schema_name="analytics", node_type=NodeType.PYTHON_NODE, resolved_relation="t",
                      runtime=Runtime.PYTHON, **common)
    assert (test.relation_name, py.relation_name) == ("", "")


def test_relation_falls_back_to_the_table_name_and_keeps_the_declared_spelling():
    common = dict(schema_name="analytics", service_name="s", owner="o", schedule_name="daily", criticality="CORE")
    assert ManifestNode(table_name="orders", **common).relation == "orders"
    aliased = ManifestNode(table_name="orders", resolved_relation="Orders_Tbl", **common)
    assert (aliased.relation, aliased.relation_name) == ("Orders_Tbl", "Orders_Tbl")


def test_the_lookup_keys_each_entry_by_its_relation():
    lookup = NodeRegistry(entries=[
        NodeRegistryEntry("orders", "Analytics", "svc", "o", relation="Orders_Tbl"),
        NodeRegistryEntry("dim.v2", "analytics", "svc", "o", relation="dim_v2"),
    ]).to_lookup()
    assert set(lookup) == {("analytics", "orders_tbl"), ("analytics", "dim_v2")}
    assert lookup[("analytics", "orders_tbl")].table_name == "orders"


def test_ephemeral_ctes_key_each_ephemeral_entry_by_its_service_and_cte_name():
    eph = NodeRegistryEntry("eph", "analytics", "svc", "o", relation="Eph", ephemeral=True)
    registry = NodeRegistry(entries=[eph, NodeRegistryEntry("orders", "analytics", "svc", "o", relation="orders")])
    assert registry.ephemeral_ctes() == {("svc", "__dbt__cte__eph"): eph}


def test_the_registry_of_nodes_leaves_tests_out_and_marks_ephemeral_models():
    common = dict(schema_name="analytics", service_name="s", owner="o", schedule_name="daily", criticality="CORE")
    registry = NodeRegistry.of_nodes([
        ManifestNode(table_name="orders", resolved_relation="orders_tbl", **common),
        ManifestNode(table_name="eph", materialized="ephemeral", **common),
        ManifestNode(table_name="not_null_x", node_type=NodeType.DBT_TEST, identity="test.s.not_null_x.1", **common),
    ])
    assert [(e.table_name, e.relation, e.ephemeral) for e in registry.entries] == [
        ("orders", "orders_tbl", False), ("eph", "eph", True)]
