from domain.model import ManifestFile, ManifestNode, NodeRegistry, NodeRegistryEntry, UpstreamDep
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
