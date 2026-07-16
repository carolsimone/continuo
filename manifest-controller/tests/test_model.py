from domain.model import (
    ManifestFile,
    ManifestNode,
    NodeRegistry,
    NodeRegistryEntry,
    RuntimeManifestRef,
    UpstreamDep,
)
from domain.exceptions import UnqualifiedTableReferenceError


def test_manifest_file_attributes():
    mf = ManifestFile(path="/manifests/service_a/manifest_v3.json", version="v3")
    assert mf.path == "/manifests/service_a/manifest_v3.json"
    assert mf.version == "v3"


def test_manifest_file_runtime_manifest_defaults_to_none():
    """A manifest without a sibling descriptor carries no runtime manifest."""
    mf = ManifestFile(path="/manifests/service_a/manifest.json", version="v1")
    assert mf.runtime_manifest is None


def test_runtime_manifest_ref_to_wire():
    ref = RuntimeManifestRef(
        uri="s3://continuo/service-1/r1/partial_parse.msgpack",
        sha256="a" * 64,
        dbt_version="1.12.0b1",
        parse_context_sha256="b" * 64,
    )
    assert ref.to_wire() == {
        "runtime_manifest_uri": "s3://continuo/service-1/r1/partial_parse.msgpack",
        "runtime_manifest_sha256": "a" * 64,
        "runtime_manifest_dbt_version": "1.12.0b1",
        "runtime_manifest_parse_context_sha256": "b" * 64,
    }


def test_runtime_manifest_ref_compares_by_value():
    """Two refs describing the same artifact are equal, so a service declared
    twice with an identical descriptor is not a conflict."""
    def _ref(sha):
        return RuntimeManifestRef(
            uri="s3://continuo/service-1/r1/partial_parse.msgpack",
            sha256=sha,
            dbt_version="1.12.0b1",
            parse_context_sha256="b" * 64,
        )

    assert _ref("a" * 64) == _ref("a" * 64)
    assert _ref("a" * 64) != _ref("c" * 64)


def test_manifest_node_dbt_unique_id_defaults_to_empty():
    node = ManifestNode(
        table_name="orders",
        schema_name="public",
        service_name="service-1",
        owner="data-platform",
        schedule_name="daily",
        criticality="SECONDARY",
        compiled_sql="SELECT 1",
    )
    assert node.dbt_unique_id == ""


def test_manifest_node_manifest_version_defaults_to_empty():
    node = ManifestNode(
        table_name="orders",
        schema_name="public",
        service_name="service-1",
        owner="data-platform",
        schedule_name="daily",
        criticality="SECONDARY",
        compiled_sql="SELECT 1",
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
        compiled_sql="SELECT 1",
    )
    assert node.criticality == "SECONDARY"
    assert node.upstream_deps == []


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
