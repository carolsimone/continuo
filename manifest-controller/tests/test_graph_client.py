from unittest.mock import MagicMock, patch
import pytest
from domain.model import ManifestNode, UpstreamDep
from adapters.grpc.graph_client import GraphClient
from proto.graph.v1 import graph_pb2


def _make_node(upstream_deps=None, node_type="dbt-model") -> ManifestNode:
    return ManifestNode(
        table_name="orders",
        schema_name="public",
        service_name="service-1",
        owner="data-platform",
        schedule_name="daily",
        criticality="CORE",
        compiled_sql="SELECT 1",
        node_type=node_type,
        upstream_deps=upstream_deps or [],
    )


def test_create_node_calls_grpc(mocker):
    mock_stub = MagicMock()
    mock_stub.CreateNode.return_value = MagicMock()

    client = GraphClient.__new__(GraphClient)
    client._stub = mock_stub

    client.create_node(_make_node())

    mock_stub.CreateNode.assert_called_once()
    call_args = mock_stub.CreateNode.call_args[0][0]
    assert call_args.table_name == "orders"
    assert call_args.schema_name == "public"
    assert call_args.service_name == "service-1"
    assert call_args.owner == "data-platform"
    assert call_args.schedule_name == "daily"


def test_create_node_maps_criticality_core(mocker):
    mock_stub = MagicMock()
    client = GraphClient.__new__(GraphClient)
    client._stub = mock_stub

    client.create_node(_make_node())

    call_args = mock_stub.CreateNode.call_args[0][0]
    assert call_args.criticality == graph_pb2.CRITICALITY_CORE


def test_create_node_maps_criticality_secondary(mocker):
    mock_stub = MagicMock()
    client = GraphClient.__new__(GraphClient)
    client._stub = mock_stub

    node = _make_node()
    node.criticality = "SECONDARY"
    client.create_node(node)

    call_args = mock_stub.CreateNode.call_args[0][0]
    assert call_args.criticality == graph_pb2.CRITICALITY_SECONDARY


def test_create_node_includes_upstream_deps(mocker):
    mock_stub = MagicMock()
    client = GraphClient.__new__(GraphClient)
    client._stub = mock_stub

    deps = [UpstreamDep(table_name="users", schema_name="public", service_name="user_service")]
    client.create_node(_make_node(upstream_deps=deps))

    call_args = mock_stub.CreateNode.call_args[0][0]
    assert len(call_args.upstream_dependencies) == 1
    assert call_args.upstream_dependencies[0].table_name == "users"
    assert call_args.upstream_dependencies[0].service_name == "user_service"


def test_create_node_passes_node_type(mocker):
    mock_stub = MagicMock()
    client = GraphClient.__new__(GraphClient)
    client._stub = mock_stub

    client.create_node(_make_node(node_type="dbt-seed"))

    call_args = mock_stub.CreateNode.call_args[0][0]
    assert call_args.node_type == "dbt-seed"


def test_create_node_passes_manifest_version():
    mock_stub = MagicMock()
    client = GraphClient.__new__(GraphClient)
    client._stub = mock_stub

    node = _make_node()
    node.manifest_version = "v3"
    client.create_node(node)

    call_args = mock_stub.CreateNode.call_args[0][0]
    assert call_args.manifest_version == "v3"
