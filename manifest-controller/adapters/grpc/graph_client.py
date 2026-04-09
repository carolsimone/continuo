import grpc
from domain.model import ManifestNode
from proto.graph.v1 import graph_pb2, graph_pb2_grpc

_CRITICALITY_MAP = {
    "REGULATORY": graph_pb2.CRITICALITY_REGULATORY,
    "CORE":       graph_pb2.CRITICALITY_CORE,
    "SECONDARY":  graph_pb2.CRITICALITY_SECONDARY,
}


class GraphClient:
    def __init__(self, address: str) -> None:
        channel = grpc.insecure_channel(address)
        self._stub = graph_pb2_grpc.GraphServiceStub(channel)

    def create_node(self, node: ManifestNode) -> None:
        upstream_deps = [
            graph_pb2.UpstreamDependency(
                table_name=dep.table_name,
                schema_name=dep.schema_name,
                service_name=dep.service_name,
            )
            for dep in node.upstream_deps
        ]
        request = graph_pb2.CreateNodeRequest(
            table_name=node.table_name,
            schema_name=node.schema_name,
            service_name=node.service_name,
            owner=node.owner,
            schedule_name=node.schedule_name,
            criticality=_CRITICALITY_MAP.get(node.criticality, graph_pb2.CRITICALITY_SECONDARY),
            upstream_dependencies=upstream_deps,
            node_type=node.node_type,
            manifest_version=node.manifest_version,
        )
        self._stub.CreateNode(request)
