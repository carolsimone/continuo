import logging
from adapters.filesystem.registry_repository import RegistryRepository
from adapters.grpc.graph_client import GraphClient
from adapters.sources import ManifestSource
from domain.model import NodeRegistry, NodeRegistryEntry
from service.parser import parse_manifest
from service.resolver import resolve_upstream_deps

logger = logging.getLogger(__name__)


class ManifestHandler:
    def __init__(
        self,
        source: ManifestSource,
        graph_client: GraphClient,
        registry_repo: RegistryRepository,
    ) -> None:
        self._source = source
        self._graph_client = graph_client
        self._registry_repo = registry_repo

    def handle(self) -> tuple[list[str], dict[str, str]]:
        manifests = self._source.list_manifests()
        if not manifests:
            logger.warning("No manifest files found — nothing to load")
            return [], {}

        logger.info("Loading manifests", extra={"count": len(manifests)})

        # Pass 1: parse all manifests into a flat node list
        all_nodes = []
        for mf in manifests:
            logger.info("Parsing manifest", extra={"manifest_path": mf.path, "version": mf.version})
            all_nodes.extend(parse_manifest(mf.path, mf.version))

        # Pass 2: build combined registry and persist
        registry = NodeRegistry(entries=[
            NodeRegistryEntry(
                table_name=n.table_name,
                schema_name=n.schema_name,
                service_name=n.service_name,
                owner=n.owner,
            )
            for n in all_nodes
        ])
        self._registry_repo.save(registry)
        logger.info("Registry persisted", extra={"node_count": len(all_nodes)})

        lookup = registry.to_lookup()

        # Pass 3: resolve deps and load each node; collect manifest_versions
        manifest_versions: dict[str, str] = {}
        loaded = 0
        failed = 0
        for node in all_nodes:
            try:
                node.upstream_deps = resolve_upstream_deps(node, lookup)
                self._graph_client.create_node(node)
                manifest_versions[node.service_name] = node.manifest_version
                loaded += 1
            except Exception as e:
                logger.error(
                    "Failed to load node",
                    extra={"table": node.table_name, "error": str(e)},
                )
                failed += 1

        logger.info(
            "Manifest load complete",
            extra={"loaded": loaded, "failed": failed},
        )

        schedule_names = list({n.schedule_name for n in all_nodes if n.schedule_name})
        return schedule_names, manifest_versions
