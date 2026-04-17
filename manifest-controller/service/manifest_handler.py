import logging
from adapters.filesystem.registry_repository import RegistryRepository
from adapters.redis.publisher import ManifestLoadedPublisher
from adapters.sources import ManifestSource
from domain.model import NodeRegistry, NodeRegistryEntry
from service.parser import parse_manifest
from service.resolver import resolve_upstream_deps

logger = logging.getLogger(__name__)


class ManifestHandler:
    def __init__(
        self,
        source: ManifestSource,
        manifest_publisher: ManifestLoadedPublisher,
        registry_repo: RegistryRepository,
    ) -> None:
        self._source = source
        self._manifest_publisher = manifest_publisher
        self._registry_repo = registry_repo

    def handle(self) -> None:
        manifests = self._source.list_manifests()
        if not manifests:
            logger.warning("No manifest files found — publishing empty topology")
            self._manifest_publisher.publish([])
            return

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

        # Pass 3: resolve deps and collect node dicts for publishing
        node_dicts: list[dict] = []
        for node in all_nodes:
            node.upstream_deps = resolve_upstream_deps(node, lookup)
            node_dicts.append({
                "service_name": node.service_name,
                "schema_name": node.schema_name,
                "table_name": node.table_name,
                "owner": node.owner,
                "schedule_name": node.schedule_name,
                "criticality": node.criticality,
                "node_type": node.node_type,
                "manifest_version": node.manifest_version,
                "dependencies": [
                    {
                        "service_name": dep.service_name,
                        "schema_name": dep.schema_name,
                        "table_name": dep.table_name,
                    }
                    for dep in node.upstream_deps
                ],
            })

        # Publish all nodes as a single manifest.loaded:v1 event
        self._manifest_publisher.publish(node_dicts)
        logger.info(
            "Manifest load complete",
            extra={"published": len(node_dicts)},
        )
