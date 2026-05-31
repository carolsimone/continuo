import json
import logging
from adapters.redis.candidate_publisher import CandidateManifestPublisher
from adapters.sources import ManifestSource
from domain.exceptions import UnqualifiedTableReferenceError
from domain.model import NodeRegistry, NodeRegistryEntry
from service.parser import parse_manifest
from service.resolver import resolve_upstream_deps

logger = logging.getLogger(__name__)


class CandidateManifestHandler:
    """Parses a per-release set of dbt manifests and publishes the resolved
    candidate topology back to release-controller.

    Differs from ManifestHandler in three places:
    - leaves image_tag empty by design; release-controller joins the
      per-service tags from the POST /releases body onto the topology.
    - does not persist the registry anywhere; it is built in-memory solely
      for dependency resolution.
    - publishes the manifest.loaded.candidate:v1 envelope shape.

    On parse/resolve failures that re-delivery cannot fix, publishes
    status=failed and returns normally so the consumer ACKs.
    """

    def __init__(self, source: ManifestSource, publisher: CandidateManifestPublisher) -> None:
        self._source = source
        self._publisher = publisher

    def handle(self, release_id: str) -> None:
        try:
            self._handle_impl(release_id)
        finally:
            self._source.cleanup()

    def _handle_impl(self, release_id: str) -> None:
        manifests = self._source.list_manifests()
        if not manifests:
            logger.warning(
                "candidate: no manifest files found — publishing empty topology",
                extra={"release_id": release_id},
            )
            self._publisher.publish_ok(release_id=release_id, topology=[])
            return

        logger.info(
            "candidate: loading manifests",
            extra={"release_id": release_id, "count": len(manifests)},
        )

        all_nodes = []
        for mf in manifests:
            try:
                all_nodes.extend(parse_manifest(mf.path, mf.version, mf.image_tag))
            except (json.JSONDecodeError, KeyError, IndexError) as exc:
                # Invalid JSON, a missing top-level `nodes` key, or a node with a
                # malformed dbt shape (missing schema/fqn, empty fqn) are all
                # permanent — re-delivery cannot fix them, so report failed and
                # let the consumer ACK. Transient errors (e.g. a download/IO
                # failure) are deliberately not caught here so they stay pending.
                self._publisher.publish_failed(
                    release_id=release_id,
                    error_class="MalformedManifest",
                    error_detail=f"{mf.path}: {exc!r}",
                )
                return

        registry = NodeRegistry(entries=[
            NodeRegistryEntry(
                table_name=n.table_name,
                schema_name=n.schema_name,
                service_name=n.service_name,
                owner=n.owner,
            )
            for n in all_nodes
        ])
        lookup = registry.to_lookup()

        topology: list[dict] = []
        for node in all_nodes:
            try:
                node.upstream_deps = resolve_upstream_deps(node, lookup)
            except UnqualifiedTableReferenceError as exc:
                self._publisher.publish_failed(
                    release_id=release_id,
                    error_class="UnqualifiedTableReference",
                    error_detail=str(exc),
                )
                return

            topology.append({
                "unique_id":           f"{node.schema_name}.{node.table_name}",
                "schema_name":         node.schema_name,
                "table_name":          node.table_name,
                "service_name":        node.service_name,
                "node_type":           node.node_type,
                "content_hash":        node.content_hash,
                "image_tag":           node.image_tag,
                "upstream_unique_ids": [
                    f"{dep.schema_name}.{dep.table_name}" for dep in node.upstream_deps
                ],
                "schedule":            node.schedule_name,
            })

        self._publisher.publish_ok(release_id=release_id, topology=topology)
        logger.info(
            "candidate: parse complete",
            extra={"release_id": release_id, "published_nodes": len(topology)},
        )
