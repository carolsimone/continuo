import json
import logging
from adapters.candidate_sql_uploader import CandidateSqlUploader
from adapters.code_bundle_uploader import CodeBundleUploader
from adapters.redis.candidate_publisher import CandidateManifestPublisher
from adapters.sources import ManifestSource
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from domain.model import NodeRegistry, NodeRegistryEntry
from service.code_bundle import build_code_bundle
from service.parser import parse_manifest
from service.resolver import resolve_upstream_deps
from service.rewriter import candidate_schema_name, rewrite_to_candidate_schema

logger = logging.getLogger(__name__)


class CandidateManifestHandler:
    """Parses a per-release set of dbt manifests and publishes the resolved
    candidate topology back to release-controller.

    image_tag is left empty by design; release-controller joins the
    per-service tags from the POST /releases body onto the topology.
    The registry is built in-memory solely for dependency resolution
    and is not persisted anywhere.

    Each node's compiled candidate SQL is uploaded to S3 via the uploader;
    the topology carries candidate_sql_uri (an s3:// reference) rather than
    the inline SQL string. Upload failures are fatal — publish_failed is
    called and the handler returns so the consumer ACKs without dangling refs.

    A single code-bundle document (one per release, covering every published
    node plus the shared-code units they depend on) is built and uploaded via
    bundle_uploader immediately before publish_ok; the resulting s3:// URI is
    published as code_bundle_uri. A bundle-upload failure is fatal for the
    same reason as a candidate-SQL upload failure.

    On parse/resolve failures that re-delivery cannot fix, publishes
    status=failed and returns normally so the consumer ACKs.
    """

    def __init__(
        self,
        source: ManifestSource,
        publisher: CandidateManifestPublisher,
        uploader: CandidateSqlUploader,
        bundle_uploader: CodeBundleUploader,
    ) -> None:
        self._source = source
        self._publisher = publisher
        self._uploader = uploader
        self._bundle_uploader = bundle_uploader

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
            self._publisher.publish_ok(release_id=release_id, topology=[], code_bundle_uri="")
            return

        logger.info(
            "candidate: loading manifests",
            extra={"release_id": release_id, "count": len(manifests)},
        )

        all_nodes = []
        shared_code: dict[str, dict] = {}
        for mf in manifests:
            try:
                nodes, mf_shared = parse_manifest(mf.path, mf.version, mf.image_tag)
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

            for unit_id, unit in mf_shared.items():
                existing = shared_code.get(unit_id)
                if existing is not None:
                    # Per-node hashes already folded each manifest's own copy of
                    # this unit; the bundle keeps the first occurrence regardless
                    # of whether a later manifest's checksum matches or conflicts.
                    if existing["checksum"] != unit["checksum"]:
                        # Cross-service package skew: two manifests ship the same
                        # unit id with different source.
                        logger.warning(
                            "candidate: conflicting shared-code unit across services",
                            extra={"release_id": release_id, "unit_id": unit_id},
                        )
                    continue
                shared_code[unit_id] = unit

            if mf.declared_service:
                # Validate that the manifest actually belongs to the declared service.
                # An empty manifest would silently retire all nodes for the declared
                # service; a wrong-service manifest would pollute the topology with
                # foreign nodes. Both are permanent failures (a re-upload is required).
                if not nodes:
                    self._publisher.publish_failed(
                        release_id=release_id,
                        error_class="EmptyManifest",
                        error_detail=(
                            f"{mf.declared_service}: manifest contains no model/seed nodes"
                        ),
                    )
                    return

                offending = {n.service_name for n in nodes if n.service_name != mf.declared_service}
                if offending:
                    self._publisher.publish_failed(
                        release_id=release_id,
                        error_class="ServiceMismatch",
                        error_detail=(
                            f"{mf.declared_service}: manifest contains nodes for "
                            f"{sorted(offending)}"
                        ),
                    )
                    return

            all_nodes.extend(nodes)

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
        candidate_schema = candidate_schema_name(release_id)

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
            except InvalidCompiledSqlError as exc:
                self._publisher.publish_failed(
                    release_id=release_id,
                    error_class="InvalidCompiledSql",
                    error_detail=str(exc),
                )
                return

            # Rewrite all known-node schema references to the candidate schema so
            # blue/green validation can build each node against its upstream closure.
            # Seeds carry no compiled SQL and yield an empty string.
            candidate_sql = rewrite_to_candidate_schema(
                node.compiled_sql, lookup, candidate_schema,
                self_schema=node.schema_name, self_table=node.table_name,
            )

            unique_id = f"{node.schema_name}.{node.table_name}"

            # Upload the rewritten SQL to S3 and store only the s3:// URI in the
            # topology event; an upload failure is fatal because publishing a node
            # without its SQL would leave release-controller with a dangling reference.
            try:
                candidate_sql_uri = self._uploader.upload(
                    release_id=release_id,
                    unique_id=unique_id,
                    sql=candidate_sql,
                )
            except Exception as exc:
                # Collapse all upload errors to a permanent failure so the load is
                # failed (operator re-triggers) rather than left pending — this
                # guarantees no dangling reference is ever published.
                self._publisher.publish_failed(
                    release_id=release_id,
                    error_class="CandidateSqlUploadFailed",
                    error_detail=str(exc),
                )
                return

            topology.append({
                "unique_id":           unique_id,
                "schema_name":         node.schema_name,
                "table_name":          node.table_name,
                "service_name":        node.service_name,
                "node_type":           node.node_type,
                "test_count":          node.test_count,
                "content_hash":        node.content_hash,
                "image_tag":           node.image_tag,
                "original_file_path":  node.original_file_path,
                "upstream_unique_ids": [
                    f"{dep.schema_name}.{dep.table_name}" for dep in node.upstream_deps
                ],
                "schedule":            node.schedule_name,
                "candidate_sql_uri":   candidate_sql_uri,
            })

        bundle = build_code_bundle(release_id, all_nodes, shared_code)
        try:
            code_bundle_uri = self._bundle_uploader.upload(release_id, bundle)
        except Exception as exc:
            # Same fatal semantics as candidate-SQL uploads: never publish a
            # topology that references a bundle that failed to land.
            self._publisher.publish_failed(
                release_id=release_id,
                error_class="CodeBundleUploadFailed",
                error_detail=str(exc),
            )
            return

        self._publisher.publish_ok(
            release_id=release_id, topology=topology, code_bundle_uri=code_bundle_uri,
        )
        logger.info(
            "candidate: parse complete",
            extra={"release_id": release_id, "published_nodes": len(topology)},
        )
