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

    Each node's candidate artifact — the object its validation Job fetches to
    build it as an empty table — is uploaded to S3 via the uploader; the
    topology carries candidate_artifact_uri (an s3:// reference) rather than
    the inline SQL string. Upload failures are fatal — publish_failed is
    called and the handler returns so the consumer ACKs without dangling refs.

    A single code-bundle document (one per release, covering every published
    node plus the shared-code units they depend on) is built and uploaded via
    bundle_uploader immediately before publish_ok; the resulting s3:// URI is
    published as code_bundle_uri. A bundle-upload failure is fatal for the
    same reason as a candidate-SQL upload failure.

    On parse/resolve failures that re-delivery cannot fix, publishes
    status=failed and returns normally so the consumer ACKs.

    dialect is the sqlglot dialect of the warehouse the install targets,
    supplied by the composition root from the configured engine. It governs
    both dependency resolution and the candidate-schema rewrite, so the
    uploaded SQL is in the dialect the validation runner will execute.
    """

    def __init__(
        self,
        source: ManifestSource,
        publisher: CandidateManifestPublisher,
        uploader: CandidateSqlUploader,
        bundle_uploader: CodeBundleUploader,
        dialect: str,
    ) -> None:
        self._source = source
        self._publisher = publisher
        self._uploader = uploader
        self._bundle_uploader = bundle_uploader
        self._dialect = dialect

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

            # Namespace this manifest's shared-code units by service so two
            # manifests pinning different versions of the same dbt package
            # never collide on a bare macro id — each service's copy of a
            # same-named macro gets its own bundle entry, and every node keeps
            # pointing at the copy its own manifest actually hashed against.
            # Per-node hashes fold each manifest's own copy of a unit already
            # (see parser._shared_code_hash), so this never changes a hash —
            # it only disambiguates which copy the bundle records for a unit id.
            namespace = mf.declared_service or (nodes[0].service_name if nodes else "")

            for unit_id, unit in mf_shared.items():
                namespaced_id = f"{namespace}:{unit_id}"
                namespaced_unit = {
                    **unit,
                    "depends_on": [f"{namespace}:{dep_id}" for dep_id in unit["depends_on"]],
                }
                existing = shared_code.get(namespaced_id)
                if existing is not None and existing["checksum"] != namespaced_unit["checksum"]:
                    # Namespacing makes cross-manifest collisions impossible by
                    # construction; this can only fire if a single manifest's
                    # own unit id were somehow re-defined, which parse_manifest's
                    # dict merge already precludes. Kept as a defensive tripwire.
                    logger.warning(
                        "candidate: shared-code unit re-defined with a different "
                        "checksum under the same namespace",
                        extra={"release_id": release_id, "unit_id": namespaced_id},
                    )
                shared_code[namespaced_id] = namespaced_unit

            for node in nodes:
                node.code_unit_ids = [f"{namespace}:{uid}" for uid in node.code_unit_ids]

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
                node.upstream_deps = resolve_upstream_deps(node, lookup, dialect=self._dialect)
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
            # Seeds carry no candidate_sql and yield an empty string.
            candidate_sql = rewrite_to_candidate_schema(
                node.candidate_sql, lookup, candidate_schema,
                self_schema=node.schema_name, self_table=node.table_name,
                dialect=self._dialect,
            )

            unique_id = f"{node.schema_name}.{node.table_name}"

            # Upload the rewritten SQL to S3 and store only the s3:// URI in the
            # topology event; an upload failure is fatal because publishing a node
            # without its SQL would leave release-controller with a dangling reference.
            try:
                candidate_artifact_uri = self._uploader.upload(
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
                    error_class="CandidateArtifactUploadFailed",
                    error_detail=str(exc),
                )
                return

            topology.append({
                "unique_id":              unique_id,
                "schema_name":            node.schema_name,
                "table_name":             node.table_name,
                "service_name":           node.service_name,
                "node_type":              node.node_type,
                "test_count":             node.test_count,
                "content_hash":           node.content_hash,
                "image_tag":              node.image_tag,
                "original_file_path":     node.original_file_path,
                "upstream_unique_ids":    [
                    f"{dep.schema_name}.{dep.table_name}" for dep in node.upstream_deps
                ],
                "schedule":               node.schedule_name,
                "candidate_artifact_uri": candidate_artifact_uri,
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
