import logging
from collections.abc import Callable, Mapping
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from domain.model import DEFAULT_TENANT_ID, FailedNode, NodeRegistry, NodeRegistryEntry, NodeType
from service.candidate_artifacts import CandidateArtifactBuilder, RewriteContext
from service.code_bundle import build_code_bundle
from service.manifest_parsers import parser_for
from service.ports import (
    CandidatePublisherPort,
    CodeBundleUploaderPort,
    ManifestSourcePort,
    TopologyArtifactWriterPort,
)
from service.resolver import resolve_upstream_deps
from service.rewriter import candidate_schema_name
from domain.contract_vocabulary import ParseFailureKind

logger = logging.getLogger(__name__)

_NO_IMAGE_TAGS = (
    "the release request carries no image_tags, so no node can be given the "
    "image it runs in"
)


def _highest_precedence(kinds: list[ParseFailureKind]) -> ParseFailureKind:
    """The release-level kind for a set of per-node kinds: the first in contract
    declaration order, which is the precedence the contract defines."""
    order = list(ParseFailureKind)
    return min(kinds, key=order.index)


def _summary(failed: list[FailedNode]) -> str:
    noun = "node" if len(failed) == 1 else "nodes"
    return f"{len(failed)} {noun} failed to parse: " + ", ".join(sorted(n.node_id for n in failed))


class CandidateManifestHandler:
    """Parses a per-release set of dbt manifests and python contracts, writes
    the resolved candidate topology to S3 as the release's topology artifact,
    and publishes a reference to it back to release-controller.

    Every node carries its service's image tag from the release request
    (image_tags), the map release-controller assembled when it activated the
    release; a service the map names no tag for keeps an empty tag. A request
    with no image_tags at all fails the release as internal before anything is
    fetched. The registry is built in-memory solely for dependency resolution
    and is not persisted anywhere.

    Each node's candidate artifact — the object its validation Job fetches to
    build it as an empty table — is built and uploaded by the artifact_builders
    entry selected by the node's runtime, at the key release-controller derives
    from the release id, the node's unique_id and its node_type; the topology
    artifact carries no reference to it.

    A single code-bundle document (one per release, covering every published
    node plus the shared-code units they depend on) is uploaded via
    bundle_uploader, then the topology artifact via artifact_writer, and only
    then is the outcome published, so a published reference never dangles. An
    empty release (no manifests) writes an artifact with no nodes and publishes
    an empty code_bundle_uri.

    A write that fails with an S3 outage (is_infrastructure_error: S3
    unreachable, a 5xx) propagates to the consumer, which pauses and redelivers
    the same message; nothing is published, so the release waits in parsing.
    Any other write failure publishes status=failed with kind internal.

    Failures publish status=failed with a typed failure_kind and return
    normally so the consumer ACKs. A node whose SQL sqlglot cannot parse, or
    that references a relation without a schema, is collected rather than
    fatal on first sight: every node is resolved, then one publish_failed
    carries the whole failed set (failure_kind by contract precedence) so the
    operator sees every broken node at once. An unreadable, empty, or
    wrong-service artifact is invalid_artifact; a missing builder, missing
    image tags or a refused S3 write is internal. Neither carries failed nodes.

    dialect is the sqlglot dialect of the warehouse the install targets,
    supplied by the composition root from the configured engine. It governs
    both dependency resolution and the candidate-schema rewrite, so the
    uploaded SQL is in the dialect the validation runner will execute.
    """

    def __init__(
        self,
        source: ManifestSourcePort,
        publisher: CandidatePublisherPort,
        bundle_uploader: CodeBundleUploaderPort,
        artifact_writer: TopologyArtifactWriterPort,
        artifact_builders: dict[str, CandidateArtifactBuilder],
        dialect: str,
        image_tags: Mapping[str, str] | None,
        is_infrastructure_error: Callable[[BaseException], bool],
    ) -> None:
        self._source = source
        self._publisher = publisher
        self._bundle_uploader = bundle_uploader
        self._artifact_writer = artifact_writer
        self._artifact_builders = artifact_builders
        self._dialect = dialect
        self._image_tags = image_tags
        self._is_infrastructure_error = is_infrastructure_error

    def handle(self, release_id: str) -> None:
        try:
            self._handle_impl(release_id)
        finally:
            self._source.cleanup()

    def _handle_impl(self, release_id: str) -> None:
        if self._image_tags is None:
            self._publisher.publish_failed(
                release_id=release_id,
                failure_kind=ParseFailureKind.INTERNAL,
                detail=_NO_IMAGE_TAGS,
                failed_nodes=[],
            )
            return

        manifests = self._source.list_manifests()
        if not manifests:
            logger.warning(
                "candidate: no manifest files found — publishing empty topology",
                extra={"release_id": release_id},
            )
            self._publish(release_id, topology=[], code_bundle_uri="")
            return

        logger.info(
            "candidate: loading manifests",
            extra={"release_id": release_id, "count": len(manifests)},
        )

        all_nodes = []
        shared_code: dict[str, dict] = {}
        for mf in manifests:
            parser = parser_for(mf.kind)
            if parser is None:
                # A kind this build cannot parse is permanent: re-delivery
                # cannot fix it, and the operator needs to see a rejected
                # release rather than a message retrying forever.
                self._publisher.publish_failed(
                    release_id=release_id,
                    failure_kind=ParseFailureKind.INVALID_ARTIFACT,
                    detail=(
                        f"{mf.declared_service or mf.path}: unknown manifest "
                        f"kind {mf.kind!r}"
                    ),
                    failed_nodes=[],
                )
                return

            try:
                nodes, mf_shared = parser.parse(mf.path, mf.image_tag)
            except parser.permanent_errors as exc:
                # Invalid JSON or yaml, a missing required key, or a malformed
                # node are all permanent — re-delivery cannot fix them, so
                # report failed and let the consumer ACK. Transient errors
                # (e.g. a download/IO failure) are deliberately not caught here
                # so they stay pending.
                self._publisher.publish_failed(
                    release_id=release_id,
                    failure_kind=ParseFailureKind.INVALID_ARTIFACT,
                    detail=f"{mf.path}: {exc!r}",
                    failed_nodes=[],
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
                node.image_tag = self._image_tags.get(node.service_name, "")

            if mf.declared_service:
                # Validate that the manifest actually belongs to the declared service.
                # An empty manifest would silently retire all nodes for the declared
                # service; a wrong-service manifest would pollute the topology with
                # foreign nodes. Both are permanent failures (a re-upload is required).
                if not nodes:
                    self._publisher.publish_failed(
                        release_id=release_id,
                        failure_kind=ParseFailureKind.INVALID_ARTIFACT,
                        detail=f"{mf.declared_service}: {parser.empty_detail}",
                        failed_nodes=[],
                    )
                    return

                offending = {n.service_name for n in nodes if n.service_name != mf.declared_service}
                if offending:
                    self._publisher.publish_failed(
                        release_id=release_id,
                        failure_kind=ParseFailureKind.INVALID_ARTIFACT,
                        detail=(
                            f"{mf.declared_service}: manifest contains nodes for "
                            f"{sorted(offending)}"
                        ),
                        failed_nodes=[],
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
            if n.node_type != NodeType.DBT_TEST  # a test writes no relation: nothing can reference it
        ])
        lookup = registry.to_lookup()
        candidate_schema = candidate_schema_name(release_id)
        ctx = RewriteContext(
            release_id=release_id,
            registry=lookup,
            candidate_schema=candidate_schema,
            dialect=self._dialect,
        )

        # Resolve every node before building anything: a release with one broken
        # node is rejected whole, and reporting every broken node at once spares
        # the operator a fix-push-reject loop per node.
        failed: list[FailedNode] = []
        for node in all_nodes:
            try:
                node.upstream_deps = resolve_upstream_deps(node, lookup, dialect=self._dialect)
            except (UnqualifiedTableReferenceError, InvalidCompiledSqlError) as exc:
                failed.append(FailedNode(
                    node_id=node.unique_id,
                    kind=exc.kind,
                    service=node.service_name,
                    file_path=node.original_file_path,
                    node_type=node.node_type,
                    detail=str(exc),
                ))
        if failed:
            self._publisher.publish_failed(
                release_id=release_id,
                failure_kind=_highest_precedence([n.kind for n in failed]),
                detail=_summary(failed),
                failed_nodes=failed,
            )
            return

        topology: list[dict] = []
        for node in all_nodes:
            builder = self._artifact_builders.get(node.runtime)
            if builder is None:
                # Unreachable with a correctly wired composition root; failing
                # closed here beats publishing a node with no validation input.
                self._publisher.publish_failed(
                    release_id=release_id,
                    failure_kind=ParseFailureKind.INTERNAL,
                    detail=(
                        f"{node.unique_id}: no candidate-artifact builder for "
                        f"runtime {node.runtime!r}"
                    ),
                    failed_nodes=[],
                )
                return

            # A node whose validation input never landed must not be published:
            # release-controller would derive a key to an object that is not there.
            try:
                builder.build(node, ctx)
            except Exception as exc:
                self._reject_internal_unless_infra(release_id, exc)
                return

            entry = {
                "unique_id":           node.unique_id,
                "schema_name":         node.schema_name,
                "table_name":          node.table_name,
                "resolved_relation_id": node.resolved_relation_id,
                "service_name":        node.service_name,
                "node_type":           node.node_type,
                "test_count":          node.test_count,
                "content_hash":        node.content_hash,
                "image_tag":           node.image_tag,
                "original_file_path":  node.original_file_path,
                "upstream_unique_ids": [
                    dep.unique_id for dep in node.upstream_deps
                ],
                "schedule":            node.schedule_name,
            }
            if node.secret_ref:
                entry["secret_ref"] = node.secret_ref
            topology.append(entry)

        bundle = build_code_bundle(
            release_id,
            [n for n in all_nodes if n.node_type != NodeType.DBT_TEST],  # tests are never fix targets nor source the agent reads
            shared_code,
        )
        try:
            code_bundle_uri = self._bundle_uploader.upload(release_id, bundle)
        except Exception as exc:
            self._reject_internal_unless_infra(release_id, exc)
            return

        self._publish(release_id, topology=topology, code_bundle_uri=code_bundle_uri)

    def _publish(self, release_id: str, *, topology: list[dict], code_bundle_uri: str) -> None:
        """Write the release's topology artifact, then publish the reference to
        it. The write follows the same outage rule as every other write."""
        try:
            artifact = self._artifact_writer.write(
                tenant_id=DEFAULT_TENANT_ID, release_id=release_id, nodes=topology,
            )
        except Exception as exc:
            self._reject_internal_unless_infra(release_id, exc)
            return

        self._publisher.publish_ok(
            release_id=release_id, artifact=artifact, code_bundle_uri=code_bundle_uri,
        )
        logger.info(
            "candidate: parse complete",
            extra={
                "release_id": release_id,
                "published_nodes": artifact.node_count,
                "topology_uri": artifact.uri,
            },
        )

    def _reject_internal_unless_infra(self, release_id: str, exc: Exception) -> None:
        """Re-raise an S3 outage (is_infrastructure_error) so the consumer
        pauses and retries; otherwise publish a failed/internal result for this
        release."""
        if self._is_infrastructure_error(exc):
            raise exc
        self._publisher.publish_failed(
            release_id=release_id,
            failure_kind=ParseFailureKind.INTERNAL,
            detail=str(exc),
            failed_nodes=[],
        )
