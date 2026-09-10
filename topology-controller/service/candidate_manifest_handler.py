import logging
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from domain.model import FailedNode, NodeRegistry, NodeRegistryEntry, NodeType
from service.candidate_artifacts import CandidateArtifactBuilder, RewriteContext
from service.code_bundle import build_code_bundle
from service.manifest_parsers import parser_for
from service.ports import (
    CandidatePublisherPort,
    CodeBundleUploaderPort,
    ManifestSourcePort,
)
from service.resolver import resolve_upstream_deps
from service.rewriter import candidate_schema_name
from streams_contract import ParseFailureKind

logger = logging.getLogger(__name__)


def _highest_precedence(kinds: list[ParseFailureKind]) -> ParseFailureKind:
    """The release-level kind for a set of per-node kinds: the first in contract
    declaration order, which is the precedence the contract defines."""
    order = list(ParseFailureKind)
    return min(kinds, key=order.index)


def _summary(failed: list[FailedNode]) -> str:
    noun = "node" if len(failed) == 1 else "nodes"
    return f"{len(failed)} {noun} failed to parse: " + ", ".join(sorted(n.node_id for n in failed))


class CandidateManifestHandler:
    """Parses a per-release set of dbt manifests and python contracts and
    publishes the resolved candidate topology back to release-controller.

    image_tag is left empty by design; release-controller joins the
    per-service tags from the POST /releases body onto the topology.
    The registry is built in-memory solely for dependency resolution
    and is not persisted anywhere.

    Each node's candidate artifact — the object its validation Job fetches to
    build it as an empty table — is built and uploaded by the artifact_builders
    entry selected by the node's runtime; the topology carries the resulting
    topology keys (e.g. candidate_artifact_uri, an s3:// reference) rather than
    the inline SQL string. Upload failures are fatal — publish_failed is
    called and the handler returns so the consumer ACKs without dangling refs.

    A single code-bundle document (one per release, covering every published
    node plus the shared-code units they depend on) is built and uploaded via
    bundle_uploader immediately before publish_ok; the resulting s3:// URI is
    published as code_bundle_uri. A bundle-upload failure is fatal for the
    same reason as a candidate-SQL upload failure.

    Failures publish status=failed with a typed failure_kind and return
    normally so the consumer ACKs. A node whose SQL sqlglot cannot parse, or
    that references a relation without a schema, is collected rather than
    fatal on first sight: every node is resolved, then one publish_failed
    carries the whole failed set (failure_kind by contract precedence) so the
    operator sees every broken node at once. An unreadable, empty, or
    wrong-service artifact is invalid_artifact; a missing builder or a failed
    S3 write is internal. Neither carries failed nodes.

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
        artifact_builders: dict[str, CandidateArtifactBuilder],
        dialect: str,
    ) -> None:
        self._source = source
        self._publisher = publisher
        self._bundle_uploader = bundle_uploader
        self._artifact_builders = artifact_builders
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
                nodes, mf_shared = parser.parse(mf.path, mf.version, mf.image_tag)
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

            # An upload failure is fatal: publishing a node whose validation
            # input never landed would leave release-controller with a dangling
            # reference. Fail the release so the operator re-triggers it.
            try:
                artifact_keys = builder.build(node, ctx)
            except Exception as exc:
                self._publisher.publish_failed(
                    release_id=release_id,
                    failure_kind=ParseFailureKind.INTERNAL,
                    detail=str(exc),
                    failed_nodes=[],
                )
                return

            topology.append({
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
                **artifact_keys,
            })

        bundle = build_code_bundle(
            release_id,
            [n for n in all_nodes if n.node_type != NodeType.DBT_TEST],  # tests are never fix targets nor source the agent reads
            shared_code,
        )
        try:
            code_bundle_uri = self._bundle_uploader.upload(release_id, bundle)
        except Exception as exc:
            # Same fatal semantics as candidate-SQL uploads: never publish a
            # topology that references a bundle that failed to land.
            self._publisher.publish_failed(
                release_id=release_id,
                failure_kind=ParseFailureKind.INTERNAL,
                detail=str(exc),
                failed_nodes=[],
            )
            return

        self._publisher.publish_ok(
            release_id=release_id, topology=topology, code_bundle_uri=code_bundle_uri,
        )
        logger.info(
            "candidate: parse complete",
            extra={"release_id": release_id, "published_nodes": len(topology)},
        )
