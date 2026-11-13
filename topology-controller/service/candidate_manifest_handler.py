import logging
from collections.abc import Callable, Mapping, Sequence
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from domain.external import SystemRelations, unknown_external_nodes
from domain.model import (
    DEFAULT_TENANT_ID,
    FailedNode,
    ManifestKind,
    ManifestNode,
    NodeRegistry,
    NodeType,
    ServiceProvenance,
    SkippedNode,
    TableRef,
)
from service.candidate_artifacts import CandidateArtifactBuilder, RewriteContext
from service.code_bundle import build_code_bundle
from service.manifest_parsers import parser_for
from service.ports import (
    CandidatePublisherPort,
    ClockPort,
    CodeBundleUploaderPort,
    ManifestSourcePort,
    MemoryProbePort,
    NoReleaseObserver,
    ReleaseObserverPort,
    TopologyArtifactWriterPort,
)
from service.release_timings import ReleaseOutcome, ReleaseTimings, Stage
from service.resolver import resolve_upstream_deps
from service.rewriter import candidate_schema_name
from service.table_refs import referenced_tables
from domain.contract_vocabulary import ParseFailureKind

logger = logging.getLogger(__name__)

_NO_IMAGE_TAGS = (
    "the release request carries no image_tags, so no node can be given the "
    "image it runs in"
)


def _no_progress() -> None:
    """The progress callback of a handler nobody watches."""


def _highest_precedence(kinds: list[ParseFailureKind]) -> ParseFailureKind:
    """The release-level kind for a set of per-node kinds: the first in contract
    declaration order, which is the precedence the contract defines."""
    order = list(ParseFailureKind)
    return min(kinds, key=order.index)


def _summary(failed: list[FailedNode]) -> str:
    noun = "node" if len(failed) == 1 else "nodes"
    return f"{len(failed)} {noun} failed to parse: " + ", ".join(sorted(n.node_id for n in failed))


def _artifact_entry(node: ManifestNode) -> dict:
    """The topology artifact's entry for one node: its identity, the relation it
    stands for, the service and image that run it, its schedule and the
    unique_ids it reads. An optional key is present only when the node sets
    it: `materialized` (every dbt node but a test), `full_refresh` (a seed that
    configures one) and `seed_columns` (a seed whose compile extras list its
    header)."""
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
    if node.timeout_seconds:
        entry["timeout_seconds"] = node.timeout_seconds
    if node.relation_name:
        entry["relation_name"] = node.relation_name
    if node.seed_columns_hash:
        entry["seed_columns_hash"] = node.seed_columns_hash
    if node.materialized:
        entry["materialized"] = node.materialized
    if node.full_refresh is not None:
        entry["full_refresh"] = node.full_refresh
    if node.seed_columns:
        entry["seed_columns"] = node.seed_columns
    return entry


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
    node plus the shared-code units they depend on, the tracked dbt tests, and
    each dbt service's provenance with the image tag the request names for it)
    is uploaded via bundle_uploader, then the topology artifact via
    artifact_writer, and only then is the outcome published, so a published
    reference never dangles. An empty release (no manifests) writes an artifact
    with no nodes and publishes an empty code_bundle_uri.

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

    system_relations names the relations the warehouse engine keeps for itself
    (its information schema and catalog tables), supplied by the composition
    root from the same configured engine as dialect.

    Once every node has resolved, each table some node reads and no node builds
    becomes an unknown-external node (unknown_external_nodes), and each node that reads it gains
    an edge to it; the resolve stage times that step too. An unknown-external node
    exists only in the topology artifact: it is in no registry or rewrite
    lookup, has no candidate artifact, no code-bundle entry and no image tag,
    because nothing in the release produces it.

    A dbt node a parse leaves out (no owner, a local stub, an untagged node of
    an installed dbt package) is never a failure: it is logged at INFO as it is
    skipped and counted on the release's line, and release-controller is told
    nothing of it. A node of the service's own project with an owner and no
    schedule tag is not left out: it is a dormant node with an empty schedule,
    in the topology, the code bundle and the candidate schema like any node, and
    the release's line counts the dormant nodes per service.

    A dbt seed whose source part is still dbt's `path` checksum (a seed over
    1 MiB that its service's compile extras do not name) is logged at WARNING,
    one line per seed, after its manifest's parse. A seed node whose extras
    give it a column fingerprint carries it on its topology entry as
    seed_columns_hash; any other node's entry has no such key.

    A node whose build writes a relation named other than its table_name (an
    alias, or a versioned model's "<name>_v<version>") carries that relation on
    its topology entry as relation_name; any other node's entry has no such key.
    Every node's upstreams resolve through the relation each node writes, and a
    reader of an ephemeral dbt model of its own service depends on that model.

    handle() times each stage of the parse (see service.release_timings) with
    clock and memory and returns the record, whether the release was published,
    failed, or empty. A release that ended, published or failed, logs one
    "release parsed" line carrying the record's fields and hands the same
    record to observer; a handler that raised published nothing, logs no such
    line and tells the observer nothing.

    progress is called once after each manifest's parse, once after each node's
    resolve and once after each node's build, a failed one included, so the
    caller can tell a release that is working through its nodes from one that
    has stopped. Between two calls the handler does one manifest's parse (after
    the in-memory bookkeeping of the manifest before it: skip logging,
    shared-code namespacing, service checks), one node's resolve or one node's
    build, except in these stretches, each of which must end within the
    caller's staleness budget:
    - from the start through the first manifest's parse: the image-tag check,
      the listing of every manifest (every manifest's download included) and
      that parse;
    - from the last manifest's parse through the first node's resolve: that
      manifest's own bookkeeping (skip logging, shared-code namespacing,
      service checks), the node registry and its lookup and the rewrite context
      (in-memory work linear in the number of nodes), then that resolve;
    - from the last node's resolve through the first node's build: the
      unknown-external step (in-memory work linear in the number of table references),
      then that build;
    - from the last node's build through the code bundle, its upload, the
      topology artifact and the publish.
    A release that ends early, with no manifest or with a failure, ends in the
    stretch it is in, after the publish of its outcome.
    """

    def __init__(
        self,
        source: ManifestSourcePort,
        publisher: CandidatePublisherPort,
        bundle_uploader: CodeBundleUploaderPort,
        artifact_writer: TopologyArtifactWriterPort,
        artifact_builders: dict[str, CandidateArtifactBuilder],
        dialect: str,
        system_relations: SystemRelations,
        image_tags: Mapping[str, str] | None,
        is_infrastructure_error: Callable[[BaseException], bool],
        clock: ClockPort,
        memory: MemoryProbePort,
        progress: Callable[[], None] = _no_progress,
        observer: ReleaseObserverPort = NoReleaseObserver(),
    ) -> None:
        self._source = source
        self._publisher = publisher
        self._bundle_uploader = bundle_uploader
        self._artifact_writer = artifact_writer
        self._artifact_builders = artifact_builders
        self._dialect = dialect
        self._system_relations = system_relations
        self._image_tags = image_tags
        self._is_infrastructure_error = is_infrastructure_error
        self._clock = clock
        self._memory = memory
        self._progress = progress
        self._observer = observer
        # Replaced by the record of each release handle() starts.
        self._timings = ReleaseTimings("", clock, memory)

    def handle(self, release_id: str) -> ReleaseTimings:
        self._timings = ReleaseTimings(release_id, self._clock, self._memory)
        try:
            self._handle_impl(release_id)
        finally:
            self._source.cleanup()
        if self._timings.outcome is not None:
            logger.info("release parsed", extra=self._timings.log_fields())
            self._observer.release_finished(self._timings)
        return self._timings

    def _handle_impl(self, release_id: str) -> None:
        if self._image_tags is None:
            self._fail(release_id, ParseFailureKind.INTERNAL, _NO_IMAGE_TAGS)
            return

        logger.info(
            "candidate: downloading manifests",
            extra={"release_id": release_id, "manifests": self._source.manifest_count()},
        )
        with self._timings.stage(Stage.DOWNLOAD) as record:
            manifests = self._source.list_manifests()
            record.add(manifests=len(manifests), bytes=sum(m.size_bytes for m in manifests))
        if not manifests:
            logger.warning(
                "candidate: no manifest files found — publishing empty topology",
                extra={"release_id": release_id},
            )
            self._publish(release_id, topology=[], code_bundle_uri="")
            return

        all_nodes = []
        shared_code: dict[str, dict] = {}
        provenance: dict[str, dict] = {}
        for mf in manifests:
            parser = parser_for(mf.kind)
            if parser is None:
                # A kind this build cannot parse is permanent: re-delivery
                # cannot fix it, and the operator needs to see a rejected
                # release rather than a message retrying forever.
                self._fail(
                    release_id,
                    ParseFailureKind.INVALID_ARTIFACT,
                    f"{mf.declared_service or mf.path}: unknown manifest kind {mf.kind!r}",
                )
                return

            parse_stage = Stage.PYTHON_PARSE if mf.kind == ManifestKind.PYTHON else Stage.DBT_PARSE
            manifest_skips: list[SkippedNode] = []
            try:
                with self._timings.stage(parse_stage, manifests=1) as record:
                    parsed = parser.parse(mf, skipped=manifest_skips)
                    record.add(nodes=len(parsed.nodes))
            except parser.permanent_errors as exc:
                # Invalid JSON or yaml, a missing required key, or a malformed
                # node are all permanent — re-delivery cannot fix them, so
                # report failed and let the consumer ACK. Transient errors
                # (e.g. a download/IO failure) are deliberately not caught here
                # so they stay pending.
                self._fail(release_id, ParseFailureKind.INVALID_ARTIFACT, f"{mf.path}: {exc!r}")
                return
            self._progress()
            nodes, mf_shared = parsed.nodes, parsed.shared_code

            # Reported before the declared-service checks below: a manifest
            # whose every node was skipped fails as empty, and its skips are
            # then the only trace of why.
            self._record_parse(release_id, nodes, manifest_skips)

            # Namespace this manifest's shared-code units by service so two
            # manifests pinning different versions of the same dbt package
            # never collide on a bare macro id — each service's copy of a
            # same-named macro gets its own bundle entry, and every node keeps
            # pointing at the copy its own manifest actually hashed against.
            # Per-node hashes fold each manifest's own copy of a unit already
            # (see dbt_hashing.shared_units), so this never changes a hash —
            # it only disambiguates which copy the bundle records for a unit id.
            namespace = mf.declared_service or (nodes[0].service_name if nodes else "")
            if parsed.provenance is not None and namespace:
                provenance[namespace] = self._service_provenance(release_id, namespace, parsed.provenance)

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
                    self._fail(
                        release_id,
                        ParseFailureKind.INVALID_ARTIFACT,
                        f"{mf.declared_service}: {parser.empty_detail}",
                    )
                    return

                offending = {n.service_name for n in nodes if n.service_name != mf.declared_service}
                if offending:
                    self._fail(
                        release_id,
                        ParseFailureKind.INVALID_ARTIFACT,
                        f"{mf.declared_service}: manifest contains nodes for {sorted(offending)}",
                    )
                    return

            all_nodes.extend(nodes)

        with self._timings.stage(Stage.REGISTRY) as record:
            registry = NodeRegistry.of_nodes(all_nodes)
            lookup = registry.to_lookup()
            ephemeral_ctes = registry.ephemeral_ctes()
            record.add(entries=len(registry.entries))
        candidate_schema = candidate_schema_name(release_id)
        ctx = RewriteContext(
            release_id=release_id,
            registry=lookup,
            candidate_schema=candidate_schema,
            dialect=self._dialect,
            system_relations=self._system_relations,
            timings=self._timings,
        )

        # Resolve every node before building anything: a release with one broken
        # node is rejected whole, and reporting every broken node at once spares
        # the operator a fix-push-reject loop per node.
        failed: list[FailedNode] = []
        # Every table each node's SQL names, by node unique_id, for each node
        # that writes a relation (a dbt test writes none). unknown_external_nodes
        # decides which of them no node of the release builds.
        reads: dict[str, list[TableRef]] = {}
        unknown_external: list[ManifestNode] = []
        with self._timings.stage(Stage.RESOLVE, nodes=len(all_nodes)):
            for node in all_nodes:
                try:
                    ctes: set[str] = set()
                    refs = referenced_tables(node, dialect=self._dialect, ctes=ctes)
                    # A system relation is never a node a release builds, so it
                    # resolves to no dependency, even when a node builds a table
                    # with the same schema and name.
                    node.upstream_deps = resolve_upstream_deps(
                        [ref for ref in refs if not self._system_relations.matches(ref)], lookup,
                        service_name=node.service_name, ctes=ctes, ephemeral_ctes=ephemeral_ctes,
                    )
                    if node.node_type != NodeType.DBT_TEST:
                        reads[node.unique_id] = refs
                except (UnqualifiedTableReferenceError, InvalidCompiledSqlError) as exc:
                    failed.append(FailedNode(
                        node_id=node.unique_id,
                        kind=exc.kind,
                        service=node.service_name,
                        file_path=node.original_file_path,
                        node_type=node.node_type,
                        detail=str(exc),
                    ))
                self._progress()
            if not failed:
                # Adds the edges to the readers before their entries are built below.
                unknown_external = unknown_external_nodes(all_nodes, reads, system=self._system_relations)
                self._timings.add_nodes(unknown_external)
        if failed:
            self._fail(
                release_id,
                _highest_precedence([n.kind for n in failed]),
                _summary(failed),
                failed,
            )
            return

        topology: list[dict] = []
        for node in all_nodes:
            builder = self._artifact_builders.get(node.runtime)
            if builder is None:
                # Unreachable with a correctly wired composition root; failing
                # closed here beats publishing a node with no validation input.
                self._fail(
                    release_id,
                    ParseFailureKind.INTERNAL,
                    f"{node.unique_id}: no candidate-artifact builder for runtime {node.runtime!r}",
                )
                return

            # A node whose validation input never landed must not be published:
            # release-controller would derive a key to an object that is not there.
            try:
                builder.build(node, ctx)
            except Exception as exc:
                self._reject_internal_unless_infra(release_id, exc)
                return
            finally:
                self._progress()

            topology.append(_artifact_entry(node))

        topology.extend(_artifact_entry(node) for node in unknown_external)

        upload_error: Exception | None = None
        with self._timings.stage(Stage.CODE_BUNDLE):
            bundle = build_code_bundle(
                release_id,
                # tests are never fix targets nor source the agent reads: they ride in the tests map
                [n for n in all_nodes if n.node_type != NodeType.DBT_TEST],
                shared_code,
                tests=[n for n in all_nodes if n.node_type == NodeType.DBT_TEST],
                provenance=provenance,
            )
            try:
                code_bundle_uri = self._bundle_uploader.upload(release_id, bundle)
            except Exception as exc:
                upload_error = exc
        if upload_error is not None:
            # Handled after the stage so the failure's publish is timed as its
            # own stage, not as part of the bundle's.
            self._reject_internal_unless_infra(release_id, upload_error)
            return

        self._publish(release_id, topology=topology, code_bundle_uri=code_bundle_uri)

    def _publish(self, release_id: str, *, topology: list[dict], code_bundle_uri: str) -> None:
        """Write the release's topology artifact, then publish the reference to
        it. The write follows the same outage rule as every other write."""
        try:
            with self._timings.stage(Stage.TOPOLOGY_ARTIFACT, nodes=len(topology)):
                artifact = self._artifact_writer.write(
                    tenant_id=DEFAULT_TENANT_ID, release_id=release_id, nodes=topology,
                )
        except Exception as exc:
            self._reject_internal_unless_infra(release_id, exc)
            return

        with self._timings.stage(Stage.PUBLISH):
            self._publisher.publish_ok(
                release_id=release_id, artifact=artifact, code_bundle_uri=code_bundle_uri,
            )
        self._timings.outcome = ReleaseOutcome.OK

    def _service_provenance(self, release_id: str, service: str, parsed: ServiceProvenance) -> dict:
        """Log each of a dbt service's seeds that keeps dbt's path checksum and
        return the service's bundle provenance entry, with the image tag the
        release request names for the service."""
        for seed in parsed.path_checksum_seeds:
            logger.warning(
                "candidate: seed keeps dbt's path checksum until its service's rebaseline release",
                extra={"release_id": release_id, "service": service, "seed": seed},
            )
        return {
            "dbt_version": parsed.dbt_version,
            "image_tag": self._image_tags.get(service, ""),
            "packages": dict(parsed.packages),
            "path_checksum_seeds": list(parsed.path_checksum_seeds),
        }

    def _record_parse(self, release_id: str, nodes: list[ManifestNode], skips: list[SkippedNode]) -> None:
        """Log each node a manifest's parse skipped, then add the manifest's
        skips and nodes to the release's counts."""
        for skip in skips:
            logger.info(
                "candidate: node skipped",
                extra={
                    "release_id": release_id,
                    "service": skip.service,
                    "node_id": skip.node_id,
                    "reason": skip.reason.value,
                },
            )
        self._timings.add_skips(skips)
        self._timings.add_nodes(nodes)

    def _reject_internal_unless_infra(self, release_id: str, exc: Exception) -> None:
        """Re-raise an S3 outage (is_infrastructure_error) so the consumer
        pauses and retries; otherwise publish a failed/internal result for this
        release."""
        if self._is_infrastructure_error(exc):
            raise exc
        self._fail(release_id, ParseFailureKind.INTERNAL, str(exc))

    def _fail(
        self,
        release_id: str,
        kind: ParseFailureKind,
        detail: str,
        failed_nodes: Sequence[FailedNode] = (),
    ) -> None:
        """Publish the release as failed, timed as the publish stage."""
        with self._timings.stage(Stage.PUBLISH):
            self._publisher.publish_failed(
                release_id=release_id,
                failure_kind=kind,
                detail=detail,
                failed_nodes=list(failed_nodes),
            )
        self._timings.outcome = ReleaseOutcome.FAILED
        self._timings.failure_kind = kind
