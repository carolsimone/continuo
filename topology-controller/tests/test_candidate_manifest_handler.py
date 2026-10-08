import hashlib
import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, create_autospec
import pytest
import yaml
import botocore.exceptions
from adapters.redis.error_class import is_infrastructure
from domain.model import FailedNode, ManifestFile, ManifestKind, Runtime
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from service import candidate_artifacts, candidate_manifest_handler
from service.candidate_artifacts import DbtSqlArtifactBuilder, PythonSpecArtifactBuilder
from service.candidate_manifest_handler import CandidateManifestHandler
from service.content_hash import content_hash_fold
from service.ports import ManifestSourcePort
from domain.contract_vocabulary import ParseFailureKind
from tests.fakes import RecordingArtifactWriter

FIXTURES = Path(__file__).parent / "fixtures"


def _make_source(*entries):
    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [
        ManifestFile(path=str(FIXTURES / name), image_tag="")
        for name in entries
    ]
    return source


def _make_uploader(uri=""):
    uploader = MagicMock()
    uploader.upload.return_value = uri
    return uploader


class FakeBundleUploader:
    """Records (release_id, bundle) calls and returns a canned URI.

    Pass `fail` to make `upload` raise instead — the failing variant used to
    exercise the CodeBundleUploadFailed path.
    """

    def __init__(self, uri: str = "s3://continuo/code-bundles/rel-1/bundle.json", fail: Exception | None = None):
        self.uploads: list[tuple[str, dict]] = []
        self._uri = uri
        self._fail = fail

    def upload(self, release_id: str, bundle: dict) -> str:
        if self._fail is not None:
            raise self._fail
        self.uploads.append((release_id, bundle))
        return self._uri


def _handler(source, publisher, uploader, bundle_uploader=None, dialect="postgres",
             image_tags=None, artifact_writer=None) -> CandidateManifestHandler:
    """Build a handler pinned to the postgres dialect, a fake bundle uploader,
    a recording artifact writer and an empty image-tag map unless a test names
    otherwise. Outages are recognised by the consumer's own classifier.

    `uploader` is the dbt candidate-SQL uploader; it is wrapped in the dbt
    artifact builder here so these cases keep asserting directly on the upload
    calls the builder makes.
    """
    return CandidateManifestHandler(
        source=source,
        publisher=publisher,
        bundle_uploader=bundle_uploader if bundle_uploader is not None else FakeBundleUploader(),
        artifact_writer=artifact_writer if artifact_writer is not None else RecordingArtifactWriter(),
        artifact_builders={Runtime.DBT: DbtSqlArtifactBuilder(uploader)},
        dialect=dialect,
        image_tags=image_tags if image_tags is not None else {},
        is_infrastructure_error=is_infrastructure,
    )


def _dispatch_handler(source, publisher, artifact_builders=None, dialect="postgres", artifact_writer=None):
    """A handler for the parse-dispatch cases.

    These cases all fail (or are rejected) before any node reaches the artifact
    builders, so the default map carries dbt only; tests that publish a python
    node pass their own map.
    """
    return CandidateManifestHandler(
        source=source,
        publisher=publisher,
        bundle_uploader=FakeBundleUploader(),
        artifact_writer=artifact_writer if artifact_writer is not None else RecordingArtifactWriter(),
        artifact_builders=artifact_builders or {Runtime.DBT: DbtSqlArtifactBuilder(_make_uploader())},
        dialect=dialect,
        image_tags={},
        is_infrastructure_error=is_infrastructure,
    )


def _python_handler(source, publisher, spec_uploader=None, dialect="postgres", artifact_writer=None):
    """A handler wired for both kinds, for cases that publish a python node."""
    return _dispatch_handler(
        source, publisher, dialect=dialect, artifact_writer=artifact_writer,
        artifact_builders={
            Runtime.DBT: DbtSqlArtifactBuilder(_make_uploader()),
            Runtime.PYTHON: PythonSpecArtifactBuilder(
                spec_uploader if spec_uploader is not None
                else _make_uploader("s3://continuo/candidate-sql/rel-1/candidate_test_schema.py_metrics.json")),
        },
    )


@pytest.fixture
def resolved_topology():
    source = _make_source(
        "manifest_service1.json",
        "manifest_service2.json",
    )
    publisher = MagicMock()
    writer = RecordingArtifactWriter()
    _handler(source, publisher, _make_uploader(), artifact_writer=writer).handle(release_id="rel-1")
    publisher.publish_ok.assert_called_once()
    assert publisher.publish_ok.call_args.kwargs["release_id"] == "rel-1"
    assert publisher.publish_ok.call_args.kwargs["artifact"] == writer.refs[0]
    return writer.nodes


@pytest.fixture
def handler_with_mocks():
    """Return (handler, publisher, uploader, writer) with a two-node,
    two-manifest source (service1's "users" and service2's "orders", which
    selects from test_schema.users) — this cross-service reference is what
    gives the candidate-schema rewrite something real to do."""
    source = _make_source(
        "manifest_service1.json",
        "manifest_service2.json",
    )
    publisher = MagicMock()
    uploader = _make_uploader()
    writer = RecordingArtifactWriter()
    handler = _handler(source, publisher, uploader, artifact_writer=writer)
    return handler, publisher, uploader, writer


def test_handle_publishes_ok_with_resolved_topology(resolved_topology):
    assert len(resolved_topology) == 2


def test_handle_publishes_ok_with_node_type_on_each_node(resolved_topology):
    valid = {"dbt-model", "dbt-seed", "dbt-snapshot"}
    for node in resolved_topology:
        assert node["node_type"] in valid
    # Both fixtures declare resource_type "model", so both resolve to dbt-model.
    assert {node["node_type"] for node in resolved_topology} == {"dbt-model"}


def test_handle_publishes_ok_with_well_formed_content_hash(resolved_topology):
    # Each published node carries a non-empty content_hash — the three-part
    # sha256:-prefixed fold of source/shared-code/config hashes — derived from,
    # but no longer verbatim equal to, dbt's per-node checksum.
    for node in resolved_topology:
        assert node["content_hash"], f"{node['table_name']} missing content_hash"
        assert node["content_hash"].startswith("sha256:")


def test_handle_publishes_ok_with_empty_topology_when_no_manifests():
    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = []
    publisher = MagicMock()
    writer = RecordingArtifactWriter()

    _handler(source, publisher, _make_uploader(), artifact_writer=writer).handle(release_id="rel-empty")

    assert writer.writes == [{"tenant_id": "default", "release_id": "rel-empty", "nodes": []}]
    publisher.publish_ok.assert_called_once_with(release_id="rel-empty", artifact=writer.refs[0], code_bundle_uri="")
    publisher.publish_failed.assert_not_called()


def _failed_kwargs(publisher):
    publisher.publish_failed.assert_called_once()
    return publisher.publish_failed.call_args.kwargs


def _dbt_node(name, *, original_file_path=""):
    """A minimal valid dbt model node, for manifests built inline where a test
    needs more than one node or a node with a source file_path — neither of
    which manifest_service1.json's single, path-less fixture node has."""
    node = {
        "unique_id": f"model.service-1.{name}",
        "name": name,
        "schema": "test_schema",
        "fqn": ["service-1", name],
        "tags": ["daily"],
        "resource_type": "model",
        "config": {"meta": {"owner": "team-a", "criticality": "CORE"}},
        "compiled_code": "SELECT 1 AS id",
        "checksum": {"name": "sha256", "checksum": f"hash-{name}"},
    }
    if original_file_path:
        node["original_file_path"] = original_file_path
    return node


def _source_with_nodes(tmp_path, *nodes):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"nodes": {n["unique_id"]: n for n in nodes}}))
    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [
        ManifestFile(path=str(manifest), image_tag="")
    ]
    return source


def test_unqualified_reference_publishes_one_failed_node(monkeypatch, tmp_path):
    def _raise(node, lookup, *, dialect):
        raise UnqualifiedTableReferenceError(table_name="orders", node_table_name=node.table_name)

    monkeypatch.setattr("service.candidate_manifest_handler.resolve_upstream_deps", _raise)
    source = _source_with_nodes(tmp_path, _dbt_node("users", original_file_path="models/users.sql"))
    publisher = MagicMock()
    uploader = _make_uploader()

    _handler(source, publisher, uploader).handle(release_id="rel-fail")

    kw = _failed_kwargs(publisher)
    assert kw["failure_kind"] == ParseFailureKind.UNQUALIFIED_REFERENCE
    assert all(n.kind == ParseFailureKind.UNQUALIFIED_REFERENCE for n in kw["failed_nodes"])
    first = kw["failed_nodes"][0]
    assert first.node_id and first.service and first.file_path and first.node_type
    assert "orders" in first.detail
    publisher.publish_ok.assert_not_called()
    uploader.upload.assert_not_called()


def test_invalid_sql_publishes_every_broken_node_not_just_the_first(monkeypatch, tmp_path):
    def _raise(node, lookup, *, dialect):
        raise InvalidCompiledSqlError(node_table_name=node.table_name, detail=f"broken {node.table_name}")

    monkeypatch.setattr("service.candidate_manifest_handler.resolve_upstream_deps", _raise)
    source = _source_with_nodes(tmp_path, _dbt_node("node_a"), _dbt_node("node_b"))
    publisher = MagicMock()

    _handler(source, publisher, _make_uploader()).handle(release_id="rel-fail")

    kw = _failed_kwargs(publisher)
    assert kw["failure_kind"] == ParseFailureKind.INVALID_SQL
    assert len(kw["failed_nodes"]) > 1, "every node raised, every node must be reported"
    assert {n.kind for n in kw["failed_nodes"]} == {ParseFailureKind.INVALID_SQL}
    assert kw["detail"].startswith(f"{len(kw['failed_nodes'])} nodes failed to parse")


def test_mixed_kinds_take_invalid_sql_by_precedence(monkeypatch, tmp_path):
    calls = {"n": 0}

    def _raise(node, lookup, *, dialect):
        calls["n"] += 1
        if calls["n"] == 1:
            raise UnqualifiedTableReferenceError(table_name="x", node_table_name=node.table_name)
        raise InvalidCompiledSqlError(node_table_name=node.table_name, detail="bad")

    monkeypatch.setattr("service.candidate_manifest_handler.resolve_upstream_deps", _raise)
    source = _source_with_nodes(tmp_path, _dbt_node("node_a"), _dbt_node("node_b"))
    publisher = MagicMock()

    _handler(source, publisher, _make_uploader()).handle(release_id="rel-fail")

    kw = _failed_kwargs(publisher)
    assert kw["failure_kind"] == ParseFailureKind.INVALID_SQL
    kinds = {n.kind for n in kw["failed_nodes"]}
    assert kinds == {ParseFailureKind.INVALID_SQL, ParseFailureKind.UNQUALIFIED_REFERENCE}


def test_one_broken_node_still_reports_the_healthy_ones_as_fine(monkeypatch):
    """A single broken node fails the release; the others are resolved and are
    not reported as failed."""
    real = candidate_manifest_handler.resolve_upstream_deps
    calls = {"n": 0}

    def _raise_once(node, lookup, *, dialect):
        calls["n"] += 1
        if calls["n"] == 1:
            raise InvalidCompiledSqlError(node_table_name=node.table_name, detail="bad")
        return real(node, lookup, dialect=dialect)

    monkeypatch.setattr("service.candidate_manifest_handler.resolve_upstream_deps", _raise_once)
    source = _make_source("manifest_service1.json")
    publisher = MagicMock()

    _handler(source, publisher, _make_uploader()).handle(release_id="rel-fail")

    kw = _failed_kwargs(publisher)
    assert len(kw["failed_nodes"]) == 1
    assert kw["detail"].startswith("1 node failed to parse")


def test_handle_publishes_failed_on_malformed_manifest(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("not json {{{")

    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [
        ManifestFile(path=str(bad), image_tag="")
    ]
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-malformed")  # must NOT raise

    kw = _failed_kwargs(publisher)
    assert kw["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    assert kw["failed_nodes"] == []
    publisher.publish_ok.assert_not_called()


def test_handle_publishes_failed_on_missing_nodes_key(tmp_path):
    """Valid JSON but no top-level `nodes` key is a permanent malformed
    manifest — publish failed and ACK, never treat as transient."""
    bad = tmp_path / "no_nodes.json"
    bad.write_text('{"metadata": {}}')

    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [
        ManifestFile(path=str(bad), image_tag="")
    ]
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-no-nodes")  # must NOT raise

    kw = _failed_kwargs(publisher)
    assert kw["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    publisher.publish_ok.assert_not_called()


def test_handle_publishes_failed_on_node_with_empty_fqn(tmp_path):
    """A node whose dbt shape is invalid (empty `fqn`) raises IndexError in
    parse_manifest; that is permanent, so publish failed rather than stranding
    the release as a transient error."""
    bad = tmp_path / "empty_fqn.json"
    bad.write_text(json.dumps({
        "nodes": {
            "model.svc.t": {
                "resource_type": "model",
                "name": "t",
                "schema": "s",
                "fqn": [],
                "config": {"meta": {"owner": "team"}},
                "tags": ["daily"],
            }
        }
    }))

    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [
        ManifestFile(path=str(bad), image_tag="")
    ]
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-empty-fqn")  # must NOT raise

    kw = _failed_kwargs(publisher)
    assert kw["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    publisher.publish_ok.assert_not_called()


def test_handle_propagates_transient_redis_error():
    source = _make_source("manifest_service1.json")
    publisher = MagicMock()
    publisher.publish_ok.side_effect = ConnectionError("redis down")

    handler = _handler(source, publisher, _make_uploader())
    with pytest.raises(ConnectionError):
        handler.handle(release_id="rel-1")


def test_handle_calls_source_cleanup_after_publish():
    source = _make_source("manifest_service1.json")
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-1")

    source.cleanup.assert_called_once()


def test_handle_calls_source_cleanup_even_on_publish_failed(monkeypatch):
    def _raise(node, lookup, *, dialect):
        raise UnqualifiedTableReferenceError(table_name="orders", node_table_name="fact")

    monkeypatch.setattr(
        "service.candidate_manifest_handler.resolve_upstream_deps", _raise
    )

    source = _make_source("manifest_service1.json")
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-fail")

    source.cleanup.assert_called_once()


# ---------------------------------------------------------------------------
# declared_service validation: EmptyManifest, ServiceMismatch, and happy path
# ---------------------------------------------------------------------------

def _make_source_with_declared(entries):
    """Build a fake manifest source returning ManifestFiles with declared_service set.

    entries is a list of (fixture_name, declared_service) pairs.
    """
    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [
        ManifestFile(path=str(FIXTURES / name), image_tag="", declared_service=declared)
        for name, declared in entries
    ]
    return source


def test_handle_publishes_failed_empty_manifest_for_declared_service(tmp_path):
    """A manifest with zero qualifying nodes for a declared service is a permanent
    failure — it would silently retire the entire service if promoted."""
    empty = tmp_path / "empty_nodes.json"
    empty.write_text('{"nodes": {}}')

    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [
        ManifestFile(path=str(empty), image_tag="", declared_service="service-1")
    ]
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-empty-svc")  # must NOT raise

    kw = _failed_kwargs(publisher)
    assert kw["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    assert "service-1" in kw["detail"]
    publisher.publish_ok.assert_not_called()


def test_handle_publishes_failed_service_mismatch(tmp_path):
    """A manifest whose nodes belong to a different service than declared triggers
    a permanent ServiceMismatch failure."""
    # Build a manifest whose node has fqn[0]="service-2" but it is declared as service-1.
    mismatch = tmp_path / "mismatch.json"
    mismatch.write_text(json.dumps({
        "nodes": {
            "model.service-2.orders": {
                "unique_id": "model.service-2.orders",
                "name": "orders",
                "schema": "test_schema",
                "fqn": ["service-2", "orders"],
                "tags": ["daily"],
                "resource_type": "model",
                "config": {"meta": {"owner": "team-b", "criticality": "SECONDARY"}},
                "compiled_code": "SELECT 1",
                "checksum": {"name": "sha256", "checksum": "abc123"},
            }
        }
    }))

    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [
        ManifestFile(path=str(mismatch), image_tag="", declared_service="service-1")
    ]
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-mismatch")  # must NOT raise

    kw = _failed_kwargs(publisher)
    assert kw["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    detail = kw["detail"]
    assert "service-1" in detail
    assert "service-2" in detail
    publisher.publish_ok.assert_not_called()


def test_handle_publishes_ok_matching_declared_service():
    """A non-empty manifest whose nodes all match the declared service succeeds."""
    source = _make_source_with_declared([
        ("manifest_service1.json", "service-1"),
        ("manifest_service2.json", "service-2"),
    ])
    publisher = MagicMock()
    writer = RecordingArtifactWriter()

    _handler(source, publisher, _make_uploader(), artifact_writer=writer).handle(release_id="rel-ok")

    publisher.publish_ok.assert_called_once()
    assert publisher.publish_ok.call_args.kwargs["release_id"] == "rel-ok"
    assert {n["service_name"] for n in writer.nodes} == {"service-1", "service-2"}
    publisher.publish_failed.assert_not_called()


def test_every_node_runs_in_the_image_its_service_is_tagged_with():
    """Each node carries the tag of its own service from the release request,
    joined by service name."""
    source = _make_source_with_declared([
        ("manifest_service1.json", "service-1"),
        ("manifest_service2.json", "service-2"),
    ])
    writer = RecordingArtifactWriter()

    _handler(source, MagicMock(), _make_uploader(), artifact_writer=writer, image_tags={
        "service-1": "reg/service-1:abc", "service-2": "reg/service-2:def",
    }).handle(release_id="rel-1")

    assert {n["service_name"]: n["image_tag"] for n in writer.nodes} == {
        "service-1": "reg/service-1:abc", "service-2": "reg/service-2:def",
    }


def test_a_service_the_request_names_no_tag_for_keeps_an_empty_image_tag():
    source = _make_source_with_declared([
        ("manifest_service1.json", "service-1"),
        ("manifest_service2.json", "service-2"),
    ])
    writer = RecordingArtifactWriter()

    _handler(source, MagicMock(), _make_uploader(), artifact_writer=writer, image_tags={
        "service-1": "reg/service-1:abc",
    }).handle(release_id="rel-1")

    assert {n["service_name"]: n["image_tag"] for n in writer.nodes} == {
        "service-1": "reg/service-1:abc", "service-2": "",
    }


def test_a_request_without_image_tags_fails_the_release_as_internal():
    """Without tags no node can be given the image it runs in. That is
    continuo's own wiring failing, not the user's source: reject the release
    (internal) before fetching anything, so it never sits in parsing."""
    source = _make_source("manifest_service1.json")
    publisher = MagicMock()
    writer = RecordingArtifactWriter()
    handler = CandidateManifestHandler(
        source=source, publisher=publisher, bundle_uploader=FakeBundleUploader(),
        artifact_writer=writer,
        artifact_builders={Runtime.DBT: DbtSqlArtifactBuilder(_make_uploader())},
        dialect="postgres",
        image_tags=None,
        is_infrastructure_error=is_infrastructure,
    )

    handler.handle(release_id="rel-1")

    kw = _failed_kwargs(publisher)
    assert kw["failure_kind"] == ParseFailureKind.INTERNAL
    assert "image_tags" in kw["detail"]
    assert kw["failed_nodes"] == []
    publisher.publish_ok.assert_not_called()
    assert writer.writes == []
    source.list_manifests.assert_not_called()
    source.cleanup.assert_called_once()


def test_handle_skips_declared_service_checks_when_declared_service_empty(tmp_path):
    """When declared_service is empty (legacy/non-per-service source), the
    EmptyManifest and ServiceMismatch checks are skipped entirely."""
    empty = tmp_path / "empty_nodes.json"
    empty.write_text('{"nodes": {}}')

    source = create_autospec(ManifestSourcePort)
    # declared_service="" — same file that triggers EmptyManifest when non-empty
    source.list_manifests.return_value = [
        ManifestFile(path=str(empty), image_tag="", declared_service="")
    ]
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-legacy")

    # No service declared — falls through to publish_ok with an empty topology
    # (the existing "no manifests found" path is bypassed because list_manifests
    # returned one file; an empty node list is still valid for undeclared sources).
    publisher.publish_failed.assert_not_called()
    publisher.publish_ok.assert_called_once()


# ---------------------------------------------------------------------------
# candidate_artifact_uri: upload-per-node and fatal-on-upload-failure
# ---------------------------------------------------------------------------

def test_uploads_the_rewritten_candidate_sql_and_keeps_its_reference_out_of_the_artifact(handler_with_mocks):
    """Each node's candidate SQL is uploaded to S3 — the candidate-schema
    rewritten SQL, so passing "" or the un-rewritten source would fail this
    test — and the artifact carries no reference to it: release-controller
    derives the object's key from the release id, unique_id and node_type."""
    handler, publisher, uploader, writer = handler_with_mocks
    uploader.upload.return_value = "s3://continuo/candidate-sql/rel-1/candidate_test_schema.orders.sql"

    handler.handle(release_id="rel-1")

    publisher.publish_ok.assert_called_once()
    for node in writer.nodes:
        assert "candidate_sql" not in node
        assert "candidate_artifact_uri" not in node
    # service2's "orders" node selects from test_schema.users (service1), so its
    # candidate_sql is genuinely rewritten onto the release's candidate schema.
    sqls = {c.kwargs["unique_id"]: c.kwargs["sql"] for c in uploader.upload.call_args_list}
    assert sqls["test_schema.orders"] == 'SELECT * FROM "_candidate_rel_1".users'


def test_configured_dialect_reaches_the_resolver_and_the_rewriter(monkeypatch):
    """The handler hands its configured dialect to both SQL stages.

    Which dialect is correct is decided at the composition root from the
    warehouse engine; the handler's job is to thread it through rather than let
    either stage fall back to an engine of its own. The fixtures' SQL renders
    identically on postgres and trino, so asserting on the uploaded text would
    pass whatever the handler passed down — these spies pin the wiring instead.
    Rendering itself is pinned in test_rewriter.py.
    """
    seen: dict[str, list[str]] = {"resolve": [], "rewrite": []}
    real_resolve = candidate_manifest_handler.resolve_upstream_deps
    real_rewrite = candidate_artifacts.rewrite_to_candidate_schema

    def spy_resolve(node, registry, *, dialect):
        seen["resolve"].append(dialect)
        return real_resolve(node, registry, dialect=dialect)

    def spy_rewrite(*args, dialect, **kwargs):
        seen["rewrite"].append(dialect)
        return real_rewrite(*args, dialect=dialect, **kwargs)

    monkeypatch.setattr(candidate_manifest_handler, "resolve_upstream_deps", spy_resolve)
    monkeypatch.setattr(candidate_artifacts, "rewrite_to_candidate_schema", spy_rewrite)

    source = _make_source(
        "manifest_service1.json",
        "manifest_service2.json",
    )
    _handler(source, MagicMock(), _make_uploader(), dialect="trino").handle(release_id="rel-1")

    assert seen["resolve"] and set(seen["resolve"]) == {"trino"}
    assert seen["rewrite"] and set(seen["rewrite"]) == {"trino"}


_WRITE_SITES = ["candidate_sql", "code_bundle", "topology_artifact"]
_OUTAGES = [
    botocore.exceptions.EndpointConnectionError(endpoint_url="http://minio:9000"),
    botocore.exceptions.ClientError(
        {"Error": {"Code": "SlowDown", "Message": "reduce your request rate"},
         "ResponseMetadata": {"HTTPStatusCode": 503}}, "PutObject"),
]


def _failing_write_handler(site, failure):
    """A handler over manifest_service1.json whose `site` write raises failure."""
    source = _make_source("manifest_service1.json")
    publisher = MagicMock()
    uploader = _make_uploader()
    bundle_uploader = FakeBundleUploader()
    writer = RecordingArtifactWriter()
    if site == "candidate_sql":
        uploader.upload.side_effect = failure
    elif site == "code_bundle":
        bundle_uploader = FakeBundleUploader(fail=failure)
    else:
        writer = RecordingArtifactWriter(fail=failure)
    handler = _handler(source, publisher, uploader, bundle_uploader=bundle_uploader, artifact_writer=writer)
    return handler, source, publisher


@pytest.mark.parametrize("outage", _OUTAGES, ids=["s3-unreachable", "s3-503"])
@pytest.mark.parametrize("site", _WRITE_SITES)
def test_an_s3_outage_on_any_write_propagates_for_the_consumer_to_wait_out(site, outage):
    """An outage is not the release's fault: the failure reaches the consumer,
    which classifies it as infrastructure, pauses and redelivers the same
    message. Nothing is published, so the release stays parsing until S3 is
    back."""
    handler, source, publisher = _failing_write_handler(site, outage)

    with pytest.raises(type(outage)) as raised:
        handler.handle(release_id="rel-1")

    assert is_infrastructure(raised.value)
    publisher.publish_ok.assert_not_called()
    publisher.publish_failed.assert_not_called()
    source.cleanup.assert_called_once()


@pytest.mark.parametrize("site", _WRITE_SITES)
def test_a_write_failure_that_is_not_an_outage_fails_the_release_as_internal(site):
    """A refused write (here a 403) will not heal by waiting: publish failed so
    the operator sees a rejected release, and never publish a reference to an
    object that did not land."""
    denied = botocore.exceptions.ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"},
         "ResponseMetadata": {"HTTPStatusCode": 403}}, "PutObject")
    handler, _source, publisher = _failing_write_handler(site, denied)

    handler.handle(release_id="rel-1")  # must NOT raise

    publisher.publish_ok.assert_not_called()
    kw = _failed_kwargs(publisher)
    assert kw["failure_kind"] == ParseFailureKind.INTERNAL
    assert kw["failed_nodes"] == []
    assert "AccessDenied" in kw["detail"]


def test_handle_calls_source_cleanup_even_on_upload_failure():
    """source.cleanup() must run even when an upload fails mid-flight."""
    source = _make_source("manifest_service1.json")
    publisher = MagicMock()
    uploader = _make_uploader()
    uploader.upload.side_effect = RuntimeError("s3 down")

    handler = _handler(source, publisher, uploader)
    handler.handle(release_id="rel-upload-fail")

    source.cleanup.assert_called_once()


# ---------------------------------------------------------------------------
# code_bundle_uri: one bundle upload per release, fatal-on-upload-failure
# ---------------------------------------------------------------------------

def test_bundle_uploaded_once_per_release():
    """One code bundle is built and uploaded per release, covering every
    published node; publish_ok receives the uploader's returned URI."""
    source = _make_source(
        "manifest_service1.json",
        "manifest_service2.json",
    )
    publisher = MagicMock()
    bundle_uploader = FakeBundleUploader(uri="s3://continuo/code-bundles/rel-1/bundle.json")
    writer = RecordingArtifactWriter()

    _handler(source, publisher, _make_uploader(), bundle_uploader=bundle_uploader,
             artifact_writer=writer).handle(release_id="rel-1")

    assert len(bundle_uploader.uploads) == 1
    uploaded_release_id, bundle = bundle_uploader.uploads[0]
    assert uploaded_release_id == "rel-1"
    assert set(bundle["nodes"].keys()) == {node["unique_id"] for node in writer.nodes}
    assert publisher.publish_ok.call_args.kwargs["code_bundle_uri"] == "s3://continuo/code-bundles/rel-1/bundle.json"


def test_bundle_upload_failure_publishes_failed():
    """A bundle-upload error is fatal — publish_failed is called with
    CodeBundleUploadFailed and publish_ok is never called."""
    source = _make_source("manifest_service1.json")
    publisher = MagicMock()
    bundle_uploader = FakeBundleUploader(fail=RuntimeError("s3 down"))

    handler = _handler(source, publisher, _make_uploader(), bundle_uploader=bundle_uploader)
    handler.handle(release_id="rel-1")  # must NOT raise

    publisher.publish_ok.assert_not_called()
    kw = _failed_kwargs(publisher)
    assert kw["failure_kind"] == ParseFailureKind.INTERNAL


def test_empty_manifests_publish_empty_bundle_uri():
    """The early no-manifests path publishes code_bundle_uri="" and never
    touches the bundle uploader."""
    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = []
    publisher = MagicMock()
    bundle_uploader = FakeBundleUploader()
    writer = RecordingArtifactWriter()

    _handler(source, publisher, _make_uploader(), bundle_uploader=bundle_uploader,
             artifact_writer=writer).handle(release_id="rel-empty")

    publisher.publish_ok.assert_called_once_with(release_id="rel-empty", artifact=writer.refs[0], code_bundle_uri="")
    assert bundle_uploader.uploads == []


def _manifest_with_single_macro(
    macro_sql: str, node_name: str, service: str, macro_depends_on: list[str] | None = None,
) -> dict:
    """A single-model manifest whose model depends on macro.svc.m1."""
    return {
        "nodes": {
            f"model.svc.{node_name}": {
                "resource_type": "model",
                "name": node_name,
                "schema": "public",
                "fqn": [service],
                "config": {"meta": {"owner": "team"}},
                "tags": ["nightly"],
                "checksum": {"name": "sha256", "checksum": f"source-{node_name}"},
                "depends_on": {"macros": ["macro.svc.m1"]},
            }
        },
        "macros": {
            "macro.svc.m1": {
                "unique_id": "macro.svc.m1",
                "macro_sql": macro_sql,
                "depends_on": {"macros": macro_depends_on or []},
            },
        },
    }


def test_shared_code_namespaced_by_service_for_colliding_unit_ids(tmp_path, caplog):
    """Two manifests shipping the same dbt macro id (macro.svc.m1) but pinning
    different package versions (cross-service skew) are namespaced by service
    in the bundle: BOTH copies survive, each under its own `<service>:<unit_id>`
    key with its own source/checksum, and each manifest's node's code_unit_ids
    points at its own service's namespaced copy — no collision, no warning."""
    first = tmp_path / "first.json"
    first.write_text(json.dumps(_manifest_with_single_macro("SELECT 'v1'", "node_a", "service-a")))
    second = tmp_path / "second.json"
    second.write_text(json.dumps(_manifest_with_single_macro("SELECT 'v2'", "node_b", "service-b")))

    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [
        ManifestFile(path=str(first), image_tag="", declared_service="service-a"),
        ManifestFile(path=str(second), image_tag="", declared_service="service-b"),
    ]
    publisher = MagicMock()
    bundle_uploader = FakeBundleUploader()

    handler = _handler(source, publisher, _make_uploader(), bundle_uploader=bundle_uploader)
    with caplog.at_level(logging.WARNING, logger="service.candidate_manifest_handler"):
        handler.handle(release_id="rel-namespaced")

    publisher.publish_failed.assert_not_called()
    assert len(bundle_uploader.uploads) == 1
    _, bundle = bundle_uploader.uploads[0]

    assert bundle["shared_code"]["service-a:macro.svc.m1"] == {
        "source": "SELECT 'v1'",
        "checksum": hashlib.sha256(b"SELECT 'v1'").hexdigest(),
        "depends_on": [],
    }
    assert bundle["shared_code"]["service-b:macro.svc.m1"] == {
        "source": "SELECT 'v2'",
        "checksum": hashlib.sha256(b"SELECT 'v2'").hexdigest(),
        "depends_on": [],
    }
    assert bundle["nodes"]["public.node_a"]["code_unit_ids"] == ["service-a:macro.svc.m1"]
    assert bundle["nodes"]["public.node_b"]["code_unit_ids"] == ["service-b:macro.svc.m1"]
    assert not any("conflicting shared-code" in rec.getMessage() for rec in caplog.records)
    assert not any("re-defined" in rec.getMessage() for rec in caplog.records)


def test_shared_code_depends_on_entries_namespaced_consistently_with_keys(tmp_path, caplog):
    """A shared-code unit's own `depends_on` list is namespaced with the same
    `<service>:` prefix as the bundle's shared_code map keys — so a consumer
    walking unit->unit edges never needs to re-derive the namespace. Exercises
    the fallback path where declared_service is unset: the namespace is
    derived from the manifest's own node service instead."""
    first = tmp_path / "first.json"
    first.write_text(json.dumps(
        _manifest_with_single_macro("SELECT 1", "node_a", "service-a", macro_depends_on=["macro.svc.m2"])
    ))
    second = tmp_path / "second.json"
    second.write_text(json.dumps(
        _manifest_with_single_macro("SELECT 1", "node_b", "service-b", macro_depends_on=[])
    ))

    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [
        ManifestFile(path=str(first), image_tag=""),
        ManifestFile(path=str(second), image_tag=""),
    ]
    publisher = MagicMock()
    bundle_uploader = FakeBundleUploader()

    handler = _handler(source, publisher, _make_uploader(), bundle_uploader=bundle_uploader)
    with caplog.at_level(logging.WARNING, logger="service.candidate_manifest_handler"):
        handler.handle(release_id="rel-depends-on")

    publisher.publish_failed.assert_not_called()
    assert len(bundle_uploader.uploads) == 1
    _, bundle = bundle_uploader.uploads[0]

    assert bundle["shared_code"]["service-a:macro.svc.m1"] == {
        "source": "SELECT 1",
        "checksum": hashlib.sha256(b"SELECT 1").hexdigest(),
        "depends_on": ["service-a:macro.svc.m2"],
    }
    assert bundle["shared_code"]["service-b:macro.svc.m1"] == {
        "source": "SELECT 1",
        "checksum": hashlib.sha256(b"SELECT 1").hexdigest(),
        "depends_on": [],
    }
    assert not any("conflicting shared-code" in rec.getMessage() for rec in caplog.records)
    assert not any("re-defined" in rec.getMessage() for rec in caplog.records)


# ---------------------------------------------------------------------------
# kind dispatch: one release may carry a dbt manifest and a python contract
# ---------------------------------------------------------------------------

def _python_entry(**overrides):
    entry = {
        "schema": "test_schema", "table": "py_metrics",
        "owner": "team-py", "schedule": "daily", "criticality": "SECONDARY",
        "script": "scripts/py_metrics.py",
        "reads": {"orders": "select id from test_schema.orders"},
        "output_columns": [{"name": "id", "type": "INTEGER", "nullable": False}],
        "source_hash": "aaa111", "shared_code_hash": "bbb222", "config_hash": "ccc333",
    }
    entry.update(overrides)
    entry.setdefault("content_hash", content_hash_fold(
        entry["source_hash"], entry["shared_code_hash"], entry["config_hash"]))
    return entry


def _python_contract(tmp_path, *entries, service="service-py", name="contract.yaml"):
    doc = {"contract_version": 1, "service": service, "nodes": list(entries)}
    path = tmp_path / name
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return str(path)


def _source_of(*files):
    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = list(files)
    return source


def test_a_malformed_python_contract_fails_the_whole_release(tmp_path):
    """The producing CI validates before upload, so a bad entry here means a
    broken pipeline — and a silently dropped node would retire from production
    on promote."""
    publisher = MagicMock()
    source = _source_of(ManifestFile(
        path=_python_contract(tmp_path, _python_entry(criticality="URGENT")),
        declared_service="service-py", kind=ManifestKind.PYTHON,
    ))

    _dispatch_handler(source, publisher).handle(release_id="rel-1")

    publisher.publish_ok.assert_not_called()
    assert publisher.publish_failed.call_args.kwargs["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT


def test_an_empty_python_contract_fails_the_release(tmp_path):
    """An artifact declaring no nodes would silently retire every node of that
    service on promote — the same hazard as an empty dbt manifest, so the same
    guard and the same failure_kind."""
    publisher = MagicMock()
    source = _source_of(ManifestFile(
        path=_python_contract(tmp_path), declared_service="service-py", kind=ManifestKind.PYTHON,
    ))

    _dispatch_handler(source, publisher).handle(release_id="rel-1")

    kwargs = publisher.publish_failed.call_args.kwargs
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    assert "contract declares no nodes" in kwargs["detail"]


def test_a_python_contract_for_another_service_fails_the_release(tmp_path):
    publisher = MagicMock()
    source = _source_of(ManifestFile(
        path=_python_contract(tmp_path, _python_entry(), service="someone-else"),
        declared_service="service-py", kind=ManifestKind.PYTHON,
    ))

    _dispatch_handler(source, publisher).handle(release_id="rel-1")

    assert publisher.publish_failed.call_args.kwargs["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT


def test_an_unknown_kind_fails_the_release_permanently():
    """A kind this build cannot parse is a permanent payload error: report it so
    the operator sees a rejected release, rather than retrying forever."""
    publisher = MagicMock()
    source = _source_of(ManifestFile(
        path="/nonexistent", declared_service="service-x", kind="spark",
    ))

    _dispatch_handler(source, publisher).handle(release_id="rel-1")

    kwargs = publisher.publish_failed.call_args.kwargs
    assert kwargs["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    assert "spark" in kwargs["detail"]


def test_an_empty_kind_fails_the_release_rather_than_parsing_as_dbt():
    """An explicitly empty kind is not a kind this build can parse, so it takes
    the same permanent-failure path as any other unrecognized value. Silently
    reading it as dbt would parse a python contract with the dbt parser and
    surface a detail about a broken dbt manifest, sending the operator
    looking at the wrong artifact entirely."""
    publisher = MagicMock()
    source = _source_of(ManifestFile(
        path="/nonexistent", declared_service="service-x", kind="",
    ))

    _dispatch_handler(source, publisher).handle(release_id="rel-1")

    publisher.publish_ok.assert_not_called()
    assert publisher.publish_failed.call_args.kwargs["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT


def test_a_python_kind_entry_is_published_as_a_python_node(tmp_path):
    publisher = MagicMock()
    writer = RecordingArtifactWriter()
    spec_uploader = _make_uploader("s3://continuo/candidate-sql/rel-1/candidate_test_schema.py_metrics.json")
    source = _source_of(ManifestFile(
        path=_python_contract(tmp_path, _python_entry()), declared_service="service-py", kind=ManifestKind.PYTHON,
    ))

    _python_handler(source, publisher, spec_uploader=spec_uploader, artifact_writer=writer).handle(release_id="rel-1")

    topology = writer.nodes
    assert [n["node_type"] for n in topology] == ["python-node"]
    assert topology[0]["unique_id"] == "test_schema.py_metrics"
    assert topology[0]["original_file_path"] == "scripts/py_metrics.py"
    assert spec_uploader.upload.call_args.kwargs["unique_id"] == "test_schema.py_metrics"


def _published_python_topology(tmp_path, entry):
    writer = RecordingArtifactWriter()
    source = _source_of(ManifestFile(
        path=_python_contract(tmp_path, entry),
        declared_service="service-py", kind=ManifestKind.PYTHON,
    ))
    _python_handler(source, MagicMock(), artifact_writer=writer).handle(release_id="rel-1")
    return writer.nodes


def test_a_python_api_secret_ref_is_published_on_its_topology_entry(tmp_path):
    entry = _python_entry(kind="python-api", reads={}, secret_ref="continuo-api-fx")
    (node,) = _published_python_topology(tmp_path, entry)
    assert node["node_type"] == "python-api"
    assert node["secret_ref"] == "continuo-api-fx"
    assert node["upstream_unique_ids"] == []


def test_a_node_without_a_secret_ref_publishes_no_secret_ref_key(tmp_path):
    entry = _python_entry(kind="python-api", reads={})
    (node,) = _published_python_topology(tmp_path, entry)
    assert "secret_ref" not in node


def test_a_release_mixing_old_and_new_python_contracts_publishes_python_node(tmp_path):
    """Every release re-parses every service's stored production contract, so a
    contract written by an older runtime (kind python-model) sits beside a new
    one (kind python-node) in the same release. Both resolve to python-node."""
    publisher = MagicMock()
    writer = RecordingArtifactWriter()
    old = ManifestFile(
        path=_python_contract(
            tmp_path, _python_entry(table="py_old", kind="python-model"),
            service="service-old", name="old.yaml"),
        declared_service="service-old", kind=ManifestKind.PYTHON,
    )
    new = ManifestFile(
        path=_python_contract(
            tmp_path, _python_entry(table="py_new", kind="python-node"),
            service="service-new", name="new.yaml"),
        declared_service="service-new", kind=ManifestKind.PYTHON,
    )

    _python_handler(_source_of(old, new), publisher, artifact_writer=writer).handle(release_id="rel-1")

    publisher.publish_failed.assert_not_called()
    topology = writer.nodes
    assert sorted((n["unique_id"], n["node_type"]) for n in topology) == [
        ("test_schema.py_new", "python-node"),
        ("test_schema.py_old", "python-node"),
    ]


# ---------------------------------------------------------------------------
# dbt tests: published as validation-only nodes, excluded from registry+bundle
# ---------------------------------------------------------------------------

def test_dbt_test_is_published_but_not_registered_or_bundled():
    """A dbt test that tests a tracked node becomes its own dbt-test topology
    entry — bind-checked against the candidate schema like a model, so its
    rewritten SQL is uploaded too — but writes no relation (empty
    resolved_relation_id) and is excluded from both the node registry (nothing
    can reference it) and the code bundle (it is never a fix target and never
    read as source)."""
    source = _make_source("manifest_with_test.json")
    publisher = MagicMock()
    uploader = _make_uploader("s3://c/candidate.sql")
    bundle_uploader = FakeBundleUploader()
    writer = RecordingArtifactWriter()

    _handler(source, publisher, uploader, bundle_uploader=bundle_uploader,
             artifact_writer=writer).handle(release_id="rel-1")

    publisher.publish_ok.assert_called_once()
    topology = writer.nodes
    tests = [n for n in topology if n["node_type"] == "dbt-test"]
    assert len(tests) == 1
    t = tests[0]
    assert t["unique_id"].startswith("test.")
    assert t["resolved_relation_id"] == ""
    assert t["upstream_unique_ids"] == ["test_schema.users"]  # resolved from the compiled SQL
    assert "test_schema.users" in {n["unique_id"] for n in topology}
    assert t["unique_id"] in {c.kwargs["unique_id"] for c in uploader.upload.call_args_list}

    _, bundle = bundle_uploader.uploads[0]
    assert all(not uid.startswith("test.") for uid in bundle["nodes"]), \
        "tests are never fix targets and never read as source"

    # the test's SQL was rewritten: the model ref points at the candidate schema
    rewritten = uploader.upload.call_args_list
    assert any('"_candidate_' in call.kwargs["sql"] for call in rewritten)


# ---------------------------------------------------------------------------
# wire-shape pin and mixed-DAG resolution
# ---------------------------------------------------------------------------

def test_the_dbt_artifact_entry_shape_is_frozen(handler_with_mocks):
    """Adding a runtime must not change one byte of what a dbt node writes into
    the artifact. If this fails, a key was added, removed, renamed, or retyped
    — decide deliberately, and change pkg/topologyartifact.Node with it."""
    handler, _publisher, _uploader, writer = handler_with_mocks
    handler.handle(release_id="rel-1")

    entry = next(n for n in writer.nodes if n["unique_id"] == "test_schema.orders")

    assert json.dumps(entry, sort_keys=True) == json.dumps({
        "unique_id": "test_schema.orders",
        "schema_name": "test_schema",
        "table_name": "orders",
        "resolved_relation_id": "test_schema.orders",
        "service_name": "service-2",
        "node_type": "dbt-model",
        "test_count": 0,
        "content_hash": "sha256:dfe4669111f4209fd63db2ad347b82aaaf149563a5f749edb454c70b7c4a3f0b",
        "image_tag": "",
        "original_file_path": "",
        "upstream_unique_ids": ["test_schema.users"],
        "schedule": "daily",
    }, sort_keys=True)


def test_a_release_mixes_dbt_and_python_and_resolves_edges_in_both_directions(tmp_path):
    """The point of one topology: a python node reading a dbt table and a dbt
    node reading a python table must both resolve as upstream edges."""
    publisher = MagicMock()
    spec_uploader = MagicMock()
    spec_uploader.upload.return_value = "s3://continuo/candidate-sql/rel-1/candidate_test_schema.py_metrics.json"
    source = _source_of(
        ManifestFile(path=str(FIXTURES / "manifest_service2.json"), declared_service="service-2"),
        ManifestFile(path=_python_contract(tmp_path, _python_entry()), declared_service="service-py", kind=ManifestKind.PYTHON),
        ManifestFile(path=str(FIXTURES / "manifest_service4.json"), declared_service="service-4"),
    )

    dbt_uploader = _make_uploader("s3://continuo/candidate-sql/rel-1/candidate_test_schema.orders.sql")
    writer = RecordingArtifactWriter()
    handler = CandidateManifestHandler(
        source=source, publisher=publisher, bundle_uploader=FakeBundleUploader(),
        artifact_writer=writer,
        artifact_builders={
            Runtime.DBT: DbtSqlArtifactBuilder(dbt_uploader),
            Runtime.PYTHON: PythonSpecArtifactBuilder(spec_uploader),
        },
        dialect="postgres",
        image_tags={},
        is_infrastructure_error=is_infrastructure,
    )
    handler.handle(release_id="rel-1")

    topology = {n["unique_id"]: n for n in writer.nodes}
    assert set(topology) == {
        "test_schema.orders", "test_schema.py_metrics", "test_schema.summary",
    }
    # python reads dbt
    assert topology["test_schema.py_metrics"]["upstream_unique_ids"] == ["test_schema.orders"]
    # dbt reads python
    assert topology["test_schema.summary"]["upstream_unique_ids"] == ["test_schema.py_metrics"]
    # each kind uploaded through its own builder: the spec for the python node,
    # compiled SQL for the dbt nodes
    assert spec_uploader.upload.call_args.kwargs["unique_id"] == "test_schema.py_metrics"
    assert {c.kwargs["unique_id"] for c in dbt_uploader.upload.call_args_list} == {
        "test_schema.orders", "test_schema.summary",
    }
    # the python node's read was redirected at the candidate schema — the
    # rewriter force-quotes only the schema identifier, never the table, so
    # the real form is `"_candidate_rel_1".orders` (schema quoted, table bare).
    spec = spec_uploader.upload.call_args.kwargs["spec"]
    assert '"_candidate_rel_1".orders' in spec["reads"][0]


def test_a_python_node_rides_the_code_bundle_with_its_runtime_marker(tmp_path):
    """The bundle contract is runtime-agnostic by construction; a python node
    needs no new key, because its parsed entry is already its raw_code."""
    publisher = MagicMock()
    bundle_uploader = FakeBundleUploader()
    source = _source_of(ManifestFile(
        path=_python_contract(tmp_path, _python_entry()), declared_service="service-py", kind=ManifestKind.PYTHON,
    ))
    handler = CandidateManifestHandler(
        source=source, publisher=publisher, bundle_uploader=bundle_uploader,
        artifact_writer=RecordingArtifactWriter(),
        artifact_builders={
            Runtime.DBT: DbtSqlArtifactBuilder(_make_uploader()),
            Runtime.PYTHON: PythonSpecArtifactBuilder(_make_uploader("s3://x.json")),
        },
        dialect="postgres",
        image_tags={},
        is_infrastructure_error=is_infrastructure,
    )

    handler.handle(release_id="rel-1")

    _release_id, bundle = bundle_uploader.uploads[0]
    node = bundle["nodes"]["test_schema.py_metrics"]
    assert node["runtime"] == "python"
    assert node["compiled_code"] == ""
    assert '"output_columns"' in node["raw_code"]


def test_upstream_unique_ids_match_node_unique_id_exactly_with_mixed_case(tmp_path):
    """upstream_unique_ids must match the upstream node's unique_id exactly,
    even when the manifest declares mixed-case schema/table names.

    When an upstream is declared as "Analytics.Orders" and a downstream node
    references it, the downstream's upstream_unique_ids entry must be
    "analytics.orders" (lowercased), matching what the upstream node's own
    unique_id property returns. This ensures the orchestrator's Neo4j DEPENDS_ON
    edge matching succeeds.
    """
    manifest_mixed_case = tmp_path / "manifest_mixed_case.json"
    manifest_mixed_case.write_text(json.dumps({
        "nodes": {
            "model.service1.upstream_table": {
                "unique_id": "model.service1.upstream_table",
                "name": "upstream_table",
                "schema": "Analytics",  # Mixed case
                "fqn": ["service1", "upstream_table"],
                "tags": ["daily"],
                "resource_type": "model",
                "config": {
                    "meta": {"owner": "team-a", "criticality": "CORE"}
                },
                "compiled_code": "SELECT 1 AS id",
                "checksum": {"name": "sha256", "checksum": "a1b2c3d4e5f60718293a4b5c6d7e8f90112233445566778899aabbccddeeff00"}
            },
            "model.service1.downstream_table": {
                "unique_id": "model.service1.downstream_table",
                "name": "downstream_table",
                "schema": "BusinessLayer",  # Mixed case
                "fqn": ["service1", "downstream_table"],
                "tags": ["daily"],
                "resource_type": "model",
                "config": {
                    "meta": {"owner": "team-a", "criticality": "CORE"}
                },
                "compiled_code": "SELECT * FROM Analytics.upstream_table",
                "checksum": {"name": "sha256", "checksum": "b2c3d4e5f60718293a4b5c6d7e8f90112233445566778899aabbccddeeff00a1"}
            }
        }
    }))

    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [
        ManifestFile(path=str(manifest_mixed_case), image_tag="")
    ]
    publisher = MagicMock()
    uploader = _make_uploader()
    writer = RecordingArtifactWriter()

    handler = _handler(source, publisher, uploader, artifact_writer=writer)
    handler.handle(release_id="rel-1")

    topology = {n["unique_id"]: n for n in writer.nodes}
    # Both nodes exist with lowercased unique_ids
    assert "analytics.upstream_table" in topology
    assert "businesslayer.downstream_table" in topology

    # The downstream node's upstream_unique_ids must reference the upstream
    # by its exact lowercased unique_id
    upstream = topology["analytics.upstream_table"]
    downstream = topology["businesslayer.downstream_table"]
    assert upstream["unique_id"] == "analytics.upstream_table"
    assert downstream["upstream_unique_ids"] == ["analytics.upstream_table"]
