import hashlib
import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, create_autospec
import pytest
import yaml
from adapters.sources import ManifestSource
from domain.model import ManifestFile, ManifestKind, Runtime
from domain.exceptions import InvalidCompiledSqlError, UnqualifiedTableReferenceError
from service import candidate_artifacts, candidate_manifest_handler
from service.candidate_artifacts import DbtSqlArtifactBuilder
from service.candidate_manifest_handler import CandidateManifestHandler
from service.content_hash import content_hash_fold

FIXTURES = Path(__file__).parent / "fixtures"


def _make_source(*entries):
    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = [
        ManifestFile(path=str(FIXTURES / name), version=version, image_tag="")
        for name, version in entries
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


def _handler(source, publisher, uploader, bundle_uploader=None, dialect="postgres") -> CandidateManifestHandler:
    """Build a handler pinned to the postgres dialect and a fake bundle uploader
    unless a test names otherwise.

    `uploader` is the dbt candidate-SQL uploader; it is wrapped in the dbt
    artifact builder here so these cases keep asserting directly on the upload
    calls the builder makes.
    """
    return CandidateManifestHandler(
        source=source,
        publisher=publisher,
        bundle_uploader=bundle_uploader if bundle_uploader is not None else FakeBundleUploader(),
        artifact_builders={Runtime.DBT: DbtSqlArtifactBuilder(uploader)},
        dialect=dialect,
    )


def _dispatch_handler(source, publisher, artifact_builders=None, dialect="postgres"):
    """A handler for the parse-dispatch cases.

    These cases all fail (or are rejected) before any node reaches the artifact
    builders, so the default map carries dbt only; tests that publish a python
    node pass their own map.
    """
    return CandidateManifestHandler(
        source=source,
        publisher=publisher,
        bundle_uploader=FakeBundleUploader(),
        artifact_builders=artifact_builders or {Runtime.DBT: DbtSqlArtifactBuilder(_make_uploader())},
        dialect=dialect,
    )


@pytest.fixture
def resolved_topology():
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    publisher = MagicMock()
    uploader = _make_uploader()
    _handler(source, publisher, uploader).handle(release_id="rel-1")
    publisher.publish_ok.assert_called_once()
    assert publisher.publish_ok.call_args.kwargs["release_id"] == "rel-1"
    return publisher.publish_ok.call_args.kwargs["topology"]


@pytest.fixture
def handler_with_mocks():
    """Return (handler, publisher, uploader) with a two-node, two-manifest source
    (service1's "users" and service2's "orders", which selects from
    test_schema.users) — this cross-service reference is what gives the
    candidate-schema rewrite something real to do."""
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    publisher = MagicMock()
    uploader = _make_uploader()
    handler = _handler(source, publisher, uploader)
    return handler, publisher, uploader


def test_handle_publishes_ok_with_resolved_topology(resolved_topology):
    assert len(resolved_topology) == 2


def test_handle_publishes_ok_with_unique_id_synthesised_from_schema_and_table(resolved_topology):
    for node in resolved_topology:
        assert node["unique_id"] == f"{node['schema_name']}.{node['table_name']}"


def test_handle_publishes_ok_with_image_tag_empty_string(resolved_topology):
    for node in resolved_topology:
        assert node["image_tag"] == ""


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
    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = []
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-empty")

    publisher.publish_ok.assert_called_once_with(release_id="rel-empty", topology=[], code_bundle_uri="")
    publisher.publish_failed.assert_not_called()


def test_handle_publishes_failed_on_unqualified_reference(monkeypatch):
    def _raise(node, lookup, *, dialect):
        raise UnqualifiedTableReferenceError(table_name="orders", node_table_name="fact")

    monkeypatch.setattr(
        "service.candidate_manifest_handler.resolve_upstream_deps", _raise
    )

    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-fail")  # must NOT raise

    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "UnqualifiedTableReference"
    assert "orders" in publisher.publish_failed.call_args.kwargs["error_detail"]
    publisher.publish_ok.assert_not_called()


def test_handle_publishes_failed_on_invalid_compiled_sql(monkeypatch):
    def _raise(node, lookup, *, dialect):
        raise InvalidCompiledSqlError(node_table_name="table_gg", detail="Invalid expression / Unexpected token.")

    monkeypatch.setattr(
        "service.candidate_manifest_handler.resolve_upstream_deps", _raise
    )

    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-fail")  # must NOT raise

    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "InvalidCompiledSql"
    assert "table_gg" in publisher.publish_failed.call_args.kwargs["error_detail"]
    publisher.publish_ok.assert_not_called()


def test_handle_publishes_failed_on_malformed_manifest(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("not json {{{")

    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = [
        ManifestFile(path=str(bad), version="v1", image_tag="")
    ]
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-malformed")  # must NOT raise

    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "MalformedManifest"
    publisher.publish_ok.assert_not_called()


def test_handle_publishes_failed_on_missing_nodes_key(tmp_path):
    """Valid JSON but no top-level `nodes` key is a permanent malformed
    manifest — publish failed and ACK, never treat as transient."""
    bad = tmp_path / "no_nodes.json"
    bad.write_text('{"metadata": {}}')

    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = [
        ManifestFile(path=str(bad), version="v1", image_tag="")
    ]
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-no-nodes")  # must NOT raise

    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "MalformedManifest"
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

    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = [
        ManifestFile(path=str(bad), version="v1", image_tag="")
    ]
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-empty-fqn")  # must NOT raise

    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "MalformedManifest"
    publisher.publish_ok.assert_not_called()


def test_handle_propagates_transient_redis_error():
    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()
    publisher.publish_ok.side_effect = ConnectionError("redis down")

    handler = _handler(source, publisher, _make_uploader())
    with pytest.raises(ConnectionError):
        handler.handle(release_id="rel-1")


def test_handle_calls_source_cleanup_after_publish():
    source = _make_source(("manifest_service1.json", "v1"))
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

    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-fail")

    source.cleanup.assert_called_once()


# ---------------------------------------------------------------------------
# declared_service validation: EmptyManifest, ServiceMismatch, and happy path
# ---------------------------------------------------------------------------

def _make_source_with_declared(entries):
    """Build a fake ManifestSource returning ManifestFiles with declared_service set.

    entries is a list of (fixture_name, version, declared_service) triples.
    """
    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = [
        ManifestFile(path=str(FIXTURES / name), version=version, image_tag="", declared_service=declared)
        for name, version, declared in entries
    ]
    return source


def test_handle_publishes_failed_empty_manifest_for_declared_service(tmp_path):
    """A manifest with zero qualifying nodes for a declared service is a permanent
    failure — it would silently retire the entire service if promoted."""
    empty = tmp_path / "empty_nodes.json"
    empty.write_text('{"nodes": {}}')

    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = [
        ManifestFile(path=str(empty), version="v1", image_tag="", declared_service="service-1")
    ]
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-empty-svc")  # must NOT raise

    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "EmptyManifest"
    assert "service-1" in publisher.publish_failed.call_args.kwargs["error_detail"]
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

    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = [
        ManifestFile(path=str(mismatch), version="v1", image_tag="", declared_service="service-1")
    ]
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-mismatch")  # must NOT raise

    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "ServiceMismatch"
    detail = publisher.publish_failed.call_args.kwargs["error_detail"]
    assert "service-1" in detail
    assert "service-2" in detail
    publisher.publish_ok.assert_not_called()


def test_handle_publishes_ok_matching_declared_service():
    """A non-empty manifest whose nodes all match the declared service succeeds."""
    source = _make_source_with_declared([
        ("manifest_service1.json", "v1", "service-1"),
        ("manifest_service2.json", "v2", "service-2"),
    ])
    publisher = MagicMock()

    handler = _handler(source, publisher, _make_uploader())
    handler.handle(release_id="rel-ok")

    publisher.publish_ok.assert_called_once()
    assert publisher.publish_ok.call_args.kwargs["release_id"] == "rel-ok"
    topology = publisher.publish_ok.call_args.kwargs["topology"]
    assert len(topology) == 2
    services = {n["service_name"] for n in topology}
    assert services == {"service-1", "service-2"}
    publisher.publish_failed.assert_not_called()


def test_handle_skips_declared_service_checks_when_declared_service_empty(tmp_path):
    """When declared_service is empty (legacy/non-per-service source), the
    EmptyManifest and ServiceMismatch checks are skipped entirely."""
    empty = tmp_path / "empty_nodes.json"
    empty.write_text('{"nodes": {}}')

    source = create_autospec(ManifestSource)
    # declared_service="" — same file that triggers EmptyManifest when non-empty
    source.list_manifests.return_value = [
        ManifestFile(path=str(empty), version="v1", image_tag="", declared_service="")
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

def test_publishes_candidate_artifact_uri_and_uploads(handler_with_mocks):
    """Each node's candidate SQL is uploaded to S3; the topology carries
    candidate_artifact_uri (not the inline candidate_sql string) — and the SQL
    text actually handed to the uploader is the candidate-schema-rewritten
    SQL, so passing "" or the un-rewritten source would fail this test."""
    handler, publisher, uploader = handler_with_mocks
    uploader.upload.return_value = "s3://continuo/candidate-sql/rel-1/public.orders.sql"

    handler.handle(release_id="rel-1")

    publisher.publish_ok.assert_called_once()
    node = publisher.publish_ok.call_args.kwargs["topology"][0]
    assert "candidate_sql" not in node
    assert node["candidate_artifact_uri"] == "s3://continuo/candidate-sql/rel-1/public.orders.sql"

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
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    _handler(source, MagicMock(), _make_uploader(), dialect="trino").handle(release_id="rel-1")

    assert seen["resolve"] and set(seen["resolve"]) == {"trino"}
    assert seen["rewrite"] and set(seen["rewrite"]) == {"trino"}


def test_upload_failure_is_fatal(handler_with_mocks):
    """An S3 upload error is fatal — publish_failed is called and publish_ok is not."""
    handler, publisher, uploader = handler_with_mocks
    uploader.upload.side_effect = RuntimeError("s3 down")

    handler.handle(release_id="rel-1")

    publisher.publish_ok.assert_not_called()
    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "CandidateArtifactUploadFailed"


def test_handle_calls_source_cleanup_even_on_upload_failure():
    """source.cleanup() must run even when an upload fails mid-flight."""
    source = _make_source(("manifest_service1.json", "v1"))
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
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    publisher = MagicMock()
    bundle_uploader = FakeBundleUploader(uri="s3://continuo/code-bundles/rel-1/bundle.json")

    handler = _handler(source, publisher, _make_uploader(), bundle_uploader=bundle_uploader)
    handler.handle(release_id="rel-1")

    assert len(bundle_uploader.uploads) == 1
    uploaded_release_id, bundle = bundle_uploader.uploads[0]
    assert uploaded_release_id == "rel-1"

    topology = publisher.publish_ok.call_args.kwargs["topology"]
    assert set(bundle["nodes"].keys()) == {node["unique_id"] for node in topology}
    assert publisher.publish_ok.call_args.kwargs["code_bundle_uri"] == "s3://continuo/code-bundles/rel-1/bundle.json"


def test_bundle_upload_failure_publishes_failed():
    """A bundle-upload error is fatal — publish_failed is called with
    CodeBundleUploadFailed and publish_ok is never called."""
    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()
    bundle_uploader = FakeBundleUploader(fail=RuntimeError("s3 down"))

    handler = _handler(source, publisher, _make_uploader(), bundle_uploader=bundle_uploader)
    handler.handle(release_id="rel-1")  # must NOT raise

    publisher.publish_ok.assert_not_called()
    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "CodeBundleUploadFailed"


def test_empty_manifests_publish_empty_bundle_uri():
    """The early no-manifests path publishes code_bundle_uri="" and never
    touches the bundle uploader."""
    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = []
    publisher = MagicMock()
    bundle_uploader = FakeBundleUploader()

    handler = _handler(source, publisher, _make_uploader(), bundle_uploader=bundle_uploader)
    handler.handle(release_id="rel-empty")

    publisher.publish_ok.assert_called_once_with(release_id="rel-empty", topology=[], code_bundle_uri="")
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

    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = [
        ManifestFile(path=str(first), version="v1", image_tag="", declared_service="service-a"),
        ManifestFile(path=str(second), version="v1", image_tag="", declared_service="service-b"),
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

    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = [
        ManifestFile(path=str(first), version="v1", image_tag=""),
        ManifestFile(path=str(second), version="v1", image_tag=""),
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
    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = list(files)
    return source


def test_a_malformed_python_contract_fails_the_whole_release(tmp_path):
    """The producing CI validates before upload, so a bad entry here means a
    broken pipeline — and a silently dropped node would retire from production
    on promote."""
    publisher = MagicMock()
    source = _source_of(ManifestFile(
        path=_python_contract(tmp_path, _python_entry(criticality="URGENT")),
        version="v1", declared_service="service-py", kind=ManifestKind.PYTHON,
    ))

    _dispatch_handler(source, publisher).handle(release_id="rel-1")

    publisher.publish_ok.assert_not_called()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "MalformedContract"


def test_an_empty_python_contract_fails_the_release(tmp_path):
    """An artifact declaring no nodes would silently retire every node of that
    service on promote — the same hazard as an empty dbt manifest, so the same
    guard and the same error class."""
    publisher = MagicMock()
    source = _source_of(ManifestFile(
        path=_python_contract(tmp_path), version="v1",
        declared_service="service-py", kind=ManifestKind.PYTHON,
    ))

    _dispatch_handler(source, publisher).handle(release_id="rel-1")

    kwargs = publisher.publish_failed.call_args.kwargs
    assert kwargs["error_class"] == "EmptyManifest"
    assert "contract declares no nodes" in kwargs["error_detail"]


def test_a_python_contract_for_another_service_fails_the_release(tmp_path):
    publisher = MagicMock()
    source = _source_of(ManifestFile(
        path=_python_contract(tmp_path, _python_entry(), service="someone-else"),
        version="v1", declared_service="service-py", kind=ManifestKind.PYTHON,
    ))

    _dispatch_handler(source, publisher).handle(release_id="rel-1")

    assert publisher.publish_failed.call_args.kwargs["error_class"] == "ServiceMismatch"


def test_an_unknown_kind_fails_the_release_permanently():
    """A kind this build cannot parse is a permanent payload error: report it so
    the operator sees a rejected release, rather than retrying forever."""
    publisher = MagicMock()
    source = _source_of(ManifestFile(
        path="/nonexistent", version="v1",
        declared_service="service-x", kind="spark",
    ))

    _dispatch_handler(source, publisher).handle(release_id="rel-1")

    kwargs = publisher.publish_failed.call_args.kwargs
    assert kwargs["error_class"] == "UnknownManifestKind"
    assert "spark" in kwargs["error_detail"]
