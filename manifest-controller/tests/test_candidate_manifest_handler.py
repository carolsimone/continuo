import json
from pathlib import Path
from unittest.mock import MagicMock, create_autospec
import pytest
from adapters.sources import ManifestSource
from domain.model import ManifestFile
from domain.exceptions import UnqualifiedTableReferenceError
from service.candidate_manifest_handler import CandidateManifestHandler

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


@pytest.fixture
def resolved_topology():
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    publisher = MagicMock()
    uploader = _make_uploader()
    CandidateManifestHandler(source=source, publisher=publisher, uploader=uploader).handle(release_id="rel-1")
    publisher.publish_ok.assert_called_once()
    assert publisher.publish_ok.call_args.kwargs["release_id"] == "rel-1"
    return publisher.publish_ok.call_args.kwargs["topology"]


@pytest.fixture
def handler_with_mocks():
    """Return (handler, publisher, uploader) with a one-node manifest source."""
    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()
    uploader = _make_uploader()
    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=uploader)
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


def test_handle_publishes_ok_with_content_hash_from_node_checksum(resolved_topology):
    # Each published node carries dbt's native per-node source checksum,
    # read from checksum.checksum in the source manifest fixtures.
    expected = {}
    for name in ("manifest_service1.json", "manifest_service2.json"):
        manifest = json.loads((FIXTURES / name).read_text())
        for node in manifest["nodes"].values():
            expected[node["name"]] = node["checksum"]["checksum"]

    for node in resolved_topology:
        assert node["content_hash"], f"{node['table_name']} missing content_hash"
        assert node["content_hash"] == expected[node["table_name"]]


def test_handle_publishes_ok_with_empty_topology_when_no_manifests():
    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = []
    publisher = MagicMock()

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=_make_uploader())
    handler.handle(release_id="rel-empty")

    publisher.publish_ok.assert_called_once_with(release_id="rel-empty", topology=[])
    publisher.publish_failed.assert_not_called()


def test_handle_publishes_failed_on_unqualified_reference(monkeypatch):
    def _raise(node, lookup):
        raise UnqualifiedTableReferenceError(table_name="orders", node_table_name="fact")

    monkeypatch.setattr(
        "service.candidate_manifest_handler.resolve_upstream_deps", _raise
    )

    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=_make_uploader())
    handler.handle(release_id="rel-fail")  # must NOT raise

    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "UnqualifiedTableReference"
    assert "orders" in publisher.publish_failed.call_args.kwargs["error_detail"]
    publisher.publish_ok.assert_not_called()


def test_handle_publishes_failed_on_malformed_manifest(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("not json {{{")

    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = [
        ManifestFile(path=str(bad), version="v1", image_tag="")
    ]
    publisher = MagicMock()

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=_make_uploader())
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

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=_make_uploader())
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

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=_make_uploader())
    handler.handle(release_id="rel-empty-fqn")  # must NOT raise

    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "MalformedManifest"
    publisher.publish_ok.assert_not_called()


def test_handle_propagates_transient_redis_error():
    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()
    publisher.publish_ok.side_effect = ConnectionError("redis down")

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=_make_uploader())
    with pytest.raises(ConnectionError):
        handler.handle(release_id="rel-1")


def test_handle_calls_source_cleanup_after_publish():
    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=_make_uploader())
    handler.handle(release_id="rel-1")

    source.cleanup.assert_called_once()


def test_handle_calls_source_cleanup_even_on_publish_failed(monkeypatch):
    def _raise(node, lookup):
        raise UnqualifiedTableReferenceError(table_name="orders", node_table_name="fact")

    monkeypatch.setattr(
        "service.candidate_manifest_handler.resolve_upstream_deps", _raise
    )

    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=_make_uploader())
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

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=_make_uploader())
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

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=_make_uploader())
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

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=_make_uploader())
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

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=_make_uploader())
    handler.handle(release_id="rel-legacy")

    # No service declared — falls through to publish_ok with an empty topology
    # (the existing "no manifests found" path is bypassed because list_manifests
    # returned one file; an empty node list is still valid for undeclared sources).
    publisher.publish_failed.assert_not_called()
    publisher.publish_ok.assert_called_once()


# ---------------------------------------------------------------------------
# candidate_sql_uri: upload-per-node and fatal-on-upload-failure
# ---------------------------------------------------------------------------

def test_publishes_candidate_sql_uri_and_uploads(handler_with_mocks):
    """Each node's compiled SQL is uploaded to S3; the topology carries
    candidate_sql_uri (not the inline candidate_sql string)."""
    handler, publisher, uploader = handler_with_mocks
    uploader.upload.return_value = "s3://continuo/candidate-sql/rel-1/public.orders.sql"

    handler.handle(release_id="rel-1")

    publisher.publish_ok.assert_called_once()
    node = publisher.publish_ok.call_args.kwargs["topology"][0]
    assert "candidate_sql" not in node
    assert node["candidate_sql_uri"] == "s3://continuo/candidate-sql/rel-1/public.orders.sql"


def test_upload_failure_is_fatal(handler_with_mocks):
    """An S3 upload error is fatal — publish_failed is called and publish_ok is not."""
    handler, publisher, uploader = handler_with_mocks
    uploader.upload.side_effect = RuntimeError("s3 down")

    handler.handle(release_id="rel-1")

    publisher.publish_ok.assert_not_called()
    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "CandidateSqlUploadFailed"


def test_handle_calls_source_cleanup_even_on_upload_failure():
    """source.cleanup() must run even when an upload fails mid-flight."""
    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()
    uploader = _make_uploader()
    uploader.upload.side_effect = RuntimeError("s3 down")

    handler = CandidateManifestHandler(source=source, publisher=publisher, uploader=uploader)
    handler.handle(release_id="rel-upload-fail")

    source.cleanup.assert_called_once()
