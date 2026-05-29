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


@pytest.fixture
def resolved_topology():
    source = _make_source(
        ("manifest_service1.json", "v1"),
        ("manifest_service2.json", "v2"),
    )
    publisher = MagicMock()
    CandidateManifestHandler(source=source, publisher=publisher).handle(release_id="rel-1")
    publisher.publish_ok.assert_called_once()
    assert publisher.publish_ok.call_args.kwargs["release_id"] == "rel-1"
    return publisher.publish_ok.call_args.kwargs["topology"]


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


def test_handle_publishes_ok_with_empty_topology_when_no_manifests():
    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = []
    publisher = MagicMock()

    handler = CandidateManifestHandler(source=source, publisher=publisher)
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

    handler = CandidateManifestHandler(source=source, publisher=publisher)
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

    handler = CandidateManifestHandler(source=source, publisher=publisher)
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

    handler = CandidateManifestHandler(source=source, publisher=publisher)
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

    handler = CandidateManifestHandler(source=source, publisher=publisher)
    handler.handle(release_id="rel-empty-fqn")  # must NOT raise

    publisher.publish_failed.assert_called_once()
    assert publisher.publish_failed.call_args.kwargs["error_class"] == "MalformedManifest"
    publisher.publish_ok.assert_not_called()


def test_handle_propagates_transient_redis_error():
    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()
    publisher.publish_ok.side_effect = ConnectionError("redis down")

    handler = CandidateManifestHandler(source=source, publisher=publisher)
    with pytest.raises(ConnectionError):
        handler.handle(release_id="rel-1")


def test_handle_calls_source_cleanup_after_publish():
    source = _make_source(("manifest_service1.json", "v1"))
    publisher = MagicMock()

    handler = CandidateManifestHandler(source=source, publisher=publisher)
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

    handler = CandidateManifestHandler(source=source, publisher=publisher)
    handler.handle(release_id="rel-fail")

    source.cleanup.assert_called_once()
