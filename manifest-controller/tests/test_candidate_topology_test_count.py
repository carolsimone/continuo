import json
from pathlib import Path
from unittest.mock import MagicMock, create_autospec

from adapters.sources import ManifestSource
from domain.model import ManifestFile
from service.candidate_manifest_handler import CandidateManifestHandler


def _write(tmp_path, manifest: dict) -> str:
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(manifest))
    return str(p)


def _model(name):
    return {
        "resource_type": "model", "name": name, "schema": "analytics",
        "fqn": ["svc_a", name], "tags": ["daily"],
        "config": {"meta": {"owner": "team@x.com"}},
        "checksum": {"checksum": "abc"}, "compiled_code": "select 1",
    }


def test_candidate_topology_carries_test_count(tmp_path):
    """The node dict published into the candidate topology carries test_count,
    forwarding the count resolved by parse_manifest (task 1) so downstream
    services (release-controller, orchestrator) can consume it."""
    manifest = {"macros": {}, "nodes": {
        "model.svc_a.orders": _model("orders"),
        # generic test attached via attached_node
        "test.svc_a.not_null_orders_id": {
            "resource_type": "test", "attached_node": "model.svc_a.orders",
            "depends_on": {"nodes": ["model.svc_a.orders"]},
        },
        # singular test attached via depends_on only
        "test.svc_a.assert_orders_positive": {
            "resource_type": "test",
            "depends_on": {"nodes": ["model.svc_a.orders"]},
        },
    }}

    source = create_autospec(ManifestSource)
    source.list_manifests.return_value = [
        ManifestFile(path=_write(tmp_path, manifest), version="v1", image_tag="")
    ]
    publisher = MagicMock()
    uploader = MagicMock()
    uploader.upload.return_value = ""
    bundle_uploader = MagicMock()
    bundle_uploader.upload.return_value = ""

    handler = CandidateManifestHandler(
        source=source, publisher=publisher, uploader=uploader, bundle_uploader=bundle_uploader,
    )
    handler.handle(release_id="rel-1")

    publisher.publish_ok.assert_called_once()
    topology = publisher.publish_ok.call_args.kwargs["topology"]
    assert len(topology) == 1
    assert topology[0]["test_count"] == 2
