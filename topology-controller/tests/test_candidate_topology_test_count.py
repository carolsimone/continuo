import json
from pathlib import Path
from unittest.mock import MagicMock, create_autospec

from domain.model import ManifestFile, Runtime
from service.candidate_artifacts import DbtSqlArtifactBuilder
from service.candidate_manifest_handler import CandidateManifestHandler
from service.ports import ManifestSourcePort


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


def _test(name, *, attached=None, targets):
    return {
        "resource_type": "test", "name": name, "schema": "analytics_dbt_test__audit",
        "fqn": ["svc_a", name], "attached_node": attached,
        "depends_on": {"nodes": targets, "macros": []},
        "original_file_path": "models/schema.yml", "config": {},
        "checksum": {"checksum": ""}, "compiled_code": "select 1",
    }


def test_candidate_topology_carries_test_count(tmp_path):
    """The node dict published into the candidate topology carries test_count,
    forwarding the count resolved by parse_manifest (task 1) so downstream
    services (release-controller, orchestrator) can consume it. Each tracked
    test is itself published as a dbt-test node alongside the model it tests,
    so the topology holds three entries: the model plus its two tests."""
    manifest = {"macros": {}, "nodes": {
        "model.svc_a.orders": _model("orders"),
        # generic test attached via attached_node
        "test.svc_a.not_null_orders_id": _test(
            "not_null_orders_id", attached="model.svc_a.orders", targets=["model.svc_a.orders"],
        ),
        # singular test attached via depends_on only
        "test.svc_a.assert_orders_positive": _test(
            "assert_orders_positive", targets=["model.svc_a.orders"],
        ),
    }}

    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = [
        ManifestFile(path=_write(tmp_path, manifest), version="v1", image_tag="")
    ]
    publisher = MagicMock()
    uploader = MagicMock()
    uploader.upload.return_value = ""
    bundle_uploader = MagicMock()
    bundle_uploader.upload.return_value = ""

    handler = CandidateManifestHandler(
        source=source, publisher=publisher, bundle_uploader=bundle_uploader,
        artifact_builders={Runtime.DBT: DbtSqlArtifactBuilder(uploader)},
        dialect="postgres",
    )
    handler.handle(release_id="rel-1")

    publisher.publish_ok.assert_called_once()
    topology = publisher.publish_ok.call_args.kwargs["topology"]
    assert len(topology) == 3
    by_id = {n["unique_id"]: n for n in topology}
    assert by_id["analytics.orders"]["test_count"] == 2
    tests = [n for n in topology if n["node_type"] == "dbt-test"]
    assert len(tests) == 2
    assert all(t["test_count"] == 0 for t in tests)
