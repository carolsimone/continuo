import json
from pathlib import Path
import pytest
from service.parser import parse_manifest

FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_valid_manifest():
    nodes = parse_manifest(str(FIXTURES / "manifest_valid.json"), manifest_version="v1")
    assert len(nodes) == 4
    names = {n.table_name for n in nodes}
    assert names == {"orders", "users", "my_seed", "my_snapshot"}


def test_parse_sets_node_type_model():
    nodes = parse_manifest(str(FIXTURES / "manifest_valid.json"), manifest_version="v1")
    orders = next(n for n in nodes if n.table_name == "orders")
    assert orders.node_type == "dbt-model"


def test_parse_sets_node_type_seed():
    nodes = parse_manifest(str(FIXTURES / "manifest_valid.json"), manifest_version="v1")
    my_seed = next(n for n in nodes if n.table_name == "my_seed")
    assert my_seed.node_type == "dbt-seed"


def test_parse_sets_node_type_snapshot():
    nodes = parse_manifest(str(FIXTURES / "manifest_valid.json"), manifest_version="v1")
    my_snapshot = next(n for n in nodes if n.table_name == "my_snapshot")
    assert my_snapshot.node_type == "dbt-snapshot"


def test_parse_sets_criticality_default():
    nodes = parse_manifest(str(FIXTURES / "manifest_valid.json"), manifest_version="v1")
    users = next(n for n in nodes if n.table_name == "users")
    assert users.criticality == "SECONDARY"


def test_parse_sets_criticality_from_meta():
    nodes = parse_manifest(str(FIXTURES / "manifest_valid.json"), manifest_version="v1")
    orders = next(n for n in nodes if n.table_name == "orders")
    assert orders.criticality == "CORE"


def test_parse_sets_schedule_from_first_tag():
    nodes = parse_manifest(str(FIXTURES / "manifest_valid.json"), manifest_version="v1")
    assert all(n.schedule_name == "daily" for n in nodes)


def test_parse_sets_service_from_fqn():
    nodes = parse_manifest(str(FIXTURES / "manifest_valid.json"), manifest_version="v1")
    assert all(n.service_name == "service-1" for n in nodes)


def test_parse_skips_node_missing_owner():
    nodes = parse_manifest(str(FIXTURES / "manifest_missing_owner.json"), manifest_version="v1")
    assert nodes == []


def test_parse_skips_node_missing_tags():
    nodes = parse_manifest(str(FIXTURES / "manifest_missing_tags.json"), manifest_version="v1")
    assert nodes == []


def test_parse_seed_without_tags_defaults_schedule_to_seed():
    nodes = parse_manifest(str(FIXTURES / "manifest_seed_no_tags.json"), manifest_version="v1")
    assert len(nodes) == 1
    assert nodes[0].schedule_name == "seed"


def test_parse_stamps_manifest_version_on_all_nodes():
    nodes = parse_manifest(str(FIXTURES / "manifest_valid.json"), manifest_version="v7")
    assert all(n.manifest_version == "v7" for n in nodes)


def test_parse_sets_content_hash_from_checksum(tmp_path):
    manifest = {
        "nodes": {
            "model.svc.users": {
                "resource_type": "model",
                "name": "users",
                "schema": "public",
                "fqn": ["svc_a"],
                "config": {"meta": {"owner": "team-a"}},
                "tags": ["nightly"],
                "checksum": {"name": "sha256", "checksum": "deadbeefcafef00d"},
            }
        }
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))

    nodes = parse_manifest(str(path), manifest_version="v1")

    assert len(nodes) == 1
    assert nodes[0].content_hash == "deadbeefcafef00d"


def test_parse_defaults_content_hash_to_empty_when_absent(tmp_path):
    manifest = {
        "nodes": {
            "model.svc.users": {
                "resource_type": "model",
                "name": "users",
                "schema": "public",
                "fqn": ["svc_a"],
                "config": {"meta": {"owner": "team-a"}},
                "tags": ["nightly"],
            }
        }
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))

    nodes = parse_manifest(str(path), manifest_version="v1")

    assert len(nodes) == 1
    assert nodes[0].content_hash == ""


def test_parse_manifest_stamps_image_tag_on_every_node(tmp_path):
    manifest = {
        "nodes": {
            "model.svc.users": {
                "resource_type": "model",
                "name": "users",
                "schema": "public",
                "fqn": ["svc_a"],
                "config": {"meta": {"owner": "team-a"}},
                "tags": ["nightly"],
            }
        }
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))

    nodes = parse_manifest(str(path), manifest_version="v3", image_tag="abc123-1714300000")

    assert len(nodes) == 1
    assert nodes[0].image_tag == "abc123-1714300000"
    assert nodes[0].manifest_version == "v3"
