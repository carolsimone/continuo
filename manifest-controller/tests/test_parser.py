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


def _manifest_with(raw_code: str | None) -> dict:
    node = {
        "resource_type": "model",
        "name": "users",
        "schema": "public",
        "fqn": ["svc_a"],
        "config": {"meta": {"owner": "team-a"}},
        "tags": ["nightly"],
    }
    if raw_code is not None:
        node["raw_code"] = raw_code
    return {"nodes": {"model.svc.users": node}}


def test_parse_falls_back_to_nonempty_hash_when_checksum_absent(tmp_path):
    # A supported node without checksum must still get a non-empty, change-sensitive
    # fingerprint — release-controller uses content_hash as the sole change detector,
    # so an empty hash would make later edits to the node undetectable.
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(_manifest_with(raw_code="select 1")))

    nodes = parse_manifest(str(path), manifest_version="v1")

    assert len(nodes) == 1
    assert nodes[0].content_hash != ""
    assert nodes[0].content_hash.startswith("sha256:")


def test_parse_fallback_hash_is_deterministic_and_change_sensitive(tmp_path):
    def hash_for(raw_code, fname):
        p = tmp_path / fname
        p.write_text(json.dumps(_manifest_with(raw_code=raw_code)))
        return parse_manifest(str(p), manifest_version="v1")[0].content_hash

    h1 = hash_for("select 1", "a.json")
    h1_again = hash_for("select 1", "a_again.json")
    h2 = hash_for("select 2", "b.json")

    assert h1 == h1_again, "same source -> same fallback hash (deterministic)"
    assert h1 != h2, "different source -> different fallback hash (change-sensitive)"


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
