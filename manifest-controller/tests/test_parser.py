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


def _manifest_with_macros(model_macros: dict[str, str]) -> dict:
    """Build a single-model manifest plus a macro graph.

    model_macros maps a macro unique_id the model directly depends on to that
    macro's source SQL. Macro-to-macro edges are added separately by the tests
    that exercise transitive resolution.
    """
    model = {
        "resource_type": "model",
        "name": "orders",
        "schema": "public",
        "fqn": ["svc_a"],
        "config": {"meta": {"owner": "team-a"}},
        "tags": ["nightly"],
        "checksum": {"name": "sha256", "checksum": "modelsource0001"},
        "depends_on": {"macros": list(model_macros.keys())},
    }
    macros = {
        mid: {"unique_id": mid, "macro_sql": sql, "depends_on": {"macros": []}}
        for mid, sql in model_macros.items()
    }
    return {"nodes": {"model.svc.orders": model}, "macros": macros}


def _hash_of(manifest: dict, tmp_path, fname: str) -> str:
    p = tmp_path / fname
    p.write_text(json.dumps(manifest))
    return parse_manifest(str(p), manifest_version="v1")[0].content_hash


def test_macro_change_flips_dependent_model_hash(tmp_path):
    # A model that depends on a macro must re-fingerprint when that macro's
    # source changes, even though the model's own .sql (and dbt checksum) did not.
    before = _manifest_with_macros({"macro.svc.cents_to_dollars": "{% macro x() %}a{% endmacro %}"})
    after = _manifest_with_macros({"macro.svc.cents_to_dollars": "{% macro x() %}b{% endmacro %}"})

    h_before = _hash_of(before, tmp_path, "before.json")
    h_after = _hash_of(after, tmp_path, "after.json")

    assert h_before != h_after, "macro-only change must flip the dependent model's content_hash"


def test_unrelated_macro_change_leaves_model_hash_untouched(tmp_path):
    # Changing a macro the model does NOT depend on must not move its hash.
    base = _manifest_with_macros({"macro.svc.used": "{% macro u() %}same{% endmacro %}"})
    # Add an unrelated macro and then change it; the model still only depends on `used`.
    with_unrelated_v1 = json.loads(json.dumps(base))
    with_unrelated_v1["macros"]["macro.svc.unused"] = {
        "unique_id": "macro.svc.unused", "macro_sql": "v1", "depends_on": {"macros": []},
    }
    with_unrelated_v2 = json.loads(json.dumps(with_unrelated_v1))
    with_unrelated_v2["macros"]["macro.svc.unused"]["macro_sql"] = "v2"

    h1 = _hash_of(with_unrelated_v1, tmp_path, "u1.json")
    h2 = _hash_of(with_unrelated_v2, tmp_path, "u2.json")

    assert h1 == h2, "a change to an unrelated macro must not move the model's content_hash"


def test_transitive_macro_change_flips_model_hash(tmp_path):
    # model -> macro A -> macro B. A change to B (reached only through A) must flip
    # the model's hash, mirroring dbt's transitive state:modified.macros behaviour.
    def manifest(b_sql: str) -> dict:
        return {
            "nodes": {
                "model.svc.orders": {
                    "resource_type": "model", "name": "orders", "schema": "public",
                    "fqn": ["svc_a"], "config": {"meta": {"owner": "team-a"}},
                    "tags": ["nightly"], "checksum": {"name": "sha256", "checksum": "modelsource0001"},
                    "depends_on": {"macros": ["macro.svc.a"]},
                }
            },
            "macros": {
                "macro.svc.a": {"unique_id": "macro.svc.a", "macro_sql": "calls b",
                                "depends_on": {"macros": ["macro.svc.b"]}},
                "macro.svc.b": {"unique_id": "macro.svc.b", "macro_sql": b_sql,
                                "depends_on": {"macros": []}},
            },
        }

    h_before = _hash_of(manifest("leaf v1"), tmp_path, "t1.json")
    h_after = _hash_of(manifest("leaf v2"), tmp_path, "t2.json")

    assert h_before != h_after, "a change to a transitively-reached macro must flip the model's hash"


def test_node_without_macros_keeps_raw_dbt_checksum(tmp_path):
    # The macro-aware scheme must not alter the fingerprint of a model with no
    # macro dependencies: it stays the verbatim dbt checksum (no extra churn).
    h = _hash_of(_manifest_with_macros({}), tmp_path, "n.json")
    assert h == "modelsource0001"


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


def test_parse_manifest_captures_original_file_path(tmp_path):
    manifest = {
        "nodes": {
            "model.svc.stg_orders": {
                "resource_type": "model",
                "name": "stg_orders",
                "schema": "analytics",
                "fqn": ["svc", "staging", "stg_orders"],
                "original_file_path": "models/staging/stg_orders.sql",
                "config": {"meta": {"owner": "team-a"}},
                "tags": ["daily"],
            }
        }
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))

    nodes = parse_manifest(str(path), manifest_version="v1")

    assert len(nodes) == 1
    assert nodes[0].original_file_path == "models/staging/stg_orders.sql"


def _node(name: str, schema: str, fqn: list, owner: str, tags: list) -> dict:
    return {
        "resource_type": "model",
        "name": name,
        "schema": schema,
        "fqn": fqn,
        "config": {"meta": {"owner": owner}},
        "tags": tags,
    }


def _write(tmp_path, manifest: dict) -> str:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return str(path)


def test_parse_manifest_drops_local_stub_nodes(tmp_path):
    manifest = {
        "macros": {},
        "nodes": {
            "model.svc.keep": _node(name="keep", schema="analytics", fqn=["svc", "keep"], owner="o", tags=["daily"]),
            "model.svc.stub": _node(name="stub", schema="analytics", fqn=["svc", "stub"], owner="o", tags=["daily", "local_stub"]),
        },
    }
    path = _write(tmp_path, manifest)
    nodes = parse_manifest(path, "v1")
    names = {n.table_name for n in nodes}
    assert "keep" in names
    assert "stub" not in names


def test_parser_fills_dependency_and_candidate_sql_from_compiled_code(tmp_path):
    node = _node(name="orders", schema="public", fqn=["svc", "orders"], owner="o", tags=["daily"])
    node["compiled_code"] = "select 1 as x"
    manifest = {"nodes": {"model.svc.orders": node}}
    manifest_path = _write(tmp_path, manifest)

    nodes = parse_manifest(manifest_path, "v1")

    model = next(n for n in nodes if n.node_type == "dbt-model")
    assert model.dependency_sqls == ["select 1 as x"]
    assert model.candidate_sql == "select 1 as x"


def test_parser_seed_yields_empty_dependency_sqls():
    # Seed nodes have no compiled_code; manifest_valid.json's my_seed already
    # carries compiled_code="", so no extra fixture needs to be built here.
    nodes = parse_manifest(str(FIXTURES / "manifest_valid.json"), manifest_version="v1")

    seed = next(n for n in nodes if n.node_type == "dbt-seed")
    assert seed.dependency_sqls == []
    assert seed.candidate_sql == ""
