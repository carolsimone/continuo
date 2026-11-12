import gzip
import json

from tests.fingerprint_fixtures import MANIFESTS_DIR, VARIANTS

BASE_MODELS = {
    "orders", "orders_dep_1", "orders_dep_2", "recent", "env_model", "src_model",
    "dim_v.v1", "dim_v.v2", "uses_dim", "eph", "from_eph", "contracted", "gov", "fresh",
    "incr", "uses_udf", "cap_src", "cap_dep", "self_incr", "macro_user",
}


def _manifest(name: str) -> dict:
    with gzip.open(MANIFESTS_DIR / name / "manifest.json.gz", "rt", encoding="utf-8") as f:
        return json.load(f)


def test_every_variant_has_a_manifest_of_dbt_1_12():
    assert "base" in VARIANTS and len(VARIANTS) > 1
    for name in VARIANTS:
        assert _manifest(name)["metadata"]["dbt_version"] == "1.12.0b1", name
        assert (MANIFESTS_DIR / name / "compile-extras.json").is_file(), name

    base = _manifest("base")
    nodes = base["nodes"]
    assert {f"model.fpshop.{m}" for m in BASE_MODELS} <= set(nodes)
    assert {f"snapshot.fpshop.{s}" for s in ("snap_a", "snap_b", "snap_yml")} <= set(nodes)
    assert {f"seed.fpshop.{s}" for s in ("seed_users", "seed_semi", "seed_big")} <= set(nodes)
    assert nodes["seed.fpshop.seed_big"]["checksum"] == {"name": "path", "checksum": "seeds/seed_big.csv"}
    assert sorted(n["test_metadata"]["name"] for n in nodes.values() if n["resource_type"] == "test") == [
        "not_null", "not_null"]
    assert set(base["functions"]) == {"function.fpshop.double_it"}
    assert set(base["unit_tests"]) == {"unit_test.fpshop.orders.orders_has_one_row"}
    assert nodes["model.fpshop.from_eph"]["compiled_code"].startswith("with __dbt__cte__eph as")
    assert nodes["model.fpshop.cap_src"]["alias"] == "OrdersCap"
    assert nodes["model.fpshop.self_incr"]["alias"] == "self_incr_tbl"
