"""Which nodes' content_hash moves for each edit of the fpshop fixture project.

The fixture gives `recent`, `env_model`, `src_model`, `seed_big` and the snapshots
no tests, so the moved sets that name them are exact.
"""
from dataclasses import replace

from domain.model import NodeType
from tests.fingerprint_fixtures import (
    base_extras, base_manifest_path, candidate_sql_of, manifest_checksum, moved, node_of, parse_and_resolve, parse_variant,
)
from service.parser import parse_manifest


def test_a_recompile_with_no_edit_moves_nothing():
    assert moved("base", "rerun") == set()


def test_compile_time_values_move_the_model_that_uses_them():
    assert moved("base", "var_change") == {"fp_analytics.recent"}
    assert moved("base", "env_var_change") == {"fp_analytics.env_model"}
    assert moved("base", "source_schema_change") == {"fp_analytics.src_model"}
    assert "fp_analytics.orders" in moved("base", "literal_whitespace")


def test_enforced_contract_columns_move_the_model():
    for variant in ("contract_type_change", "contract_reorder", "contract_quote"):
        assert "fp_analytics.contracted" in moved("base", variant), variant


def test_access_group_and_freshness_move_nothing():
    for variant in ("access_change", "group_change", "freshness_change"):
        assert moved("base", variant) == set(), variant


def test_a_snapshot_hashes_its_own_code_only():
    assert "fp_analytics.snap_a" not in moved("base", "snapshot_sibling_change")
    assert "fp_analytics.snap_b" in moved("base", "snapshot_sibling_change")
    assert "fp_analytics.snap_yml" not in moved("base", "snapshot_yaml_desc_change")


def test_an_explicit_default_severity_moves_nothing():
    assert moved("base", "severity_spelling") == set()


def test_seed_bytes_move_the_seed():
    assert moved("base", "seed_big_cell_change") == {"fp_analytics.seed_big"}
    assert "fp_analytics.seed_users" in moved("base", "seed_trailing_space")


def test_without_extras_a_seed_over_1_mib_keeps_its_path_checksum():
    seed = node_of(parse_variant("base", extras=False), "fp_analytics.seed_big")
    assert seed.source_hash == "seeds/seed_big.csv"


def test_a_seed_missing_from_the_extras_falls_back_to_the_dbt_checksum():
    extras = base_extras()
    trimmed = replace(extras, seeds={k: v for k, v in extras.seeds.items() if not k.endswith(".seed_users")})
    parsed = parse_manifest(base_manifest_path(), "", trimmed)
    assert node_of(parsed, "fp_analytics.seed_users").source_hash == manifest_checksum("seed.fpshop.seed_users")
    assert node_of(parsed, "fp_analytics.seed_big").source_hash == extras.seeds["seed.fpshop.seed_big"].sha256


def _models_seeds_and_snapshots() -> set[str]:
    return {n.unique_id for n in parse_variant("base").nodes if n.node_type != NodeType.DBT_TEST}


def test_macro_overrides_teams_write_move_what_they_reach():
    assert "fp_analytics.orders" in moved("base", "materialization_override")
    assert {"fp_analytics.seed_users", "fp_analytics.seed_big"} <= moved("base", "seed_loader_override")
    assert any(i.startswith("test.") for i in moved("base", "test_sql_override"))
    assert "fp_analytics.snap_a" in moved("base", "snapshot_strategy_override")
    assert {"fp_analytics.seed_users", "fp_analytics.seed_big"} <= moved("base", "should_full_refresh_override")


def test_the_flags_block_moves_every_model_seed_and_snapshot():
    assert moved("base", "flags_change") == _models_seeds_and_snapshots()
    assert moved("base", "flags_change", extras=False) == set()


def test_a_macro_called_only_inside_is_incremental_moves_the_model():
    assert "fp_analytics.incr" in moved("base", "incremental_static_call")


def test_an_on_run_start_change_moves_every_model_seed_and_snapshot():
    assert moved("base", "on_run_start_change") == _models_seeds_and_snapshots()


def test_a_function_body_change_moves_its_caller():
    assert moved("base", "udf_change") == {"fp_analytics.uses_udf"}


def test_a_unit_test_change_moves_its_model_and_counts_as_a_test():
    assert moved("base", "unit_test_change") == {"fp_analytics.orders"}
    assert node_of(parse_variant("base"), "fp_analytics.orders").test_count == 3   # 2 data tests + 1 unit test


def test_a_dbt_adapters_upgrade_moves_nothing():
    assert moved("base", "adapters_upgrade") == set()


def test_a_generate_alias_name_override_moves_every_model_seed_and_snapshot():
    assert _models_seeds_and_snapshots() <= moved("base", "generate_alias_override")


def test_swapping_two_macro_bodies_moves_their_caller():
    assert "fp_analytics.macro_user" in moved("base", "macro_swap")


def test_the_seed_column_fingerprint_moves_only_with_the_columns():
    def seed(variant, **kw):
        return node_of(parse_variant(variant, **kw), "fp_analytics.seed_users")

    base = seed("base")
    assert base.seed_columns_hash.startswith("sha256:")
    assert base.seed_header == ("id", "name", "amount")
    assert base.seed_column_types == {"id": "integer"}
    assert seed("seed_trailing_space").seed_columns_hash == base.seed_columns_hash
    assert seed("seed_column_add").seed_columns_hash != base.seed_columns_hash
    assert seed("seed_column_add").seed_header == ("id", "name", "amount", "source")
    without = seed("base", extras=False)
    assert (without.seed_columns_hash, without.seed_header, without.seed_column_types) == ("", (), {})
    extras = base_extras()
    trimmed = replace(extras, seeds={k: v for k, v in extras.seeds.items() if not k.endswith(".seed_users")})
    missing = node_of(parse_manifest(base_manifest_path(), "", trimmed), "fp_analytics.seed_users")
    assert (missing.seed_columns_hash, missing.seed_header, missing.seed_column_types) == ("", (), {})


def test_provenance_lists_the_path_checksum_seeds_only_without_extras():
    assert parse_variant("base", extras=False).provenance.path_checksum_seeds == ("fp_analytics.seed_big",)
    assert parse_variant("base").provenance.path_checksum_seeds == ()
    assert parse_variant("base").provenance.packages["dbt-adapters"] == "1.24.5"


def ups(variant, uid):
    return {d.unique_id for d in node_of(parse_and_resolve(variant), uid).upstream_deps}


def test_c2_a_dependant_of_an_aliased_model_keeps_its_edge_and_moves():
    assert "fp_analytics.orders" in ups("alias_change", "fp_analytics.orders_dep_1")
    assert {"fp_analytics.orders", "fp_analytics.orders_dep_1", "fp_analytics.orders_dep_2"} <= moved("base", "alias_change")


def test_c3_the_consumer_of_an_ephemeral_model_depends_on_it_and_moves_with_it():
    assert "fp_analytics.eph" in ups("base", "fp_analytics.from_eph")
    assert "fp_analytics.orders" in ups("base", "fp_analytics.from_eph"), "the table eph reads stays a direct upstream"
    assert {"fp_analytics.eph", "fp_analytics.from_eph"} <= moved("base", "ephemeral_change")


def test_n37_each_model_version_is_its_own_node():
    parsed = parse_variant("base")
    v2 = node_of(parsed, "fp_analytics.dim_v.v2")
    assert v2.table_name == "dim_v.v2" and v2.resolved_relation_id == "fp_analytics.dim_v_v2" and v2.relation_name == "dim_v_v2"
    assert node_of(parsed, "fp_analytics.dim_v.v1").table_name == "dim_v.v1"


def test_c1_a_latest_version_change_moves_the_unversioned_consumer():
    assert "fp_analytics.uses_dim" in moved("base", "latest_version_change")
    assert "fp_analytics.dim_v.v1" in ups("base", "fp_analytics.uses_dim")


def test_n38_relation_name_keeps_the_declared_spelling():
    parsed = parse_variant("base")
    assert node_of(parsed, "fp_analytics.cap_src").relation_name == "OrdersCap"
    assert node_of(parsed, "fp_analytics.orders").relation_name == ""


def test_n38_a_dependant_reads_the_candidate_table_in_the_declared_spelling():
    sql = candidate_sql_of("base", "fp_analytics.cap_dep")
    assert '"_candidate_rel_1"."OrdersCap"' in sql


def test_n38_an_aliased_incremental_model_reads_itself_from_production():
    assert "fp_analytics.self_incr" not in ups("base", "fp_analytics.self_incr")
    assert '"_candidate_rel_1"."self_incr_tbl"' not in candidate_sql_of("base", "fp_analytics.self_incr")
