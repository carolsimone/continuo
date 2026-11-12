"""Which nodes' content_hash moves for each edit of the fpshop fixture project.

The fixture gives `recent`, `env_model`, `src_model`, `seed_big` and the snapshots
no tests, so the moved sets that name them are exact.
"""
from dataclasses import replace

from domain.model import NodeType
from tests.fingerprint_fixtures import base_extras, base_manifest_path, manifest_checksum, moved, node_of, parse_variant
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
