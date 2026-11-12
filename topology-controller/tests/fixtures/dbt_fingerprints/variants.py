"""The variants of the fpshop fixture project that generate.py compiles.

Each variant is the project under project/ with a few text edits applied. An edit is
(path, old, new): `old` must occur exactly once in the file at `path` (relative to the
project) and is replaced by `new`; an edit whose `old` is "" creates the file with `new`.
`seeds/seed_big.csv` is written by generate.py before the edits, so an edit can change it.

`env` is passed to dbt, `image` is the image that compiles the variant, and
`pre_compile`, when set, is a shell command run in the container before the compile,
as root (the image's site-packages belongs to root).
"""
from __future__ import annotations

from dataclasses import dataclass, field

DEFAULT_IMAGE = "dbt-base:latest"


@dataclass(frozen=True)
class Variant:
    edits: tuple[tuple[str, str, str], ...] = ()
    env: dict[str, str] = field(default_factory=dict)
    image: str = DEFAULT_IMAGE
    pre_compile: str = ""


_MATERIALIZATION_TABLE = """\
{% materialization table, default %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {{ run_hooks(pre_hooks) }}
  {{ drop_relation_if_exists(load_cached_relation(target_relation)) }}
  {% call statement('main') -%}
    {{ get_create_table_as_sql(False, target_relation, sql) }}
  {%- endcall %}
  {{ run_hooks(post_hooks) }}
  {{ adapter.commit() }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
"""

_GET_BATCH_SIZE = """\
{% macro get_batch_size() -%}
  {{ return(5000) }}
{%- endmacro %}
"""

_GET_TEST_SQL = """\
{% macro get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
    select
      {{ fail_calc }} as failures,
      {{ fail_calc }} {{ warn_if }} as should_warn,
      {{ fail_calc }} {{ error_if }} as should_error
    from (
      {{ main_sql }}
      {{ "limit " ~ limit if limit != none }}
    ) as dbt_internal_test
{%- endmacro %}
"""

_SNAPSHOT_CHECK_STRATEGY = """\
{% macro snapshot_check_strategy(node, snapshotted_rel, current_rel, model_config, target_exists) %}
    {% set check_cols_config = config.get('check_cols') %}
    {% set primary_key = config.get('unique_key') %}
    {% set hard_deletes = adapter.get_hard_deletes_behavior(config) %}
    {% set invalidate_hard_deletes = hard_deletes == 'invalidate' %}
    {% set updated_at = config.get('updated_at') or snapshot_get_time() %}

    {% set column_added, check_cols = snapshot_check_all_get_existing_columns(node, target_exists, check_cols_config) %}

    {%- set row_changed_expr -%}
    (
    {%- if column_added -%}
        {{ get_true_sql() }}
    {%- else -%}
    {%- for col in check_cols -%}
        {{ snapshotted_rel }}.{{ col }} is distinct from {{ current_rel }}.{{ col }}
        {%- if not loop.last %} or {% endif -%}
    {%- endfor -%}
    {%- endif -%}
    )
    {%- endset %}

    {% set scd_args = api.Relation.scd_args(primary_key, updated_at) %}
    {% set scd_id_expr = snapshot_hash_arguments(scd_args) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "hard_deletes": hard_deletes
    }) %}
{% endmacro %}
"""

_SHOULD_FULL_REFRESH = """\
{% macro should_full_refresh() %}
  {% set config_full_refresh = config.get('full_refresh') %}
  {% if config_full_refresh is none %}
    {% set config_full_refresh = false %}
  {% endif %}
  {% do return(config_full_refresh) %}
{% endmacro %}
"""

_GENERATE_ALIAS_NAME = """\
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    fp_{{ default__generate_alias_name(custom_alias_name, node) | trim }}
{%- endmacro %}
"""

_CONTRACT_COLUMNS = """\
      - name: id
        data_type: integer
      - name: amount
        data_type: numeric(12,2)
"""

_CONTRACT_COLUMNS_SWAPPED = """\
      - name: amount
        data_type: numeric(12,2)
      - name: id
        data_type: integer
"""

VARIANTS: dict[str, Variant] = {
    "base": Variant(),
    "rerun": Variant(),
    "var_change": Variant(edits=(("dbt_project.yml", "cutoff_days: 30", "cutoff_days: 45"),)),
    "env_var_change": Variant(env={"FP_REGION": "us"}),
    "source_schema_change": Variant(edits=(("models/schema.yml", "    schema: raw\n", "    schema: raw2\n"),)),
    "literal_whitespace": Variant(edits=(("models/orders.sql", "'ok  x'", "'ok x'"),)),
    "latest_version_change": Variant(edits=(("models/schema.yml", "latest_version: 1", "latest_version: 2"),)),
    "alias_change": Variant(edits=(
        ("models/orders.sql", "select 1 as id", "{{ config(alias='orders_tbl') }}\nselect 1 as id"),
    )),
    "ephemeral_change": Variant(edits=(("models/eph.sql", "where amount > 0", "where amount > 100"),)),
    "contract_type_change": Variant(edits=(("models/schema.yml", "data_type: numeric(12,2)", "data_type: numeric(14,2)"),)),
    "contract_reorder": Variant(edits=(("models/schema.yml", _CONTRACT_COLUMNS, _CONTRACT_COLUMNS_SWAPPED),)),
    "contract_quote": Variant(edits=(
        ("models/schema.yml",
         "      - name: id\n        data_type: integer\n",
         "      - name: id\n        data_type: integer\n        quote: true\n"),
    )),
    "materialization_override": Variant(edits=(("macros/materialization_table.sql", "", _MATERIALIZATION_TABLE),)),
    "seed_loader_override": Variant(edits=(("macros/get_batch_size.sql", "", _GET_BATCH_SIZE),)),
    "test_sql_override": Variant(edits=(("macros/get_test_sql.sql", "", _GET_TEST_SQL),)),
    "snapshot_strategy_override": Variant(edits=(("macros/snapshot_check_strategy.sql", "", _SNAPSHOT_CHECK_STRATEGY),)),
    "should_full_refresh_override": Variant(edits=(("macros/should_full_refresh.sql", "", _SHOULD_FULL_REFRESH),)),
    "flags_change": Variant(edits=(
        ("dbt_project.yml",
         "on-run-start:",
         "flags:\n  require_explicit_package_overrides_for_builtin_materializations: false\non-run-start:"),
    )),
    "incremental_static_call": Variant(edits=(("macros/since.sql", "coalesce(max(id), 0)", "coalesce(max(id), -1)"),)),
    "on_run_start_change": Variant(edits=(("dbt_project.yml", "select 1 as hook_start", "select 2 as hook_start"),)),
    "udf_change": Variant(edits=(("functions/double_it.sql", "x * 2", "x * 3"),)),
    "unit_test_change": Variant(edits=(("models/schema.yml", "{id: 1, amount: 10}", "{id: 1, amount: 11}"),)),
    "adapters_upgrade": Variant(pre_compile="pip install --no-cache-dir --quiet dbt-adapters==1.23.0"),
    "generate_alias_override": Variant(edits=(("macros/generate_alias_name.sql", "", _GENERATE_ALIAS_NAME),)),
    "access_change": Variant(edits=(("models/schema.yml", "access: public", "access: protected"),)),
    "group_change": Variant(edits=(("models/schema.yml", "group: sales", "group: ops"),)),
    "freshness_change": Variant(edits=(("models/schema.yml", "count: 4", "count: 6"),)),
    "severity_spelling": Variant(edits=(
        ("models/schema.yml",
         "          - not_null\n",
         "          - not_null:\n              config:\n                severity: error\n"),
    )),
    "snapshot_sibling_change": Variant(edits=(
        ("snapshots/two.sql", "select id, amount from", "select id, amount, status from"),
    )),
    "snapshot_yaml_desc_change": Variant(edits=(
        ("models/schema.yml", 'description: "Order amounts"', 'description: "Order amounts in cents"'),
    )),
    "macro_swap": Variant(edits=(
        ("macros/m_p.sql", "{% macro m_p() %}1{% endmacro %}", "{% macro m_p() %}2{% endmacro %}"),
        ("macros/m_q.sql", "{% macro m_q() %}2{% endmacro %}", "{% macro m_q() %}1{% endmacro %}"),
    )),
    "seed_big_cell_change": Variant(edits=(
        ("seeds/seed_big.csv", "\n42,row_0000042,", "\n42,row_0000042_changed,"),
    )),
    "seed_trailing_space": Variant(edits=(("seeds/seed_users.csv", "2,bob,20\n", "2,bob,20 \n"),)),
    "seed_column_add": Variant(edits=(
        ("seeds/seed_users.csv",
         "id,name,amount\n1,alice,10\n2,bob,20\n",
         "id,name,amount,source\n1,alice,10,web\n2,bob,20,shop\n"),
    )),
}
