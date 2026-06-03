{{ config(materialized='table') }}
SELECT * FROM {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.table_b JOIN {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.table_c USING (id)
