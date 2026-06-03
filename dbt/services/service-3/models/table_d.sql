{{ config(materialized='table') }}
SELECT * FROM {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.table_a JOIN {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.table_b USING (id)
