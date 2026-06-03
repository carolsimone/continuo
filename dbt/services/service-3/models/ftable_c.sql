{{ config(materialized='table') }}
SELECT a.id
FROM {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.ftable_a a
LEFT JOIN {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.ftable_b b ON a.id = b.id
