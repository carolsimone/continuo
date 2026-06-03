{{ config(materialized='table') }}
SELECT d.id
FROM {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.ftable_d d
LEFT JOIN {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.ftable_e e ON d.id = e.id
