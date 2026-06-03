{{ config(materialized='table') }}
SELECT c.id
FROM {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.ftable_c c
LEFT JOIN public.wrong_name w ON c.id = w.id
