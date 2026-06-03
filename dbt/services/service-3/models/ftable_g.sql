{{ config(materialized='table') }}
SELECT a.id
FROM {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.ftable_a a
LEFT JOIN public.wrong_name_2 w ON a.id = w.id
