{{ config(materialized='table') }}
SELECT id FROM {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.ftable_g
