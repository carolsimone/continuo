{{ config(materialized='table') }}
SELECT * FROM {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.table_e JOIN {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.table_f USING (id)
