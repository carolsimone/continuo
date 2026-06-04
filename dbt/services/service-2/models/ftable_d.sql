{{ config(materialized='table') }}
SELECT id FROM e2e_schema.ftable_c
