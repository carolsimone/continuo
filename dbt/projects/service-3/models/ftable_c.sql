{{ config(materialized='table') }}
SELECT a.id
FROM e2e_schema.ftable_a a
LEFT JOIN e2e_schema.ftable_b b ON a.id = b.id
