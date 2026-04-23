{{ config(materialized='table') }}
SELECT d.id
FROM e2e_schema.ftable_d d
LEFT JOIN e2e_schema.ftable_e e ON d.id = e.id
