{{ config(materialized='table') }}
SELECT a.id
FROM {{ xschema() }}.ftable_a a
LEFT JOIN {{ xschema() }}.ftable_b b ON a.id = b.id
