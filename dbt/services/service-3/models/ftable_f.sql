{{ config(materialized='table') }}
SELECT d.id
FROM {{ xschema() }}.ftable_d d
LEFT JOIN {{ xschema() }}.ftable_e e ON d.id = e.id
