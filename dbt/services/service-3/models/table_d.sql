{{ config(materialized='table') }}
SELECT * FROM {{ xschema() }}.table_a JOIN {{ xschema() }}.table_b USING (id)
