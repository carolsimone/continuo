{{ config(materialized='table') }}
SELECT * FROM {{ xschema() }}.table_b JOIN {{ xschema() }}.table_c USING (id)
