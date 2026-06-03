{{ config(materialized='table') }}
SELECT * FROM {{ xschema() }}.table_g JOIN {{ xschema() }}.table_h USING (id)
