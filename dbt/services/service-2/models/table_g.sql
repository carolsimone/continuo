{{ config(materialized='table') }}
SELECT * FROM {{ xschema() }}.table_d JOIN {{ xschema() }}.table_e USING (id)
