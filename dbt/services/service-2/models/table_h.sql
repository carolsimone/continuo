{{ config(materialized='table') }}
SELECT * FROM {{ xschema() }}.table_e JOIN {{ xschema() }}.table_f USING (id)
