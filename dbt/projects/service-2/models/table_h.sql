{{ config(materialized='table') }}
SELECT * FROM e2e_schema.table_e JOIN e2e_schema.table_f USING (id)
