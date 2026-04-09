{{ config(materialized='table') }}
SELECT * FROM e2e_schema.table_d JOIN e2e_schema.table_e USING (id)
