{{ config(materialized='table') }}
SELECT * FROM e2e_schema.table_g JOIN e2e_schema.table_h USING (id)
