{{ config(materialized='table') }}
SELECT * FROM e2e_schema.table_a JOIN e2e_schema.table_b USING (id)
