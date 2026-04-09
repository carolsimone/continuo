{{ config(materialized='table') }}
SELECT * FROM e2e_schema.table_b JOIN e2e_schema.table_c USING (id)
