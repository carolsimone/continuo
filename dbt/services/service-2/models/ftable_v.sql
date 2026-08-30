{{ config(materialized='table') }}
SELECT u.id, u.amount FROM e2e_schema.ftable_u u
