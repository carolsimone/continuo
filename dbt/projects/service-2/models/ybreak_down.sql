{{ config(materialized='table') }}
SELECT amount_eur FROM e2e_schema.ybreak_up
