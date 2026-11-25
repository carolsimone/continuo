{{ config(materialized='table') }}
SELECT amount_eur FROM e2e_schema.xbreak_up
