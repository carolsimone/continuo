{{ config(materialized='table') }}
SELECT * FROM {{ ref('seed_table_1') }}
