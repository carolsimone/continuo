{{ config(materialized='table') }}
SELECT id FROM {{ xschema() }}.ftable_c
