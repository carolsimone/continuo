{{ config(materialized='table') }}
SELECT id FROM {{ ref('rel_probe_up') }}
