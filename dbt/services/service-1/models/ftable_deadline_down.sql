{{ config(materialized='table') }}
SELECT id FROM {{ ref('ftable_deadline') }}
