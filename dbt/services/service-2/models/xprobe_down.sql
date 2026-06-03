{{ config(materialized='table') }}
SELECT id FROM {{ xschema() }}.xprobe_up
