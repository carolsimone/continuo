{{ config(materialized='table') }}
SELECT id FROM public.wrong_table
