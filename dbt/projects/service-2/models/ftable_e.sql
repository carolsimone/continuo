{{ config(materialized='table') }}
SELECT c.id
FROM e2e_schema.ftable_c c
LEFT JOIN public.wrong_name w ON c.id = w.id
