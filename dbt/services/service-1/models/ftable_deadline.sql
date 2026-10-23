{{ config(materialized='table', meta={'timeout_minutes': 1}) }}
{#- Declares a one-minute task timeout and sleeps for ten, so Kubernetes stops
    its Job at the Job's deadline. The sleep is a column of the result: a table
    cannot hold pg_sleep's void, but it can hold that value cast to text. -#}
SELECT 1 AS id, pg_sleep(600)::text AS slept
