{{ config(materialized='table') }}
{#- Fails its first attempt and succeeds on every later one. TASK_ATTEMPT is the
    attempt number execution-controller gives the pod. The failure is a cast
    Postgres refuses when the model runs, so dbt parses and compiles the model
    whatever the attempt. -#}
{% if env_var('TASK_ATTEMPT', '0') == '0' %}
SELECT CAST('the first attempt of ftable_retry fails' AS integer) AS id
{% else %}
SELECT 1 AS id
{% endif %}
