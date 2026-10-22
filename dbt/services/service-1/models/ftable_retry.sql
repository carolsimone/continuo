{{ config(materialized='table') }}
{#- Fails its first attempt and succeeds on every later one. TASK_ATTEMPT is the
    attempt number execution-controller gives the pod. The failure is a cast of
    a value Postgres computes only when a row is produced, so dbt parses and
    compiles the model whatever the attempt, and a validation that builds the
    table WITH NO DATA succeeds. -#}
{% if env_var('TASK_ATTEMPT', '0') == '0' %}
SELECT CAST(gen_random_uuid()::text AS integer) AS id
{% else %}
SELECT 1 AS id
{% endif %}
