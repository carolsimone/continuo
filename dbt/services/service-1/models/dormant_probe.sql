{{ config(materialized='table') }}
{#- Carries the project's owner and no schedule tag, so topology-controller
    keeps it as a dormant node: in the topology with no schedule, never run by
    a cron or a schedule's run, and run only by hand. The dormant-node e2e test
    runs it with a single-node run. -#}
SELECT 1 AS id
