{{ config(materialized='table') }}
{#- Carries the project's owner and no schedule tag, so topology-controller
    keeps it as a dormant node: in the topology with no schedule. It is a table
    below no seed, so no seed reload rebuilds it either: no cron or schedule's
    run ever runs it, and it runs only by hand. The dormant-node e2e test
    runs it with a single-node run. -#}
SELECT 1 AS id
