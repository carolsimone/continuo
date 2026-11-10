{{ config(materialized='table') }}
{#- Reads e2e_raw.ingest_probe, a table no model builds: scripts/setup.sh
    creates it in the warehouse. topology-controller therefore adds an
    ingested node for it, upstream of this model. -#}
SELECT id FROM e2e_raw.ingest_probe
