{{ config(materialized='view') }}

select * from {{ ref('worker_incremental') }}
