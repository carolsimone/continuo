{{ config(materialized='table') }}

-- The performance gate's native worker sample. It is deliberately self-contained
-- and trivial: the gate measures how long a worker takes to get from a granted
-- lease to dbt starting, so anything this model asks of the warehouse is
-- measurement noise. It depends on no other node, which keeps repeated runs of it
-- independent of the rest of the e2e DAG.
select 1 as id
