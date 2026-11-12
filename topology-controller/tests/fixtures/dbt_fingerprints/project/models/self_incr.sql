{{ config(materialized='incremental', alias='self_incr_tbl') }}
select o.id, o.amount
from {{ ref('orders') }} as o
left join {{ this }} as t on t.id = o.id
where t.id is null
