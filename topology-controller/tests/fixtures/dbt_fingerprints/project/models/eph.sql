{{ config(materialized='ephemeral') }}
select id, amount from {{ ref('orders') }} where amount > 0
