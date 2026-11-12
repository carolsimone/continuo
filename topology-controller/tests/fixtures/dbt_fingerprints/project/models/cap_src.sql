{{ config(alias='OrdersCap') }}
select id, amount from {{ ref('orders') }}
