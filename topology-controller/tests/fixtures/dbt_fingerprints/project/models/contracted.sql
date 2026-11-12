select id, cast(amount as numeric(12,2)) as amount from {{ ref('orders') }}
