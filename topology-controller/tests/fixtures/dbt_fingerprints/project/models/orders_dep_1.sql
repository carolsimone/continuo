select id, amount from {{ ref('orders') }}
