select id, {{ function('double_it') }}(amount) as doubled from {{ ref('orders') }}
