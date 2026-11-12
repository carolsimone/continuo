select id, amount
from {{ ref('orders') }}
where created_at >= current_date - {{ var('cutoff_days') }}
