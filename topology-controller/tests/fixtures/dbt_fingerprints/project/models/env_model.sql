select id, '{{ env_var("FP_REGION", "eu") }}' as region
from {{ ref('orders') }}
