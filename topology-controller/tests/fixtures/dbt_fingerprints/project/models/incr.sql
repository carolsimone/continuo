{{ config(materialized='incremental', unique_key='id') }}
select id, amount from {{ ref('orders') }}
{% if is_incremental() %} where id > {{ since() }} {% endif %}
