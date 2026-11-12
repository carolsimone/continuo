{% snapshot snap_a %}
{{ config(unique_key='id', strategy='check', check_cols='all') }}
select * from {{ ref('orders') }}
{% endsnapshot %}

{% snapshot snap_b %}
{{ config(unique_key='id', strategy='check', check_cols='all') }}
select id, amount from {{ ref('orders') }}
{% endsnapshot %}
