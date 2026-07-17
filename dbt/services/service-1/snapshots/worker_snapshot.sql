{% snapshot worker_snapshot %}

{{
    config(
        strategy='check',
        unique_key='id',
        check_cols=['value'],
    )
}}

select * from {{ ref('worker_incremental') }}

{% endsnapshot %}
