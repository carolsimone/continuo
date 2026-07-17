{{ config(materialized='incremental', unique_key='id') }}

select 1 as id, 'current'::text as value
{% if is_incremental() %}
where not exists (select 1 from {{ this }} where id = 1)
{% endif %}
