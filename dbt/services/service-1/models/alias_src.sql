{#- Declares an alias, so dbt builds this model as alias_src_tbl rather than
    alias_src: its node keeps the declared name, and its relation is the alias. -#}
{{ config(alias='alias_src_tbl') }}
select 1 as id
