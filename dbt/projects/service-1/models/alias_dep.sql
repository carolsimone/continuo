{#- Reads the aliased model through ref(), which compiles to the alias
    alias_src_tbl: in a release's validation it reads the candidate's copy. -#}
select id from {{ ref('alias_src') }}
