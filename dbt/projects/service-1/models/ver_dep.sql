{#- Reads version 2 of ver_src through ref(), which compiles to ver_src_v2. -#}
select id from {{ ref('ver_src', v=2) }}
