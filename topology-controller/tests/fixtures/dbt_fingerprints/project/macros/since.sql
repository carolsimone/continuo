{% macro since() %}(select coalesce(max(id), 0) from {{ this }}){% endmacro %}
