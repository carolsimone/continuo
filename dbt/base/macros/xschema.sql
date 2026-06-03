{#
    Cross-service input-schema resolver, shared by every dbt service image.

    Models reference another service's tables with a schema-qualified name. This
    macro supplies that schema so blue/green validation can redirect cross-service
    reads to the isolated candidate schema. The executor-controller sets
    DBT_UPSTREAM_SCHEMA to the candidate schema on validation jobs; when it is
    absent (CI compile and the production query-job path) the schema falls back to
    target.schema, leaving compiled SQL and prod materialization byte-identical to a
    project without this macro. Mirror of generate_schema_name, which redirects the
    OUTPUT schema via DBT_TARGET_SCHEMA.

    Usage in a model:  SELECT ... FROM {{ xschema() }}.other_service_table
#}
{% macro xschema() %}{{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}{% endmacro %}
