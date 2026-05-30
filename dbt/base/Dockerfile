FROM python:3.12-slim

RUN pip install --no-cache-dir dbt-postgres

WORKDIR /project

# Shared dbt macros inherited by every service image (e.g. generate_schema_name,
# which routes validation runs to the candidate schema via DBT_TARGET_SCHEMA).
COPY macros/ /project/macros/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
