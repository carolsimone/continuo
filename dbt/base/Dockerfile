FROM python:3.12-slim

# Pin dbt to the classic postgres-supporting line. An unpinned `pip install
# dbt-postgres` now resolves to dbt Fusion, which dropped the postgres adapter,
# breaking every dbt compile/run. These are the exact versions the e2e suite is
# validated against. This image is the dbt project base inherited by every team
# service image; it is run only for compile, seed-build, and scheduled dbt runs.
# Validation no longer runs here — it runs the minimal validation-runner image.
RUN pip install --no-cache-dir "dbt-core==1.12.0b1" "dbt-postgres==1.10.0"

WORKDIR /project

# Shared dbt macros inherited by every service image (e.g. generate_schema_name,
# which routes validation runs to the candidate schema via DBT_TARGET_SCHEMA).
COPY macros/ /project/macros/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
