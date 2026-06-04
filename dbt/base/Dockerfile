FROM python:3.12-slim

# Pin dbt to the classic postgres-supporting line. An unpinned `pip install
# dbt-postgres` now resolves to dbt Fusion, which dropped the postgres adapter
# ("InvalidConfig dbt1005: 'postgres' adapter is not yet supported by dbt
# Fusion") — breaking every dbt compile/run/validation. These are the exact
# versions the e2e suite is validated against.
RUN pip install --no-cache-dir "dbt-core==1.12.0b1" "dbt-postgres==1.10.0"

WORKDIR /project

# Shared dbt macros inherited by every service image (e.g. generate_schema_name,
# which routes validation runs to the candidate schema via DBT_TARGET_SCHEMA).
COPY macros/ /project/macros/

# Blue/green validation runner: builds a node as an empty table in the candidate
# schema from execution-time-rewritten compiled SQL. Inherited by every service
# image (FROM dbt-base); invoked by the executor as the validation Job command for
# model/snapshot nodes instead of `dbt run`.
COPY validation_runner.py /validation_runner.py

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
