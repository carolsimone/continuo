FROM python:3.12-slim

# Pin dbt to the classic postgres-supporting line. An unpinned `pip install
# dbt-postgres` now resolves to dbt Fusion, which dropped the postgres adapter
# ("InvalidConfig dbt1005: 'postgres' adapter is not yet supported by dbt
# Fusion") — breaking every dbt compile/run/validation. These are the exact
# versions the e2e suite is validated against. No boto3: the validation Job's S3 fetch
# lives in the s3-sidecar init container, so this image is pure dbt + psycopg2.
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

# Warehouse adapter (engine DDL behind a port). validation_runner.py imports it
# via the flat-layout shim (`from warehouse import adapter_from_env`), so it must
# sit flat at / alongside the runner.
COPY warehouse.py /warehouse.py

# Shared structured-result contract + the seed validation wrapper. Both validation
# entrypoints print a sentinel-framed result block on stdout; the seed wrapper runs
# `dbt seed --empty` and projects target/run_results.json into that contract. Run
# flat at /, so the contract module is imported via the import shim in each script.
COPY validation_result.py /validation_result.py
COPY seed_validation_runner.py /seed_validation_runner.py

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
