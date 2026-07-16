FROM python:3.12-slim

# Pin dbt to the classic postgres-supporting line. An unpinned `pip install
# dbt-postgres` now resolves to dbt Fusion, which dropped the postgres adapter,
# breaking every dbt compile/run. These are the exact versions the e2e suite is
# validated against. This image is the dbt project base inherited by every team
# service image; it is run only for compile, seed-build, and scheduled dbt runs.
# Validation nodes run in a separate minimal image (validation-runner), not here.
RUN pip install --no-cache-dir "dbt-core==1.12.0b1" "dbt-postgres==1.10.0"

WORKDIR /project

# Shared dbt macros inherited by every service image (e.g. generate_schema_name,
# which routes validation runs to the candidate schema via DBT_TARGET_SCHEMA).
COPY macros/ /project/macros/

# Continuo's dbt runtime support, importable by every team image. It exports a
# release's runtime manifest artifacts after the team's own compile command runs.
# PYTHONPATH is set rather than pip-installing so the modules stay readable and
# no team image gains a build dependency on them.
COPY continuo_dbt_runtime/ /opt/continuo/continuo_dbt_runtime/
COPY bin/continuo-export-runtime-manifest /continuo/bin/continuo-export-runtime-manifest
ENV PYTHONPATH="/opt/continuo"
RUN chmod 0555 /continuo/bin/continuo-export-runtime-manifest

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
