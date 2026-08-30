FROM python:3.12-slim

# Pin dbt to the classic postgres-supporting line. An unpinned `pip install
# dbt-postgres` now resolves to dbt Fusion, which dropped the postgres adapter,
# breaking every dbt compile/run. These are the exact versions the e2e suite is
# validated against. This image is the dbt project base inherited by every team
# service image; it is run only for compile, seed-build, and scheduled dbt runs.
# Validation nodes run in a separate minimal image (continuo-python-runtime-<engine>), not here.
RUN pip install --no-cache-dir "dbt-core==1.12.0b1" "dbt-postgres==1.10.0"

WORKDIR /project

# Shared dbt macros inherited by every service image (e.g. generate_schema_name,
# which routes validation runs to the candidate schema via DBT_TARGET_SCHEMA).
COPY macros/ /project/macros/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# dbt needs to create target/ and logs/ under /project at run time; own the
# project tree so the image can run unprivileged.
#
# This chown covers only what this stage copied. A service image built FROM here
# must COPY --chown=dbt:dbt, because a plain COPY writes root-owned files
# regardless of USER and would leave /project unwritable by the user the
# container runs as. A shadow release verifying a proposed fix lays its source
# overlay over the project with `cp -R /shared/overlay/. ./` as that user, so a
# root-owned project silently fails every fix verification.
RUN useradd --uid 1000 --create-home dbt && chown -R dbt:dbt /project
USER dbt

ENTRYPOINT ["/entrypoint.sh"]
