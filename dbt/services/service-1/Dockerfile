FROM carolsimone/dbt-base:latest

COPY --chown=dbt:dbt dbt_project.yml profiles.yml ./
COPY --chown=dbt:dbt models/ ./models/
COPY --chown=dbt:dbt seeds/ ./seeds/

# Explicit non-root user (inherited from dbt-base; restated so image scanners
# can verify this stage does not run as root).
USER dbt
