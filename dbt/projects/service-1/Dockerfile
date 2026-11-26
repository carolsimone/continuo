FROM carolsimone/dbt-base:latest

COPY dbt_project.yml profiles.yml ./
COPY models/ ./models/
COPY seeds/ ./seeds/

# Explicit non-root user (inherited from dbt-base; restated so image scanners
# can verify this stage does not run as root).
USER dbt
