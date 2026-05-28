#!/bin/sh
set -eu

: "${POSTGRES_HOST:?POSTGRES_HOST is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

FLYWAY_OPTS="-connectRetries=30 -baselineOnMigrate=true"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"

for db in state executor orchestrator k8s release; do
  echo "Running migrations for continuo_${db}..."
  flyway \
    -url="jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/continuo_${db}" \
    -user="${POSTGRES_USER}" \
    -password="${POSTGRES_PASSWORD}" \
    -locations="filesystem:/flyway/sql/${db}" \
    ${FLYWAY_OPTS} \
    migrate
done

echo "All migrations complete."
