#!/bin/sh
set -eu

: "${POSTGRES_HOST:?POSTGRES_HOST is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

FLYWAY_OPTS="-connectRetries=30 -baselineOnMigrate=true"

for db in state startup executor dependency k8s; do
  echo "Running migrations for continuo_${db}..."
  flyway \
    -url="jdbc:postgresql://${POSTGRES_HOST}:5432/continuo_${db}" \
    -user="${POSTGRES_USER}" \
    -password="${POSTGRES_PASSWORD}" \
    -locations="filesystem:/flyway/migrations/${db}" \
    ${FLYWAY_OPTS} \
    migrate
done

echo "All migrations complete."
