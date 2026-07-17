#!/bin/sh
set -eu

: "${POSTGRES_HOST:?POSTGRES_HOST is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

POSTGRES_PORT="${POSTGRES_PORT:-5432}"

# Optional. Empty (default) leaves the JDBC URL exactly as before — required
# for byte-identical behavior against the existing Hetzner/deploy-app path.
# Set (e.g. "require") to append "?sslmode=..." for a BYO Postgres that
# enforces TLS (RDS, Azure Database for PostgreSQL, etc.); psql/libpq in the
# db-init step already defaults to sslmode=prefer, but the flyway JDBC driver
# does not negotiate SSL unless the URL says so.
DB_SSLMODE="${DB_SSLMODE:-}"

# Single source of truth: every database this job owns. The same list drives
# both the create-loop and the flyway-loop below, so they cannot drift.
DATABASES="state executor orchestrator k8s release agent remediation remediation_agent"

# -connectRetries rides out Postgres cold start. Kept low enough that a genuine
# misconfiguration surfaces well within the Helm pre-upgrade hook timeout rather
# than hanging until it expires.
FLYWAY_OPTS="-connectRetries=10 -baselineOnMigrate=true"

export PGPASSWORD="${POSTGRES_PASSWORD}"
PSQL="psql -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USER}"

# Wait for Postgres to accept connections before touching it.
echo "Waiting for Postgres at ${POSTGRES_HOST}:${POSTGRES_PORT}..."
i=1
while [ "$i" -le 30 ]; do
  if pg_isready -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "Postgres not reachable after 60s" >&2
    exit 1
  fi
  i=$((i + 1))
  sleep 2
done

# Ensure every database exists before migrating it. Creating them here (rather
# than relying on the Postgres initdb scripts, which only run on a fresh data
# directory) makes provisioning idempotent and safe on long-lived volumes:
# adding a new database never requires a manual CREATE on an existing cluster.
# The migration user owns the databases it creates, so no extra GRANTs are
# needed.
for db in ${DATABASES}; do
  if [ "$(${PSQL} -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'continuo_${db}'")" != "1" ]; then
    echo "Creating database continuo_${db}..."
    ${PSQL} -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE continuo_${db}"
  fi
done

for db in ${DATABASES}; do
  jdbc_url="jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/continuo_${db}"
  if [ -n "${DB_SSLMODE}" ]; then
    jdbc_url="${jdbc_url}?sslmode=${DB_SSLMODE}"
  fi
  echo "Running migrations for continuo_${db}..."
  flyway \
    -url="${jdbc_url}" \
    -user="${POSTGRES_USER}" \
    -password="${POSTGRES_PASSWORD}" \
    -locations="filesystem:/flyway/sql/${db}" \
    ${FLYWAY_OPTS} \
    migrate
done

echo "All migrations complete."
