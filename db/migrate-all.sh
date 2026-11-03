#!/bin/sh
set -eu

: "${POSTGRES_HOST:?POSTGRES_HOST is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

POSTGRES_PORT="${POSTGRES_PORT:-5432}"

# Optional. Empty (default) leaves the JDBC URL without TLS parameters. Set
# (e.g. "require") to append "?sslmode=..." for a Postgres that enforces TLS
# (RDS, Azure Database for PostgreSQL, etc.); psql/libpq already defaults to
# sslmode=prefer, but the Flyway JDBC driver does not negotiate TLS unless the
# URL says so.
DB_SSLMODE="${DB_SSLMODE:-}"

# Each service's runtime login, as "<database short name>=<login>" pairs
# separated by spaces, e.g. "run_controller=continuo_run_controller ...".
# Every database below needs exactly one.
CONTINUO_PG_LOGINS="${CONTINUO_PG_LOGINS:-}"

# Bundled quickstart only: the job creates each runtime login itself, with the
# password in CONTINUO_PG_PASSWORD_<SHORT NAME IN CAPITALS>, and hands the
# warehouse database CONTINUO_PG_WAREHOUSE_DB to the warehouse login
# CONTINUO_PG_WAREHOUSE_LOGIN (password CONTINUO_PG_WAREHOUSE_PASSWORD).
CONTINUO_PG_CREATE_LOGINS="${CONTINUO_PG_CREATE_LOGINS:-false}"

# Single source of truth: every database this job owns. The same list drives
# the login lookup, the create loop, the Flyway loop and the check, so they
# cannot drift.
DATABASES="run_controller execution release agent_chat remediation agent_remediation dead_letter"

LOGINS_DIR="$(dirname "$0")/logins"

# -connectRetries rides out Postgres cold start. Kept low enough that a genuine
# misconfiguration surfaces well within the Helm pre-upgrade hook timeout rather
# than hanging until it expires.
FLYWAY_OPTS="-connectRetries=10 -baselineOnMigrate=true"

export PGPASSWORD="${POSTGRES_PASSWORD}"
PSQL="psql -X -q -v ON_ERROR_STOP=1 -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} -U ${POSTGRES_USER}"

# login_for <short name>: prints the runtime login CONTINUO_PG_LOGINS names
# for that database, or nothing.
login_for() {
  for pair in ${CONTINUO_PG_LOGINS}; do
    if [ "${pair%%=*}" = "$1" ]; then
      echo "${pair#*=}"
      return 0
    fi
  done
}

if [ -z "$(echo "${CONTINUO_PG_LOGINS}" | tr -d '[:space:]')" ]; then
  echo "CONTINUO_PG_LOGINS is empty: every service needs its own login (deploy/CREDENTIALS.md)" >&2
  exit 1
fi
for pair in ${CONTINUO_PG_LOGINS}; do
  case " ${DATABASES} " in
    *" ${pair%%=*} "*) ;;
    *) echo "CONTINUO_PG_LOGINS names ${pair%%=*}, which is not a continuo database (${DATABASES})" >&2; exit 1 ;;
  esac
done
ALL_LOGINS=""
for db in ${DATABASES}; do
  login="$(login_for "${db}")"
  if [ -z "${login}" ]; then
    echo "CONTINUO_PG_LOGINS names no login for continuo_${db}: every service needs its own login (deploy/CREDENTIALS.md)" >&2
    exit 1
  fi
  ALL_LOGINS="${ALL_LOGINS:+${ALL_LOGINS},}${login}"
done

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

# create_login <login> <password>: bundled quickstart only. The password
# reaches psql through the environment, never on a command line.
create_login() {
  CONTINUO_PG_LOGIN_PASSWORD="$2" ${PSQL} -d postgres -v login="$1" -f "${LOGINS_DIR}/create-bundled.sql"
}

if [ "${CONTINUO_PG_CREATE_LOGINS}" = "true" ]; then
  for db in ${DATABASES}; do
    var="CONTINUO_PG_PASSWORD_$(echo "${db}" | tr '[:lower:]' '[:upper:]')"
    eval "password=\${${var}:-}"
    if [ -z "${password}" ]; then
      echo "${var} is empty: the bundled quickstart sets a password for every login" >&2
      exit 1
    fi
    echo "Creating login $(login_for "${db}")..."
    create_login "$(login_for "${db}")" "${password}"
  done
  : "${CONTINUO_PG_WAREHOUSE_LOGIN:?CONTINUO_PG_WAREHOUSE_LOGIN is required when CONTINUO_PG_CREATE_LOGINS=true}"
  : "${CONTINUO_PG_WAREHOUSE_PASSWORD:?CONTINUO_PG_WAREHOUSE_PASSWORD is required when CONTINUO_PG_CREATE_LOGINS=true}"
  : "${CONTINUO_PG_WAREHOUSE_DB:?CONTINUO_PG_WAREHOUSE_DB is required when CONTINUO_PG_CREATE_LOGINS=true}"
  echo "Creating login ${CONTINUO_PG_WAREHOUSE_LOGIN}..."
  create_login "${CONTINUO_PG_WAREHOUSE_LOGIN}" "${CONTINUO_PG_WAREHOUSE_PASSWORD}"
fi

# Every runtime login must exist before anything changes, so an upgrade with a
# missing login stops here with the running version untouched.
missing="$(${PSQL} -d postgres -tA -v logins="${ALL_LOGINS}" -f "${LOGINS_DIR}/require.sql")"
if [ -n "${missing}" ]; then
  echo "${missing}" >&2
  exit 1
fi

# Ensure every database exists before migrating it. Creating them here (rather
# than relying on the Postgres initdb scripts, which only run on a fresh data
# directory) makes provisioning idempotent and safe on long-lived volumes:
# adding a new database never requires a manual CREATE on an existing cluster.
# The migration login owns the databases it creates, which lets it grant on
# them.
for db in ${DATABASES}; do
  if [ "$(${PSQL} -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'continuo_${db}'")" != "1" ]; then
    echo "Creating database continuo_${db}..."
    ${PSQL} -d postgres -c "CREATE DATABASE continuo_${db}"
  fi
done

for db in ${DATABASES}; do
  login="$(login_for "${db}")"
  ${PSQL} -d postgres -v db="continuo_${db}" -v login="${login}" -f "${LOGINS_DIR}/connect.sql"

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

  echo "Granting ${login} its rights on continuo_${db}..."
  ${PSQL} -d "continuo_${db}" -v login="${login}" -f "${LOGINS_DIR}/grant.sql"
done

CHECKED_DBS=""
for db in ${DATABASES}; do
  CHECKED_DBS="${CHECKED_DBS:+${CHECKED_DBS},}continuo_${db}"
done

if [ "${CONTINUO_PG_CREATE_LOGINS}" = "true" ]; then
  wh_db="${CONTINUO_PG_WAREHOUSE_DB}"
  wh_login="${CONTINUO_PG_WAREHOUSE_LOGIN}"
  wh_exists="$(${PSQL} -d postgres -tA -v db="${wh_db}" <<'SQL'
SELECT 1 FROM pg_database WHERE datname = :'db';
SQL
)"
  if [ "${wh_exists}" != "1" ]; then
    echo "Creating database ${wh_db}..."
    ${PSQL} -d postgres -v db="${wh_db}" -v login="${wh_login}" <<'SQL'
CREATE DATABASE :"db" OWNER :"login";
SQL
  fi
  echo "Handing ${wh_db} to ${wh_login}..."
  ${PSQL} -d postgres -v db="${wh_db}" -v login="${wh_login}" <<'SQL'
ALTER DATABASE :"db" OWNER TO :"login";
SQL
  ${PSQL} -d postgres -v db="${wh_db}" -v login="${wh_login}" -f "${LOGINS_DIR}/connect.sql"
  ${PSQL} -d "${wh_db}" -v login="${wh_login}" -f "${LOGINS_DIR}/warehouse-handover.sql"
  CHECKED_DBS="${CHECKED_DBS},${wh_db}"
fi

# Read the catalog back: every runtime login holds its own rights and nothing
# more. A login a company created with extra attributes, or a database where
# PUBLIC can still connect, stops the deploy here.
problems=""
for db in ${DATABASES}; do
  found="$(${PSQL} -d "continuo_${db}" -tA \
    -v login="$(login_for "${db}")" -v own_db="continuo_${db}" \
    -v dbs="${CHECKED_DBS}" -v migration_login="${POSTGRES_USER}" \
    -f "${LOGINS_DIR}/check.sql")"
  if [ -n "${found}" ]; then
    problems="${problems}${found}
"
  fi
done
if [ -n "${problems}" ]; then
  printf '%s' "${problems}" >&2
  echo "fix each login as described in deploy/CREDENTIALS.md" >&2
  exit 1
fi

echo "All migrations complete."
