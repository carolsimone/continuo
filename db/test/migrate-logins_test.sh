#!/usr/bin/env bash
# Runs the migrations image (db/migrate-all.sh) against throwaway Postgres
# containers and checks what it does with each service's login:
#   1. external Postgres, every login created by the company: the job
#      succeeds twice, and a runtime login reaches only its own tables;
#   2. one login missing: the job names it and creates no database;
#   3. a login created with CREATEDB: the catalog check names it;
#   4. PUBLIC granted CREATE on schema public: the job revokes it;
#   5. bundled quickstart: the job creates every login and hands the
#      superuser's warehouse objects to the warehouse login.
# Every container and the network are removed on exit.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="continuo-migrations-logins-test:local"
NET="continuo-migrate-logins-$$"
OUT="$(mktemp)"
CONTAINERS=()
fail=0

cleanup() {
  for c in "${CONTAINERS[@]}"; do docker rm -f "$c" >/dev/null 2>&1; done
  docker network rm "$NET" >/dev/null 2>&1
  rm -f "$OUT"
}
trap cleanup EXIT

ok() { echo "ok - $1"; }
not_ok() { echo "NOT OK - $1"; fail=1; sed 's/^/    /' "$OUT"; }
check() { if eval "$2"; then ok "$1"; else not_ok "$1"; fi; }

RUNTIME_LOGINS="continuo_run_controller continuo_execution_controller continuo_release_controller continuo_remediation continuo_agent_remediation continuo_agent_chat continuo_dead_letter_controller"
LOGINS_ENV="run_controller=continuo_run_controller execution=continuo_execution_controller release=continuo_release_controller agent_chat=continuo_agent_chat remediation=continuo_remediation agent_remediation=continuo_agent_remediation dead_letter=continuo_dead_letter_controller"
CONTINUO_DBS="continuo_run_controller continuo_execution continuo_release continuo_agent_chat continuo_remediation continuo_agent_remediation continuo_dead_letter"

# start_pg <name> <image>: starts Postgres on the test network and waits
# until it accepts connections.
start_pg() {
  local name="$1" image="$2"
  CONTAINERS+=("$name")
  docker run -d --name "$name" --network "$NET" -e POSTGRES_PASSWORD=super "$image" >/dev/null
  for _ in $(seq 1 60); do
    if docker exec "$name" pg_isready -U postgres -h 127.0.0.1 >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "postgres $name never became ready" >&2
  exit 1
}

# su_sql <pg> <db> <sql>: runs SQL as the postgres superuser.
su_sql() { docker exec -i "$1" psql -X -q -tA -v ON_ERROR_STOP=1 -U postgres -d "$2" -c "$3"; }

# as_login <pg> <login> <password> <db> <sql>: signs in over the network,
# with the login's password, as a service would.
as_login() {
  docker run --rm --network "$NET" -e PGPASSWORD="$3" --entrypoint psql "$IMAGE" \
    -X -tA -v ON_ERROR_STOP=1 -h "$1" -U "$2" -d "$4" -c "$5" 2>&1
}

# migrate <pg> <user> <password> [extra docker -e args...]: runs the
# migrations image, writes its output to $OUT and returns its exit code.
migrate() {
  local pg="$1" user="$2" pw="$3"; shift 3
  docker run --rm --network "$NET" \
    -e POSTGRES_HOST="$pg" -e POSTGRES_USER="$user" -e POSTGRES_PASSWORD="$pw" \
    -e CONTINUO_PG_LOGINS="$LOGINS_ENV" "$@" "$IMAGE" >"$OUT" 2>&1
}

# company_logins <pg> [skip-login] [extra-attributes-login extra-attributes]:
# acts as a company: a migration login with CREATEDB, and each runtime
# login with LOGIN and a password.
company_logins() {
  local pg="$1" skip="${2:-}" special="${3:-}" attrs="${4:-}"
  su_sql "$pg" postgres "CREATE ROLE continuo_svc LOGIN CREATEDB PASSWORD 'svc'" >/dev/null
  for l in $RUNTIME_LOGINS; do
    [ "$l" = "$skip" ] && continue
    if [ "$l" = "$special" ]; then
      su_sql "$pg" postgres "CREATE ROLE $l LOGIN $attrs PASSWORD 'pw-$l'" >/dev/null
    else
      su_sql "$pg" postgres "CREATE ROLE $l LOGIN PASSWORD 'pw-$l'" >/dev/null
    fi
  done
}

echo "building the migrations image..."
docker build -q -t "$IMAGE" -f "$ROOT/db/Dockerfile.migrate" "$ROOT/db" >/dev/null || { echo "image build failed" >&2; exit 1; }
docker network create "$NET" >/dev/null

# --- 1. external Postgres, happy path --------------------------------------
start_pg pg-happy-$$ postgres:16.4
company_logins pg-happy-$$
migrate pg-happy-$$ continuo_svc svc; rc=$?
check "1. the job succeeds when every login exists" '[ "$rc" -eq 0 ]'
migrate pg-happy-$$ continuo_svc svc; rc=$?
check "1. a second run succeeds and changes nothing it should not" '[ "$rc" -eq 0 ]'
res="$(as_login pg-happy-$$ continuo_run_controller pw-continuo_run_controller continuo_run_controller 'SELECT count(*) FROM schedules')"
check "1. continuo_run_controller reads its own table" '[ "$res" = "0" ] || { echo "$res" >"$OUT"; false; }'
res="$(as_login pg-happy-$$ continuo_run_controller pw-continuo_run_controller continuo_run_controller "INSERT INTO schedules (tenant_id, name, cron, timezone, updated_by) VALUES ('t', 's', '@daily', 'UTC', 'test') RETURNING 1" 2>&1)"
check "1. continuo_run_controller writes its own table" 'echo "$res" | grep -q "^1" || { echo "$res" >"$OUT"; false; }'
res="$(as_login pg-happy-$$ continuo_run_controller pw-continuo_run_controller continuo_release 'SELECT 1')"
check "1. continuo_run_controller cannot connect to continuo_release" 'echo "$res" | grep -q "permission denied for database" || { echo "$res" >"$OUT"; false; }'
res="$(as_login pg-happy-$$ continuo_run_controller pw-continuo_run_controller continuo_run_controller 'CREATE TABLE intruder (id int)')"
check "1. continuo_run_controller cannot create a table" 'echo "$res" | grep -q "permission denied for schema public" || { echo "$res" >"$OUT"; false; }'

# --- 2. a login is missing --------------------------------------------------
start_pg pg-missing-$$ postgres:16.4
company_logins pg-missing-$$ continuo_release_controller
migrate pg-missing-$$ continuo_svc svc; rc=$?
check "2. the job fails when a login is missing" '[ "$rc" -ne 0 ]'
check "2. the output names the missing login" 'grep -q "login continuo_release_controller does not exist" "$OUT"'
n="$(su_sql pg-missing-$$ postgres "SELECT count(*) FROM pg_database WHERE datname LIKE 'continuo\_%'")"
check "2. no database was created" '[ "$n" = "0" ]'

# --- 3. a login with too many rights ----------------------------------------
start_pg pg-createdb-$$ postgres:16.4
company_logins pg-createdb-$$ "" continuo_agent_chat CREATEDB
migrate pg-createdb-$$ continuo_svc svc; rc=$?
check "3. the job fails when a login has CREATEDB" '[ "$rc" -ne 0 ]'
check "3. the output names the login and the right" 'grep "login continuo_agent_chat:" "$OUT" | grep -q CREATEDB'

# --- 4. PUBLIC holds CREATE on schema public --------------------------------
start_pg pg-public-$$ postgres:15
company_logins pg-public-$$
for db in $CONTINUO_DBS; do
  su_sql pg-public-$$ postgres "CREATE DATABASE $db OWNER continuo_svc" >/dev/null
  su_sql pg-public-$$ "$db" "GRANT CREATE ON SCHEMA public TO PUBLIC" >/dev/null
done
migrate pg-public-$$ continuo_svc svc; rc=$?
check "4. the job succeeds when PUBLIC holds CREATE on schema public" '[ "$rc" -eq 0 ]'
for db in $CONTINUO_DBS; do
  v="$(su_sql pg-public-$$ "$db" "SELECT has_schema_privilege('continuo_run_controller', 'public', 'CREATE')")"
  check "4. no CREATE on schema public in $db" '[ "$v" = "f" ]'
done

# --- 5. bundled quickstart --------------------------------------------------
start_pg pg-bundled-$$ postgres:16.4
su_sql pg-bundled-$$ postgres "CREATE DATABASE continuo_dbt" >/dev/null
su_sql pg-bundled-$$ continuo_dbt "
  CREATE SCHEMA analytics;
  CREATE TABLE analytics.orders (id serial PRIMARY KEY, amount int);
  CREATE VIEW analytics.big_orders AS SELECT * FROM analytics.orders WHERE amount > 10;
  CREATE MATERIALIZED VIEW analytics.order_count AS SELECT count(*) AS n FROM analytics.orders;
  CREATE SEQUENCE analytics.batch_seq;
  CREATE FUNCTION analytics.double_it(x int) RETURNS int LANGUAGE sql AS 'SELECT x * 2';
  CREATE TABLE public.seed_regions (code text);" >/dev/null
bundled_env=(-e CONTINUO_PG_CREATE_LOGINS=true
  -e CONTINUO_PG_WAREHOUSE_LOGIN=continuo_warehouse -e "CONTINUO_PG_WAREHOUSE_PASSWORD=wh pw'with \\ quote"
  -e CONTINUO_PG_WAREHOUSE_DB=continuo_dbt)
for pair in $LOGINS_ENV; do
  short="${pair%%=*}"; login="${pair#*=}"
  bundled_env+=(-e "CONTINUO_PG_PASSWORD_$(echo "$short" | tr '[:lower:]' '[:upper:]')=pw-$login")
done
migrate pg-bundled-$$ postgres super "${bundled_env[@]}"; rc=$?
check "5. the bundled job succeeds" '[ "$rc" -eq 0 ]'
migrate pg-bundled-$$ postgres super "${bundled_env[@]}"; rc=$?
check "5. a second bundled run succeeds" '[ "$rc" -eq 0 ]'
check "5. no password reaches the output" '! grep -q "pw-continuo\|wh pw" "$OUT"'
res="$(as_login pg-bundled-$$ continuo_release_controller pw-continuo_release_controller continuo_release 'SELECT 1')"
check "5. continuo_release_controller signs in with its generated password" '[ "$res" = "1" ] || { echo "$res" >"$OUT"; false; }'
n="$(su_sql pg-bundled-$$ continuo_dbt "
  SELECT count(*) FROM (
    SELECT nspowner AS owner FROM pg_namespace WHERE nspname = 'analytics'
    UNION ALL SELECT relowner FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname IN ('analytics', 'public') AND c.relkind IN ('r', 'v', 'm', 'S', 'i')
    UNION ALL SELECT proowner FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname = 'analytics'
  ) o WHERE owner <> 'continuo_warehouse'::regrole")"
check "5. every warehouse object belongs to continuo_warehouse" '[ "$n" = "0" ]'
owner="$(su_sql pg-bundled-$$ postgres "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = 'continuo_dbt'")"
check "5. continuo_warehouse owns continuo_dbt" '[ "$owner" = "continuo_warehouse" ]'
res="$(as_login pg-bundled-$$ continuo_warehouse "wh pw'with \\ quote" continuo_dbt 'DROP VIEW analytics.big_orders; SELECT 1')"
check "5. continuo_warehouse replaces a model the superuser built" 'echo "$res" | grep -q "^1" || { echo "$res" >"$OUT"; false; }'
res="$(as_login pg-bundled-$$ continuo_run_controller pw-continuo_run_controller continuo_dbt 'SELECT 1')"
check "5. continuo_run_controller cannot connect to continuo_dbt" 'echo "$res" | grep -q "permission denied for database" || { echo "$res" >"$OUT"; false; }'

if [ "$fail" -ne 0 ]; then echo "FAIL"; exit 1; fi
echo "PASS"
