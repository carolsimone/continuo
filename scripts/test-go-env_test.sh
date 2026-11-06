#!/usr/bin/env bash
# Test for the Makefile's test-deps-up and test-go targets. A stub compose and a
# stub go record what the targets run and the environment they run it in, so no
# container starts and no test runs.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; ROOT="$(cd "${HERE}/.." && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; sed 's/^/    /' "$tmp/out"; fi; }

mkdir -p "$tmp/bin"
cat > "$tmp/bin/compose" <<'STUB'
#!/usr/bin/env bash
printf 'compose %s\n' "$*" >> "$STUB_LOG"
printf 'ports %s %s %s %s\n' "$PG_HOST_PORT" "$REDIS_HOST_PORT" "$NEO4J_HOST_PORT" "$NEO4J_HTTP_HOST_PORT" >> "$STUB_LOG"
if [ "$1" = up ] && [ -n "${STUB_FAILING_JOB:-}" ] && [ "${!#}" = "$STUB_FAILING_JOB" ]; then
  exit 3
fi
exit 0
STUB
cat > "$tmp/bin/go" <<'STUB'
#!/usr/bin/env bash
env | grep -E '^(POSTGRES_PORT|REDIS_ADDR|NEO4J_URI|RELEASE_TEST_PG_DSN)=' >> "$STUB_LOG"
STUB
chmod +x "$tmp/bin/compose" "$tmp/bin/go"

# run ARGS...: runs make with the stubs, from a clean environment for every
# variable under test; exit code in $rc, everything recorded in $tmp/log.
run(){
  : > "$tmp/log"
  env -u PG_HOST_PORT -u REDIS_HOST_PORT -u NEO4J_HOST_PORT -u NEO4J_HTTP_HOST_PORT \
    PATH="$tmp/bin:$PATH" STUB_LOG="$tmp/log" \
    make -s -C "$ROOT" DOCKER_COMPOSE="$tmp/bin/compose" CI_HOST_IP=testhost "$@" >"$tmp/out" 2>&1
  rc=$?
}
logged(){ grep -qxF "$1" "$tmp/log"; }

run test-go SERVICE=pkg
assert "test-go passes" "[ $rc -eq 0 ]"
assert "the default Postgres port reaches the tests" "logged POSTGRES_PORT=5432"
assert "the default Redis port reaches the tests" "logged REDIS_ADDR=testhost:6379"
assert "the default Neo4j port reaches the tests" "logged NEO4J_URI=bolt://testhost:7687"
assert "compose gets the default host ports" "logged 'ports 5432 6379 7687 7474'"

ports=(PG_HOST_PORT=25432 REDIS_HOST_PORT=26379 NEO4J_HOST_PORT=27687 NEO4J_HTTP_HOST_PORT=27474)
run test-go SERVICE=pkg "${ports[@]}"
assert "test-go passes on other ports" "[ $rc -eq 0 ]"
assert "another Postgres port reaches the tests" "logged POSTGRES_PORT=25432"
assert "another Redis port reaches the tests" "logged REDIS_ADDR=testhost:26379"
assert "another Neo4j port reaches the tests" "logged NEO4J_URI=bolt://testhost:27687"
assert "compose gets every other host port" "logged 'ports 25432 26379 27687 27474'"
assert "no compose call keeps the default ports" "! logged 'ports 5432 6379 7687 7474'"

run test-go SERVICE=release-controller "${ports[@]}"
assert "release-controller's own DSN follows the Postgres port" \
  "grep -q '^RELEASE_TEST_PG_DSN=postgres://[^@]*@testhost:25432/' \"$tmp/log\""
run test-go SERVICE=run-controller "${ports[@]}"
assert "run-controller's Redis address follows the Redis port" "logged REDIS_ADDR=testhost:26379"
run test-go SERVICE=dead-letter-controller "${ports[@]}"
assert "dead-letter-controller's Redis address follows the Redis port" "logged REDIS_ADDR=testhost:26379"

run test-deps-up
assert "test-deps-up passes" "[ $rc -eq 0 ]"
assert "the data dependencies start" "logged 'compose up -d postgres neo4j redis'"
for job in migrations streams-init; do
  assert "$job is removed first, so a job left running cannot block the run" "logged 'compose rm -fsv $job'"
  assert "$job is rebuilt before it runs" "logged 'compose up --build --exit-code-from $job --abort-on-container-exit $job'"
done

: > "$tmp/log"
STUB_FAILING_JOB=migrations
export STUB_FAILING_JOB
run test-deps-up
unset STUB_FAILING_JOB
assert "a job that fails fails the target" "[ $rc -ne 0 ]"
assert "a job that fails stops the jobs after it" "! grep -q 'streams-init' \"$tmp/log\""

compose="${ROOT}/docker-compose.yml"
for mapping in '${PG_HOST_PORT:-5432}:5432' '${REDIS_HOST_PORT:-6379}:6379' \
  '${NEO4J_HOST_PORT:-7687}:7687' '${NEO4J_HTTP_HOST_PORT:-7474}:7474'; do
  assert "docker-compose.yml publishes $mapping" "grep -qF '\"$mapping\"' \"$compose\""
done

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
