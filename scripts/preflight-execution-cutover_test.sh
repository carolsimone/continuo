#!/usr/bin/env bash
# Exercises scripts/preflight-execution-cutover.sh with stubbed psql/redis-cli/kubectl.
# The stubs read STUB_* environment variables so each case sets its own state.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stub="$(mktemp -d)"
trap 'rm -rf "$stub"' EXIT

cat > "$stub/psql" <<'EOS'
#!/usr/bin/env bash
case "$*" in
  # Order matters: the unsettled-candidate query also names executor_deployments,
  # so match its distinctive "outcome IS NULL" first.
  *executor_deployments*outcome*IS*NULL*) echo "${STUB_UNSETTLED:-0}";;
  *executor_deployments*)                 echo "${STUB_DEPLOYMENTS:-0}";;
  *executor_outbox*)                      echo "${STUB_EXEC_OUTBOX:-0}";;
  *k8s_outbox*)                           echo "${STUB_K8S_OUTBOX:-0}";;
  *)                                      echo 0;;
esac
EOS

cat > "$stub/redis-cli" <<'EOS'
#!/usr/bin/env bash
if [ -n "${STUB_REDIS_FAIL:-}" ]; then echo "Could not connect to Redis" >&2; exit 1; fi
case "$*" in
  *PING*) echo PONG;;
  *HLEN*) echo "${STUB_TICKETS:-0}";;
  *"XINFO GROUPS"*)
    # One group carrying pending + lag, in redis-cli's default reply layout
    # (field name on one line, value on the next).
    printf '1) 1) "name"\n   2) "grp"\n   3) "pending"\n   4) (integer) %s\n   5) "lag"\n   6) (integer) %s\n' \
      "${STUB_PENDING:-0}" "${STUB_LAG:-0}";;
  *) echo 0;;
esac
EOS

cat > "$stub/kubectl" <<'EOS'
#!/usr/bin/env bash
if [ -n "${STUB_KUBECTL_FAIL:-}" ]; then echo "error: You must be logged in to the server (Unauthorized)" >&2; exit 1; fi
printf '%b' "${STUB_JOBS_LINES:-}"
EOS
chmod +x "$stub"/psql "$stub"/redis-cli "$stub"/kubectl

reset_stubs() {
  export STUB_DEPLOYMENTS=0 STUB_UNSETTLED=0 STUB_EXEC_OUTBOX=0 STUB_K8S_OUTBOX=0
  export STUB_TICKETS=0 STUB_PENDING=0 STUB_LAG=0
  export STUB_JOBS_LINES="" STUB_KUBECTL_FAIL="" STUB_REDIS_FAIL=""
}

run() {
  PATH="$stub:$PATH" POSTGRES_HOST=h POSTGRES_USER=u PGPASSWORD=p REDIS_HOST=r K8S_NAMESPACE=default \
    bash "$here/preflight-execution-cutover.sh"
}

fail() { echo "FAIL: $1"; exit 1; }

# all-zero → GO
reset_stubs
run >/dev/null || fail "all-zero must be GO"

# pending/blocked deployments → NO-GO
reset_stubs; export STUB_DEPLOYMENTS=2
if run >/dev/null 2>&1; then fail "pending deployments must be NO-GO"; fi

# unsettled candidate deployment (deployed, outcome NULL) → NO-GO
reset_stubs; export STUB_UNSETTLED=1
if run >/dev/null 2>&1; then fail "unsettled candidate deployment must be NO-GO"; fi

# delay-queue tickets → NO-GO
reset_stubs; export STUB_TICKETS=3
if run >/dev/null 2>&1; then fail "delay-queue tickets must be NO-GO"; fi

# undrained self-loop consumer group, delivered-unacked (pending) → NO-GO
reset_stubs; export STUB_PENDING=1
if run >/dev/null 2>&1; then fail "pending self-loop messages must be NO-GO"; fi

# undrained self-loop consumer group, undelivered (lag) → NO-GO
reset_stubs; export STUB_LAG=1
if run >/dev/null 2>&1; then fail "lagging self-loop messages must be NO-GO"; fi

# a non-terminal (pending, no conditions) Job → NO-GO
reset_stubs; export STUB_JOBS_LINES='JOB:\n'
if run >/dev/null 2>&1; then fail "non-terminal Job must be NO-GO"; fi

# a terminal FAILED Job must NOT block (this is the field-selector bug being fixed)
reset_stubs; export STUB_JOBS_LINES='JOB:Failed\n'
run >/dev/null || fail "a terminal failed Job must not block cutover"

# a terminal Complete Job alongside a pending one → NO-GO (only the pending counts)
reset_stubs; export STUB_JOBS_LINES='JOB:Complete\nJOB:\n'
if run >/dev/null 2>&1; then fail "a pending Job beside a completed one must be NO-GO"; fi

# kubectl query failure must abort as NO-GO, not read zero
reset_stubs; export STUB_KUBECTL_FAIL=1
if run >/dev/null 2>&1; then fail "kubectl failure must be NO-GO"; fi

# redis unreachable must abort as NO-GO, not read zero
reset_stubs; export STUB_REDIS_FAIL=1
if run >/dev/null 2>&1; then fail "redis failure must be NO-GO"; fi

echo "preflight-execution-cutover_test: ok"
