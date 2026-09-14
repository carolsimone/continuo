#!/usr/bin/env bash
# Exercises scripts/preflight-execution-cutover.sh with stubbed psql/redis-cli/kubectl.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stub="$(mktemp -d)"
trap 'rm -rf "$stub"' EXIT

make_stubs() { # $1 = deployments, $2 = exec outbox, $3 = k8s outbox, $4 = tickets, $5 = jobs
  cat > "$stub/psql" <<EOS
#!/usr/bin/env bash
case "\$*" in
  *continuo_executor*executor_deployments*) echo "$1";;
  *continuo_executor*executor_outbox*) echo "$2";;
  *continuo_k8s*k8s_outbox*) echo "$3";;
  *) echo 0;;
esac
EOS
  cat > "$stub/redis-cli" <<EOS
#!/usr/bin/env bash
echo "$4"
EOS
  cat > "$stub/kubectl" <<EOS
#!/usr/bin/env bash
if [ "$5" -gt 0 ]; then
  for i in \$(seq 1 $5); do echo "job-\$i"; done
fi
EOS
  chmod +x "$stub"/psql "$stub"/redis-cli "$stub"/kubectl
}

run() { PATH="$stub:$PATH" POSTGRES_HOST=h POSTGRES_USER=u PGPASSWORD=p REDIS_HOST=r bash "$here/preflight-execution-cutover.sh"; }

make_stubs 0 0 0 0 0
run >/dev/null || { echo "FAIL: all-zero must be GO"; exit 1; }

make_stubs 2 0 0 0 0
if run >/dev/null 2>&1; then echo "FAIL: pending deployments must be NO-GO"; exit 1; fi

make_stubs 0 0 0 3 0
if run >/dev/null 2>&1; then echo "FAIL: delay-queue tickets must be NO-GO"; exit 1; fi

make_stubs 0 0 0 0 1
if run >/dev/null 2>&1; then echo "FAIL: active jobs must be NO-GO"; exit 1; fi

echo "preflight-execution-cutover_test: ok"
