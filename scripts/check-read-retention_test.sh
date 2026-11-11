#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; G="${HERE}/check-read-retention.sh"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }

rule() { printf '            {"ID": "%s", "Filter": {"Prefix": "%s"}, "Status": "Enabled", "Expiration": {"Days": 30}}\n' "$1" "$2"; }

{ rule expire-logs logs/; rule expire-candidate-sql candidate-sql/; } > "$tmp/good.yml"
bash "$G" "$tmp/good.yml" >/dev/null 2>&1; assert "expiry on other prefixes passes" "[ $? -eq 0 ]"

{ rule expire-logs logs/; rule expire-code-bundles code-bundles/; } > "$tmp/bundles.yml"
bash "$G" "$tmp/bundles.yml" >/dev/null 2>&1; assert "expiry on code-bundles/ fails" "[ $? -ne 0 ]"

rule expire-tenants tenants/ > "$tmp/tenants.yml"
bash "$G" "$tmp/tenants.yml" >/dev/null 2>&1; assert "expiry on tenants/ fails" "[ $? -ne 0 ]"

# A prefix that merely starts like a read prefix is not one.
rule expire-other code-bundles-scratch/ > "$tmp/lookalike.yml"
bash "$G" "$tmp/lookalike.yml" >/dev/null 2>&1; assert "a different prefix passes" "[ $? -eq 0 ]"

# A rule that names the prefix but does not expire it is allowed.
printf '            {"ID": "abort-uploads", "Filter": {"Prefix": "tenants/"}, "Status": "Enabled", "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}\n' > "$tmp/abort.yml"
bash "$G" "$tmp/abort.yml" >/dev/null 2>&1; assert "multipart-abort rule on tenants/ passes" "[ $? -eq 0 ]"

bash "$G" >/dev/null 2>&1; assert "repo compose files pass" "[ $? -eq 0 ]"

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
