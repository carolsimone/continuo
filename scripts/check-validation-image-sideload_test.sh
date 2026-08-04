#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; G="${HERE}/check-validation-image-sideload.sh"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }

REF="ghcr.io/carolsimone/continuo-validation-postgres:v0.2.0"

write_fixture() {
  # %b (not %s) so a literal \n embedded in $setup_line/$prov_line becomes a
  # real newline — some fixtures below pack two script lines into one
  # argument, and grep must see them as the two separate lines a real script
  # would have, not one long line the bare-form regex could span by accident.
  local dir="$1" setup_line="$2" prov_line="$3"
  mkdir -p "$dir/scripts" "$dir/tests/e2e"
  printf '#!/bin/bash\ndocker pull %s\n%b\n' "$REF" "$setup_line" > "$dir/scripts/setup.sh"
  printf '#!/usr/bin/env bash\ndocker pull %s\n%b\n' "$REF" "$prov_line" > "$dir/tests/e2e/provision-k8s-test-env.sh"
}

# --- both scripts use the platform-scoped route -----------------------------
write_fixture "$tmp/good" \
  "kind_load_pulled_image ${REF} \"\${CLUSTER_NAME}\"" \
  "kind_load_pulled_image ${REF} continuo || exit 1"
out="$(bash "$G" "$tmp/good" 2>&1)"; rc=$?
assert "platform-scoped route in both scripts passes" "[ $rc -eq 0 ]"

# --- setup.sh regresses to a bare kind load ---------------------------------
write_fixture "$tmp/setup-regressed" \
  "kind load docker-image ${REF} --name \"\${CLUSTER_NAME}\"" \
  "kind_load_pulled_image ${REF} continuo || exit 1"
out="$(bash "$G" "$tmp/setup-regressed" 2>&1)"; rc=$?
assert "bare kind load in setup.sh is caught" "[ $rc -ne 0 ]"
assert "bare kind load failure names setup.sh" "[[ \"\$out\" == *'scripts/setup.sh'* ]]"

# --- provision-k8s-test-env.sh regresses to a bare kind load ----------------
write_fixture "$tmp/prov-regressed" \
  "kind_load_pulled_image ${REF} \"\${CLUSTER_NAME}\"" \
  "kind load docker-image ${REF} --name continuo || { log_error nope; exit 1; }"
out="$(bash "$G" "$tmp/prov-regressed" 2>&1)"; rc=$?
assert "bare kind load in provision-k8s-test-env.sh is caught" "[ $rc -ne 0 ]"
assert "bare kind load failure names provision-k8s-test-env.sh" "[[ \"\$out\" == *'provision-k8s-test-env.sh'* ]]"

# --- a script drops the validation-image side-load entirely -----------------
write_fixture "$tmp/dropped" \
  "# no validation image handling at all" \
  "kind_load_pulled_image ${REF} continuo || exit 1"
out="$(bash "$G" "$tmp/dropped" 2>&1)"; rc=$?
assert "a script that stops loading the validation image at all fails" "[ $rc -ne 0 ]"

# --- other kind load docker-image lines (locally built images) are ignored --
write_fixture "$tmp/other-images-ok" \
  "kind load docker-image continuo-executor-controller:latest --name \"\${CLUSTER_NAME}\"\nkind_load_pulled_image ${REF} \"\${CLUSTER_NAME}\"" \
  "kind load docker-image dbt-base:latest --name continuo || exit 1\nkind_load_pulled_image ${REF} continuo || exit 1"
out="$(bash "$G" "$tmp/other-images-ok" 2>&1)"; rc=$?
assert "bare kind load of locally-built images is not flagged" "[ $rc -eq 0 ]"

# --- a THIRD file (not one of the two known scripts) uses the bare form -----
# The rule this guard exists to enforce is "nothing side-loads this ref with
# the bare form," not "these two specific files don't" — a new install-test
# harness or docs snippet doing it must be caught too.
write_fixture "$tmp/third-file" \
  "kind_load_pulled_image ${REF} \"\${CLUSTER_NAME}\"" \
  "kind_load_pulled_image ${REF} continuo || exit 1"
mkdir -p "$tmp/third-file/scripts"
printf '#!/usr/bin/env bash\nkind load docker-image %s --name continuo\n' "$REF" \
  > "$tmp/third-file/scripts/other-provisioner.sh"
out="$(bash "$G" "$tmp/third-file" 2>&1)"; rc=$?
assert "a bare form in a third, unlisted file is caught" "[ $rc -ne 0 ]"
assert "third-file failure names the offending file" "[[ \"\$out\" == *'other-provisioner.sh'* ]]"

# --- a flag placed BEFORE the ref is still the bare form ---------------------
write_fixture "$tmp/flag-before-ref" \
  "kind load docker-image --name \"\${CLUSTER_NAME}\" ${REF}" \
  "kind_load_pulled_image ${REF} continuo || exit 1"
out="$(bash "$G" "$tmp/flag-before-ref" 2>&1)"; rc=$?
assert "a flag before the ref does not dodge the bare-form check" "[ $rc -ne 0 ]"
assert "flag-before-ref failure names setup.sh" "[[ \"\$out\" == *'scripts/setup.sh'* ]]"

# --- the guard's own *_test.sh fixtures are excluded, not flagged forever ---
# This file (and check-validation-image-pin_test.sh) deliberately embed the
# bare form as literal negative-test text. If the repo-wide scan ever swept
# those in, the guard would fail permanently against its own test suite.
write_fixture "$tmp/own-fixtures-excluded" \
  "kind_load_pulled_image ${REF} \"\${CLUSTER_NAME}\"" \
  "kind_load_pulled_image ${REF} continuo || exit 1"
printf 'printf "kind load docker-image %s --name continuo\\n"\n' "$REF" \
  > "$tmp/own-fixtures-excluded/scripts/some-guard_test.sh"
out="$(bash "$G" "$tmp/own-fixtures-excluded" 2>&1)"; rc=$?
assert "a *_test.sh fixture embedding the bare form is not flagged" "[ $rc -eq 0 ]"

# --- the real repo must already be clean ------------------------------------
bash "$G" "${HERE}/.." >/dev/null 2>&1; assert "the real repo uses the platform-scoped route" "[ $? -eq 0 ]"

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
