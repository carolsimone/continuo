#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; G="${HERE}/check-validation-image-pin.sh"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }

# Builds a full fixture tree — every location the guard reads — all pinned to
# $2. A minimal fixture chart stands in for deploy/continuo: its default.yaml
# composes the same ref the real chart's _helpers.tpl does, so the guard
# exercises real `helm template` rendering without depending on the real
# chart's full value surface.
write_fixture() {
  local dir="$1" tag="$2"
  local ref="ghcr.io/carolsimone/continuo-python-runtime-postgres:${tag}"

  mkdir -p "$dir/scripts" "$dir/tests/e2e" "$dir/tests/e2e/k8s" \
    "$dir/executor-controller/adapters/k8s" \
    "$dir/deploy/continuo/templates"

  printf 'validate:\n\t@docker pull %s\n' "$ref" > "$dir/Makefile"

  printf '#!/usr/bin/env bash\ndocker pull %s\n...\nkind load docker-image %s --name continuo\n' \
    "$ref" "$ref" > "$dir/scripts/setup.sh"

  printf '#!/usr/bin/env bash\ndocker pull %s \\\n  --quiet\nkind load docker-image %s --name continuo || exit 1\n' \
    "$ref" "$ref" > "$dir/tests/e2e/provision-k8s-test-env.sh"

  printf -- '- name: VALIDATION_IMAGE\n  value: "%s"\n' "$ref" \
    > "$dir/tests/e2e/k8s/executor-controller-deployment.yaml"

  mkdir -p "$dir/tests/e2e/fixtures/py-probe"
  printf 'FROM %s\nCOPY contracts/ /app/contracts/\n' "$ref" \
    > "$dir/tests/e2e/fixtures/py-probe/Dockerfile"

  printf 'services:\n  executor-controller:\n    environment:\n      - VALIDATION_IMAGE=%s\n' "$ref" \
    > "$dir/docker-compose.yml"

  printf 'package k8s\n\nfunc TestX() { assertEqual("%s", c.Image) }\n' "$ref" \
    > "$dir/executor-controller/adapters/k8s/candidate_schema_lifecycle_test.go"

  printf 'package k8s\n\nfunc TestMain() { os.Setenv("VALIDATION_IMAGE", "%s") }\n\nfunc TestY() { assertEqual("%s", main.Image) }\n' \
    "$ref" "$ref" > "$dir/executor-controller/adapters/k8s/create_validation_job_test.go"

  cat > "$dir/deploy/continuo/Chart.yaml" <<'EOF'
apiVersion: v2
name: fixture
version: 0.1.0
EOF

  printf 'validation:\n  imageTag: "%s"\n' "$tag" > "$dir/deploy/continuo/values.yaml"

  # Mirrors _helpers.tpl's continuo.validation.image: falls back to a
  # hand-maintained literal (defaulting here to the same $tag, like the real
  # helper's literal matches values.yaml's default) whenever
  # validation.imageTag is absent from the merged values — which is what the
  # guard's own `--set validation.imageTag=null` render produces, since Helm
  # drops a null-valued override key before merging. The key path must match
  # the real chart's (nested validation.imageTag, not a flat key): the guard
  # script hardcodes that path for both the real chart and this fixture.
  cat > "$dir/deploy/continuo/templates/configmap.yaml" <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: fixture
data:
  VALIDATION_IMAGE: 'ghcr.io/carolsimone/continuo-python-runtime-postgres:{{ .Values.validation.imageTag | default "${tag}" }}'
EOF
}

# --- every location agrees -------------------------------------------------
write_fixture "$tmp/good" "v0.2.0"
out="$(bash "$G" "$tmp/good" 2>&1)"; rc=$?
assert "all-agree fixture passes" "[ $rc -eq 0 ]"
assert "all-agree fixture reports the shared ref" "[[ \"\$out\" == *v0.2.0* ]]"

# --- one hand-maintained location drifts -----------------------------------
write_fixture "$tmp/drift" "v0.2.0"
sed -i.bak 's/v0.2.0/v0.3.0/' "$tmp/drift/Makefile"
out="$(bash "$G" "$tmp/drift" 2>&1)"; rc=$?
assert "Makefile drift is caught" "[ $rc -ne 0 ]"
assert "Makefile drift names the offending location" "[[ \"\$out\" == *Makefile* ]]"
assert "Makefile drift is a DRIFT report, not a missing-ref report" "[[ \"\$out\" == *'PIN DRIFT'* ]]"

# --- the python-node e2e fixture drifts ------------------------------------
# The case this location exists for: a runtime release moves every other pin
# and leaves the fixture behind, so the python-node e2e keeps proving the
# harness against the OLD base while CI stays green.
write_fixture "$tmp/fixture-drift" "v0.2.0"
sed -i.bak 's/v0.2.0/v0.3.0/' "$tmp/fixture-drift/tests/e2e/fixtures/py-probe/Dockerfile"
rm -f "$tmp/fixture-drift/tests/e2e/fixtures/py-probe/Dockerfile.bak"
out="$(bash "$G" "$tmp/fixture-drift" 2>&1)"; rc=$?
assert "py-probe fixture drift is caught" "[ $rc -ne 0 ]"
assert "py-probe fixture drift names the offending location" "[[ \"\$out\" == *'py-probe/Dockerfile'* ]]"
assert "py-probe fixture drift is a DRIFT report" "[[ \"\$out\" == *'PIN DRIFT'* ]]"

# --- the python-node e2e fixture stops pinning entirely --------------------
write_fixture "$tmp/fixture-missing" "v0.2.0"
printf 'FROM scratch\n' > "$tmp/fixture-missing/tests/e2e/fixtures/py-probe/Dockerfile"
out="$(bash "$G" "$tmp/fixture-missing" 2>&1)"; rc=$?
assert "py-probe fixture losing its pin is caught" "[ $rc -ne 0 ]"
assert "py-probe missing pin names the location" "[[ \"\$out\" == *'py-probe/Dockerfile'* ]]"

# --- the chart's rendered default drifts, not the literal pins ------------
write_fixture "$tmp/chart-drift" "v0.2.0"
sed -i.bak 's/v0.2.0/v0.3.0/' "$tmp/chart-drift/deploy/continuo/values.yaml"
out="$(bash "$G" "$tmp/chart-drift" 2>&1)"; rc=$?
assert "chart-default drift is caught" "[ $rc -ne 0 ]"
assert "chart-default drift names the rendered-default location" "[[ \"\$out\" == *'helm template default'* ]]"

# --- the helper's fallback default drifts from values.yaml's default -------
# (only reachable by rendering with validation.imageTag explicitly unset,
# i.e. the guard's own `--set validation.imageTag=null` render — the shape a
# pre-existing release with no imageTag key merges to.)
write_fixture "$tmp/helper-default-drift" "v0.2.0"
# The .bak must not be left behind: anything under templates/ is itself a
# template Helm renders, so a stray unmutated .bak here would render a
# second ConfigMap still carrying the old (undrifted) ref and mask the drift.
sed -i.bak 's/default "v0.2.0"/default "v0.3.0"/' "$tmp/helper-default-drift/deploy/continuo/templates/configmap.yaml"
rm -f "$tmp/helper-default-drift/deploy/continuo/templates/configmap.yaml.bak"
out="$(bash "$G" "$tmp/helper-default-drift" 2>&1)"; rc=$?
assert "helper-fallback-default drift is caught" "[ $rc -ne 0 ]"
assert "helper-fallback-default drift names the imageTag-unset render" "[[ \"\$out\" == *'imageTag unset'* ]]"

# --- an operator's digest pin (values.yaml only) disagrees with the bare-tag
# dev/e2e locations, as intended: the digest must not get truncated away ----
write_fixture "$tmp/digest-vs-bare" "v0.2.0"
sed -i.bak 's/imageTag: "v0.2.0"/imageTag: "v0.2.0@sha256:1111111111111111111111111111111111111111111111111111111111111111"/' \
  "$tmp/digest-vs-bare/deploy/continuo/values.yaml"
out="$(bash "$G" "$tmp/digest-vs-bare" 2>&1)"; rc=$?
assert "digest-pinned chart vs bare-tag scripts is caught" "[ $rc -ne 0 ]"
assert "digest-pinned drift is a DRIFT report" "[[ \"\$out\" == *'PIN DRIFT'* ]]"

# --- two different digest pins must not compare equal ----------------------
# (regression case for R3: a pattern that stops matching at "@" would
# truncate both refs to the same bare repo:tag and report a false pass.)
write_fixture "$tmp/digest-vs-digest" "v0.2.0"
sed -i.bak 's/imageTag: "v0.2.0"/imageTag: "v0.2.0@sha256:1111111111111111111111111111111111111111111111111111111111111111"/' \
  "$tmp/digest-vs-digest/deploy/continuo/values.yaml"
sed -i.bak 's/v0.2.0/v0.2.0@sha256:2222222222222222222222222222222222222222222222222222222222222222/' \
  "$tmp/digest-vs-digest/Makefile"
out="$(bash "$G" "$tmp/digest-vs-digest" 2>&1)"; rc=$?
assert "two different digest pins are caught as drift" "[ $rc -ne 0 ]"
assert "differing-digest drift is a DRIFT report" "[[ \"\$out\" == *'PIN DRIFT'* ]]"

# --- a malformed digest is a named error, not a silent truncate-and-pass ---
# (regression case for S2: a pattern anchored on the correct `@sha256:<64
# hex>` shape simply fails to match a malformed one and falls back to the
# bare repo:tag, comparing equal to everything else — a typo'd digest must
# not pass.)
write_fixture "$tmp/bad-digest-short" "v0.2.0"
sed -i.bak 's/imageTag: "v0.2.0"/imageTag: "v0.2.0@sha256:deadbeef"/' \
  "$tmp/bad-digest-short/deploy/continuo/values.yaml"
out="$(bash "$G" "$tmp/bad-digest-short" 2>&1)"; rc=$?
assert "a too-short digest fails" "[ $rc -ne 0 ]"
assert "too-short digest is a malformed-digest report, not a false pass" "[[ \"\$out\" == *'malformed digest pin'* ]]"
assert "too-short digest names the bad ref" "[[ \"\$out\" == *'v0.2.0@sha256:deadbeef'* ]]"

write_fixture "$tmp/bad-digest-algo" "v0.2.0"
sed -i.bak 's/imageTag: "v0.2.0"/imageTag: "v0.2.0@sha512:1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111"/' \
  "$tmp/bad-digest-algo/deploy/continuo/values.yaml"
out="$(bash "$G" "$tmp/bad-digest-algo" 2>&1)"; rc=$?
assert "a wrong-algorithm digest fails" "[ $rc -ne 0 ]"
assert "wrong-algorithm digest is a malformed-digest report" "[[ \"\$out\" == *'malformed digest pin'* ]]"

# --- a location stops pinning the image at all -----------------------------
write_fixture "$tmp/missing" "v0.2.0"
: > "$tmp/missing/docker-compose.yml"
out="$(bash "$G" "$tmp/missing" 2>&1)"; rc=$?
assert "a location with no ref at all fails" "[ $rc -ne 0 ]"
assert "missing-ref failure names docker-compose.yml" "[[ \"\$out\" == *docker-compose.yml* ]]"

# --- helm itself failing to render is a hard failure, not a false pass -----
write_fixture "$tmp/badchart" "v0.2.0"
printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: fixture\ndata:\n  x: {{ .Values.doesNotExist.nested }}\n' \
  > "$tmp/badchart/deploy/continuo/templates/configmap.yaml"
out="$(bash "$G" "$tmp/badchart" 2>&1)"; rc=$?
assert "a chart that fails to render fails the guard" "[ $rc -ne 0 ]"

# --- the real repo must be clean --------------------------------------------
bash "$G" "${HERE}/.." >/dev/null 2>&1; assert "the real repo's pins agree" "[ $? -eq 0 ]"

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
