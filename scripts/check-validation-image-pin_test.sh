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
  local ref="ghcr.io/carolsimone/continuo-validation-postgres:${tag}"

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

  printf 'validationImageTag: "%s"\n' "$tag" > "$dir/deploy/continuo/values.yaml"

  cat > "$dir/deploy/continuo/templates/configmap.yaml" <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: fixture
data:
  VALIDATION_IMAGE: "ghcr.io/carolsimone/continuo-validation-postgres:{{ .Values.validationImageTag }}"
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

# --- the chart's rendered default drifts, not the literal pins ------------
write_fixture "$tmp/chart-drift" "v0.2.0"
sed -i.bak 's/v0.2.0/v0.3.0/' "$tmp/chart-drift/deploy/continuo/values.yaml"
out="$(bash "$G" "$tmp/chart-drift" 2>&1)"; rc=$?
assert "chart-default drift is caught" "[ $rc -ne 0 ]"
assert "chart-default drift names the rendered-default location" "[[ \"\$out\" == *'helm template default'* ]]"

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
