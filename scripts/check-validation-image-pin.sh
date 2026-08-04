#!/usr/bin/env bash
# Fails if the pinned continuo-validation-<engine> image ref has drifted
# between any of the places that hard-code it.
#
# The validation image now ships from its own repository
# (github.com/carolsimone/continuo-validation) on its own release train, so
# its version is a plain string, not something `go.mod`/`package.json`
# resolve for us. That string is hand-maintained in every location that
# side-loads or references it: the Makefile, the local/e2e cluster bootstrap
# scripts, the e2e static Deployment fixture, docker-compose, the
# executor-controller Go test fixtures, the chart's own default (read by
# rendering the chart, not by grepping values.yaml as text, since the actual
# ref is composed from registry/prefix/engine/tag at template time), and the
# chart's *fallback* default in templates/_helpers.tpl (read by rendering the
# chart a second time with validation.imageTag explicitly unset, since that
# literal only ever appears when the key is absent from the merged values).
#
# Nothing arbitrates between these locations, and the quiet failure is worse
# than the loud one: if one location advances and another lags, either the
# lagging side fails an image pull, or — with
# VALIDATION_IMAGE_PULL_POLICY=IfNotPresent — it silently validates against
# the OLD runner instead of failing at all.
#
# This check extracts the full image ref from every location above and fails
# unless they are all one identical string.
#
# Scope note: every hand-maintained location here only ever pins the
# postgres engine (the default/e2e engine) as a bare `repo:tag` ref. The only
# place an operator can legitimately move to a `tag@sha256:digest` pin is
# `deploy/continuo/values.yaml`'s `validation.imageTag` (see its comment).
# REF_RE captures that optional `@...` suffix rather than stopping at the
# tag, so a digest-pinned chart still disagrees with the bare-tag dev/e2e
# locations as intended (those track the chart's *default*, not an
# operator's digest override), two genuinely different digest pins disagree
# with each other too (instead of both truncating to the same bare
# `repo:tag` and comparing equal), and — since REF_RE deliberately matches
# ANY `@...` suffix, not just a well-formed one — `record()` separately
# rejects a malformed digest (wrong algorithm, wrong length, stray
# characters) as a named error instead of silently truncating it away and
# comparing the mangled ref equal to everything else.

set -uo pipefail

REPO_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
CHART_DIR="${REPO_ROOT}/deploy/continuo"

# Matches the full ref: ghcr.io/carolsimone/continuo-validation-postgres:vX.Y.Z,
# optionally followed by an `@...` digest suffix. Deliberately permissive
# about the digest's *shape* (any algorithm-name-and-value-like text after
# the `@`, not just a well-formed `@sha256:<64 hex>`) so a malformed digest
# is captured whole rather than silently dropped by a pattern that only
# recognizes the correct form — record() below is what actually validates
# it, and reports a bad digest as a named error instead of quietly
# truncating it (and comparing the mangled, digest-less ref equal to
# everything else).
REF_RE='ghcr\.io/carolsimone/continuo-validation-postgres:[A-Za-z0-9._-]+(@[A-Za-z0-9:_-]+)?'

# A ref's optional digest suffix, if present, must be exactly this shape.
DIGEST_SUFFIX_RE='@sha256:[0-9a-f]{64}$'

# Prints every ref matching REF_RE in $1, one per line (possibly none).
extract_all_refs() {
  grep -oE "$REF_RE" "$1" 2>/dev/null
}

names=()
refs=()
fail=0

# Records one (name, ref) pair. An empty ref is a hard failure — a location
# that stopped pinning the image (moved file, edited-away line) must be
# caught, not silently skipped from the comparison. A ref carrying an `@`
# that isn't a well-formed `@sha256:<64 hex>` digest is also a hard failure,
# named on its own — not folded into the drift comparison, where it would
# either compare (wrongly) equal to a bare-tag ref sharing the same repo:tag
# prefix, or just show up as one more "disagreeing" ref without saying what
# is actually wrong with it.
record() {
  local name="$1" ref="$2"
  if [ -z "$ref" ]; then
    echo "check-validation-image-pin: no image ref found for ${name}" >&2
    fail=1
    return
  fi
  if [[ "$ref" == *@* ]] && ! [[ "$ref" =~ $DIGEST_SUFFIX_RE ]]; then
    echo "check-validation-image-pin: malformed digest pin for ${name}: ${ref} (want @sha256:<64 lowercase hex chars>)" >&2
    fail=1
    return
  fi
  names+=("$name")
  refs+=("$ref")
}

# Reads every ref out of a file into the named array, the long way: arrays
# read from a `while` loop rather than `mapfile`, since this has to run on
# the macOS system bash (3.2) too.
read_refs() {
  local file="$1" out_name="$2"
  local ref
  local -a collected=()
  while IFS= read -r ref; do
    [ -n "$ref" ] && collected+=("$ref")
  done < <(extract_all_refs "$file")
  eval "$out_name=(\"\${collected[@]}\")"
}

# --- literal, hand-maintained pins ---------------------------------------

record "Makefile" "$(extract_all_refs "${REPO_ROOT}/Makefile" | head -1)"

read_refs "${REPO_ROOT}/scripts/setup.sh" setup_refs
record "scripts/setup.sh (docker pull)" "${setup_refs[0]:-}"
record "scripts/setup.sh (kind load)" "${setup_refs[1]:-}"

read_refs "${REPO_ROOT}/tests/e2e/provision-k8s-test-env.sh" prov_refs
record "tests/e2e/provision-k8s-test-env.sh (docker pull)" "${prov_refs[0]:-}"
record "tests/e2e/provision-k8s-test-env.sh (kind load)" "${prov_refs[1]:-}"

record "tests/e2e/k8s/executor-controller-deployment.yaml" \
  "$(extract_all_refs "${REPO_ROOT}/tests/e2e/k8s/executor-controller-deployment.yaml" | head -1)"

record "docker-compose.yml" \
  "$(extract_all_refs "${REPO_ROOT}/docker-compose.yml" | head -1)"

record "executor-controller/adapters/k8s/candidate_schema_lifecycle_test.go" \
  "$(extract_all_refs "${REPO_ROOT}/executor-controller/adapters/k8s/candidate_schema_lifecycle_test.go" | head -1)"

read_refs "${REPO_ROOT}/executor-controller/adapters/k8s/create_validation_job_test.go" job_refs
record "executor-controller/adapters/k8s/create_validation_job_test.go (setenv)" "${job_refs[0]:-}"
record "executor-controller/adapters/k8s/create_validation_job_test.go (assert)" "${job_refs[1]:-}"

# --- the chart's own default, read the only honest way: rendered ---------

if [ ! -d "$CHART_DIR" ]; then
  echo "check-validation-image-pin: ${CHART_DIR} not found" >&2
  exit 1
fi

if ! command -v helm >/dev/null 2>&1; then
  echo "check-validation-image-pin: helm not found on PATH" >&2
  exit 1
fi

rendered="$(helm template pin-check "$CHART_DIR" --kube-version 1.29.0 2>&1)"
render_rc=$?
if [ "$render_rc" -ne 0 ]; then
  echo "check-validation-image-pin: helm template failed:" >&2
  echo "$rendered" >&2
  exit 1
fi
chart_ref="$(printf '%s\n' "$rendered" | grep -oE "$REF_RE" | head -1)"
record "deploy/continuo (helm template default)" "$chart_ref"

# templates/_helpers.tpl's continuo.validation.image also hand-maintains a
# fallback tag literal, used only when validation.imageTag is absent from the
# merged values (a release upgraded before the key existed, or an explicit
# `imageTag: null` override — Helm drops a null-valued override key before
# merging, so both collapse to "key absent"). values.schema.json cannot see
# that literal, since it never appears in values.yaml, so nothing else in
# this script would catch it drifting from the values.yaml default. Rendering
# with the key explicitly unset exercises that fallback path directly.
rendered_unset="$(helm template pin-check "$CHART_DIR" --kube-version 1.29.0 --set validation.imageTag=null 2>&1)"
render_unset_rc=$?
if [ "$render_unset_rc" -ne 0 ]; then
  echo "check-validation-image-pin: helm template (validation.imageTag unset) failed:" >&2
  echo "$rendered_unset" >&2
  exit 1
fi
chart_ref_unset="$(printf '%s\n' "$rendered_unset" | grep -oE "$REF_RE" | head -1)"
record "deploy/continuo (helm template, imageTag unset — helper default)" "$chart_ref_unset"

if [ "${#refs[@]}" -eq 0 ]; then
  echo "check-validation-image-pin: found no pins at all — the extraction patterns may be stale" >&2
  exit 1
fi

canonical="${refs[0]}"
for ref in "${refs[@]}"; do
  [ "$ref" != "$canonical" ] && fail=1
done

if [ "$fail" -ne 0 ]; then
  echo "VALIDATION IMAGE PIN DRIFT — these locations disagree on the continuo-validation image ref:" >&2
  for i in "${!refs[@]}"; do
    echo "  ${refs[$i]}  <-  ${names[$i]}" >&2
  done
  exit 1
fi

echo "Validation image pin OK: ${canonical} (${#refs[@]} locations)"
exit 0
