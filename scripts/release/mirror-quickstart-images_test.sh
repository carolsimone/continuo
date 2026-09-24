#!/usr/bin/env bash
# Test for mirror-quickstart-images.sh. Stubs `docker` on PATH so no
# network/registry is touched.
set -euo pipefail
cd "$(dirname "$0")/../.."
SCRIPT=scripts/release/mirror-quickstart-images.sh
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
fail=0

cat > "$TMP/docker" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
echo "docker $*" >> "$STUB_LOG"
sub="$*"
if [[ "$sub" == *"imagetools inspect"* ]]; then
  ref="${sub##* }"
  if [[ -n "${STUB_INSPECT_FAIL:-}" && "$ref" == *"$STUB_INSPECT_FAIL"* && "$sub" != *"--raw"* ]]; then
    exit 1
  fi
  if [[ "$sub" == *"--raw"* ]]; then
    arches="${STUB_ARCHES:-amd64 arm64}"; printf '{"manifests":['; first=1
    for a in $arches; do [[ $first -eq 0 ]] && printf ','; printf '{"platform":{"os":"linux","architecture":"%s"}}' "$a"; first=0; done
    printf ']}'
  fi
  exit 0
fi
exit 0
STUB
chmod +x "$TMP/docker"; export PATH="$TMP:$PATH" STUB_LOG="$TMP/log"
run() { : > "$STUB_LOG"; MINIO_SRC="$1" MC_SRC="$2" bash "$SCRIPT" carolsimone; }

# Case 1: both images copied to ghcr continuo-* at the source tag.
if run "bitnamilegacy/minio:2025.7.23" "bitnamilegacy/minio-client:2025.7.21"; then
  grep -q "imagetools create -t ghcr.io/carolsimone/continuo-minio:2025.7.23" "$STUB_LOG" || { echo "FAIL: minio not copied"; fail=1; }
  grep -q "imagetools create -t ghcr.io/carolsimone/continuo-mc:2025.7.21" "$STUB_LOG" || { echo "FAIL: mc not copied"; fail=1; }
else echo "FAIL: happy path non-zero"; fail=1; fi

# Case 2: a missing source aborts BEFORE any create (verify-all-then-copy).
: > "$STUB_LOG"
if STUB_INSPECT_FAIL="minio-client" run "bitnamilegacy/minio:2025.7.23" "bitnamilegacy/minio-client:2025.7.21"; then
  echo "FAIL: missing source should abort"; fail=1
else grep -q "imagetools create" "$STUB_LOG" && { echo "FAIL: partial mirror"; fail=1; } || true; fi

# Case 3: single-arch result fails the multi-arch assertion.
if STUB_ARCHES="amd64" run "bitnamilegacy/minio:2025.7.23" "bitnamilegacy/minio-client:2025.7.21"; then
  echo "FAIL: single-arch should fail"; fail=1
fi

[[ $fail -eq 0 ]] && echo "ALL PASS" || { echo "TESTS FAILED"; exit 1; }
