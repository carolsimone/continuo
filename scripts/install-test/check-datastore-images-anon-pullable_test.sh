#!/usr/bin/env bash
# Test for check-datastore-images-anon-pullable.sh. Stubs helm + docker on PATH.
set -euo pipefail
cd "$(dirname "$0")/../.."
SCRIPT=scripts/install-test/check-datastore-images-anon-pullable.sh
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
fail=0

cat > "$TMP/helm" <<'STUB'
#!/usr/bin/env bash
echo 'image: "ghcr.io/carolsimone/continuo-minio:2025.7.23"'
echo 'image: "ghcr.io/carolsimone/continuo-mc:2025.7.21"'
echo 'image: "ghcr.io/carolsimone/continuo-state:v9"'
STUB
cat > "$TMP/docker" <<'STUB'
#!/usr/bin/env bash
sub="$*"; ref="${sub##* }"
[[ -n "${STUB_BAD:-}" && "$ref" == *"$STUB_BAD"* ]] && exit 1
exit 0
STUB
chmod +x "$TMP/helm" "$TMP/docker"; export PATH="$TMP:$PATH"

# Case 1: everything pullable → pass.
bash "$SCRIPT" >/dev/null 2>&1 || { echo "FAIL: all-pullable should pass"; fail=1; }
# Case 2: a datastore image 401s → fail.
STUB_BAD="continuo-mc" bash "$SCRIPT" >/dev/null 2>&1 && { echo "FAIL: a 401 datastore image should fail"; fail=1; } || true
# Case 3: a 401 on a NON-datastore (our service) image must NOT fail this guard.
STUB_BAD="continuo-state" bash "$SCRIPT" >/dev/null 2>&1 || { echo "FAIL: service image is out of scope, should still pass"; fail=1; }

[[ $fail -eq 0 ]] && echo "ALL PASS" || { echo "TESTS FAILED"; exit 1; }
