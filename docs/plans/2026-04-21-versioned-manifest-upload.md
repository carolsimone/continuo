# Versioned Manifest Upload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `dbt/dbt_upload/upload.py` check S3 for the current highest manifest version per service and upload as `manifest_v{N+1}.json`, then wire `dbt/update-graph.sh` to run the upload before triggering the UI.

**Architecture:** Add a `next_version()` helper to `upload.py` that lists S3 objects under a service prefix, parses version numbers from `manifest_v{N}.json` filenames, and returns `max+1` (defaulting to 1 on an empty prefix). `upload_manifest()` calls `next_version()` to compute the destination key before uploading. `update-graph.sh` calls `dbt_upload load` (compile + versioned upload) then POSTs to the UI.

**Tech Stack:** Python 3.12, boto3, pytest, uv (test runner), bash

---

## File Map

| File | Change |
|------|--------|
| `dbt/dbt_upload/upload.py` | Add `next_version()`, update `upload_manifest()` to use it |
| `dbt/tests/test_upload_unit.py` | Add `next_version()` tests, fix existing assertions to expect versioned key |
| `dbt/tests/test_upload.py` | Fix integration test assertions to expect versioned key |
| `dbt/update-graph.sh` | Call `dbt_upload load` before UI trigger, restore localhost default |

---

## Task 1: Add `next_version()` to upload.py

**Files:**
- Modify: `dbt/dbt_upload/upload.py`
- Test: `dbt/tests/test_upload_unit.py`

- [ ] **Step 1: Write the failing tests for `next_version()`**

Append to `dbt/tests/test_upload_unit.py`:

```python
from dbt_upload.upload import next_version


class TestNextVersion:
    def test_returns_1_when_prefix_is_empty(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {}  # no Contents key

        assert next_version(s3, "my-bucket", "dev/manifest/service-1/") == 1

    def test_returns_1_when_no_versioned_files(self):
        s3 = MagicMock()
        # prefix has a manifest.json but no versioned file
        s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "dev/manifest/service-1/manifest.json"}]
        }

        assert next_version(s3, "my-bucket", "dev/manifest/service-1/") == 1

    def test_returns_max_plus_1(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "dev/manifest/service-1/manifest_v1.json"},
                {"Key": "dev/manifest/service-1/manifest_v3.json"},
            ]
        }

        assert next_version(s3, "my-bucket", "dev/manifest/service-1/") == 4

    def test_single_existing_version(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "dev/manifest/service-1/manifest_v2.json"}]
        }

        assert next_version(s3, "my-bucket", "dev/manifest/service-1/") == 3

    def test_passes_correct_bucket_and_prefix(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {}

        next_version(s3, "continuo-dev", "dev/manifest/svc/")

        s3.list_objects_v2.assert_called_once_with(
            Bucket="continuo-dev", Prefix="dev/manifest/svc/"
        )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd dbt && uv run pytest tests/test_upload_unit.py::TestNextVersion -v
```

Expected: `ImportError` or `AttributeError` — `next_version` does not exist yet.

- [ ] **Step 3: Implement `next_version()` in upload.py**

Add after the imports in `dbt/dbt_upload/upload.py`, before `filter_manifest`:

```python
import re

_VERSION_RE = re.compile(r'^manifest_(v\d+)\.json$')


def next_version(s3_client, bucket: str, prefix: str) -> int:
    """Return the next version int for a service S3 prefix.

    Lists all objects under prefix, finds the highest manifest_v{N}.json,
    and returns N+1. Returns 1 if no versioned manifest exists yet.
    """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    max_v = 0
    for obj in response.get("Contents", []):
        filename = obj["Key"].split("/")[-1]
        m = _VERSION_RE.match(filename)
        if m:
            n = int(m.group(1)[1:])  # "v3" → 3
            max_v = max(max_v, n)
    return max_v + 1
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd dbt && uv run pytest tests/test_upload_unit.py::TestNextVersion -v
```

Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add dbt/dbt_upload/upload.py dbt/tests/test_upload_unit.py
rtk git commit -m "feat(dbt-upload): add next_version() S3 version detection helper"
```

---

## Task 2: Update `upload_manifest()` to use versioned key

**Files:**
- Modify: `dbt/dbt_upload/upload.py`
- Test: `dbt/tests/test_upload_unit.py`

- [ ] **Step 1: Update existing `TestUploadManifest` tests to expect versioned key**

In `dbt/tests/test_upload_unit.py`, replace the entire `TestUploadManifest` class:

```python
class TestUploadManifest:
    def test_uploads_to_versioned_key_when_s3_is_empty(self, tmp_path):
        service_dir = tmp_path / "service-1"
        target_dir = service_dir / "target"
        target_dir.mkdir(parents=True)
        (target_dir / "manifest.json").write_text('{"nodes": {}}')

        s3 = MagicMock()
        s3.list_objects_v2.return_value = {}  # empty prefix → version 1

        result = upload_manifest(s3, str(service_dir), "dev", "my-bucket")

        assert result is True
        s3.upload_file.assert_called_once_with(
            str(target_dir / "manifest.json"),
            "my-bucket",
            "dev/manifest/service-1/manifest_v1.json",
        )

    def test_increments_version_when_v1_exists(self, tmp_path):
        service_dir = tmp_path / "service-1"
        target_dir = service_dir / "target"
        target_dir.mkdir(parents=True)
        (target_dir / "manifest.json").write_text('{"nodes": {}}')

        s3 = MagicMock()
        s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "dev/manifest/service-1/manifest_v1.json"}]
        }

        result = upload_manifest(s3, str(service_dir), "dev", "my-bucket")

        assert result is True
        s3.upload_file.assert_called_once_with(
            str(target_dir / "manifest.json"),
            "my-bucket",
            "dev/manifest/service-1/manifest_v2.json",
        )

    def test_returns_false_when_manifest_missing(self, tmp_path):
        service_dir = tmp_path / "service-1"
        service_dir.mkdir()
        s3 = MagicMock()

        result = upload_manifest(s3, str(service_dir), "dev", "my-bucket")

        assert result is False
        s3.upload_file.assert_not_called()
        s3.list_objects_v2.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd dbt && uv run pytest tests/test_upload_unit.py::TestUploadManifest -v
```

Expected: `test_uploads_to_versioned_key_when_s3_is_empty` and `test_increments_version_when_v1_exists` FAIL — current code uses unversioned key.

- [ ] **Step 3: Update `upload_manifest()` in upload.py**

Replace the existing `upload_manifest` function in `dbt/dbt_upload/upload.py`:

```python
def upload_manifest(
    s3_client, service_dir: str, env: str, bucket: str
) -> bool:
    """Upload target/manifest.json to S3 with an auto-incremented version key.

    Checks the current highest manifest_v{N}.json in the service S3 prefix
    and uploads as manifest_v{N+1}.json. Returns True on success.
    """
    service_name = os.path.basename(service_dir)
    manifest_path = os.path.join(service_dir, "target", "manifest.json")

    if not os.path.exists(manifest_path):
        logger.error("manifest.json not found at %s", manifest_path)
        return False

    prefix = f"{env}/manifest/{service_name}/"
    version = next_version(s3_client, bucket, prefix)
    key = f"{env}/manifest/{service_name}/manifest_v{version}.json"
    try:
        s3_client.upload_file(manifest_path, bucket, key)
    except Exception:
        logger.exception("S3 upload failed for %s", service_name)
        return False
    logger.info("Uploaded %s -> s3://%s/%s (v%d)", service_name, bucket, key, version)
    return True
```

- [ ] **Step 4: Run all upload unit tests to verify they pass**

```bash
cd dbt && uv run pytest tests/test_upload_unit.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add dbt/dbt_upload/upload.py dbt/tests/test_upload_unit.py
rtk git commit -m "fix(dbt-upload): upload manifests with versioned key (manifest_v{N}.json)"
```

---

## Task 3: Fix integration test assertions

**Files:**
- Modify: `dbt/tests/test_upload.py`

The integration tests (`test_upload_and_read_back`, `test_all_valid_services_upload`) assert on unversioned key `manifest.json`. After the fix they need to look for `manifest_v1.json` (or any versioned key).

- [ ] **Step 1: Update `test_upload_and_read_back`**

In `dbt/tests/test_upload.py`, replace `test_upload_and_read_back`:

```python
def test_upload_and_read_back(s3):
    """compile + upload produces a readable manifest_v1.json in S3."""
    from dbt_upload.compile import compile_service
    from dbt_upload.upload import upload_manifest

    service_dir = os.path.join(SERVICES_DIR, "service-1")
    assert compile_service(service_dir), "compile_service returned False"
    assert upload_manifest(s3, service_dir, S3_ENV, S3_BUCKET), "upload_manifest returned False"

    key = f"{S3_ENV}/manifest/service-1/manifest_v1.json"
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = json.loads(response["Body"].read())

    assert "nodes" in content
    node_names = [n["name"] for n in content["nodes"].values()]
    assert "table_a" in node_names
```

- [ ] **Step 2: Update `test_all_valid_services_upload`**

Replace `test_all_valid_services_upload` in `dbt/tests/test_upload.py`:

```python
def test_all_valid_services_upload(s3):
    """service-1, service-2, service-3 all compile and upload; service-3-broken is skipped."""
    from dbt_upload.compile import compile_service
    from dbt_upload.upload import upload_manifest

    valid = ["service-1", "service-2", "service-3"]
    for name in valid:
        service_dir = os.path.join(SERVICES_DIR, name)
        assert compile_service(service_dir), f"{name} failed to compile"
        assert upload_manifest(s3, service_dir, S3_ENV, S3_BUCKET), f"{name} failed to upload"

    # verify versioned keys exist in S3
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{S3_ENV}/manifest/")
    keys = {obj["Key"] for obj in response.get("Contents", [])}
    for name in valid:
        versioned_keys = {k for k in keys if k.startswith(f"{S3_ENV}/manifest/{name}/manifest_v")}
        assert versioned_keys, f"No versioned manifest found in S3 for {name}"
```

- [ ] **Step 3: Run unit tests to make sure nothing broke**

```bash
cd dbt && uv run pytest tests/test_upload_unit.py tests/test_config.py tests/test_cli.py -v
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
rtk git add dbt/tests/test_upload.py
rtk git commit -m "fix(dbt-upload): update integration test assertions for versioned manifest keys"
```

---

## Task 4: Fix `update-graph.sh`

**Files:**
- Modify: `dbt/update-graph.sh`

The script has two problems:
1. Hardcoded server IP `168.119.224.110:8090` as default — should be `localhost:8090`
2. Does not run the upload step before triggering the UI

- [ ] **Step 1: Rewrite `dbt/update-graph.sh`**

Replace the entire file:

```bash
#!/usr/bin/env bash
# Upload compiled dbt manifests to S3 (versioned), then trigger graph reload in the UI.
#
# Usage:
#   ./dbt/update-graph.sh [s3|local]
#
# Env vars:
#   UI_BASE_URL  — UI endpoint (default: http://localhost:8090)
#   TARGET       — dbt_upload target profile (default: hetzner)
#   SERVICES_DIR — path to services directory (default: dbt/services relative to repo root)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UI_BASE="${UI_BASE_URL:-http://localhost:8090}"
SOURCE="${1:-s3}"
TARGET="${TARGET:-hetzner}"
SERVICES_DIR="${SERVICES_DIR:-$REPO_ROOT/dbt/services}"

echo "==> Uploading manifests to S3 (target: $TARGET)"
(cd "$REPO_ROOT/dbt" && uv run python -m dbt_upload upload \
  --services-dir "$SERVICES_DIR" \
  --target "$TARGET")

echo "==> Triggering graph reload (source: $SOURCE)"
http_code=$(curl -s -o /tmp/graph_update_resp.json -w "%{http_code}" -X POST "$UI_BASE/api/graph/update" \
  -H "Content-Type: application/json" \
  -d "{\"source\":\"$SOURCE\"}")
resp=$(cat /tmp/graph_update_resp.json)

echo "HTTP $http_code — $resp"
[ "$http_code" = "200" ] || exit 1
```

- [ ] **Step 2: Verify script syntax**

```bash
bash -n dbt/update-graph.sh && echo "syntax OK"
```

Expected: `syntax OK`

- [ ] **Step 3: Smoke-test locally (UI must be port-forwarded or running)**

```bash
# With the local docker-compose UI running:
UI_BASE_URL=http://localhost:8090 TARGET=localstack bash dbt/update-graph.sh local
```

Expected: upload step logs each service, then `HTTP 200 — {"ok":true,...}`.

- [ ] **Step 4: Commit**

```bash
rtk git add dbt/update-graph.sh
rtk git commit -m "fix(update-graph): run versioned upload before UI trigger, restore localhost default"
```

---

## Self-Review

**Spec coverage:**
- ✅ Read S3 prefix per service to find highest version → `next_version()`
- ✅ Upload as `manifest_v{N+1}.json` → `upload_manifest()` updated
- ✅ `update-graph.sh` calls upload before UI trigger
- ✅ Hardcoded server IP removed
- ✅ TDD: failing tests written before each implementation
- ✅ Integration test expectations updated

**Placeholder scan:** None found.

**Type consistency:**
- `next_version(s3_client, bucket: str, prefix: str) -> int` — used consistently in `upload_manifest()` and tests
- `upload_manifest(s3_client, service_dir: str, env: str, bucket: str) -> bool` — signature unchanged, assertions updated in all test files
