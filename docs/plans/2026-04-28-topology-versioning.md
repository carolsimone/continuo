# Topology Versioning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Strategy B (Run isolation via lazy generation switch) per `docs/specs/2026-04-28-topology-versioning-design.md`. A new `manifest.loaded:v1` arriving mid-Run never affects the in-flight Run; the next Run picks up the new topology atomically. Close the container-immutability gap by carrying a per-service content-addressed `image_tag` from manifest publication all the way to the K8s Pod spec, removing reliance on the global `IMAGE_TAG` env var.

**Architecture:** Additive event-schema extensions across `manifest.loaded:v1`, `run.entries.dispatched:v1`, and `query.model:v1`. Orchestrator gains a monotonic `topology_generation` counter (Postgres singleton) and stamps it onto Run nodes at SnapshotGraph time. EXECUTES edges gain `image_tag` alongside the existing `manifest_version`. State persists `manifest_version` per task. Executor reads `image_tag` from the event payload and refuses empty tags. Local-dev tooling (`scripts/setup.sh`, `dbt-compile-and-load`, `docker-compose.yml`) generates content-addressed image tags per build and writes `service_metadata.json` to S3 alongside dbt manifests.

**Tech Stack:** Go 1.23 (orchestrator, state, executor-controller), Python 3.12 + uv (manifest-controller, dbt-compile-and-load), Neo4j 5, PostgreSQL 16, Redis Streams, Kubernetes via `client-go`, kind for local cluster, LocalStack S3, Docker Compose.

**Sequencing rationale:** Foundation (types + migrations) lands first so later tasks can reference the new struct fields. Then producer-side code is wired in dependency order (manifest-controller → orchestrator ingest → SnapshotGraph → init-node queries → outbox emit). Then consumer-side (state, executor). Tooling lands before the executor's fail-loud behavior so by the time the executor refuses empty `image_tag`, the value is actually arriving on the wire. E2E test validates the full chain.

---

## File Structure

**Created:**
- `db/migration/orchestrator/V6__init_topology_state.sql` — singleton table for `topology_generation` counter.
- `db/migration/state/V13__add_manifest_version_to_task_tracker.sql` — propagates `manifest_version` to read side.
- `orchestrator/adapters/postgres/topology_state_repository.go` — `IncrementGeneration` / `GetGeneration` against the new table.
- `orchestrator/adapters/postgres/topology_state_repository_test.go` — integration tests against real Postgres.
- `dbt/dbt_upload/service_metadata.py` — parsing `IMAGE_TAG_PER_SERVICE`, writing `service_metadata.json`.
- `dbt/tests/test_service_metadata.py` — unit tests for the env parser + writer.
- `tests/e2e/test_topology_versioning.py` (or matching Go file under `tests/e2e/`) — regression test for mid-run isolation.

**Modified:**
- `pkg/events/run_lifecycle.go` — `DispatchedTask` gains `ManifestVersion`, `ImageTag`.
- `orchestrator/domain/model.go` — `NodeReadyForExecution` gains `ManifestVersion`, `ImageTag`.
- `orchestrator/domain/topology/node.go` — `TopologyNode` gains `ImageTag`.
- `orchestrator/domain/command/command.go` — `IngestTopologyNodePayload` gains `ImageTag`.
- `orchestrator/service/command/ingest_topology.go` — increment generation, build per-service `service_metadata` map, pass `image_tag` to topology repo.
- `orchestrator/adapters/neo4j/topology_repository.go` — write `image_tag` and `topology_generation` onto Table nodes; persist per-service `service_metadata` on a `:TopologyRoot` node.
- `orchestrator/adapters/neo4j/run_repository.go` — `SnapshotGraph` stamps `topology_generation` and `service_metadata` on Run, `image_tag` on EXECUTES; init-node queries return `e.manifest_version` and `e.image_tag`.
- `orchestrator/domain/run/run.go` (or wherever `domain.TableNode` lives) — `TableNode` gains `ManifestVersion`, `ImageTag` (per-task, populated from EXECUTES edge).
- `orchestrator/service/command/handle_scheduler_started.go` — `buildRunEntriesDispatchedPayload` populates `ManifestVersion` + `ImageTag` per task; `NodeReadyForExecution` likewise.
- `orchestrator/service/command/handle_rerun.go` — same per-task field propagation.
- `state/domain/model/model.go` — `TaskTracker` gains `ManifestVersion`.
- `state/adapters/postgres/task_repository.go` — INSERT statements include `manifest_version`; SELECTs return it.
- `state/service/handlers/run_entries_dispatched_handler.go` — pass `ManifestVersion` from event payload into `TaskTracker.Create`.
- `executor-controller/adapters/k8s/client.go` — `JobParams.ImageTag` (new field), `buildPodSpec` uses it, returns error on empty.
- `executor-controller/adapters/redis/consumer.go` (or wherever `query.model:v1` is parsed) — extract `image_tag` from payload, pass to `JobParams`.
- `manifest-controller/adapters/sources/s3.py` — return `service_metadata.json` data alongside `manifest.json`.
- `manifest-controller/adapters/sources/local.py` — same.
- `manifest-controller/service/parser.py` — `parse_manifest` accepts `image_tag` and stamps it on each `ManifestNode`.
- `manifest-controller/domain/model.py` — `ManifestNode` gains `image_tag`; `ManifestFile` may gain `service_metadata`.
- `manifest-controller/service/manifest_handler.py` — pass `image_tag` from source through to the published node dicts.
- `dbt/dbt_upload/upload.py` — write `service_metadata.json` to S3 per uploaded service.
- `dbt/dbt_upload/cli.py` — `load`/`upload` subcommands read `IMAGE_TAG_PER_SERVICE` env.
- `scripts/setup.sh` — derive content-addressed tag, build/load tagged service images, export `IMAGE_TAG_PER_SERVICE`.
- `docker-compose.yml` — add `IMAGE_TAG_PER_SERVICE` env to `dbt-compile-and-load`; remove `IMAGE_TAG` from `executor-controller`.
- `docs/arch/01-topology.md`, `docs/arch/03-sequence-flows.md`, `docs/arch/04-service-ownership.md` — reflect new fields and lifecycle.

---

## Task 1: Foundation — schema migrations + shared event types

Adds the `topology_generation` counter table, the `task_tracker.manifest_version` column, and extends shared event structs with the new fields. No behavior change yet — later tasks consume these.

**Files:**
- Create: `db/migration/orchestrator/V6__init_topology_state.sql`
- Create: `db/migration/state/V13__add_manifest_version_to_task_tracker.sql`
- Modify: `pkg/events/run_lifecycle.go`
- Modify: `orchestrator/domain/model.go` (NodeReadyForExecution)
- Modify: `orchestrator/domain/topology/node.go` (TopologyNode.ImageTag)
- Modify: `orchestrator/domain/command/command.go` (IngestTopologyNodePayload.ImageTag)
- Modify: `state/domain/model/model.go` (TaskTracker.ManifestVersion)
- Test: `pkg/events/payloads_test.go`

- [ ] **Step 1: Write the failing test for DispatchedTask round-trip**

Append to `pkg/events/payloads_test.go`:
```go
func TestDispatchedTask_RoundTripWithManifestVersionAndImageTag(t *testing.T) {
    in := events.DispatchedTask{
        TaskID:          uuid.New().String(),
        ServiceName:     "svc-a",
        SchemaName:      "public",
        TableName:       "users",
        NodeType:        "dbt-model",
        MaxRetries:      3,
        ManifestVersion: "v7",
        ImageTag:        "abcd123-1714300000",
    }
    raw, err := json.Marshal(in)
    require.NoError(t, err)

    var out events.DispatchedTask
    require.NoError(t, json.Unmarshal(raw, &out))
    assert.Equal(t, in, out)
    assert.Contains(t, string(raw), `"manifest_version":"v7"`)
    assert.Contains(t, string(raw), `"image_tag":"abcd123-1714300000"`)
}
```

- [ ] **Step 2: Run the test, expect failure**

Run: `docker exec orchestrator go test ./pkg/events/ -run TestDispatchedTask_RoundTripWithManifestVersionAndImageTag -v`
Expected: FAIL — `unknown field ManifestVersion in struct literal of type events.DispatchedTask`.

- [ ] **Step 3: Extend DispatchedTask in `pkg/events/run_lifecycle.go`**

Replace the `DispatchedTask` struct (lines 4-11) with:
```go
type DispatchedTask struct {
    TaskID          string `json:"task_id"`
    ServiceName     string `json:"service_name"`
    SchemaName      string `json:"schema_name"`
    TableName       string `json:"table_name"`
    NodeType        string `json:"node_type"`
    MaxRetries      int32  `json:"max_retries"`
    ManifestVersion string `json:"manifest_version"`
    ImageTag        string `json:"image_tag"`
}
```

- [ ] **Step 4: Re-run the test, expect pass**

Run: `docker exec orchestrator go test ./pkg/events/ -run TestDispatchedTask_RoundTripWithManifestVersionAndImageTag -v`
Expected: PASS.

- [ ] **Step 5: Extend NodeReadyForExecution in `orchestrator/domain/model.go`**

Replace the `NodeReadyForExecution` struct (lines 109-118) with:
```go
type NodeReadyForExecution struct {
    ScheduleID      string `json:"schedule_id"`
    ScheduleName    string `json:"schedule_name"`
    ServiceName     string `json:"service_name"`
    SchemaName      string `json:"schema_name"`
    TableName       string `json:"table_name"`
    TaskID          string `json:"task_id"`
    JobName         string `json:"job_name"`
    NodeType        string `json:"node_type"`
    ManifestVersion string `json:"manifest_version"`
    ImageTag        string `json:"image_tag"`
}
```

- [ ] **Step 6: Add ImageTag to TopologyNode in `orchestrator/domain/topology/node.go`**

Inside the `TopologyNode` struct (around line 17), add the field next to `ManifestVersion`:
```go
ImageTag string
```

Also add `ImageTag string` next to the matching field at line ~40 (the second occurrence the spec referenced).

- [ ] **Step 7: Add ImageTag to IngestTopologyNodePayload in `orchestrator/domain/command/command.go`**

Inside the payload struct (line 68 area), add:
```go
ImageTag string `json:"image_tag"`
```

- [ ] **Step 8: Add ManifestVersion to TaskTracker in `state/domain/model/model.go`**

Inside the `TaskTracker` struct (around line 169), add the field next to `ServiceName`:
```go
ManifestVersion string `json:"manifest_version" db:"manifest_version"`
```

- [ ] **Step 9: Write the V6 orchestrator migration**

Create `db/migration/orchestrator/V6__init_topology_state.sql`:
```sql
-- Singleton table holding the global monotonic topology_generation counter.
-- The boolean PK + CHECK constraint enforces exactly one row.
CREATE TABLE topology_state (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE,
    topology_generation BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT topology_state_singleton CHECK (id = TRUE)
);

INSERT INTO topology_state (id) VALUES (TRUE) ON CONFLICT DO NOTHING;

COMMENT ON TABLE topology_state IS
    'Singleton row holding the monotonic topology_generation counter. Increments on every accepted manifest.loaded:v1.';
```

- [ ] **Step 10: Write the V13 state migration**

Create `db/migration/state/V13__add_manifest_version_to_task_tracker.sql`:
```sql
ALTER TABLE task_tracker
    ADD COLUMN IF NOT EXISTS manifest_version VARCHAR(50) NOT NULL DEFAULT '';

COMMENT ON COLUMN task_tracker.manifest_version IS
    'The manifest_version snapshotted on the EXECUTES edge at SnapshotGraph time. Pinned for the lifetime of the task.';
```

- [ ] **Step 11: Verify all packages still compile**

Run: `docker exec orchestrator go build ./... && docker exec state go build ./...`
Expected: no output, exit code 0.

- [ ] **Step 12: Run the full pkg/events test suite**

Run: `docker exec orchestrator go test ./pkg/events/ -v`
Expected: all tests pass, including the new round-trip test.

- [ ] **Step 13: Commit**

```bash
git add db/migration/orchestrator/V6__init_topology_state.sql \
        db/migration/state/V13__add_manifest_version_to_task_tracker.sql \
        pkg/events/run_lifecycle.go pkg/events/payloads_test.go \
        orchestrator/domain/model.go orchestrator/domain/topology/node.go \
        orchestrator/domain/command/command.go \
        state/domain/model/model.go
git commit -m "feat(topology-versioning): foundation — migrations + shared event types"
```

---

## Task 2: Local-dev tooling — content-addressed image tags + service_metadata.json

Replaces the `:latest` tag in `scripts/setup.sh`, derives a per-build content-addressed tag, exports `IMAGE_TAG_PER_SERVICE`, wires it through Compose to `dbt-compile-and-load`, and teaches `dbt-compile-and-load` to write `service_metadata.json` to S3 alongside `manifest.json`. Lands before the producer chain so the new wire fields actually have non-empty values when the rest of the pipeline starts consuming them.

**Files:**
- Create: `dbt/dbt_upload/service_metadata.py`
- Create: `dbt/tests/test_service_metadata.py`
- Modify: `dbt/dbt_upload/upload.py`
- Modify: `dbt/dbt_upload/cli.py`
- Modify: `scripts/setup.sh`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Write the failing test for IMAGE_TAG_PER_SERVICE parser**

Create `dbt/tests/test_service_metadata.py`:
```python
import json
import os
from pathlib import Path

import pytest

from dbt_upload.service_metadata import (
    parse_image_tag_env,
    write_service_metadata_json,
    MissingImageTagError,
)


def test_parse_image_tag_env_basic():
    raw = "service-1=abc123,service-2=def456,service-3=ghi789"
    assert parse_image_tag_env(raw) == {
        "service-1": "abc123",
        "service-2": "def456",
        "service-3": "ghi789",
    }


def test_parse_image_tag_env_strips_whitespace():
    raw = " service-1 = abc123 , service-2 = def456 "
    assert parse_image_tag_env(raw) == {
        "service-1": "abc123",
        "service-2": "def456",
    }


def test_parse_image_tag_env_empty_string_returns_empty_map():
    assert parse_image_tag_env("") == {}


def test_parse_image_tag_env_malformed_raises():
    with pytest.raises(ValueError, match="malformed entry"):
        parse_image_tag_env("service-1abc123,service-2=def456")


def test_write_service_metadata_json_creates_file(tmp_path: Path):
    write_service_metadata_json(
        out_dir=tmp_path,
        service_name="service-1",
        manifest_version="v3",
        image_tag="abc123-1714300000",
    )
    written = tmp_path / "service_metadata.json"
    assert written.exists()
    data = json.loads(written.read_text())
    assert data == {
        "manifest_version": "v3",
        "image_tag": "abc123-1714300000",
    }


def test_write_service_metadata_json_raises_on_empty_image_tag(tmp_path: Path):
    with pytest.raises(MissingImageTagError, match="service-2"):
        write_service_metadata_json(
            out_dir=tmp_path,
            service_name="service-2",
            manifest_version="v3",
            image_tag="",
        )
```

- [ ] **Step 2: Run the test, expect import failure**

Run: `docker exec dbt-compile-and-load uv run pytest tests/test_service_metadata.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'dbt_upload.service_metadata'`.

- [ ] **Step 3: Create the service_metadata module**

Create `dbt/dbt_upload/service_metadata.py`:
```python
"""Parse IMAGE_TAG_PER_SERVICE env and write service_metadata.json per service."""
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class MissingImageTagError(ValueError):
    """Raised when a service has no image_tag — refuses silent fallback to 'latest'."""


def parse_image_tag_env(raw: str) -> dict[str, str]:
    """Parse 'svc1=tag1,svc2=tag2' into {'svc1': 'tag1', 'svc2': 'tag2'}.

    Empty string returns {}. Malformed entries (no '=') raise ValueError.
    """
    if not raw or not raw.strip():
        return {}

    out: dict[str, str] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "=" not in entry:
            raise ValueError(f"malformed entry in IMAGE_TAG_PER_SERVICE: {entry!r}")
        svc, tag = entry.split("=", 1)
        out[svc.strip()] = tag.strip()
    return out


def write_service_metadata_json(
    out_dir: Path,
    service_name: str,
    manifest_version: str,
    image_tag: str,
) -> None:
    """Write {out_dir}/service_metadata.json containing {manifest_version, image_tag}.

    Refuses to write an empty image_tag — fail loud rather than poison downstream snapshots.
    """
    if not image_tag:
        raise MissingImageTagError(
            f"image_tag empty for service {service_name!r} — refuse to write service_metadata.json"
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "service_metadata.json"
    target.write_text(json.dumps({
        "manifest_version": manifest_version,
        "image_tag": image_tag,
    }))
    logger.info(
        "Wrote service_metadata.json",
        extra={"service": service_name, "image_tag": image_tag, "manifest_version": manifest_version},
    )
```

- [ ] **Step 4: Re-run the test, expect pass**

Run: `docker exec dbt-compile-and-load uv run pytest tests/test_service_metadata.py -v`
Expected: 5 passed.

- [ ] **Step 5: Wire service_metadata.json into upload.py**

Open `dbt/dbt_upload/upload.py`. After each per-service upload of `manifest.json`, also upload `service_metadata.json`. Add at the top of the file:
```python
import os
from dbt_upload.service_metadata import (
    parse_image_tag_env,
    write_service_metadata_json,
    MissingImageTagError,
)
```

In the per-service upload loop, after the manifest upload completes, add (showing the conceptual diff — adapt to the existing loop's variable names; the variables you need are `service_name`, `manifest_version`, the local temp dir, the S3 key prefix, and the s3 client):
```python
image_tag_map = parse_image_tag_env(os.getenv("IMAGE_TAG_PER_SERVICE", ""))
image_tag = image_tag_map.get(service_name, "")
if not image_tag:
    raise MissingImageTagError(
        f"IMAGE_TAG_PER_SERVICE missing entry for {service_name!r} — set it in setup.sh"
    )

# Write the metadata sidecar locally, then upload alongside the manifest.
local_meta_dir = Path(local_tmp_dir) / service_name
write_service_metadata_json(
    out_dir=local_meta_dir,
    service_name=service_name,
    manifest_version=manifest_version,
    image_tag=image_tag,
)
s3_client.upload_file(
    str(local_meta_dir / "service_metadata.json"),
    bucket,
    f"{s3_key_prefix}/service_metadata.json",
)
```

(If `upload.py` does not currently iterate per service in this shape, restructure it minimally — keep all behavior, just thread `service_name` + `manifest_version` to where they are needed for the metadata write.)

- [ ] **Step 6: Add IMAGE_TAG_PER_SERVICE to docker-compose.yml**

In `docker-compose.yml`, locate the `dbt-compile-and-load` service block (line ~441). In its `environment:` list (line ~455), add:
```yaml
      - IMAGE_TAG_PER_SERVICE=${IMAGE_TAG_PER_SERVICE:-}
```

In the same file, locate the `executor-controller` service block. **Remove** any `IMAGE_TAG` env var (search the file for `IMAGE_TAG=` and delete the line — the executor will refuse to fall back in Task 8). Also remove the `DOCKERHUB_USERNAME` env from `executor-controller` if present, since with content-addressed tagging we are no longer constructing `user/svc:tag`.

- [ ] **Step 7: Update scripts/setup.sh — content-addressed tags**

Open `scripts/setup.sh`. Replace lines 68-71 (the three `docker build ... -t service-N:latest` lines) with:
```bash
# Derive a per-build content-addressed image tag. Commit SHA as the base; the
# epoch suffix lets local rebuilds without a commit still produce a fresh tag
# so kind reloads pick up new layers instead of caching stale ones.
IMAGE_TAG="$(git rev-parse --short HEAD)-$(date +%s)"
echo "Using IMAGE_TAG=${IMAGE_TAG} for dbt service images"

DBT_SERVICES=(service-1 service-2 service-3)
echo "Building dbt service images with content-addressed tag..."
for svc in "${DBT_SERVICES[@]}"; do
    DOCKER_BUILDKIT=1 docker build \
        -f "dbt/services/${svc}/Dockerfile.local" \
        -t "${svc}:${IMAGE_TAG}" \
        "dbt/services/${svc}/"
done

# Export so dbt-compile-and-load (started below) inherits it via compose.
PER_SERVICE=""
for svc in "${DBT_SERVICES[@]}"; do
    [ -n "$PER_SERVICE" ] && PER_SERVICE="${PER_SERVICE},"
    PER_SERVICE="${PER_SERVICE}${svc}=${IMAGE_TAG}"
done
export IMAGE_TAG_PER_SERVICE="$PER_SERVICE"
echo "Exported IMAGE_TAG_PER_SERVICE=${IMAGE_TAG_PER_SERVICE}"
```

- [ ] **Step 8: Update scripts/setup.sh — kind load with new tags**

Replace lines 83-85 (the three `kind load docker-image service-N:latest` lines) with:
```bash
for svc in "${DBT_SERVICES[@]}"; do
    kind load docker-image "${svc}:${IMAGE_TAG}" --name "${CLUSTER_NAME}" &
done
```

- [ ] **Step 9: Smoke-test setup.sh changes**

Run: `bash scripts/setup.sh 2>&1 | tee /tmp/setup.log`
Expected: completes successfully; grep for the new tag — `grep "Exported IMAGE_TAG_PER_SERVICE" /tmp/setup.log` shows the env line; `kubectl get pods -A -o jsonpath='{..image}' | tr ' ' '\n' | grep -E '^service-' | grep -v ':latest' | wc -l` returns the count of running dbt service images (none should have `:latest`).

If the cluster already exists with stale state, run `kind delete cluster --name continuo` first and re-run setup.sh.

- [ ] **Step 10: Verify service_metadata.json landed in S3**

Run:
```bash
docker exec localstack awslocal s3 ls s3://continuo/manifest/ --recursive | grep service_metadata.json
```
Expected: three lines, one per service, each ending in `service_metadata.json`.

Run a content check:
```bash
docker exec localstack awslocal s3 cp s3://continuo/manifest/service-1/service_metadata.json -
```
Expected: JSON with `manifest_version` and `image_tag` (the `image_tag` matches what `setup.sh` exported).

- [ ] **Step 11: Commit**

```bash
git add dbt/dbt_upload/service_metadata.py dbt/dbt_upload/upload.py \
        dbt/dbt_upload/cli.py dbt/tests/test_service_metadata.py \
        scripts/setup.sh docker-compose.yml
git commit -m "feat(tooling): content-addressed image tags + service_metadata.json sidecars"
```

---

## Task 3: manifest-controller — read service_metadata.json and emit per-node image_tag

Teaches the S3 + local manifest sources to look for `service_metadata.json` next to each `manifest.json`, and threads `image_tag` through the parser onto each `ManifestNode`. The published `manifest.loaded:v1` payload then carries `image_tag` per node alongside the existing `manifest_version`.

**Files:**
- Modify: `manifest-controller/domain/model.py`
- Modify: `manifest-controller/adapters/sources/s3.py`
- Modify: `manifest-controller/adapters/sources/local.py`
- Modify: `manifest-controller/service/parser.py`
- Modify: `manifest-controller/service/manifest_handler.py`
- Modify: `manifest-controller/tests/test_parser.py`
- Modify: `manifest-controller/tests/test_sources.py` (or create if missing)
- Modify: `manifest-controller/tests/test_manifest_handler.py`

- [ ] **Step 1: Write the failing test for parser image_tag propagation**

In `manifest-controller/tests/test_parser.py`, append:
```python
def test_parse_manifest_stamps_image_tag_on_every_node(tmp_path):
    manifest = {
        "nodes": {
            "model.svc.users": {
                "resource_type": "model",
                "name": "users",
                "schema": "public",
                "fqn": ["svc_a"],
                "config": {"meta": {"owner": "team-a"}},
                "tags": ["nightly"],
            }
        }
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))

    nodes = parse_manifest(str(path), manifest_version="v3", image_tag="abc123-1714300000")

    assert len(nodes) == 1
    assert nodes[0].image_tag == "abc123-1714300000"
    assert nodes[0].manifest_version == "v3"
```

- [ ] **Step 2: Run the test, expect failure**

Run: `docker exec manifest-controller uv run pytest tests/test_parser.py -k image_tag -v`
Expected: FAIL — either `parse_manifest() got an unexpected keyword argument 'image_tag'` or `'ManifestNode' object has no attribute 'image_tag'`.

- [ ] **Step 3: Add image_tag to ManifestNode in `manifest-controller/domain/model.py`**

In the `ManifestNode` dataclass, add (next to `manifest_version`):
```python
image_tag: str = ""
```

Also add a new dataclass next to `ManifestFile`:
```python
@dataclass
class ServiceMetadata:
    manifest_version: str
    image_tag: str
```

And modify `ManifestFile` to optionally carry the metadata sidecar:
```python
@dataclass
class ManifestFile:
    path: str
    version: str
    image_tag: str = ""  # populated from service_metadata.json sidecar when present
```

- [ ] **Step 4: Update parser to accept and stamp image_tag**

In `manifest-controller/service/parser.py`, change the `parse_manifest` signature (line 24):
```python
def parse_manifest(manifest_path: str, manifest_version: str, image_tag: str = "") -> list[ManifestNode]:
```

In the `ManifestNode(...)` constructor call inside the loop (around line 53), add as the last argument:
```python
            image_tag=image_tag,
```

- [ ] **Step 5: Re-run the parser test, expect pass**

Run: `docker exec manifest-controller uv run pytest tests/test_parser.py -k image_tag -v`
Expected: PASS.

- [ ] **Step 6: Write the failing test for S3 source reading service_metadata.json**

Append to `manifest-controller/tests/test_sources.py`:
```python
def test_s3_source_attaches_image_tag_from_sidecar(s3_client_with_fixtures, monkeypatch):
    """S3Source returns ManifestFile.image_tag pulled from service_metadata.json sidecar."""
    bucket = "continuo"
    env = "manifest"

    # Fixture has manifest/service-1/v3.json and manifest/service-1/service_metadata.json
    s3_client_with_fixtures.put_object(
        Bucket=bucket,
        Key=f"{env}/service-1/service_metadata.json",
        Body=json.dumps({"manifest_version": "v3", "image_tag": "abc123-1714300000"}),
    )

    source = S3Source(bucket=bucket, env=env, s3_client=s3_client_with_fixtures)
    manifests = source.list_manifests()

    by_service = {m.path.rsplit("_", 2)[0].rsplit("/")[-1]: m for m in manifests}
    assert manifests, "S3Source returned no manifests"
    assert any(m.image_tag == "abc123-1714300000" for m in manifests), \
        f"no manifest carried image_tag; got {[(m.path, m.image_tag) for m in manifests]}"
```

(If `test_sources.py` does not have a `s3_client_with_fixtures` fixture or matching imports, copy the pattern from existing tests in that file. The assertion is the load-bearing part.)

- [ ] **Step 7: Run the source test, expect failure**

Run: `docker exec manifest-controller uv run pytest tests/test_sources.py -k image_tag -v`
Expected: FAIL — `image_tag` attribute always empty because S3Source does not yet read the sidecar.

- [ ] **Step 8: Update S3Source to read service_metadata.json**

In `manifest-controller/adapters/sources/s3.py`, modify `list_manifests` so that for each service prefix, after locating the highest-versioned `vN.json`, it also fetches `service_metadata.json` from the same prefix and parses out `image_tag`. Replace the body of the inner loop (lines 36-54) with:
```python
        for service_prefix in sorted(all_by_service):
            keys = all_by_service[service_prefix]
            candidates: list[tuple[int, str]] = []
            for key in keys:
                filename = key.split("/")[-1]
                m = _VERSION_RE.match(filename)
                if m:
                    n = int(m.group(1)[1:])
                    candidates.append((n, key))
            if not candidates:
                logger.warning(
                    "No versioned manifest found for S3 prefix — skipping",
                    extra={"service_prefix": service_prefix},
                )
                continue
            _, key = max(candidates)
            filename = key.split("/")[-1]
            version = parse_version(filename)
            local_path = os.path.join(self._tmpdir.name, key.replace("/", "_"))
            self._s3.download_file(self._bucket, key, local_path)

            # Read service_metadata.json sidecar if present.
            image_tag = ""
            meta_key = f"{service_prefix}/service_metadata.json"
            try:
                meta_obj = self._s3.get_object(Bucket=self._bucket, Key=meta_key)
                meta = json.loads(meta_obj["Body"].read())
                image_tag = meta.get("image_tag", "")
            except self._s3.exceptions.NoSuchKey:
                logger.warning(
                    "service_metadata.json missing for service prefix — image_tag will be empty",
                    extra={"service_prefix": service_prefix},
                )

            result.append(ManifestFile(path=local_path, version=version, image_tag=image_tag))
```

Also add `import json` at the top of the file.

- [ ] **Step 9: Apply the same change to local source**

In `manifest-controller/adapters/sources/local.py`, mirror the S3 change: when listing local manifests, also look for `service_metadata.json` in the same directory. Read it if present and populate `ManifestFile.image_tag`.

- [ ] **Step 10: Update manifest_handler.py to thread image_tag from source to parser to wire**

In `manifest-controller/service/manifest_handler.py`, modify the parse call (line 36):
```python
            all_nodes.extend(parse_manifest(mf.path, mf.version, mf.image_tag))
```

In the node-dict construction (lines 57-74), add to the dict:
```python
                "image_tag": node.image_tag,
```

- [ ] **Step 11: Run the source test, expect pass**

Run: `docker exec manifest-controller uv run pytest tests/test_sources.py -k image_tag -v`
Expected: PASS.

- [ ] **Step 12: Run all manifest-controller tests**

Run: `docker exec manifest-controller uv run pytest -v`
Expected: all green. Pre-existing tests that did not pass `image_tag` to `parse_manifest` still work because of the `image_tag: str = ""` default.

- [ ] **Step 13: Commit**

```bash
git add manifest-controller/
git commit -m "feat(manifest-controller): propagate image_tag from S3 sidecar to manifest.loaded:v1"
```

---

## Task 4: orchestrator — ingest_topology consumes image_tag, increments topology_generation

Wires the new wire field through the orchestrator's ingest path. Adds the `TopologyStateRepository` that wraps the singleton `topology_state` table, increments the counter on each accepted manifest, stamps `image_tag` and `topology_generation` onto Table nodes, and writes the per-service `service_metadata` map onto a `:TopologyRoot` node so SnapshotGraph can read it later.

**Files:**
- Create: `orchestrator/adapters/postgres/topology_state_repository.go`
- Create: `orchestrator/adapters/postgres/topology_state_repository_test.go`
- Modify: `orchestrator/adapters/neo4j/topology_repository.go`
- Modify: `orchestrator/service/command/ingest_topology.go`
- Modify: `orchestrator/service/command/ingest_topology_integration_test.go`

- [ ] **Step 1: Write the failing repository test**

Create `orchestrator/adapters/postgres/topology_state_repository_test.go`:
```go
package postgres_test

import (
    "context"
    "testing"

    "github.com/carolsimone/continuo/orchestrator/adapters/postgres"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestTopologyStateRepository_IncrementGeneration_Monotonic(t *testing.T) {
    db := openOrchestratorTestDB(t)
    defer db.Close()
    truncateTopologyState(t, db)

    repo := postgres.NewTopologyStateRepository(db)
    ctx := context.Background()

    n1, err := repo.IncrementGeneration(ctx)
    require.NoError(t, err)
    assert.Equal(t, int64(1), n1)

    n2, err := repo.IncrementGeneration(ctx)
    require.NoError(t, err)
    assert.Equal(t, int64(2), n2)

    current, err := repo.GetGeneration(ctx)
    require.NoError(t, err)
    assert.Equal(t, int64(2), current)
}
```

(The helpers `openOrchestratorTestDB` and `truncateTopologyState` follow the patterns already used in this directory — copy from any existing repository test in `orchestrator/adapters/postgres/`.)

- [ ] **Step 2: Run the test, expect failure**

Run: `docker exec orchestrator go test ./adapters/postgres/ -run TestTopologyStateRepository_IncrementGeneration_Monotonic -v`
Expected: FAIL — undefined `postgres.NewTopologyStateRepository`.

- [ ] **Step 3: Create the repository**

Create `orchestrator/adapters/postgres/topology_state_repository.go`:
```go
package postgres

import (
    "context"
    "fmt"

    "github.com/jmoiron/sqlx"
)

// TopologyStateRepository owns the singleton topology_state row that tracks
// the monotonic topology_generation counter.
type TopologyStateRepository struct {
    db *sqlx.DB
}

func NewTopologyStateRepository(db *sqlx.DB) *TopologyStateRepository {
    return &TopologyStateRepository{db: db}
}

// IncrementGeneration atomically advances the counter and returns the new value.
// Safe under concurrent calls — Postgres serializes the UPDATE.
func (r *TopologyStateRepository) IncrementGeneration(ctx context.Context) (int64, error) {
    var next int64
    err := r.db.QueryRowxContext(ctx, `
        UPDATE topology_state
        SET topology_generation = topology_generation + 1,
            updated_at = now()
        WHERE id = TRUE
        RETURNING topology_generation
    `).Scan(&next)
    if err != nil {
        return 0, fmt.Errorf("increment topology_generation: %w", err)
    }
    return next, nil
}

// GetGeneration returns the current value without modifying it.
func (r *TopologyStateRepository) GetGeneration(ctx context.Context) (int64, error) {
    var current int64
    err := r.db.QueryRowxContext(ctx, `
        SELECT topology_generation FROM topology_state WHERE id = TRUE
    `).Scan(&current)
    if err != nil {
        return 0, fmt.Errorf("get topology_generation: %w", err)
    }
    return current, nil
}
```

- [ ] **Step 4: Re-run the test, expect pass**

Run: `docker exec orchestrator go test ./adapters/postgres/ -run TestTopologyStateRepository_IncrementGeneration_Monotonic -v`
Expected: PASS.

- [ ] **Step 5: Update topology_repository.go to write image_tag and topology_generation**

In `orchestrator/adapters/neo4j/topology_repository.go`, locate the four MERGE blocks that set `t.manifest_version = $manifest_version` (lines 91, 102, 121, 132). For each block, add right after the `manifest_version` line:
```cypher
              t.image_tag = $image_tag,
              t.topology_generation = $topology_generation,
```

Update the parameter map at line 171 (where `manifest_version` is set) to also pass:
```go
            "image_tag":            node.ImageTag,
            "topology_generation":  topologyGeneration,
```

The `topologyGeneration` value will come from the handler — change the `ApplySnapshot` method signature to accept it:
```go
func (r *TopologyRepository) ApplySnapshot(ctx context.Context, nodes []*topology.TopologyNode, topologyGeneration int64) error {
```

Add a method that persists the per-service `service_metadata` map onto a `:TopologyRoot` singleton node:
```go
// SetServiceMetadata writes the per-service service_metadata map onto the singleton
// :TopologyRoot node so SnapshotGraph can copy it onto Run nodes verbatim.
func (r *TopologyRepository) SetServiceMetadata(
    ctx context.Context,
    serviceMetadata map[string]map[string]string,
    topologyGeneration int64,
) error {
    session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
    defer session.Close(ctx)

    payload, err := json.Marshal(serviceMetadata)
    if err != nil {
        return fmt.Errorf("marshal service_metadata: %w", err)
    }

    _, err = session.Run(ctx, `
        MERGE (root:TopologyRoot {id: 'singleton'})
        SET root.service_metadata = $service_metadata,
            root.topology_generation = $topology_generation,
            root.updated_at = datetime()
    `, map[string]interface{}{
        "service_metadata":      string(payload),
        "topology_generation":   topologyGeneration,
    })
    return err
}
```

(Add `encoding/json` to the imports if not already present.)

- [ ] **Step 6: Update ingest_topology.go to increment generation, build map, call SetServiceMetadata**

In `orchestrator/service/command/ingest_topology.go`:

a. Add a new field on the handler struct for the topology-state repo (alongside whatever repos are there). Update the constructor accordingly.

b. Replace the loop at lines 69-83 with a version that builds `service_metadata` (per-service struct) and stamps `ImageTag` onto each `topology.TopologyNode`:
```go
    scheduleNamesSet := make(map[string]struct{})
    serviceMetadata := make(map[string]map[string]string) // svc -> {manifest_version, image_tag}
    topologyNodes := make([]*topology.TopologyNode, 0, len(cmd.Nodes))

    for _, n := range cmd.Nodes {
        node := toTopologyNode(n)
        topologyNodes = append(topologyNodes, node)

        if n.ScheduleName != "" {
            scheduleNamesSet[n.ScheduleName] = struct{}{}
        }
        if n.ServiceName != "" && n.ManifestVersion != "" {
            serviceMetadata[n.ServiceName] = map[string]string{
                "manifest_version": n.ManifestVersion,
                "image_tag":        n.ImageTag,
            }
        }
    }
```

c. Before calling `ApplySnapshot`, increment the generation:
```go
    topologyGeneration, err := h.topologyStateRepo.IncrementGeneration(ctx)
    if err != nil {
        return fmt.Errorf("increment topology_generation: %w", err)
    }
```

d. Pass the generation to `ApplySnapshot`:
```go
    if err := h.topologyRepo.ApplySnapshot(ctx, topologyNodes, topologyGeneration); err != nil {
        return fmt.Errorf("failed to apply topology snapshot: %w", err)
    }
```

e. After `ApplySnapshot`, call the new `SetServiceMetadata`:
```go
    if err := h.topologyRepo.SetServiceMetadata(ctx, serviceMetadata, topologyGeneration); err != nil {
        return fmt.Errorf("failed to set service_metadata: %w", err)
    }
```

f. Replace the outbox-payload `manifest_versions` field (line 101) with the new shape:
```go
        "service_metadata": serviceMetadata,
```

(Keep the same outer keys `event_id`, `schedule_names`. The downstream consumer in `state` is updated in Task 5.)

g. Update `toTopologyNode` to copy `ImageTag` from payload into the topology node (the field exists from Task 1).

- [ ] **Step 7: Update the integration test**

In `orchestrator/service/command/ingest_topology_integration_test.go`, find the existing assertion that validates `manifest_versions` and replace with assertions on `service_metadata`. Add a new assertion that the singleton `:TopologyRoot` node was created:
```go
// After the handler call
record := neo4jSession.Run(ctx, `MATCH (r:TopologyRoot {id: 'singleton'}) RETURN r.service_metadata AS sm, r.topology_generation AS gen`, nil)
require.True(t, record.Next(ctx))
gen, _ := record.Record().Get("gen")
assert.Equal(t, int64(1), gen.(int64))

sm, _ := record.Record().Get("sm")
var serviceMeta map[string]map[string]string
require.NoError(t, json.Unmarshal([]byte(sm.(string)), &serviceMeta))
assert.Equal(t, "v3", serviceMeta["svc-a"]["manifest_version"])
assert.Equal(t, "abc123-1714300000", serviceMeta["svc-a"]["image_tag"])
```

Adjust the test fixture nodes to include `ImageTag` in the inbound payload.

- [ ] **Step 8: Run integration test, expect pass**

Run: `docker exec orchestrator go test ./service/command/ -run TestIngestTopology -v`
Expected: PASS.

- [ ] **Step 9: Wire the new repo into main.go**

In `orchestrator/main.go`, add construction of `TopologyStateRepository` and pass it to the ingest handler constructor (mirroring how other Postgres repos are wired).

Run: `docker exec orchestrator go build ./...`
Expected: clean exit.

- [ ] **Step 10: Update state's ScheduleCatalogHandler to read the new shape**

`state` consumes `schedules.loaded:v1` (the outbox we modified). Find the consumer (likely `state/service/handlers/`) and update the JSON unmarshal to read `service_metadata` (a `map[string]map[string]string`) instead of `manifest_versions` (a `map[string]string`). Persist the full `service_metadata` map into the renamed column — the rename itself lands in Task 7 (steps 8-12), so this step writes into a struct field whose name will be `ServiceMetadata` after Task 7 lands. Keep the changes here at the unmarshal-and-extract level; defer the column write update to Task 7 if file-touching order is awkward.

- [ ] **Step 11: Commit**

```bash
git add orchestrator/adapters/postgres/topology_state_repository.go \
        orchestrator/adapters/postgres/topology_state_repository_test.go \
        orchestrator/adapters/neo4j/topology_repository.go \
        orchestrator/service/command/ingest_topology.go \
        orchestrator/service/command/ingest_topology_integration_test.go \
        orchestrator/main.go \
        state/service/handlers/
git commit -m "feat(orchestrator): topology_generation counter + image_tag stamping on Table"
```

---

## Task 5: orchestrator — SnapshotGraph stamps generation, service_metadata, and edge image_tag

`SnapshotGraph` is the atomic switch point. It already stamps `manifest_version` onto every EXECUTES edge. This task adds: copy `topology_generation` and `service_metadata` from the singleton `:TopologyRoot` onto the new Run node; copy `image_tag` from each Table onto its EXECUTES edge alongside the existing `manifest_version`.

**Files:**
- Modify: `orchestrator/adapters/neo4j/run_repository.go`
- Modify: `orchestrator/adapters/neo4j/run_repository_test.go`

- [ ] **Step 1: Write the failing test**

In `orchestrator/adapters/neo4j/run_repository_test.go`, add a test (use the existing test setup helpers in this file as a template):
```go
func TestSnapshotGraph_StampsGenerationAndServiceMetadataAndEdgeImageTag(t *testing.T) {
    ctx := context.Background()
    client, cleanup := openTestNeo4j(t)
    defer cleanup()

    // Seed: a Table node with image_tag and manifest_version, plus the :TopologyRoot.
    seedSession := client.NewSession(ctx, neo4j.AccessModeWrite)
    _, err := seedSession.Run(ctx, `
        MERGE (root:TopologyRoot {id: 'singleton'})
        SET root.topology_generation = 7,
            root.service_metadata = '{"svc-a":{"manifest_version":"v3","image_tag":"abc123"}}'

        MERGE (t:Table {schema_name:'public', table_name:'users', service_name:'svc-a', schedule_name:'nightly'})
        SET t.active = true,
            t.manifest_version = 'v3',
            t.image_tag = 'abc123',
            t.topology_generation = 7
    `, nil)
    require.NoError(t, err)
    seedSession.Close(ctx)

    repo := neo4jinfra.NewRunRepository(client, slog.Default())
    runID := uuid.New().String()
    require.NoError(t, repo.SnapshotGraph(ctx, runID, "nightly"))

    // Assert Run node has gen + service_metadata.
    readSession := client.NewSession(ctx, neo4j.AccessModeRead)
    defer readSession.Close(ctx)
    res, err := readSession.Run(ctx, `
        MATCH (r:Run {run_id: $run_id})
        RETURN r.topology_generation AS gen, r.service_metadata AS sm
    `, map[string]interface{}{"run_id": runID})
    require.NoError(t, err)
    require.True(t, res.Next(ctx))
    gen, _ := res.Record().Get("gen")
    assert.Equal(t, int64(7), gen.(int64))
    sm, _ := res.Record().Get("sm")
    assert.Contains(t, sm.(string), `"image_tag":"abc123"`)

    // Assert EXECUTES edge has image_tag stamped.
    res2, err := readSession.Run(ctx, `
        MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(:Table {table_name:'users'})
        RETURN e.image_tag AS image_tag, e.manifest_version AS mv
    `, map[string]interface{}{"run_id": runID})
    require.NoError(t, err)
    require.True(t, res2.Next(ctx))
    edgeTag, _ := res2.Record().Get("image_tag")
    edgeMV, _ := res2.Record().Get("mv")
    assert.Equal(t, "abc123", edgeTag)
    assert.Equal(t, "v3", edgeMV)
}
```

- [ ] **Step 2: Run the test, expect failure**

Run: `docker exec orchestrator go test ./adapters/neo4j/ -run TestSnapshotGraph_StampsGenerationAndServiceMetadataAndEdgeImageTag -v`
Expected: FAIL — `gen` is null and `e.image_tag` is null.

- [ ] **Step 3: Update SnapshotGraph in `orchestrator/adapters/neo4j/run_repository.go`**

Replace the `mergeQuery` (lines 89-103) with a version that:
- Reads `:TopologyRoot` to copy `topology_generation` + `service_metadata` onto Run
- Stamps `e.image_tag` from the Table

```cypher
        MATCH (root:TopologyRoot {id: 'singleton'})
        WITH root
        MERGE (run:Run {run_id: $run_id})
        ON CREATE SET run.schedule_name = $schedule_name,
                      run.created_at = datetime(),
                      run.topology_generation = root.topology_generation,
                      run.service_metadata = root.service_metadata
        WITH run
        UNWIND $assignments AS a
        MATCH (node:Table {schema_name:   a.schema_name,
                           table_name:    a.table_name,
                           service_name:  a.service_name,
                           schedule_name: a.schedule_name})
        MERGE (run)-[e:EXECUTES]->(node)
        ON CREATE SET e.status = 'PENDING',
                      e.manifest_version = COALESCE(node.manifest_version, ''),
                      e.image_tag = COALESCE(node.image_tag, ''),
                      e.task_id = a.task_id
        RETURN count(e) AS edges_created
```

Note: the leading `MATCH (root:TopologyRoot ...)` will fail the whole query if no root exists. That is intentional — running SnapshotGraph before any manifest has been ingested is a programmer error. If there is a legitimate path where SnapshotGraph runs before any manifest (e.g., bootstrap), guard with `OPTIONAL MATCH` and default both fields to defaults; document the choice.

- [ ] **Step 4: Re-run the test, expect pass**

Run: `docker exec orchestrator go test ./adapters/neo4j/ -run TestSnapshotGraph_StampsGenerationAndServiceMetadataAndEdgeImageTag -v`
Expected: PASS.

- [ ] **Step 5: Update init-node Cypher queries to return the new edge fields**

In `orchestrator/adapters/neo4j/run_repository.go`, the three queries `getRootNodesInRun` (line 371), `getUpstreamSeedNodesInRun` (line 398), and `getAllNodesInRun` (line 426) RETURN the per-task fields. Add to each `RETURN` block:
```cypher
            COALESCE(e.manifest_version, "") AS manifest_version,
            COALESCE(e.image_tag, "") AS image_tag,
```

(For `getAllNodesInRun`, both legs of the `UNION` need this addition.)

Update `collectNodes` (find it in the same file) to read the new fields:
```go
        ManifestVersion: safeString(recordValue(record, "manifest_version")),
        ImageTag:        safeString(recordValue(record, "image_tag")),
```

Add `ManifestVersion` and `ImageTag` fields to the `domain.TableNode` struct (find it via `git grep "type TableNode struct"`) — `string` JSON tags optional since these are read-only inside the orchestrator process.

- [ ] **Step 6: Run the existing run_repository tests for regressions**

Run: `docker exec orchestrator go test ./adapters/neo4j/ -v`
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add orchestrator/adapters/neo4j/run_repository.go \
        orchestrator/adapters/neo4j/run_repository_test.go \
        orchestrator/domain/   # for TableNode struct change
git commit -m "feat(orchestrator): SnapshotGraph stamps generation, service_metadata, edge image_tag"
```

---

## Task 6: orchestrator — handlers populate DispatchedTask + NodeReadyForExecution with new fields

Now that the data is on the EXECUTES edge and on the Run node, the producer handlers (`handle_scheduler_started`, `handle_rerun`) need to populate the wire structs.

**Files:**
- Modify: `orchestrator/service/command/handle_scheduler_started.go`
- Modify: `orchestrator/service/command/handle_scheduler_started_test.go`
- Modify: `orchestrator/service/command/handle_rerun.go`
- Modify: `orchestrator/service/command/handle_rerun_test.go`

- [ ] **Step 1: Write the failing test**

In `orchestrator/service/command/handle_scheduler_started_test.go`, locate the existing assertion that walks the outbox entries (around line 117-130). Add:
```go
// Newly required fields on every DispatchedTask
for _, task := range entriesDispatched.AllTasks {
    assert.NotEmpty(t, task.ManifestVersion, "every dispatched task must carry manifest_version from EXECUTES edge")
    assert.NotEmpty(t, task.ImageTag, "every dispatched task must carry image_tag from EXECUTES edge")
}

// Newly required fields on every query.model:v1 payload
for _, qm := range queryModelEntries {
    var n domain.NodeReadyForExecution
    require.NoError(t, json.Unmarshal(qm.Payload, &n))
    assert.NotEmpty(t, n.ManifestVersion)
    assert.NotEmpty(t, n.ImageTag)
}
```

(Update the test fixture so the seeded Tables have non-empty `ManifestVersion` and `ImageTag` on their EXECUTES edges — the easiest way is to call SnapshotGraph against a topology that already has these fields, mirroring Task 5's seed.)

- [ ] **Step 2: Run the test, expect failure**

Run: `docker exec orchestrator go test ./service/command/ -run TestHandleSchedulerStarted -v`
Expected: FAIL — both assertions trip because the handler does not yet read the fields.

- [ ] **Step 3: Update buildRunEntriesDispatchedPayload**

In `orchestrator/service/command/handle_scheduler_started.go`, modify `buildRunEntriesDispatchedPayload` (line 229). Inside the loop, append the new fields:
```go
        allTasks = append(allTasks, pkgevents.DispatchedTask{
            TaskID:          node.TaskID,
            ServiceName:     node.ServiceName,
            SchemaName:      node.SchemaName,
            TableName:       node.TableName,
            NodeType:        node.NodeType,
            ManifestVersion: node.ManifestVersion,
            ImageTag:        node.ImageTag,
        })
```

(`node.ManifestVersion` and `node.ImageTag` are populated by the Cypher changes in Task 5.)

- [ ] **Step 4: Update the dispatch loop for query.model:v1**

In the same file, modify the `evt := domain.NodeReadyForExecution{...}` block (line 129). Append:
```go
            ManifestVersion: node.ManifestVersion,
            ImageTag:        node.ImageTag,
```

- [ ] **Step 5: Apply the same changes in handle_rerun.go**

In `orchestrator/service/command/handle_rerun.go`, locate the matching `NodeReadyForExecution{...}` construction (around line 159) and the corresponding `RunRerunDispatched` payload build. Add `ManifestVersion` and `ImageTag` from the rerun target's edge data.

(If `RunRerunDispatched` does not currently carry per-task fields and the rerun creates a single new task row in `state`, only the `query.model:v1` payload needs the new fields. The state-side rerun handler creates a single task using an existing path — confirm whether it needs `manifest_version` and add to `RunRerunDispatched.TasksToReset` or wherever appropriate.)

- [ ] **Step 6: Re-run the tests, expect pass**

Run: `docker exec orchestrator go test ./service/command/ -v`
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add orchestrator/service/command/handle_scheduler_started.go \
        orchestrator/service/command/handle_scheduler_started_test.go \
        orchestrator/service/command/handle_rerun.go \
        orchestrator/service/command/handle_rerun_test.go
git commit -m "feat(orchestrator): wire manifest_version + image_tag onto run.entries.dispatched and query.model"
```

---

## Task 7: state — persist manifest_version on task_tracker

Adds the column write so the read side (UI, audits) can answer "which manifest produced this task?" without traversing Neo4j.

**Files:**
- Modify: `state/adapters/postgres/task_repository.go`
- Modify: `state/service/handlers/run_entries_dispatched_handler.go`
- Modify: `state/service/handlers/run_entries_dispatched_handler_test.go`
- Modify: `state/adapters/postgres/task_repository_test.go`

- [ ] **Step 1: Write the failing test**

In `state/service/handlers/run_entries_dispatched_handler_test.go`, add a test asserting that when the inbound event carries `manifest_version` per task, the persisted row has it:
```go
func TestRunEntriesDispatchedHandler_PersistsManifestVersionPerTask(t *testing.T) {
    db, cleanup := openStateTestDB(t)
    defer cleanup()
    handler := newRunEntriesDispatchedHandler(t, db)

    scheduleID := uuid.New().String()
    seedSchedulerTracker(t, db, scheduleID)

    payload := events.RunEntriesDispatched{
        ScheduleID:     scheduleID,
        ScheduleName:   "nightly",
        TotalTaskCount: 1,
        AllTasks: []events.DispatchedTask{
            {
                TaskID:          uuid.New().String(),
                ServiceName:     "svc-a",
                SchemaName:      "public",
                TableName:       "users",
                NodeType:        "dbt-model",
                MaxRetries:      3,
                ManifestVersion: "v7",
                ImageTag:        "abc123-1714300000",
            },
        },
    }
    raw, _ := json.Marshal(payload)

    ack, err := handler.Handle(context.Background(), "msg-1", string(raw))
    require.NoError(t, err)
    assert.True(t, ack)

    var persistedManifestVersion string
    require.NoError(t, db.GetContext(context.Background(), &persistedManifestVersion,
        `SELECT manifest_version FROM task_tracker WHERE schedule_id = $1`, scheduleID))
    assert.Equal(t, "v7", persistedManifestVersion)
}
```

- [ ] **Step 2: Run the test, expect failure**

Run: `docker exec state go test ./service/handlers/ -run TestRunEntriesDispatchedHandler_PersistsManifestVersionPerTask -v`
Expected: FAIL — `manifest_version` is empty (default '') because the handler does not read it.

- [ ] **Step 3: Update task_repository.go INSERT**

In `state/adapters/postgres/task_repository.go`, both `Create` and `BulkCreate(Tx)` paths need to include `manifest_version`. Modify the INSERT in `Create` (line 73-79):
```go
        INSERT INTO task_tracker (
            task_id, schedule_id, created_at, service_name, schema_name,
            table_name, job_name, status, retry_count, max_retries, cancelled_at, cancelled_by,
            manifest_version
        ) VALUES (
            :task_id, :schedule_id, :created_at, :service_name, :schema_name,
            :table_name, :job_name, :status, :retry_count, :max_retries, :cancelled_at, :cancelled_by,
            :manifest_version
        )
```

Apply the same column addition to the `BulkCreateTx` INSERT (around line 340).

For each SELECT statement (lines 111, 144, 301, 360, 539), append `manifest_version` to the column list and to the matching `model.TaskTracker` scan.

- [ ] **Step 4: Update the handler to copy ManifestVersion from event**

In `state/service/handlers/run_entries_dispatched_handler.go`, modify the `model.TaskTracker{...}` construction (line 122-132). Add:
```go
                ManifestVersion: t.ManifestVersion,
```

- [ ] **Step 5: Re-run the test, expect pass**

Run: `docker exec state go test ./service/handlers/ -run TestRunEntriesDispatchedHandler_PersistsManifestVersionPerTask -v`
Expected: PASS.

- [ ] **Step 6: Run the full state test suite**

Run: `docker exec state go test ./... -v`
Expected: all green.

- [ ] **Step 7: Commit the task_tracker change**

```bash
git add state/adapters/postgres/task_repository.go \
        state/service/handlers/run_entries_dispatched_handler.go \
        state/service/handlers/run_entries_dispatched_handler_test.go \
        state/adapters/postgres/task_repository_test.go
git commit -m "feat(state): persist manifest_version per task on task_tracker"
```

### 7b. Rename manifest_versions → service_metadata across state's schema and domain

The spec mandates a system-wide rename; the orchestrator already emits `service_metadata` (Task 4). Now the state side moves to match. Migration V14 renames the column and changes its type from `JSONB {svc: ver}` to `JSONB {svc: {manifest_version, image_tag}}`. Go domain models follow.

- [ ] **Step 8: Write the V14 migration**

Create `db/migration/state/V14__rename_manifest_versions_to_service_metadata.sql`:
```sql
-- Direct cutover: pre-production, no rollout shim. The new shape is a JSONB
-- map of service -> {manifest_version, image_tag}; old rows are coerced.

ALTER TABLE schedule_catalog
    RENAME COLUMN manifest_versions TO service_metadata;

ALTER TABLE scheduler_tracker
    RENAME COLUMN manifest_versions TO service_metadata;

-- Coerce existing string values into the new {manifest_version, image_tag} shape.
-- image_tag defaults to '' for legacy rows; new rows from orchestrator carry the real value.
UPDATE schedule_catalog
SET service_metadata = (
    SELECT jsonb_object_agg(key, jsonb_build_object('manifest_version', value, 'image_tag', ''))
    FROM jsonb_each_text(service_metadata)
)
WHERE jsonb_typeof(service_metadata) = 'object'
  AND NOT (service_metadata = '{}'::jsonb)
  AND (
      SELECT bool_or(jsonb_typeof(value) = 'string')
      FROM jsonb_each(service_metadata)
  );

UPDATE scheduler_tracker
SET service_metadata = (
    SELECT jsonb_object_agg(key, jsonb_build_object('manifest_version', value, 'image_tag', ''))
    FROM jsonb_each_text(service_metadata)
)
WHERE jsonb_typeof(service_metadata) = 'object'
  AND NOT (service_metadata = '{}'::jsonb)
  AND (
      SELECT bool_or(jsonb_typeof(value) = 'string')
      FROM jsonb_each(service_metadata)
  );

COMMENT ON COLUMN schedule_catalog.service_metadata IS
    'Per-service manifest_version + image_tag, snapshotted at schedule load time.';
COMMENT ON COLUMN scheduler_tracker.service_metadata IS
    'Per-service manifest_version + image_tag, snapshotted at schedule activation time.';
```

- [ ] **Step 9: Update Go domain models**

In `state/domain/model/model.go`, find `ManifestVersions` (line 141) and `ManifestVersionsRaw` (line 142). Rename and retype:
```go
type ServiceMetadata struct {
    ManifestVersion string `json:"manifest_version"`
    ImageTag        string `json:"image_tag"`
}

// Inside the relevant struct (ScheduleCatalog / SchedulerTracker — wherever the field lives today):
ServiceMetadata     map[string]ServiceMetadata `json:"service_metadata"`
ServiceMetadataRaw  json.RawMessage            `json:"-" db:"service_metadata"`
```

(Search for every reference to `ManifestVersions` in the state service via `git grep ManifestVersions state/` and update each call site.)

- [ ] **Step 10: Update state's repositories and handlers**

For every SELECT/INSERT in `state/adapters/postgres/scheduler_repository.go` (and similar repos) that references `manifest_versions`, rename to `service_metadata`. For any handler that unmarshalled `map[string]string`, change the type to `map[string]model.ServiceMetadata`.

For the `schedules.loaded:v1` consumer (the one updated in Task 4 step 10), confirm it now persists the full `service_metadata` map into the renamed column.

- [ ] **Step 11: Update tests**

Search `state/` for `manifest_versions` and `ManifestVersions` and update test fixtures to use the new shape:
```go
ServiceMetadata: map[string]model.ServiceMetadata{
    "svc-a": {ManifestVersion: "v3", ImageTag: "abc123"},
}
```

Run: `docker exec state go test ./... -v`
Expected: all green.

- [ ] **Step 12: Commit the rename**

```bash
git add db/migration/state/V14__rename_manifest_versions_to_service_metadata.sql \
        state/
git commit -m "refactor(state): rename manifest_versions → service_metadata system-wide"
```

---

## Task 8: executor-controller — JobParams.ImageTag, fail-loud, remove IMAGE_TAG fallback

The terminal consumer change. Reads `image_tag` from `query.model:v1`, refuses to construct a Pod with an empty tag, and removes the global `IMAGE_TAG` env var fallback. Lands after the producer chain (Tasks 4-7) so by the time fail-loud is enabled, the wire actually carries a non-empty tag.

**Files:**
- Modify: `executor-controller/adapters/k8s/client.go`
- Modify: `executor-controller/adapters/k8s/client_test.go`
- Modify: `executor-controller/adapters/redis/consumer.go` (or wherever `query.model:v1` is parsed into `JobParams`)
- Modify: `executor-controller/adapters/redis/consumer_test.go`

- [ ] **Step 1: Write the failing test for buildPodSpec**

In `executor-controller/adapters/k8s/client_test.go`, add:
```go
func TestBuildPodSpec_UsesImageTagFromParams(t *testing.T) {
    params := JobParams{
        JobName:     "test-job",
        ServiceName: "service-1",
        TaskID:      uuid.New().String(),
        SchemaName:  "public",
        TableName:   "users",
        ImageTag:    "abc123-1714300000",
        NodeType:    pkg_model.NodeTypeDbtModel,
    }
    spec, err := buildPodSpec(params)
    require.NoError(t, err)
    assert.Len(t, spec.Containers, 1)
    assert.Equal(t, "service-1:abc123-1714300000", spec.Containers[0].Image)
}

func TestBuildPodSpec_RefusesEmptyImageTag(t *testing.T) {
    params := JobParams{
        JobName:     "test-job",
        ServiceName: "service-1",
        TaskID:      uuid.New().String(),
        SchemaName:  "public",
        TableName:   "users",
        ImageTag:    "",
        NodeType:    pkg_model.NodeTypeDbtModel,
    }
    _, err := buildPodSpec(params)
    require.Error(t, err)
    assert.Contains(t, err.Error(), "image_tag missing")
}
```

(Note `buildPodSpec` currently returns `corev1.PodSpec` not `(corev1.PodSpec, error)` — Step 3 changes the signature.)

- [ ] **Step 2: Run the tests, expect failure**

Run: `docker exec executor-controller go test ./adapters/k8s/ -run 'TestBuildPodSpec_(UsesImageTagFromParams|RefusesEmptyImageTag)' -v`
Expected: FAIL — `JobParams` has no `ImageTag` field; `buildPodSpec` does not return an error.

- [ ] **Step 3: Update JobParams + buildPodSpec in `executor-controller/adapters/k8s/client.go`**

Add to `JobParams` (line 19):
```go
    ImageTag string
```

Replace `buildPodSpec` (lines 167-207) with:
```go
// buildPodSpec constructs the PodSpec for a query executor job. The image is
// resolved from JobParams.ImageTag — there is no fallback to a global env var.
// Empty ImageTag is a programmer error: the orchestrator must populate it from
// the snapshotted EXECUTES edge before emitting query.model:v1.
func buildPodSpec(params JobParams) (corev1.PodSpec, error) {
    if params.ImageTag == "" {
        return corev1.PodSpec{}, fmt.Errorf(
            "image_tag missing from JobParams for service %s — refuse to fall back to 'latest'",
            params.ServiceName,
        )
    }

    image := params.ServiceName + ":" + params.ImageTag

    // Pull policy: if the tag looks content-addressed (e.g., commit SHA),
    // PullIfNotPresent is correct. The previous PullAlways behavior was a
    // workaround for the mutable :latest tag and is no longer needed.
    pullPolicy := corev1.PullIfNotPresent
    if user := os.Getenv("DOCKERHUB_USERNAME"); user != "" {
        image = user + "/" + image
        pullPolicy = corev1.PullAlways
    }

    envVars := []corev1.EnvVar{
        {Name: "TASK_ID", Value: params.TaskID},
        {Name: "SCHEDULE_ID", Value: params.ScheduleID},
        {Name: "SCHEDULE_NAME", Value: params.ScheduleName},
        {Name: "SERVICE_NAME", Value: params.ServiceName},
        {Name: "SCHEMA", Value: params.SchemaName},
        {Name: "TABLE_NAME", Value: params.TableName},
        {Name: "JOB_NAME", Value: params.JobName},
        {Name: "DBT_POSTGRES_HOST", Value: os.Getenv("POSTGRES_HOST")},
        {Name: "DBT_POSTGRES_PORT", Value: os.Getenv("POSTGRES_PORT")},
        {Name: "DBT_POSTGRES_DB", Value: os.Getenv("DBT_POSTGRES_DB")},
        {Name: "DBT_POSTGRES_USER", Value: os.Getenv("POSTGRES_USER")},
        {Name: "DBT_POSTGRES_PASSWORD", Value: os.Getenv("POSTGRES_PASSWORD")},
    }

    return corev1.PodSpec{
        RestartPolicy: corev1.RestartPolicyNever,
        Containers: []corev1.Container{
            {
                Name:            "dbt-job",
                Image:           image,
                ImagePullPolicy: pullPolicy,
                Command:         params.NodeType.Command(params.TableName),
                Env:             envVars,
            },
        },
    }, nil
}
```

Update the caller in `CreateQueryJob` (line 155): replace `Spec: buildPodSpec(params),` with:
```go
    podSpec, err := buildPodSpec(params)
    if err != nil {
        return err
    }
    job.Spec.Template.Spec = podSpec
```

(Restructure the surrounding `Job{}` initializer as needed — the test in Step 1 only asserts on `buildPodSpec`'s output; the caller wiring is mechanical.)

- [ ] **Step 4: Re-run the buildPodSpec tests, expect pass**

Run: `docker exec executor-controller go test ./adapters/k8s/ -run 'TestBuildPodSpec_(UsesImageTagFromParams|RefusesEmptyImageTag)' -v`
Expected: PASS.

- [ ] **Step 5: Update the consumer to thread image_tag from event into JobParams**

In `executor-controller/adapters/redis/consumer.go`, find where `query.model:v1` is unmarshalled into `domain.NodeReadyForExecution` (the existing test `TestConsumer_DropsQueryModelWhenScheduleCancelled` references the path). When constructing `JobParams`, add:
```go
        ImageTag: evt.ImageTag,
```

Add a test in `consumer_test.go`:
```go
func TestConsumer_PassesImageTagToJobParams(t *testing.T) {
    payload := domain.NodeReadyForExecution{
        ScheduleID:   uuid.New().String(),
        ScheduleName: "nightly",
        ServiceName:  "service-1",
        SchemaName:   "public",
        TableName:    "users",
        TaskID:       uuid.New().String(),
        JobName:      "test-job",
        NodeType:     "dbt-model",
        ImageTag:     "abc123-1714300000",
    }
    raw, _ := json.Marshal(payload)
    msg := redis.XMessage{ID: "1", Values: map[string]interface{}{"payload": string(raw)}}

    fakeK8s := newFakeK8sClient()
    c := newConsumerWithK8s(t, fakeK8s)
    require.NoError(t, c.processMessage(context.Background(), msg, "query.model:v1"))

    require.Len(t, fakeK8s.createdJobs, 1)
    job := fakeK8s.createdJobs[0]
    assert.Equal(t, "service-1:abc123-1714300000", job.Spec.Template.Spec.Containers[0].Image)
}
```

(Use the existing fake/mock pattern from `consumer_test.go`. `newFakeK8sClient` and `newConsumerWithK8s` are likely already present — adapt names.)

- [ ] **Step 6: Run all executor-controller tests**

Run: `docker exec executor-controller go test ./... -v`
Expected: all green. Pre-existing tests that did not set `ImageTag` will fail until they are updated; update each test fixture to include `ImageTag: "test-tag"`.

- [ ] **Step 7: Remove IMAGE_TAG from any deployment manifests**

Search for `IMAGE_TAG` in `deploy/`, `executor-controller/config/`, and `docker-compose.yml`:
```bash
grep -rn 'IMAGE_TAG' deploy/ executor-controller/config/ docker-compose.yml
```
Remove every match that sets `IMAGE_TAG=...` for the executor-controller. Leave `IMAGE_TAG_PER_SERVICE` (used by `dbt-compile-and-load`) intact.

- [ ] **Step 8: Commit**

```bash
git add executor-controller/ deploy/ docker-compose.yml
git commit -m "feat(executor-controller): use snapshotted image_tag, fail loud on empty"
```

---

## Task 9: End-to-end regression test — mid-run topology isolation

Validates the full chain: schedule starts on image tag T1; mid-run a new manifest with tag T2 is ingested; the in-flight Run completes on T1; the next Run uses T2. Per `CLAUDE.md`'s edge-case rule, this becomes a permanent regression test.

**Files:**
- Create: `tests/e2e/test_topology_versioning.py` (or `.go`, matching the existing e2e suite's style — read `tests/e2e/README.md` and follow it)

- [ ] **Step 1: Read the e2e harness docs**

Run: `cat tests/e2e/README.md`
Note the test harness, fixtures, and runner. Identify the existing test that most closely resembles "schedule a run, wait for completion, assert on outputs."

- [ ] **Step 2: Write the failing e2e test**

Create the file in the language the existing e2e suite uses. Pseudocode (adapt to the harness):

```
1. setup(): kind cluster up, services running, dbt-compile-and-load has loaded the initial manifest with image_tag=T1.
2. trigger schedule "nightly" → wait for run.entries.dispatched:v1.
3. assert: every task_tracker row for this run has manifest_version=V1.
4. mid-run: rebuild service-1 with a new tag T2, upload service_metadata.json with image_tag=T2 + manifest_version=V2 to S3, trigger update.graph:v1 so manifest-controller re-publishes.
5. assert: orchestrator's topology_state.topology_generation has incremented.
6. wait for the in-flight run to complete.
7. assert: every K8s Job created during the in-flight run used image=service-1:T1 (not T2). Query via kubectl or job audit logs.
8. trigger a second schedule "nightly" → wait for run.entries.dispatched:v1.
9. assert: every task_tracker row for the new run has manifest_version=V2.
10. assert: every K8s Job created uses image=service-1:T2.
```

- [ ] **Step 3: Run the e2e test, expect failure (until services pick up new code)**

Run the e2e per `tests/e2e/README.md` (likely `bash scripts/run-e2e.sh test_topology_versioning` or similar).
Expected initial run: should pass given Tasks 1-8 are landed. If it fails, debug until it passes.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/
git commit -m "test(e2e): regression test for topology versioning lazy generation switch"
```

---

## Task 10: Reconcile docs/arch/* per CLAUDE.md

CLAUDE.md mandates that the architecture pack is updated alongside any service-behavior change. Update the four affected files to reflect the new event fields, the topology_generation lifecycle, and the executor's image-resolution path.

**Files:**
- Modify: `docs/arch/01-topology.md`
- Modify: `docs/arch/02-interaction-matrix.md`
- Modify: `docs/arch/03-sequence-flows.md`
- Modify: `docs/arch/04-service-ownership.md`

- [ ] **Step 1: Read each file and identify outdated sections**

Run: `for f in docs/arch/0[1-4]*.md; do echo "=== $f ==="; cat "$f"; done`

- [ ] **Step 2: Update 01-topology.md**

Add a section describing the `topology_generation` counter, the singleton `:TopologyRoot` Neo4j node, and the new properties (`image_tag`, `topology_generation`) on `Table`, `Run`, and EXECUTES.

- [ ] **Step 3: Update 02-interaction-matrix.md**

Update the row for `manifest.loaded:v1` to note that each node carries `image_tag`. Update `run.entries.dispatched:v1` to note `manifest_version` and `image_tag` per task. Update `query.model:v1` to note the same.

- [ ] **Step 4: Update 03-sequence-flows.md**

Add a sequence flow showing: `manifest.loaded:v1` arrives mid-Run → `topology_generation++` → in-flight Run continues unaffected on its bound generation → next Run picks up new generation at SnapshotGraph time.

- [ ] **Step 5: Update 04-service-ownership.md**

Note the new `topology_state` Postgres table under orchestrator's owned storage. Note the new `task_tracker.manifest_version` column under state's owned storage.

- [ ] **Step 6: Commit**

```bash
git add docs/arch/
git commit -m "docs(arch): reflect topology_generation, image_tag flow, and lazy generation switch"
```

---

## Final validation

- [ ] **All tests across all services pass:**

```bash
docker exec orchestrator go test ./... -v
docker exec state go test ./... -v
docker exec executor-controller go test ./... -v
docker exec manifest-controller uv run pytest -v
docker exec dbt-compile-and-load uv run pytest -v
```

- [ ] **E2E suite passes:** see `tests/e2e/README.md` for the full run command.

- [ ] **No `:latest` tags in the cluster after a fresh setup:**

```bash
bash scripts/setup.sh
kubectl get pods -A -o jsonpath='{..image}' | tr ' ' '\n' | grep -E '^(service-|continuo-)' | grep ':latest' | wc -l
```
Expected: `0`.

- [ ] **Push the branch and open a PR per CLAUDE.md's "merge to main from a PR" rule.**
