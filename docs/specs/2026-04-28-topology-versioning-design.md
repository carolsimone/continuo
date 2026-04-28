# Topology Versioning — Run Isolation via Lazy Generation Switch

**Date:** 2026-04-28
**Scope:** Orchestrator topology lifecycle, Run snapshotting, executor image resolution.
**Services touched:** `orchestrator`, `state`, `executor-controller`, `manifest-controller`.
**Tooling touched:** `scripts/setup.sh`, `dbt/dbt_upload` (the `dbt-compile-and-load` compose service), `docker-compose.yml`.

## 1. Recommendation

**Adopt Strategy B — Run isolation via a lazy generation switch.** A Run is bound permanently to the topology snapshot it started with; a new `manifest.loaded:v1` event takes effect at the next Run, never the in-flight one.

A Run is an event-sourced aggregate, and its EXECUTES edges already pin `task_id` and `manifest_version` at SnapshotGraph time. Mutating an in-flight Run's topology violates the aggregate's immutable-history invariant, breaks event-replay determinism, and makes audit reproduction impossible. The natural drain is the Run lifecycle itself; a new manifest is a new epoch that becomes visible at the next SnapshotGraph call. No global signal, no mid-run reconciliation.

## 2. Why pause/resume signals are the wrong primitive

A `pause:v1` / `resume:v1` event-pair is tempting because it sounds like a clean coordination point. It is not.

- **Global coordination barrier.** Pause demands every consumer ack before resume. One stuck consumer — `executor-controller` blocked on a K8s API call, `state` lagging on its outbox — freezes the entire scheduling plane. This centralizes liveness, the opposite of what CQRS is for.
- **In-flight message ambiguity.** A `query.model:v1` already in Redis at pause time has no defined disposition. Process it (the world is paused), drop it (data loss), or buffer it (now you have a side queue with its own consistency problem). Each consumer would resolve this differently — that's a bug factory.
- **Partial-failure window.** Between "orchestrator emits pause" and "executor-controller observes pause," the system is in a half-paused state where the orchestrator believes nothing should execute and the executor is still creating Jobs. Pause adds a distributed-consensus problem to a system that explicitly chose eventual consistency.
- **Coupling vs. CQRS.** Pause requires every consumer to know about a global control plane. Lazy generation switch lets each consumer make local decisions on its own input — which is the property CQRS bought us in the first place.

## 3. The recommended pattern: lazy generation switch (generational isolation)

### 3.1 Mental model

The Run's snapshot is its *constitution* — immutable for the Run's lifetime. A new manifest is a new *epoch* that applies to the next Run instance. The "drain" is the natural Run lifecycle; the "switch" is atomic at SnapshotGraph time.

This is epoch-based scheduling with fencing tokens (Kleppmann, *Designing Data-Intensive Applications* §9), and process-manager versioning (Hohpe & Woolf, *Enterprise Integration Patterns*). Flink, Kafka Streams, and the broader event-sourcing community converge on it because it eliminates the coordination problem rather than solving it.

### 3.2 State additions

- **New monotonic global counter `topology_generation`.** Persisted in Postgres in the orchestrator's state. Opaque integer. Starts at 0. Increments under outbox/dedup guard on each accepted `manifest.loaded:v1`.
- **New `Table` node property `topology_generation: int`.** Set during `ingest_topology` to the value at ingest time.
- **New `Run` node property `topology_generation: int`.** Stamped at SnapshotGraph time — the gen this Run is bound to.
- **Existing EXECUTES edge gains `image_tag: string`.** Snapshotted at edge creation time alongside the existing `manifest_version`.
- **New Run node property `service_metadata: JSON`.** Per-service map of the form `{ svc: { manifest_version: str, image_tag: str } }`. Snapshotted atomically at SnapshotGraph time.

`topology_generation` is purely a label for auditability — "Run X was bound to topology gen N" is a single integer that answers compliance questions without re-deriving them from edge properties. The actual gating mechanism (what makes Run isolation work) is the existing `active=true` filter combined with EXECUTES-edge references holding nodes alive past topology mutations.

### 3.3 Algorithm — `manifest.loaded:v1` arrival

1. Begin transaction; increment `topology_generation` to N+1 in Postgres under outbox/dedup guard.
2. `ingest_topology` runs as today — deactivate missing nodes, MERGE present nodes — additionally stamping `topology_generation = N+1` on every Table touched and writing the per-service `image_tag` into the topology's per-service metadata.
3. **No `pause:v1` is emitted. No mutation of any existing Run node or EXECUTES edge.**
4. In-flight Runs stay bound to generation N. Every property the executor needs (`manifest_version`, `image_tag`, `task_id`) was copied to the EXECUTES edge or the Run node at SnapshotGraph time, so topology mutations are opaque to them.
5. Concurrent manifest events while a Run is in flight: each bumps gen further (N+1, N+2, …). The next Run picks up `MAX(topology_generation)` at its SnapshotGraph time. Multiple manifest arrivals collapse into one switch.

### 3.4 Algorithm — Run finalization

No special action. The "switch" is the next SnapshotGraph call: it reads `active=true` Tables, which are already at the latest gen. The "pending topology" exists only as a conceptual label between manifest arrival and run finalization; there is no separate state-machine entry to reconcile.

### 3.5 Algorithm — SnapshotGraph (orchestrator/adapters/neo4j/run_repository.go:24–121)

Extends current implementation with:

- Read the current `topology_generation` from Postgres at the start of SnapshotGraph; stamp it onto the new Run node.
- Read the per-service `service_metadata` map (manifest_version + image_tag per service) from topology state; stamp it onto the new Run node as a JSON property.
- For each EXECUTES edge created, stamp `image_tag = service_metadata[node.service_name].image_tag` onto the edge alongside the existing `manifest_version`.

### 3.6 Why explicit `topology_generation` if `active=true` already isolates

The existing code already provides *implicit* generational isolation: `topology_repository.go:217` `deleteInactiveOrphans` only deletes inactive nodes that are not referenced by any Run's EXECUTES edge, so an in-flight Run holds its snapshot alive past topology removal. The explicit counter adds nothing to correctness; it adds a single integer for compliance and audit.

## 4. Compliance and the container-immutability gap

### 4.1 Compliance fix — `manifest_version` propagation to the read side

Each EXECUTES edge already carries `manifest_version` (`run_repository.go:100`). It does not yet flow to the read side, so UI/audit queries must traverse Neo4j to recover it. We propagate it onto `task_tracker`.

**State migration V13** (`db/migration/state/V13__add_manifest_version_to_task_tracker.sql`):
```sql
ALTER TABLE task_tracker
    ADD COLUMN IF NOT EXISTS manifest_version VARCHAR(50) NOT NULL DEFAULT '';
```

**Event extension — `query.model:v1` (`NodeReadyForExecution`).** Add two additive optional string fields: `manifest_version` and `image_tag`. Old payloads still parse; consumers that don't care for them ignore them.

**Orchestrator emit-side changes:**
- `handle_scheduler_started.go:115–157` and `handle_rerun.go:126–180`: when constructing `NodeReadyForExecution`, read `e.manifest_version` from the EXECUTES edge into the payload, and read `image_tag` from the Run's `service_metadata` map keyed by `service_name`.

**State consume-side changes:**
- When `state` persists a row into `task_tracker` (existing task creation path), include `manifest_version` from the event payload. Read side (UI, audit gRPC) returns it without touching Neo4j.

### 4.2 Image-tag fix — closing the container-immutability gap

The topology snapshot is immutable, but `executor-controller/adapters/k8s/client.go:172–176` resolves the image from a single global `IMAGE_TAG` env var shared across services with `latest` as the fallback. A dbt node always runs the latest pushed container regardless of what was snapshotted. This is the open correctness gap.

**Publishing — two paths, one contract.** The contract is: every emitted `manifest.loaded:v1` carries a `service_metadata: {svc: {manifest_version, image_tag}}` map where `image_tag` is content-addressed (commit SHA or semver, never `latest`). Both publish paths satisfy this contract by writing `service_metadata.json` into S3 alongside the per-service `manifest.json`; `manifest-controller` reads both files and merges them into the event payload at emit time.

*Production path (CI):* CI builds each service image with `dockerhub/<svc>:<commit-sha>`, then runs the same `dbt_upload` flow used locally — but `dbt-compile-and-load` reads the tag from `IMAGE_TAG_PER_SERVICE` env (set by CI) and writes `service_metadata.json` to S3. No new production-only code path.

*Local-dev path (`scripts/setup.sh` + `dbt-compile-and-load`):*
- `scripts/setup.sh` derives a per-build content-addressed tag — `IMAGE_TAG="$(git rev-parse --short HEAD)-$(date +%s)"` (the timestamp suffix lets local rebuilds without a commit still produce a fresh tag). Replace lines 69–71:
  ```bash
  DOCKER_BUILDKIT=1 docker build -f dbt/services/service-1/Dockerfile.local -t service-1:latest dbt/services/service-1/
  DOCKER_BUILDKIT=1 docker build -f dbt/services/service-2/Dockerfile.local -t service-2:latest dbt/services/service-2/
  DOCKER_BUILDKIT=1 docker build -f dbt/services/service-3/Dockerfile.local -t service-3:latest dbt/services/service-3/
  ```
  with content-addressed tagging only:
  ```bash
  IMAGE_TAG="$(git rev-parse --short HEAD)-$(date +%s)"
  for svc in service-1 service-2 service-3; do
      DOCKER_BUILDKIT=1 docker build -f dbt/services/${svc}/Dockerfile.local -t ${svc}:${IMAGE_TAG} dbt/services/${svc}/
  done
  export IMAGE_TAG_PER_SERVICE="service-1=${IMAGE_TAG},service-2=${IMAGE_TAG},service-3=${IMAGE_TAG}"
  ```
  The matching `kind load docker-image` calls (lines 83–85) update to use `${IMAGE_TAG}` instead of `latest`.
- `docker compose up -d dbt-compile-and-load` inherits `IMAGE_TAG_PER_SERVICE` from the host shell (compose service definition adds the env var to its `environment:` block).
- `dbt-compile-and-load` (`dbt/dbt_upload/upload.py`): for each service uploaded, parse `IMAGE_TAG_PER_SERVICE`, look up the service's tag, and write `s3://<bucket>/<env>/<service>/service_metadata.json` containing `{manifest_version, image_tag}`. The existing `dbt_upload load` flow needs only this one extra write per service.
- `manifest-controller` reads `manifest.json` and `service_metadata.json` from S3, merges the per-service entries into the `service_metadata` map, and emits `manifest.loaded:v1` with the new shape.

**`manifest.loaded:v1` schema change.** The existing `manifest_versions: {svc: ver}` map is replaced with `service_metadata: {svc: {manifest_version: str, image_tag: str}}`. Direct cutover — system is pre-production, no compatibility shim needed.

**Rename propagates everywhere `manifest_versions` exists today.** This is a single coordinated cutover, not a per-component migration:
- `schedule_catalog.manifest_versions` JSONB → `schedule_catalog.service_metadata` JSONB (state migration).
- `scheduler_tracker.manifest_versions` JSONB → `scheduler_tracker.service_metadata` JSONB (state migration).
- `ScheduleEvent.ManifestVersions` and `SchedulerTracker.ManifestVersions` Go fields in `state/domain/model/model.go:141` rename to `ServiceMetadata` with the new map type.
- Per-service-string `manifest_versions` properties on `:Schedule` / `:Service` Neo4j nodes (if present in `ingest_topology.go`) update to the new map shape.
- `task_tracker.manifest_version` (added in §4.1) keeps its scalar shape — it's the per-task pinned string from the EXECUTES edge, not the schedule-level map.

**Topology ingest (orchestrator `service/command/ingest_topology.go`).** Stamps `image_tag` per service onto the Table node, and writes the per-service `service_metadata` map to the topology root (or a dedicated `:Service` node — implementation choice deferred to the plan stage).

**SnapshotGraph (orchestrator `adapters/neo4j/run_repository.go`).** Copies `service_metadata` onto the new Run node as a single JSON property — atomic, immutable, ride-along to the next Run lookup. Copies `image_tag` onto every EXECUTES edge alongside `manifest_version`.

**Executor-controller (`adapters/k8s/client.go:164–207`).**
- Add `ImageTag string` to `JobParams`.
- Replace:
  ```go
  tag := os.Getenv("IMAGE_TAG")
  if tag == "" { tag = "latest" }
  ```
  with:
  ```go
  if params.ImageTag == "" {
      return fmt.Errorf("image_tag missing from query.model:v1 payload — refusing to fall back to 'latest'")
  }
  tag := params.ImageTag
  ```
- Remove the `IMAGE_TAG` env var from the executor-controller deployment manifest. Failing loudly on missing tag surfaces misconfiguration immediately rather than silently running stale code.

**Pull policy.** Stays as-is (`PullAlways` for remote, `PullIfNotPresent` for local). With content-addressed tags, `PullIfNotPresent` is now correct everywhere — revisit in a follow-up.

### 4.3 Constraint compliance

| Constraint | Honored |
|---|---|
| No global pause/barrier primitive | Yes |
| No mutation of in-flight Run snapshot | Yes — all new fields land on new Run/edge writes only |
| New event fields are additive and versioned | Yes — `manifest_version` and `image_tag` are additive on `query.model:v1`; `manifest.loaded:v1` payload shape changes as a direct cutover (pre-production) |
| Executor-controller no longer relies on global `IMAGE_TAG` | Yes — env var removed; tag rides on the event |

## 5. Out of scope (explicit)

- Multi-version coexistence of the same logical Table node in Neo4j (e.g., gen-N and gen-N+1 nodes side-by-side). Not needed: single mutable Table + edge-time snapshotting + `active` flag delivers Run isolation.
- A separate "released" / "pending" generation gate. Not needed: SnapshotGraph naturally reads the latest gen; in-flight Runs are isolated by their bound gen.
- Generation scope per service rather than global. Considered and rejected: per-service generations create a 2D versioning matrix with no compliance benefit over a single monotonic counter combined with per-service `manifest_version` strings.
- Run cancellation and topology rollback. Out of scope for this change.

## 6. Test strategy

- **Unit:** `ingest_topology` increments `topology_generation`; concurrent ingest under outbox dedup yields exactly one increment per accepted event.
- **Integration (orchestrator):** in-flight Run remains executable on its bound gen N after a `manifest.loaded:v1` arrives that bumps to N+1; subsequent SnapshotGraph stamps N+1 on the new Run.
- **Integration (executor-controller):** `query.model:v1` with empty `image_tag` is rejected with a clear error; valid `image_tag` produces a Job with the correct image.
- **End-to-end (`tests/e2e`):** schedule starts on image tag T1; mid-run a new manifest with image tag T2 is loaded; in-flight Run completes on T1; next Run uses T2. Per CLAUDE.md, this edge case earns a permanent regression test.
- **Local-dev smoke (`scripts/setup.sh`):** after a fresh setup, assert that no service container in the kind cluster runs the `latest` tag — `kubectl get pods -o jsonpath='{..image}' | grep -c ':latest'` returns 0. Catches regressions where a future contributor reintroduces `latest` in either `setup.sh` or compose.
- **`dbt-compile-and-load` unit:** given `IMAGE_TAG_PER_SERVICE=service-1=abc123,service-2=def456`, `upload.py` writes `service_metadata.json` per service with the correct `image_tag`. Empty / malformed env yields a clear error rather than silent fallback.
