# state

## Purpose

`state` is the single source of truth for orchestration state:

- scheduler run lifecycle (`scheduler_tracker`)
- task lifecycle (`task_tracker`)
- task execution records (`task_execution`)
- schedule catalog (`schedule_catalog`)

State mutates these records in two ways: via gRPC for UI-facing reads and user-initiated commands, and via Redis event consumers for all internal pipeline writes. No other service writes to these tables directly.

## Owned Storage

| Table | Purpose |
|---|---|
| `scheduler_tracker` | One row per schedule run; owns status and init lifecycle; includes `total_task_count` and `terminal_task_count` columns |
| `task_tracker` | One row per task within a run |
| `task_execution` | One row per execution attempt of a task |
| `schedule_catalog` | Active schedule names derived from manifests; soft-delete by `removed_at` |
| `state_outbox` | Transactional outbox for Redis publication |
| `processed_events` | Deduplication of consumed Redis events |

### New columns on `scheduler_tracker`

| Column | Type | Purpose |
|---|---|---|
| `total_task_count` | `integer` | Set when `run.entries.dispatched:v1` is consumed; total number of tasks in the run |
| `terminal_task_count` | `integer` | Incremented by the finalization state machine each time a task reaches a terminal state |
| `kind` | `character varying(20)` NOT NULL DEFAULT `'cron'` | Run discriminator. CHECK constraint allows: `cron`, `trigger`, `rerun`, `rebase`, `single_node_run`. Set at activation; the rerun handler may flip an existing tracker's kind from `'cron'` to `'rerun'` on `TriggerRerun`. Migration: V15. |
| `source_run_id` | `uuid` NULL | Lineage pointer to a parent run. NULL for `cron`/`trigger`. Populated for `rerun`, `rebase`, and stale-mode `single_node_run`. Not a foreign key — orphans are fine. Migration: V15. |

### New columns on `task_tracker`

| Column | Type | Purpose |
|---|---|---|
| `image_tag` | `character varying(255)` NOT NULL DEFAULT `''` | Per-task audit pair to `manifest_version`. Pinned at task creation by `task_repository.Create` from the upstream event payload; never mutated. Migration: V16. |
| `inherited_from_task_id` | `uuid` NULL | Lineage pointer for rebase-projected rows. `NULL` = a real execution (cron/trigger/rerun/rebased/single-node). Non-NULL = projected inherit from a rebase parent run; the row was never executed in this run, only carried forward from a SUCCEEDED ancestor. **Resolve-to-root semantics:** the value always points to the ROOT executed `task_id` — chain depth is bounded at 1 forever, including rebase-of-rebase (the projector resolves transitively at write time). Not a foreign key — the referenced row may eventually be sweep-deleted; orphan inherits are tolerated by readers. Migration: V17. |

### Run kind values

The `scheduler_tracker.kind` enum (also surfaced as the `kind` field on `:Run` in Neo4j and in the `scheduler.started:v1` outbox payload) discriminates run semantics:

- `cron` — cron-triggered activation; uniform metadata. Today every non-rerun run lands here.
- `trigger` — manual API trigger (reserved; not yet wired in v1).
- `rerun` — re-execute failed node + downstream against the source run's pinned snapshot. **PR2 semantic shift:** `TriggerRerun` now mints a NEW `scheduler_tracker` row on the source's schedule (`kind='rerun'`, `source_run_id=<source>`). The source run row stays at `FAILED`/`CANCELLED` forever — it is now an immutable historical record. Pre-PR2 `kind='cron'` → `kind='rerun'` flip on the existing tracker is gone.
- `rebase` — Feature 2 (PR2 — 2026-05-08): re-execute failed/cancelled tasks + descendants + new arrivals against latest topology; inherit successful tasks with old metadata. New `scheduler_tracker` row on source's schedule (`kind='rebase'`, `source_run_id=<source>`).
- `single_node_run` — Feature 4 (PR1): exactly one task; latest metadata by default, stale via picker.

## Inbound Interfaces

### gRPC — `StateService` (port 50051)

gRPC is now the interface for UI reads and user-initiated commands only. All internal pipeline writes flow through Redis consumers.

#### Schedule activation and control (by `schedule_name` string)

| Method | Catalog check | Active-run check | Outbox write |
|---|---|---|---|
| `ActivateSchedule` | Yes (NotFound if absent) | Yes (skips if active) | Yes — transactional via `ScheduleActivationService` |
| `TriggerSchedule` | Yes (NotFound if absent) | Yes (FailedPrecondition if active) | Yes — transactional via `ScheduleActivationService` |
| `CancelSchedule` | No | Looks up active run | No — direct cancel via `scheduler_tracker` |
| `ListAllSchedules` | Reads catalog | — | No |

> **`ActivateSchedule` vs `TriggerSchedule`**: Both go through `ScheduleActivationService` (transactional outbox) and both require the schedule to exist in `schedule_catalog` (NotFound if absent). The key difference is the concurrent-run guard: `TriggerSchedule` returns `FailedPrecondition` if a run is active; `ActivateSchedule` silently skips (idempotent for the cron loop). `ActivateSchedule` is used by the cron loop and e2e tests; `TriggerSchedule` is the UI-exposed manual trigger.

#### Scheduler run reads

| Method | Description |
|---|---|
| `CreateScheduler` | Create a tracker row directly (used internally and in tests) |
| `GetScheduler` | Fetch a run by UUID |
| `CancelScheduler` | Cancel a run by UUID |
| `GetSchedulerInitStatus` | Read `initialization_status` for a run |

#### Task reads and user commands

| Method | Description |
|---|---|
| `GetTask` | Fetch by UUID |
| `GetTaskByScheduleAndNode` | Fetch by `(schedule_id, service_name, schema_name, table_name)` |
| `DeleteTask` | Delete a task row |
| `ListTasks` | Paginated list with filters |
| `TriggerRerun` | Mint a new `scheduler_tracker` row (`kind='rerun'`, `source_run_id=src`) on the source's schedule + write `trigger.rerun:v1` outbox entry. Source row is left untouched. Returns `run_id` + `schedule_name`. |
| `TriggerSingleNodeRun` | Create a one-task run for a single node; write `trigger.single_node_run:v1` outbox entry |
| `TriggerRebase` | Mint a new `scheduler_tracker` row (`kind='rebase'`, `source_run_id=src`) on the source's schedule + write `trigger.rebase:v1` outbox entry. Source row is left untouched. Returns `run_id` + `schedule_name`. |

##### `TriggerSingleNodeRun`

Request fields: `service_name`, `schema_name`, `table_name`, `metadata_source` (enum: `latest` / `snapshot_of_run`), `source_run_id` (UUID string; required when `metadata_source=snapshot_of_run`).

Response fields: `run_id` (UUID of the new `scheduler_tracker` row), `schedule_name` (synthesised as `"single-node-run-<8 random hex chars>"`).

Error contract:
- `INVALID_ARGUMENT` — missing required fields or `metadata_source` unknown
- `NOT_FOUND` — target node does not exist in the topology
- `FAILED_PRECONDITION` — `source_run_id` references a run that does not exist (stale mode only)

On success: a new `scheduler_tracker` row is inserted with `kind='single_node_run'` and a `trigger.single_node_run:v1` outbox entry is written — both in one transaction.

The synthesised `schedule_name` is not inserted into `schedule_catalog`, so the run is excluded from `ListAllSchedules` by construction (catalog-driven listing).

##### `TriggerRebase`

Request fields: `source_run_id` (UUID string of the failed/cancelled source run).

Response fields: `run_id` (UUID of the newly minted `scheduler_tracker` row), `schedule_name` (the source run's schedule name — rebase always lands on the same schedule).

Error contract:
- `INVALID_ARGUMENT` — `source_run_id` missing or malformed
- `NOT_FOUND` — source run does not exist
- `FAILED_PRECONDITION` — source run is not terminal, or is terminal in a state other than `FAILED` / `CANCELLED`, or already has zero non-SUCCEEDED tasks (nothing to rebase)

On success: a new `scheduler_tracker` row is inserted with `kind='rebase'`, `source_run_id=<src>`, `schedule_name=<src.schedule_name>`, `status=PENDING`, `init_status=in_progress`, and a `trigger.rebase:v1` outbox entry is written — both in one transaction. The source `scheduler_tracker` row is **not** mutated; it remains at its terminal status forever as the historical record.

#### Task execution reads

| Method | Description |
|---|---|
| `GetTaskExecution` | Fetch by UUID |
| `ListTaskExecutions` | List executions, optionally filtered by task |

### HTTP server (port 8082)

| Route | Method | Description |
|---|---|---|
| `/health` | GET | Liveness probe |

The rerun trigger was migrated from HTTP to gRPC (`TriggerRerun`). Port 8082 now serves health checks only.

**TriggerRerun preconditions (enforced atomically):**
1. Source scheduler run must exist
2. Target task must exist within that run
3. Source run must be terminal (no tasks currently RUNNING)
4. Target task must be in FAILED state

On success (PR2 semantics): a NEW `scheduler_tracker` row is inserted with `kind='rerun'`, `source_run_id=<src>`, `schedule_name=<src.schedule_name>`, `status=PENDING`, `init_status=in_progress`. The source row is left untouched — it stays at its terminal `FAILED` status as the immutable historical record. A `trigger.rerun:v1` outbox entry is written in the same transaction. The orchestrator's `HandleRerun` consumer then runs `Snapshot(SourcePinnedDAG)` against the new run, projecting the source's pinned DAG with the target node + non-SUCCEEDED descendants flipped to PENDING (rebased) and the rest carried forward as SUCCEEDED inherits. `task_tracker` rows for the new run are created by `RunEntriesDispatchedHandler` from the orchestrator's `run.entries.dispatched:v1` payload — there is no longer an in-place reset of the source's task rows.

### Redis consumers

| Stream | Consumer group | Handler |
|---|---|---|
| `schedules.loaded:v1` | state service | `ScheduleCatalogHandler` — reconciles `schedule_catalog` |
| `run.entries.dispatched:v1` | state service | `RunEntriesDispatchedHandler` — creates tasks (each carries the orchestrator-stamped `MaxRetries=DefaultTaskMaxRetries`); honours per-task `Status` and `InheritedFromTaskID`; sets `total_task_count`, marks `init_status=completed`. Auto-rollups directly to terminal (`SUCCEEDED`/`FAILED`, `completed_at` set) when every dispatched task is already terminal — otherwise marks `status=running`. |
| `run.entries.dispatch_failed:v1` | state service | `RunEntriesDispatchFailedHandler` — symmetric counterpart of `RunEntriesDispatchedHandler`. Row-locks `scheduler_tracker`, marks status=`failed`, emits `run.finalized:v1`. Idempotent on already-terminal rows. |
| `task.status.updated:v1` | state service | `TaskStatusUpdatedHandler` — updates task status, drives finalization state machine |
| `task.execution.recorded:v1` | state service | `TaskExecutionRecordedHandler` — persists task execution records |

## Outbound Interfaces

### Redis producers (via transactional outbox)

All Redis publishes go through `state_outbox` → background `OutboxProcessor`. The outbox entry and the state mutation are committed in the same transaction, guaranteeing at-least-once delivery.

#### `scheduler.started:v1`

Emitted on: `ActivateSchedule` / `TriggerSchedule` (both paths)

Payload fields:
- `runner_id` — schedule UUID
- `schedule_name`
- `manifest_versions` — map of service → manifest hash read from `schedule_catalog`
- `kind` — run discriminator string (`cron`, `trigger`, `rerun`, etc.); sourced from `scheduler_tracker.kind`
- `source_run_id` — optional UUID string; omitted or empty for `cron`/`trigger` runs; set for `rerun` and `rebase` runs

Effect: `orchestrator` begins task graph initialization, stamping `:Run.kind` and `:Run.source_run_id` in Neo4j.

#### `trigger.rerun:v1`

Emitted on: `TriggerRerun` gRPC call

Payload fields:
- `schedule_id` — UUID of the **newly-minted** `scheduler_tracker` row (PR2; previously the source run's id)
- `schedule_name`
- `scope` — always `"node"`
- `schema_name`
- `table_name`
- `service_name`
- `source_run_id` — UUID of the source run (carries the pinned snapshot)

Effect: `orchestrator.HandleRerun` runs `Snapshot(SourcePinnedDAG)` against the new run, producing a `:Run` node + `:EXECUTES` edges that mirror the source's DAG, with the target + non-SUCCEEDED descendants flipped to PENDING (rebased, will dispatch) and the rest carried forward as SUCCEEDED inherits.

#### `trigger.rebase:v1`

Emitted on: `TriggerRebase` gRPC call

Payload fields:
- `schedule_id` — UUID of the newly-minted `scheduler_tracker` row
- `schedule_name`
- `source_run_id` — UUID of the source (terminal `FAILED`/`CANCELLED`) run

Effect: `orchestrator.HandleRebase` runs `Snapshot(RebasePartition)` against the new run, computing the rebase set ∪ inherit set against the latest topology and projecting the result onto the new `:Run` with split metadata (rebased rows = latest pair; inherited rows = source's pinned pair + root-resolved `inherited_from_task_id`).

#### `trigger.single_node_run:v1`

Emitted on: `TriggerSingleNodeRun` gRPC call

Payload fields:
- `schedule_id` — UUID of the newly created `scheduler_tracker` row
- `schedule_name` — synthesised name (`"single-node-run-<8hex>"`)
- `service_name`, `schema_name`, `table_name` — target node coordinates
- `metadata_source` — `"latest"` or `"snapshot_of_run"`
- `source_run_id` — source run UUID (stale mode only; omitted for latest)

Effect: `orchestrator` snapshots the single node and dispatches it for execution.

#### `run.finalized:v1`

Emitted on: finalization state machine trigger (see below)

Payload fields:
- `schedule_id`
- `schedule_name`
- `status` — `SUCCEEDED` or `FAILED`

Effect: signals downstream consumers that the run is complete.

## Finalization State Machine

When `task.status.updated:v1` is processed and the new status is terminal (SUCCEEDED or FAILED):

1. Increment `terminal_task_count` on `scheduler_tracker`.
2. Check: `terminal_task_count == total_task_count && init_status == 'completed' && status == 'running'`
3. If true: transition scheduler to terminal status, emit `run.finalized:v1` via outbox.

The check is performed atomically within the same transaction as the task status update and the `terminal_task_count` increment.

### Auto-rollup at dispatch time (PR2)

`RunEntriesDispatchedHandler` now honours per-task `Status` in the dispatched payload. When the orchestrator's projection contains tasks that are already terminal at dispatch time (typically `SUCCEEDED` inherits in a rebase or rerun snapshot), `task_tracker` rows are inserted at that terminal status — the executor pipeline never touches them.

If **every** dispatched task is in a terminal state (i.e. there is nothing to execute), the handler short-circuits the normal `status='running'` transition and instead transitions the `scheduler_tracker` directly to a terminal status (`SUCCEEDED` if every projected task is `SUCCEEDED`, `FAILED` otherwise) with `completed_at` set. `init_status='completed'` is still set, and `run.finalized:v1` is emitted via the outbox in the same transaction. This avoids a transient `status=running` flicker on no-op rebases (e.g. user rebased a run with zero non-SUCCEEDED tasks; rejected upstream by `TriggerRebase` precondition checks but the auto-rollup is the last line of defence).

## What It Reads

- All owned tables for every gRPC read/write
- `schedule_catalog.manifest_versions` during activation (passes to outbox payload)
- `schedules.loaded:v1`, `run.entries.dispatched:v1`, `task.status.updated:v1`, `task.execution.recorded:v1` payloads from Redis

## What It Writes

- `scheduler_tracker` — on activation, status transitions, rerun reset, finalization
- `task_tracker` — on task create/update/reset (via events)
- `task_execution` — on execution create/update (via events)
- `schedule_catalog` — on `schedules.loaded:v1` consumption (upsert active, soft-delete absent)
- `state_outbox` — atomically with every activation, rerun, or finalization
- `processed_events` — after processing each consumed Redis event (deduplication)

## Background Loops

| Loop | Description |
|---|---|
| `CronScheduler` | Fires on schedule per `schedules.yaml`; calls `ActivateSchedule` via `ScheduleActivationService` |
| `OutboxProcessor` | Polls `state_outbox` for pending entries; publishes to Redis and marks processed |
| `ScheduleCatalogConsumer` | Reads `schedules.loaded:v1` stream |
| `RunEntriesDispatchedConsumer` | Reads `run.entries.dispatched:v1` stream |
| `TaskStatusUpdatedConsumer` | Reads `task.status.updated:v1` stream |
| `TaskExecutionRecordedConsumer` | Reads `task.execution.recorded:v1` stream |
| gRPC server | Serves `StateService` on port 50051 |
| HTTP server | Serves health endpoint on port 8082 |

### Cron scheduler config

`CronScheduler` reads `schedules.yaml` at startup. Each entry has:
- `name` — must match a schedule name in the catalog for `TriggerSchedule` to succeed
- `cron` — standard 5-field cron expression (seconds optional)
- `description`
- top-level `timezone`

## Activation Paths

```
Cron tick
    → CronScheduler.activateSchedule(name)
    → ActivateSchedule gRPC handler
    → ExistsActive (catalog check — NotFound if absent)
    → ScheduleActivationService.ActivateSchedule
    → PrepareActivation (HasActiveSchedule check — skips if active)
    → GetManifestVersions from catalog
    → tx: CreateTx(scheduler_tracker) + Create(outbox entry)
    → OutboxProcessor publishes scheduler.started:v1

UI manual trigger
    → TriggerSchedule gRPC
    → ExistsActive (catalog check — NotFound if absent)
    → HasActiveSchedule check (FailedPrecondition if active)
    → ScheduleActivationService.ActivateSchedule (same path as above)
```

## Redis Behavior

### Consumes: `schedules.loaded:v1`

Payload (nested under Redis field `payload`):
- `event_id` — UUID for deduplication
- `schedule_names` — list of currently active schedule names
- `manifest_versions` — map of service → manifest hash

Effects (all or nothing — transient errors are retried, not ACK'd):
1. `UpsertAll` — insert or reactivate all names in the list
2. `SoftDeleteAbsent` — set `removed_at` on any active row not in the list
3. Record `event_id` in `processed_events`

> **Warning**: if `schedule_names` is empty, `SoftDeleteAbsent([])` will soft-delete all active catalog entries. The manifest-controller must never publish an empty list.

### Consumes: `run.entries.dispatched:v1`

Published by: `orchestrator`

Payload: `run_id`, `schedule_name`, `manifest_versions`, `dispatched_tasks` — each `DispatchedTask` carries `task_id`, node coordinates, `node_type`, `service_name`, `image_tag`, `manifest_version`, `max_retries`, **`Status`** (defaults to `"pending"`; set to `"succeeded"` for inherited rebase rows), and **`InheritedFromTaskID`** (empty for non-inherited rows; root-resolved `task_id` of the source's executed ancestor for inherited rows).

Effects:
1. Create task rows in `task_tracker` for all dispatched entries — honouring per-task `Status` and stamping `inherited_from_task_id` from `InheritedFromTaskID`
2. Set `total_task_count` on `scheduler_tracker`
3. Set `init_status = completed`. Status transition:
   - If every dispatched task is already terminal: directly to terminal (`SUCCEEDED`/`FAILED`) with `completed_at` set; emit `run.finalized:v1` via outbox.
   - Otherwise: `status = running`.
4. Record event ID in `processed_events`

### Consumes: `task.status.updated:v1`

Published by: `executor-controller` (RUNNING) and `k8s-controller` (SUCCEEDED/FAILED)

Effects:
1. Update task status in `task_tracker`
2. If status is terminal: increment `terminal_task_count`, check finalization condition
3. If finalization condition met: transition scheduler to terminal, emit `run.finalized:v1` via outbox
4. Record event ID in `processed_events`

### Consumes: `task.execution.recorded:v1`

Published by: `k8s-controller`

Effects:
1. Insert row in `task_execution`
2. Record event ID in `processed_events`

## gRPC Callers

| Service | Methods used |
|---|---|
| `ui-service` | `ListAllSchedules`, `GetScheduler`, `ListTasks`, `ListTaskExecutions`, `TriggerRerun`, `TriggerSchedule`, `TriggerSingleNodeRun`, `TriggerRebase` |
| `continuo CLI` | `ListAllSchedules`, `TriggerSchedule` |

State calls no external gRPC services.

## Reliability Patterns

- **Transactional outbox**: every Redis publish is backed by a `state_outbox` row committed in the same transaction as the state mutation — no partial writes
- **Event deduplication**: `event_id` stored in `processed_events`; duplicate messages are ACK'd without re-processing
- **Rerun / rebase atomicity**: new `scheduler_tracker` row INSERT + outbox write are a single SQL transaction; the source row is read for eligibility checks but never mutated
- **Finalization atomicity**: `terminal_task_count` increment + finalization check + `run.finalized:v1` outbox write are a single SQL transaction
- **At-least-once consumers**: all Redis consumers retry on transient errors; idempotency is enforced via `processed_events`

## Source-of-Truth Notes

- Scheduler and task status are owned exclusively by `state` — no other service updates these rows directly
- Internal pipeline writes reach `state` via Redis events (`run.entries.dispatched:v1`, `task.status.updated:v1`, `task.execution.recorded:v1`)
- gRPC write surface is limited to user-initiated commands (`TriggerRerun`, `TriggerRebase`, `TriggerSingleNodeRun`, `CancelScheduler`, etc.) and schedule activation
- The legacy `run.rerun.dispatched:v1` consumer + handler were deleted in PR2 — rerun now mints a new `scheduler_tracker` row + new `:Run` and uses the same `run.entries.dispatched:v1` pipeline as every other run kind
- `schedule_catalog` is the authoritative list of known schedules; `ListAllSchedules` reads from it — a running schedule not yet in the catalog will not appear in the UI
