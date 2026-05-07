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

### Run kind values

The `scheduler_tracker.kind` enum (also surfaced as the `kind` field on `:Run` in Neo4j and in the `scheduler.started:v1` outbox payload) discriminates run semantics:

- `cron` — cron-triggered activation; uniform metadata. Today every non-rerun run lands here.
- `trigger` — manual API trigger (reserved; not yet wired in v1).
- `rerun` — re-execute failed node + downstream against the run's pinned snapshot. PR0 flips `kind` from `cron` to `rerun` on the existing tracker via `UpdateTx`.
- `rebase` — Feature 2 (planned PR2): re-execute failed/cancelled tasks + descendants + new arrivals against latest topology; inherit successful tasks with old metadata.
- `single_node_run` — Feature 4 (planned PR1): exactly one task; latest metadata by default, stale via picker.

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
| `TriggerRerun` | Atomically reset scheduler + target task + write `rerun:v1` outbox entry |

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
1. Scheduler run must exist
2. Target task must exist within that run
3. No tasks currently RUNNING in that run
4. Target task must be in FAILED state

On success: scheduler is reset to RUNNING, `initialization_status` reset to `pending`, target task reset to PENDING, `rerun:v1` outbox entry written — all in one transaction.

### Redis consumers

| Stream | Consumer group | Handler |
|---|---|---|
| `schedules.loaded:v1` | state service | `ScheduleCatalogHandler` — reconciles `schedule_catalog` |
| `run.entries.dispatched:v1` | state service | `RunEntriesDispatchedHandler` — creates tasks, sets `total_task_count`, marks `init_status=completed`, `status=running` |
| `run.rerun.dispatched:v1` | state service | `RunRerunDispatchedHandler` — resets tasks for rerun |
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

#### `rerun:v1`

Emitted on: `TriggerRerun` gRPC call

Payload fields:
- `schedule_id`
- `schedule_name`
- `scope` — always `"node"`
- `schema_name`
- `table_name`
- `service_name`

Effect: `orchestrator` re-initializes the single failed node.

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

## What It Reads

- All owned tables for every gRPC read/write
- `schedule_catalog.manifest_versions` during activation (passes to outbox payload)
- `schedules.loaded:v1`, `run.entries.dispatched:v1`, `run.rerun.dispatched:v1`, `task.status.updated:v1`, `task.execution.recorded:v1` payloads from Redis

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
| `RunRerunDispatchedConsumer` | Reads `run.rerun.dispatched:v1` stream |
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

Effects:
1. Create task rows in `task_tracker` for all dispatched entries
2. Set `total_task_count` on `scheduler_tracker`
3. Set `init_status = completed` and `status = running` on `scheduler_tracker`
4. Record event ID in `processed_events`

### Consumes: `run.rerun.dispatched:v1`

Published by: `orchestrator`

Effects:
1. Reset target task(s) to PENDING in `task_tracker`
2. Record event ID in `processed_events`

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
| `ui-service` | `ListAllSchedules`, `GetScheduler`, `ListTasks`, `ListTaskExecutions`, `TriggerRerun`, `TriggerSchedule` |
| `continuo CLI` | `ListAllSchedules`, `TriggerSchedule` |

State calls no external gRPC services.

## Reliability Patterns

- **Transactional outbox**: every Redis publish is backed by a `state_outbox` row committed in the same transaction as the state mutation — no partial writes
- **Event deduplication**: `event_id` stored in `processed_events`; duplicate messages are ACK'd without re-processing
- **Rerun atomicity**: scheduler reset + task reset + outbox write are a single SQL transaction
- **Finalization atomicity**: `terminal_task_count` increment + finalization check + `run.finalized:v1` outbox write are a single SQL transaction
- **At-least-once consumers**: all Redis consumers retry on transient errors; idempotency is enforced via `processed_events`

## Source-of-Truth Notes

- Scheduler and task status are owned exclusively by `state` — no other service updates these rows directly
- Internal pipeline writes reach `state` via Redis events (`run.entries.dispatched:v1`, `run.rerun.dispatched:v1`, `task.status.updated:v1`, `task.execution.recorded:v1`)
- gRPC write surface is limited to user-initiated commands (`TriggerRerun`, `CancelScheduler`, etc.) and schedule activation
- `schedule_catalog` is the authoritative list of known schedules; `ListAllSchedules` reads from it — a running schedule not yet in the catalog will not appear in the UI
