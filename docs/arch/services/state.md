# state

## Purpose

`state` is the single source of truth for orchestration state:

- scheduler run lifecycle (`scheduler_tracker`)
- task lifecycle (`task_tracker`)
- task execution records (`task_execution`)
- schedule catalog (`schedule_catalog`)

All other services mutate these records exclusively through the state gRPC API.
No other service owns or writes to these tables.

## Owned Storage

| Table | Purpose |
|---|---|
| `scheduler_tracker` | One row per schedule run; owns status and init lifecycle |
| `task_tracker` | One row per task within a run |
| `task_execution` | One row per execution attempt of a task |
| `schedule_catalog` | Active schedule names derived from manifests; soft-delete by `removed_at` |
| `state_outbox` | Transactional outbox for Redis publication |
| `processed_events` | Deduplication of consumed `schedules.loaded:v1` events |

## Inbound Interfaces

### gRPC — `StateService` (port 50051)

#### Scheduler run management (by `schedule_id` UUID)

| Method | Description |
|---|---|
| `CreateScheduler` | Create a tracker row directly (used internally and in tests) |
| `GetScheduler` | Fetch a run by UUID |
| `UpdateScheduler` | Update status, timestamps, heartbeat |
| `CancelScheduler` | Cancel a run by UUID |
| `UpdateSchedulerInitStatus` | Set `initialization_status`; auto-transitions to RUNNING when set to `completed` |
| `GetSchedulerInitStatus` | Read `initialization_status` for a run (used by `dependency-controller` to guard premature finalization) |
| `ResetInProgressInitializations` | On startup, reset stale `in_progress` init statuses to `pending` |

#### Schedule activation and control (by `schedule_name` string)

| Method | Catalog check | Active-run check | Outbox write |
|---|---|---|---|
| `ActivateSchedule` | Yes (NotFound if absent) | Yes (skips if active) | Yes — transactional via `ScheduleActivationService` |
| `TriggerSchedule` | Yes (NotFound if absent) | Yes (FailedPrecondition if active) | Yes — transactional via `ScheduleActivationService` |
| `CancelSchedule` | No | Looks up active run | No — direct cancel via `scheduler_tracker` |
| `ListAllSchedules` | Reads catalog | — | No |

> **`ActivateSchedule` vs `TriggerSchedule`**: Both go through `ScheduleActivationService` (transactional outbox) and both require the schedule to exist in `schedule_catalog` (NotFound if absent). The key difference is the concurrent-run guard: `TriggerSchedule` returns `FailedPrecondition` if a run is active; `ActivateSchedule` silently skips (idempotent for the cron loop). `ActivateSchedule` is used by the cron loop and e2e tests; `TriggerSchedule` is the UI-exposed manual trigger.

#### Task management

| Method | Description |
|---|---|
| `CreateTask` | Create a task row |
| `GetTask` | Fetch by UUID |
| `GetTaskByScheduleAndNode` | Fetch by `(schedule_id, service_name, schema, table_name)` |
| `UpdateTask` | Update status, retry count |
| `DeleteTask` | Delete a task row |
| `ListTasks` | Paginated list with filters |
| `ResetTask` | Reset a task to PENDING for rerun |
| `TriggerRerun` | Atomically reset scheduler + target task + write command.rerun:v1 outbox entry |

#### Task execution management

| Method | Description |
|---|---|
| `CreateTaskExecution` | Record a new execution attempt |
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
| `schedules.loaded:v1` | state service | `ScheduleCatalogHandler` |

## Outbound Interfaces

### Redis producers (via transactional outbox)

All Redis publishes go through `state_outbox` → background `OutboxProcessor`. The outbox entry and the state mutation are committed in the same transaction, guaranteeing at-least-once delivery.

#### `scheduler.started:v1`

Emitted on: `ActivateSchedule` / `TriggerSchedule` (both paths)

Payload fields:
- `runner_id` — schedule UUID
- `schedule_name`
- `manifest_versions` — map of service → manifest hash read from `schedule_catalog`

Effect: `startup-controller` begins task graph initialization.

#### `rerun:v1`

Emitted on: `TriggerRerun` gRPC call

Payload fields:
- `schedule_id`
- `schedule_name`
- `scope` — always `"node"`
- `schema`
- `table_name`
- `service_name`

Effect: `startup-controller` re-initializes the single failed node.

## What It Reads

- All owned tables for every gRPC read/write
- `schedule_catalog.manifest_versions` during activation (passes to outbox payload)
- `schedules.loaded:v1` payloads from Redis

## What It Writes

- `scheduler_tracker` — on activation, status transitions, rerun reset
- `task_tracker` — on task create/update/reset
- `task_execution` — on execution create/update
- `schedule_catalog` — on `schedules.loaded:v1` consumption (upsert active, soft-delete absent)
- `state_outbox` — atomically with every activation or rerun
- `processed_events` — after processing each `schedules.loaded:v1` (deduplication)

## Background Loops

| Loop | Description |
|---|---|
| `CronScheduler` | Fires on schedule per `schedules.yaml`; calls `ActivateSchedule` via `ScheduleActivationService` |
| `OutboxProcessor` | Polls `state_outbox` for pending entries; publishes to Redis and marks processed |
| `ScheduleCatalogConsumer` | Reads `schedules.loaded:v1` stream; calls `ScheduleCatalogHandler.Handle` |
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

## gRPC Callers

| Service | Methods used |
|---|---|
| `startup-controller` | `UpdateSchedulerInitStatus`, `CreateTask`, `UpdateTask`, `GetTask`, `ListTasks`, `GetSchedulerInitStatus`, `ResetTask` |
| `dependency-controller` | `GetTask`, `UpdateTask`, `GetSchedulerInitStatus`, `ListTasks` |
| `executor-controller` | `GetTask`, `UpdateTask`, `CreateTaskExecution`, `GetTaskExecution` |
| `k8s-controller` | `GetTask`, `UpdateTask`, `ListTaskExecutions` |
| `ui-service` | `ListAllSchedules`, `GetScheduler`, `ListTasks`, `ListTaskExecutions`, `TriggerRerun` |

State calls no external gRPC services.

## Reliability Patterns

- **Transactional outbox**: every Redis publish is backed by a `state_outbox` row committed in the same transaction as the state mutation — no partial writes
- **`schedules.loaded:v1` deduplication**: `event_id` stored in `processed_events`; duplicate messages are ACK'd without re-processing
- **Rerun atomicity**: scheduler reset + task reset + outbox write are a single SQL transaction
- **Initialization recovery**: `ResetInProgressInitializations` is called at startup to recover any runs left in `in_progress` init state from a prior crash

## Source-of-Truth Notes

- Scheduler and task status are owned exclusively by `state` — no other service updates these rows directly
- `graph` holds only an execution projection (for dependency evaluation and historical run views); it is not the authority on run status
- `schedule_catalog` is the authoritative list of known schedules; `ListAllSchedules` reads from it — a running schedule not yet in the catalog will not appear in the UI
