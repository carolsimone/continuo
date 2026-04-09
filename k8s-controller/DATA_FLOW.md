# K8s Controller — Data Flow Diagram

## Overview

The k8s-controller monitors Kubernetes Jobs deployed by the executor-controller and drives the task lifecycle forward. On every check it queries the K8s API, determines the outcome (running / succeeded / failed), writes outbox entries transactionally, and a background processor then performs the actual state mutations (gRPC) and event publishes (Redis). A second background process resolves outbox entries that exhaust their retry budget.

---

## Level 0 — Context

```
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │                             External Systems                                  │
 │                                                                               │
 │  executor-controller ──► Redis (executor.deployed:v1)                         │
 │                                       │                                      │
 │                           k8s-controller ◄──────────────────────────────┐    │
 │                                       │  (self-loop: delayed re-check)   │    │
 │                                       │                                  │    │
 │                         ┌─────────────┴───────────────────────────────┐ │    │
 │                         │                                             │ │    │
 │                   Kubernetes API              Redis streams:           │ │    │
 │                   (Job/Pod status)             k8s.check:v1 ──────────┘ │    │
 │                   State Service (gRPC)         task.retry:v1            │    │
 │                                                task.failed:v1           │    │
 │                                                update.table:v1          │    │
 └──────────────────────────────────────────────────────────────────────────────┘
```

---

## Level 1 — Main Data Flow

```
Redis Streams                      k8s-controller                      External Systems
(executor.deployed:v1              ┌──────────────────────────┐
 k8s.check:v1)                     │                          │
                                   │  1. DualStreamConsumer   │
  ┌──────────────────────┐         │     reads from both      │
  │ {                    │────────►│     streams              │
  │   task_id,           │         │                          │
  │   schedule_id,       │         │     for k8s.check:v1:    │
  │   schedule_name,     │         │     skip if NOW() <      │
  │   service_name,      │         │     check_after ts       │
  │   schema,            │         │                          │
  │   table_name,        │         │  2. Build CheckJobStatus │
  │   job_name,          │         │     command              │
  │   node_type,         │         │                          │
  │   [check_after],     │         │                          │
  │   [outbox_entry_id]  │         │                          │
  └──────────────────────┘         │  3. MessageBus routes    │
                                   │     to CheckStatusHandler│
                                   │         │                │
                                   │         ▼                │
                                   │  4. Query K8s API        │────► Kubernetes
                                   │     GetJobStatus(        │◄──── K8sPodResult{
                                   │       namespace,         │       Status,
                                   │       jobName)           │       ExitCode,
                                   │                          │       StartedAt,
                                   │                          │       CompletedAt,
                                   │                          │       ErrorMessage}
                                   │         │                │
                                   │         ▼                │
                                   │  5. GetTask from state   │────► State Service (gRPC :50051)
                                   │     service              │◄──── Task{RetryCount, MaxRetries}
                                   │                          │
                                   │  6. Begin TX             │◄──── PostgreSQL
                                   │                          │      (runner DB)
                                   │  7. Dedup claim:         │────► INSERT INTO processed_events
                                   │     if outbox_entry_id   │       ON CONFLICT DO NOTHING
                                   │     set, atomic INSERT;  │◄──── 0 rows = duplicate → skip
                                   │     skip if duplicate    │       1 row  = claimed  → proceed
                                   │                          │
                                   │  8. Write outbox         │────► PostgreSQL
                                   │     entries based on     │      INSERT INTO k8s_status_outbox
                                   │     job status           │      (see decision tree below)
                                   │                          │
                                   │  9. Commit TX            │────► PostgreSQL COMMIT
                                   │     (processed_events    │
                                   │      + outbox entries    │
                                   │      land atomically)    │
                                   │                          │
                                   │ 10. ACK message          │────► Redis XACK
                                   └──────────────────────────┘

Background 1 — OutboxProcessor (polls every 1s, batch 100):

  PostgreSQL ──────────────────► OutboxProcessor ───────────────────────────────────
  k8s_status_outbox              ┌───────────────────────┐
  status = 'pending'             │ 1. GetPendingBatch     │
  retry < max_retries            │    FOR UPDATE          │
                                 │    SKIP LOCKED         │
                                 │                        │
                                 │ 2. if UpdateTaskStatus │────► State Service (gRPC)
                                 │    → UpdateTaskWithRetry      UpdateTaskWithRetry(
                                 │      (taskID, status,  │◄──── taskID, status, retryCount)
                                 │       retryCount)      │
                                 │                        │
                                 │ 3. if CreateExecution  │────► State Service (gRPC)
                                 │    → CreateTaskExecution      CreateTaskExecution(
                                 │      (startedAt,       │◄──── startedAt, completedAt,
                                 │       completedAt,     │       seconds, errorMessage)
                                 │       seconds,         │
                                 │       errorMessage)    │
                                 │                        │
                                 │ 4. if StreamName != "" │────► Redis
                                 │    → PublishToStream   │      XADD <stream> * {...payload}
                                 │                        │
                                 │ 5. MarkProcessed or    │────► PostgreSQL
                                 │    IncrementRetry or   │
                                 │    MarkFailed          │
                                 └───────────────────────┘

Background 2 — StuckEntryResolver (polls every 30s, batch 50):

  PostgreSQL ──────────────────► StuckEntryResolver
  k8s_status_outbox              ┌───────────────────────┐
  status = 'pending'             │ 1. Query stuck entries │
  retry >= max_retries           │    created_at <        │
  created_at < NOW()-60s         │    NOW() - 60s         │
                                 │                        │
                                 │ 2. ForceMarkFailed()   │────► PostgreSQL
                                 │    (up to 5 attempts   │
                                 │     per entry, tracked │
                                 │     in-memory)         │
                                 │                        │
                                 │ 3. On success → WARN   │
                                 │    On 5 failures →     │
                                 │    CRITICAL log with   │
                                 │    manual SQL command  │
                                 └───────────────────────┘
```

---

## Level 2 — CheckStatusHandler Decision Tree

```
CheckStatusHandler.Handle(ctx, cmd CheckJobStatus)
│
├── k8sClient.GetJobStatus(namespace, cmd.JobName)
│     └── returns K8sPodResult{Status, ExitCode, StartedAt, CompletedAt, ErrorMsg}
│
├── stateClient.GetTask(cmd.TaskID)
│     └── returns Task{RetryCount, MaxRetries}
│
├── uow.Begin()
│
├── if cmd.OutboxEntryID != nil:
│     └── processedEventsRepo.TryMarkProcessed(ctx, *cmd.OutboxEntryID)
│           │   ← INSERT INTO processed_events ON CONFLICT DO NOTHING
│           │   ← atomic: check + claim in one operation (race-free even on first INSERT)
│           ├── if duplicate (0 rows affected) → return nil  (consumer ACKs, no re-processing)
│           └── if claimed  (1 row inserted)   → continue
│
├── switch jobStatus:
│
│   ── RUNNING ──────────────────────────────────────────────────────────────────
│     └── outboxRepo.Create({
│               EventType:         "check_delayed",
│               StreamName:        "k8s.check:v1",
│               CheckAfter:        NOW() + K8S_CHECK_DELAY_SECONDS (unix),
│               UpdateTaskStatus:  false,
│               CreateExecution:   false,
│               ...taskContext
│         })
│
│   ── SUCCEEDED ────────────────────────────────────────────────────────────────
│     ├── outboxRepo.Create({          ← entry 1: update task state + execution record
│     │       EventType:         "task_succeeded",
│     │       StreamName:        "",   ← no Redis publish, gRPC only
│     │       UpdateTaskStatus:  true,
│     │       NewTaskStatus:     "SUCCEEDED",
│     │       NewRetryCount:     task.RetryCount,
│     │       CreateExecution:   true,
│     │       ExecutionStartedAt, ExecutionCompletedAt, ExecutionSeconds,
│     │       ...taskContext
│     │   })
│     └── outboxRepo.Create({          ← entry 2: notify dependency-controller
│               EventType:         "node_status_updated",
│               StreamName:        "update.table:v1",
│               UpdateTaskStatus:  false,
│               CreateExecution:   false,
│               ...taskContext
│         })
│
│   ── FAILED (retry_count < max_retries) ──────────────────────────────────────
│     └── outboxRepo.Create({
│               EventType:         "task_retry",
│               StreamName:        "task.retry:v1",
│               UpdateTaskStatus:  true,
│               NewTaskStatus:     "FAILED",
│               NewRetryCount:     task.RetryCount + 1,
│               CreateExecution:   true,
│               ExecutionStartedAt, ExecutionCompletedAt, ExecutionSeconds,
│               ErrorMessage:      truncate(errorMsg, ERROR_MESSAGE_MAX_LENGTH),
│               TaskRetryCount:    task.RetryCount + 1,
│               ...taskContext
│         })
│
│   ── FAILED (retry_count >= max_retries) or UNKNOWN ──────────────────────────
│     ├── outboxRepo.Create({          ← entry 1: permanent failure
│     │       EventType:         "task_failed",
│     │       StreamName:        "task.failed:v1",
│     │       UpdateTaskStatus:  true,
│     │       NewTaskStatus:     "FAILED",
│     │       NewRetryCount:     task.RetryCount + 1,
│     │       CreateExecution:   true,
│     │       ExecutionStartedAt, ExecutionCompletedAt, ExecutionSeconds,
│     │       ErrorMessage:      truncate(errorMsg, ERROR_MESSAGE_MAX_LENGTH),
│     │       TaskRetryCount:    task.RetryCount,
│     │       ...taskContext
│     │   })
│     └── outboxRepo.Create({          ← entry 2: notify dependency-controller
│               EventType:         "node_status_updated",
│               StreamName:        "update.table:v1",
│               UpdateTaskStatus:  false,
│               CreateExecution:   false,
│               ...taskContext
│         })
│
└── uow.Commit()
      └── on failure → Rollback(), Redis message not ACKed → redelivered
```

---

## Level 2 — OutboxProcessor Detail

```
OutboxProcessor.Run(ctx)              ← goroutine, polls every 1s
│
└── loop:
      │
      ├── outboxRepo.GetPendingBatch(limit=100)
      │     └── SELECT ... FROM k8s_status_outbox
      │         WHERE status = 'pending' AND outbox_retry_count < max_retries
      │         ORDER BY created_at ASC
      │         FOR UPDATE SKIP LOCKED
      │
      └── for each entry:
            │
            ├── if entry.UpdateTaskStatus:
            │     └── stateClient.UpdateTaskWithRetry(
            │               taskID, newTaskStatus, newRetryCount)
            │
            ├── if entry.CreateExecution:
            │     └── stateClient.CreateTaskExecution({
            │               TaskID, ScheduleID, ServiceName,
            │               SchemaName, TableName,
            │               StartedAt, CompletedAt,
            │               ExecutionSeconds, ErrorMessage,
            │         })
            │
            ├── if entry.StreamName != "":
            │     └── producer.PublishToStream(entry.StreamName, buildPayload(entry))
            │           EventType mapping:
            │           "task_retry"          → task.retry:v1
            │             payload: {task_id, schedule_id, schedule_name,
            │                       service_name, schema, table_name,
            │                       job_name, retry_count}
            │           "task_failed"         → task.failed:v1
            │             payload: {task_id, schedule_id, schedule_name,
            │                       service_name, schema, table_name,
            │                       job_name, error_message, retry_count}
            │           "check_delayed"       → k8s.check:v1
            │             payload: {task_id, schedule_id, schedule_name,
            │                       service_name, schema, table_name,
            │                       job_name, check_after (unix ts)}
            │           "node_status_updated" → update.table:v1
            │             payload: {task_id, schedule_id, schedule_name,
            │                       service_name, schema, table_name,
            │                       status (SUCCEEDED or FAILED)}
            │
            ├── on success:
            │     └── outboxRepo.MarkProcessed(entry.ID)
            │
            └── on failure:
                  ├── if outbox_retry_count + 1 < max_retries:
                  │     └── outboxRepo.IncrementRetry(entry.ID)
                  └── if outbox_retry_count + 1 >= max_retries:
                        └── outboxRepo.MarkFailed(entry.ID, errorMessage)
```

---

## Level 2 — StuckEntryResolver Detail

```
StuckEntryResolver.Run(ctx)           ← goroutine, polls every 30s
│
└── loop:
      │
      ├── outboxRepo.GetStuckEntries(
      │       limit=50,
      │       stuckThreshold=60s)
      │     └── SELECT ... FROM k8s_status_outbox
      │         WHERE status = 'pending'
      │           AND outbox_retry_count >= max_retries
      │           AND created_at < NOW() - $threshold
      │         FOR UPDATE SKIP LOCKED
      │
      └── for each entry:
            │
            ├── attempts[entry.ID]++   ← in-memory counter per entry
            │
            ├── outboxRepo.ForceMarkFailed(entry.ID, "Resolved by StuckEntryResolver")
            │
            ├── on success:
            │     ├── log WARN "Successfully resolved stuck entry"
            │     └── delete(attempts, entry.ID)
            │
            └── on failure after RESOLVER_MAX_ATTEMPTS (5):
                  └── log CRITICAL {
                          action_required:    "MANUAL_DATABASE_INTERVENTION",
                          entry_id:           entry.ID,
                          recommended_action: "UPDATE k8s_status_outbox
                                               SET status='failed', processed_at=NOW()
                                               WHERE id='<id>'"
                      }
```

---

## Startup / Crash Recovery Flow

```
Service starts
│
├── Connect to Redis, PostgreSQL, gRPC (state service), Kubernetes
│
├── Create consumer group for executor.deployed:v1
│   Create consumer group for k8s.check:v1
│   (XGroupCreateMkStream — no-op if already exists)
│
├── processPendingMessages(executor.deployed:v1)   ← crash recovery
│     └── XPendingExt → XClaim → re-process un-ACKed messages
│
├── processPendingMessages(k8s.check:v1)           ← crash recovery
│     └── XPendingExt → XClaim → re-process un-ACKed messages
│
├── Start OutboxProcessor goroutine (1s poll)
│     └── immediately picks up 'pending' entries left from a crash-after-commit
│
├── Start StuckEntryResolver goroutine (30s poll)
│
├── Start HTTP server (:8085) — /health, /ready
│
└── Start DualStreamConsumer (blocking)
      └── reads from both streams, respects check_after timestamp for k8s.check:v1
```

---

## Delayed Re-check Mechanism

```
Job is still Running when first checked:
│
├── CheckStatusHandler writes outbox entry:
│     EventType:  "check_delayed"
│     StreamName: "k8s.check:v1"
│     CheckAfter: unix timestamp (NOW + 30s)
│
├── OutboxProcessor publishes to k8s.check:v1:
│     { task_id, ..., check_after: <unix_ts> }
│
└── DualStreamConsumer reads message from k8s.check:v1:
      ├── if NOW() < check_after → requeue (NACK, back to pending list)
      └── if NOW() >= check_after → process → CheckStatusHandler runs again
```

This self-scheduling loop continues until the job reaches a terminal state (Succeeded, Failed, or Unknown).

---

## Data Structures

### Input Event — `executor.deployed:v1`

| Field           | Type   | Description                                                              |
|-----------------|--------|--------------------------------------------------------------------------|
| `task_id`         | UUID   | Task tracker ID in state service                                         |
| `schedule_id`     | UUID   | Scheduler run ID                                                         |
| `schedule_name`   | string | Name of the DAG schedule                                                 |
| `service_name`    | string | Data warehouse service name                                              |
| `schema`          | string | Database schema of the node                                              |
| `table_name`      | string | Node's table name                                                        |
| `job_name`        | string | K8s job name to monitor                                                  |
| `node_type`       | string | dbt resource type: `dbt-model`, `dbt-seed`, or `dbt-snapshot`           |
| `outbox_entry_id` | UUID   | Stable dedup key (optional; absent in pre-v2 executor-controller builds) |

### Input Event — `k8s.check:v1` (delayed re-check)

Same fields as above, plus:

| Field        | Type  | Description                                |
|--------------|-------|--------------------------------------------|
| `check_after`| int64 | Unix timestamp — do not process before this|

### Outbox Entry — `k8s_status_outbox` (PostgreSQL)

| Field                  | Type      | Description                                          |
|------------------------|-----------|------------------------------------------------------|
| `id`                   | UUID      | Primary key                                          |
| `event_type`           | string    | `task_retry`, `task_failed`, `check_delayed`, `task_succeeded`, `node_status_updated` |
| `stream_name`          | string    | Target Redis stream (empty = gRPC-only entry)        |
| `task_id`              | UUID      | Task tracker ID                                      |
| `schedule_id`          | UUID      | Scheduler run ID                                     |
| `schedule_name`        | string    | Name of the DAG schedule                             |
| `service_name`         | string    | Data warehouse service name                          |
| `schema_name`          | string    | Database schema                                      |
| `table_name`           | string    | Node's table name                                    |
| `job_name`             | string    | K8s job name (max 63 chars)                          |
| `node_type`            | string    | dbt resource type (propagated from executor event)   |
| `error_message`        | string?   | Truncated to `ERROR_MESSAGE_MAX_LENGTH` (4096)       |
| `task_retry_count`     | int       | Retry count to include in the event payload          |
| `check_after`          | int64?    | Unix timestamp for delayed checks                    |
| `update_task_status`   | bool      | Whether OutboxProcessor should call UpdateTaskWithRetry |
| `new_task_status`      | string?   | Task status to set (e.g. `SUCCEEDED`, `FAILED`)      |
| `new_retry_count`      | int?      | Retry count to set on task                           |
| `create_execution`     | bool      | Whether OutboxProcessor should call CreateTaskExecution |
| `execution_started_at` | timestamp?| Job start time                                       |
| `execution_completed_at`| timestamp?| Job completion time                                  |
| `execution_seconds`    | float64?  | Duration in seconds                                  |
| `status`               | string    | `pending` → `processed` or `failed`                 |
| `outbox_retry_count`   | int       | Number of processing attempts                        |
| `max_retries`          | int       | Max attempts (3)                                     |
| `outbox_error_message` | string?   | Last processing error                                |
| `created_at`           | timestamp | Entry creation time                                  |
| `processed_at`         | timestamp?| Time of successful processing                        |

### Deduplication Table — `processed_events` (PostgreSQL)

| Field               | Type      | Description                                                  |
|---------------------|-----------|--------------------------------------------------------------|
| `outbox_entry_id`   | UUID      | PK; equals the `outbox_entry_id` from the consumed event     |
| `processed_at`      | timestamp | Time the event was first successfully handled                |

### Output Events

**`task.retry:v1`** — consumed by executor-controller to re-deploy the job

| Field           | Type   |
|-----------------|--------|
| `task_id`       | UUID   |
| `schedule_id`   | UUID   |
| `schedule_name` | string |
| `service_name`  | string |
| `schema`        | string |
| `table_name`    | string |
| `job_name`      | string |
| `node_type`     | string |
| `retry_count`   | int    |

**`task.failed:v1`** — consumed by startup-controller to handle permanent schedule failure

| Field           | Type   |
|-----------------|--------|
| `task_id`       | UUID   |
| `schedule_id`   | UUID   |
| `schedule_name` | string |
| `service_name`  | string |
| `schema`        | string |
| `table_name`    | string |
| `job_name`      | string |
| `node_type`     | string |
| `error_message` | string |
| `retry_count`   | int    |

**`k8s.check:v1`** — self-loop, consumed by k8s-controller itself

| Field           | Type   |
|-----------------|--------|
| `task_id`       | UUID   |
| `schedule_id`   | UUID   |
| `schedule_name` | string |
| `service_name`  | string |
| `schema`        | string |
| `table_name`    | string |
| `job_name`      | string |
| `check_after`   | int64  |

**`update.table:v1`** — consumed by dependency-controller to unblock downstream nodes

| Field           | Type   |
|-----------------|--------|
| `task_id`       | UUID   |
| `schedule_id`   | UUID   |
| `schedule_name` | string |
| `service_name`  | string |
| `schema`        | string |
| `table_name`    | string |
| `status`        | string |

---

## Outbox Entry Count per Job Outcome

| Job Outcome                     | Outbox Entries Written | Streams Published         |
|---------------------------------|------------------------|---------------------------|
| Running                         | 1                      | `k8s.check:v1` (self)     |
| Succeeded                       | 2                      | `update.table:v1` (gRPC + stream) |
| Failed, retry available         | 1                      | `task.retry:v1`           |
| Failed, no retries left         | 2                      | `task.failed:v1` + `update.table:v1` |
| Unknown                         | 2                      | `task.failed:v1` + `update.table:v1` |

---

## Failure Modes and Recovery

| Failure Point                             | Consequence                          | Recovery Mechanism                                           |
|-------------------------------------------|--------------------------------------|--------------------------------------------------------------|
| Crash before TX commits                   | No outbox entry written              | Redis message not ACKed → redelivered and reprocessed        |
| Crash after TX commit, before ACK         | Outbox entry written, message not ACKed | Redis redelivers → dedup guard detects duplicate via `processed_events` (if `outbox_entry_id` present), skips re-processing |
| K8s API unreachable                       | TX rolls back                        | Redis message not ACKed → retried                            |
| State service (GetTask) fails             | TX rolls back                        | Redis message not ACKed → retried                            |
| OutboxProcessor: state gRPC fails         | Entry retried up to max_retries      | IncrementRetry; eventually MarkFailed                        |
| OutboxProcessor: Redis publish fails      | Entry retried up to max_retries      | IncrementRetry; eventually MarkFailed                        |
| OutboxProcessor: MarkFailed fails         | Entry stuck at max_retries           | StuckEntryResolver forcibly marks it failed                  |
| StuckEntryResolver exhausts attempts (5)  | Entry permanently stuck              | CRITICAL log with manual SQL intervention command            |

---

## Component Interactions Summary

```
                        ┌──────────────────────────────────────────┐
                        │           k8s-controller                  │
                        │                                          │
Redis ─────────────────►│  DualStreamConsumer                      │
(executor.deployed:v1   │  (crash recovery: XPendingExt+XClaim)    │
 k8s.check:v1)          │     │                                    │
                        │     ▼                                    │
Kubernetes ◄────────────│  CheckStatusHandler                      │────► State Service (gRPC)
(GetJobStatus)          │  (K8s query + state query +              │      GetTask()
                        │   transactional outbox write)            │
                        │     │                                    │
PostgreSQL ◄────────────│  k8s_status_outbox write (atomic TX)     │
(k8s_status_outbox)     │                                          │
                        │  OutboxProcessor (bg, 1s poll)           │
PostgreSQL ────────────►│     │                                    │
(pending entries)       │     ├─ UpdateTaskWithRetry ──────────────│────► State Service (gRPC)
                        │     ├─ CreateTaskExecution ──────────────│────► State Service (gRPC)
                        │     └─ PublishToStream ──────────────────│────► Redis
                        │                                          │      (task.retry:v1
                        │  StuckEntryResolver (bg, 30s poll)       │       task.failed:v1
                        │     └─ ForceMarkFailed ──────────────────│────►  k8s.check:v1
                        │        (or CRITICAL log)                 │       update.table:v1)
                        │                                          │
                        │  HTTP :8085  /health  /ready             │
                        └──────────────────────────────────────────┘
```
