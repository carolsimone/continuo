# Executor Controller — Data Flow Diagram

## Overview

The executor-controller receives ready-to-execute root nodes from the startup-controller, deploys a Kubernetes Job for each one, updates the task status to running, and notifies downstream consumers. It uses the transactional outbox pattern for reliable delivery and idempotent K8s job creation to handle retries safely.

---

## Level 0 — Context

```
 ┌──────────────────────────────────────────────────────────────────────┐
 │                          External Systems                            │
 │                                                                      │
 │  startup-controller ──► Redis (query.model:v1)                       │
 │                                     │                               │
 │                         executor-controller                          │
 │                                     │                               │
 │                         ┌───────────┴──────────────┐               │
 │                         │                          │               │
 │                   K8s (Job created)    Redis (executor.deployed:v1) │
 │                                                    │               │
 │                                        dependency-controller        │
 └──────────────────────────────────────────────────────────────────────┘
```

The service sits between the startup-controller and the dependency-controller. It receives a node ready for execution, deploys a K8s Job for it, and emits a deployed event for downstream processing.

Additionally, a `task.retry:v1` stream feeds retry requests back into the same pipeline when a previously deployed job needs to be re-executed.

---

## Level 1 — Main Data Flow

```
Redis Streams                    executor-controller                    External Systems
(query.model:v1                  ┌──────────────────────┐
 task.retry:v1)                  │                      │
                                 │  1. Consumer         │
  ┌─────────────────────┐        │     reads event      │
  │ {                   │───────►│     (both streams)   │
  │   task_id,          │        │                      │
  │   schedule_id,      │        │  2. Deduplication    │◄──── PostgreSQL
  │   schedule_name,    │        │     check via        │      (processed_events)
  │   service_name,     │        │     outbox_entry_id  │
  │   schema,           │        │                      │
  │   table_name,       │        │  3. Message Bus      │
  │   job_name,         │        │     routes to        │
  │   node_type,        │        │     DeployHandler    │
  │   outbox_entry_id   │        │         │            │
  └─────────────────────┘        │         │            │
                                 │         ▼            │
                                 │  4. Begin            │◄──── PostgreSQL
                                 │     transaction      │      (continuo_executor)
                                 │         │            │
                                 │         ▼            │
                                 │  5. Write outbox     │────► PostgreSQL
                                 │     entry            │      INSERT INTO
                                 │     (transactional)  │      deployment_outbox
                                 │         │            │      status = 'pending'
                                 │         ▼            │
                                 │  6. Commit +         │────► PostgreSQL COMMIT
                                 │     ACK message      │────► Redis XACK
                                 └──────────────────────┘

Background (continuous polling every 5s):

  PostgreSQL ──────────────────► Outbox Processor ──────────────────────────────────
  deployment_outbox              ┌────────────────────┐
  status = 'pending'             │ 1. SELECT pending  │
                                 │    (FOR UPDATE      │
                                 │     SKIP LOCKED)    │
                                 │                    │
                                 │ 2. CreateQueryJob  │────► Kubernetes
                                 │    (idempotent)    │      CREATE Job
                                 │                    │      (skip if exists)
                                 │ 3. UpdateTask      │────► State Service (gRPC :50051)
                                 │    → RUNNING       │      UpdateTask(TASK_STATUS_RUNNING)
                                 │                    │
                                 │ 4. Publish event   │────► Redis (executor.deployed:v1)
                                 │                    │      {task_id, schedule_id,
                                 │ 5. MarkProcessed   │────►  schedule_name, service_name,
                                 │    or IncrRetry    │       schema, table_name, job_name,
                                 │    or MarkFailed   │       node_type, outbox_entry_id}
                                 └────────────────────┘
```

---

## Level 2 — Consumer / Deduplication Detail

```
Consumer.Start(ctx)
│
├── processPendingMessages(ctx, query.model:v1)     ← crash recovery: reclaim un-ACKed
├── processPendingMessages(ctx, task.retry:v1)      ← crash recovery: reclaim un-ACKed
│
└── readAndProcess loop:
      │
      ├── XRead (blocking, 3s timeout) from both streams
      │
      └── for each message:
            │
            ├── parse outbox_entry_id (if present)
            │
            ├── if outbox_entry_id present:
            │     └── SELECT EXISTS FROM processed_events WHERE outbox_entry_id = $1
            │           ├── if EXISTS → XACK (already done) → skip
            │           └── if NOT exists → continue
            │
            ├── parse message into DeployJob command:
            │     {TaskID, ScheduleID, ScheduleName, ServiceName, Schema, TableName, JobName, NodeType}
            │     (unknown node_type → ACK and discard; prevents consumer group backlog)
            │
            ├── messageBus.Handle(ctx, cmd)
            │     └── DeployHandler.Handle(cmd) → writes to deployment_outbox (transactional)
            │
            ├── on success:
            │     ├── INSERT INTO processed_events (outbox_entry_id) ON CONFLICT DO NOTHING
            │     └── XACK <stream> <group> <message_id>
            │
            └── on failure:
                  └── NO ACK → message stays in pending list → retried on next restart
```

---

## Level 2 — DeployHandler Detail

```
DeployHandler.Handle(cmd DeployJob)
│
├── uow.Begin()                          ← PostgreSQL transaction starts
│
├── outboxRepo.Create(DeploymentOutboxEntry{
│       ID:           newUUID,
│       TaskID:       cmd.TaskID,
│       ScheduleID:   cmd.ScheduleID,
│       ScheduleName: cmd.ScheduleName,
│       ServiceName:  cmd.ServiceName,
│       Schema:       cmd.Schema,
│       TableName:    cmd.TableName,
│       JobName:      cmd.JobName,
│       NodeType:     string(cmd.NodeType),
│       Status:       "pending",
│       MaxRetries:   3,
│   })
│
└── uow.Commit()                         ← All-or-nothing
```

---

## Level 2 — Outbox Processor Detail

```
OutboxProcessor.Run(ctx)                 ← goroutine, polls every 5s
│
└── loop:
      │
      ├── outboxRepo.GetPendingBatch(limit=100)
      │     └── SELECT ... FROM deployment_outbox
      │         WHERE status = 'pending' AND retry_count < max_retries
      │         ORDER BY created_at ASC
      │         FOR UPDATE SKIP LOCKED
      │
      └── for each entry:
            │
            ├── k8sClient.CreateQueryJob(JobParams{
            │       JobName, TaskID, ScheduleID, ScheduleName,
            │       ServiceName, Schema, TableName,
            │       Namespace, Image, NodeType,
            │   })
            │     ├── if job already exists → skip (idempotent)
            │     └── if job does not exist → CREATE Job with labels + env vars
            │           Container command derived from NodeType:
            │             dbt-model    → ["dbt", "run",      "--select", tableName]
            │             dbt-seed     → ["dbt", "seed",     "--select", tableName]
            │             dbt-snapshot → ["dbt", "snapshot", "--select", tableName]
            │
            ├── stateClient.UpdateTaskStatus(entry.TaskID, TASK_STATUS_RUNNING)
            │     └── gRPC: StateService.UpdateTask(task_id, RUNNING)
            │
            ├── producer.Publish(JobDeployed{...}.ToMap())
            │     └── XADD executor.deployed:v1 * {...}
            │
            ├── on success:
            │     └── outboxRepo.MarkProcessed(entry.ID)
            │           UPDATE ... SET status='processed', processed_at=NOW()
            │
            └── on failure:
                  ├── if retry_count + 1 < max_retries:
                  │     └── outboxRepo.IncrementRetry(entry.ID)
                  └── if retry_count + 1 >= max_retries:
                        └── outboxRepo.MarkFailed(entry.ID, errorMessage)
```

---

## Startup / Crash Recovery Flow

```
Service starts
│
├── Connect to PostgreSQL, Redis, Kubernetes, gRPC (state service)
│
├── Create consumer groups for query.model:v1 and task.retry:v1
│   (no-op if groups already exist)
│
├── processPendingMessages(query.model:v1)
│     └── XPendingExt → XClaim → re-process un-ACKed messages
│         left over from a previous crash
│
├── processPendingMessages(task.retry:v1)
│     └── same as above for retry stream
│
├── Start HTTP server (:8084) — /health, /ready
│
├── Start OutboxProcessor goroutine (5s poll interval)
│     └── immediately picks up any 'pending' outbox entries
│         left over from a crash-after-outbox-write
│
└── Start Redis consumer (blocking)
```

---

## Data Structures

### Input Event — `query.model:v1` and `task.retry:v1`

| Field             | Type   | Description                                                    |
|-------------------|--------|----------------------------------------------------------------|
| `task_id`         | UUID   | Task tracker ID from state service                             |
| `schedule_id`     | UUID   | The scheduler run ID                                           |
| `schedule_name`   | string | Name of the DAG schedule                                       |
| `service_name`    | string | Data warehouse service name                                    |
| `schema`          | string | Database schema of the node                                    |
| `table_name`      | string | Name of the node's table                                       |
| `job_name`        | string | K8s DNS-1123 compliant identifier (max 63 chars)               |
| `node_type`       | string | dbt resource type: `dbt-model`, `dbt-seed`, or `dbt-snapshot` |
| `outbox_entry_id` | UUID   | Outbox entry ID for deduplication (optional)                   |

### Outbox Entry — `deployment_outbox` (PostgreSQL)

| Field            | Type      | Description                                                    |
|------------------|-----------|----------------------------------------------------------------|
| `id`             | UUID      | Primary key                                                    |
| `task_id`        | UUID      | Task tracker ID                                                |
| `schedule_id`    | UUID      | Scheduler run ID                                               |
| `schedule_name`  | string    | Name of the DAG schedule                                       |
| `service_name`   | string    | Data warehouse service name                                    |
| `schema_name`    | string    | Database schema                                                |
| `table_name`     | string    | Node's table name                                              |
| `job_name`       | string    | K8s job name (max 63 chars)                                    |
| `node_type`      | string    | dbt resource type: `dbt-model`, `dbt-seed`, or `dbt-snapshot` |
| `status`         | string    | `pending` → `processed` or `failed`                           |
| `retry_count`    | int       | Number of deploy attempts                                      |
| `max_retries`    | int       | Max attempts before marking failed (3)                        |
| `error_message`  | string?   | Set on permanent failure                                       |
| `created_at`     | timestamp | Entry creation time                                            |
| `processed_at`   | timestamp | Time of successful processing                                  |

### Deduplication Table — `processed_events` (PostgreSQL)

| Field             | Type | Description                                        |
|-------------------|------|----------------------------------------------------|
| `outbox_entry_id` | UUID | PK; outbox entry ID from the upstream event source |

### Output Event — `executor.deployed:v1`

| Field               | Type   | Description                                                      |
|---------------------|--------|------------------------------------------------------------------|
| `task_id`           | UUID   | Task tracker ID                                                  |
| `schedule_id`       | UUID   | Scheduler run ID                                                 |
| `schedule_name`     | string | Name of the DAG schedule                                         |
| `service_name`      | string | Data warehouse service name                                      |
| `schema`            | string | Database schema                                                  |
| `table_name`        | string | Node's table name                                                |
| `job_name`          | string | K8s job name that was created                                    |
| `node_type`         | string | dbt resource type propagated from input event                    |
| `outbox_entry_id`   | UUID   | Stable dedup key — equals `deployment_outbox.id` for this entry |

### Kubernetes Job

Labels set on every created Job:

| Label          | Value                        |
|----------------|------------------------------|
| `app`          | `query-executor`             |
| `task-id`      | task UUID                    |
| `schedule-id`  | schedule UUID                |
| `schedule`     | schedule name                |
| `table_name`   | table name                   |
| `schema_name`  | schema name                  |
| `service_name` | service name                 |

Container `command` is set from `NodeType`:

| `node_type`      | Container command                              |
|------------------|------------------------------------------------|
| `dbt-model`      | `["dbt", "run",      "--select", table_name]`  |
| `dbt-seed`       | `["dbt", "seed",     "--select", table_name]`  |
| `dbt-snapshot`   | `["dbt", "snapshot", "--select", table_name]`  |

---

## Failure Modes and Recovery

| Failure Point                          | Consequence                           | Recovery Mechanism                                       |
|----------------------------------------|---------------------------------------|----------------------------------------------------------|
| Crash before outbox write commits      | No state change anywhere              | Redis message not ACKed → reclaimed and replayed         |
| Crash after commit, before K8s deploy  | Outbox entry remains `pending`        | OutboxProcessor deploys it on next poll                  |
| K8s job creation failure               | Retry up to `max_retries` times       | RetryCount incremented; eventually marked `failed`       |
| K8s job already exists                 | No-op, continue                       | Idempotent check in `CreateQueryJob`                     |
| gRPC state update failure              | Retry (K8s job already exists safely) | RetryCount incremented; idempotent K8s re-deploy         |
| Redis publish failure                  | Retry                                 | RetryCount incremented                                   |
| Duplicate event (same outbox_entry_id) | Detected before handler runs          | `processed_events` dedup table; message ACKed and skipped|
| Max retries exceeded                   | Entry marked `failed`                 | Logged; requires manual intervention                     |

---

## Component Interactions Summary

```
                          ┌───────────────────────────────────┐
                          │        executor-controller         │
                          │                                   │
Redis ───────────────────►│  Consumer                         │
(query.model:v1)          │  (+ dedup via processed_events)   │
(task.retry:v1)           │     │                             │
                          │     ▼                             │
                          │  Message Bus                      │
                          │     │                             │
                          │     ▼                             │
PostgreSQL ◄──────────────│  DeployHandler                    │
(deployment_outbox)       │  (transactional outbox write)     │
                          │                                   │
                          │  Outbox Processor (bg, 5s poll)   │
                          │     │                             │
PostgreSQL ──────────────►│     ├─ CreateQueryJob ────────────│────► Kubernetes
(pending entries)         │     │                             │      (batch/v1 Job)
                          │     ├─ UpdateTaskStatus ──────────│────► State Service (gRPC)
                          │     │                             │
                          │     └─ Publish ────────────────── │────► Redis
                          │                                   │      (executor.deployed:v1)
                          │  HTTP :8084                       │
                          │  /health  /ready                  │
                          └───────────────────────────────────┘
```
