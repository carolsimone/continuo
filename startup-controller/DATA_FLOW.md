# Startup Controller — Data Flow Diagram

## Overview

The startup-controller initializes DAG execution by identifying root nodes (tables with no upstream dependencies) and preparing them for execution. It follows an event-driven, CQRS architecture with a transactional outbox pattern for guaranteed delivery.

---

## Level 0 — Context

```
 ┌─────────────────────────────────────────────────────────────┐
 │                     External Systems                        │
 │                                                             │
 │   Scheduler ──► Redis (scheduler.started:v1)                │
 │                                                             │
 │   startup-controller ──► Redis (query.model:v1) ──► Executor│
 └─────────────────────────────────────────────────────────────┘
```

The service sits between the scheduler and the executor. It receives a trigger, resolves the DAG structure, and emits one ready-to-execute event per root node.

---

## Level 1 — Main Data Flow

```
Redis Stream                     startup-controller                    External Systems
(scheduler.started:v1)           ┌────────────────────┐
                                 │                    │
  ┌──────────────────┐           │  1. Consumer       │
  │ {                │──────────►│     reads event    │
  │   runner_id,     │           │                    │
  │   schedule_name  │           │  2. Message Bus    │
  └──────────────────┘           │     routes to      │
                                 │     handler        │
                                 │         │          │
                                 │         ▼          │
                                 │  3. Begin          │◄──── PostgreSQL
                                 │     transaction    │      (continuo_startup)
                                 │         │          │
                                 │         ▼          │
                                 │  4. Mark init      │────► State Service (gRPC :50051)
                                 │     in_progress    │      UpdateSchedulerInitStatus
                                 │         │          │
                                 │         ▼          │
                                 │  5. Query root     │────► Neo4j (:7687)
                                 │     nodes          │◄──── MATCH (t:Table) WHERE NOT
                                 │         │          │           (t)-[:DEPENDS_ON]->()
                                 │         ▼          │
                                 │  6. For each       │────► State Service (gRPC)
                                 │     root node:     │      GetTaskByScheduleAndNode
                                 │     create/update  │      CreateTask / UpdateTaskStatus
                                 │     task           │◄──── returns: task_id, job_name
                                 │         │          │
                                 │         ▼          │
                                 │  7. Write outbox   │────► PostgreSQL
                                 │     entry per node │      INSERT INTO startup_outbox
                                 │     (transactional)│      status = 'pending'
                                 │         │          │
                                 │         ▼          │
                                 │  8. Mark init      │────► State Service (gRPC)
                                 │     completed      │      UpdateSchedulerInitStatus
                                 │         │          │
                                 │         ▼          │
                                 │  9. Commit         │────► PostgreSQL
                                 │     transaction    │      COMMIT
                                 │         │          │
                                 │         ▼          │
                                 │  10. ACK message   │────► Redis
                                 │                    │      XACK scheduler.started:v1
                                 └────────────────────┘

Background (continuous polling every 1s):

  PostgreSQL ──────────────────► Outbox Processor ──────────────────► Redis Stream
  startup_outbox                 ┌──────────────────┐                 (query.model:v1)
  status = 'pending'             │ 1. SELECT pending │
                                 │    entries        │
                                 │    (FOR UPDATE    │
                                 │     SKIP LOCKED)  │
                                 │ 2. XADD to Redis  │──────────────► {
                                 │ 3. Mark processed │                  schedule_id,
                                 │    (or increment  │                  schedule_name,
                                 │     retry_count   │                  service_name,
                                 │     on failure)   │                  schema,
                                 └──────────────────┘                  table_name,
                                                                        task_id,
                                                                        job_name,
                                                                        node_type
                                                                      }
```

---

## Level 2 — Handler Detail

```
InitializeSchedulerHandler.Handle(cmd InitializeScheduler)
│
├── uow.Begin()                          ← PostgreSQL transaction starts
│
├── stateClient.UpdateSchedulerInitStatus(runner_id, "in_progress")
│     └── if already "completed" → return early (idempotency guard)
│
├── neo4jRepo.GetRootNodes(schedule_name)
│     └── Cypher:
│           MATCH (t:Table {schedule_name: $name})
│           WHERE NOT (t)-[:DEPENDS_ON]->()
│           RETURN t.schema, t.table_name, t.service_name,
│                  COALESCE(t.node_type, "") AS node_type
│           (nodes with an invalid/missing node_type are skipped)
│
├── for each rootNode:
│     ├── stateClient.GetTaskByScheduleAndNode(runner_id, service, schema, table)
│     │
│     ├── if task NOT found:
│     │     └── stateClient.CreateTask(newUUID, runner_id, service, schema, table, maxRetries=3)
│     │           └── returns task with task_id + computed job_name
│     │
│     ├── if task found but NOT pending:
│     │     └── stateClient.UpdateTaskStatus(task_id, PENDING)
│     │
│     └── outboxRepo.Create(OutboxEntry{
│               aggregate_type: "scheduler",
│               aggregate_id:   runner_id,
│               event_type:     "node_ready_for_execution",
│               payload:        NodeReadyForExecution{...},
│               stream_name:    "query.model:v1",
│               status:         "pending",
│               max_retries:    3,
│         })
│
├── stateClient.UpdateSchedulerInitStatus(runner_id, "completed")
│
└── uow.Commit()                         ← All-or-nothing
```

---

## Level 2 — Outbox Processor Detail

```
OutboxProcessor (goroutine, polls every 1s)
│
└── loop:
      ├── outboxRepo.GetPending(limit=100)
      │     └── SELECT ... FROM startup_outbox
      │         WHERE status = 'pending' AND retry_count < max_retries
      │         ORDER BY created_at ASC
      │         FOR UPDATE SKIP LOCKED
      │
      └── for each entry:
            ├── redisProducer.Publish(entry.StreamName, entry.Payload)
            │     └── XADD query.model:v1 * {...payload fields...}
            │
            ├── on success:
            │     └── outboxRepo.MarkProcessed(entry.ID)
            │           └── UPDATE ... SET status='processed', processed_at=NOW()
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
├── Connect to PostgreSQL, Neo4j, Redis, gRPC (state service)
│
├── stateClient.ResetInProgressInitializations()
│     └── UPDATE scheduler_init SET status='pending'
│         WHERE status = 'in_progress'
│         (recovers any run interrupted mid-transaction)
│
├── Start HTTP server (:8083) — /health, /ready
│
├── Start OutboxProcessor goroutine
│     └── immediately picks up any 'pending' outbox entries
│         left over from a previous crash-after-commit
│
└── Start Redis consumer
      └── reads un-ACKed messages from startup_controller_consumers group
          (messages not ACKed before crash are replayed automatically)
```

---

## Data Structures

### Input Event — `scheduler.started:v1`

| Field           | Type   | Description                         |
|-----------------|--------|-------------------------------------|
| `runner_id`     | UUID   | Identifies the scheduler run        |
| `schedule_name` | string | Name of the DAG schedule to execute |

### Outbox Entry — `startup_outbox` (PostgreSQL)

| Field            | Type      | Description                              |
|------------------|-----------|------------------------------------------|
| `id`             | UUID      | Primary key                              |
| `aggregate_type` | string    | Always `"scheduler"`                     |
| `aggregate_id`   | UUID      | The `runner_id` (schedule run ID)        |
| `event_type`     | string    | Always `"node_ready_for_execution"`      |
| `payload`        | JSONB     | Serialized `NodeReadyForExecution` event |
| `stream_name`    | string    | Always `"query.model:v1"`               |
| `status`         | string    | `pending` → `processed` or `failed`     |
| `retry_count`    | int       | Number of publish attempts              |
| `max_retries`    | int       | Max attempts before marking failed (3)  |
| `error_message`  | string?   | Set on permanent failure                |
| `created_at`     | timestamp | Entry creation time                     |
| `processed_at`   | timestamp | Time of successful publish              |

### Output Event — `query.model:v1`

| Field           | Type   | Description                                                          |
|-----------------|--------|----------------------------------------------------------------------|
| `schedule_id`   | UUID   | The `runner_id` from the input event                                 |
| `schedule_name` | string | Name of the DAG schedule                                             |
| `service_name`  | string | Data warehouse service name                                          |
| `schema`        | string | Database schema of the root table                                    |
| `table_name`    | string | Name of the root table                                               |
| `task_id`       | UUID   | ID of the task_tracker entry in state service                        |
| `job_name`      | string | K8s DNS-1123 compliant identifier (max 63 chars)                     |
| `node_type`     | string | dbt resource type: `dbt-model`, `dbt-seed`, or `dbt-snapshot`       |

---

## Failure Modes and Recovery

| Failure Point                        | Consequence                          | Recovery Mechanism                                  |
|--------------------------------------|--------------------------------------|-----------------------------------------------------|
| Crash before transaction commits     | No state change anywhere             | Redis message not ACKed → replayed on restart       |
| Crash after commit, before publish   | Outbox entries remain `pending`      | OutboxProcessor publishes them on next poll         |
| Neo4j query failure                  | Transaction rolls back               | Redis message not ACKed → retried                   |
| gRPC call failure                    | Transaction rolls back               | Redis message not ACKed → retried                   |
| Redis publish failure (outbox)       | Retry up to `max_retries` times      | Retry count incremented; eventually marked `failed` |
| Service restart (in_progress status) | Stale `in_progress` initialization   | `ResetInProgressInitializations()` on startup       |

---

## Component Interactions Summary

```
                         ┌──────────────────────────────┐
                         │      startup-controller       │
                         │                               │
Redis ─────────────────► │  Consumer                     │
(scheduler.started:v1)   │     │                         │
                         │     ▼                         │
                         │  Message Bus                  │
                         │     │                         │
                         │     ▼                         │
Neo4j ◄──────────────────│  Handler ──────────────────── │────► State Service (gRPC)
(root node query)        │     │                         │      (task create/update)
                         │     ▼                         │
PostgreSQL ◄─────────────│  Outbox Write (transactional) │
(startup_outbox)         │                               │
                         │  Outbox Processor (bg)        │
PostgreSQL ─────────────►│     │                         │
(pending entries)        │     ▼                         │
                         │  Publish ──────────────────────│────► Redis
                         │                               │      (query.model:v1)
                         │  HTTP :8083                   │
                         │  /health  /ready              │
                         └──────────────────────────────┘
```
