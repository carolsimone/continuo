# Dependency Controller — Data Flow Diagram

## Overview

The dependency-controller is the orchestration brain of the Continuo platform. When a job completes, it updates the dependency graph, resolves which downstream nodes are now unblocked, and triggers their execution — using a three-table transactional messaging pattern to guarantee exactly-once semantics end-to-end.

---

## Level 0 — Context

```
 ┌───────────────────────────────────────────────────────────────────────┐
 │                          External Systems                             │
 │                                                                       │
 │  executor-controller ──► Redis (update.table:v1)                      │
 │                                      │                               │
 │                          dependency-controller                        │
 │                                      │                               │
 │                          ┌───────────┴────────────────────┐          │
 │                          │                                │          │
 │                    Neo4j (graph update)    Redis (query.model:v1)    │
 │                    State Service (gRPC)         │                    │
 │                                         executor-controller          │
 └───────────────────────────────────────────────────────────────────────┘
```

The service sits between job completion and the next round of job execution. It consumes a node-status event, walks the dependency graph, and emits one ready-to-execute event per newly unblocked downstream node.

---

## Level 1 — Main Data Flow

```
Redis Stream                      dependency-controller                  External Systems
(update.table:v1)                 ┌───────────────────────┐
                                  │                       │
  ┌──────────────────────┐        │  1. Consumer reads    │
  │ {                    │───────►│     message           │
  │   task_id,           │        │                       │
  │   schedule_id,       │        │  2. Dedup check:      │◄──── PostgreSQL
  │   schedule_name,     │        │     INSERT INTO        │      message_processing
  │   service_name,      │        │     message_processing │      ON CONFLICT DO NOTHING
  │   schema,            │        │     ON CONFLICT        │
  │   table_name,        │        │     → if already        │
  │   status             │        │       completed/acked   │
  │ }                    │        │       → ACK & skip      │
  └──────────────────────┘        │                       │
                                  │  3. Update node       │────► Neo4j (:7687)
                                  │     status in graph   │      SET t.status = SUCCEEDED/FAILED
                                  │     (outside TX,      │      SET t.last_updated_at = datetime()
                                  │      idempotent)      │
                                  │                       │
                                  │  4. Query ready       │────► Neo4j
                                  │     downstream nodes  │◄──── MATCH (t)-[:DEPENDS_ON]->(up)
                                  │     (outside TX)      │      WHERE ALL upstreams SUCCEEDED
                                  │                       │      AND t.status NULL or PENDING
                                  │                       │
                                  │  5. Begin TX          │◄──── PostgreSQL
                                  │                       │
                                  │  6. For each          │────► State Service (gRPC :50051)
                                  │     downstream node:  │      GetTaskByScheduleAndNode
                                  │     ensure task       │      CreateTask / UpdateTaskStatus
                                  │     exists + pending  │◄──── returns: task_id
                                  │                       │
                                  │  7. Write outbox      │────► PostgreSQL
                                  │     entry per node    │      INSERT INTO outbox
                                  │     (linked to        │      (message_processing_id FK)
                                  │      message_proc)    │      status = 'pending'
                                  │                       │
                                  │  8. Mark message      │────► PostgreSQL
                                  │     → 'completed'     │      UPDATE message_processing
                                  │                       │      SET state = 'completed'
                                  │                       │
                                  │  9. Commit TX         │────► PostgreSQL COMMIT
                                  │                       │
                                  │  10. ACK message      │────► Redis XACK
                                  │                       │
                                  │  11. Mark message     │────► PostgreSQL (best-effort)
                                  │     → 'acked'         │      UPDATE message_processing
                                  │                       │      SET state = 'acked'
                                  └───────────────────────┘

Background (continuous polling every 1s):

  PostgreSQL ──────────────────► Outbox Processor ───────────────────────────────────
  outbox                         ┌────────────────────────┐
  status = 'pending'             │ 1. SELECT pending batch│
                                 │    (FOR UPDATE          │
                                 │     SKIP LOCKED)        │
                                 │                        │
                                 │ 2. Check               │────► PostgreSQL
                                 │    published_messages  │◄──── EXISTS(outbox_entry_id)?
                                 │    → if already done   │      if yes → MarkProcessed, skip
                                 │                        │
                                 │ 3. Optimistic lock:    │────► PostgreSQL
                                 │    UPDATE outbox       │      status: pending → publishing
                                 │    WHERE status=pending│      0 rows → claimed by other
                                 │                        │
                                 │ 4. Publish to Redis    │────► Redis (query.model:v1)
                                 │    (includes           │      XADD with outbox_entry_id
                                 │     outbox_entry_id)   │      for downstream dedup
                                 │                        │
                                 │ 5. Record publish      │────► PostgreSQL
                                 │                        │      INSERT INTO published_messages
                                 │                        │      (outbox_entry_id, redis_msg_id)
                                 │                        │
                                 │ 6. MarkProcessed or    │────► PostgreSQL
                                 │    IncrRetry or        │
                                 │    MarkFailed          │
                                 └────────────────────────┘
                                          │
                                          ▼
                                 Redis (query.model:v1)
                                 {
                                   outbox_entry_id,   ← for downstream dedup
                                   schedule_id,
                                   schedule_name,
                                   service_name,
                                   schema,
                                   table_name,
                                   task_id,
                                   job_name
                                 }
```

---

## Level 2 — Message Handler Detail

```
ProcessStatusHandler.Handle(ctx, cmd ProcessNodeStatus, messageID string)
│
├── json.Marshal(cmd) → payload
│
├── uow.Begin()                              ← PostgreSQL transaction starts
│
├── messageProcessingRepo.InsertIfNotExists({
│       MessageID:  messageID,               ← Redis stream message ID (UNIQUE key)
│       StreamName: "update.table:v1",
│       State:      "processing",
│       Payload:    payload,
│   })
│     ├── returns (id, inserted=true)  → new message, continue
│     └── returns (_, inserted=false) → already exists:
│           ├── GetByMessageID → check state
│           ├── state == "completed" or "acked" → uow.Rollback(); return nil (skip)
│           └── state == "processing" → uow.Rollback(); return nil (concurrent instance)
│
├── neo4jRepo.UpdateNodeStatus(scheduleName, schema, tableName, status)
│     └── MATCH (t:Table {schedule_name, schema, table_name})
│         SET t.status = $status, t.last_updated_at = datetime()
│         (outside transaction, idempotent)
│
├── if status != "SUCCEEDED":
│     └── skip downstream resolution → go to step: mark completed, commit
│
├── neo4jRepo.GetReadyDownstream(scheduleName, schema, tableName)
│     └── MATCH (t:Table)-[:DEPENDS_ON]->(upstream)
│         WHERE upstream.schedule_name = $name AND upstream.schema = $schema ...
│         AND NOT EXISTS {
│           MATCH (t)-[:DEPENDS_ON]->(dep)
│           WHERE dep.status <> 'SUCCEEDED'
│         }
│         AND (t.status IS NULL OR t.status = 'PENDING')
│         RETURN t.service_name, t.schema, t.table_name,
│                COALESCE(t.node_type, "") AS node_type
│         (nodes with invalid/missing node_type are skipped with an error log)
│
├── for each downstream node:
│     ├── stateClient.GetTaskByScheduleAndNode(scheduleID, serviceName, schema, tableName)
│     │     ├── if NOT found → stateClient.CreateTask(newUUID, scheduleID, ..., maxRetries=3)
│     │     └── if found but not PENDING → stateClient.UpdateTaskStatus(taskID, PENDING)
│     │
│     ├── pkg/domain.ComputeJobName(serviceName, schema, tableName)
│     │     └── K8s DNS-1123 compliant, max 63 chars
│     │
│     └── outboxRepo.Create(OutboxEntry{
│               MessageProcessingID: msgProcID,   ← FK linking outbox to source message
│               AggregateType:       "dependency",
│               AggregateID:         scheduleID,
│               EventType:           "node_ready_for_execution",
│               Payload:             NodeReadyForExecution{...} JSON,
│               StreamName:          "query.model:v1",
│               Status:              "pending",
│               MaxRetries:          3,
│         })
│
├── messageProcessingRepo.UpdateState(msgProcID, "completed")
│
├── uow.Commit()                              ← All-or-nothing
│     └── on failure → Rollback(), Redis message not ACKed → redelivered
│
├── redis.XAck("update.table:v1", "dependency_controller_consumers", messageID)
│
└── messageProcessingRepo.UpdateState(msgProcID, "acked")   ← best-effort
```

---

## Level 2 — Outbox Processor Detail

```
OutboxProcessor.Run(ctx)                     ← goroutine, ticks every 1s
│
└── loop:
      │
      ├── outboxRepo.GetPendingBatch(limit=100)
      │     └── SELECT ... FROM outbox
      │         WHERE status = 'pending' AND retry_count < max_retries
      │         ORDER BY created_at ASC
      │         FOR UPDATE SKIP LOCKED
      │
      └── for each entry:
            │
            ├── publishedRepo.Exists(entry.ID)
            │     ├── if true → outboxRepo.MarkProcessed(entry.ID); continue
            │     └── if false → continue
            │
            ├── outboxRepo.UpdateStatus(entry.ID, "publishing", "pending")
            │     ├── UPDATE ... WHERE id=$id AND status='pending'
            │     ├── 1 row updated → this instance owns it, continue
            │     └── 0 rows updated → errAlreadyClaimed → skip silently
            │
            ├── json.Unmarshal(entry.Payload) → NodeReadyForExecution
            │
            ├── producer.Publish("query.model:v1", {
            │       "outbox_entry_id": entry.ID,   ← CRITICAL for downstream dedup
            │       "schedule_id":     ...,
            │       "schedule_name":   ...,
            │       "service_name":    ...,
            │       "schema":          ...,
            │       "table_name":      ...,
            │       "task_id":         ...,
            │       "job_name":        ...,
            │       "node_type":       ...,
            │   })
            │     └── XADD query.model:v1 MaxLen~=10000 * {...}
            │         returns: redisMessageID
            │
            ├── publishedRepo.Create({OutboxEntryID: entry.ID, RedisMessageID: msgID})
            │     └── INSERT INTO published_messages ON CONFLICT DO NOTHING
            │
            ├── on success:
            │     └── outboxRepo.MarkProcessed(entry.ID)
            │
            └── on failure:
                  ├── if retry_count + 1 < max_retries:
                  │     └── outboxRepo.IncrementRetry(entry.ID)
                  │           UPDATE ... SET retry_count = retry_count + 1, status = 'pending'
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
├── Create consumer group for update.table:v1
│   (XGroupCreateMkStream — no-op if already exists)
│
├── Start HTTP server (:8086) — /health, /ready
│
├── Start OutboxProcessor goroutine (1s poll interval)
│     └── immediately picks up 'pending' outbox entries
│         left over from a crash-after-commit
│
└── Start Redis Consumer (blocking, reads from ">")
      └── Redis PEL (Pending Entry List) automatically redelivers
          any un-ACKed messages to the consumer group on reconnect
          → handler deduplicates via message_processing.message_id
```

---

## Three-Table Deduplication Design

```
                   ┌─────────────────────────────────────────────────────┐
                   │              message_processing                      │
                   │  (one row per Redis message ID)                      │
                   │                                                      │
                   │  id (PK), message_id (UNIQUE), stream_name,          │
                   │  state: processing → completed → acked,              │
                   │  payload, error, created_at, updated_at              │
                   └─────────────────┬───────────────────────────────────┘
                                     │  message_processing_id (FK)
                                     ▼
                   ┌─────────────────────────────────────────────────────┐
                   │                    outbox                            │
                   │  (one row per downstream node per source message)    │
                   │                                                      │
                   │  id (PK), message_processing_id (FK),               │
                   │  aggregate_type, aggregate_id, event_type,          │
                   │  payload (JSONB), stream_name,                       │
                   │  status: pending → publishing → processed/failed,   │
                   │  retry_count, max_retries, error,                    │
                   │  created_at, updated_at                              │
                   └─────────────────┬───────────────────────────────────┘
                                     │  outbox_entry_id (FK, UNIQUE)
                                     ▼
                   ┌─────────────────────────────────────────────────────┐
                   │              published_messages                      │
                   │  (one row per successfully published outbox entry)   │
                   │                                                      │
                   │  id (PK), outbox_entry_id (UNIQUE FK),              │
                   │  redis_message_id, published_at                      │
                   └─────────────────────────────────────────────────────┘
```

**What each table prevents:**

| Table | Prevents |
|-------|----------|
| `message_processing` | Re-processing the same Redis message twice (on redelivery) |
| `outbox` | Losing events — they persist until published |
| `published_messages` | Publishing the same outbox entry twice to Redis |

The `outbox_entry_id` field included in every published Redis message allows the downstream `executor-controller` to deduplicate at its own boundary, closing the final gap.

---

## Data Structures

### Input Event — `update.table:v1`

| Field           | Type   | Description                           |
|-----------------|--------|---------------------------------------|
| `task_id`       | UUID   | Task tracker ID in state service      |
| `schedule_id`   | UUID   | Scheduler run ID                      |
| `schedule_name` | string | Name of the DAG schedule              |
| `service_name`  | string | Data warehouse service name           |
| `schema`        | string | Database schema of the completed node |
| `table_name`    | string | Name of the completed node's table    |
| `status`        | string | `SUCCEEDED` or `FAILED`              |

### `message_processing` (PostgreSQL)

| Field        | Type      | Description                                      |
|--------------|-----------|--------------------------------------------------|
| `id`         | UUID      | Primary key                                      |
| `message_id` | string    | Redis stream message ID — UNIQUE constraint      |
| `stream_name`| string    | Always `"update.table:v1"`                       |
| `state`      | string    | `processing` → `completed` → `acked`            |
| `payload`    | JSONB     | Full command payload                             |
| `error`      | string?   | Set if processing failed                         |
| `created_at` | timestamp | Row creation time                                |
| `updated_at` | timestamp | Last state transition time                       |

### `outbox` (PostgreSQL)

| Field                  | Type      | Description                                   |
|------------------------|-----------|-----------------------------------------------|
| `id`                   | UUID      | Primary key                                   |
| `message_processing_id`| UUID      | FK → message_processing (source linkage)      |
| `aggregate_type`       | string    | Always `"dependency"`                         |
| `aggregate_id`         | UUID      | The `schedule_id`                             |
| `event_type`           | string    | Always `"node_ready_for_execution"`           |
| `payload`              | JSONB     | Serialized `NodeReadyForExecution` event      |
| `stream_name`          | string    | Always `"query.model:v1"`                    |
| `status`               | string    | `pending` → `publishing` → `processed/failed`|
| `retry_count`          | int       | Number of publish attempts                    |
| `max_retries`          | int       | Max attempts before marking failed (3)        |
| `error`                | string?   | Set on permanent failure                      |
| `created_at`           | timestamp | Entry creation time                           |
| `updated_at`           | timestamp | Last status change time                       |

### `published_messages` (PostgreSQL)

| Field             | Type      | Description                               |
|-------------------|-----------|-------------------------------------------|
| `id`              | UUID      | Primary key                               |
| `outbox_entry_id` | UUID      | FK → outbox (UNIQUE — one record per pub) |
| `redis_message_id`| string    | Message ID returned by Redis XADD         |
| `published_at`    | timestamp | Publish time                              |

### Output Event — `query.model:v1`

| Field             | Type   | Description                                                    |
|-------------------|--------|----------------------------------------------------------------|
| `outbox_entry_id` | UUID   | Critical: used by executor-controller for dedup                |
| `schedule_id`     | UUID   | Scheduler run ID                                               |
| `schedule_name`   | string | Name of the DAG schedule                                       |
| `service_name`    | string | Data warehouse service name                                    |
| `schema`          | string | Database schema of the downstream node                         |
| `table_name`      | string | Name of the downstream node's table                            |
| `task_id`         | UUID   | Task tracker ID (created/found in state service)               |
| `job_name`        | string | K8s DNS-1123 compliant identifier (max 63 chars)               |
| `node_type`       | string | dbt resource type: `dbt-model`, `dbt-seed`, or `dbt-snapshot` |

---

## Failure Modes and Recovery

| Failure Point                              | Consequence                            | Recovery Mechanism                                              |
|--------------------------------------------|----------------------------------------|-----------------------------------------------------------------|
| Crash before TX commits                    | No state change anywhere               | Redis redelivers → `message_processing` dedup detects first run|
| Commit succeeds, ACK fails                 | Outbox written, message not ACKed      | Redis redelivers → `message_processing` finds `completed` → ACK only |
| Neo4j update fails                         | TX rolls back                          | Redis redelivers → retried (idempotent Neo4j update)            |
| State service call fails                   | TX rolls back                          | Redis redelivers → retried (idempotent state ops)               |
| Outbox publish fails                       | Entry remains `pending`                | Retry up to `max_retries`; eventually marked `failed`           |
| Publish succeeds, record fails             | Redis has message, `published_messages` missing | Next poll re-publishes; executor deduplicates via `outbox_entry_id` |
| Duplicate message (same `message_id`)      | `INSERT ON CONFLICT` → 0 rows          | Check `message_processing.state`; skip if `completed`/`acked`  |
| Concurrent outbox processors               | Both try to claim same entry           | Optimistic lock (`WHERE status='pending'`) → only one wins      |
| Max retries exceeded                       | Entry marked `failed`                  | Logged; requires manual intervention                            |

---

## Component Interactions Summary

```
                         ┌───────────────────────────────────────────┐
                         │         dependency-controller              │
                         │                                           │
Redis ──────────────────►│  Consumer                                 │
(update.table:v1)        │  (dedup via message_processing)           │
                         │     │                                     │
                         │     ▼                                     │
Neo4j ◄──────────────────│  ProcessStatusHandler                     │────► State Service (gRPC)
(update node status,     │  (UpdateNodeStatus, GetReadyDownstream)   │      (GetTask/CreateTask/
 get ready downstream)   │     │                                     │       UpdateTaskStatus)
                         │     ▼                                     │
PostgreSQL ◄─────────────│  Transactional write:                     │
(message_processing,     │  message_processing + outbox entries      │
 outbox)                 │  committed atomically                     │
                         │                                           │
                         │  Outbox Processor (bg, 1s poll)           │
                         │     │                                     │
PostgreSQL ─────────────►│     ├─ check published_messages ──────────│────► PostgreSQL
(pending outbox entries) │     ├─ optimistic lock ───────────────────│────► PostgreSQL
                         │     ├─ Publish ──────────────────────────│────► Redis
                         │     └─ record published_messages ─────────│────► PostgreSQL
                         │                                           │
                         │  HTTP :8086                               │
                         │  /health  /ready                          │
                         └───────────────────────────────────────────┘
```
