# Startup Controller - System Flow

## Architecture Overview

```
┌──────────────────┐         ┌─────────────────────────────────────────┐
│  Redis Stream    │         │    Startup Controller :8083              │
│  scheduler.      │─Input──▶│  ┌──────────────────────────────────┐   │
│  started:v1      │         │  │   Consumer (Redis)               │   │
└──────────────────┘         │  │   - Read scheduler.started       │   │
                             │  │   - ACK after processing         │   │
                             │  └──────────┬───────────────────────┘   │
                             │             │                           │
                             │             ▼                           │
                             │  ┌──────────────────────────────────┐   │
                             │  │   Message Bus (CQRS)             │   │
                             │  │   - Route commands to handlers   │   │
                             │  └──────────┬───────────────────────┘   │
                             │             │                           │
                             │             ▼                           │
                             │  ┌──────────────────────────────────┐   │
                             │  │   InitializeScheduler Handler    │   │
                             │  │   1. Update init status          │   │
                             │  │   2. Query Neo4j for roots       │   │
                             │  │   3. Create tasks via state gRPC │   │
                             │  │   4. Write to outbox (txn)       │   │
                             │  │   5. Mark init complete          │   │
                             │  └──────────┬───────────────────────┘   │
                             │             │                           │
                             │             ▼                           │
                             │  ┌──────────────────────────────────┐   │
                             │  │   Outbox Processor               │   │
                             │  │   - Poll pending entries         │   │
                             │  │   - Publish to Redis             │   │
                             │  │   - Mark as processed            │   │
                             │  └──────────┬───────────────────────┘   │
                             └─────────────┼───────────────────────────┘
                                           │
                                           ▼
                             ┌──────────────────────┐
                             │  Redis Stream        │
                             │  query.model:v1      │◀──────Output
                             └──────────────────────┘

External Dependencies:
- Neo4j (graph queries)
- State Service gRPC (task CRUD)
- PostgreSQL (outbox table)
```

## Data Flow - Complete Initialization Workflow

---

### Step 1: Consume scheduler.started Event

```
REDIS STREAM: scheduler.started:v1
Message ID: 1768474650022-0
{
  "runner_id": "8db77e66-9e7f-45ce-a81b-f58f9d2d3b9a",
  "schedule_name": "daily"
}
       │
       ▼
┌──────────────────────────────────────┐
│ Redis Consumer                       │
│ - Consumer Group: "startup_consumers"│
│ - Consumer Name: "worker-1"          │
│ - Read with XREADGROUP               │
└──────────┬───────────────────────────┘
           │
           ▼
┌──────────────────────────────────────┐
│ Message Bus                          │
│ - Unmarshal JSON                     │
│ - Create InitializeScheduler command │
└──────────┬───────────────────────────┘
           │
           ▼
┌──────────────────────────────────────┐
│ InitializeScheduler Handler          │
│ - Receives command                   │
└──────────────────────────────────────┘
```

**Natural Language:**
Redis consumer reads from `scheduler.started:v1` stream using consumer group → Unmarshals message → Creates InitializeScheduler command → Routes to handler.

---

### Step 2: Initialize Schedule (Transactional)

```
┌─────────────────────────────────────────────────┐
│ Handler: InitializeScheduler                    │
│ Input: schedule_id, schedule_name               │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│ UnitOfWork.Begin() ──▶ START TRANSACTION        │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│ State Service gRPC Call                         │
│ UpdateInitializationStatus(                     │
│   schedule_id,                                  │
│   status = "in_progress"                        │
│ )                                               │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│ Neo4j Repository                                │
│ GetRootNodes(schedule_name = "daily")           │
│ Returns: [                                      │
│   {schema: "public", table: "users", ...},      │
│   {schema: "public", table: "products", ...},   │
│   {schema: "analytics", table: "orders", ...}   │
│ ]                                               │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│ FOR EACH root node:                             │
│                                                 │
│ 1. Check if task exists                         │
│    State.GetTaskByScheduleAndNode()             │
│                                                 │
│ 2. If NOT exists:                               │
│    State.CreateTask(                            │
│      task_id = uuid.New(),                      │
│      schedule_id,                               │
│      service_name,                              │
│      schema_name,                               │
│      table_name,                                │
│      max_retries = 3                            │
│    )                                            │
│    ⭐ State service computes job_name ⭐         │
│    Returns: task with job_name field            │
│                                                 │
│ 3. Create outbox entry (in PostgreSQL txn)      │
│    INSERT INTO outbox (                         │
│      aggregate_id = schedule_id,                │
│      event_type = "node_ready_for_execution",   │
│      payload = {                                │
│        schedule_id,                             │
│        schedule_name,                           │
│        service_name,                            │
│        schema_name,                             │
│        table_name,                              │
│        job_name  ⭐ NEW                          │
│      },                                         │
│      stream_name = "query.model:v1",            │
│      status = "pending"                         │
│    )                                            │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│ State Service gRPC Call                         │
│ UpdateInitializationStatus(                     │
│   schedule_id,                                  │
│   status = "completed"                          │
│ )                                               │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│ UnitOfWork.Commit() ──▶ COMMIT TRANSACTION      │
│ All operations succeed atomically               │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│ Redis Consumer                                  │
│ XACK message_id                                 │
│ (Acknowledges successful processing)            │
└─────────────────────────────────────────────────┘
```

**Natural Language:**
Start transaction → Update init status to "in_progress" → Query Neo4j for root nodes → For each root: create task in state service (**job_name auto-computed**) → Write event to outbox table → Update init status to "completed" → Commit transaction → ACK Redis message.

**Critical: Transactional Guarantee**
All operations within the transaction succeed or fail together:
- If Neo4j query fails → rollback, no tasks created
- If state service call fails → rollback, no outbox entries
- If outbox write fails → rollback, tasks deleted
- Only on COMMIT are outbox entries visible to processor

---

### Step 3: Outbox Processing (Background)

```
┌─────────────────────────────────────────────────┐
│ Outbox Processor (Background goroutine)         │
│ Polling interval: 1 second                      │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│ Query PostgreSQL                                │
│ SELECT * FROM outbox                            │
│ WHERE status = 'pending'                        │
│ ORDER BY created_at ASC                         │
│ LIMIT 100                                       │
│                                                 │
│ Returns:                                        │
│ [                                               │
│   {                                             │
│     id: uuid,                                   │
│     stream_name: "query.model:v1",              │
│     payload: {                                  │
│       schedule_id,                              │
│       schedule_name,                            │
│       service_name,                             │
│       schema_name,                              │
│       table_name,                               │
│       job_name: "analytics-public-users" ⭐     │
│     }                                           │
│   },                                            │
│   ...                                           │
│ ]                                               │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│ FOR EACH outbox entry:                          │
│                                                 │
│ 1. Publish to Redis                             │
│    XADD query.model:v1 * payload                │
│    Returns: message_id                          │
│                                                 │
│ 2. Mark as processed (in same txn)              │
│    UPDATE outbox                                │
│    SET status = 'processed',                    │
│        processed_at = NOW()                     │
│    WHERE id = $entry_id                         │
│                                                 │
│ 3. On error:                                    │
│    - Increment retry_count                      │
│    - If retry_count > max_retries:              │
│        SET status = 'failed'                    │
│        SET error_message = $error               │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│ Redis Stream: query.model:v1                    │
│ Message published:                              │
│ {                                               │
│   "schedule_id": "uuid",                        │
│   "schedule_name": "daily",                     │
│   "service_name": "analytics",                  │
│   "schema_name": "public",                      │
│   "table_name": "users",                        │
│   "job_name": "analytics-public-users" ⭐       │
│ }                                               │
└─────────────────────────────────────────────────┘
```

**Natural Language:**
Background processor polls outbox table → Retrieves pending entries → For each: publish to Redis stream (**including job_name**) → Mark as processed → On error: retry up to 3 times, then mark as failed.

**Reliability Guarantees:**
- **At-least-once delivery**: Outbox ensures events survive crashes
- **Idempotency**: Downstream consumers must handle duplicate messages
- **Crash recovery**: On restart, processor picks up pending entries
- **Failure isolation**: Failed entries don't block other messages

---

## Crash Recovery

### Scenario: Service Crashes During Initialization

```
TIME:  t0          t1          t2          t3
       │           │           │           │
       │ Start     │ Query     │ CRASH!    │ Restart
       │ txn       │ Neo4j     │ (before   │ Service
       │           │           │  commit)  │
       ▼           ▼           ▼           ▼
    ┌─────┐   ┌─────┐     ┌─────┐     ┌─────┐
    │BEGIN│──▶│WORK │────▶│  X  │     │RETRY│
    └─────┘   └─────┘     └─────┘     └─────┘
                                          │
                                          ▼
                                    ┌──────────────┐
                                    │ Recover:     │
                                    │ 1. Check DB  │
                                    │ 2. Find      │
                                    │    in_progress│
                                    │ 3. Reset to  │
                                    │    pending   │
                                    │ 4. Retry     │
                                    └──────────────┘
```

**Natural Language:**
If service crashes during processing, transaction rolls back automatically. On restart, state service resets all `in_progress` initialization_status to `pending`. Next consumption retries the operation.

**Recovery Steps:**
1. Service restarts
2. State service runs `ResetInProgressInitializations()` on startup
3. Redis consumer picks up un-ACKed messages (automatic retry)
4. Handler processes again with fresh transaction

---

### Scenario: Outbox Entry Not Published Before Crash

```
TIME:  t0          t1          t2          t3
       │           │           │           │
       │ Commit    │ Create    │ CRASH!    │ Restart
       │ txn       │ outbox    │ (before   │ Service
       │           │ entry     │  publish) │
       ▼           ▼           ▼           ▼
    ┌─────┐   ┌─────┐     ┌─────┐     ┌─────┐
    │DONE │──▶│PEND-│────▶│  X  │     │PROC │
    └─────┘   │ING  │     └─────┘     │ESS  │
              └─────┘                  └──┬──┘
               (saved                     │
                in DB)                    ▼
                                    ┌──────────────┐
                                    │ OutboxProc   │
                                    │ finds pending│
                                    │ publishes to │
                                    │ Redis        │
                                    └──────────────┘
```

**Natural Language:**
Outbox entry is saved in database during transaction. If service crashes before publishing, entry remains in `pending` status. On restart, OutboxProcessor automatically finds and publishes pending entries.

---

## Database Schema

### Outbox Table

```sql
CREATE TABLE outbox (
  id                UUID PRIMARY KEY,
  aggregate_type    VARCHAR(50) NOT NULL,
  aggregate_id      UUID NOT NULL,
  event_type        VARCHAR(50) NOT NULL,
  payload           JSONB NOT NULL,        -- Includes job_name ⭐
  stream_name       VARCHAR(100) NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at      TIMESTAMPTZ,
  status            VARCHAR(20) NOT NULL DEFAULT 'pending',
  retry_count       INTEGER NOT NULL DEFAULT 0,
  max_retries       INTEGER NOT NULL DEFAULT 3,
  error_message     TEXT
);

CREATE INDEX idx_outbox_status_created
  ON outbox(status, created_at)
  WHERE status = 'pending';
```

**Payload Example:**
```json
{
  "schedule_id": "8db77e66-9e7f-45ce-a81b-f58f9d2d3b9a",
  "schedule_name": "daily",
  "service_name": "analytics",
  "schema_name": "public",
  "table_name": "users",
  "job_name": "analytics-public-users"
}
```

---

## Component Interactions

```
┌───────────────┐
│ State Service │
│ :50051        │
└───────┬───────┘
        │ gRPC calls:
        │ - UpdateInitializationStatus()
        │ - CreateTask() → Returns job_name ⭐
        │ - GetTaskByScheduleAndNode()
        │
        ▼
┌──────────────────────┐
│ Startup Controller   │
│                      │
│ ┌────────────────┐   │
│ │ Consumer       │   │
│ │ (Redis)        │   │
│ └────────┬───────┘   │
│          ▼           │
│ ┌────────────────┐   │
│ │ Handler        │   │
│ │ (CQRS)         │   │
│ └────────┬───────┘   │
│          ▼           │
│ ┌────────────────┐   │
│ │ UnitOfWork     │   │
│ │ (Transaction)  │   │
│ └────────┬───────┘   │
│          ▼           │
│ ┌────────────────┐   │
│ │ OutboxProcessor│   │
│ │ (Background)   │   │
│ └────────┬───────┘   │
└──────────┼───────────┘
           │
           ▼
┌──────────────────────┐
│ External Services    │
│ - Neo4j :7687        │
│ - PostgreSQL :5432   │
│ - Redis :6379        │
└──────────────────────┘
```

---

## Error Handling

### Neo4j Connection Error
```
GetRootNodes(schedule_name) → connection timeout
→ Transaction rolls back
→ Handler returns error
→ Redis message NOT ACKed
→ Automatic retry by Redis consumer
```

### State Service gRPC Error
```
CreateTask(...) → gRPC Unavailable
→ Transaction rolls back
→ No outbox entries created
→ Redis message NOT ACKed
→ Automatic retry
```

### Outbox Publish Error
```
XADD query.model:v1 → Redis connection error
→ Outbox entry remains 'pending'
→ retry_count incremented
→ Next poll cycle retries
→ After 3 failures → status = 'failed'
```

---

## Performance Considerations

### Concurrency
- Single consumer instance per consumer group
- Parallel processing of multiple root nodes within transaction
- Outbox processor polls every 1 second (configurable)

### Optimization
- Batch outbox processing (up to 100 entries per poll)
- Connection pooling for PostgreSQL and Neo4j
- gRPC connection reuse to state service

### Monitoring
- Outbox pending count (should trend to 0)
- Failed outbox entries (investigate if > 0)
- Processing latency (time from Redis → published)

---

## Testing

### E2E Test 1: Full Stream Consumption
**Validates:**
- Consume scheduler.started event
- Initialize schedule with root nodes
- Write to outbox (transactional)
- Publish to query.model stream
- Verify job_name in published events ⭐

### E2E Test 2: Crash Recovery
**Validates:**
- Process half of outbox entries
- Simulate crash (stop processor)
- Restart processor
- Verify remaining entries published
- No duplicate publishes

---

## License

Internal use only.
