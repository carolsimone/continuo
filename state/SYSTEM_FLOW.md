# State Service - System Flow

## Architecture Overview

```
                                    ┌─────────────────────┐
                                    │  Cron Scheduler     │
                                    │  (Internal Timer)   │
                                    │  - Daily: 1am CET   │
                                    │  - Hourly: Every hr │
                                    └──────────┬──────────┘
                                               │
                                               │ Triggers
                                               ▼
┌─────────────┐          ┌──────────────────────────────────┐          ┌────────────────┐
│   Client    │  gRPC    │      State Service :50051        │  SQL     │   PostgreSQL   │
│ (grpcurl,   │─────────▶│  ┌───────────────────────────┐  │─────────▶│   Database     │
│  services)  │          │  │  Schedule Activator       │  │          │   :5432        │
└─────────────┘          │  │  - Check active schedules │  │          └────────────────┘
                         │  │  - Create tracker records │  │                  ▲
                         │  │  - Publish Redis events   │  │                  │ SQL
                         │  └────────────┬──────────────┘  │          ┌───────┴────────┐
                         │               │                  │          │ScheduleCatalog │
                         │  ┌────────────────────────────┐ │          │  Reconciler    │
                         │  │ScheduleCatalogConsumer     │─┼──────────│ (Upsert/Soft-  │
                         │  │ Group: state-schedule-cat  │ │          │  delete names) │
                         │  └────────────┬───────────────┘ │          └────────────────┘
                         │               │                  │
                         │               │ Redis (XREADGROUP)
                         └───────────────┼──────────────────┘
                                         │
                          ┌──────────────┴──────────────┐
                          │                             │
                          ▼                             ▼
                   ┌─────────────────────┐    ┌──────────────────────┐
                   │  Redis Server       │    │  Redis Server        │
                   │  Stream:            │    │  Stream:             │
                   │ scheduler.started:v1│    │ schedules.loaded:v1  │
                   │  (produced)         │    │  (consumed)          │
                   └─────────────────────┘    └──────────────────────┘
                          │                             ▲
                          │ Consumed by                 │ Published by
                          ▼                             │
                   ┌──────────────────┐    ┌───────────────────────┐
                   │  Other Services  │    │  manifest-controller  │
                   │  (Controller,    │    │  (after graph load)   │
                   │   Executor, etc) │    └───────────────────────┘
                   └──────────────────┘

                         ┌──────────────┐
                         │ Health Check │  HTTP
                         │   :8082      │◀──────────── Monitoring
                         └──────────────┘
```

## Data Flow

---

## 🔄 Automatic Scheduler Activation (New Feature)

### Overview
The state service automatically activates daily and hourly schedules at predefined times using an internal cron scheduler.

### Activation Flow

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          AUTOMATIC ACTIVATION                               │
└────────────────────────────────────────────────────────────────────────────┘

TRIGGER (Cron):
  Daily:  "0 1 * * *"  (1am CET)
  Hourly: "0 * * * *"  (Every hour at minute 0)
       │
       │ Europe/Paris Timezone
       ▼
┌──────────────────────────────┐
│   Cron Scheduler             │
│ - robfig/cron/v3             │
│ - Fires activation callback  │
└───────────┬──────────────────┘
            │
            ▼
┌──────────────────────────────┐
│   Schedule Activator         │
│ Step 1: Check Database       │
│   Query: HasActiveSchedule() │
│   WHERE schedule_name = ?    │
│   AND status IN              │
│     ('pending', 'running')   │
└───────────┬──────────────────┘
            │
            ├─────────────┐
            │             │
      YES (blocked)    NO (proceed)
            │             │
            ▼             ▼
    ┌──────────────┐  ┌──────────────────────────────┐
    │ SKIP         │  │ Step 2: Create Record        │
    │ Log: Active  │  │   INSERT INTO                │
    │ run exists   │  │     scheduler_tracker        │
    │              │  │   VALUES:                    │
    └──────────────┘  │   - schedule_id (new UUID)   │
                      │   - schedule_name ("daily")  │
                      │   - status (PENDING)         │
                      │   - created_at (NOW())       │
                      └───────────┬──────────────────┘
                                  │
                                  ▼
                      ┌────────────────────────────────┐
                      │ Step 3: Publish Redis Event    │
                      │   Stream: scheduler.started:v1 │
                      │   Fields:                      │
                      │   {                            │
                      │     runner_id: "uuid",         │
                      │     schedule_name: "daily"     │
                      │   }                            │
                      │   Returns: message_id          │
                      └───────────┬────────────────────┘
                                  │
                                  ▼
                      ┌──────────────────────────────┐
                      │ Redis Server                 │
                      │ - XADD command               │
                      │ - MaxLen: 10000 (approx)     │
                      │ - Synchronous write          │
                      └───────────┬──────────────────┘
                                  │
                                  ▼
                              SUCCESS
                      (Logged with schedule_id)
```

**Natural Language:**
Cron fires at scheduled time → Activator checks for active schedules in database → If none active, creates new scheduler_tracker record with PENDING status → Publishes event to Redis stream with runner_id and schedule_name → Other services consume event via consumer groups and start execution.

---

### Duplicate Prevention Example

```
Timeline:

00:59:30  Scheduler_tracker:
          ┌────────────────────────────────┐
          │ schedule_id  | name   | status │
          ├────────────────────────────────┤
          │ aaa-111-bbb  | daily  | SUCCEEDED │  ← Previous run (terminal)
          └────────────────────────────────┘

01:00:00  ⏰ CRON FIRES (Daily schedule)

          Check: HasActiveSchedule("daily")
          Query: SELECT EXISTS(... WHERE name='daily' AND status IN ('pending','running'))
          Result: FALSE ✓

          CREATE new record:
          ┌────────────────────────────────┐
          │ schedule_id  | name   | status │
          ├────────────────────────────────┤
          │ aaa-111-bbb  | daily  | SUCCEEDED │
          │ ccc-222-ddd  | daily  | PENDING │  ← NEW
          └────────────────────────────────┘

          PUBLISH: {runner_id: "ccc-222-ddd", schedule_name: "daily"}

01:05:00  Another service still processing...
          Status remains PENDING

02:00:00  ⏰ CRON FIRES (Daily schedule again)

          Check: HasActiveSchedule("daily")
          Query: SELECT EXISTS(... WHERE name='daily' AND status IN ('pending','running'))
          Result: TRUE ✗

          SKIP ← Duplicate run prevented!
          Log: "Schedule activation skipped - active run exists"

          Database unchanged:
          ┌────────────────────────────────┐
          │ schedule_id  | name   | status │
          ├────────────────────────────────┤
          │ aaa-111-bbb  | daily  | SUCCEEDED │
          │ ccc-222-ddd  | daily  | PENDING │  ← Still active
          └────────────────────────────────┘
```

---

### Component Interaction Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         STATE SERVICE INTERNALS                          │
└─────────────────────────────────────────────────────────────────────────┘

  ┌─────────────┐
  │   main()    │
  └──────┬──────┘
         │
         ├──────────────────────────────────┐
         │                                  │
         ▼                                  ▼
  ┌──────────────┐                  ┌──────────────────┐
  │ gRPC Server  │                  │ Cron Scheduler   │
  │  :50051      │                  │ (Background)     │
  └──────┬───────┘                  └────────┬─────────┘
         │                                   │
         │ API Calls                         │ Time-based
         │                                   │
         ▼                                   ▼
  ┌──────────────┐                  ┌──────────────────┐
  │  Handlers    │                  │ Activator        │
  │ - Scheduler  │                  │ - ActivateSchedule()│
  │ - Task       │                  └────────┬─────────┘
  └──────┬───────┘                           │
         │                                   │
         │                                   │
         └─────────────┬─────────────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │  Repositories   │
              │ - SchedulerRepo │
              │ - TaskRepo      │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │   PostgreSQL    │
              │   Database      │
              └─────────────────┘

  ┌──────────────────┐
  │ Redis Producer   │◀─────── Schedule Activator
  │ (Singleton)      │
  └────────┬─────────┘
           │
           ▼
  ┌──────────────────┐
  │  Redis Server    │
  │  (Stream)        │
  └──────────────────┘
```

---

### Redis Streams Event Schema

**Stream:** `scheduler.started:v1`

**Message Format (Redis Stream Entry):**
```json
{
  "runner_id": "58947c36-b234-43a4-bf8f-2dd2755c9140",
  "schedule_name": "daily"
}
```

**Example Message ID:** `1705254000123-0` (timestamp-sequence)

**Redis Streams Configuration:**
- **Stream Name:** `scheduler.started:v1` (versioned for schema evolution)
- **Trimming:** MaxLen 10,000 messages (approximate, for memory efficiency)
- **Message ID:** Auto-generated by Redis (timestamp-sequence format)
- **Write Mode:** Synchronous (XADD returns message ID immediately)
- **Ordering:** Guaranteed insertion order within stream
- **Persistence:** Controlled by Redis persistence settings (RDB/AOF)

**Consumer Expectations:**
Other services (startup-controller) should:
1. Join consumer group via XGROUP CREATE (e.g., `startup_controller_consumers`)
2. Read from stream using XREADGROUP with unique consumer name
3. Parse `runner_id` and `schedule_name` from message fields
4. Query state service via gRPC for task details using `runner_id`
5. Update scheduler status to RUNNING when processing begins
6. Create and execute tasks
7. **XACK message ID after successful processing** (critical for exactly-once semantics)
8. Update scheduler status to SUCCEEDED/FAILED when complete

**Crash Recovery:**
- On startup, check for pending messages using XPendingExt
- Claim abandoned messages using XClaim (MinIdle: 0)
- Reprocess and ACK claimed messages

---

### Error Handling in Activation

```
┌──────────────────────────────────────────────────────────┐
│                    ERROR SCENARIOS                        │
└──────────────────────────────────────────────────────────┘

1. Database Connection Lost
   ├─ HasActiveSchedule() fails
   ├─ Log error
   ├─ Skip this activation cycle
   └─ Next cron trigger will retry

2. Duplicate Key Error (Race Condition)
   ├─ Two activators create simultaneously
   ├─ One succeeds, one gets ErrDuplicateKey
   ├─ Failed activator logs and returns
   └─ Successful one publishes event

3. Redis Server Unavailable
   ├─ Record created in database (status=PENDING)
   ├─ Redis XADD fails (connection error)
   ├─ Log error (high severity)
   ├─ Record remains in PENDING
   └─ Manual intervention may be required

4. Redis XADD Timeout
   ├─ Record created successfully
   ├─ XADD command times out (network issue)
   ├─ Log warning
   ├─ Message state unknown (may or may not be added)
   ├─ Retry may cause duplicate entries
   └─ Consumers should implement idempotency via message deduplication

**Note:** Unlike Kafka, Redis XADD is typically fast. Timeouts are rare but possible
with network issues or very high load. Message IDs can be used for deduplication.

All errors are logged with structured logging:
{
  "level": "ERROR",
  "msg": "Failed to activate schedule",
  "schedule_name": "daily",
  "error": "connection refused",
  "timestamp": "2026-01-04T18:25:00Z"
}
```

---

### Lifecycle and Graceful Shutdown

```
Startup Order:
  1. Database connection
  2. Repositories (SchedulerTracker, ScheduleCatalog, Task, TaskExecution)
  3. Redis client connection
  4. Redis producer (wraps client)
  5. Schedule catalog consumer ← Started in background goroutine
  6. Schedule activator
  7. Load schedules.yaml config (fail-fast)
  8. Cron scheduler ← Started after config load
  9. gRPC server (started in background goroutine)
  10. Health server (started in background goroutine)

Shutdown Order (LIFO):
  1. Health server ← Stops accepting health checks
  2. gRPC server ← Stops accepting API calls
  3. Cron scheduler ← Stops triggering new activations
  4. Schedule catalog consumer ← Context cancel propagates
  5. Redis producer ← Closes cleanly (no flush needed)
  6. Redis client ← Closes connection
  7. Database ← Closes connections

During shutdown:
  - Cron.Stop() waits for running jobs to complete (max 5s)
  - Redis writes are synchronous (no buffering to flush)
  - Consumer loop exits on context cancellation
  - Database closes pool gracefully
```

---

---

## 📥 Schedule Catalog Consumer (`schedules.loaded:v1`)

### Overview
The state service maintains a `schedule_catalog` table that reflects the full set of known schedule names. This catalog is kept in sync by consuming `schedules.loaded:v1` events published by manifest-controller after each successful graph load.

### Consumer Flow

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        SCHEDULE CATALOG CONSUMER                            │
└────────────────────────────────────────────────────────────────────────────┘

  Redis Stream: schedules.loaded:v1
  Consumer Group: state-schedule-catalog
       │
       │ XREADGROUP (blocking)
       ▼
┌──────────────────────────────────┐
│  ScheduleCatalogConsumer         │
│  Step 0 (startup):               │
│    processPending() — claim &    │
│    reprocess any messages left   │
│    pending from prior crash      │
└───────────┬──────────────────────┘
            │
            ▼
┌──────────────────────────────────┐
│  Payload Deserialization         │
│  {                               │
│    "event_id": "uuid",           │
│    "schedule_names": ["daily",…] │
│  }                               │
└───────────┬──────────────────────┘
            │
            ▼
┌──────────────────────────────────┐
│  ScheduleCatalogHandler          │
│  Step 1: Deduplication check     │
│    SELECT FROM processed_events  │
│    WHERE event_id = ?            │
│    → already processed? skip ACK │
│      (transient path)            │
└───────────┬──────────────────────┘
            │ (not seen before)
            ▼
┌──────────────────────────────────┐
│  Step 2: Upsert schedule names   │
│    INSERT INTO schedule_catalog  │
│    ON CONFLICT DO UPDATE         │
│    (clears deleted_at if re-added│
└───────────┬──────────────────────┘
            │
            ▼
┌──────────────────────────────────┐
│  Step 3: Soft-delete removed     │
│    UPDATE schedule_catalog       │
│    SET deleted_at = NOW()        │
│    WHERE name NOT IN payload     │
└───────────┬──────────────────────┘
            │
            ▼
┌──────────────────────────────────┐
│  Step 4: Record processed event  │
│    INSERT INTO processed_events  │
│    (event_id, processed_at)      │
└───────────┬──────────────────────┘
            │
            ▼
        XACK message
```

**Natural Language:**
Consumer reads events from `schedules.loaded:v1` → deduplication check prevents re-processing → upserts new/re-added schedules → soft-deletes schedules no longer in the list → records event ID → ACKs message. Non-ACKed messages remain pending and are reclaimed on restart.

### At-Least-Once Guarantee

```
On startup:
  processPending() → XPendingExt to list unacknowledged messages
                   → XClaim to take ownership of abandoned ones
                   → reprocess each through ScheduleCatalogHandler
                   → ACK on success (idempotent due to dedup check)

On processing error:
  → do NOT ACK
  → message stays pending
  → next restart will reclaim and retry
```

---

## 📡 Manual Operations via gRPC

### Create Scheduler

```
INPUT (gRPC):
{
  "schedule_name": "daily-batch",
  "status": PENDING
}
       │
       ▼
┌──────────────────┐
│ gRPC Handler     │
│ - Generate UUID  │
│ - Set created_at │
└──────────────────┘
       │
       ▼
┌──────────────────┐
│ Repository       │
│ - SQL INSERT     │
└──────────────────┘
       │
       ▼
OUTPUT (gRPC):
{
  "scheduler": {
    "schedule_id": "uuid",
    "schedule_name": "daily-batch",
    "status": "PENDING",
    "created_at": "timestamp"
  }
}
```

**Natural Language:**
Client sends schedule name and status → Handler generates UUID and timestamp → Repository inserts into database → Returns created scheduler with ID.

---

### Create Task

```
INPUT (gRPC):
{
  "task_id": "client-generated-uuid",
  "schedule_id": "parent-scheduler-uuid",
  "service_name": "dbt-service",
  "schema_name": "analytics",
  "table_name": "users",
  "max_retries": 3,
  "status": PENDING
}
       │
       ▼
┌──────────────────────┐
│ gRPC Handler         │
│ - Validate UUIDs     │
│ - Set created_at     │
│ - Compute job_name ⭐ │
└──────────┬───────────┘
           │
           ▼
    ┌──────────────────────────────────────┐
    │ computeJobName()                     │
    │ Input: service_name, schema, table   │
    │ Steps:                               │
    │ 1. Concatenate: service-schema-table │
    │ 2. Lowercase transformation          │
    │ 3. Sanitize (alphanumeric + hyphens) │
    │ 4. Collapse consecutive hyphens      │
    │ 5. Trim leading/trailing hyphens     │
    │ 6. Truncate to 63 chars (K8s limit)  │
    │ Output: "dbt-service-analytics-users"│
    └──────────┬───────────────────────────┘
               │
               ▼
       ┌──────────────────┐
       │ Repository       │
       │ - SQL INSERT     │
       │ - FK check       │
       │ - Store job_name │
       └──────────────────┘
               │
               ▼
OUTPUT (gRPC):
{
  "task": {
    "task_id": "uuid",
    "schedule_id": "parent-uuid",
    "service_name": "dbt-service",
    "schema_name": "analytics",
    "table_name": "users",
    "job_name": "dbt-service-analytics-users",  ⭐ NEW
    "status": "PENDING",
    "retry_count": 0,
    "max_retries": 3,
    "created_at": "timestamp"
  }
}
```

**Natural Language:**
Client provides task details with pre-generated UUID → Handler validates and **computes Kubernetes-compliant job_name** → Repository inserts with foreign key reference to scheduler → Returns created task with job_name.

**job_name Validation Rules (Kubernetes DNS-1123):**
- Max length: 63 characters
- Allowed: lowercase letters, digits, hyphens (-)
- Must start and end with alphanumeric
- No consecutive hyphens
- Example transformations:
  - `Dbt_Service.Analytics.Users` → `dbt-service-analytics-users`
  - `Service__Name___Public--Users` → `service-name-public-users`
  - `very-long-service-name-that-exceeds-limits...` → `very-long-service-name-that-exceeds-kubernetes-limits-significa` (truncated)

---

### Cancel Scheduler

```
INPUT (gRPC):
{
  "schedule_id": "uuid",
  "cancelled_by": "admin@example.com",
  "cancellation_reason": "Manual stop"
}
       │
       ▼
┌──────────────────┐
│ gRPC Handler     │
│ - Validate UUID  │
└──────────────────┘
       │
       ▼
┌──────────────────┐
│ Repository       │
│ UPDATE:          │
│ - status=CANCEL  │
│ - cancelled_at   │
│ - cancelled_by   │
│ - reason         │
│ WHERE:           │
│ - schedule_id    │
│ - NOT terminal   │
└──────────────────┘
       │
       ▼
OUTPUT (gRPC):
{
  "scheduler": {
    "schedule_id": "uuid",
    "status": "CANCELLED",
    "cancelled_at": "timestamp",
    "cancelled_by": "admin@example.com",
    "cancellation_reason": "Manual stop",
    ...
  }
}
```

**Natural Language:**
Client requests cancellation → Handler validates → Repository updates status only if not already terminal (succeeded/failed/cancelled) → Returns updated scheduler with audit trail.

---

### List Tasks

```
INPUT (gRPC):
{
  "schedule_id": "uuid",
  "status": PENDING,      // optional
  "page_size": 50,
  "page_offset": 0
}
       │
       ▼
┌──────────────────┐
│ gRPC Handler     │
│ - Build filters  │
└──────────────────┘
       │
       ▼
┌──────────────────┐
│ Repository       │
│ SELECT:          │
│ - WHERE filters  │
│ - ORDER BY       │
│ - LIMIT/OFFSET   │
│ - COUNT(*)       │
└──────────────────┘
       │
       ▼
OUTPUT (gRPC):
{
  "tasks": [
    {
      "task_id": "uuid1",
      "status": "PENDING",
      ...
    },
    {
      "task_id": "uuid2",
      "status": "PENDING",
      ...
    }
  ],
  "total_count": 42
}
```

**Natural Language:**
Client requests tasks with filters → Handler builds SQL filters → Repository executes filtered query with pagination → Returns array of tasks and total count.

---

## Complete Request Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        gRPC Request                              │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │ Logging Interceptor   │
                    │ - Log method name     │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   gRPC Server         │
                    │ - Route to handler    │
                    └───────────┬───────────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
                ▼                               ▼
    ┌───────────────────┐         ┌───────────────────┐
    │ SchedulerHandler  │         │   TaskHandler     │
    │ - Validate input  │         │ - Validate input  │
    │ - Proto→Domain    │         │ - Proto→Domain    │
    └─────────┬─────────┘         └─────────┬─────────┘
              │                             │
              ▼                             ▼
    ┌───────────────────┐         ┌───────────────────┐
    │ SchedulerRepo     │         │   TaskRepo        │
    │ - SQL queries     │         │ - SQL queries     │
    └─────────┬─────────┘         └─────────┬─────────┘
              │                             │
              └──────────────┬──────────────┘
                             │
                             ▼
                    ┌───────────────────┐
                    │   PostgreSQL      │
                    │ - Execute query   │
                    │ - Return rows     │
                    └─────────┬─────────┘
                              │
                    ┌─────────┴─────────┐
                    │   Error Handling  │
                    │ - ErrNotFound     │
                    │ - ErrDuplicateKey │
                    │ - SQL errors      │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ Domain→Proto      │
                    │ - Convert models  │
                    │ - Set timestamps  │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │  gRPC Response    │
                    └───────────────────┘
```

## Error Flow

```
Database Error
      │
      ▼
┌─────────────────┐
│  Repository     │
│ - sql.ErrNoRows ──────▶ ErrNotFound
│ - pq.Error      ──────▶ ErrDuplicateKey
│ - Other         ──────▶ Generic Error
└─────────────────┘
      │
      ▼
┌─────────────────┐
│  Handler        │
│ - ErrNotFound   ──────▶ codes.NotFound
│ - ErrDuplicate  ──────▶ codes.AlreadyExists
│ - Validation    ──────▶ codes.InvalidArgument
│ - Other         ──────▶ codes.Internal
└─────────────────┘
      │
      ▼
gRPC Error Response
```

**Natural Language:**
Database errors are caught by repository → Converted to domain errors → Handler maps to gRPC status codes → Client receives appropriate error code and message.

---

## State Transitions

### Scheduler Status Flow

```
┌─────────┐
│ PENDING │─────┐
└────┬────┘     │
     │          │
     ▼          │
┌─────────┐     │
│ RUNNING │     │
└────┬────┘     │
     │          │
     ├──────────┼────────┐
     │          │        │
     ▼          ▼        ▼
┌───────────┐ ┌──────────┐ ┌───────┐
│ SUCCEEDED │ │ FAILED   │ │CANCEL │
└───────────┘ └──────────┘ └───────┘
   (terminal)   (terminal)  (terminal)
```

**Natural Language:**
Scheduler starts PENDING → moves to RUNNING → ends in SUCCEEDED, FAILED, or CANCELLED (terminal states). Can be cancelled from PENDING or RUNNING only.

### Task Status Flow

```
┌─────────┐
│ PENDING │
└────┬────┘
     │
     ▼
┌─────────┐
│ RUNNING │
└────┬────┘
     │
     ├───────────┬──────────┐
     │           │          │
     ▼           ▼          ▼
┌───────────┐ ┌──────┐  ┌────────┐
│ SUCCEEDED │ │FAILED│  │CANCEL  │
└───────────┘ └──┬───┘  └────────┘
   (terminal)    │      (terminal)
                 │
                 ▼
        (retry if count < max)
                 │
                 ▼
            ┌─────────┐
            │ PENDING │
            └─────────┘
```

**Natural Language:**
Task starts PENDING → RUNNING → SUCCEEDED/FAILED/CANCELLED. If FAILED and retries remaining, returns to PENDING. Otherwise terminal.

---

## Data Model Relationships

```
┌────────────────────────────────────┐
│     scheduler_tracker              │
├────────────────────────────────────┤
│ PK: schedule_id (UUID)             │
│     schedule_name                  │
│     status                         │
│     initialization_status ⭐ NEW    │
│     created_at                     │
│     started_at                     │
│     completed_at                   │
│     last_heartbeat_at              │
│     cancelled_at                   │
│     cancelled_by                   │
│     cancellation_reason            │
└───────────────┬────────────────────┘
                │ 1
                │
                │ has many
                │
                │ *
┌───────────────┴────────────────────┐
│       task_tracker                 │
├────────────────────────────────────┤
│ PK: task_id (UUID)                 │
│ FK: schedule_id → scheduler_tracker│
│     service_name                   │
│     schema_name                    │
│     table_name                     │
│     job_name (VARCHAR 63) ⭐ NEW    │
│     status                         │
│     retry_count                    │
│     max_retries                    │
│     created_at                     │
│     cancelled_at                   │
│     cancelled_by                   │
└────────────────────────────────────┘
```

**Natural Language:**
One scheduler has many tasks. Tasks reference scheduler via foreign key with CASCADE delete. When scheduler is deleted, all its tasks are automatically removed.

---

## Typical Usage Scenarios

### Scenario A: Automatic Activation (New)

```
1. 🕐 Cron Fires at Scheduled Time
   └─▶ Daily: 1am CET
   └─▶ Hourly: Every hour

2. ✅ State Service Activates Schedule
   └─▶ Creates: scheduler_tracker record (status=PENDING)
   └─▶ Publishes: Redis stream event with runner_id (returns message_id)

3. 🎯 Consumer Service Receives Event
   └─▶ Reads from stream: {runner_id, schedule_name}
   └─▶ Queries: GetScheduler(runner_id) via gRPC

4. 🏃 Consumer Begins Execution
   └─▶ UpdateScheduler(runner_id, status=RUNNING, started_at)
   └─▶ Creates tasks for execution
   └─▶ Executes workflow

5. ✓ Consumer Completes
   └─▶ UpdateScheduler(runner_id, status=SUCCEEDED, completed_at)
   └─▶ XACK message (acknowledges successful processing)

OR Cancel:
   └─▶ CancelScheduler(runner_id, cancelled_by, reason)
```

**Natural Language:**
Automatic flow: Cron triggers → State service creates record & publishes to stream → Consumer reads from stream via consumer group → Consumer queries state → Consumer executes → Consumer updates completion status → Consumer ACKs message.

---

### Scenario B: Manual Creation (Original)

```
1. Create Scheduler
   └─▶ Returns: schedule_id

2. Create Tasks (multiple)
   └─▶ Input: schedule_id + task details
   └─▶ Returns: task_id for each

3. Update Scheduler to RUNNING
   └─▶ Set: started_at, status=RUNNING

4. Monitor Tasks
   └─▶ ListTasks(schedule_id, status=PENDING)

5. Update Task Progress
   └─▶ UpdateTask(task_id, status=RUNNING)
   └─▶ UpdateTask(task_id, status=SUCCEEDED)

6. Complete Scheduler
   └─▶ UpdateScheduler(schedule_id, status=SUCCEEDED, completed_at)

OR

6. Cancel Scheduler
   └─▶ CancelScheduler(schedule_id, cancelled_by, reason)
   └─▶ Manually cancel associated tasks if needed
```

**Natural Language:**
Manual flow: Client creates scheduler → Creates tasks → Starts execution → Updates progress → Completes or cancels.

---

## 🔧 Configuration

### Environment Variables

```bash
# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=runner
POSTGRES_USER=runner
POSTGRES_PASSWORD=runner
DB_SSLMODE=disable
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20

# gRPC Server
GRPC_PORT=50051

# Health Check
HEALTH_PORT=8082

# Redis Streams
REDIS_ADDR=redis:6379
REDIS_STREAM_SCHEDULER_STARTED=scheduler.started:v1

# Consumer Configuration (for other services)
REDIS_CONSUMER_STREAM=scheduler.started:v1
REDIS_CONSUMER_GROUP=startup_controller_consumers

# Logging
LOG_LEVEL=INFO
ENV=local
```

### Cron Schedules

```go
// Production (Default)
Daily:  "0 1 * * *"    // 1am CET every day
Hourly: "0 * * * *"    // Every hour at minute 0

// Testing (Configurable)
Test:   "*/30 * * * * *"  // Every 30 seconds (with WithSeconds option)
```
