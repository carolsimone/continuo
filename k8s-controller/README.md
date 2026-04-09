# K8s-Controller Service

## Overview

The **k8s-controller** is a critical service in the Continuo platform that monitors and manages Kubernetes job execution lifecycle. It acts as a bridge between Kubernetes clusters and the Continuo state management system, tracking job status changes and coordinating task retry/failure workflows.

## Purpose

- **Monitor Kubernetes Jobs**: Track the status of jobs deployed by the executor-controller
- **Status Synchronization**: Update task state in the Continuo state service based on K8s job outcomes
- **Event-Driven Coordination**: Publish task events (retry, failed, check-delayed) to Redis streams
- **Reliable Event Publishing**: Use Transactional Outbox pattern to ensure at-least-once delivery
- **Deduplication**: `processed_events` table prevents double-processing of redelivered messages
- **Operational Resilience**: Automatically resolve stuck outbox entries

## Architecture

The service follows **Domain-Driven Design (DDD)** and **CQRS** patterns with event-driven architecture.

See [DATA_FLOW.md](./DATA_FLOW.md) for detailed data flow diagrams.

### Tech Stack

- **Language**: Go 1.25+
- **Kubernetes**: Client-go for K8s API interactions
- **State Management**: gRPC client to state service
- **Messaging**: Redis Streams for event-driven communication
- **Persistence**: PostgreSQL (outbox pattern)
- **Observability**: Structured logging (slog)

---

## Design Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         K8S-CONTROLLER SERVICE                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                            INPUT STREAMS                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────┐      ┌──────────────────────┐                  │
│  │ executor.deployed:v1   │      │ k8s.check:v1         │                  │
│  │ (Redis Stream)         │      │ (Redis Stream)       │                  │
│  └────────────────────────┘      └──────────────────────┘                  │
│           │                                 │                                │
│           │                                 │                                │
│           ▼                                 ▼                                │
│  ┌──────────────────────────────────────────────────────┐                  │
│  │         DualStreamConsumer                           │                  │
│  │  - Consumes from both streams                        │                  │
│  │  - Consumer group: k8s_controller_consumers          │                  │
│  │  - Extracts outbox_entry_id for dedup (optional)     │                  │
│  │  - Converts to CheckJobStatus commands               │                  │
│  └──────────────────────────────────────────────────────┘                  │
│                            │                                                 │
└────────────────────────────┼─────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       COMMAND PROCESSING (CQRS)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────┐                         │
│  │            MessageBus                          │                         │
│  │  Routes: command.CheckJobStatus                │                         │
│  └────────────────────────────────────────────────┘                         │
│                       │                                                      │
│                       ▼                                                      │
│  ┌────────────────────────────────────────────────┐                         │
│  │       CheckStatusHandler                       │                         │
│  │  1. Query K8s API for job/pod status          │                         │
│  │  2. Get task details from state service        │                         │
│  │  3. Dedup check via processed_events           │                         │
│  │  4. Determine action based on status           │                         │
│  │  5. Write to outbox (transactional)            │                         │
│  └────────────────────────────────────────────────┘                         │
│              │              │              │                                 │
│              ▼              ▼              ▼                                 │
│    ┌──────────────┐ ┌─────────────┐ ┌──────────────┐                       │
│    │   Running    │ │  Succeeded  │ │   Failed     │                       │
│    │              │ │             │ │              │                       │
│    │ Schedule     │ │ Update to   │ │ Check retry  │                       │
│    │ delayed      │ │ SUCCEEDED   │ │ count        │                       │
│    │ re-check     │ │ Create      │ │ Update state │                       │
│    │              │ │ execution   │ │ Create exec  │                       │
│    └──────────────┘ └─────────────┘ └──────────────┘                       │
│              │              │              │                                 │
│              ▼              ▼              ▼                                 │
└──────────────┼──────────────┼──────────────┼─────────────────────────────────┘
               │              │              │
               ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    TRANSACTIONAL OUTBOX PATTERN                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────┐                         │
│  │         k8s_status_outbox (PostgreSQL)         │                         │
│  │  - Atomically written with state updates       │                         │
│  │  - Contains: event type, task context, flags   │                         │
│  │  - Status: pending → processed/failed          │                         │
│  └────────────────────────────────────────────────┘                         │
│                       │                    │                                 │
│                       ▼                    ▼                                 │
│         ┌────────────────────┐  ┌──────────────────────┐                   │
│         │  OutboxProcessor   │  │ StuckEntryResolver   │                   │
│         │  - Polls every 1s  │  │ - Polls every 30s    │                   │
│         │  - Batch: 100      │  │ - Batch: 50          │                   │
│         │  - Max retries: 3  │  │ - Max attempts: 5    │                   │
│         │  - FOR UPDATE      │  │ - FOR UPDATE         │                   │
│         │    SKIP LOCKED     │  │   SKIP LOCKED        │                   │
│         └────────────────────┘  └──────────────────────┘                   │
│                  │                         │                                 │
│                  │                         ▼                                 │
│                  │              (Handles stuck entries:                     │
│                  │               retry_count >= max_retries)                │
│                  │                         │                                 │
│                  ▼                         ▼                                 │
└──────────────────┼─────────────────────────┼─────────────────────────────────┘
                   │                         │
                   ▼                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        OUTPUT OPERATIONS                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────┐       ┌─────────────────────────────┐            │
│  │   State Service      │       │    Redis Streams            │            │
│  │   (gRPC)             │       │    (Event Publishing)       │            │
│  ├──────────────────────┤       ├─────────────────────────────┤            │
│  │ UpdateTaskStatus()   │       │ task.retry:v1               │            │
│  │ UpdateTaskWithRetry()│       │  - TaskID, RetryCount       │            │
│  │ CreateTaskExecution()│       │  - Next retry scheduled     │            │
│  └──────────────────────┘       │                             │            │
│                                  │ task.failed:v1              │            │
│                                  │  - TaskID, ErrorMessage     │            │
│                                  │  - Permanent failure        │            │
│                                  │                             │            │
│                                  │ k8s.check:v1                │            │
│                                  │  - TaskID, CheckAfter       │            │
│                                  │  - Delayed status check     │            │
│                                  └─────────────────────────────┘            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL INTEGRATIONS                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────┐                                                   │
│  │ Kubernetes API       │                                                   │
│  │ - Get Job status     │                                                   │
│  │ - Get Pod details    │                                                   │
│  │ - Extract logs       │                                                   │
│  └──────────────────────┘                                                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Details

### 1. DualStreamConsumer
**Input:**
- `executor.deployed:v1` - Jobs deployed by executor-controller
- `k8s.check:v1` - Delayed check requests

**Output:**
- `CheckJobStatus` commands to MessageBus

**Behavior:**
- Consumer group ensures horizontal scalability
- Extracts `outbox_entry_id` from incoming messages (optional; absent from older executor-controller deployments — nil means dedup is skipped)
- Acknowledges messages after successful processing

---

### 2. CheckStatusHandler
**Input:**
- `CheckJobStatus` command (taskID, scheduleID, jobName, etc.)

**Processing:**
1. Query Kubernetes API for job status
2. Retrieve task details from state service (retry count, max retries)
3. Begin transaction
4. **Dedup guard**: if `outbox_entry_id` is present, attempt atomic claim via `INSERT INTO processed_events ON CONFLICT DO NOTHING` — return early (skip) if duplicate (0 rows affected); proceed if claimed (1 row inserted)
5. Determine outcome:
   - **Running**: Schedule delayed re-check
   - **Succeeded**: Mark task as succeeded, create execution record
   - **Failed**:
     - If `retry_count < max_retries`: Increment retry, publish to `task.retry:v1`
     - If `retry_count >= max_retries`: Mark failed permanently, publish to `task.failed:v1`
6. Commit transaction (`processed_events` claim + outbox entries land atomically)

**Output:**
- Outbox entries (written transactionally)
- gRPC calls to state service
- Execution records

---

### 3. OutboxProcessor
**Input:**
- Outbox entries from `k8s_status_outbox` table
- Query: `WHERE status = 'pending' AND outbox_retry_count < max_retries`

**Processing:**
1. Fetch batch (100 entries) every 1 second
2. Process each entry:
   - Update task status (if flagged)
   - Create execution record (if flagged)
   - Publish to Redis stream
3. Mark as `processed` or increment retry count

**Output:**
- Events to Redis streams (`task.retry:v1`, `task.failed:v1`, `k8s.check:v1`)
- gRPC calls to state service

**Concurrency Safety:**
- Uses `FOR UPDATE SKIP LOCKED` to prevent duplicate processing across multiple pods

---

### 4. StuckEntryResolver
**Input:**
- Stuck outbox entries from `k8s_status_outbox` table
- Query: `WHERE status = 'pending' AND outbox_retry_count >= max_retries AND created_at < NOW() - 60 seconds`

**Processing:**
1. Fetch batch (50 entries) every 30 seconds
2. Attempt to force mark as failed (up to 5 attempts)
3. If all attempts fail: Log CRITICAL alert for manual intervention

**Output:**
- Failed outbox entries
- CRITICAL alerts in logs

**Purpose:**
- Prevents operational deadlocks when `MarkFailed()` fails
- Provides escalation path for stuck entries

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis server host |
| `REDIS_PORT` | `6379` | Redis server port |
| `REDIS_CONSUMER_DEPLOYED_STREAM` | `executor.deployed:v1` | Stream for deployed jobs |
| `REDIS_CONSUMER_CHECK_STREAM` | `k8s.check:v1` | Stream for delayed checks |
| `REDIS_CONSUMER_GROUP` | `k8s_controller_consumers` | Consumer group name |
| `REDIS_PRODUCER_CHECK_STREAM` | `k8s.check:v1` | Output stream for delayed checks |
| `REDIS_PRODUCER_RETRY_STREAM` | `task.retry:v1` | Output stream for retries |
| `REDIS_PRODUCER_FAILED_STREAM` | `task.failed:v1` | Output stream for failures |
| `REDIS_PRODUCER_UPDATE_TABLE_STREAM` | `update.table:v1` | Output stream for dependency-controller |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `runner` | Database name |
| `POSTGRES_USER` | `runner` | Database user |
| `POSTGRES_PASSWORD` | `runner` | Database password |
| `STATE_SERVICE_GRPC_ADDR` | `localhost:50051` | State service gRPC address |
| `HTTP_PORT` | `8085` | Health check HTTP port |
| `K8S_NAMESPACE` | `default` | Kubernetes namespace to monitor |
| `K8S_CHECK_DELAY_SECONDS` | `30` | Delay before re-checking running jobs |
| `ERROR_MESSAGE_MAX_LENGTH` | `4096` | Max error message length |
| `RESOLVER_CHECK_INTERVAL_SECONDS` | `30` | Stuck entry resolver poll interval |
| `RESOLVER_STUCK_THRESHOLD_SECONDS` | `60` | Time before entry considered stuck |
| `RESOLVER_BATCH_SIZE` | `50` | Stuck entry batch size |
| `RESOLVER_MAX_ATTEMPTS` | `5` | Max resolution attempts before escalation |

---

## Database Schema

### k8s_status_outbox

Stores pending events for reliable publishing (Transactional Outbox Pattern).

```sql
CREATE TABLE k8s_status_outbox (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type              VARCHAR(50) NOT NULL,  -- 'task_retry', 'task_failed', 'check_delayed'
    stream_name             VARCHAR(100) NOT NULL, -- Target Redis stream

    -- Task context
    task_id                 UUID NOT NULL,
    schedule_id             UUID NOT NULL,
    schedule_name           VARCHAR(255) NOT NULL,
    service_name            VARCHAR(255) NOT NULL,
    schema_name             VARCHAR(255) NOT NULL,
    table_name              VARCHAR(255) NOT NULL,
    job_name                VARCHAR(63) NOT NULL,

    -- Event payload
    error_message           TEXT,
    task_retry_count        INTEGER DEFAULT 0,
    check_after             BIGINT,

    -- State update flags
    update_task_status      BOOLEAN DEFAULT false,
    new_task_status         VARCHAR(20),
    new_retry_count         INTEGER,
    create_execution        BOOLEAN DEFAULT false,
    execution_started_at    TIMESTAMPTZ,
    execution_completed_at  TIMESTAMPTZ,
    execution_seconds       DOUBLE PRECISION,

    -- Outbox metadata
    status                  VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at            TIMESTAMPTZ,
    outbox_retry_count      INTEGER DEFAULT 0,
    max_retries             INTEGER DEFAULT 3,
    outbox_error_message    TEXT,

    CONSTRAINT k8s_status_outbox_status_check
        CHECK (status IN ('pending', 'processed', 'failed'))
);

-- Indexes for efficient querying
CREATE INDEX idx_k8s_status_outbox_pending
    ON k8s_status_outbox(created_at)
    WHERE status = 'pending' AND outbox_retry_count < max_retries;

CREATE INDEX idx_k8s_status_outbox_stuck
    ON k8s_status_outbox(created_at)
    WHERE status = 'pending' AND outbox_retry_count >= max_retries;
```

### processed_events

Tracks which `outbox_entry_id` values have already been handled, preventing double-processing when Redis redelivers a message after a crash or transient failure.

```sql
CREATE TABLE processed_events (
    outbox_entry_id UUID        PRIMARY KEY,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_k8s_processed_events_processed_at
    ON processed_events(processed_at);
```

---

## Event Flow Examples

### Example 1: Successful Job

```
1. executor-controller publishes to executor.deployed:v1
   └─ {task_id: "abc123", job_name: "dbt-run-xyz"}

2. k8s-controller receives event → CheckStatusHandler
   └─ Queries K8s API → Job status: Running

3. CheckStatusHandler writes to outbox:
   └─ {event_type: "check_delayed", stream_name: "k8s.check:v1",
       check_after: NOW() + 30s}

4. OutboxProcessor publishes to k8s.check:v1
   └─ {task_id: "abc123", check_after: <timestamp>}

5. k8s-controller re-checks after 30s → Job status: Succeeded

6. CheckStatusHandler writes 2 outbox entries:
   ├─ {event_type: "task_succeeded", stream_name: "",
   │    update_task_status: true, new_task_status: "SUCCEEDED",
   │    create_execution: true}           ← gRPC calls only, no Redis publish
   └─ {event_type: "node_status_updated", stream_name: "update.table:v1"}

7. OutboxProcessor:
   └─ Calls state service: UpdateTaskWithRetry(SUCCEEDED, retry_count)
   └─ Calls state service: CreateTaskExecution()
   └─ Publishes to update.table:v1 → dependency-controller unblocks downstream nodes
```

---

### Example 2: Failed Job with Retry

```
1. k8s-controller checks job → Job status: Failed (exit code 1)

2. CheckStatusHandler queries state service:
   └─ Current retry_count: 1, max_retries: 3

3. CheckStatusHandler writes to outbox:
   └─ {event_type: "task_retry", stream_name: "task.retry:v1",
       update_task_status: true, new_task_status: "FAILED",
       new_retry_count: 2, create_execution: true}

4. OutboxProcessor:
   └─ Calls state service: UpdateTaskWithRetry(FAILED, retry_count=2)
   └─ Calls state service: CreateTaskExecution()
   └─ Publishes to task.retry:v1
       └─ {task_id: "abc123", retry_count: 2, schedule_id: "xyz", ...}

5. executor-controller consumes task.retry:v1 and schedules new execution
```

---

### Example 3: Permanent Failure

```
1. k8s-controller checks job → Job status: Failed

2. CheckStatusHandler queries state service:
   └─ Current retry_count: 3, max_retries: 3

3. CheckStatusHandler writes to outbox:
   └─ {event_type: "task_failed", stream_name: "task.failed:v1",
       update_task_status: true, new_task_status: "FAILED",
       new_retry_count: 4, create_execution: true}

4. OutboxProcessor:
   └─ Calls state service: UpdateTaskWithRetry(FAILED, retry_count=4)
   └─ Calls state service: CreateTaskExecution()
   └─ Publishes to task.failed:v1
       └─ {task_id: "abc123", error_message: "...", retry_count: 3}

5. startup-controller consumes task.failed:v1 and updates schedule metadata
```

---

## Operational Resilience

### Transactional Outbox Pattern

**Problem:** Dual writes (database + Redis) can fail partially.

**Solution:**
1. Write to outbox table in the same transaction as state updates
2. Separate processor polls outbox and publishes to Redis
3. Retry logic with exponential backoff
4. At-least-once delivery guarantee

### Stuck Entry Resolution

**Problem:** If `MarkFailed()` fails when `retry_count >= max_retries`, entry becomes stuck.

**Solution:**
- **StuckEntryResolver** background process
- Detects entries with `retry_count >= max_retries AND status = 'pending'`
- Attempts aggressive retry (5 attempts)
- Escalates to CRITICAL alert for manual intervention

### Concurrency Safety

- **FOR UPDATE SKIP LOCKED** prevents multiple pods from processing same entry
- Consumer groups ensure message processing is distributed
- No application-level locks needed

---

## Health Checks

- `GET /health` — returns `200 OK` with body `OK`
- `GET /ready` — returns `200 OK` with body `READY`

Default port: `8085` (configurable via `HTTP_PORT`).

---

## Monitoring

### Key Metrics (Log-based)

```bash
# Outbox processing rate
{service="k8s-controller"} |= "Processing k8s status outbox batch" | json count

# Stuck entries detected
{service="k8s-controller"} |= "Found stuck outbox entries" | json count

# Critical alerts
{service="k8s-controller"} |= "CRITICAL: Stuck entry cannot be auto-resolved"
```

### Important Log Patterns

| Pattern | Severity | Description |
|---------|----------|-------------|
| `Processing k8s status outbox batch` | INFO | Outbox processor activity |
| `Found stuck outbox entries` | WARN | Stuck entries detected |
| `Successfully resolved stuck entry` | WARN | Stuck entry auto-resolved |
| `CRITICAL: Stuck entry cannot be auto-resolved` | ERROR | Manual intervention required |
| `Failed to process outbox entry` | ERROR | Transient failure |

---

## Testing

```bash
# Run all tests
go test ./...

# Run integration tests (requires docker-compose)
docker-compose up -d postgres flyway
go test -v ./test -run TestStuckEntryResolver
```

### Test Coverage

- Unit tests for handlers and processors
- Integration tests against real PostgreSQL
- Concurrency tests for `FOR UPDATE SKIP LOCKED`
- Edge case tests (boundary conditions, timeouts, context cancellation)

---

## Deployment

### Prerequisites

1. **Kubernetes Cluster**: Service must have access to K8s API
2. **PostgreSQL**: Database with applied migrations (Flyway)
3. **Redis**: Running Redis instance with stream support
4. **State Service**: gRPC endpoint available

### Docker Build

```bash
docker build -f Dockerfile.dev -t k8s-controller:latest .
```

### Kubernetes Deployment

See `docker-compose.yml` for configuration reference.

**Key Points:**
- Use service account with K8s API permissions
- Configure kubeconfig or in-cluster authentication
- Set appropriate resource limits
- Use health checks for liveness/readiness probes

---

## Troubleshooting

### Stuck Outbox Entries

**Symptom:** Logs show `CRITICAL: Stuck entry cannot be auto-resolved`

**Solution:**
```sql
-- Manually mark entry as failed
UPDATE k8s_status_outbox
SET status = 'failed',
    outbox_error_message = 'Manual intervention required',
    processed_at = NOW()
WHERE id = '<entry-id>';
```

### High Retry Count

**Symptom:** Many entries with `outbox_retry_count = max_retries`

**Potential Causes:**
- State service unavailable
- Redis connection issues
- Database connectivity problems

**Solution:** Check logs for root cause, fix infrastructure issue.

### Delayed Event Processing

**Symptom:** Events published to Redis streams with significant lag

**Potential Causes:**
- Large outbox backlog
- Insufficient processing capacity

**Solution:**
- Increase `RESOLVER_BATCH_SIZE` or deploy more pods
- Monitor database performance

---

## Architecture Decisions

### Why Transactional Outbox?

- **Consistency**: Ensures events are published only if state updates succeed
- **Reliability**: Handles partial failures gracefully
- **Auditability**: All events stored in database
- **Retry Logic**: Built-in retry with backoff

### Why Stuck Entry Resolver?

- **Operational Safety**: Prevents silent failures
- **Self-Healing**: Automatically resolves transient issues
- **Escalation Path**: CRITICAL alerts for unresolvable cases

### Why FOR UPDATE SKIP LOCKED?

- **Horizontal Scalability**: Multiple pods can process outbox concurrently
- **No Blocking**: Skips locked rows instead of waiting
- **High Throughput**: Maximizes processing rate

---

## Future Enhancements

- [ ] Prometheus metrics integration
- [ ] Adaptive retry intervals based on error patterns
- [ ] Slack/PagerDuty integration for CRITICAL alerts
- [ ] Root cause analysis dashboard for failures
- [ ] OpenTelemetry tracing

---

## Related Services

- **executor-controller**: Deploys K8s jobs for task execution
- **state**: Manages task lifecycle and metadata
- **startup-controller**: Handles schedule orchestration

---

## License

See `LICENSE` file at repository root.
