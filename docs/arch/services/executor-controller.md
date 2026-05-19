# executor-controller

## Purpose

`executor-controller` turns dispatch messages into Kubernetes Jobs.

It is responsible for:
- consuming executable node intents from Redis
- deduplicating repeated dispatches via `pkg/messageprocessing` keyed on `(message_id, stream_name)`
- durably recording deployment intent in its own outbox
- creating K8s Jobs via the Kubernetes API
- publishing `task.status.updated:v1` (RUNNING) so `state` can track task progress
- publishing `node.deployed:v1` so `k8s-controller` can begin monitoring

## Owned Storage (Postgres: `continuo_executor`)

| Table | Purpose |
|---|---|
| `deployment_outbox` | One row per pending K8s deployment intent; tracks retry count, status (`pending` → `processed` / `failed`) |
| `message_processing` | Inbound dedup: keyed on `(message_id, stream_name)`; prevents double-processing of duplicate Redis messages |
| `cancelled_schedules` | Records schedule cancellations; consulted by deploy handlers before writing to `deployment_outbox` |

## Inbound Interfaces

### Redis consumers

| Stream | Consumer group | Description |
|---|---|---|
| `query.model:v1` | `executor-query-model` | Primary dispatch: new node ready for execution |
| `retry.task:v1` | `executor-retry` | Retry dispatch: re-attempt a failed node |
| `schedule.cancelled:v1` | `executor-schedule-cancelled` | Schedule cancellation: suppress future deployments for the schedule |

`query.model:v1` and `retry.task:v1` carry the same fields:
- `task_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema_name`, `table_name`, `job_name`
- `node_type`

### Inbound message processing

executor-controller consumes the three streams above via `pkg/redis.StreamConsumer`. For each stream the wire path is:

`pkg/redis.StreamConsumer` → `adapters/redis/<stream>_binding.go` → `service/handlers/<stream>_handler.go`

The binding parses the XMessage into a typed `domain/events.<Event>`, runs `pkg/messageprocessing.Dedup` against the per-service `message_processing` table (keyed on `(message_id, stream_name)`), and invokes the handler inside a single Unit-of-Work transaction. `schedule.cancelled:v1` skips dedup because `cancelled_schedules.Insert` is `INSERT ... ON CONFLICT DO NOTHING` and is naturally idempotent.

The cancelled-schedule guard runs inside `QueryModelHandler` and `RetryTaskHandler` via `uow.CancelledSchedulesRepo().Exists`; a cancelled match commits the dedup row (so the message is ACKed and never reprocessed) and returns without writing to `deployment_outbox`.

### HTTP (port 8084)

- `GET /health` — liveness probe only

## Outbound Interfaces

### Redis producers

| Stream | Description |
|---|---|
| `task.status.updated:v1` | Published after K8s job creation succeeds with `status=RUNNING`; consumed by `state` to update task status |
| `node.deployed:v1` | Published after K8s job creation succeeds; triggers `k8s-controller` monitoring |

`task.status.updated:v1` payload fields:
- `task_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema_name`, `table_name`
- `status` — always `RUNNING` from this producer

`node.deployed:v1` payload fields:
- `outbox_entry_id`
- `task_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema_name`, `table_name`, `job_name`
- `node_type`

### Kubernetes API

- `CreateQueryJob` — creates a K8s batch Job in the configured namespace with label `app=dbt-job`; container name is `dbt-job`; treated as idempotent (already-exists is not an error on retry)

### K8s client configuration

`NewK8sClient` uses `KUBECONFIG` when set (local / docker-compose). If `KUBECONFIG` is not set it falls back to `rest.InClusterConfig()` for pod deployments with a ServiceAccount.

## Processing Logic

### On `query.model:v1` or `retry.task:v1`

```
1. Dedup check via pkg/messageprocessing against message_processing (message_id, stream_name)
   → if already present: skip (ACK without processing)
2. Check cancelled_schedules for the schedule_id
   → if cancelled: commit dedup row and return (no deployment written)
3. Write deployment intent to deployment_outbox (pending)
4. Commit (dedup row + outbox entry in one transaction)
```

### Outbox processor (every 5 seconds, batch of 100)

```
For each pending deployment_outbox entry:
  1. ParseNodeType — if invalid: MarkFailed immediately (data corruption, permanent)
  2. CreateQueryJob (K8s) — idempotent; failure → retry up to MaxRetries
  3. Publish task.status.updated:v1 (Redis) with status=RUNNING
  4. Publish node.deployed:v1 (Redis)
  5. MarkProcessed

On failure:
  - retry_count < max_retries: IncrementRetry, try again next poll
  - retry_count >= max_retries: MarkFailed (permanent)
```

## Background Loops

| Loop | Description |
|---|---|
| Redis consumers (three streams) | Reads `query.model:v1`, `retry.task:v1`, and `schedule.cancelled:v1` via `pkg/redis.StreamConsumer`; crash-recovery for pending messages on startup |
| Outbox processor | Polls `deployment_outbox` every 5 seconds; processes up to 100 entries per batch |

## Reliability Patterns

- **Inbound dedup**: `message_processing` keyed on `(message_id, stream_name)` prevents double-processing of duplicate Redis messages; managed by `pkg/messageprocessing.Dedup`
- **Deployment outbox**: intent is committed to Postgres before any K8s call; crash-safe
- **Explicit transaction boundary**: each inbound message runs dedup + outbox insert inside a single Unit-of-Work transaction; the dedup row and deployment intent are committed atomically
- **K8s idempotency**: `CreateQueryJob` treats already-exists as success; retries after crash are safe
- **Step ordering**: K8s creation → `task.status.updated:v1` (RUNNING) → `node.deployed:v1`; if Redis publishes fail after K8s succeeds, the retry will re-attempt idempotently
- **No state gRPC dependency**: executor-controller does not call state gRPC; task status updates flow via `task.status.updated:v1`
- **Permanent failure**: invalid `node_type` (data corruption) is immediately marked failed rather than retried indefinitely
- **Outbox delivery retries**: 3 per `deployment_outbox` entry (governs K8s job creation attempts); after that, entry is marked `failed` and must be manually recovered
- **Task max retries**: `task_max_retries` written into `deployment_outbox` defaults to 2 (3 total execution attempts: initial + 2 retries); propagated to `k8s-controller` via `node.deployed:v1` to govern task-level retry logic
