# executor-controller

## Purpose

`executor-controller` turns dispatch messages into Kubernetes Jobs.

It is responsible for:
- consuming executable node intents from Redis
- deduplicating repeated dispatches by upstream `outbox_entry_id`
- durably recording deployment intent in its own outbox
- creating K8s Jobs via the Kubernetes API
- marking tasks `RUNNING` in `state`
- publishing `executor.deployed:v1` so `k8s-controller` can begin monitoring

## Owned Storage (Postgres: `continuo_executor`)

| Table | Purpose |
|---|---|
| `deployment_outbox` | One row per pending K8s deployment intent; tracks retry count, status (`pending` → `processed` / `failed`) |
| `processed_events` | Inbound dedup: keyed by upstream `outbox_entry_id`; prevents double-deployment |

## Inbound Interfaces

### Redis consumers

| Stream | Description |
|---|---|
| `query.model:v1` | Primary dispatch: new node ready for execution |
| `task.retry:v1` | Retry dispatch: re-attempt a previously-failed node |

Both streams carry the same fields:
- `outbox_entry_id` (used for dedup in `processed_events`)
- `task_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema`, `table_name`, `job_name`
- `node_type`

### HTTP (port 8084)

- `GET /health` — liveness probe only

## Outbound Interfaces

### Redis producer

| Stream | Description |
|---|---|
| `executor.deployed:v1` | Published after K8s job creation succeeds; triggers `k8s-controller` monitoring |

`executor.deployed:v1` payload fields:
- `outbox_entry_id`
- `task_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema`, `table_name`, `job_name`
- `node_type`

### gRPC to `state`

| Method | When |
|---|---|
| `UpdateTask` (status → RUNNING) | After K8s job is created successfully, before publishing `executor.deployed:v1` |

### Kubernetes API

- `CreateQueryJob` — creates a K8s batch Job in the configured namespace; treated as idempotent (already-exists is not an error on retry)

## Processing Logic

### On `query.model:v1` or `task.retry:v1`

```
1. If outbox_entry_id present: check processed_events
   → if already present: skip (dedup)
2. Write deployment intent to deployment_outbox (pending)
3. Commit
```

### Outbox processor (every 5 seconds, batch of 100)

```
For each pending deployment_outbox entry:
  1. ParseNodeType — if invalid: MarkFailed immediately (data corruption, permanent)
  2. CreateQueryJob (K8s) — idempotent; failure → retry up to MaxRetries
  3. UpdateTask → RUNNING (state gRPC)
     → if fails after K8s job exists: retry (K8s create is idempotent)
  4. Publish executor.deployed:v1 (Redis)
  5. MarkProcessed

On failure:
  - retry_count < max_retries: IncrementRetry, try again next poll
  - retry_count >= max_retries: MarkFailed (permanent)
```

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer (dual-stream) | Reads `query.model:v1` and `task.retry:v1`; crash-recovery for pending messages on startup |
| Outbox processor | Polls `deployment_outbox` every 5 seconds; processes up to 100 entries per batch |

## Reliability Patterns

- **Inbound dedup**: `processed_events` keyed by `outbox_entry_id` prevents double-deployment from duplicate Redis messages
- **Deployment outbox**: intent is committed to Postgres before any K8s call; crash-safe
- **K8s idempotency**: `CreateQueryJob` treats already-exists as success; retries after crash are safe
- **Step ordering**: K8s creation → state RUNNING → Redis publish; if state update or Redis publish fail after K8s succeeds, the retry will re-attempt idempotently
- **Permanent failure**: invalid `node_type` (data corruption) is immediately marked failed rather than retried indefinitely
- **Max retries**: 3 per outbox entry; after that, entry is marked `failed` and must be manually recovered
