# executor-controller

## Purpose

`executor-controller` turns dispatch messages into Kubernetes Jobs.

It is responsible for:
- consuming executable node intents from Redis
- deduplicating repeated dispatches by upstream `outbox_entry_id`
- durably recording deployment intent in its own outbox
- creating K8s Jobs via the Kubernetes API
- publishing `task.status.updated:v1` (RUNNING) so `state` can track task progress
- publishing `node.deployed:v1` so `k8s-controller` can begin monitoring

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
| `retry.task:v1` | Retry dispatch: re-attempt a previously-failed node |

Both streams carry the same fields:
- `outbox_entry_id` (used for dedup in `processed_events`)
- `task_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema_name`, `table_name`, `job_name`
- `node_type`

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
| Redis consumer (dual-stream) | Reads `query.model:v1` and `retry.task:v1`; crash-recovery for pending messages on startup |
| Outbox processor | Polls `deployment_outbox` every 5 seconds; processes up to 100 entries per batch |

## Reliability Patterns

- **Inbound dedup**: `processed_events` keyed by `outbox_entry_id` prevents double-deployment from duplicate Redis messages
- **Deployment outbox**: intent is committed to Postgres before any K8s call; crash-safe
- **Explicit transaction boundary**: each inbound message runs its outbox insert inside a fresh transaction created by a transaction runner; parallel stream consumers may share one handler safely because transaction state is not stored on the handler or runner
- **K8s idempotency**: `CreateQueryJob` treats already-exists as success; retries after crash are safe
- **Step ordering**: K8s creation → `task.status.updated:v1` (RUNNING) → `node.deployed:v1`; if Redis publishes fail after K8s succeeds, the retry will re-attempt idempotently
- **No state gRPC dependency**: executor-controller no longer calls state gRPC; task status updates flow via `task.status.updated:v1`
- **Permanent failure**: invalid `node_type` (data corruption) is immediately marked failed rather than retried indefinitely
- **Outbox delivery retries**: 3 per `deployment_outbox` entry (governs K8s job creation attempts); after that, entry is marked `failed` and must be manually recovered
- **Task max retries**: `task_max_retries` written into `deployment_outbox` defaults to 2 (3 total execution attempts: initial + 2 retries); propagated to `k8s-controller` via `node.deployed:v1` to govern task-level retry logic
