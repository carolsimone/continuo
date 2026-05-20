# k8s-controller

## Purpose

`k8s-controller` monitors running Kubernetes Jobs, decides their outcome, and routes results back into the pipeline.

It is responsible for:
- consuming deployment notifications and delayed re-checks from Redis
- querying K8s for actual job/pod status
- deciding: still running → re-check, failed → retry or permanent failure, succeeded → complete
- publishing `task.status.updated:v1` so `state` can update task status
- publishing `task.execution.recorded:v1` so `state` can persist execution records
- uploading full pod logs to S3
- publishing terminal status updates to `orchestrator` (via `node.updated:v1`)

## Owned Storage (Postgres: `continuo_k8s`)

| Table | Purpose |
|---|---|
| `k8s_outbox` | Canonical transactional outbox — each write-time side effect is a separate row with a JSONB payload; `pkg/outbox.Processor` polls and publishes to the typed Redis stream per row |
| `message_processing` | Per-stream inbound dedup table keyed on `(message_id, stream_name)`; `pkg/messageprocessing.Dedup` inserts atomically inside the same transaction as outbox writes |

All `k8s_outbox` rows conform to the canonical schema: `id`, `message_processing_id` (nullable), `aggregate_type`, `aggregate_id`, `event_type`, `payload` (JSONB), `stream_name`, `status`, `retry_count`, `max_retries`, `created_at`, `processed_at`, `error_message`.

## Inbound Interfaces

### Redis consumers

| Stream | Description |
|---|---|
| `node.deployed:v1` | New job deployed; triggers first status check |
| `check.k8s:v1` | Delayed re-check for still-running jobs |

Both streams carry: `task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `node_type` (plus `check_after` on `check.k8s:v1`). The Redis message ID (`msg.ID`) and stream name are used as the dedup key — no application-level `outbox_entry_id` field is required.

### HTTP (port 8085)

- `GET /health` — liveness probe only

## Outbound Interfaces

### Redis producers (via outbox)

| Stream | Trigger |
|---|---|
| `check.k8s:v1` | Job still running; re-enqueue with `check_after` delay |
| `retry.task:v1` | Job failed, retryable; `executor-controller` will re-deploy |
| `task.failed:v1` | Job failed permanently; currently has no in-repo consumer |
| `task.status.updated:v1` | Job terminal (SUCCEEDED or FAILED); consumed by `state` to update task status |
| `task.execution.recorded:v1` | Job terminal (SUCCEEDED or FAILED); consumed by `state` to persist execution record |
| `node.updated:v1` | Job terminal (SUCCEEDED or FAILED); consumed by `orchestrator` for topology projection |

### External systems

| System | Operation |
|---|---|
| Kubernetes API | `GetJobStatus` — read job/pod status and termination message |
| Kubernetes API | `GetPodLogs` — fetch full log + configurable tail (default configured) |
| S3 | `PutObject` — upload full pod log; key format: `logs/task-executions/{service_name}/{schema_name}/{table_name}/{execution_id}.log` |

`k8s-controller` no longer calls `state` gRPC. All state mutations flow via `task.status.updated:v1` and `task.execution.recorded:v1` Redis events.

### K8s client configuration

`NewK8sClient` uses `KUBECONFIG` when set (local / docker-compose). If `KUBECONFIG` is not set it falls back to `rest.InClusterConfig()` for pod deployments with a ServiceAccount.

## Processing Logic

### On `node.deployed:v1` or `check.k8s:v1`

```
1. GetJobStatus (K8s) — query current pod status; `started_at` uses a three-tier priority: job-level `StartTime` (persists after pod GC) → pod-level `StartTime` → container-level `terminated.StartedAt`
2. Determine retry_count from message payload (max_retries from config default = 2, meaning 3 total attempts: initial + 2 retries)
3. Begin Postgres transaction
4. Dedup: messageprocessing.Dedup(msg.ID, stream_name) — INSERT ON CONFLICT DO NOTHING in message_processing
   → if duplicate: skip, return nil (consumer ACKs)
5. Write outbox entries per outcome (see Outcome branches)
6. Commit (message_processing insert + outbox entries land atomically)
```

### Outcome branches

| Status | Action |
|---|---|
| **Running** | Write `check_delayed` outbox entry → re-enqueue to `check.k8s:v1` with `check_after` |
| **Failed, retryable** (`retry_count < max_retries`) | Fetch+upload logs (soft-fail) → entry A: publish `task.status.updated:v1` (FAILED) with incremented retry; entry B: publish `retry.task:v1` |
| **Failed, permanent** (`retry_count >= max_retries`) | Fetch+upload logs (soft-fail) → entry A: publish `task.status.updated:v1` (FAILED) + `task.execution.recorded:v1`; entry B: publish `task.failed:v1` + `node.updated:v1` FAILED |
| **Succeeded** | Entry A: publish `task.status.updated:v1` (SUCCEEDED) + `task.execution.recorded:v1`; entry B: publish `node.updated:v1` SUCCEEDED |
| **NotFound** (K8s API returns 404) | Mapped to `JobStatusFailed`; flows through the same Failed path as above (retryable or permanent) |

### Write-time fanout (D1 pattern)

Each handler outcome writes 1–3 canonical `k8s_outbox` rows inside a single transaction. The `pkg/outbox.Processor` polls the table and publishes each row to its `stream_name` via `k8s-controller/adapters/publisher.OutboxPublisher`, which routes the JSONB payload to the correct typed struct per `event_type`.

| Outcome | Rows written |
|---|---|
| Succeeded | `task_status_updated` (SUCCEEDED) + `task_execution_recorded` + `node_status_updated` |
| Failed, retryable | `task_status_updated` (FAILED) + `task_execution_recorded` + `task_retry` |
| Failed, permanent | `task_status_updated` (FAILED) + `task_execution_recorded` + `node_status_updated` |
| Running | `check_delayed` (1 row) |
| Unknown | `task_status_updated` (FAILED) + `task_failed` |

## S3 Log Behavior

- Full pod log uploaded on any failure (retryable or permanent)
- S3 upload is **soft-fail**: if upload fails, a warning is logged but processing continues with empty log key
- Error message stored in `task.execution.recorded:v1` payload is taken from log tail; falls back to K8s termination message
- `ErrorMessageMaxLen` config truncates the stored error message

## Redis Payload Reference

### `check.k8s:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `check_after`, `node_type`, `retry_count`, `max_retries`, `image_tag`

### `retry.task:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `retry_count`, `node_type`

### `task.failed:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `error_message`, `retry_count`

### `task.status.updated:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `status`, `retry_count`

### `task.execution.recorded:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `execution_id`, `started_at` (job-level → pod-level → container-level priority), `finished_at`, `status`, `error_message`, `s3_log_key`

### `node.updated:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `status`

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer (dual-stream) | Reads `node.deployed:v1` and `check.k8s:v1`; includes crash-recovery for pending messages |
| Delayed requeue | Holds `check.k8s:v1` messages until `check_after` time before dispatching to handler |
| Outbox processor (`pkg/outbox.Processor`) | Polls `k8s_outbox`; publishes each row to its `stream_name` via `OutboxPublisher` |
| Stuck entry resolver | Periodically finds `k8s_outbox` entries stuck in pending beyond `stuck_threshold_seconds`; force-marks them failed via `K8sOutboxStuckRepository`; tracks resolve attempts in memory; escalates with CRITICAL log if `max_resolve_attempts` exceeded |

## Reliability Patterns

- **Canonical outbox (D1 fanout)**: each business decision writes 1–3 `k8s_outbox` rows in a single transaction; the `pkg/outbox.Processor` publishes each row independently — no multi-effect fanout in the publisher
- **Inbound dedup inside transaction**: `message_processing` insert (keyed on `(message_id, stream_name)`) and outbox writes are in the same transaction; a crash after commit is idempotent on retry (dedup fires)
- **Explicit transaction boundary**: each status message gets its own transaction-scoped repositories for dedup and outbox writes; parallel `node.deployed:v1` and `check.k8s:v1` consumers can reuse one handler without sharing mutable transaction state
- **S3 soft-fail**: log upload failure does not block task completion; execution record is written with empty S3 key
- **No state gRPC dependency**: k8s-controller no longer calls state gRPC; all state mutations flow via `task.status.updated:v1` and `task.execution.recorded:v1`
- **Retry count in payload**: `retry_count` flows through Redis messages; `max_retries` uses the service config default (default = 2, meaning 3 total execution attempts: initial + 2 retries); permanent failure occurs when `retry_count >= max_retries`
- **Stuck resolver**: catches outbox entries that exceed `max_retries` but haven't been cleaned up; force-marks them as failed with a diagnostic message; escalates to CRITICAL log if auto-resolution fails after `max_resolve_attempts`
- **`task.failed:v1`**: currently has no in-repo consumer; exists for external observability or future integration
