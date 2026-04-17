# k8s-controller

## Purpose

`k8s-controller` monitors running Kubernetes Jobs, decides their outcome, and routes results back into the pipeline.

It is responsible for:
- consuming deployment notifications and delayed re-checks from Redis
- querying K8s for actual job/pod status
- deciding: still running → re-check, failed → retry or permanent failure, succeeded → complete
- recording task execution history in `state`
- uploading full pod logs to S3
- publishing terminal status updates to `orchestrator` (via `node.updated:v1`)

## Owned Storage (Postgres: `continuo_k8s`)

| Table | Purpose |
|---|---|
| `k8s_status_outbox` | Stages all side effects (state updates, execution records, Redis publishes) atomically before execution |
| `processed_events` | Inbound dedup: keyed by upstream `outbox_entry_id` (INSERT ON CONFLICT DO NOTHING, inside transaction) |

## Inbound Interfaces

### Redis consumers

| Stream | Description |
|---|---|
| `node.deployed:v1` | New job deployed; triggers first status check |
| `check.k8s:v1` | Delayed re-check for still-running jobs |

Both streams carry: `outbox_entry_id`, `task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `node_type` (plus `check_after` on `check.k8s:v1`).

### HTTP (port 8085)

- `GET /health` — liveness probe only

## Outbound Interfaces

### Redis producers

| Stream | Trigger |
|---|---|
| `check.k8s:v1` | Job still running; re-enqueue with `check_after` delay |
| `retry.task:v1` | Job failed, retryable; `executor-controller` will re-deploy |
| `task.failed:v1` | Job failed permanently; currently has no in-repo consumer |
| `node.updated:v1` | Job terminal (SUCCEEDED or FAILED); consumed by `orchestrator` |

### gRPC to `state`

| Method | When |
|---|---|
| `GetTask` (by task_id UUID) | At check time, to read `retry_count` and `max_retries` |
| `UpdateTask` (status + retry_count) | Via outbox processor: after K8s outcome is known |
| `CreateTaskExecution` | Via outbox processor: records execution attempt with timing and S3 log key |

### External systems

| System | Operation |
|---|---|
| Kubernetes API | `GetJobStatus` — read job/pod status and termination message |
| Kubernetes API | `GetPodLogs` — fetch full log + configurable tail (default configured) |
| S3 | `PutObject` — upload full pod log; key format: `logs/task-executions/{service_name}/{schema_name}/{table_name}/{execution_id}.log` |

### K8s client configuration

`NewK8sClient` uses `KUBECONFIG` when set (local / docker-compose). If `KUBECONFIG` is not set it falls back to `rest.InClusterConfig()` for pod deployments with a ServiceAccount.

## Processing Logic

### On `node.deployed:v1` or `check.k8s:v1`

```
1. GetJobStatus (K8s) — query current pod status
2. GetTask (state gRPC) — read retry_count, max_retries
3. Begin Postgres transaction
4. Dedup: TryMarkProcessed(outbox_entry_id) — INSERT ON CONFLICT DO NOTHING
   → if duplicate: rollback + XACK
5. Write outbox entries (two entries per outcome):
   - Entry A: internal task update (UpdateTask + CreateTaskExecution)
   - Entry B: external Redis notification (node.updated:v1 or check.k8s:v1 etc.)
6. Commit (outbox entries + processed_events land atomically)
```

### Outcome branches

| Status | Action |
|---|---|
| **Running** | Write `check_delayed` outbox entry → re-enqueue to `check.k8s:v1` with `check_after` |
| **Failed, retryable** (`retry_count < max_retries`) | Fetch+upload logs (soft-fail) → entry A: update task status=failed, increment retry; entry B: publish `retry.task:v1` |
| **Failed, permanent** (`retry_count >= max_retries`) | Fetch+upload logs (soft-fail) → entry A: update task status=failed; entry B: publish `task.failed:v1` + `node.updated:v1` FAILED |
| **Succeeded** | Entry A: update task status=succeeded, create execution record; entry B: publish `node.updated:v1` SUCCEEDED |
| **Unknown** | Treated as transient; entry written to stage further investigation |

### Outbox processor

Reads `k8s_status_outbox` entries and executes the staged side effects:
- Calls `UpdateTask` in state via gRPC
- Calls `CreateTaskExecution` in state via gRPC
- Publishes to the configured Redis stream (if `stream_name` is non-empty)

## S3 Log Behavior

- Full pod log uploaded on any failure (retryable or permanent)
- S3 upload is **soft-fail**: if upload fails, a warning is logged but processing continues with empty log key
- Error message stored in task execution is taken from log tail; falls back to K8s termination message
- `ErrorMessageMaxLen` config truncates the stored error message

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer (dual-stream) | Reads `node.deployed:v1` and `check.k8s:v1`; includes crash-recovery for pending messages |
| Delayed requeue | Holds `check.k8s:v1` messages until `check_after` time before dispatching to handler |
| Outbox processor | Polls `k8s_status_outbox`; executes staged state updates, executions, and Redis publishes |
| Stuck entry resolver | Periodically finds `k8s_status_outbox` entries stuck in pending beyond `stuck_threshold_seconds`; force-marks them failed; tracks resolve attempts in memory; escalates with CRITICAL log if `max_resolve_attempts` exceeded |

## Redis Payload Reference

### `check.k8s:v1`
`outbox_entry_id`, `task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `check_after`, `node_type`

### `retry.task:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `retry_count`, `node_type`

### `task.failed:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `error_message`, `retry_count`

### `node.updated:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `status`

## Reliability Patterns

- **Two-entry outbox pattern**: internal side effects (state writes) and external notifications (Redis) are staged as separate outbox entries within the same transaction — both commit or neither does
- **Inbound dedup inside transaction**: `processed_events` insert and outbox writes are in the same transaction; a crash after commit is idempotent on retry (dedup fires)
- **S3 soft-fail**: log upload failure does not block task completion; execution record is written with empty S3 key
- **Stuck resolver**: catches outbox entries that exceed `max_retries` but haven't been cleaned up; force-marks them as failed with a diagnostic message; escalates to CRITICAL log if auto-resolution fails after `max_resolve_attempts`
- **`task.failed:v1`**: currently has no in-repo consumer; exists for external observability or future integration
