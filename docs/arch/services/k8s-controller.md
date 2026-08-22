# k8s-controller

## Purpose

`k8s-controller` monitors running Kubernetes Jobs, decides their outcome, and routes results back into the pipeline.

It is responsible for:
- consuming deployment notifications and delayed re-checks from Redis
- querying K8s for actual job/pod status
- deciding: still running → re-check, failed → retry or permanent failure, succeeded → complete
- publishing `task.status.updated:v1` so `state` can update task status
- publishing `task.execution.recorded:v1` so `state` can persist execution records
- uploading full pod logs to S3 (and, for validation Jobs, the structured validation-result artifact)
- publishing terminal status updates to `orchestrator` (via `node.updated:v1`)

## Package Structure

Port ownership follows the repo-wide convention:

| Layer | Package | Contents |
|---|---|---|
| Domain repository port | `k8s-controller/domain/repository` | `CancelledSchedulesRepository` — records/queries cancelled schedule IDs |
| Application / technical ports | `k8s-controller/service/ports` | `LogUploader` — uploads log content to object storage |
| UnitOfWork interface | `k8s-controller/service/uow` | `UnitOfWork` — tx lifecycle + `OutboxRepo` + `MessageProcessingRepo` |
| Concrete implementations | `k8s-controller/adapters/postgres` | `PostgresUnitOfWork`, `CancelledSchedulesRepository` adapter |
| Concrete implementations | `k8s-controller/adapters/s3` | `LogUploader` adapter |

`service/handlers` imports no `adapters/*` package; every collaborator is reached through a port. This is enforced by `TestServiceHandlersDoNotImportAdapters` in `pkg/streams/handler_imports_test.go`.

## Owned Storage (Postgres: `continuo_k8s`)

| Table | Purpose |
|---|---|
| `k8s_outbox` | Canonical transactional outbox — each write-time side effect is a separate row with a JSONB payload; `pkg/outbox.Processor` polls and publishes to the typed Redis stream per row |
| `message_processing` | Per-stream inbound dedup table keyed on `(message_id, stream_name)`; `pkg/messageprocessing.Dedup` inserts atomically inside the same transaction as outbox writes |

All `k8s_outbox` rows conform to the canonical schema: `id`, `message_processing_id` (nullable), `aggregate_type`, `aggregate_id`, `event_type`, `payload` (JSONB), `stream_name`, `status`, `retry_count`, `max_retries`, `created_at`, `processed_at`, `error_message`, `next_attempt_at` (nullable; `NULL` means due now — see `docs/arch/05-error-classification.md` §Outbox processor resilience).

## Inbound Interfaces

### Redis consumers

All three streams are consumed via `pkg/redis.StreamConsumer` with per-stream parser + binding pairs.

| Stream | Consumer group | Binding pattern |
|---|---|---|
| `node.deployed:v1` | `K8sDeployed` | Full: parse → `UnitOfWork.Begin` → dedup via `pkg/messageprocessing.DedupWithOutboxEntryID` → `CheckStatusHandler.Handle` → commit |
| `check.k8s:v1` | `K8sCheckStatus` | Full (same as above). Every message is already due — the delay-queue promoter only XADDs due tickets (see Delay queue below) — so the binding has no delay gate and processes each message immediately |
| `schedule.cancelled:v1` | `K8sScheduleCancelled` | Lightweight: parse `schedule_id` → insert into `cancelled_schedules` guard table (idempotent); no dedup, no outbox |

Production and all candidate-mode Jobs (`mode=validation`, `mode=seed_build`, `mode=compile`) are observed over these same `node.deployed:v1` / `check.k8s:v1` consumers; there is no separate consumer group and no label-selector filter on the consumers themselves. Routing to the appropriate outbox row happens inside `CheckStatusHandler` by inspecting the live Job's labels (see Candidate-mode job routing).

`node.deployed:v1` and `check.k8s:v1` carry their event as a typed JSON `payload` field, decoded by the per-stream parsers into `pkg/events.NodeDeployed` and `pkg/events.CheckK8s` (`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `node_type`, `image_tag`, plus retry/max-retries). The task-level retry count is named `task_retry_count` on `node.deployed:v1` and `retry_count` on `check.k8s:v1`; both parsers map it onto `command.CheckJobStatus.RetryCount`. `check.k8s:v1` additionally carries `running_announced` — false on a fresh `node.deployed:v1` (a new attempt), set true once RUNNING has been announced, so the self-poll loop announces RUNNING exactly once per attempt without persistent state. Transport metadata travels as a flat `outbox_entry_id` sibling field on both streams, consumed by `DedupWithOutboxEntryID`; on `check.k8s:v1` the promoter copies it from the delay-queue ticket so a re-promoted check is deduped on it.

### Delay queue (Redis: `checkk8s:pending` ZSET + `checkk8s:tickets` HASH)

A not-yet-due status re-check waits in a Redis delay queue rather than being re-XADD'd onto `check.k8s:v1`, so the stream cannot grow unbounded. The queue is two durable keys, both keyed by `JobName` (a K8s Job's stable identity), kept in lockstep:

- `checkk8s:pending` — a ZSET acting as the clock: member = `JobName`, score = `check_after` (unix seconds). Because a member is keyed by value, re-scheduling a Job is an in-place score update — one entry per in-flight Job.
- `checkk8s:tickets` — a HASH holding each pending check's ticket: field = `JobName`, value = a JSON envelope `{entry_id, payload}` wrapping the typed `CheckK8s` payload with the source outbox row's ID.

`OutboxPublisher` writes a `check_delayed` row into this queue (HSET ticket + ZADD due time in one `MULTI`/`EXEC`). A background **promoter** ticks every second, running one atomic Lua script that moves every due ticket (score ≤ now, bounded per batch) into `check.k8s:v1`: it XADDs the ticket's `payload` plus a flat `outbox_entry_id` field, then `HDEL`/`ZREM`s the ticket. Running the whole script uninterrupted makes promotion exactly-once across replicas. Carrying `outbox_entry_id` onto the stream lets the consumer's secondary dedup key suppress a replay if an outbox publish reaches Redis but its Postgres transaction rolls back and the row is retried after its ticket was already promoted.

### HTTP (port 8085)

Three endpoints backed by the `pkg/liveness` registry, with readiness and
liveness deliberately split:

- `GET /ready` — readiness. Fails (503) when any registered worker (stream
  consumers, outbox processor, stuck-entry resolver) has exited with an error,
  any consumer read-loop heartbeat has gone stale, **or** any dependency probe
  (Redis, Postgres) fails. A dependency outage pulls the pod out of the
  Service endpoints so no traffic is routed to it.
- `GET /livez` — liveness. Fails (503) **only** for worker/heartbeat failures —
  dependency probes are excluded, so a Redis/Postgres outage does not restart a
  pod whose consumers are already retrying through it, while a dead or wedged
  consumer goroutine does.
- `GET /health` — plain process-up probe (always 200); retained for manual use.

The Kubernetes `readinessProbe` points at `/ready` and the `livenessProbe` at
`/livez` (`deploy/continuo/values.yaml`: `readinessPath: /ready`,
`livenessPath: /livez`).

### Graceful shutdown

On SIGTERM/SIGINT the lifecycle manager runs an ordered sequence whose total
duration is bounded by `SHUTDOWN_GRACE` (default 15s) for handlers that honor
their context — a `Close()`-style handler that ignores `ctx` is not itself
bounded by it: (1) stop intake by cancelling the root context so the stream
consumers (`node_deployed`, `check_k8s`, `schedule_cancelled`), the outbox
processor, the delay-queue promoter, and the stuck-entry resolver stop
reading new work and unwind their in-flight handler or batch — the handler's
next database call fails against the cancelled context, its transaction
rolls back, and the message is left un-ACKed for redelivery; (2) drain —
wait on a WaitGroup for those tracked goroutines to return, capped at half
the grace period; (3) close infra — run the registered shutdown handlers
(health HTTP server, Postgres, Redis) against a fresh live context with its
own deadline fixed at the moment `Shutdown` starts, while step 2 is capped
independently at half the grace budget; together they leave infra teardown
at least the other half of the budget, live, derived from
`context.Background()`, never the just-cancelled root context. `main` blocks
on the lifecycle completion channel, so there is no fixed sleep. The
cancelled-schedules TTL sweeper and the health server itself are not tracked
goroutines: the sweeper's ticker loop returns instantly on cancellation with
no in-flight work to drain, and the health server blocks in `ListenAndServe`
until its own shutdown handler calls `Shutdown` in step 3, so tracking it
would deadlock the drain against the very step that stops it.

The `check_k8s` consumer runs as a tracked goroutine like every other
consumer rather than as the process's main blocking loop, and two separate
mechanisms catch its two failure modes. A permanent bootstrap failure calls
`WorkerExited("check_k8s", err)`, which flips `/livez` to 503 directly. A
wedged read loop — which logs and retries forever rather than returning —
is instead caught by the heartbeat probe registered alongside it, which
trips `/livez` once the consumer has gone too long without polling. Either
way Kubernetes restarts the pod through the liveness probe rather than the
process exiting itself. This trades an immediate exit for a bounded restart
delay (up to `periodSeconds` × `failureThreshold` on the liveness probe) in
exchange for every consumer failure going through the same graceful-shutdown
sequence — Postgres and Redis connections close and
the health server drains instead of being dropped mid-flight.

## Outbound Interfaces

### Redis producers (via outbox)

| Stream | Trigger |
|---|---|
| `check.k8s:v1` | Job still running; the `check_delayed` outbox row enqueues a delay-queue ticket, which the promoter moves onto this stream once due (see Delay queue) |
| `retry.task:v1` | Job failed, retryable; `executor-controller` will re-deploy |
| `task.failed:v1` | Job status Unknown (treated as a permanent failure); currently has no in-repo consumer |
| `task.status.updated:v1` | Job running (first observation) and terminal (SUCCEEDED/FAILED); consumed by `state` to update task status. k8s-controller is the sole producer of the running/terminal pod lifecycle. |
| `task.execution.recorded:v1` | Job terminal (SUCCEEDED or FAILED); consumed by `state` to persist execution record |
| `node.updated:v1` | Job terminal (SUCCEEDED or FAILED); consumed by `orchestrator` for topology projection |
| `validation.node.completed:v1` | `mode=validation` Job terminal (SUCCEEDED or FAILED); single per-node result consumed by `executor-controller` instead of the three production task-status rows |
| `seed.build.node.completed:v1` | `mode=seed_build` Job terminal (SUCCEEDED or FAILED); consumed by `executor-controller` instead of the production task-status rows |
| `compile.node.completed:v1` | `mode=compile` Job terminal (SUCCEEDED or FAILED); consumed by `executor-controller` instead of the production task-status rows |

### External systems

| System | Operation |
|---|---|
| Kubernetes API | `GetJobStatus` — read job/pod status; the main container's termination message and exit code; the name of the first container (init or main) that terminated non-zero (`FailedContainer`, drives compile-leg failure attribution); and, per terminated init container, its termination message (`InitTerminationMessages`, carries the `hydrate-parse-cache` initContainer's `"hydrated"` / `"degraded:<reason>"` outcome) |
| Kubernetes API | `GetPodLogs` — fetch full log + configurable tail (default configured) |
| S3 | `PutObject` — upload full pod log; key format: `logs/task-executions/{artifact_path}/{execution_id}.log`. `artifact_path` identifies which Job produced the object and takes one of two shapes: a Job that ran a node is addressed by that node, `{service_name}/{schema_name}/{table_name}`; a release's compile Job runs no node and carries neither schema nor table, so it is addressed by its service and leg, `{service_name}/compile`. Addressing compile as a node would leave both segments empty (`{service_name}///{execution_id}.log`), which MinIO rejects as an invalid object name while AWS S3 accepts it — losing the compile log only on installs backed by the bundled MinIO, including the log of a *failed* compile that a rejected release needs to explain itself. Uploaded for every terminal Job, succeeded as well as failed: the pod is garbage-collected at the Job's TTL, so an unuploaded log reduces a finished run to timings with no evidence. When the pod emitted a structured result block, a second `PutObject` uploads that JSON under the parallel key `run-results/task-executions/{artifact_path}/{execution_id}.json`. Both uploads soft-fail independently to an empty key, so S3 being unavailable never changes a Job's recorded outcome. Operators should set an S3 lifecycle policy on the `logs/` prefix — volume scales with DAG width × run frequency. |

`k8s-controller` does not call `state` gRPC. All state mutations flow via `task.status.updated:v1` and `task.execution.recorded:v1` Redis events.

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
| **Running** | The first time an attempt is observed running (`running_announced=false`), announce `task.status.updated:v1` (RUNNING) stamped with the running attempt — suppressed for candidate-mode Jobs (`mode=validation`, `mode=seed_build`, `mode=compile`). Always write a `check_delayed` outbox entry → enqueue a delay-queue ticket with `check_after` and `running_announced=true` (the promoter later moves it to `check.k8s:v1`), so RUNNING is announced exactly once per attempt. k8s-controller is the sole producer of the running/terminal pod lifecycle. |
| **Failed, retryable** (`retry_count < max_retries`) | Fetch+upload logs (soft-fail) → entry A: publish `task.status.updated:v1` (FAILED) stamping the attempt that ran; entry B: publish `retry.task:v1` for the next attempt (`retry_count + 1`) |
| **Failed, permanent** (`retry_count >= max_retries`) | Fetch+upload logs (soft-fail) → publish `task.status.updated:v1` (FAILED) + `task.execution.recorded:v1` + `node.updated:v1` FAILED |
| **Succeeded** | Entry A: publish `task.status.updated:v1` (SUCCEEDED) + `task.execution.recorded:v1`; entry B: publish `node.updated:v1` SUCCEEDED |
| **NotFound** (K8s API returns 404) | Mapped to `JobStatusFailed`; flows through the same Failed path as above (retryable or permanent) |

### Write-time fanout (D1 pattern)

Each handler outcome writes 1–3 canonical `k8s_outbox` rows inside a single transaction. The `pkg/outbox.Processor` polls the table and publishes each row to its `stream_name` via `k8s-controller/adapters/publisher.OutboxPublisher`, which routes the JSONB payload to the correct typed struct per `event_type`.

#### Stream trimming

k8s-controller writes to its Redis streams on two paths, and **both** cap the stream at `MaxLen 10000` (approximate, `~`), bounding Redis memory for streams a consumer group may lag on:

- **`OutboxPublisher`** — every published `k8s_outbox` row (all producer streams above).
- **Delay-queue promoter** — the XADD onto `check.k8s:v1` when a ticket becomes due.

**Caveat:** approximate trimming can drop the oldest entries before a lagging consumer group reads them; 10000 is the accepted bound, matching state's and the orchestrator's publishers. A stream ACK does not delete the entry, so without a cap a busy stream can grow until it OOM-kills Redis; the cap and the delay queue (which keeps not-yet-due checks off the stream) together bound that growth.

| Outcome | Rows written |
|---|---|
| Succeeded | `task_status_updated` (SUCCEEDED) + `task_execution_recorded` + `node_status_updated` |
| Failed, retryable | `task_status_updated` (FAILED) + `task_execution_recorded` + `task_retry` |
| Failed, permanent | `task_status_updated` (FAILED) + `task_execution_recorded` + `node_status_updated` |
| Running (first observation, production) | `task_status_updated` (RUNNING) + `check_delayed` |
| Running (already announced, or candidate mode) | `check_delayed` (1 row) |
| Unknown | `task_status_updated` (FAILED) + `task_failed` |

For candidate-mode Jobs (`mode=validation`, `mode=seed_build`, `mode=compile`), the Succeeded/Failed rows above do not apply. Any terminal candidate-mode Job writes exactly one `*_node_completed` outbox row (see Candidate-mode job routing); no `task_status_updated`, `task_execution_recorded`, or `node_status_updated` rows are written.

### Candidate-mode job routing

`CheckStatusHandler` fetches the live Job's labels and annotations and routes on the `mode` label. Jobs carrying `mode=validation`, `mode=seed_build`, or `mode=compile` (all created by `executor-controller`'s candidate dispatch paths) bypass the production outcome branches. The `mode` label is read from the live Job at handle time; the same consumers, parsers, and consumer groups serve both production and candidate-mode Jobs — only the label inspection determines which outbox row is written.

All three candidate modes share the same non-terminal behaviour:

- **Running** — the handler writes one `check_delayed` row (enqueued in the delay queue, promoted to `check.k8s:v1` when due) with a `check_after` delay and **suppresses** the RUNNING `task_status_updated` announcement (candidate Jobs have no real task/schedule). `running_announced=true` is set on the forward ticket so the mode is not re-read wastefully every poll; on the next check the Job's `mode` is re-read and routing recurs. Candidate Jobs are always Running on the first check fired by the `node.deployed:v1` trigger, so this re-poll carries them to a terminal status.
- **Unknown** — not treated as a permanent failure; the handler re-polls via the same `check.k8s:v1` ticket. (Production treats Unknown as a permanent failure — `task_status_updated` (FAILED) + `task_failed`.)

On terminal (SUCCEEDED or FAILED) each mode routes to its own handler and writes exactly ONE outbox row; no `task_status_updated`, `task_execution_recorded`, or `node_status_updated` is written for any candidate-mode Job:

| `mode` label | Handler | Outbox row type | Stream |
|---|---|---|---|
| `validation` | `handleValidationTerminal` | `validation_node_completed` | `validation.node.completed:v1` |
| `seed_build` | `handleSeedBuildTerminal` | `seed_build_node_completed` | `seed.build.node.completed:v1` |
| `compile` | `handleCompileTerminal` | `compile_node_completed` | `compile.node.completed:v1` |

All three outbox rows carry `{release_id, node_id, outcome}` as their core payload; `outcome` is `ok` when the Job succeeded, else `failed`. `release_id` and `node_id` are read from the Job **annotations** (`continuo.dev/release-id`, `continuo.dev/node-id`), not the labels. The executor stamps the raw, unmodified dbt `unique_id`/release id in those annotations; the matching labels are sanitized for K8s (out-of-charset characters replaced, truncated to 63 chars) and serve only routing/selection. Reading the raw annotation values keeps the payload lossless, so `executor-controller`'s outcome lookup (keyed on the unmodified `executor_deployments` row) matches even when the sanitized label would differ.

For `mode=validation` the handler additionally uploads pod logs (soft-fail) and extracts a structured result block if present. The validation pod prints a result block (`===CONTINUO_VALIDATION_RESULT_BEGIN===` … `===CONTINUO_VALIDATION_RESULT_END===`) as its last stdout; the handler splits it out, uploads it under the `run-results/` key, strips it from the text log before uploading that, and sets `run_results_uri` on the row (omitted when no block is present — old image or non-validation Job).

For `mode=compile`, `handleCompileTerminal` additionally adds `failed_container` to the payload when the terminal `K8sPodResult` carries one — the name of the first pod container (init or main) that terminated non-zero. The key is omitted entirely on a successful compile. `executor-controller` reads it to attribute a compile failure to a specific stage (`compile` / `parse-prod` / `parse-candidate` / `upload`).

Each completed row's `aggregate_id` is a deterministic UUIDv5 over an immutable namespace and `release:<release_id>`, so a re-observed terminal Job maps to the same aggregate for downstream dedup.

## S3 Log Behavior

- Full pod log uploaded on any failure (retryable or permanent)
- S3 upload is **soft-fail**: if upload fails, a warning is logged but processing continues with empty log key
- Error message stored in `task.execution.recorded:v1` payload prefers the structured result block's `message` — parsed from the full log, or from the tail when the full log carried no block (the two are fetched independently and either can come back empty); the block is accepted only when its `schema_version` and `status` match the contract, and its `status` is not `success`. Falls back to the raw log tail, then to the K8s termination message, when no valid block is found either way.
- `ErrorMessageMaxLen` config truncates the stored error message

## Redis Payload Reference

### `check.k8s:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `node_type`, `retry_count`, `max_retries`, `image_tag`, `operation` (omitted when empty), `running_announced`. The delay (`check_after`) is not a stream field — it is the ZSET score on the delay queue; `outbox_entry_id` rides as a flat sibling field for dedup.

### `retry.task:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `image_tag`, `task_retry_count`, `max_retries`, `node_type`, `operation` (omitted when empty)

`operation` is the dbt verb the Job runs (e.g. `test`, `build`). It is sourced from durable check/retry data, not from Job metadata: it arrives on `node.deployed:v1`, is held on `CheckJobStatus.Operation`, rides the delay-queue ticket onto every `check.k8s:v1` self-poll, and is copied onto `retry.task:v1`. Sourcing it from the Job's labels would be unsafe because a TTL-reaped ("vanished") Job returns empty labels from `GetJobMeta`, which would silently rebuild a `dbt test`/`dbt build` Job as `dbt run`; carrying it in the durable payload keeps a retried `dbt test` or `dbt build` Job the same verb. Normal production runs have an empty `operation`, so the field is omitted and the wire format is unchanged for them.

### `task.failed:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `error_message`, `retry_count`

### `task.status.updated:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `status`, `retry_count`

### `task.execution.recorded:v1`
`execution_id`, `task_id`, `job_name`, `execution_seconds`, `started_at` (job-level → pod-level → container-level priority; omitted if unset), `completed_at` (omitted if unset), `error_message` (omitted if empty), `log_s3_key` (omitted if empty), `run_results_uri` (omitted if empty; the S3 key of the structured result block the pod printed — python-family containers (`python-model`, `python-csv`) always print one, dbt containers never do, so the field is absent from every dbt payload), `parse_cache` (omitted if empty; one of `hydrated` / `degraded` / `unknown`, derived from the `hydrate-parse-cache` initContainer's termination message via `parseCacheFromResult` — absent entirely for Jobs with no such initContainer, e.g. `S3_BUCKET` unset), `parse_cache_reason` (omitted if empty; the degrade reason, set only when `parse_cache=degraded`)

### `node.updated:v1`
`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `status`

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer (three-stream) | Reads `node.deployed:v1`, `check.k8s:v1`, and `schedule.cancelled:v1` via `pkg/redis.StreamConsumer`; periodic XAUTOCLAIM sweep reclaims pending entries after `MinIdle` threshold for crash recovery |
| Delay-queue promoter | Ticks every second; one atomic Lua script moves every due ticket (score ≤ now) from the `checkk8s:pending` ZSET into `check.k8s:v1` (see Delay queue). A missed tick loses nothing — the ZSET is durable and the next tick promotes the backlog |
| Outbox processor (`pkg/outbox.Processor`) | Polls `k8s_outbox`; publishes each row to its `stream_name` via `OutboxPublisher` |
| Stuck entry resolver | Periodically finds `k8s_outbox` entries stuck in pending beyond `stuck_threshold_seconds`; force-marks them failed via `K8sOutboxStuckRepository`; tracks resolve attempts in memory; escalates with CRITICAL log if `max_resolve_attempts` exceeded |

## Reliability Patterns

- **Canonical outbox (D1 fanout)**: each business decision writes 1–3 `k8s_outbox` rows in a single transaction; the `pkg/outbox.Processor` publishes each row independently — no multi-effect fanout in the publisher
- **Inbound dedup inside transaction**: `message_processing` insert (keyed on `(message_id, stream_name)`) and outbox writes are in the same transaction; a crash after commit is idempotent on retry (dedup fires)
- **Explicit transaction boundary via `UnitOfWork`**: each binding calls `UnitOfWork.Begin` before the handler and `Commit/Rollback` after; dedup and outbox writes land in the same Postgres transaction; parallel consumers for `node.deployed:v1` and `check.k8s:v1` share the handler without shared mutable state
- **S3 soft-fail**: log upload failure does not block task completion; execution record is written with empty S3 key
- **No state gRPC dependency**: all state mutations flow via `task.status.updated:v1` and `task.execution.recorded:v1` Redis events
- **Retry count in payload**: `retry_count` flows through Redis messages; `max_retries` uses the service config default (default = 2, meaning 3 total execution attempts: initial + 2 retries); permanent failure occurs when `retry_count >= max_retries`
- **Stuck resolver**: catches outbox entries that exceed `max_retries` but haven't been cleaned up; force-marks them as failed with a diagnostic message; escalates to CRITICAL log if auto-resolution fails after `max_resolve_attempts`
- **`task.failed:v1`**: currently has no in-repo consumer; exists for external observability or future integration
