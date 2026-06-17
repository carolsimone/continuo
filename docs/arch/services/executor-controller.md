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
| `executor_deployments` | K8s-deploy command queue. Handlers write a row here inside their Unit-of-Work transaction (pure Postgres write, no Kubernetes I/O). `deployer.Dispatcher` drains due rows, calls `CreateQueryJob`, and on success writes canonical announcement rows to `executor_outbox`. A `mode` column distinguishes `production` rows (the default query.model path) from `validation` rows (candidate-release validation checks, which carry `release_id`/`node_id`, `upstream_node_ids` (in-set gating edges, covering both intra- and cross-service upstreams), and a per-node terminal `outcome`). Validation rows start `pending` (no in-set upstreams) or `blocked` (has in-set upstreams that are not yet `ok`). |
| `executor_outbox` | Canonical transactional outbox — one row per pending Redis announcement (`task_status_updated` FAILED on the never-deployed path, plus `node_deployed`/`node_updated`); `pkg/outbox.Processor` polls and performs the Redis XADD per row. The deploy-success path no longer announces RUNNING — k8s-controller owns the running/terminal pod lifecycle. |
| `message_processing` | Inbound dedup: keyed on `(message_id, stream_name)`; prevents double-processing of duplicate Redis messages |
| `cancelled_schedules` | Records schedule cancellations; consulted by deploy handlers before writing to `executor_deployments` |
| `validation_aggregates` | Per-release sentinel (`release_id` PK). `ClaimEmission` does an `INSERT … ON CONFLICT DO NOTHING` so exactly one caller wins the right to emit the aggregate validation-completed announcement when the final per-node outcomes land concurrently. `LockRelease` takes a transaction-scoped advisory lock (`pg_advisory_xact_lock(hashtext(release_id))`) that serializes the whole count→claim→emit gate per release, so concurrent last-node terminals cannot both no-op (lost emission) under READ COMMITTED. |

`executor_deployments` schema: `id`, `message_processing_id` (nullable FK), `task_id`, `schedule_id`, `job_params` (JSONB), `status` (`pending` / `blocked` / `deployed` / `failed`), `retry_count`, `max_retries`, `next_attempt_at`, `created_at`, `deployed_at`, `error_message`, plus the validation-mode columns `mode` (`production` / `validation`), `release_id`, `node_id`, `outcome` (`ok` / `failed` / `skipped`), `dbt_log_uri`, `outcome_at`. Production rows leave the validation columns NULL. Validation rows have no real task/schedule identity, so the `NOT NULL` `task_id`/`schedule_id` columns are filled with deterministic UUIDv5 values derived from an immutable namespace over `(release_id, node_id)`; a partial unique index on `(release_id, node_id) WHERE mode='validation'` enforces one validation row per node. Migration V16 relaxed the status and outcome CHECK constraints to accommodate the new values. `blocked` is non-terminal in the aggregate gate (the release cannot complete while any node is `blocked`). `skipped` is a terminal non-`ok` outcome (a node that could not run because an upstream failed; its presence fails the release).

`executor_outbox` rows conform to the canonical schema: `id`, `message_processing_id` (nullable), `aggregate_type`, `aggregate_id`, `event_type`, `payload` (JSONB), `stream_name`, `status`, `retry_count`, `max_retries`, `created_at`, `processed_at`, `error_message`.

## Inbound Interfaces

### Redis consumers

| Stream | Consumer group | Description |
|---|---|---|
| `query.model:v1` | `executor-query-model` | Primary dispatch: new node ready for execution |
| `retry.task:v1` | `executor-retry` | Retry dispatch: re-attempt a failed node |
| `schedule.cancelled:v1` | `executor-schedule-cancelled` | Schedule cancellation: suppress future deployments for the schedule |
| `validation.requested:v1` | `executor-validation-requested` | Candidate-release validation request: enqueue one `mode=validation` deployment per node |
| `validation.node.completed:v1` | `executor-validation-node-completed` | Per-node validation Job terminal status from k8s-controller; records the node outcome, unblocks or skips in-set downstreams, and runs the per-release aggregate-emit gate |
| `validation.completed:v1` | `executor-validation-completed` | Per-release validation aggregate emitted by executor-controller itself; consumed by a second consumer in the same process to drop the `_candidate_<release>` schema from the dbt warehouse via `CandidateSchemaCleaner` |

`query.model:v1` and `retry.task:v1` carry the same fields:
- `task_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema_name`, `table_name`, `job_name`
- `node_type`

### Inbound message processing

executor-controller consumes the streams above via `pkg/redis.StreamConsumer`. For each stream the wire path is:

`pkg/redis.StreamConsumer` → `adapters/redis/<stream>_binding.go` → `service/handlers/<stream>_handler.go`

The binding parses the XMessage into a typed `domain/events.<Event>`, runs `pkg/messageprocessing.Dedup` against the per-service `message_processing` table (keyed on `(message_id, stream_name)`), and invokes the handler inside a single Unit-of-Work transaction. `schedule.cancelled:v1` skips dedup because `cancelled_schedules.Insert` is `INSERT ... ON CONFLICT DO NOTHING` and is naturally idempotent.

The cancelled-schedule guard runs inside `QueryModelHandler` and `RetryTaskHandler` via `uow.CancelledSchedulesRepo().Exists`; a cancelled match commits the dedup row (so the message is ACKed and never reprocessed) and returns without writing to `executor_deployments`.

`validation.requested:v1` carries every node for a release in one message (flat JSON `payload` field). Each node entry includes `upstream_node_ids` — the in-set upstreams (intra- and cross-service) that must complete before this node can be dispatched. The handler writes one `executor_deployments` row per node: `blocked` if `upstream_node_ids` is non-empty, `pending` otherwise. Its binding deduplicates per-release on a deterministic release-derived `outbox_entry_id` rather than the inbound `msg.ID`, so a redelivery with a fresh Redis ID still collides on the same key.

`validation.node.completed:v1` carries one node's terminal result as a flat JSON `payload` field (`release_id`, `node_id`, `outcome` ∈ {`ok`,`failed`}, optional `dbt_log_uri`) with `outbox_entry_id` as a flat sibling. Its binding uses STANDARD `(message_id, stream_name)` dedup with an `outbox_entry_id` fallback, because it carries a normal upstream outbox row id from k8s-controller. The `ValidationNodeCompletedHandler` looks up the `(release_id, node_id)` validation deployment, attaches the outcome via `RecordOutcome`, saves, then propagates topological state: on `ok`, any `blocked` downstream whose every in-set upstream is now `ok` transitions to `pending` (unblocked for dispatch); on `failed`, every transitively `blocked` downstream is marked `skipped` (terminal, non-`ok`). The handler then runs the aggregate-emit gate. An unknown `(release_id, node_id)` (no matching row) is logged and ACKed; a redelivery whose deployment already carries an outcome is a no-op ACK (no double-record, no duplicate aggregate).

`validation.completed:v1` is both produced and consumed by executor-controller (via the `executor-validation-completed` consumer group). The teardown consumer calls `CandidateSchemaCleaner.DropCandidateSchema(candidate_schema)`, which issues a `DROP SCHEMA … CASCADE` against the dbt warehouse (`DBT_POSTGRES_DB`). This removes the shared `_candidate_<release>` schema regardless of whether validation passed or failed.

### HTTP (port 8084)

- `GET /health` — liveness probe only

## Outbound Interfaces

### Redis producers (via outbox)

`deployer.Dispatcher` writes canonical announcement rows to `executor_outbox` as part of each deploy cycle (never at inbound-message time). `pkg/outbox.Processor` then drains those rows via the executor `OutboxPublisher`, which is a uniform marshal-and-XADD — one Redis XADD per row, no K8s I/O and no executor-specific fanout logic in the publisher.

On a successful deploy, the dispatcher writes two outbox rows in one transaction:
1. `task_status_updated` (RUNNING) → XADD `task.status.updated:v1`
2. `node_deployed` → XADD `node.deployed:v1`

On terminal failure (permanent error or retry-budget exhaustion), the dispatcher writes two outbox rows:
1. `task_status_updated` (FAILED) → XADD `task.status.updated:v1`
2. `node_updated` (FAILED) → XADD `node.updated:v1`

| Stream | Description |
|---|---|
| `task.status.updated:v1` | Published after K8s job creation succeeds with `status=RUNNING`; also published with `status=FAILED` on terminal dispatch failure |
| `node.deployed:v1` | Published after K8s job creation succeeds (both production and validation Jobs); triggers `k8s-controller` monitoring. For validation Jobs the `task_id`/`schedule_id` are the deterministic synthetic UUIDs derived from `(release_id, node_id)`; they are inert carriers because k8s-controller routes the validation Job's status by its `mode=validation` label, not by these IDs |
| `node.updated:v1` | Published on terminal dispatch failure only; consumed by `orchestrator` to advance the schedule |
| `validation.completed:v1` | Per-release validation aggregate; emitted exactly once when every `mode=validation` node for a release is terminal. Payload: `release_id`, `candidate_schema` (the `_candidate_<release>` schema name, for teardown), `aggregate_status` (`ok` iff every node is `ok`, else `failed`), and `per_node_results[]` (`node_id`, `status`, optional `dbt_log_uri`) |

`task.status.updated:v1` payload fields:
- `task_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema_name`, `table_name`
- `status` — `RUNNING` on success, `FAILED` on terminal failure

`node.deployed:v1` is emitted as a typed JSON `payload` field (`pkg/events.NodeDeployed`), with `outbox_entry_id` as a flat sibling field for consumer-side dedup. Payload fields:
- `task_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema_name`, `table_name`, `job_name`
- `node_type`, `image_tag`
- `task_retry_count`, `max_retries`

### Kubernetes API

- `CreateQueryJob` — creates a K8s batch Job in the configured namespace with label `app=dbt-job`; container name is `dbt-job`; treated as idempotent (already-exists is not an error on retry)
- `CreateValidationJob` — creates a `mode=validation` K8s batch Job in the configured namespace (idempotent by job name, `BackoffLimit` 0, `RestartPolicy` Never). Labels: `app=dbt-job` (so existing watchers stay correct) plus `mode=validation` and `release-id`/`node-id` (sanitised to valid label values) for selection/observability; the `mode` label is what k8s-controller routes on. The raw, unmodified release/node identity is stamped separately as annotations `continuo.dev/release-id` and `continuo.dev/node-id` (allowing arbitrary values); k8s-controller reads these — not the sanitised labels — into the `validation.node.completed:v1` payload so the outcome lookup keyed on the unmodified `executor_deployments` row matches even when a label would be altered. The container command is determined by `ValidationCommand(nodeType, tableName)`: for **model and snapshot** nodes it is `python /validation_runner.py`, which connects to the dbt warehouse via `psycopg2`, issues `CREATE SCHEMA IF NOT EXISTS <candidate>`, then `CREATE TABLE <candidate>.<table> AS (<CANDIDATE_SQL>) WITH NO DATA`; for **seed** nodes it is `dbt seed --select <table> --empty`, which builds an empty seed table from the CSV column definitions. Topological gating ensures every in-set upstream (intra- and cross-service) has been built as an empty table in the shared `_candidate_<release>` schema before any dependent node runs; because `CANDIDATE_SQL` already has every known-node schema ref rewritten to the candidate schema, no model edits and no dbt recompile are needed. Env vars set on every validation pod: `DBT_TARGET_SCHEMA` (= candidate schema, consumed by the `generate_schema_name` macro in `dbt-base` for the seed path's output schema), `CANDIDATE_SQL` (the rewritten SQL for model/snapshot pods; empty for seeds), `TABLE_NAME`, `RELEASE_ID`, `NODE_ID`, plus `DBT_POSTGRES_*` connection forwarding. A missing `image_tag` is a permanent error.

### K8s client configuration

`NewK8sClient` uses `KUBECONFIG` when set (local / docker-compose). If `KUBECONFIG` is not set it falls back to `rest.InClusterConfig()` for pod deployments with a ServiceAccount.

## Processing Logic

### On `query.model:v1` or `retry.task:v1`

```
1. Dedup check via pkg/messageprocessing against message_processing (message_id, stream_name)
   → if already present: skip (ACK without processing)
2. Check cancelled_schedules for the schedule_id
   → if cancelled: commit dedup row and return (no deployment row written)
3. Write deploy intent to executor_deployments (status=pending)
4. Commit (dedup row + deployment row in one transaction)
```

### Deploy dispatcher (every 5 seconds)

`deployer.Dispatcher` polls `executor_deployments` for due rows, capped by active K8s Jobs:

```
1. CountActiveJobs — label selector app=dbt-job, .status.active > 0
   headroom = max(0, MAX_CONCURRENT_JOBS - active)
   if headroom == 0: return (pending rows stay pending until next tick)

2. GetDueBatch(headroom) — rows WHERE status='pending' AND next_attempt_at <= NOW()
   up to min(headroom, batchSize) rows

For each due row (inside one transaction for the batch):
  a. Unmarshal job_params into DeployJob
     → unmarshal failure or invalid fields: writeFailed → write FAILED outbox rows, MarkFailed
  b. CreateQueryJob (K8s) — idempotent by job name
     → success: writeDeployed
       - write node_deployed outbox row (k8s-controller announces RUNNING on first observed run)
       - MarkDeployed
     → transient error AND retry budget remains:
       - Reschedule with exponential backoff (base 5s, cap 2m) via next_attempt_at
     → permanent error (errors.Is ErrPermanent) OR retry budget exhausted:
       - writeFailed: write task_status_updated (FAILED) + node_updated (FAILED) outbox rows, MarkFailed
```

The dispatcher only dispatches `pending` validation rows; `blocked` rows (with unresolved in-set upstreams) remain in place until the `ValidationNodeCompletedHandler` transitions them to `pending`. On a successful K8s validation Job creation, the dispatcher `MarkDeployed`s the row and writes a single `node_deployed` → `node.deployed:v1` outbox row so k8s-controller status-checks the Job — it never polls, so without this trigger the release would hang in `validating`. This is the same single-row deploy announcement the production path now writes; validation rows have no real task/schedule, and k8s-controller suppresses the RUNNING announcement for `mode=validation` Jobs, so no `task_status_updated` row is ever surfaced for them. The per-node terminal outcome (`ok`/`failed`) arrives later via `validation.node.completed:v1`. A validation row that fails AT dispatch (not deployable, or a permanent pre-deploy error) is made terminal via `FailValidation`, records `outcome=failed`, emits no `node.deployed` trigger, skips transitively `blocked` downstreams (marking them `skipped`), and runs the aggregate gate.

### Per-release validation aggregate gate

The `validation.completed:v1` aggregate is gated by one shared, infrastructure-free helper (`service/validation.EmitValidationAggregateIfComplete`) invoked from two call sites that both run it inside their own transaction:

- the deploy dispatcher, when a `mode=validation` node fails AT dispatch (not deployable, or a permanent pre-deploy deployer error) and `FailValidation` makes it terminal;
- the `validation.node.completed:v1` handler, when a node terminates AFTER dispatch.

The gate first takes a per-release transaction advisory lock (`LockRelease` → `pg_advisory_xact_lock(hashtext(release_id))`) so the whole count→claim→emit sequence is serialized across concurrent transactions; the second caller blocks until the first commits and then evaluates against its committed state. This closes the lost-emission window where the dispatcher's `FailValidation`-at-dispatch path and the `validation.node.completed:v1` handler (or two replicas) bring the last two nodes terminal in overlapping transactions and, under READ COMMITTED, each reads the other as still pending and both no-op — hanging the release in `validating`.

Under the lock the gate is a no-op while `PendingValidationCount(release_id) > 0`. `PendingValidationCount` counts rows that are not yet terminal: `pending`, `blocked`, and `deployed` are all non-terminal. Once every node is terminal (`outcome` is `ok`, `failed`, or `skipped`) the gate claims the `validation_aggregates` sentinel (`INSERT … ON CONFLICT DO NOTHING`); the single winner reads `ListValidationResults`, builds the per-node payload, and writes one `validation.completed:v1` outbox row whose `aggregate_id` is a deterministic UUIDv5 over an immutable namespace and `release:<release_id>`, so any re-emission deduplicates downstream. The outbox payload includes `candidate_schema` so the teardown consumer knows which schema to drop. The advisory lock guarantees exactly-once (never zero); the sentinel guarantees exactly-once (never double). Losers return without emitting.

### Outbox processor (every 5 seconds, batch of 100)

`pkg/outbox.Processor` polls `executor_outbox` and calls `OutboxPublisher.Publish` per entry. The publisher marshals the payload and performs a single Redis XADD. There is no `TerminalFailureHook` — terminal outcomes are written as ordinary outbox rows by the dispatcher.

```
For each pending executor_outbox entry:
  1. Marshal payload (already a typed event struct)
  2. XADD to the row's stream_name
  3. MarkProcessed

On XADD failure:
  - retry_count < max_retries: retry on next poll
  - retry_count >= max_retries: MarkFailed (the deployment row is already failed/deployed)
```

## Background Loops

| Loop | Description |
|---|---|
| Redis consumers | Reads `query.model:v1`, `retry.task:v1`, `schedule.cancelled:v1`, `validation.requested:v1`, `validation.node.completed:v1`, and `validation.completed:v1` via `pkg/redis.StreamConsumer`; crash-recovery for pending messages on startup |
| Deploy dispatcher (`deployer.Dispatcher`) | Polls `executor_deployments` every 5 seconds; creates K8s Jobs and writes outbox announcement rows, capped by `MAX_CONCURRENT_JOBS` |
| Outbox processor (`pkg/outbox.Processor`) | Polls `executor_outbox` every 5 seconds; processes up to 100 entries per batch via `OutboxPublisher` (uniform marshal-and-XADD) |

## Reliability Patterns

- **Inbound dedup**: `message_processing` keyed on `(message_id, stream_name)` prevents double-processing of duplicate Redis messages; managed by `pkg/messageprocessing.Dedup`
- **Decoupled command queue**: inbound handlers write only a `pending` row to `executor_deployments` (a pure Postgres write, no Kubernetes I/O); the K8s deploy happens asynchronously in the dispatcher, keeping the Unit-of-Work transaction free of external side effects
- **Explicit transaction boundary**: each inbound message runs dedup + deployment row insert inside a single Unit-of-Work transaction; the dedup row and deployment intent are committed atomically
- **Concurrency cap**: `deployer.Dispatcher` counts live K8s Jobs (`app=dbt-job`, `.status.active > 0`) on every tick and processes at most `max(0, MAX_CONCURRENT_JOBS - active)` rows; rows beyond the cap stay `pending` until the next tick
- **K8s idempotency**: `CreateQueryJob` treats already-exists as success; a dispatcher restart or crash after K8s success but before commit will re-attempt safely
- **Dispatcher backoff**: transient K8s failures reschedule the row via `next_attempt_at` with exponential backoff (base 5s, cap 2 min); the row stays `pending` and is retried on the next tick when due
- **Terminal failure propagation**: on `ErrPermanent` or retry-budget exhaustion, the dispatcher writes `task_status_updated` FAILED + `node_updated` FAILED as ordinary `executor_outbox` rows before marking the deployment `failed` — ensuring orchestrator and state always learn of the terminal outcome
- **Uniform outbox publisher**: the executor `OutboxPublisher` is a plain marshal-and-XADD; it carries no K8s deploy logic and has no `TerminalFailureHook`; all failure signalling is handled upstream by the dispatcher
- **No state gRPC dependency**: executor-controller does not call state gRPC; task status updates flow via `task.status.updated:v1`
- **Task max retries**: `task_max_retries` written into `executor_deployments.job_params` defaults to 2 (3 total execution attempts: initial + 2 retries); propagated to `k8s-controller` via `node.deployed:v1` to govern task-level retry logic
