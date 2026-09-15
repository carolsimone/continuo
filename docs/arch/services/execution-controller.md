# execution-controller

## Purpose

`execution-controller` turns dispatch messages into Kubernetes Jobs, watches those Jobs to a terminal state, and settles the result.

It is responsible for:
- consuming executable node intents and delayed re-checks from Redis
- deduplicating repeated dispatches and repeated status checks via `pkg/messageprocessing` keyed on `(message_id, stream_name)`
- durably recording deploy intent and per-node candidate state in one outbox-backed command queue
- creating Kubernetes Jobs via the Kubernetes API and querying them for status
- deciding a Job's outcome — still running (re-check), failed (retry or permanent), succeeded (complete)
- uploading full pod logs, and (for validation and python-family jobs) a structured result block, to S3
- publishing `task.status.updated:v1` and `task.execution.recorded:v1` so `state` learns of every task transition
- publishing `node.updated:v1` so `orchestrator` advances its topology projection
- settling candidate-release legs for `release-controller`: per-node validation results, and the aggregated `validation.result:v1`, `seed.build.completed:v1`, and `compile.completed:v1` decisions
- owning the candidate schema's lifecycle (`_candidate_<release>` create/drop) through one-shot engine-image Jobs, so no part of this service ever holds a warehouse connection of its own

## Package Structure

Port ownership follows the repo-wide convention:

| Layer | Package | Contents |
|---|---|---|
| Domain repository ports | `execution-controller/domain/repository` | `DeploymentRepository` — the deploy command queue; `ValidationAggregateRepository` — the per-(release, leg) emission sentinel; `CancelledSchedulesRepository` — cancelled schedule guard |
| Application / technical ports | `execution-controller/service/ports` | `CommandResolver` — dbt-dialect command resolution; `CandidateSchemaCreator` / `CandidateSchemaCleaner` — candidate-schema lifecycle; `LogUploader` — uploads log/result content to object storage; `JobObserver` — reads a Kubernetes Job's status, metadata, and logs |
| UnitOfWork interface | `execution-controller/service/uow` | `UnitOfWork` — tx lifecycle plus `OutboxRepo`, `DeploymentsRepo`, `ValidationAggregateRepo`, `CancelledSchedulesRepo`, `MessageProcessingRepo` |
| Concrete implementations | `execution-controller/adapters/postgres` | `PostgresUnitOfWork`, `DeploymentsRepository`, `ValidationAggregateRepository`, `CancelledSchedulesRepository` |
| Concrete implementations | `execution-controller/adapters/k8s` | `K8sClient` (clientset bootstrap in `client.go`; create-side `jobs_create.go`/`podspec_*.go`/`schema_ops.go`; observe-side `jobs_observe.go`; `candidate_schema_lifecycle.go`); `Deployer` (implements `domain/deploy.Deployer`) |
| Concrete implementations | `execution-controller/adapters/s3` | `LogUploader` adapter |
| Concrete implementations | `execution-controller/adapters/delayqueue` | the `check.k8s:v1` delay-queue scheduler and promoter |
| Concrete implementations | `execution-controller/adapters/publisher` | `OutboxPublisher` |
| Concrete implementations | `execution-controller/adapters/commandcfg` | `CommandResolver` (`dbt-commands.yaml` loader) |
| Concrete implementations | `execution-controller/adapters/redis` | one binding + parser pair per consumed stream |
| Concrete implementations | `execution-controller/adapters/http` | the `/ready` / `/livez` / `/health` server |

`service/handlers` imports no `adapters/*` package; every collaborator is reached through a port owned by `domain/repository` or `service/ports`. This is enforced by `TestServiceHandlersDoNotImportAdapters` in `pkg/streams/handler_imports_test.go`. `service/deployer` holds the deploy dispatcher; `service/validation` holds the per-release gating-propagation and aggregate-emit gate shared by the dispatcher's at-dispatch failure path and `service/outcomes.Recorder` (which the job-status handler calls to settle a node after dispatch); `service/artifacts` is the single home for the compile leg's S3 URI layout (manifest and parse-cache addresses). `K8sClient` implements `domain/deploy.Deployer` (through the `adapters/k8s.Deployer` wrapper) for the create side and `service/ports.JobObserver` directly for the observe side — one Kubernetes clientset, two ports, with the `var _ ports.JobObserver = (*K8sClient)(nil)` assertion living in `adapters/k8s`.

## Owned Storage (Postgres: `continuo_execution`)

| Table | Purpose |
|---|---|
| `message_processing` | Inbound dedup: keyed on `(message_id, stream_name)`, with a unique `(outbox_entry_id, stream_name)` index for the streams that dedup on a carried outbox id instead of the raw Redis message id |
| `execution_outbox` | Canonical transactional outbox — one row per pending Redis announcement; `pkg/outbox.Processor` polls it and performs the Redis XADD (or, for a `check_delayed` row, the delay-queue enqueue) per row |
| `cancelled_schedules` | Guard table fed by `schedule.cancelled:v1`; consulted before writing a `deployments` row and before acting on a Job status result, so a dispatch or a Job result for a cancelled schedule is dropped. Rows are swept after a configurable TTL |
| `deployments` | The deploy command queue and the per-node candidate-leg state store. A `mode` column distinguishes `production` rows (the default `query.model:v1` path, and the -rN retry rows the job-status handler queues in-process on a retryable failure) from `validation`/`seed_build`/`compile` rows (candidate-release legs, which carry `release_id`/`node_id` and a per-node terminal `outcome`) |
| `validation_aggregates` | Per-(release, leg) emission sentinel; the settle path claims a row under the per-release advisory lock before emitting a leg's completion event, so two concurrent final nodes cannot both emit it |

`message_processing` columns: `id`, `message_id`, `stream_name`, `state` (`processing`/`completed`/`acked`), `payload` (JSONB), `error`, `created_at`, `updated_at`, `outbox_entry_id`. Unique on `(message_id, stream_name)`; a second unique index on `(outbox_entry_id, stream_name)` (partial, `WHERE outbox_entry_id IS NOT NULL`) backs the deterministic and outbox-entry-carried dedup strategies described under Inbound Interfaces below.

`execution_outbox` columns: `id`, `message_processing_id` (nullable FK to `message_processing`), `aggregate_type`, `aggregate_id`, `event_type`, `payload` (JSONB), `stream_name`, `status` (`pending`/`scheduled`/`processed`/`failed`), `retry_count`, `max_retries` (default 13), `created_at`, `processed_at`, `error_message`, `next_attempt_at` (nullable; `NULL` means due now — see `docs/arch/05-error-classification.md` §Outbox processor resilience). Indexes: `idx_execution_outbox_pending` (`created_at` where `status='pending'`), `idx_execution_outbox_aggregate` (`aggregate_type, aggregate_id`), `idx_execution_outbox_due` (`next_attempt_at` where `status='scheduled'`).

`cancelled_schedules` columns: `schedule_id` (PK), `cancelled_at`.

`deployments` columns: `id`, `message_processing_id` (nullable FK), `task_id`, `schedule_id`, `job_params` (JSONB), `status` (`pending`/`blocked`/`deployed`/`failed`/`skipped`), `retry_count`, `max_retries` (default 3), `next_attempt_at`, `created_at`, `deployed_at`, `error_message`, `mode` (`production`/`validation`/`seed_build`/`compile`, default `production`), `release_id`, `node_id`, `outcome` (`ok`/`failed`/`skipped`, nullable), `dbt_log_uri`, `outcome_at`, `run_results_uri` (S3 key of the structured validation result; `NULL` when the pod emitted no block), `failed_container` (the pod container that terminated non-zero on a failed compile — `compile`/`parse-prod`/`parse-candidate`/`upload`; `NULL` for successful outcomes, non-compile rows, and pre-attribution rows). Production rows leave the candidate-only columns `NULL`; a check constraint requires `release_id` and `node_id` whenever `mode` is not `production`. Validation, seed-build, and compile rows have no real task/schedule identity, so the `NOT NULL` `task_id`/`schedule_id` columns are filled with deterministic UUIDv5 values derived from an immutable namespace over `(release_id, node_id)`. Indexes: `idx_deployments_due` (`next_attempt_at` where `status='pending'`); `idx_deployments_candidate_release` (`release_id, mode` where `mode IN ('validation','seed_build','compile')`); a unique index `uq_deployments_candidate_release_node_mode` on `(release_id, node_id, mode)` for the same predicate enforces one row per (release, node, mode). `blocked` is non-terminal in the aggregate gate (the release cannot complete while any node is `blocked`); `skipped` is a terminal non-`ok` outcome (a node that could not run because an upstream failed; its presence fails the release).

`validation_aggregates` columns: `release_id`, `mode` (`validation`/`seed_build`/`compile`, default `validation`), `aggregate_emitted_at`. Primary key `(release_id, mode)`. `ClaimEmission(release_id, mode)` does an `INSERT … ON CONFLICT DO NOTHING` so exactly one caller wins the right to emit the aggregate announcement for that leg. `LockRelease(release_id, mode)` takes a transaction-scoped advisory lock (`pg_advisory_xact_lock(hashtext(release_id || ':' || mode))`) that serializes the whole count→claim→emit gate per `(release_id, mode)` pair, so the three legs of one release lock independently and concurrent last-node terminals cannot both no-op (lost emission) under READ COMMITTED.

## Inbound Interfaces

### Redis consumers

Every stream is consumed via `pkg/redis.StreamConsumer` with a per-stream parser + binding pair: `pkg/redis.StreamConsumer` → `adapters/redis/<stream>_binding.go` → `service/handlers/<stream>_handler.go` (or, for the four candidate-schema teardown streams, a binding that calls `CandidateSchemaCleaner` directly with no separate handler package). The binding runs its dedup strategy and invokes the handler inside a single Unit-of-Work transaction.

| Stream | Consumer group | Dedup strategy | Description |
|---|---|---|---|
| `query.model:v1` | `executor-query-model` | Outbox-entry-carried (`DedupWithOutboxEntryID` on `(message_id, stream_name)`, falling back to the carried `outbox_entry_id`) | Primary dispatch: new node ready for execution |
| `schedule.cancelled:v1` | `executor-schedule-cancelled` | None — `cancelled_schedules.Insert` is `INSERT ... ON CONFLICT DO NOTHING` and is naturally idempotent | Schedule cancellation: suppress future deployments and absorb in-flight Job results for the schedule |
| `validation.requested:v1` | `executor-validation-requested` | Deterministic: a release-derived `outbox_entry_id` override (not the inbound `msg.ID`), so a redelivery with a fresh Redis id still collides on the same key | Candidate-release validation request: enqueue one `mode=validation` deployment per node |
| `validation.result:v1` (`kind=complete`) | `executor-validation-result-teardown` | None — `DropCandidateSchema` is idempotent | The `kind=complete` message on this stream is both produced and consumed by this service; the teardown consumer drops the `_candidate_<release>` schema via `CandidateSchemaCleaner`. `kind=node` messages are ignored by this consumer |
| `seed.build.requested:v1` | `executor-seed-build-requested` | Deterministic (release-derived `outbox_entry_id` override) | Candidate seed-build request: enqueue one `mode=seed_build` deployment per new/changed dbt-seed node |
| `compile.requested:v1` | `executor-compile-requested` | Deterministic (release-derived `outbox_entry_id` override) | Compile request: enqueue exactly one `mode=compile` deployment for the changed service |
| `pipeline.run.finished:v1` | `executor-pipeline-run-finished` | None — idempotent teardown | Emitted by `release-controller` for every terminal decision of every pipeline run — a candidate release or a fix-verification run alike. Drops the run's `candidate_schema` via `CandidateSchemaCleaner`; best-effort, since a leftover candidate schema must never block a run's terminal decision |
| `release.promoted:v1` | `executor-release-promoted` | None — idempotent teardown backstop | Idempotent backstop for the same `candidate_schema` teardown when the payload carries one |
| `release.rejected:v1` | `executor-release-rejected` | None — idempotent teardown backstop | Idempotent backstop for the same `candidate_schema` teardown when the payload carries one |
| `check.k8s:v1` | `k8s-check-status` | Outbox-entry-carried | Delayed status-check tickets; the promoter moves due tickets from the delay queue into this stream. Every message is already due — the promoter only XADDs due tickets — so the binding processes each message immediately. The dispatcher writes the first ticket for a Job directly after creating it; the job-status handler writes every later ticket while the Job is still running — both routes go through the same delay queue and land on this one stream, so the job-status handler observes a fresh Job and a re-polled one identically |

`query.model:v1` carries `task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `node_type`, `operation` (the dbt verb to run — `""` default `run`, `test`, or `build`; selects the argv `CommandResolver.NodeCommand` builds for the node, independently of `node_type`). A retryable production failure queues the same shape of `deployments` row in-process — the job-status handler calls `createDeployment` directly, in the same transaction as the FAILED announcement it writes for the exhausted attempt, rather than round-tripping through Redis — so `operation` and the other fields reach the `-rN` retry row unchanged. `check.k8s:v1` carries `pkg/events.CheckK8s` (`task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema_name`, `table_name`, `job_name`, `node_type`, `image_tag`, `operation`, `retry_count`, `max_retries`, plus `running_announced` — false on the dispatcher's first ticket for a fresh attempt, set true once RUNNING has been announced, so the self-poll loop announces RUNNING exactly once per attempt without persistent state).

The cancelled-schedule guard runs inside `QueryModelHandler` via `uow.CancelledSchedulesRepo().Exists`, and again inside the job-status handler (across every Job-status branch, including the retry branch that would otherwise queue a new `deployments` row) before it acts on any Job result; a cancelled match commits the dedup row (so the message is ACKed and never reprocessed) and returns without writing to `deployments` or emitting any outbox row.

`validation.requested:v1` carries every node for a release in one message (flat JSON `payload` field). Each node entry includes `upstream_node_ids` — the in-set upstreams (intra- and cross-service) that must complete before this node can be dispatched — and `candidate_artifact_uri`, the `s3://` pointer to the object the node's validation Job fetches: rewritten SQL for a dbt node, empty for seeds. The handler writes one `deployments` row per node: `blocked` if `upstream_node_ids` is non-empty, `pending` otherwise, and records `candidate_artifact_uri` in the row's `job_params` for use when the K8s validation Job is created. Before enqueuing any node, the binding creates the release's `_candidate_<release>` schema exactly once via `CandidateSchemaCreator.EnsureCandidateSchema` — a one-shot engine-image Job (the `VALIDATION_IMAGE` runner with the operator warehouse Secret via `envFrom`, running the `ensure_schema` op on `DBT_TARGET_SCHEMA`) that the binding blocks on. Pre-creating the schema once, ahead of the fan-out, means every validation Job — and every dbt seed Job, which may run when no validation node does — finds the schema already present. The ensure Job runs outside the Unit-of-Work transaction; a failure returns before any deployment row is enqueued and the message is retried. The pre-create runs only for a fresh release: a deduped redelivery skips it.

A validation, seed-build, or compile Job's terminal result never travels as a Redis message: once the job-status handler observes it (via `check.k8s:v1`), it calls `service/outcomes.Recorder.Record` in the SAME transaction as that observation, passing the mode, `release_id`/`node_id` (read from the Job's raw annotations), the derived `outcome` (`ok`/`failed`), and the log/run-results URIs. `Record` looks up the `(release_id, node_id, mode)` deployment, attaches the outcome via `RecordOutcome`, saves it, then settles the leg: for `mode=validation`, on `ok` any `blocked` downstream whose every in-set upstream is now `ok` transitions to `pending` (unblocked for dispatch), and on `failed` every transitively `blocked` downstream is marked `skipped` (terminal, non-`ok`); `mode=seed_build`/`mode=compile` have no in-leg gating to propagate. `Record` then runs the leg's aggregate-emit gate. An unknown `(release_id, node_id, mode)` (no matching row) is logged and ignored; a re-observed terminal Job whose deployment already carries an outcome is a no-op (no double-record, no duplicate aggregate).

### Delay queue (Redis: `checkk8s:pending` ZSET + `checkk8s:tickets` HASH)

A not-yet-due status re-check waits in a Redis delay queue rather than being re-XADD'd onto `check.k8s:v1`, so the stream cannot grow unbounded. The queue is two durable keys, both keyed by `JobName` (a K8s Job's stable identity), kept in lockstep:

- `checkk8s:pending` — a ZSET acting as the clock: member = `JobName`, score = `check_after` (unix seconds). Because a member is keyed by value, re-scheduling a Job is an in-place score update — one entry per in-flight Job.
- `checkk8s:tickets` — a HASH holding each pending check's ticket: field = `JobName`, value = a JSON envelope `{entry_id, payload}` wrapping the typed `CheckK8s` payload with the source outbox row's ID.

`OutboxPublisher` writes a `check_delayed` row into this queue (HSET ticket + ZADD due time in one `MULTI`/`EXEC`). The promoter ticks every second, running one atomic Lua script that moves every due ticket (score ≤ now, bounded per batch) into `check.k8s:v1`: it XADDs the ticket's `payload` plus a flat `outbox_entry_id` field, then `HDEL`/`ZREM`s the ticket. Running the whole script uninterrupted makes promotion exactly-once across replicas. Carrying `outbox_entry_id` onto the stream lets the consumer's secondary dedup key suppress a replay if an outbox publish reaches Redis but its Postgres transaction rolls back and the row is retried after its ticket was already promoted. A missed tick loses nothing — the ZSET is durable and the next tick promotes the backlog.

### HTTP (port 8084)

Three endpoints backed by the `pkg/liveness` registry, with readiness and liveness deliberately split:

- `GET /ready` — readiness. Fails (503) when any registered worker (stream consumers, outbox processor, deploy dispatcher, delay-queue promoter, cancelled-schedules sweeper) has exited with an error, any consumer read-loop heartbeat has gone stale, **or** any dependency probe (Redis, Postgres) fails. This service holds no warehouse connection, so there is no warehouse probe. A dependency outage pulls the pod out of the Service endpoints so no traffic is routed to it.
- `GET /livez` — liveness. Fails (503) **only** for worker/heartbeat failures — dependency probes are excluded, so a Redis/Postgres outage does not restart a pod whose consumers are already retrying through it, while a dead or wedged consumer goroutine does.
- `GET /health` — plain process-up probe (always 200); retained for manual use.

The Kubernetes `readinessProbe` points at `/ready` and the `livenessProbe` at `/livez` (`deploy/continuo/values.yaml`: `readinessPath: /ready`, `livenessPath: /livez`).

### Graceful shutdown

On SIGTERM/SIGINT the lifecycle manager runs an ordered sequence whose total duration is bounded by `SHUTDOWN_GRACE` (default 15s) for handlers that honor their context — a `Close()`-style handler that ignores `ctx` is not itself bounded by it: (1) stop intake by cancelling the root context so the 15 stream consumers, the outbox processor, the deploy dispatcher, the delay-queue promoter, and the cancelled-schedules sweeper stop reading new work and unwind their in-flight handler or batch — the handler's next database call fails against the cancelled context, its transaction rolls back, and the message is left un-ACKed for redelivery; (2) drain — wait on a WaitGroup for those tracked goroutines to return, capped at half the grace period; (3) close infra — run the registered shutdown handlers (health HTTP server, Postgres, Redis) against a fresh live context with its own deadline fixed at the moment `Shutdown` starts, while step 2 is capped independently at half the grace budget; together they leave infra teardown at least the other half of the budget, live, derived from `context.Background()`, never the just-cancelled root context. `main` blocks on the lifecycle completion channel, so there is no fixed sleep. The health server itself is not a tracked goroutine: it blocks in `ListenAndServe` until its own shutdown handler calls `Shutdown` in step 3, so tracking it would deadlock the drain against the very step that stops it.

## Outbound Interfaces

### Redis producers (via outbox)

Every business decision writes one or more `execution_outbox` rows inside the same transaction as its triggering write; `pkg/outbox.Processor` then drains those rows via `OutboxPublisher`, which is a uniform marshal-and-XADD (except `check_delayed`, routed to the delay queue) — no Job-creation or Job-observation logic lives in the publisher. A production Job that fails with retry budget remaining is the one business decision that never reaches this table: the job-status handler queues the `-rN` retry directly as a new pending `deployments` row (via `createDeployment`) in the same transaction as the `task_status_updated`/`task_execution_recorded` rows below, rather than announcing the retry over Redis.

| Stream | Trigger |
|---|---|
| `check.k8s:v1` (via the delay queue) | The dispatcher writes the first `check_delayed` outbox row immediately after K8s job creation succeeds (production and every candidate mode), in the same transaction as marking the deployment deployed; the job-status handler writes every later one while the Job is still running. Either way the publisher routes the row to the delay queue as a ticket rather than an XADD, and the promoter moves it onto this stream once due. For candidate rows the `task_id`/`schedule_id` are the deterministic synthetic UUIDs derived from `(release_id, node_id)` — inert carriers, since the job-status handler routes the Job's status by its `mode` label, not by these IDs |
| `task.status.updated:v1` | Published with `status=FAILED` on the never-deployed terminal dispatch failure (permanent error or retry-budget exhaustion before a pod exists), with `RUNNING` the first time the job-status handler observes an attempt's Job running (suppressed for every candidate mode, which carries no real task/schedule), and with the pod's terminal `SUCCEEDED`/`FAILED` |
| `task.execution.recorded:v1` | Published by the job-status handler on every production Job terminal (succeeded, permanently failed, or failed-with-retry); consumed by `state` to persist the execution record |
| `node.updated:v1` | Published on a never-deployed terminal dispatch failure, and on a production Job's terminal (`SUCCEEDED`/`FAILED`); consumed by `orchestrator` for topology projection |
| `validation.result:v1` (`kind=complete`) | Per-release validation decision; emitted exactly once when every `mode=validation` node for a release is terminal, under the per-release aggregate gate. Payload is the decision only: `kind` (`"complete"`), `release_id`, `candidate_schema` (for teardown), `aggregate_status` (`ok` iff every node is `ok`, else `failed`), derived from the stored per-node outcomes directly, not from stream delivery order |
| `validation.result:v1` (`kind=node`) | Per-node validation projection; emitted as each node settles, so `release-controller` can render per-node validation results live. One emit for the settled node itself, plus one for every downstream node the settle's failure propagation skips (status `"skipped"`, no URIs). Each is an idempotent last-write projection keyed by `node_id` with its own generated `aggregate_id` |
| `seed.build.completed:v1` | Per-release seed-build aggregate; emitted exactly once when every `mode=seed_build` node for a release is terminal. Payload: `release_id`, `status` (`ok`/`failed`), `per_node`, `candidate_schema` |
| `compile.completed:v1` | Compile aggregate; emitted exactly once when the compile node settles. Payload: `release_id`, `status` (`ok`/`failed`), `per_node` (each entry: `node_id`, `status`, optional `dbt_log_uri`, optional `run_results_uri`, optional `failed_container`), `candidate_schema` |
| `outbox.dead_letter:v1` | Written by `pkg/outbox.Processor` when a row exhausts `max_retries`; an operational dead-letter queue, not a domain event |

`operation` (the dbt verb the Job runs, e.g. `test`, `build`) is sourced from durable check/retry data, not from Job metadata: the dispatcher stamps it on the first `check.k8s:v1` ticket it writes for a Job, that value rides the delay-queue ticket onto every subsequent `check.k8s:v1` self-poll, and it is carried into the `-rN` retry deployment the job-status handler queues in-process from the durable `CheckJobStatus.Operation`. Sourcing it from the Job's labels would be unsafe because a TTL-reaped ("vanished") Job returns empty labels, which would silently rebuild a `dbt test`/`dbt build` Job as `dbt run`; carrying it in the durable payload keeps a retried `dbt test` or `dbt build` Job the same verb. Normal production runs have an empty `operation`, so the field is omitted and the wire format is unchanged for them.

### Command resolution (`dbt-commands.yaml`)

Container commands for production runs, seed-build, compile Jobs, and the compile leg's parse-export/rehearsal containers are resolved through the `ports.CommandResolver` port rather than a hardcoded dbt invocation. `adapters/commandcfg.Resolver` implements the port by loading an optional `dbt-commands.yaml` file at the path given by the `DBT_COMMANDS_CONFIG_PATH` environment variable. The file covers eight operations — `run`, `seed`, `snapshot`, `seed_build`, `test`, `build`, `compile`, `parse`. When a file is present, the `default` block is required and must define all eight, and every `services.<name>` override must define all eight too; a service never falls through to `default` or a built-in for a missing key. An incomplete or missing block is a fatal boot error. With no file, the built-in complete plain-dbt default is used for every service:

| Operation | Built-in command |
|---|---|
| `run` | `dbt run --select <node>` |
| `seed` | `dbt seed --select <node>` |
| `snapshot` | `dbt snapshot --select <node>` |
| `seed_build` | `dbt seed --select <node>` (schema routing relies on the `DBT_TARGET_SCHEMA` env var contract) |
| `test` | `dbt test --select <node>` |
| `build` | `dbt build --select <node>` — materializes and tests the node in one invocation |
| `compile` | `dbt compile --profiles-dir /project`, writing its manifest to `/project/target/manifest.json` |
| `parse` | `dbt parse` — the parse-export/rehearsal leg run by the compile Job's `parse-prod`/`parse-candidate` initContainers; takes no placeholders |

`run`, `seed`, and `snapshot` are selected by the node's `node_type` when the query's `operation` is empty (the default). `test` and `build` are selected by `operation` directly — `CommandResolver.NodeCommand(serviceName, operation, nodeType, node)` resolves the `test`/`build` template regardless of `node_type` once `operation` requests it. `parse` is selected only by the compile Job's parse-export initContainers, via `CommandResolver.ParseCommand(serviceName)`.

Templates support two placeholders: `{{ node }}` (the dbt `--select` target; required in `run`, `seed`, `snapshot`, `test`, `build`, and `seed_build`) and `{{ target_schema }}` (the release's candidate schema; allowed only in `seed_build`). `parse` and `compile.command` take no placeholders. A `compile` override pairs its argv with an explicit `manifest_path` — the absolute, placeholder-free filesystem path where that team's tool writes `manifest.json` — and an optional `partial_parse_path`. `CommandResolver.PartialParsePath(serviceName)` resolves it: an explicit `compile.partial_parse_path` wins; otherwise it defaults to `manifest_path`'s directory + `/partial_parse.msgpack`. When set explicitly, `partial_parse_path` must resolve to the same directory as `manifest_path`.

`DBT_COMMANDS_CONFIG_PATH` unset, or set to a path with no file present, yields the built-in commands for every operation. A field this build does not recognize is logged as a warning (`unknown_fields`) and dropped, not a fatal error — the config is then re-decoded leniently and validated as usual. A file that exists but otherwise fails to parse or fails validation (empty argv, an unrecognised `{{ ... }}` placeholder, a missing required placeholder, a relative/placeholder-bearing `compile.manifest_path`/`compile.partial_parse_path`, a `compile.partial_parse_path` outside `compile.manifest_path`'s directory, a `run`/`seed`/`snapshot`/`test`/`build`/`seed_build` argv whose parse-affecting flags (`--vars`, `--target`, `--profile`, `--profiles-dir`, `--project-dir`, `--no-partial-parse`) don't exactly match `parse`'s, or an incomplete block) is a fatal boot error. Resolution through `CommandResolver` applies only to `CreateQueryJob` (production runs, including `test`), `CreateSeedBuildJob`, and `CreateCompileJob`; `CreateValidationJob` always runs the `continuo-python-runtime-<engine>` image's `["continuo-runtime","validation-op"]` command regardless of any configured dialect. The file is read once at process start, so changing the mounted file — e.g. a Helm ConfigMap update — takes effect only after this service's pod restarts.

**Team-facing preconditions.** The parse-export/rehearsal gate and the run-pod cache hydration it feeds both rely on dbt's own partial-parse discovery — no flags are injected into the team's argv. Load-time validation catches the two preconditions a team dialect could otherwise violate silently — a `compile.partial_parse_path` outside `manifest_path`'s directory, and a run-family argv whose parse-affecting flags diverge from `parse`'s. One footgun remains unenforced because it is invisible to the config schema itself: **partial parsing must not be disabled** (`flags: partial_parse: false` in `dbt_project.yml`, or `--no-partial-parse` in every op's argv); a disabled project always fully re-parses regardless of any hydrated cache, and the rehearsal gate detects this at release time and fails the release with `parse_rehearsal_failed` rather than silently shipping a useless cache.

**Example: finance service.** A deployed `dbt-commands.yaml` can route the `finance` service through its own CLI wrapper, `wise-dbt`, baked into the finance dbt image at `/usr/local/bin/wise-dbt`. The service declares all eight keys: `run: ["wise-dbt", "run-model", "{{ node }}"]`, `seed: ["wise-dbt", "load-seed", "{{ node }}"]`, `snapshot: ["wise-dbt", "capture-snapshot", "{{ node }}"]`, `test: ["wise-dbt", "test-model", "{{ node }}"]`, `build: ["wise-dbt", "build-model", "{{ node }}"]`, `seed_build: ["wise-dbt", "load-seed", "{{ node }}"]`, `parse: ["wise-dbt", "parse-project"]`, and `compile: {command: ["wise-dbt", "compile-project"], manifest_path: "/project/target/manifest.json"}`. Building a Job for a finance model run (node type `dbt-model`, table name `orders`) returns `["wise-dbt", "run-model", "orders"]`. The resolver applies this substitution at Job-build time; domain events carry only intent (node type and table name), so the command dialect choice is decoupled from the event stream.

### Kubernetes API

Every Job built by `CreateQueryJob`, `CreateValidationJob`, `CreateSeedBuildJob`, and `CreateCompileJob` carries `TTLSecondsAfterFinished: 86400` (24h): once a Job reaches a terminal state, Kubernetes deletes the Job and its pod 24h later, so failed one-shot Job pods do not accumulate on the cluster indefinitely. This is a pure cleanup backstop — the job-status handler reads and uploads a Job's pod logs to S3 synchronously the moment it observes the Job go terminal (`GetPodLogs`/`fetchAndUploadLogs`), well before the TTL elapses, so the deletion never races a still-pending log read. `RunSchemaOpJob`'s Jobs use a much shorter TTL (120s) for the separate reasons described below.

- `CreateQueryJob` — creates a K8s batch Job in the configured namespace with label `app=dbt-job`; treated as idempotent (already-exists is not an error on retry). It branches on node type to build the pod, and only the pod: Job name, labels, TTL, and `BackoffLimit` are constructed once below the branch, so both kinds route through the job-status handler's production lifecycle by construction rather than by two code paths agreeing.

  **python-family nodes** (`node_type.IsPython()` — `python-model` and `python-csv`, `buildPythonPodSpec`) get a single container named `python-job` running the release's `image_tag` **verbatim** — a complete registry reference built from the engine-matched `continuo-python-runtime` base, never composed with the service name and never `DOCKERHUB_USERNAME`-prefixed. No command is set: the image's entrypoint is the runtime harness, which selects the node from the contract files baked inside it. The container env is exactly three variables — `NODE_ID` (`<schema>.<table>`, the same identity that keys the topology entry), `TABLE_NAME`, and `TARGET_SCHEMA` (the production schema) — deliberately diverging from the dbt Job env, since the harness recognises no `SCHEMA`/`DBT_TARGET_SCHEMA` fallback. The warehouse connection arrives via `envFrom` of the same operator-owned Secret dbt Jobs attach. None of the dbt pod plumbing applies: no `hydrate-parse-cache` initContainer even when `S3_BUCKET` is set, no volumes, and — for `python-model` — no S3 credentials, since its contract files travel inside the image itself. A `python-csv` node is the one deliberate exception: its harness fetches the CSV source itself, so its container additionally gets the same four S3 credential env vars (`S3_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`) the validation pods use. The container drops all capabilities and forbids privilege escalation while keeping the image's own user; `imagePullPolicy` is `IfNotPresent`. Only `operation=run` and `operation=build` are accepted, dispatching identically. The Job additionally carries a `runtime=python` label for operator selection; `app=dbt-job` is retained because it is the selector `CountActiveJobs` uses for the concurrency cap. See `deploy/python-image-contract.md`.

  **dbt node types** keep the standard path: container name `dbt-job`, image `<prefix>/<service>:<image_tag>`, and the container command is `CommandResolver.NodeCommand(serviceName, operation, nodeType, tableName)` — for the default `operation=run` the resolved `run`, `seed`, or `snapshot` argv for the node's type; for `operation=test` the resolved `test` argv; for `operation=build` the resolved `build` argv — both apply regardless of the node's type. The container `imagePullPolicy` is `IfNotPresent`. Every Job pod carries pod-level `seccompProfile: RuntimeDefault`; the `dbt-job` container drops all Linux capabilities and forbids privilege escalation, while keeping the team image's own user. When the `S3_BUCKET` env var is set, the pod additionally gets a `hydrate-parse-cache` initContainer that pre-seeds the service's prod-context partial-parse artifact before the team container starts.
- `CreateSeedBuildJob` — creates a `mode=seed_build` K8s batch Job (idempotent by job name). Uses the team service image and runs the container command from `CommandResolver.SeedBuildCommand(serviceName, tableName, candidateSchema)`. `DBT_TARGET_SCHEMA=<CandidateSchema>` is set on the Job unconditionally. The pod carries `seccompProfile: RuntimeDefault`; when `S3_BUCKET` is set, it gets a `hydrate-parse-cache` initContainer that pre-seeds the release's candidate-context partial-parse artifact. When the triggering `seed.build.requested:v1` message carries a non-empty `source_overlay_uri`, the seed Job gets the same overlay treatment the compile Job does (below): an `overlay` initContainer fetches the proposed source into a shared volume, the pod gains a `/work` emptyDir, and the team container's command is wrapped and prefixed with the staging prologue.
- `CreateCompileJob` — creates a `mode=compile` K8s batch Job (idempotent by job name). The Job pod has a shared `emptyDir` volume (`shared`, mounted at `/shared` in every container) and pod-level `seccompProfile: RuntimeDefault`. The initContainer (`compile`) uses the team service image, drops all capabilities and forbids privilege escalation while keeping the team image's own user, and runs a single `sh -c` line built from `CommandResolver.CompileCommand(serviceName)` followed by `&& cp <manifest_path> /shared/manifest.json && chmod 644 /shared/manifest.json`. The warehouse connection arrives by `envFrom` of the Secret named by `VALIDATION_WAREHOUSE_SECRET`.

  When the triggering `compile.requested:v1` message carries a non-empty `candidate_schema` — release-controller always sets it, so this is the normal case — the pod grows two more team-image initContainers, `parse-prod` and `parse-candidate` (the parse-export/rehearsal gate below), making it a four-container pod, or five when a fix-verification run's `source_overlay_uri` adds the `overlay` initContainer ahead of them; with an empty `candidate_schema` the pod stays at a two-container layout and the parse-export leg is skipped entirely. The main container (`upload`) uses the continuo-owned `s3-sidecar` image (a minimal python+boto3 image with no dbt; hosts `compile_uploader.py`), configured via `S3_SIDECAR_IMAGE`. It runs `python /compile_uploader.py` with `COMPILE_MANIFEST_PATH=/shared/manifest.json`, `MANIFEST_S3_URI`, and S3 credential env vars — plus, when the parse-export leg ran, `PARSE_PROD_LOCAL_PATH`/`PARSE_PROD_S3_URI` and `PARSE_CANDIDATE_LOCAL_PATH`/`PARSE_CANDIDATE_S3_URI`. Its pull policy is governed by `VALIDATION_IMAGE_PULL_POLICY`.

  **Verification-run source overlay.** When `compile.requested:v1` carries a non-empty `source_overlay_uri` — set only for a dbt fix-verification run verifying a proposed fix — the pod gains one more initContainer, `overlay`, placed first, running the `s3-sidecar` image's `python /overlay_fetcher.py` to unpack the proposed source files into the shared volume. This fetcher fails the Job on every problem (missing config, an unfetchable object, an unreadable or unsafe archive), because a fix-verification run exists only to judge a fix. Every team-image init container's shell command is then prefixed with a staging prologue so the overlay is the project under test throughout the whole pod: each staging container gets its own `workdir` emptyDir mounted at `/work`, and its command copies the team image's own project directory under `/work`, then overlays the fetched source on top, before compiling from there — so a team image needs no Dockerfile change to be verifiable.

  **Parse-export/rehearsal gate.** `parse-prod` and `parse-candidate` each run the service's resolved `parse` argv twice against `/shared/parse/<prod|candidate>/`, differing only in `DBT_TARGET_SCHEMA`. Run 1 (export) executes the argv as-is and requires `partial_parse_path` to exist afterward. Run 2 (rehearsal) re-runs the identical argv with `DBT_LOG_LEVEL=debug` and greps the debug log for one of three mutually exclusive markers (empirically pinned against `dbt-core==1.12.0b1`): partial parsing disabled for the project, an unrecoverable re-parse under run-pod conditions, or neither marker present. Only a clean run 2 copies `partial_parse.msgpack` to `/shared/parse/<ctx>/partial_parse.msgpack` for the upload container. Any non-zero initContainer exit fails the whole compile Job; the job-status handler attributes the failure to whichever container (`compile`/`parse-prod`/`parse-candidate`/`upload`) actually failed, and `release-controller` maps a `parse-prod`/`parse-candidate` failure to reject reason `parse_rehearsal_failed` rather than `compile_failed`.

  **Run-pod cache hydration.** `parseCacheInitContainer` builds the shared `hydrate-parse-cache` initContainer used by `CreateQueryJob`, `CreateSeedBuildJob`, and (implicitly, via the parse-export leg's own upload) the compile leg: it runs the `s3-sidecar` image's `parse_cache_fetcher.py`, which downloads the context's S3 artifact into an `emptyDir` mounted over the team container's dbt target directory. The fetcher never fails the Job: a missing bucket/key config, an unfetchable object, or a write failure all degrade — the container logs the reason, writes `degraded:<reason>` to its termination message, and exits 0, so the team container starts and runs a normal full parse. A successful fetch writes `hydrated` to the termination message. The job-status handler reads that termination message off the `hydrate-parse-cache` initContainer's status and derives `task_execution.parse_cache` (`hydrated`/`degraded`/`unknown`, absent entirely for pods with no such initContainer) and `parse_cache_reason`.

  The artifact fetched depends on context: `CreateQueryJob` (production run) fetches the prod-context artifact keyed by `(service, image_tag)` at `s3://<bucket>/<service>/parse-cache/<image_tag>/partial_parse.msgpack`. `CreateSeedBuildJob` fetches the candidate-context artifact for the release being validated, at `s3://<bucket>/<service>/<release_id>/partial_parse.candidate.msgpack`. Both artifacts are produced by the compile Job's parse-export/rehearsal gate above.
- `CreateValidationJob` — creates a `mode=validation` K8s batch Job in the configured namespace (idempotent by job name, `BackoffLimit` 0, `RestartPolicy` Never). The `imagePullPolicy` defaults to `Always`: `VALIDATION_IMAGE` is the external `continuo-python-runtime-<engine>` image (PostgreSQL and Trino today), overridable via `VALIDATION_IMAGE_PULL_POLICY` for clusters that side-load images. Labels: `app=dbt-job` plus `mode=validation` and sanitised `release-id`/`node-id` for selection/observability; annotations `continuo.dev/release-id` and `continuo.dev/node-id` carry the raw, unmodified values, which the job-status handler reads into the `outcomes.NodeOutcome` it records so the outcome lookup keyed on the unmodified `deployments` row matches even when a label would be altered.

  The container's command is set explicitly to `["continuo-runtime","validation-op"]` on every node type. That command dispatches on `VALIDATION_OP`: **`build_from_sql`** (default) — fetches the node's compiled SQL from `CANDIDATE_SQL_URI` in S3 and runs `CREATE TABLE <candidate>.<table> AS (<sql>) WITH NO DATA`. **`build_from_columns`** — the python-node equivalent: fetches a JSON validation spec from `CANDIDATE_SPEC_URI`, bind-checks each declared read, and creates the empty typed table directly from the spec's output columns. **`clone_from_prod`** — clones an existing prod table's shape empty. **`check_binds`** — a changed-closure `dbt-test` node's compiled assertion query, EXPLAINed against the candidate schema instead of creating anything. All four ops are single-container: no init container, no shared `emptyDir`, no dbt in the validation path. The runner prints the same sentinel-framed structured validation-result block (`status`/`message`/`failures`/`unique_id`) as its last stdout, which the job-status handler uploads to S3 as `run_results_uri` — a frozen cross-repo wire contract with `continuo-python-runtime` (see `pkg/validationresult`). The candidate schema already exists by the time any validation Job runs (created once, race-safely, by the `validation.requested:v1` handler before the fan-out); the runner still calls `ensure_schema` as a second line of defense. Env vars set on every validation container: `DBT_TARGET_SCHEMA` (= candidate schema), `TABLE_NAME`, `RELEASE_ID`, `NODE_ID`, `SERVICE_NAME`, `SCHEMA`, `JOB_NAME`, `VALIDATION_OP`, `PROD_SCHEMA`. The warehouse connection arrives via `envFrom` of the Secret named by `VALIDATION_WAREHOUSE_SECRET`. S3 credentials are set on `build_from_sql`/`build_from_columns` pods only, each with its own candidate-artifact env var (`CANDIDATE_SQL_URI` or `CANDIDATE_SPEC_URI`).
- `RunSchemaOpJob` — the candidate-schema lifecycle. `CandidateSchemaCreator.EnsureCandidateSchema` and `CandidateSchemaCleaner.DropCandidateSchema` both delegate here: it schedules a one-shot Job running the same `VALIDATION_IMAGE` under the same explicit command, with the warehouse Secret via `envFrom`, but with only `DBT_TARGET_SCHEMA` + `VALIDATION_OP` (`ensure_schema`/`drop_schema`) and no table, S3, or candidate SQL — it runs one DDL statement through the engine adapter and exits. This service holds no warehouse connection of its own; the engine image owns the dialect. The Job is idempotent by a deterministic DNS-safe name (`ensure-schema-<schema>` / `drop-schema-<schema>`) so a redelivered trigger waits on the in-flight Job rather than duplicating it, and a leftover terminal Job from a prior attempt is cleared before a retry. `RunSchemaOpJob` blocks until the Job reaches a terminal state within `SchemaOpJobTimeout` (5 minutes); a Job's `ActiveDeadlineSeconds` is matched to that wait so a hung DDL pod is killed cleanly rather than becoming an Active Job that livelocks every retry. These Jobs carry `app=continuo-schema-op` (never `mode=validation`), so the job-status handler's mode-based routing ignores them — their lifecycle is owned entirely by the candidate-schema adapter. `TTLSecondsAfterFinished` (120s) plus explicit delete-on-success keep them from accumulating. The six stream consumers whose handlers block on a schema-op Job (`validation_requested`, `seed_build_requested`, and the four teardown consumers) run with a handler budget of `SchemaOpJobTimeout` plus a minute (6 minutes), a heartbeat-stale threshold two minutes above that (8 minutes), and a pending-entry reclaim `MinIdle` a minute above the handler budget (7 minutes) — so a peer replica never re-claims a message whose handler is legitimately mid-wait — while every other consumer keeps the ordinary 60s handler / 3-minute heartbeat budget.

### K8s client configuration

`NewK8sClient` uses `KUBECONFIG` when set (local / docker-compose). If `KUBECONFIG` is not set it falls back to `rest.InClusterConfig()` for pod deployments with a ServiceAccount.

### S3

Two upload keys per terminal Job, each independently soft-failing to an empty key so S3 unavailability never changes a Job's recorded outcome:
- `logs/task-executions/{artifact_path}/{execution_id}.log` — the full pod log with any structured-result sentinel block stripped. `artifact_path` addresses which Job produced the object: a Job that ran a node is addressed by that node, `{service_name}/{schema_name}/{table_name}`; the compile Job runs no node and carries neither schema nor table, so it is addressed by its service and leg, `{service_name}/compile` — addressing it as a node would leave both segments empty, which MinIO rejects as an invalid object name while AWS S3 accepts it, losing the compile log (including a *failed* compile's log) only on installs backed by the bundled MinIO.
- `run-results/task-executions/{artifact_path}/{execution_id}.json` — the structured sentinel-framed result block, uploaded only when the pod printed one (validation pods and python-family production containers always print one; dbt containers never do).

Two more keys address the compile leg's dbt artifacts, both built by `service/artifacts` so the compile handler and the k8s adapter never drift apart: `s3://<bucket>/<service>/<release_id>/manifest.json` (the release's compiled manifest, fetched later by `topology-controller`) and the parse-cache artifacts — `s3://<bucket>/<service>/parse-cache/<image_tag>/partial_parse.msgpack` (prod context, keyed by service + image tag) and `s3://<bucket>/<service>/<release_id>/partial_parse.candidate.msgpack` (candidate context, a sibling of that release's manifest).

Uploads run for every terminal Job, succeeded as well as failed: the pod is garbage-collected at the Job's TTL, so an unuploaded log reduces a finished run to timings with no evidence. Operators should set an S3 lifecycle policy on the `logs/` prefix — volume scales with DAG width × run frequency.

## Processing Logic

### On `query.model:v1`

```
1. Dedup check via pkg/messageprocessing against message_processing (message_id, stream_name)
   → if already present: skip (ACK without processing)
2. Check cancelled_schedules for the schedule_id
   → if cancelled: commit dedup row and return (no deployment row written)
3. Write deploy intent to deployments (status=pending)
4. Commit (dedup row + deployment row in one transaction)
```

### Deploy dispatcher (every 5 seconds)

`deployer.Dispatcher` polls `deployments` for due rows, capped by active K8s Jobs, one row per transaction:

```
1. CountActive — label selector app=dbt-job, .status.active > 0
   headroom = max(0, MAX_CONCURRENT_JOBS - active)
   if headroom == 0: return (pending rows stay pending until next tick)

2. For up to headroom due rows (each in its OWN transaction):
   a. GetDueBatch(1) — row WHERE status='pending' AND next_attempt_at <= NOW(), FOR UPDATE SKIP LOCKED
   b. Route on mode:
      - production: CreateQueryJob (python-family node types route to the python pod
        builder, every other node type to the dbt pod builder)
      - validation: CreateValidationJob
      - seed_build: CreateSeedBuildJob
      - compile: CreateCompileJob
   c. On success: MarkDeployed, write the first check_delayed outbox row (→
      check.k8s:v1 via the delay queue) that starts the job-status handler's
      polling loop (production also leaves the running/terminal announcement to
      the job-status handler; candidate modes emit only this first check
      ticket, since they carry no real task/schedule)
   d. On transient error with retry budget remaining: reschedule with exponential
      backoff (base 5s, cap 2m) via next_attempt_at
   e. On permanent error (errors.Is ErrPermanent) or retry-budget exhaustion:
      - production: write task_status_updated (FAILED) + node_updated (FAILED)
        outbox rows, MarkFailed
      - candidate mode: record the node's outcome=failed and run the per-release
        gating-propagation + aggregate-emit gate (service/validation), so a node
        that fails at dispatch — never reaching a Job at all — still skips its
        blocked descendants and lets the release's leg complete
```

A candidate row is dispatched only once `pending` (unblocked); `blocked` rows (with unresolved in-set upstreams) remain in place until `outcomes.Recorder` transitions them to `pending` once their upstreams settle. The dispatcher never dispatches `blocked` rows.

### Job-status handler

The job-status handler (`service/handlers/job_status_handler.go`) processes every `check.k8s:v1` ticket identically, whether it is the dispatcher's first check written right after Job creation or a later re-poll of a still-running Job:

```
1. GetJobStatus (K8s) — query current pod status; started_at uses a three-tier
   priority: job-level StartTime (persists after pod GC) → pod-level StartTime →
   container-level terminated.StartedAt
2. Determine retry_count/max_retries from the message payload (falling back to
   DEFAULT_TASK_MAX_RETRIES when the message carries none)
3. Check cancelled_schedules for the schedule_id
   → if cancelled: absorb the result and return (no outbox rows written)
4. Running: re-poll (see Running below) without reading Job metadata
5. Not running: GetJobMeta (labels + annotations) to route the terminal/unknown result
6. Begin Postgres transaction; write outbox entries per outcome; commit
```

Outcome branches, once metadata is fetched:

| Outcome | Action |
|---|---|
| **Running** | The first time an attempt is observed running (`RunningAnnounced == false`), fetches Job metadata and announces `task.status.updated:v1` (RUNNING) stamped with the running attempt — suppressed for every candidate mode (`validation`/`seed_build`/`compile`/legacy `promote-seed`), which carries no real task/schedule. Always writes a `check_delayed` outbox entry (enqueued in the delay queue, promoted to `check.k8s:v1` when due) with `running_announced=true`, so RUNNING is announced exactly once per attempt. The job-status handler is the sole producer of the running/terminal pod lifecycle |
| **Succeeded** (production) | Fetches and uploads logs (soft-fail) → writes `task_status_updated` (SUCCEEDED) stamped with the attempt's retry count, `task_execution_recorded`, and `node_updated` (SUCCEEDED) |
| **Failed, retryable** (`retry_count < max_retries`, production) | Fetches and uploads logs → writes `task_status_updated` (FAILED) stamping the attempt that ran and `task_execution_recorded`, then calls `createDeployment` in the SAME transaction to queue a new pending `deployments` row for the next attempt (`retry_count + 1`) with the `-rN` job name — the retry and the FAILED announcement cannot diverge |
| **Failed, permanent** (`retry_count >= max_retries`, production) | Fetches and uploads logs → writes `task_status_updated` (FAILED), `task_execution_recorded`, and `node_updated` (FAILED) |
| **Unknown** (production) | Not retryable state: writes only `task_status_updated` (FAILED), with the pod's termination message or a default reason folded into the log line — no execution record and no node projection, since no terminal Job outcome was actually observed |
| **`mode=validation` terminal** | An Unknown status here is treated as not-yet-terminal and re-polled via `check.k8s:v1` rather than failed. On Succeeded/Failed: fetches and uploads logs, derives `outcome` (`ok`/`failed`), reads `release_id`/`node_id` from the Job's raw `continuo.dev/*` annotations, and calls `outcomes.Recorder.Record` in the same transaction — recording the outcome on the deployments row and settling the leg (gating propagation, per-node projection, and the aggregate once every node is terminal) |
| **`mode=seed_build` terminal** | Same shape as validation, recording the outcome as `mode=seed_build`; no gating to propagate (seeds are flat roots) |
| **`mode=compile` terminal** | Same shape, recording the outcome as `mode=compile` (also carrying `failed_container` when the terminal result attributes the failure to a specific pod container); the compile leg's log and run-results are filed under `compileArtifactPath` (`{service}/compile`), not a node path |
| **`mode=promote-seed` terminal** | A legacy label from a Job queued before a rolling deploy caught up; the terminal is absorbed with no lifecycle event, since its synthetic task id has no run in `state`. Current promoted-seed dispatches carry no `mode` label and fall through to the production path |
| **Empty Job metadata** (Job deleted/TTL-reaped) | `GetJobMeta` returns empty maps rather than an error; a vanished Job falls through to the production path (correct, since its NotFound→Failed status still drives the retry/permanent handlers), and is logged as a warning since a vanished *candidate* Job cannot recover its `release_id`/`node_id` to emit a per-node outcome |

The error message recorded on a failed production Job prefers the structured sentinel block's `message` (parsed from the full log, or from the tail when the full log carried no block), then the raw log tail, then the pod's K8s termination message; `ErrorMessageMaxLen` truncates the stored value.

### Per-release aggregate gate

The aggregate gate is mode-parametrized. Each mode has its own helper invoked from the same two call sites (the dispatcher's at-dispatch failure path and `outcomes.Recorder`'s post-dispatch settle), both running inside their own transaction:

- `EmitValidationAggregateIfComplete` handles `mode=validation` and emits the `kind=complete` message on `validation.result:v1`.
- `EmitSeedBuildAggregateIfComplete` handles `mode=seed_build` and emits `seed.build.completed:v1`.
- The compile equivalent handles `mode=compile` and emits `compile.completed:v1`.

Each helper first takes a per-`(release_id, mode)` transaction advisory lock (`LockRelease(release_id, mode)` → `pg_advisory_xact_lock(hashtext(release_id || ':' || mode))`) so the whole count→claim→emit sequence is serialized for that leg; the three legs of one release lock independently. The second caller for any leg blocks until the first commits and then evaluates against its committed state. This closes the lost-emission window where the dispatcher's at-dispatch failure path and `outcomes.Recorder`'s settle (or two replicas) bring the last two nodes terminal in overlapping transactions and, under READ COMMITTED, each reads the other as still pending and both no-op — hanging the release.

Under the lock the gate is a no-op while `PendingValidationCount(release_id, mode) > 0` (`pending`, `blocked`, and `deployed` are all non-terminal). Once every node for the leg is terminal (`outcome` is `ok`, `failed`, or `skipped`) the gate claims the `validation_aggregates` sentinel via `ClaimEmission(release_id, mode)`; the single winner reads the per-node results to compute the aggregate status, builds the decision payload, and writes one outbox row whose `aggregate_id` is a deterministic UUIDv5 over an immutable namespace and `release:<release_id>:<mode>`, so any re-emission deduplicates downstream. For the validation leg the payload is the decision only (no per-node array); each node's content was already streamed as it settled via the `kind=node` messages on the same stream. The advisory lock guarantees exactly-once (never zero); the sentinel guarantees exactly-once (never double). Losers return without emitting.

### Outbox processor (every 1 second, batch of 100, per-aggregate FIFO)

`pkg/outbox.Processor` polls `execution_outbox` and calls `OutboxPublisher.Publish` per entry, with `PerAggregateFIFO` enabled — rows sharing an aggregate publish in insertion order, so validation ordering (a task's RUNNING announcement before its later rows) holds without a separate lock:

```
For each pending execution_outbox entry:
  1. Marshal payload (already a typed event struct), or convert to a delay-queue
     ticket for check_delayed
  2. XADD to the row's stream_name (or enqueue the ticket)
  3. MarkProcessed

On publish failure:
  - retry_count < max_retries: retry on next poll
  - retry_count >= max_retries: MarkFailed and write an outbox.dead_letter:v1 row
```

## Background Loops

| Loop | Description |
|---|---|
| Redis consumers (10 streams) | One tracked goroutine per stream, registered with the liveness registry as `query_model`, `schedule_cancelled`, `validation_requested`, `seed_build_requested`, `compile_requested`, `validation_result_teardown`, `pipeline_run_finished_teardown`, `release_promoted_teardown`, `release_rejected_teardown`, `check_k8s`; each carries a heartbeat probe and, for the six schema-op consumers, a longer handler timeout and pending-entry reclaim window (see Kubernetes API → `RunSchemaOpJob`) |
| Deploy dispatcher (`deployer.Dispatcher`, worker `deploy_dispatcher`) | Polls `deployments` every 5 seconds; creates K8s Jobs and writes outbox announcement rows, capped by `MAX_CONCURRENT_JOBS`, up to 50 rows per tick |
| Outbox processor (`pkg/outbox.Processor`, worker `outbox_processor`) | Polls `execution_outbox` every second; publishes up to 100 entries per batch via `OutboxPublisher`, `PerAggregateFIFO` |
| Delay-queue promoter (`worker delayqueue_promoter`) | Ticks every second; one atomic Lua script moves every due ticket (score ≤ now) from `checkk8s:pending` into `check.k8s:v1` |
| Cancelled-schedules sweeper (`worker cancelled_schedules_sweeper`) | Ticks every `CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES` (default 60); deletes `cancelled_schedules` rows older than `CANCELLED_SCHEDULES_TTL_HOURS` (default 24) |

## Reliability Patterns

- **Inbound dedup, three strategies**: standard `(message_id, stream_name)` for streams that carry no upstream identity of their own; outbox-entry-carried (`DedupWithOutboxEntryID`) for streams whose payload rides a producer's `outbox_entry_id`, so a row republished with a fresh Redis message id still deduplicates; deterministic release-derived keys for `validation.requested:v1`/`seed.build.requested:v1`/`compile.requested:v1`, which carry every node for a release in one message and so cannot dedup on the message id alone. `schedule.cancelled:v1` and the four candidate-schema teardown streams skip dedup because their effects (`INSERT ... ON CONFLICT DO NOTHING`, `DropCandidateSchema`) are naturally idempotent.
- **Canonical outbox**: every business decision writes its outbox row(s) inside the same transaction as the domain write that triggered them; `pkg/outbox.Processor` publishes each row independently, `PerAggregateFIFO`-ordered within an aggregate.
- **Delay-queue exactly-once promotion**: the promoter's Lua script moves a ticket from the ZSET/HASH to the stream and deletes the ticket in one atomic operation, so a promotion is never partially applied and never duplicated across replicas.
- **Cancelled-schedule guard**: a `cancelled_schedules` row, checked both at dispatch (`QueryModelHandler`) and at Job-status time (the job-status handler, including its retry branch), drops a schedule's dispatch or in-flight Job result without writing a `deployments` row or an outbox announcement; rows are swept after a configurable TTL.
- **Schema-op reclaim window**: the six consumers whose handlers block on a candidate-schema Job run with a pending-entry reclaim `MinIdle` above their own handler timeout, so a peer replica's periodic sweep never re-claims a message whose handler is legitimately mid-wait on `RunSchemaOpJob`.
- **Unrecoverable-start-error detection**: `GetJobStatus` inspects pod (and init-container) `ContainerStatus.Waiting.Reason` for conditions the Kubernetes Job controller never resolves on its own (`ImagePullBackOff`, `ErrImagePull`, `InvalidImageName`, `CreateContainerConfigError`, `CreateContainerError`) and fails the Job cleanly instead of leaving it to loop and hang a release forever.
- **dbt no-op detection**: a Job whose pod exits 0 having matched no dbt models ("Nothing to do") is remapped from Succeeded to Failed by tailing its log, so a selector that matches nothing is not silently reported as a successful run.
- **Init-container log fallback**: when the main container never started because a preceding init container failed (e.g. the compile Job's `compile` init container), `GetPodLogs` falls back to that init container's logs so the error message still reaches the classifier instead of being silently swallowed.
- **Concurrency cap**: the dispatcher counts live K8s Jobs (`app=dbt-job`, `.status.active > 0`) on every tick and processes at most `max(0, MAX_CONCURRENT_JOBS - active)` rows; rows beyond the cap stay `pending` until the next tick.
- **K8s idempotency**: every `Create*Job` method treats already-exists as success, so a dispatcher restart or crash after K8s success but before commit re-attempts safely.
- **Dispatcher backoff**: transient K8s failures reschedule the row via `next_attempt_at` with exponential backoff (base 5s, cap 2 min).
- **Terminal failure propagation**: on `ErrPermanent` or retry-budget exhaustion, the dispatcher writes `task_status_updated` FAILED + `node_updated` FAILED (production) or records the node's outcome and runs the aggregate gate (candidate modes) before marking the row failed — ensuring `orchestrator`, `state`, and `release-controller` always learn of the terminal outcome.
- **Uniform outbox publisher**: `OutboxPublisher` is a plain marshal-and-XADD (or delay-queue enqueue for `check_delayed`); no Job-creation or Job-observation logic runs in the publisher.
- **No `state` gRPC dependency**: this service does not call `state` gRPC; all state mutations flow via `task.status.updated:v1` and `task.execution.recorded:v1` Redis events.
- **S3 soft-fail**: log and result-block upload failure does not block task completion; the execution record is written with an empty S3 key.
- **Job GC backstop**: every dbt/validation/seed-build/compile Job carries `TTLSecondsAfterFinished: 86400`, so Kubernetes deletes a terminal Job and its pod 24h after it finishes — long enough to inspect a failed pod directly (`kubectl describe`/`kubectl logs`) but bounded, instead of one-shot Job pods accumulating on the cluster indefinitely.
