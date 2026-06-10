# Service Ownership Quick Reference

This sheet is the fastest way to answer three questions for each service:

- What durable state does it own?
- Which gRPC server surface does it own?
- Which Redis streams does it consume and produce?

Use this before diving into the full service dossiers.

## Port Ownership Convention

All Go services follow the same layering rule for ports and adapters:

| What | Where |
|---|---|
| Domain repository ports (collection-like aggregate abstractions, e.g. `RunRepository`, `CancelledSchedulesRepository`) | `<service>/domain/repository` |
| Technical / application ports (non-domain collaborators, e.g. `LogUploader`, `OutboxPublisher`, `Clock`) | `<service>/service/ports` |
| `UnitOfWork` interface | `<service>/service/uow` |
| Concrete implementations — including every `*UnitOfWork` | `<service>/adapters/*` |

The dependency arrow always runs adapter → port. Code under `<service>/service/handlers` imports no `adapters/*` package; every collaborator is reached through a port interface. The AST guard `TestServiceHandlersDoNotImportAdapters` in `pkg/streams/handler_imports_test.go` enforces this at CI time.

## Startup Environment Validation

All Go services use `pkg/config.Validator` to validate required environment variables before accepting any traffic or opening connections.

Pattern (in every Go service `main.go`):
```go
v := &pkgconfig.Validator{}
cfg := config.Load(v)
if missing := v.Missing(); len(missing) > 0 {
    logger.Error("missing required env vars", "vars", strings.Join(missing, ", "))
    os.Exit(1)
}
```

`LoadPostgres`, `LoadRedis`, `LoadRedisFromAddr`, and `LoadS3` all accept a `*Validator` and register any missing required key into it. Optional keys with safe defaults use the package-private `env`/`envInt` helpers instead.

**Tiers:**
- **Tier 1 (required)**: recorded via `v.Require` / `v.RequireInt`; missing -> process exits with a single error listing all absent keys.
- **Tier 2 (with default)**: read via `env` / `envInt`; missing -> silently uses the default value.

`manifest-controller` (Python) performs the equivalent check at startup: it reads required env vars and raises a descriptive `RuntimeError` listing all missing keys before the event loop starts.

The process exits before any connection is attempted, so missing-config failures are immediately visible in `docker logs` or pod logs rather than surfacing as obscure connection errors.

## Bootstrap Migration Image

The dedicated Flyway image artifact runs as the `pre-upgrade` Helm hook for `continuo-app` and both provisions and migrates the per-service Postgres databases. For each service in `{state, executor, orchestrator, k8s, release}` it idempotently creates `continuo_<service>` if it does not already exist, then applies the SQL files under `db/migration/<service>` against that database. `db/migrate-all.sh` holds this database list as a single source of truth driving both the create step and the migrate step, so they cannot drift.

Provisioning databases inside the job — rather than relying solely on the Postgres `initdb` scripts, which run only when the data directory is first initialised — keeps provisioning correct on long-lived volumes: adding a new database never requires a manual `CREATE DATABASE` on an existing cluster. The migration user owns the databases it creates, so no additional grants are required. The image owns no runtime state; it is only the packaging and entrypoint for those migrations.

## `state`

| Category | Owned / used surface |
|---|---|
| Durable state | `scheduler_tracker` (+ `service_metadata` JSONB column), `task_tracker` (+ `manifest_version` column), `task_execution`, `schedule_catalog` (+ `service_metadata` JSONB column), `state_outbox`, `message_processing` |
| gRPC server methods owned | `CreateScheduler`, `GetScheduler`, `CancelScheduler`, `ActivateSchedule`, `ListAllSchedules`, `TriggerSchedule`, `CancelSchedule`, `TriggerRerun`, `TriggerRebase`, `TriggerSingleNodeRun`, `CreateTask`, `GetTask`, `GetTaskByScheduleAndNode`, `DeleteTask`, `ListTasks`, `GetSchedulerInitStatus`, `GetTaskExecution`, `ListTaskExecutions`, `ListNodes` (node catalog: per-node aggregate stats — run count, success rate, avg/p95 duration, flakiness, last run — over the most recent 50 runs, read from `task_tracker`/`scheduler_tracker`/`task_execution`; supports exact table-name + service filter and paging) |
| Redis consumes | `schedules.loaded:v1`, `run.entries.dispatched:v1`, `run.entries.dispatch_failed:v1`, `task.status.updated:v1`, `task.execution.recorded:v1` |
| Redis produces | `scheduler.started:v1`, `trigger.rerun:v1`, `trigger.rebase:v1`, `trigger.single_node_run:v1`, `run.finalized:v1`, `schedule.cancelled:v1` |
| Outbound gRPC calls | none |

> All internal pipeline writes (scheduler/task/init-status updates, task-execution records, in-progress initialisation resets) flow through Redis consumers. The gRPC surface is UI-facing reads + user-initiated commands only.

## `orchestrator`

| Category | Owned / used surface |
|---|---|
| Durable state | Neo4j `Table` nodes (+ `image_tag`, `topology_generation` props), `Run` nodes (+ `topology_generation`, `service_metadata` props), `DEPENDS_ON` edges, `EXECUTES` edges (+ `image_tag` prop); Neo4j `:TopologyRoot {id:'singleton'}` (generation + service_metadata); Postgres `topology_state`, `message_processing`, `orchestrator_outbox` |
| gRPC server methods owned | `GetScheduleGraph`, `ListRuns`, `GetRunGraph`, `ListActiveRunDrifts` |
| Redis consumes | `node.updated:v1`, `release.promoted:v1`, `scheduler.started:v1`, `trigger.rerun:v1`, `trigger.rebase:v1`, `trigger.single_node_run:v1`, `run.finalized:v1` |
| Redis produces | `query.model:v1`, `schedules.loaded:v1`, `run.entries.dispatched:v1`, `run.entries.dispatch_failed:v1`, `task.status.updated:v1` (SKIPPED on cascade-skip of a downstream node) |
| Outbound gRPC calls | `state`: `ListAllSchedules`, `ListTasks`, `CancelSchedule` (watchdog only) |

### Invariants

- **Topology swap on promotion.** `ReleasePromotedHandler` consumes
  `release.promoted:v1` and calls `ReleasePromotionRepository.PromoteRelease`
  to swap the Neo4j topology. `image_tag` arrives already populated:
  `manifest-controller` leaves it empty and `release-controller` joins the
  per-service tags it assembled for the release onto the topology before
  promotion, so there is no orchestrator-side `image_tag` rejection.
- **Dispatch watchdog.** Periodic loop terminates `is_running=true`
  schedules that have no task in `RUNNING` and no task progress within
  `ORCHESTRATOR_WATCHDOG_NO_PROGRESS_MINUTES` (default 30m), via the
  established `state.CancelSchedule` cancellation pathway — no new
  terminal state introduced. See sequence flow §8.

## `executor-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | `executor_deployments`, `executor_outbox`, `message_processing`, `cancelled_schedules`, `validation_aggregates` |
| gRPC server methods owned | none |
| Redis consumes | `query.model:v1`, `retry.task:v1`, `schedule.cancelled:v1`, `validation.requested:v1`, `validation.node.completed:v1`, `validation.completed:v1` |
| Redis produces | `node.deployed:v1`, `task.status.updated:v1` (RUNNING on success; FAILED on terminal dispatch failure), `node.updated:v1` (FAILED on terminal dispatch failure only), `validation.completed:v1` (per-release validation aggregate) |
| External DB writes | dbt warehouse (`DBT_POSTGRES_DB`) — drops `_candidate_<release>` schema on `validation.completed:v1` via `CandidateSchemaCleaner` |
| Outbound gRPC calls | none |

### Invariants

- **Inbound handlers write only to `executor_deployments`.** `QueryModelHandler` and `RetryTaskHandler` commit a `pending` row inside their Unit-of-Work transaction. No Kubernetes I/O occurs during inbound message handling.
- **Concurrency is capped by live K8s Jobs.** `deployer.Dispatcher` counts Jobs with label `app=dbt-job` and `.status.active > 0` on every tick. It processes at most `max(0, MAX_CONCURRENT_JOBS - active)` rows per cycle; rows beyond the cap remain `pending`.
- **Validation rows start `blocked` or `pending`.** Each per-node `executor_deployments` row written by `ValidationRequestedHandler` starts `pending` (no in-set upstreams) or `blocked` (has in-set upstreams — intra- or cross-service — that are not yet `ok`). The dispatcher only dispatches `pending` rows.
- **Topological unblock/skip on node completion.** `ValidationNodeCompletedHandler` transitions `blocked` downstreams whose every in-set upstream is now `ok` to `pending` (ready for dispatch); on a node failure it marks transitively `blocked` downstreams `skipped` (terminal non-`ok`). `blocked` is non-terminal; `skipped` fails the release.
- **Permanent dispatch failures bypass the retry budget.** `dispatchRow` classifies `CreateQueryJob` errors via `errors.Is(err, events.ErrPermanent)`. On match, `writeFailed` is called immediately regardless of remaining retries, writing `task.status.updated:v1` FAILED + `node.updated:v1` FAILED outbox rows and marking the deployment `failed`.
- **Retry-exhaustion uses the same propagation.** When `retry_count + 1 >= max_retries` on a transient error, `writeFailed` is called, so transient errors that exhaust the retry budget also reach orchestrator's `HandleNodeCompleted` (via `node.updated:v1`) and state's `TaskStatusUpdatedHandler` (via `task.status.updated:v1`).
- **Uniform outbox publisher.** The executor `OutboxPublisher` is a marshal-and-XADD; it has no `TerminalFailureHook` and carries no K8s logic. All failure signalling is performed upstream by the dispatcher.
- **Candidate schema teardown.** A dedicated consumer on `validation.completed:v1` (group `executor-validation-completed`) calls `CandidateSchemaCleaner.DropCandidateSchema` against `DBT_POSTGRES_DB`; this drops the shared `_candidate_<release>` schema regardless of pass/fail outcome.

## `k8s-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | `k8s_outbox`, `message_processing` |
| gRPC server methods owned | none |
| Redis consumes | `node.deployed:v1`, `check.k8s:v1`, `schedule.cancelled:v1` |
| Redis produces | `check.k8s:v1`, `retry.task:v1`, `task.failed:v1`, `task.status.updated:v1` (SUCCEEDED/FAILED), `task.execution.recorded:v1`, `node.updated:v1` |
| Outbound gRPC calls | none |

## `manifest-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | none |
| gRPC server methods owned | none |
| Redis consumes | `release.requested:v1` |
| Redis produces | `manifest.loaded.candidate:v1` |
| Outbound gRPC calls | none |

## `release-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | Postgres `releases` (per-candidate state, `changed_service`, assembled per-service `image_tags`, candidate topology, validation results, transitions), `current_prod` (singleton live `topology_snapshot` + promoted `release_id`), `service_prod` (one row per dbt service: live `manifest_s3_key` + `image_tag` + `release_id`), `message_processing`, `release_controller_outbox` |
| HTTP server | `POST /releases` (single-service candidate), `GET /releases`, `GET /releases/{id}`, `GET /current-prod`, `GET /healthz` |
| gRPC server methods owned | none |
| Redis consumes | `manifest.loaded.candidate:v1`, `validation.completed:v1` |
| Redis produces | `release.requested:v1`, `validation.requested:v1`, `release.promoted:v1`, `release.rejected:v1` |
| Outbound gRPC calls | none |

### Invariants

- **One service per release.** `POST /releases` accepts `{service, release_id, image_tag, bootstrap?}` — a delta for a single dbt service. The full per-service manifest set and image-tag map are assembled later, at activation, never at receipt.
- **Assembly reads live `service_prod`.** When a release transitions to `Parsing`, the queue advance combines the changed service's new canonical manifest key with every other service's current `service_prod` pointer. Reading at activation (not receipt) reflects any promotion an earlier-queued release made meanwhile.
- **Promotion refreshes the pointer.** Every promotion path (validation-passed, bootstrap, empty-diff) upserts the changed service's `service_prod` row (canonical key + image tag + release id) in the same transaction that updates `current_prod`.
- **Activation requires full coverage.** A release does not activate while any service live in `current_prod` lacks a `service_prod` pointer (and is not the changed service); it stays queued until the pointers are seeded. This prevents a populated-`current_prod`/empty-`service_prod` state from assembling a partial topology and retiring the unpointered services on promotion.

## `ui-service`

| Category | Owned / used surface |
|---|---|
| Durable state | none |
| gRPC server methods owned | none |
| Redis consumes | none |
| Redis produces | none |
| Outbound gRPC calls | `state`: `ListAllSchedules`, `ListTasks`, `GetScheduler`, `ListTaskExecutions`, `ListNodeRuns`, `ListNodes`, `TriggerRerun`, `TriggerRebase`, `TriggerSingleNodeRun`, `TriggerSchedule`, `CancelSchedule`; `orchestrator`: `GetScheduleGraph`, `ListRuns`, `GetRunGraph` |

## `continuo CLI`

A standalone command-line client, invoked by humans and LLM agents rather than run as a Docker Compose service. It owns no storage and constructs no Redis client; it reaches the system exclusively through public gRPC.

| Category | Owned / used surface |
|---|---|
| Durable state | none |
| gRPC server methods owned | none |
| Redis consumes | none |
| Redis produces | none |
| Outbound gRPC calls | `state`: `ListAllSchedules`, `ListTasks`, `TriggerSchedule`; `orchestrator`: `GetScheduleGraph` |
