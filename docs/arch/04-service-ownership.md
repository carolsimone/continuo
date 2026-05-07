# Service Ownership Quick Reference

This sheet is the fastest way to answer three questions for each service:

- What durable state does it own?
- Which gRPC server surface does it own?
- Which Redis streams does it consume and produce?

Use this before diving into the full service dossiers.

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

The dedicated Flyway image artifact sequentially applies the SQL files under `db/migration/{state,executor,orchestrator,k8s}` against the corresponding `continuo_*` databases. It owns no runtime state; it is only the packaging and entrypoint for those migrations.

## `state`

| Category | Owned / used surface |
|---|---|
| Durable state | `scheduler_tracker` (+ `service_metadata` JSONB column), `task_tracker` (+ `manifest_version` column), `task_execution`, `schedule_catalog` (+ `service_metadata` JSONB column), `state_outbox`, `processed_events` |
| gRPC server methods owned | `CreateScheduler`, `GetScheduler`, `CancelScheduler`, `ActivateSchedule`, `ListAllSchedules`, `TriggerSchedule`, `CancelSchedule`, `TriggerRerun`, `CreateTask`, `GetTask`, `GetTaskByScheduleAndNode`, `DeleteTask`, `ListTasks`, `ResetTask`, `GetSchedulerInitStatus`, `GetTaskExecution`, `ListTaskExecutions` |
| Redis consumes | `schedules.loaded:v1`, `run.entries.dispatched:v1`, `run.rerun.dispatched:v1`, `task.status.updated:v1`, `task.execution.recorded:v1` |
| Redis produces | `scheduler.started:v1`, `rerun:v1`, `run.finalized:v1`, `schedule.cancelled:v1` |
| Outbound gRPC calls | none |

> Internal pipeline writes that previously crossed gRPC (`UpdateScheduler`, `UpdateSchedulerInitStatus`, `UpdateTask`, `CreateTaskExecution`, `ResetInProgressInitializations`) are now event-driven via Redis consumers. The remaining gRPC surface is UI-facing reads + user-initiated commands only.

## `orchestrator`

| Category | Owned / used surface |
|---|---|
| Durable state | Neo4j `Table` nodes (+ `image_tag`, `topology_generation` props), `Run` nodes (+ `topology_generation`, `service_metadata` props), `DEPENDS_ON` edges, `EXECUTES` edges (+ `image_tag` prop); Neo4j `:TopologyRoot {id:'singleton'}` (generation + service_metadata); Postgres `topology_state`, `message_processing`, `outbox`, `published_messages` |
| gRPC server methods owned | `GetScheduleGraph`, `ListRuns`, `GetRunGraph`, `ListActiveRunDrifts` |
| Redis consumes | `node.updated:v1`, `manifest.loaded:v1`, `initialize.run:v1`, `scheduler.started:v1`, `rerun:v1` |
| Redis produces | `query.model:v1`, `schedules.loaded:v1`, `run.entries.dispatched:v1`, `run.rerun.dispatched:v1` |
| Outbound gRPC calls | `state`: `ListAllSchedules`, `ListTasks`, `CancelSchedule` (watchdog only) |

### Invariants

- **Ingest-time validation.** `IngestTopologyHandler` rejects any
  `manifest.loaded:v1` batch carrying an empty `image_tag`. Rejections are
  durable (`rejected_topology_messages` Postgres table) and ACKed in Redis
  to keep the pending list clean. The validator runs before any side
  effect; on rejection the consumer (`adapters/redis/consumer.go`)
  recognises `events.ErrPermanent` via `errors.Is` and ACKs.
- **Dispatch watchdog.** Periodic loop terminates `is_running=true`
  schedules that have no task in `RUNNING` and no task progress within
  `ORCHESTRATOR_WATCHDOG_NO_PROGRESS_MINUTES` (default 30m), via the
  established `state.CancelSchedule` cancellation pathway — no new
  terminal state introduced. See sequence flow §8.

## `executor-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | `deployment_outbox` (+ `image_tag` column), `processed_events` |
| gRPC server methods owned | none |
| Redis consumes | `query.model:v1`, `retry.task:v1`, `schedule.cancelled:v1` |
| Redis produces | `node.deployed:v1`, `task.status.updated:v1` (RUNNING; **also FAILED on permanent dispatch error or retry-exhaustion**), `node.updated:v1` (FAILED on terminal dispatch failure only) |
| Outbound gRPC calls | none |

### Invariants

- **Permanent dispatch failures take the terminal-failure branch on
  attempt 1.** `processEntry` classifies `CreateQueryJob` errors via
  `errors.Is(err, events.ErrPermanent)`. On match, calls
  `MarkTaskTerminallyFailed` (publishes `task.status.updated:v1` FAILED
  + `node.updated:v1` FAILED + marks outbox failed) and returns
  `errPermanentFailure`. No retry budget consumed for deterministic
  errors like `image_tag missing`.
- **Retry-exhaustion uses the same propagation.** `ProcessBatch`'s
  retry-exhaustion branch calls `MarkTaskTerminallyFailed` instead of
  bare `MarkFailed`, so transient errors that exceed the retry budget
  also reach orchestrator's `CheckScheduleCompletion`.

## `k8s-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | `k8s_status_outbox`, `processed_events` |
| gRPC server methods owned | none |
| Redis consumes | `node.deployed:v1`, `check.k8s:v1`, `schedule.cancelled:v1` |
| Redis produces | `check.k8s:v1`, `retry.task:v1`, `task.failed:v1`, `task.status.updated:v1` (SUCCEEDED/FAILED), `task.execution.recorded:v1`, `node.updated:v1` |
| Outbound gRPC calls | none |

## `manifest-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | none |
| gRPC server methods owned | none |
| Redis consumes | `update.graph:v1` |
| Redis produces | `manifest.loaded:v1` |
| Outbound gRPC calls | none |

### Invariants

- **Publish-time validation.** Refuses to publish `manifest.loaded:v1`
  if any node has an empty `image_tag`. The triggering `update.graph:v1`
  is ACKed regardless — redelivery cannot help when the source of truth
  (S3) hasn't changed. Logs structured ERROR
  `event=manifest_publish_rejected` with `missing_image_tag_count`,
  `total_node_count`, and `offenders`.

## `ui-service`

| Category | Owned / used surface |
|---|---|
| Durable state | none |
| gRPC server methods owned | none |
| Redis consumes | none |
| Redis produces | `update.graph:v1` |
| Outbound gRPC calls | `state`: `ListAllSchedules`, `ListTasks`, `GetScheduler`, `ListTaskExecutions`, `TriggerRerun`, `TriggerSchedule`; `orchestrator`: `GetScheduleGraph`, `ListRuns`, `GetRunGraph`, `ListActiveRunDrifts` |
