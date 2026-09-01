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

The dependency arrow always runs adapter → port. Application-layer code imports no `adapters/*` package; every collaborator is reached through a port interface. The AST guard `TestServiceHandlersDoNotImportAdapters` in `pkg/streams/handler_imports_test.go` enforces this at CI time over each service's handler packages and its `service/uow` package — the `UnitOfWork` port is application vocabulary, so the package declaring it must not reach back into an adapter either. Test files are exempt, because integration tests wire concrete implementations on purpose.

Every `*UnitOfWork` shares the same transaction lifecycle contract, so an instance can be reused safely across messages: `Commit` and `Rollback` clear the in-flight transaction state unconditionally, even when the underlying operation fails. `database/sql` marks a transaction done even when its `Commit` returns an error, so a handler's deferred `Rollback` that follows a failed `Commit` sees `sql.ErrTxDone`; `Rollback` treats that as a successful no-op. The result is that a single failed commit can never leave a `UnitOfWork` stuck in "transaction already in progress" — the next `Begin` always starts cleanly, whether the service reuses one long-lived instance per consumer (orchestrator) or constructs one per message (state, k8s-controller, executor-controller, release-controller).

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

`topology-controller` (Python) performs the equivalent check at startup: it reads required env vars and raises a descriptive `RuntimeError` listing all missing keys before the event loop starts.

The process exits before any connection is attempted, so missing-config failures are immediately visible in `docker logs` or pod logs rather than surfacing as obscure connection errors.

## Graceful Shutdown Convention

`state`, `orchestrator`, `executor-controller`, `k8s-controller`, and `agent-chat` drive process shutdown through the shared `pkg/lifecycle.ApplicationLifecycle`. `release-controller`, `remediation`, and `agent-remediation` are not on it; each installs `signal.Notify` directly in its own `main` instead.

On SIGTERM/SIGINT, `ApplicationLifecycle` runs an ordered sequence whose total duration is bounded by `SHUTDOWN_GRACE` (default 15s; `agent-chat` defaults to 10s) for handlers that honor their context — a `Close()`-style handler that ignores `ctx` is not itself bounded by it:

1. **Stop intake** — cancel the root context so consumers and background loops stop reading new work.
2. **Drain** — wait on a `WaitGroup` for goroutines tracked via `ApplicationLifecycle.Go(...)` to return, bounded by half the grace period.
3. **Close infra** — run the registered shutdown handlers, in LIFO order, against a fresh context derived from `context.Background()`, never the just-cancelled root context. This context runs to a deadline fixed at the moment `Shutdown` starts, while step 2 is capped independently at half the grace budget; together the two keep infra teardown retaining at least the other half of the budget, live, even when the drain used its entire share.

`Done()` then closes and `main` blocks on it instead of on `<-ctx.Done()`, so there is no fixed sleep.

Three properties of this sequence matter beyond the mechanics:

- **Step 1 aborts in-flight work; it does not complete it.** The root context is threaded straight into the handler, so cancellation makes the in-flight handler's next database call fail, its transaction roll back, and the message stay un-ACKed for redelivery. The drain in step 2 waits for tracked goroutines to unwind, not for their work to finish — correctness comes from at-least-once redelivery plus dedup, not from finishing the message before shutdown.
- **`Go(...)` refuses to start new tracked work once shutdown has begun.** It checks the shutdown flag and adds to the `WaitGroup` under the same mutex `Shutdown` uses to set that flag, so every `Add` either happens-before the drain's `Wait` or is refused outright — never racing it. A caller still holding a reference to `ApplicationLifecycle` during teardown gets a logged warning instead of a goroutine that escapes the drain or a `WaitGroup` misuse panic.
- **`RegisterShutdownHandler(...)` refuses new registrations once shutdown has begun, for the same reason.** `Shutdown` sets the flag and takes a snapshot of the handler slice under one lock in `RegisterShutdownHandler`'s critical section, so every append either happens-before that snapshot or is refused outright — never racing the read step 3 iterates. A caller registering after that point gets a logged warning instead of a handler that silently never runs.
- **A server whose only stopper is a shutdown handler must never be tracked with `Go(...)`.** Such a server blocks in its start call until the handler registered via `RegisterShutdownHandler` calls `Shutdown`, and that handler only runs in step 3, after the drain. Tracking the server's start call in `Go(...)` would make `wg.Wait()` wait on a goroutine that cannot return before the wait itself completes, so every shutdown would burn the full drain budget. The AST guard `TestLifecycleGoNeverWrapsAServerStart` in `pkg/streams/lifecycle_wiring_test.go` detects a blocking-server method (`Start`/`Serve`/`ListenAndServe`/`ListenAndServeTLS`/`Run`) called on a stoppable receiver inside an inline `func(){}` literal passed to `Go(...)`, in every service `main.go` that registers at least one shutdown handler.

## Bootstrap Migration Image

The dedicated Flyway image artifact runs as the `pre-upgrade` Helm hook for `continuo-app` and both provisions and migrates the per-service Postgres databases. For each service in `{state, executor, orchestrator, k8s, release}` it idempotently creates `continuo_<service>` if it does not already exist, then applies the SQL files under `db/migration/<service>` against that database. `db/migrate-all.sh` holds this database list as a single source of truth driving both the create step and the migrate step, so they cannot drift.

Provisioning databases inside the job — rather than relying solely on the Postgres `initdb` scripts, which run only when the data directory is first initialised — keeps provisioning correct on long-lived volumes: adding a new database never requires a manual `CREATE DATABASE` on an existing cluster. The migration user owns the databases it creates, so no additional grants are required. The image owns no runtime state; it is only the packaging and entrypoint for those migrations.

## `state`

| Category | Owned / used surface |
|---|---|
| Durable state | `scheduler_tracker` (+ `service_metadata` JSONB column), `task_tracker` (+ `manifest_version` column), `task_execution`, `schedule_catalog` (+ `service_metadata` JSONB column), `state_outbox`, `message_processing` |
| gRPC server methods owned | `GetScheduler`, `CancelScheduler`, `ActivateSchedule`, `ListAllSchedules`, `TriggerSchedule`, `CancelSchedule`, `TriggerRerun`, `TriggerRebase`, `TriggerSingleNodeRun`, `GetTask`, `GetTaskByScheduleAndNode`, `ListTasks`, `GetSchedulerInitStatus`, `GetTaskExecution`, `ListTaskExecutions`, `ListNodes` (node catalog: per-node aggregate stats — run count, success rate, avg/p95 duration, flakiness, last run — over the most recent 50 runs, read from `task_tracker`/`scheduler_tracker`/`task_execution`; supports exact table-name + service filter, the `run`\|`test`\|`build` operation dimension, and paging — a node absent from the requested operation is absent from the catalog entirely, not returned with empty stats) |
| Redis consumes | `schedules.loaded:v1`, `run.entries.dispatched:v1`, `run.entries.dispatch_failed:v1`, `task.status.updated:v1`, `task.execution.recorded:v1`, `release.seeds.pending:v1` (creates the run for a release's changed seeds) |
| Redis produces | `scheduler.started:v1`, `trigger.rerun:v1`, `trigger.rebase:v1`, `trigger.single_node_run:v1`, `trigger.promoted_seeds:v1`, `run.finalized:v1`, `schedule.cancelled:v1` |
| Outbound gRPC calls | none |

> All internal pipeline writes (scheduler/task/init-status updates, task-execution records, in-progress initialisation resets) flow through Redis consumers. The gRPC surface is UI-facing reads + user-initiated commands only.

## `orchestrator`

| Category | Owned / used surface |
|---|---|
| Durable state | Neo4j `Table` nodes (+ `image_tag`, `content_hash`, `code_version_promoted_at`, `topology_generation` props), `Run` nodes (+ `topology_generation`, `service_metadata` props), `DEPENDS_ON` edges, `EXECUTES` edges (+ `image_tag`, `content_hash` props); the code-version graph — `NodeVersion` / `CodeUnit` / `CodeUnitVersion` nodes and their `CURRENT`, `USES_CODE` edges; the failure-precedent case base — `Rejection` / `ErrorSignature` / `Proposal` / `PullRequest` nodes and their `HAS_SIGNATURE`, `PROPOSED`, `HAS_PR`, `EDITED` edges, plus `[:FAILED {release_id}]` from an existing `Table`; `RESOLVED_BY` — a shared relationship type with two target-label families, written independently: `(:Rejection)-[:RESOLVED_BY]->(:NodeVersion)` (own-timeline resolution) and `(:Rejection)-[:RESOLVED_BY {amended, service}]->(:Proposal)` (merged-fix-PR provenance); Neo4j `:TopologyRoot {id:'singleton'}` (generation + service_metadata); Postgres `topology_state`, `message_processing`, `orchestrator_outbox` |
| gRPC server methods owned | `GetScheduleGraph`, `ListRuns`, `GetRunGraph`, `ListActiveRunDrifts`, `ListScheduleTopologies`, `GetNode`, `GetNodeLocation`, `GetNodeVersions`, `GetNodeVersionDiff`, `GetUpstreamChanges`, `GetCodeUnitVersions`, `GetNodeRunHistory`, `GetPrecedents` |
| Redis consumes | `node.updated:v1`, `release.promoted:v1`, `scheduler.started:v1`, `trigger.rerun:v1`, `trigger.rebase:v1`, `trigger.single_node_run:v1`, `trigger.promoted_seeds:v1`, `run.finalized:v1`, `remediation.requested:v2` (group `orchestrator-remediation-requested-rejections`), `remediation.pr_opened:v1` (group `orchestrator-remediation-pr-opened-proposals`), `remediation.pr_closed:v1` (group `orchestrator-remediation-pr-closed-provenance`) |
| Redis produces | `query.model:v1`, `schedules.loaded:v1`, `release.seeds.pending:v1`, `run.entries.dispatched:v1`, `run.entries.dispatch_failed:v1`, `task.status.updated:v1` (SKIPPED on cascade-skip of a downstream node) |
| S3 reads | `GetObject` — the code-bundle document at `code-bundles/<release_id>/bundle.json`: named by each `release.promoted:v1`'s `code_bundle_uri` on the version-ingestion consumer group, and by each `remediation.requested:v2`'s `code_bundle_uri` on the case-base rejections consumer group; read-only, and the bucket is not owned |
| Outbound gRPC calls | `state`: `ListAllSchedules`, `ListTasks`, `CancelSchedule` (watchdog only) |

### Invariants

- **Topology swap on promotion.** `ReleasePromotedHandler` consumes
  `release.promoted:v1` and calls `ReleasePromotionRepository.PromoteRelease`
  to swap the Neo4j topology. `image_tag` arrives already populated:
  `topology-controller` leaves it empty and `release-controller` joins the
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
| Redis consumes | `query.model:v1`, `retry.task:v1`, `schedule.cancelled:v1`, `validation.requested:v1`, `validation.node.completed:v1`, `validation.result:v1` (kind=complete, for candidate schema teardown) |
| Redis produces | `node.deployed:v1`, `task.status.updated:v1` (FAILED only, on the never-deployed terminal dispatch failure — k8s-controller owns RUNNING and the pod terminal), `node.updated:v1` (FAILED on terminal dispatch failure only), `validation.result:v1` (unified validation leg: `kind=node` per-node projections as each node settles, then a trailing `kind=complete` per-release decision) |
| External DB writes | None directly. Candidate-schema create/drop run as one-shot engine-image K8s Jobs (`CandidateSchemaCreator`/`CandidateSchemaCleaner` schedule them), so the executor holds no warehouse connection. The dbt job pods it creates for compile/seed-build/run receive the warehouse connection by `envFrom` of the operator-owned Secret named by `VALIDATION_WAREHOUSE_SECRET`; team dbt profiles read the Secret's engine-native keys. |
| Outbound gRPC calls | none |

### Invariants

- **Inbound handlers write only to `executor_deployments`.** `QueryModelHandler` and `RetryTaskHandler` commit a `pending` row inside their Unit-of-Work transaction. No Kubernetes I/O occurs during inbound message handling.
- **Concurrency is capped by live K8s Jobs.** `deployer.Dispatcher` counts Jobs with label `app=dbt-job` and `.status.active > 0` on every tick. It processes at most `max(0, MAX_CONCURRENT_JOBS - active)` rows per cycle; rows beyond the cap remain `pending`.
- **Validation rows start `blocked` or `pending`.** Each per-node `executor_deployments` row written by `ValidationRequestedHandler` starts `pending` (no in-set upstreams) or `blocked` (has in-set upstreams — intra- or cross-service — that are not yet `ok`). The dispatcher only dispatches `pending` rows.
- **Topological unblock/skip on node completion.** `ValidationNodeCompletedHandler` transitions `blocked` downstreams whose every in-set upstream is now `ok` to `pending` (ready for dispatch); on a node failure it marks transitively `blocked` downstreams `skipped` (terminal non-`ok`). `blocked` is non-terminal; `skipped` fails the release.
- **Permanent dispatch failures bypass the retry budget.** `dispatchRow` classifies `CreateQueryJob` errors via `errors.Is(err, events.ErrPermanent)`. On match, `writeFailed` is called immediately regardless of remaining retries, writing `task.status.updated:v1` FAILED + `node.updated:v1` FAILED outbox rows and marking the deployment `failed`.
- **Retry-exhaustion uses the same propagation.** When `retry_count + 1 >= max_retries` on a transient error, `writeFailed` is called, so transient errors that exhaust the retry budget also reach orchestrator's `HandleNodeCompleted` (via `node.updated:v1`) and state's `TaskStatusUpdatedHandler` (via `task.status.updated:v1`).
- **Uniform outbox publisher.** The executor `OutboxPublisher` is a marshal-and-XADD; it has no `TerminalFailureHook` and carries no K8s logic. All failure signalling is performed upstream by the dispatcher.
- **Candidate schema is created once, by the engine.** The `validation.requested:v1` binding calls `CandidateSchemaCreator.EnsureCandidateSchema` before enqueuing any node, which schedules a one-shot engine-image `ensure_schema` Job (warehouse Secret via `envFrom`) and blocks on it; the engine adapter owns idempotency (the postgres adapter takes a session advisory lock and tolerates a duplicate schema). The schema must be created explicitly because node validation is materialized by the engine directly — dbt runs only for seeds — so a nodes-only release never invokes dbt to create it. A failure aborts the message before any deployment row is written, and the message is retried.
- **Candidate schema teardown.** A dedicated consumer on `validation.result:v1` (group `executor-validation-result-teardown`) reacts to the `kind=complete` message and calls `CandidateSchemaCleaner.DropCandidateSchema`, which schedules a one-shot engine-image `drop_schema` Job; this drops the shared `_candidate_<release>` schema regardless of pass/fail outcome. A teardown failure is logged and ACKed so a leftover schema never blocks release finalization.

## `k8s-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | `k8s_outbox`, `message_processing` |
| gRPC server methods owned | none |
| Redis consumes | `node.deployed:v1`, `check.k8s:v1`, `schedule.cancelled:v1` |
| Redis produces | `check.k8s:v1`, `retry.task:v1`, `task.failed:v1`, `task.status.updated:v1` (RUNNING + SUCCEEDED/FAILED — the full pod lifecycle), `task.execution.recorded:v1`, `node.updated:v1` |
| Outbound gRPC calls | none |

## `topology-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | none (S3 objects at `candidate-sql/<release_id>/candidate_<unique_id>.<sql\|json>` and `code-bundles/<release_id>/bundle.json` are written but not owned; retention is managed by release-controller prune, backstopped by a 30-day S3 lifecycle rule on each of the `candidate-sql/` and `code-bundles/` prefixes) |
| gRPC server methods owned | none |
| Redis consumes | `release.requested:v1` (per-entry `kind` selects the parser — dbt manifest or python contract — for that `manifest_keys` entry; absent defaults to dbt) |
| Redis produces | `manifest.loaded.candidate:v1` (per-node `candidate_artifact_uri` — `s3://` reference to the object the node's validation Job fetches: rewritten SQL for a dbt node, a validation spec (reads + output_columns + config) for a python node, empty string for dbt seeds; top-level `code_bundle_uri` — `s3://` reference to the release's code-bundle contract document, empty string for an empty-manifest release) |
| S3 writes | `PutObject` to `candidate-sql/<release_id>/candidate_<unique_id>.sql` per non-seed dbt node; `PutObject` to `candidate-sql/<release_id>/candidate_<unique_id>.json` per python node; `PutObject` to `code-bundles/<release_id>/bundle.json` once per release; any of these failing is fatal and causes `status=failed` on `manifest.loaded.candidate:v1` |
| Outbound gRPC calls | none |

## `release-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | Postgres `releases` (per-candidate state — `received`, `compiling`, `parsing`, `seed_building`, `validating`, then one terminal status: `promoted`, `rejected`, `superseded`, or `validated` (shadow only) — `kind` (`dbt`/`python`), `shadow` (a fix-verification release posted by agent-remediation, immutable after receipt), `changed_service`, assembled per-service `image_tags`, candidate topology including per-node `candidate_artifact_uri`, the release-level `code_bundle_uri` set from the parse result, validation results, transitions, immutable provenance `repo` + `commit_sha`), `current_prod` (singleton live `topology_snapshot` + promoted `release_id`; `candidate_artifact_uri` is stripped on promotion), `service_prod` (one row per service, dbt or python: live `manifest_s3_key` + `manifest_kind` + `image_tag` + `release_id`), `message_processing`, `release_controller_outbox` |
| HTTP server | `POST /releases` (single-service candidate; requires `repo` + `commit_sha`; optional `kind` (`dbt`/`python`, default `dbt`), `bootstrap`, and `shadow`), `GET /releases`, `GET /releases/{id}` (returns `repo` + `commit_sha` + `shadow`), `GET /current-prod`, `GET /healthz` |
| gRPC server methods owned | none |
| Redis consumes | `compile.completed:v1`, `manifest.loaded.candidate:v1`, `seed.build.completed:v1`, `validation.result:v1` |
| Redis produces | `release.requested:v1`, `validation.requested:v1` (per node: `candidate_artifact_uri`), `release.promoted:v1` (top-level `code_bundle_uri`, `bootstrap`), `release.rejected:v1` (top-level `shadow`, telling the remediation classifier whether the rejection came from a fix-verification release; on `validation_failed` also includes top-level `repo` + `commit_sha` and, per node, `candidate_artifact_uri` plus the candidate topology's `node_type`, `file_path`, and `service`) |
| S3 writes | `DeleteObjects` — prune-time delete of `candidate-sql/<release_id>/` and `code-bundles/<release_id>/` prefixes per pruned release, both soft-fail and both backstopped by a 30-day S3 lifecycle rule on the respective prefix |
| Outbound gRPC calls | none |

### Invariants

- **One service per release.** `POST /releases` accepts `{service, release_id, image_tag, repo, commit_sha, kind?, bootstrap?, shadow?}` — a delta for a single service, dbt or python. `repo` (GitHub owner/name) and `commit_sha` (full SHA) are required provenance fields captured at receipt, stored as NOT NULL immutable columns, and returned on `GET /releases/{id}`. The full per-service manifest set and image-tag map are assembled later, at activation, never at receipt.
- **A shadow release never promotes.** A release received with `shadow: true` carries a fix proposed by agent-remediation that no human has approved. It runs the same parse -> candidate-schema -> validation pipeline as any other release and then stops at the terminal `validated` status, so it never writes `current_prod`, never upserts `service_prod`, and never emits `release.promoted:v1`. The `Release` aggregate enforces both halves: it refuses to promote a shadow release, and refuses to end a non-shadow release in `validated` (which is terminal and would strand it).
- **Assembly reads live `service_prod`.** When a release transitions to `Parsing`, the queue advance combines the changed service's new canonical manifest key with every other service's current `service_prod` pointer. Reading at activation (not receipt) reflects any promotion an earlier-queued release made meanwhile.
- **Promotion refreshes the pointer.** Every promotion path (validation-passed, bootstrap, empty-diff) upserts the changed service's `service_prod` row (canonical key + image tag + release id) in the same transaction that updates `current_prod`.
- **Activation requires full coverage.** A release does not activate while any service live in `current_prod` lacks a `service_prod` pointer (and is not the changed service); it stays queued until the pointers are seeded. This prevents a populated-`current_prod`/empty-`service_prod` state from assembling a partial topology and retiring the unpointered services on promotion.

## `ui`

| Category | Owned / used surface |
|---|---|
| Durable state | Redis `uisession:<id>` plain keys — server-side OIDC (OpenID Connect) login sessions (`AUTH_MODE=oidc`), TTL-bound, not streams |
| gRPC server methods owned | none |
| Redis consumes | none (no stream consumption; session keys only) |
| Redis produces | none (no stream production; session keys only) |
| Outbound gRPC calls | `state`: `ListAllSchedules`, `ListTasks`, `GetScheduler`, `ListTaskExecutions`, `ListNodeRuns`, `ListNodes`, `TriggerRerun`, `TriggerRebase`, `TriggerSingleNodeRun`, `TriggerSchedule`, `CancelSchedule`; `orchestrator`: `GetScheduleGraph`, `ListRuns`, `GetRunGraph`, `GetNode`; `agent-chat`: `AgentChat.Chat` (bidirectional streaming, `/ws/chat` relay, operator-only, feature-flagged by `CHAT_BRIDGE_ENABLED`); `agent-remediation`: `ListProposals`, `GetProposal`, `BeginPullRequest`, `RecordPullRequest`, `FailPullRequest` |
| External write | GitHub App API — create branch + one tree commit carrying every changed file + open pull request on `continuo-demo` (operator-only, one repo, `contents:write` + `pull-requests:write`) |

## `agent-chat`

| Category | Owned / used surface |
|---|---|
| Durable state | Postgres `continuo_agent_chat`: `threads` (conversation metadata per user), `messages` (full turn history per thread), `pending_actions` (tool calls awaiting human confirmation before execution) |
| gRPC server methods owned | `AgentChat.Chat` (bidirectional streaming; port 50053, cluster-internal) |
| Redis consumes | none |
| Redis produces | none |
| Outbound connections | LLM provider HTTPS (Anthropic Messages API, OpenAI, or any OpenAI-compatible endpoint; operator-configured via `LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL`, `LLM_BASE_URL`); S3 `PutObject` (optional chat-archive to `chat-archive/<user>/<thread>.json` on thread expiry, enabled when `RETENTION_ARCHIVE_S3=true`); `continuo` CLI subprocess (argv exec, no shell) which in turn calls `state` gRPC (port 50051) and `orchestrator` gRPC (port 50052) |

### Invariants

- **Tool catalog from CLI self-description.** At boot, agent-chat runs `continuo describe` and builds its tool catalog from the output. Adding a CLI command makes it available to the agent automatically without changes to agent-chat.
- **Direct argv exec, no shell.** CLI tools are executed by spawning the `continuo` binary directly via OS exec (no shell interposition). Arguments are validated against the catalog (membership check, schema check, no flag injection) before the process is created.
- **Read-only tools run immediately; mutating tools require confirmation.** The agent loop emits a `confirm_request` event to the client and persists a `PendingAction` row in Postgres before any tool annotated as mutating executes. Execution only proceeds on an explicit `approve` message from the client. No mutation can occur without a round-trip human approval.
- **No direct gRPC connections to backend services.** agent-chat never imports or holds connections to `state`, `orchestrator`, or any other service internals. All system reads happen through the `continuo` CLI subprocess, which uses the services' public gRPC interfaces.
- **Conversation persistence and retention.** Threads and messages are persisted in `continuo_agent_chat`; a background retention job deletes threads idle past `RETENTION_DAYS`. When `RETENTION_ARCHIVE_S3=true`, each thread is written to S3 as a JSON archive before deletion.

## `continuo CLI`

A standalone command-line client, invoked by humans and LLM agents rather than run as a Docker Compose service. It owns no storage and constructs no Redis client; it reaches the system exclusively through public gRPC.

| Category | Owned / used surface |
|---|---|
| Durable state | none |
| gRPC server methods owned | none |
| Redis consumes | none |
| Redis produces | none |
| Outbound gRPC calls | `state`: `ListAllSchedules`, `ListTasks`, `TriggerSchedule`, `CancelSchedule`, `ListNodeRuns`, `TriggerSingleNodeRun`; `orchestrator`: `GetScheduleGraph`, `GetNodeVersions`, `GetNodeVersionDiff`, `GetUpstreamChanges`, `GetCodeUnitVersions`, `GetNodeRunHistory` |
