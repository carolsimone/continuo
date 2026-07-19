# state

## Purpose

`state` is the single source of truth for orchestration state:

- scheduler run lifecycle (`scheduler_tracker`)
- task lifecycle (`task_tracker`)
- task execution records (`task_execution`)
- schedule catalog (`schedule_catalog`)

State mutates these records in two ways: via gRPC for UI-facing reads and user-initiated commands, and via Redis event consumers for all internal pipeline writes. No other service writes to these tables directly.

## Owned Storage

| Table | Purpose |
|---|---|
| `scheduler_tracker` | One row per schedule run; owns status and init lifecycle; includes `total_task_count` and `terminal_task_count` columns |
| `task_tracker` | One row per task within a run |
| `task_execution` | One row per execution attempt of a task |
| `schedule_catalog` | Active schedule names derived from manifests; soft-delete by `removed_at` |
| `state_outbox` | Canonical transactional outbox — one row per Redis publish intent; `pkg/outbox.Processor` polls and publishes each row to its `stream_name` |
| `message_processing` | Inbound dedup: one row per consumed Redis message ID, scoped by `stream_name`; tracks state (`processing` / `completed` / `acked`) |

### Aggregate boundaries

Two domain aggregates own the write-side logic for these tables:

- **`run.Run`** (`state/domain/aggregate/run`) — owns `scheduler_tracker` and its associated `task_tracker` rows. All scheduler and task status transitions, the finalization condition check, and `terminal_task_count`/`total_task_count` bookkeeping are encapsulated in `Run.RecordTaskStatus` and related methods. The application layer loads and persists `Run` through the `repository.RunRepository` port (`state/domain/repository`), implemented by `postgres.RunRepositoryAdapter`.
- **`catalog.ScheduleCatalog`** (`state/domain/aggregate/catalog`) — owns `schedule_catalog`. Reconciliation (upsert active, soft-delete absent) is encapsulated in `ScheduleCatalog.Reconcile`, which rejects an empty `scheduleNames` list as a safety guard against inadvertently wiping all catalog entries.

Row carrier structs for Postgres (`SchedulerTracker`, `TaskTracker`, `TaskExecution`) live in `state/adapters/postgres` — they are infrastructure types used for scanning database rows, not domain objects.

### New columns on `scheduler_tracker`

| Column | Type | Purpose |
|---|---|---|
| `total_task_count` | `integer` | Set when `run.entries.dispatched:v1` is consumed; total number of tasks in the run |
| `terminal_task_count` | `integer` | Incremented by the finalization state machine each time a task reaches a terminal state |
| `kind` | `character varying(20)` NOT NULL DEFAULT `'cron'` | Run discriminator. CHECK constraint allows: `cron`, `trigger`, `rerun`, `rebase`, `single_node_run`. Set when the row is inserted and immutable thereafter — `rerun` and `rebase` rows are minted as fresh trackers, never by mutating an existing one. Migration: V15. |
| `source_run_id` | `uuid` NULL | Lineage pointer to a parent run. NULL for `cron`/`trigger`. Populated for `rerun`, `rebase`, and stale-mode `single_node_run`. Not a foreign key — orphans are fine. Migration: V15. |
| `initiated_by` | `text` NOT NULL DEFAULT `'system'` | The user who initiated the run, or the `system` sentinel for cron / platform-initiated runs. Stamped at row creation from the gRPC `x-continuo-user-id` metadata header (see "User provenance" below); immutable thereafter. `text` (not a fixed width) because an OIDC `issuer-host\|sub` identifier can exceed 255 characters. Migration: V26. |
| `operation` | `varchar(10)` NOT NULL DEFAULT `'run'`, CHECK `IN ('run','test','build')` | The dbt verb this run applies to its nodes — `run` (model), `test`, or `build`. Stamped at activation from the triggering request/event (`ActivateSchedule`/`TriggerSchedule`/`TriggerSingleNodeRun`, etc.) and immutable thereafter. `ListNodes`/`ListNodeRuns` filter the node catalog on this dimension (denormalized onto `task_tracker.operation` at dispatch — see below). Migration: V28. |

### New columns on `task_tracker`

| Column | Type | Purpose |
|---|---|---|
| `image_tag` | `character varying(255)` NOT NULL DEFAULT `''` | Per-task audit pair to `manifest_version`. Pinned at task creation by `task_repository.Create` from the upstream event payload; never mutated. Migration: V16. |
| `inherited_from_task_id` | `uuid` NULL | Lineage pointer for rebase-projected rows. `NULL` = a real execution (cron/trigger/rerun/rebased/single-node). Non-NULL = projected inherit from a rebase parent run; the row was never executed in this run, only carried forward from a SUCCEEDED ancestor. **Resolve-to-root semantics:** the value always points to the ROOT executed `task_id` — chain depth is bounded at 1 forever, including rebase-of-rebase (the projector resolves transitively at write time). Not a foreign key — the referenced row may eventually be sweep-deleted; orphan inherits are tolerated by readers. Migration: V17. |
| `operation` | `varchar(10)` NOT NULL DEFAULT `'run'`, CHECK `IN ('run','test','build')` | The dbt verb this task ran — `run` (model), `test`, or `build`. Denormalized from the owning run's `scheduler_tracker.operation` when the task row is created (`RunEntriesDispatchedHandler`, at dispatch); immutable thereafter. Every `ListNodes`/`ListNodeRuns` query filters on this column, so a node's model-run stats and test-run stats never blend. Migration: V28. |

### Parse-cache columns on `task_execution`

| Column | Type | Purpose |
|---|---|---|
| `parse_cache` | `varchar(16)` NULL | Whether the executing Job's team container ran with the hydrated partial-parse cache: `hydrated` / `degraded` / `unknown`. NULL for executions that predate hydration or whose Job had no `hydrate-parse-cache` initContainer (e.g. `S3_BUCKET` unset). Persisted verbatim from the `parse_cache` field on `task.execution.recorded:v1`, which k8s-controller derives from that initContainer's termination message. Migration: V30. |
| `parse_cache_reason` | `text` NULL | The degrade reason, set only when `parse_cache='degraded'` (e.g. an S3 fetch failure). NULL otherwise. Migration: V30. |

### `scheduler_tracker` indexes for schedule_name access

| Index | Definition | Purpose |
|---|---|---|
| `uq_scheduler_tracker_active_per_schedule` | UNIQUE `(schedule_name) WHERE status IN ('pending','running')` | Partial unique index enforcing one active run per schedule. The DB-level backstop for the activation check-then-act race; a violating insert (SQLSTATE 23505) is mapped to `run.ErrScheduleHasActiveRun`. Migration: V25. |
| `idx_scheduler_tracker_schedule_name_created` | `(schedule_name, created_at DESC)` | Serves the active-run lookups (`HasActiveSchedule`, `GetActiveScheduler`) and the `DISTINCT ON (schedule_name) … ORDER BY schedule_name, created_at DESC` last-run scan behind `ListAllSchedules` / `ListStuckCandidates`. Migration: V25. |

> Pre-flight for V25: existing data must contain no `schedule_name` with more than one row in `status IN ('pending','running')`, or the unique index creation fails. The migration file documents the verification query.

### Run kind values

The `scheduler_tracker.kind` enum (also surfaced as the `kind` field on `:Run` in Neo4j and in the `scheduler.started:v1` outbox payload) discriminates run semantics:

- `cron` — cron-triggered activation; uniform metadata. Today every non-rerun run lands here.
- `trigger` — manual API trigger (reserved; not yet wired in v1).
- `rerun` — re-execute non-SUCCEEDED tasks + their descendants against the source run's pinned snapshot. `TriggerRerun` mints a new `scheduler_tracker` row on the source's schedule (`kind='rerun'`, `source_run_id=<source>`). The source row is left at its terminal `FAILED`/`CANCELLED` status as an immutable historical record.
- `rebase` — re-execute failed/cancelled tasks + their descendants + new arrivals against latest topology; inherit successful tasks with their pinned metadata. `TriggerRebase` mints a new `scheduler_tracker` row on the source's schedule (`kind='rebase'`, `source_run_id=<source>`).
- `single_node_run` — exactly one task; latest metadata by default, stale via picker.

### User provenance

Every run records the user who initiated it. The authenticated identity originates
at the HTTP edge (`ui-service`, the OIDC relying party) as the stable
`issuer-host|sub` user id and is carried into `state` as the gRPC metadata header
`x-continuo-user-id` (the single canonical key, defined in `pkg/identity`). A unary
server interceptor (`identity.UnaryServerInterceptor`) extracts the header at the
gRPC boundary and places an `identity.Identity` value on the request context; the
gRPC handlers read it via `identity.FromContext` and pass it into the
application/use-case layer, which stamps it onto the `run.Run` aggregate at creation
(`NewPendingRun` / `NewDerivedRun` / `NewSingleNodeRun`). It persists to
`scheduler_tracker.initiated_by`.

When no authenticated user is present — the cron loop's `ActivateSchedule`, internal
callers, or any client that does not set the header — the identity resolves to the
`system` sentinel, so every row carries a non-null initiator. The cancel path
(`CancelSchedule` / `CancelScheduler`) records the same authenticated identity into
`cancelled_by`: when the metadata header names a real user it is authoritative;
otherwise the request's `cancelled_by` field is used (so the dispatch watchdog's
self-supplied label is preserved).

The four run-creation events carry `initiated_by` on the wire so the orchestrator can
record the same provenance on its `:Run` projection: `scheduler.started:v1`,
`trigger.rerun:v1`, `trigger.rebase:v1`, and `trigger.single_node_run:v1`. The
`schedule.cancelled:v1` event likewise carries `cancelled_by`, defaulting to the
`system` sentinel when no actor was recorded.

## Inbound Interfaces

### gRPC — `StateService` (port 50051)

gRPC is now the interface for UI reads and user-initiated commands only. All internal pipeline writes flow through Redis consumers.

#### Schedule activation and control (by `schedule_name` string)

| Method | Catalog check | Active-run check | Outbox write |
|---|---|---|---|
| `ActivateSchedule` | Yes (NotFound if absent) | Yes (skips if active) | Yes — transactional via `ScheduleActivationService` |
| `TriggerSchedule` | Yes (NotFound if absent) | Yes (FailedPrecondition if active) | Yes — transactional via `ScheduleActivationService`. Accepts an optional `operation` (`""` \| `"run"` \| `"test"` \| `"build"`, default `run`) forwarded onto `scheduler.started:v1` for the orchestrator to build the run projection; state does not otherwise act on it. `operation=build` runs the whole-DAG dependency order (seeds-first-else-roots, cascade-skip on failure) exactly like a plain run, except every node runs `dbt build --select <node>` (materializes and tests it) instead of `dbt run`/`seed`/`snapshot`. |
| `CancelSchedule` | No | Looks up active run | No — direct cancel via `scheduler_tracker`; records the authenticated user (metadata header, else request `cancelled_by`) into `cancelled_by` |
| `ListAllSchedules` | Reads catalog | — | No |
| `ListStuckCandidates` | No | — | No — read-only, one indexed query |

> **`ActivateSchedule` vs `TriggerSchedule`**: Both go through `ScheduleActivationService` (transactional outbox) and both require the schedule to exist in `schedule_catalog` (NotFound if absent). The key differences are the concurrent-run guard and the run kind: `TriggerSchedule` returns `FailedPrecondition` if a run is active and stamps `scheduler_tracker.kind = 'trigger'`; `ActivateSchedule` silently skips (idempotent for the cron loop) and stamps `kind = 'cron'`. `ActivateSchedule` is used by the cron loop and e2e tests; `TriggerSchedule` is the UI-exposed manual trigger.

> **One active run per schedule (TOCTOU backstop)**: the "at most one active run per schedule_name" invariant is enforced by the partial unique index `uq_scheduler_tracker_active_per_schedule ON scheduler_tracker (schedule_name) WHERE status IN ('pending','running')`. The activation handlers (`ActivateSchedule`, `TriggerRerun`, `TriggerRebase`) keep a friendly pre-check (`HasActiveSchedule`) on the autocommit connection for the fast path, but the index is the source of truth: two concurrent activations can both pass the pre-check, and the loser's `INSERT` fails with SQLSTATE 23505, which the repository maps to `run.ErrScheduleHasActiveRun` — so the loser gets the same outcome (cron skip / manual `FailedPrecondition`) as the check-first path.

> **`ListStuckCandidates`**: returns the active (`pending|running`) runs whose dispatch has silently stalled — at least one task, no task in `running`, and the most recent task's `created_at` strictly older than the request `cutoff`. It is a single server-side aggregation over `scheduler_tracker ⋈ task_tracker` (not paged), consumed by the orchestrator dispatch watchdog. Empty `cutoff` is `InvalidArgument`.

#### Scheduler run reads

| Method | Description |
|---|---|
| `GetScheduler` | Fetch a run by UUID |
| `CancelScheduler` | Cancel a run by UUID |
| `GetSchedulerInitStatus` | Read `initialization_status` for a run |

#### Task reads and user commands

| Method | Description |
|---|---|
| `GetTask` | Fetch by UUID |
| `GetTaskByScheduleAndNode` | Fetch by `(schedule_id, service_name, schema_name, table_name)` |
| `ListTasks` | Paginated list with filters |
| `ListNodeRuns` | Read-only query returning the most recent task instances that executed on a given node, ordered by `scheduler_tracker.created_at DESC`, scoped to one `operation`. See `ListNodeRuns` detail below. |
| `ListNodes` | Read-only query returning the node catalog: one summary row per node that has run under the requested `operation`, with stats aggregated over each node's most recent 50 runs. See `ListNodes` detail below. |
| `ListNodeNames` | Distinct table names of nodes that have run, optionally filtered by service. Powers the Nodes-tab search autocomplete. |
| `TriggerRerun` | Mint a new `scheduler_tracker` row (`kind='rerun'`, `source_run_id=src`) on the source's schedule + write `trigger.rerun:v1` outbox entry. Same eligibility as `TriggerRebase`: source FAILED/CANCELLED, has ≥1 non-SUCCEEDED task, no active run on `schedule_name`. Backed by the shared `synthesise_derived_run.go` helper. Returns `run_id` + `schedule_name`. |
| `TriggerSingleNodeRun` | Create a one-task run for a single node; write `trigger.single_node_run:v1` outbox entry. Accepts an optional `operation` (`""` \| `"run"` \| `"test"` \| `"build"`, default `run`), forwarded onto `trigger.single_node_run:v1` for the orchestrator to resolve the dbt verb and, for `test`, gate zero-test nodes. `operation=build` has no equivalent gate — a node with zero tests is still built (`dbt build --select <node>`). |
| `TriggerRebase` | Mint a new `scheduler_tracker` row (`kind='rebase'`, `source_run_id=src`) on the source's schedule + write `trigger.rebase:v1` outbox entry. Source row is left untouched. Returns `run_id` + `schedule_name`. |

##### `TriggerSingleNodeRun`

Request fields: `service_name`, `schema_name`, `table_name`, `metadata_source` (enum: `latest` / `snapshot_of_run`), `source_run_id` (UUID string; required when `metadata_source=snapshot_of_run`), `operation` (optional: `""` \| `"run"` \| `"test"` \| `"build"`, default `run`).

Response fields: `run_id` (UUID of the new `scheduler_tracker` row), `schedule_name` (synthesised as `"single-node-run-<8 random hex chars>"`).

Error contract:
- `INVALID_ARGUMENT` — missing required fields, `metadata_source` unknown, or `operation` unparseable
- `NOT_FOUND` — target node does not exist in the topology
- `FAILED_PRECONDITION` — `source_run_id` references a run that does not exist (stale mode only)

On success: a new `scheduler_tracker` row is inserted with `kind='single_node_run'` and a `trigger.single_node_run:v1` outbox entry is written — both in one transaction.

The synthesised `schedule_name` is not inserted into `schedule_catalog`, so the run is excluded from `ListAllSchedules` by construction (catalog-driven listing).

##### `TriggerRebase`

Request fields: `source_run_id` (UUID string of the failed/cancelled source run).

Response fields: `run_id` (UUID of the newly minted `scheduler_tracker` row), `schedule_name` (the source run's schedule name — rebase always lands on the same schedule).

Error contract:
- `INVALID_ARGUMENT` — `source_run_id` missing or malformed
- `NOT_FOUND` — source run does not exist
- `FAILED_PRECONDITION` — source run is not terminal, or is terminal in a state other than `FAILED` / `CANCELLED`, or already has zero non-SUCCEEDED tasks (nothing to rebase)

On success: a new `scheduler_tracker` row is inserted with `kind='rebase'`, `source_run_id=<src>`, `schedule_name=<src.schedule_name>`, `status=PENDING`, `init_status=in_progress`, and a `trigger.rebase:v1` outbox entry is written — both in one transaction. The source `scheduler_tracker` row is **not** mutated; it remains at its terminal status forever as the historical record.

##### `ListNodeRuns`

Request fields: `service_name`, `schema_name`, `table_name`, `limit` (capped server-side at 50; passing 0 or a value greater than 50 yields 50), `operation` (`""` \| `"run"` \| `"test"` \| `"build"`, default `run`).

Response: an ordered list of node-run rows, most recent first (`scheduler_tracker.created_at DESC`), scoped to the requested `operation` — a node's `run`-operation history and `test`-operation history are always disjoint slices, never blended. Each row joins `task_tracker × scheduler_tracker × task_execution` (latest execution per task via `DISTINCT ON`), filtered by `task_tracker.operation = <operation>`, and carries:

- run-level: `run_id` (= `scheduler_tracker.schedule_id`), `schedule_name`, `kind` (`cron` | `trigger` | `rerun` | `rebase` | `single_node_run`), `terminal_status` (empty string while the run is in flight)
- task-level: `task_id`, `task_status`, `retry_count`, `image_tag`, `manifest_version`, `operation` (`run` | `test` | `build`)
- exec-level: `started_at`, `completed_at`, `error_message`, `log_s3_key` — empty/null when no execution record has been written yet (e.g. a task still in `PENDING`)

All timestamps are RFC3339 strings; empty string indicates the field is not yet populated.

Implementation: `postgres.NodeRunRepository.List`. A single SQL query uses a `target_tasks → latest_exec` CTE chain: `target_tasks` filters `task_tracker` to the node coordinates and the requested `operation`; `latest_exec` applies `DISTINCT ON (task_id)` scoped to the matched task set only (not the full `task_execution` table). The two CTEs join with `scheduler_tracker` to produce the response rows in one round-trip.

Error contract:
- `INVALID_ARGUMENT` — any of `service_name`, `schema_name`, `table_name` missing

##### `ListNodes`

Request fields: `search` (case-insensitive **exact** match on `table_name`; `""` = no filter — not a substring match despite matching against a single coordinate), `service_name` (exact match; `""` = all services), `limit` (page size, default 50, clamped to `[1, 200]`), `offset` (page offset, default 0, negative treated as 0), `operation` (`""` \| `"run"` \| `"test"` \| `"build"`, default `run`).

Response: `nodes` — one `NodeSummary` per node that has at least one task under the requested `operation`, aggregated over that node's most recent 50 matching runs, ordered by `last_run_at DESC` with `(service_name, schema_name, table_name)` tiebreakers; `total_count` — the full match count before paging. A node with zero runs under the requested `operation` is **absent from the result entirely** (the query filters `task_tracker.operation` inside the aggregation, not a left join against topology) — it never appears with null/zero stats. This is how the Nodes catalog keeps a node's model-run health and test-run health from blending: the same node can appear under `operation=run` with its model stats and simultaneously be absent under `operation=test` if it has never had a test-operation run.

Each `NodeSummary` carries `service_name`, `schema_name`, `table_name`, `run_count`, `success_rate_pct` (-1 when no terminal runs in the window), `avg_duration_sec`/`p95_duration_sec` (-1 when unmeasurable), `flaky_rate_pct`, `last_status`, `last_run_at`, and `operation` (echoing the requested filter).

Implementation: `postgres.NodeRunRepository.ListNodes`. A windowed CTE (`ranked` → `windowed`, capped at each node's most recent 50 matching task rows) filters `task_tracker ⋈ scheduler_tracker` on `search`, `service_name`, and `operation` in one scan; the page query layers per-node aggregation and a `COUNT(*) OVER ()` total on top, so the common case costs a single pass. An empty page (offset past the end, or zero matches) falls back to a cheap count-only query to recover the true `total_count`.

Error contract: none — an empty or non-matching filter set yields an empty `nodes` list with `total_count = 0`, not an error.

##### Shared gRPC handler helpers

The `internal/grpc/handlers/synthesise_derived_run.go` helper backs both `RerunHandler` and `RebaseHandler`. It performs the four-step eligibility check (source exists, source terminal, source has ≥1 non-SUCCEEDED task, no active run on `schedule_name`) and the atomic write (new `scheduler_tracker` row + outbox entry) in one Postgres transaction. Per-handler files are ~20-line wrappers that name their `kind` / `stream` / `event` and delegate.

#### Task execution reads

| Method | Description |
|---|---|
| `GetTaskExecution` | Fetch by UUID |
| `ListTaskExecutions` | List executions for a schedule. Paginated via `page_size` (clamped to `[1, 200]`, default 50 when unset) and `page_offset` (negatives treated as 0); the response carries `total_count`. |

### HTTP server (port 8082)

| Route | Method | Description |
|---|---|---|
| `/health` | GET | Liveness probe; 200 while the process can serve HTTP |
| `/ready` | GET | Readiness probe backed by the liveness registry |

Port 8082 serves health checks only; trigger commands run over gRPC on 50051.

`/ready` returns 200 only when every registered background worker (each Redis
stream consumer plus the outbox processor) is live and the cached Redis/Postgres
dependency probes (5s TTL) pass; otherwise 503. A consumer goroutine that
returns a non-nil error (a genuine exit, distinct from the clean `nil` return on
context cancel) marks itself unhealthy in the registry, flipping `/ready` to 503
so Kubernetes stops routing to the degraded pod and restarts it.

### Graceful shutdown

On SIGTERM/SIGINT the lifecycle manager runs an ordered sequence bounded by
`SHUTDOWN_GRACE` (default 15s): (1) stop intake by cancelling the root context so
consumers and the outbox processor return after the in-flight message; (2) drain
— wait on a WaitGroup for those tracked goroutines to return, capped at the
grace period; (3) close infra — run the registered shutdown handlers (gRPC/HTTP
servers, cron scheduler, Postgres, Redis) against a fresh live context derived
from `context.Background()`, never the just-cancelled root context. `main` blocks
on the lifecycle completion channel, so there is no fixed sleep.

**TriggerRerun preconditions (enforced atomically):**
1. Source scheduler run must exist
2. Source run must be terminal (`FAILED` or `CANCELLED`)
3. Source run must have at least one non-SUCCEEDED task
4. No active run on `schedule_name`

On success: a new `scheduler_tracker` row is inserted with `kind='rerun'`, `source_run_id=<src>`, `schedule_name=<src.schedule_name>`, `status=PENDING`, `init_status=in_progress`. The source row is left untouched — it stays at its terminal status as the immutable historical record. A `trigger.rerun:v1` outbox entry is written in the same transaction. The orchestrator's `HandleRerun` consumer then runs `Snapshot(SourcePinnedDAG{})` against the new run, projecting the source's pinned DAG with all non-SUCCEEDED tasks + their descendants flipped to PENDING (rebased) and the rest carried forward as inherited at their source status. `task_tracker` rows for the new run are created by `RunEntriesDispatchedHandler` from the orchestrator's `run.entries.dispatched:v1` payload.

### Redis consumers

Every consumed stream follows the same three-layer path:

1. **Adapter** (`state/adapters/redis/`): a `pkg/redis.StreamConsumer` reads the stream and delegates each `goredis.XMessage` to a per-stream binding (`*_binding.go`). The binding calls a per-stream parser (`*_parser.go`) to turn the raw `XMessage` into a typed `events.<Event>` struct from `state/domain/events`. Parser failures (malformed payload, missing required field, bad UUID, unknown enum value) are wrapped with `pkg/events.ErrPermanent`; `pkg/redis.StreamConsumer` ACKs and drops `ErrPermanent`-wrapped errors so they leave the pending list immediately. Plain handler errors are retried inline by the consumer (~2.6s bounded backoff); if every attempt still fails the message stays in the PEL and the periodic reclaim sweep picks it up.
2. **Dedup + transaction** (in the binding): the binding obtains a `state/service/uow.UnitOfWork`, calls `Begin`, then runs `pkg/messageprocessing.Dedup` against state's `message_processing` table keyed on `(message_id, stream_name)`. A duplicate commits the empty txn and returns nil (consumer ACKs). A miss inserts a `processing` row and continues into the handler under the same tx.
3. **Handler** (`state/service/handlers/`): pure orchestration over `uow.UnitOfWork` — no `sqlx`, no `goredis`, no JSON parsing, and no `state/adapters/*` import. The handler reads/writes through the aggregate-level ports exposed by the UnitOfWork (`Run() repository.RunRepository`, `Catalog() repository.ScheduleCatalogRepository`, `Outbox() ports.OutboxPublisher`, `Clock() ports.Clock`, `TaskExecutions() repository.TaskExecutionWriter`). These ports live in `state/domain/repository` (collection-like aggregate ports) and `state/service/ports` (`Clock`, `OutboxPublisher`); concrete implementations live in `state/adapters/postgres`. On success the binding marks the `message_processing` row `completed`, the outbox-and-state writes commit together, and the consumer ACKs.

| Stream | Consumer group | Handler |
|---|---|---|
| `schedules.loaded:v1` | state service | `ScheduleCatalogHandler` — reconciles `schedule_catalog` |
| `run.entries.dispatched:v1` | state service | `RunEntriesDispatchedHandler` — creates tasks (each carries the orchestrator-stamped `MaxRetries=DefaultTaskMaxRetries`); honours per-task `Status` and `InheritedFromTaskID`; sets `total_task_count`, marks `init_status=completed`. Auto-rollups directly to terminal (`SUCCEEDED`/`FAILED`, `completed_at` set) when every dispatched task is already terminal — otherwise marks `status=running`. |
| `run.entries.dispatch_failed:v1` | state service | `RunEntriesDispatchFailedHandler` — symmetric counterpart of `RunEntriesDispatchedHandler`. Row-locks `scheduler_tracker` and finalizes via `MarkDispatchTerminal`, emitting `run.finalized:v1`: the benign `reason=no_tests` marks status=`skipped`, every other reason marks status=`failed`. Idempotent on already-terminal rows. |
| `task.status.updated:v1` | state service | `TaskStatusUpdatedHandler` — updates task status, drives finalization state machine |
| `task.execution.recorded:v1` | state service | `TaskExecutionRecordedHandler` — persists task execution records |

Transient handler errors (e.g. `task_tracker` row not found because `RunEntriesDispatchedHandler` has not yet caught up) intentionally return plain errors. The consumer retries them inline first (~2.6s budget); only if that budget is exhausted does the message stay pending for the periodic reclaim tick.

## Outbound Interfaces

### Redis producers (via transactional outbox)

All Redis publishes go through `state_outbox` → background `pkg/outbox.Processor`. The outbox entry and the state mutation are committed in the same transaction, guaranteeing at-least-once delivery.

The processor polls every 500ms and claims up to 100 pending rows per batch (`FOR UPDATE SKIP LOCKED`). Within one batch it pipelines all XADDs over a single Redis connection (`OutboxPublisher.PublishBatch`) and flips the whole successful subset to `processed` in one `UPDATE … WHERE id = ANY(...)`; only the failed subset takes per-row retry/fail handling. After a full batch the processor immediately drains the next batch before sleeping to the next tick, so a burst of thousands of pending rows clears within seconds rather than one batch per tick. Per-aggregate FIFO is preserved because the pipeline issues XADDs in the batch's SELECT order over one connection.

Each XADD caps its stream at `MaxLen 10000` (approximate, `~`), bounding Redis memory for streams a consumer group may lag on. **Caveat:** approximate trimming can drop the oldest entries before a slow consumer group reads them; 10000 is the accepted bound, matching the orchestrator's publisher.

A retention sweeper (`pkg/outbox.RetentionSweeper`, default hourly, `RETENTION_SWEEP_INTERVAL_MINUTES`) prunes two otherwise-unbounded tables using DB-clock cutoffs:
- `state_outbox` rows with `status='processed'` older than the retention window (`RETENTION_DAYS`, default 7).
- `message_processing` dedup rows in a terminal state (`completed`/`acked`) older than the same window; `processing` rows are never purged so an in-flight or stuck message keeps its dedup guard.
Each delete is bounded by a per-statement `LIMIT` loop to keep lock footprints small. All knobs have safe defaults, so no configuration is required.

#### `scheduler.started:v1`

Emitted on: `ActivateSchedule` / `TriggerSchedule` (both paths)

Payload fields:
- `runner_id` — schedule UUID
- `schedule_name`
- `service_metadata` — map of service name → `{ manifest_version, image_tag }` (snake_case keys), pinned from `schedule_catalog` at activation
- `kind` — run discriminator string (`cron`, `trigger`, `rerun`, etc.); sourced from `scheduler_tracker.kind`
- `source_run_id` — optional UUID string; omitted or empty for `cron`/`trigger` runs; set for `rerun` and `rebase` runs
- `operation` — dbt verb string (`""` \| `"run"` \| `"test"` \| `"build"`); only a manual `TriggerSchedule` caller passes a non-default value — the cron path always sends `run`. State does not act on it; it is forwarded verbatim for `orchestrator` to build the run projection.

Effect: `orchestrator` begins task graph initialization, stamping `:Run.kind`, `:Run.operation`, and `:Run.source_run_id` in Neo4j. `operation="test"` selects the `LatestFullDAG` selector's flat-fan-out mode: only nodes with `test_count > 0` are projected, each dispatching `dbt test` independently with no blocking frontier. `operation="build"` (and the default `""`) take the normal dependency-ordered, cascade-skip frontier — the same seeds-first-else-roots path as a plain run — with every dispatched node running `dbt build` instead of `dbt run`/`seed`/`snapshot`.

#### `trigger.rerun:v1`

Emitted on: `TriggerRerun` gRPC call

Payload fields:
- `schedule_id` — UUID of the **newly-minted** `scheduler_tracker` row
- `schedule_name`
- `kind` — always `"rerun"`
- `source_run_id` — UUID of the source run (carries the pinned snapshot)

Effect: `orchestrator.HandleRerun` resolves the source run's operation and runs `Snapshot(SourcePinnedDAG{})` against the new run, producing a `:Run` node + `:EXECUTES` edges that mirror the source's DAG, with all non-SUCCEEDED tasks + their descendants flipped to PENDING (rebased, will dispatch) and the rest carried forward as inherited at their source status. If the source run's operation was `test`, the selector rejects the rerun outright (`run.entries.dispatch_failed:v1`, `reason=rerun_of_test_unsupported`) instead of dispatching `dbt run` against tasks that were never meant to run: the newly-minted `scheduler_tracker` row finalizes as `failed`. If the source run's operation was `build`, it is inherited: the rebased tasks re-dispatch `dbt build`.

#### `trigger.rebase:v1`

Emitted on: `TriggerRebase` gRPC call

Payload fields:
- `schedule_id` — UUID of the newly-minted `scheduler_tracker` row
- `schedule_name`
- `source_run_id` — UUID of the source (terminal `FAILED`/`CANCELLED`) run

Effect: `orchestrator.HandleRebase` resolves the source run's operation and runs `Snapshot(RebasePartition)` against the new run, computing the rebase set ∪ inherit set against the latest topology and projecting the result onto the new `:Run` with split metadata (rebased rows = latest pair; inherited rows = source's pinned pair + root-resolved `inherited_from_task_id`). Same guard as rerun: a source run with operation `test` is rejected (`reason=rerun_of_test_unsupported`), not rebased; a source run with operation `build` is inherited and the rebased tasks re-dispatch `dbt build`.

#### `trigger.single_node_run:v1`

Emitted on: `TriggerSingleNodeRun` gRPC call

Payload fields:
- `schedule_id` — UUID of the newly created `scheduler_tracker` row
- `schedule_name` — synthesised name (`"single-node-run-<8hex>"`)
- `service_name`, `schema_name`, `table_name` — target node coordinates
- `metadata_source` — `"latest"` or `"snapshot_of_run"`
- `source_run_id` — source run UUID (stale mode only; omitted for latest)
- `operation` — dbt verb string (`""` \| `"run"` \| `"test"` \| `"build"`), passed through from the `TriggerSingleNodeRun` request

Effect: `orchestrator` snapshots the single node and dispatches it for execution. For `operation="test"`, the `SingleNode` selector runs `dbt test --select <node>` instead of the node's default verb, and gates the dispatch: a target with no known-positive `test_count` produces `run.entries.dispatch_failed:v1` (`reason=no_tests`) instead of a Job, and the run finalizes as `skipped` — a benign non-failure, not an error. For `operation="build"`, the selector runs `dbt build --select <node>` with no equivalent gate — a node with zero tests is still built.

#### `run.finalized:v1`

Emitted on: every terminal outcome — succeeded, failed, cancelled, or skipped (see Finalization State Machine below and Cancellation below)

Payload fields:
- `schedule_id`
- `schedule_name`
- `status` — one of `succeeded | failed | cancelled | skipped`

Effect: signals downstream consumers that the run is complete; the orchestrator projects the outcome onto `:Run.terminal_status` and `:Run.completed_at`.

## Finalization State Machine

All finalization logic is owned by the `run.Run` aggregate. `TaskStatusUpdatedHandler` loads the `Run` through the `repository.RunRepository` port (`LoadRunForUpdate`), calls `Run.RecordTaskStatus(taskID, newStatus, caller)`, and persists the result via `SaveRun`.

`Run.RecordTaskStatus` orders updates by attempt (`retry_count`), making the projection independent of the order in which the producers' messages are processed (see `docs/arch/state-machine-transition.md` for the producer invariant). It loads the prior status and stored attempt under a `FOR UPDATE` row lock. A failure of that locked read is propagated as a transient error — never coerced to "no prior status" — so the consumer redelivers; coercing it would let a stale delivery be adopted as a newer attempt and overwrite an already-recorded terminal. After a successful read it then:

1. **Older attempt** (`retry_count < ` stored): superseded — ignored. Covers both a stale RUNNING and a stale terminal from an attempt the projection has already moved past.
2. **Same attempt** (`retry_count == ` stored):
   - prior already terminal → no-op (replayed terminal, or a RUNNING re-delivered after its own terminal — no un-fill, no status regression);
   - prior non-terminal, new non-terminal → persist the status change (e.g. PENDING→RUNNING); slot unaffected;
   - prior non-terminal, new terminal → persist and **fill** the slot (`terminal_task_count++`).
3. **Newer attempt** (`retry_count > ` stored, or no prior status): persist, then adjust the slot by the fill-state transition — terminal→non-terminal **un-fills** (genuine retry running), non-terminal→terminal **fills**, and terminal→terminal (a newer attempt's terminal after an older one) leaves the count unchanged (no double-count).
4. Whenever the task is terminal after the update, re-check finalization: `terminal_task_count == total_task_count && init_status == 'completed' && scheduler_status == 'running'` and no failed task still retryable. (Re-checking on a terminal→terminal step matters because the newer attempt may flip a deferred run to finalizable — e.g. a retryable failure followed by a permanent one.)
5. If finalization holds: transition the scheduler to the terminal status (`SUCCEEDED` if every task is `SUCCEEDED`, `FAILED` otherwise) and mark the aggregate as needing a `run.finalized:v1` outbox entry.

A cancelled task is never overwritten (the write is a no-op and leaves the counters untouched).

`repository.RunRepository.SaveRun` (implemented by `postgres.RunRepositoryAdapter`) writes the updated `scheduler_tracker` row; the handler then appends any emitted finalization event to `state_outbox` through `ports.OutboxPublisher.Append` — all within the same Postgres transaction opened by the binding's `UnitOfWork`.

`SaveRun` collects every dirty column reported by the aggregate's change set (`status`, `initialization_status`, `total_task_count`, `terminal_task_count`, `started_at`, and `completed_at`) into a single parameterised `UPDATE scheduler_tracker SET … WHERE schedule_id = $n` issued by `UpdateRunRowTx`. Because the run row is already held under `SELECT … FOR UPDATE` by `LoadRunForUpdate`, this is one round-trip per save. The column list is a fixed allowlist driven by the change-set flags; values are always bound as parameters. Cancellation is the one exception: it persists through the guarded `CancelTx` statement (`WHERE status NOT IN (terminal)`, mapped to `ErrAlreadyTerminal`), which writes `status`, `cancelled_at`, `completed_at`, `cancelled_by`, and `cancellation_reason` together. `CancelTx` takes the authoritative cancellation instant produced by `Run.Cancel` (`ports.Clock.Now()` at the gRPC handler) and persists it verbatim to both `cancelled_at` and `completed_at`, so the value the gRPC response reports equals the stored row — one timestamp per logical event.

`started_at` is stamped when the run transitions PENDING→RUNNING (a non-terminal dispatch in `AcceptDispatch`) and persisted on that save. Runs that auto-rollup directly to a terminal status at dispatch time never enter RUNNING, so their `started_at` stays NULL while `completed_at` is set — the `valid_timestamps` CHECK tolerates a NULL `started_at`.

### Auto-rollup at dispatch time

`RunEntriesDispatchedHandler` honours per-task `Status` in the dispatched payload. When the orchestrator's projection contains tasks that are already terminal at dispatch time (typically `SUCCEEDED` inherits in a rebase or rerun snapshot), `task_tracker` rows are inserted at that terminal status — the executor pipeline never touches them.

If **every** dispatched task is in a terminal state (i.e. there is nothing to execute), the handler short-circuits the normal `status='running'` transition and instead transitions the `scheduler_tracker` directly to a terminal status (`SUCCEEDED` if every projected task is `SUCCEEDED`, `FAILED` otherwise) with `completed_at` set. `init_status='completed'` is still set, and `run.finalized:v1` is emitted via the outbox in the same transaction. This avoids a transient `status=running` flicker on no-op rebases (e.g. a rebase whose projection contains only inherited rows — rejected upstream by `TriggerRebase` precondition checks, but the auto-rollup is the last line of defence).

### Cancellation as a finalizing terminal transition

`Run.Cancel()` is a terminal transition with the same finalization side-effects as SUCCEEDED and FAILED. When a run is cancelled it:

1. Sets `cancelled_at`, `cancelled_by`, and `cancellation_reason` — the cancellation-specific metadata.
2. Sets `completed_at` (alongside `cancelled_at`, both to the single `ports.Clock.Now()` instant the handler passed in) and emits `RunFinalized{Outcome: cancelled}` — the same finalization side-effects SUCCEEDED and FAILED produce. The cancel path persists through `cancelDirty`/`CancelTx`, so it stamps these directly rather than through the `completedDirty`/`FinalizeRunTx` path the SUCCEEDED/FAILED rollup uses. The child `task_tracker` rows cancelled in the same transaction (`BulkCancel`) are stamped with that same instant.
3. Emits `RunCancelled` — the work-suppression guard event.

`SaveRun` persists both `cancelled_at` and `completed_at` in the same `CancelTx` SQL statement using the aggregate's authoritative instant, so state's `scheduler_tracker.completed_at` is always non-NULL for a cancelled run and equals the value returned to the caller.

`Run.Cancel()` therefore produces two domain events published to two separate streams in one outbox transaction:

| Event | Stream | Consumer effect |
|---|---|---|
| `RunCancelled` | `schedule.cancelled:v1` | Orchestrator, executor, and k8s-controller record the cancelled schedule ID and suppress further work on that run |
| `RunFinalized{Outcome: cancelled}` | `run.finalized:v1` | Orchestrator projects `terminal_status='cancelled'` and `completed_at` onto the `:Run` node, removing the run from the active set |

The two events are independent and commutative: the guard path keys on `schedule.cancelled:v1`; the projection path keys on `run.finalized:v1`. Both writes commit atomically with the `scheduler_tracker` mutation; neither can partially appear. A re-cancel returns `ErrAlreadyTerminal` and emits nothing.

## What It Reads

- All owned tables for every gRPC read/write
- `schedule_catalog.manifest_versions` during activation (passes to outbox payload)
- `schedules.loaded:v1`, `run.entries.dispatched:v1`, `task.status.updated:v1`, `task.execution.recorded:v1` payloads from Redis

## What It Writes

- `scheduler_tracker` — on activation, status transitions, rerun reset, finalization
- `task_tracker` — on task create/update/reset (via events)
- `task_execution` — on execution create/update (via events)
- `schedule_catalog` — on `schedules.loaded:v1` consumption. `ScheduleCatalogHandler` loads the catalog with `LoadCatalogForUpdate`, reconciles, then `SaveCatalog` upserts the active set and soft-deletes absent rows. `LoadCatalogForUpdate` first takes a transaction-scoped advisory lock (`pg_advisory_xact_lock`) and then `SELECT … FOR UPDATE`s every existing row. The advisory lock is what serialises the read-modify-write against any concurrent reconciler on the **first** reconcile: an empty `schedule_catalog` has no rows for `FOR UPDATE` to lock, so without it two replicas could both read the empty table and upsert a divergent snapshot. This honours the `Load*`-means-write-path-under-lock contract. `first_seen_at`/`last_seen_at`/`removed_at` are stamped with the DB clock (`NOW()`), not a Go wall-clock value.
- `state_outbox` — atomically with every activation, rerun, or finalization
- `message_processing` — one row per consumed Redis message ID; written by the binding inside the same transaction as the handler's state mutation

## Background Loops

| Loop | Description |
|---|---|
| `CronScheduler` | Fires on schedule per `schedules.yaml`; calls `ActivateSchedule` via `ScheduleActivationService` |
| Outbox processor (`pkg/outbox.Processor`) | Polls `state_outbox` for pending entries; publishes each row to its `stream_name` via `state/adapters/postgres.OutboxPublisher` |
| `pkg/redis.StreamConsumer` (one per stream) | Reads each consumed stream and delegates to the matching binding in `state/adapters/redis/`. One instance per stream: `schedules.loaded:v1`, `run.entries.dispatched:v1`, `run.entries.dispatch_failed:v1`, `task.status.updated:v1`, `task.execution.recorded:v1`. Includes a periodic reclaim ticker that re-delivers messages whose handler returned a non-`ErrPermanent` error. |
| gRPC server | Serves `StateService` on port 50051 |
| HTTP server | Serves health endpoint on port 8082 |

### Cron scheduler config

`CronScheduler` reads `schedules.yaml` at startup. Each entry has:
- `name` — must match a schedule name in the catalog for `TriggerSchedule` to succeed
- `cron` — standard 5-field cron expression (seconds optional)
- `description`
- top-level `timezone`

## Activation Paths

Both cron-loop activations and UI manual triggers share the same transactional write path through `ScheduleActivationService`.

```
Cron tick
    → CronScheduler fires on schedule
    → ActivateScheduleHandler.Handle (svchandlers)
    → UnitOfWork.Begin
    → CatalogRepo.ExistsActive (NotFound if schedule not in catalog)
    → SchedulerRepo.GetActiveScheduler (skip if already active — idempotent)
    → CatalogRepo.GetServiceMetadata
    → tx: RunRepo.CreateRun(scheduler_tracker) + OutboxRepo.Create(state_outbox)
    → UnitOfWork.Commit
    → pkg/outbox.Processor publishes scheduler.started:v1

UI manual trigger
    → TriggerSchedule gRPC handler
    → UnitOfWork.Begin
    → CatalogRepo.ExistsActive (NotFound if absent)
    → SchedulerRepo.GetActiveScheduler (FailedPrecondition if active)
    → CatalogRepo.GetServiceMetadata
    → tx: RunRepo.CreateRun(scheduler_tracker) + OutboxRepo.Create(state_outbox)
    → UnitOfWork.Commit
    → pkg/outbox.Processor publishes scheduler.started:v1
```

## Redis Behavior

### Consumes: `schedules.loaded:v1`

Payload (nested under Redis field `payload`):
- `event_id` — UUID for deduplication
- `schedule_names` — list of currently active schedule names
- `manifest_versions` — map of service → manifest hash

Effects (all or nothing — transient errors are retried, not ACK'd):

`ScheduleCatalogHandler` loads the `catalog.ScheduleCatalog` aggregate and calls `ScheduleCatalog.Reconcile(scheduleNames, serviceMetadata)`. `Reconcile` returns `ErrEmptyScheduleList` if `scheduleNames` is empty, causing the binding to propagate a permanent error — the message is ACK'd and dropped rather than writing an empty list to the catalog. When `scheduleNames` is non-empty, the aggregate performs:

1. `UpsertAll` — insert or reactivate all names in the list
2. `SoftDeleteAbsent` — set `removed_at` on any active row not in the list

Dedup against `message_processing` is performed by the binding before the handler runs; the handler itself sees only typed `events.ScheduleCatalogLoaded` data.

### Consumes: `run.entries.dispatched:v1`

Published by: `orchestrator`

Payload: `run_id`, `schedule_name`, `manifest_versions`, `dispatched_tasks` — each `DispatchedTask` carries `task_id`, node coordinates, `node_type`, `service_name`, `image_tag`, `manifest_version`, `max_retries`, **`Status`** (defaults to `"pending"`; set to `"succeeded"` for inherited rebase rows), and **`InheritedFromTaskID`** (empty for non-inherited rows; root-resolved `task_id` of the source's executed ancestor for inherited rows).

Effects:
1. Create task rows in `task_tracker` for all dispatched entries — honouring per-task `Status` and stamping `inherited_from_task_id` from `InheritedFromTaskID`
2. Set `total_task_count` on `scheduler_tracker`
3. Set `init_status = completed`. Status transition:
   - If every dispatched task is already terminal: directly to terminal (`SUCCEEDED`/`FAILED`) with `completed_at` set; emit `run.finalized:v1` via outbox.
   - Otherwise: `status = running`.

Dedup against `message_processing` is performed by the binding before the handler runs.

### Consumes: `task.status.updated:v1`

Published by: `k8s-controller` (RUNNING + SUCCEEDED/FAILED — the pod lifecycle), `executor-controller` (FAILED only, on the never-deployed path), and `orchestrator` (SKIPPED on cascade-skip)

Effects:
1. Update task status in `task_tracker`
2. If status is terminal: increment `terminal_task_count`, check finalization condition
3. If finalization condition met: transition scheduler to terminal, emit `run.finalized:v1` via outbox

Dedup against `message_processing` is performed by the binding before the handler runs.

### Consumes: `task.execution.recorded:v1`

Published by: `k8s-controller`

Effects:
1. Insert row in `task_execution`, including `parse_cache`/`parse_cache_reason` verbatim from the payload (NULL when the payload omits them)

Dedup against `message_processing` is performed by the binding before the handler runs.

## gRPC Callers

| Service | Methods used |
|---|---|
| `ui-service` | `ListAllSchedules`, `GetScheduler`, `ListTasks`, `ListTaskExecutions`, `ListNodeRuns`, `ListNodes`, `ListNodeNames`, `TriggerRerun`, `TriggerRebase`, `TriggerSingleNodeRun`, `TriggerSchedule`, `CancelSchedule` |
| `continuo CLI` | `ListAllSchedules`, `TriggerSchedule` |
| `tests/e2e` | `TriggerSingleNodeRun`, `TriggerRebase` |

State calls no external gRPC services.

## Reliability Patterns

- **Transactional outbox**: every Redis publish is backed by a `state_outbox` row committed in the same transaction as the state mutation — no partial writes
- **Inbound dedup**: `message_processing` keyed by `(message_id, stream_name)`; `pkg/messageprocessing.Dedup` INSERTs IF NOT EXISTS inside the handler transaction. Duplicates short-circuit before the handler runs and the binding ACKs.
- **Rerun / rebase atomicity**: new `scheduler_tracker` row INSERT + outbox write are a single SQL transaction; the source row is read for eligibility checks but never mutated
- **Finalization atomicity**: `terminal_task_count` increment + finalization check + `run.finalized:v1` outbox write are a single SQL transaction
- **Error classification**: parse failures inside bindings are wrapped with `pkg/events.ErrPermanent`; `pkg/redis.StreamConsumer` ACKs+drops `ErrPermanent`-wrapped errors so malformed payloads do not loop in the PEL. Plain errors stay pending and are reclaimed by the consumer's periodic ticker (`reclaimInterval`).

## Source-of-Truth Notes

- Scheduler and task status are owned exclusively by `state` — no other service updates these rows directly
- Internal pipeline writes reach `state` via Redis events (`run.entries.dispatched:v1`, `task.status.updated:v1`, `task.execution.recorded:v1`)
- gRPC write surface is limited to user-initiated commands (`TriggerRerun`, `TriggerRebase`, `TriggerSingleNodeRun`, `CancelScheduler`, etc.) and schedule activation
- Rerun, rebase, and single-node runs all flow through the same `run.entries.dispatched:v1` pipeline as cron/trigger runs — there is no separate dispatch stream per run kind
- `schedule_catalog` is the authoritative list of known schedules; `ListAllSchedules` reads from it — a running schedule not yet in the catalog will not appear in the UI
