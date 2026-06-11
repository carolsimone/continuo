# orchestrator

## Purpose

`orchestrator` owns the dependency topology in Neo4j, handles run initialization and node completion events, and serves gRPC queries for the UI.

It is responsible for:
- swapping the production topology on release promotion (via `release.promoted:v1`)
- initializing run snapshots on scheduler start (via `scheduler.started:v1`)
- processing node completion events, unlocking downstream nodes (via `node.updated:v1`)
- producing task dispatch events for the executor pipeline
- serving read queries for schedule graphs, run listings, and run graphs (gRPC `OrchestratorQuery` service)

## Owned Storage

### Neo4j

| Entity | Description |
|---|---|
| `Table` node | One per model/seed; carries topology metadata (`schema_name`, `table_name`, `service_name`, `node_type`, `schedule_name`, `image_tag`), `last_updated_at`, and an `active` flag for current-topology reconciliation |
| `Run` node | One per schedule run; carries `terminal_status`, `created_at`, `completed_at`, `kind`, `source_run_id`, `topology_generation`, `total_nodes`, `terminal_count`, `version` |
| `DEPENDS_ON` relationship | Directed edge from downstream to upstream `Table` |
| `EXECUTES` relationship | Directed edge from `Run` to `Table`; carries per-run `status`, pre-assigned `task_id` UUID, per-task `image_tag` + `manifest_version`, and (for rebase-projected inherited rows only) an optional `inherited_from_task_id` property pointing to the root executed `task_id` in the source lineage |

Task UUIDs are pre-assigned when the `EXECUTES` edges are created during run snapshot initialization. This allows `run.entries.dispatched:v1` to carry canonical task IDs without a round-trip to `state`.

#### `:Run` node properties

| Property | Type | Notes |
|---|---|---|
| `terminal_status` | string | Set at run completion |
| `created_at` | datetime | Stamped at `Snapshot` time |
| `completed_at` | datetime | Stamped when the run reaches any terminal outcome (succeeded, failed, or cancelled) |
| `topology_generation` | int64 | Stamped at `Snapshot` time. Derived runs (`rerun`, `rebase`, stale-mode `single_node_run`) copy it from the source `:Run`; fresh runs (`cron`, `trigger`, latest-mode `single_node_run`) copy it from `:TopologyRoot`. `0` means drift unknown. |
| `service_metadata` | map | Same source-vs-`:TopologyRoot` rule as `topology_generation` — derived runs inherit the pair from the source `:Run`; fresh runs read it from `:TopologyRoot`. |
| `kind` | string | Mirrors `scheduler_tracker.kind` (`cron`, `trigger`, `rerun`, `rebase`, `single_node_run`). Stamped at `Snapshot` time via `ON CREATE SET run.kind = $kind` / `ON MATCH SET run.kind = COALESCE(run.kind, $kind)` — the original kind survives idempotent replay. Reads use `COALESCE(r.kind, "cron")` as a defensive default. |
| `source_run_id` | string (UUID) | Optional; set via Cypher `FOREACH` only when non-nil. Reads use `COALESCE(r.source_run_id, "")` for the absent case. NULL for `cron`/`trigger` runs. **Drives the topology-metadata source decision:** when present, the new `:Run` inherits `topology_generation` + `service_metadata` from the referenced source run; when absent, both are read from `:TopologyRoot`. |
| `total_nodes` | int | Number of `:EXECUTES` edges materialised at `Snapshot` time. Used by the `Run` aggregate to detect terminal-count == total-count finalization. |
| `terminal_count` | int | Number of `:EXECUTES` edges currently in a terminal status (`SUCCEEDED`, `FAILED`, `SKIPPED`, `CANCELLED`). Incremented by `AggregateRepository.Save` as nodes complete; equal to `total_nodes` when the run finalises. |
| `failed_count` | int | Number of `:EXECUTES` edges that transitioned to `FAILED` directly (cascade-skipped nodes do not count). Drives the aggregate's terminal-status decision when `terminal_count == total_nodes` so finalisation works even when the failed node is outside the currently loaded subgraph. |
| `version` | int | Optimistic-concurrency token. Incremented on every aggregate mutation (`CompleteNode`); `AggregateRepository.Save` compares against `COALESCE(run.version, 0)` and returns `ErrVersionConflict` on mismatch. |

#### Schema (constraints + indexes)

`adapters/neo4j/schema.go` (`InitSchema`) applies the following DDL at startup, before any consumer or the gRPC server begins serving. Every statement uses `IF NOT EXISTS`, so it is idempotent and restart-safe; a failure aborts startup (the service refuses to serve traffic against an unindexed graph).

| Object | Type | Backs |
|---|---|---|
| `run_id_unique` | constraint, `:Run(run_id)` unique | Every `MATCH (:Run {run_id})` lookup — rehydrate, snapshot writer, all query-repository reads (one per `node.updated:v1`). Guarantees a single `:Run` per `run_id`. |
| `table_uid_unique` | constraint, `:Table(unique_id)` unique | The `MERGE (:Table {unique_id})` upsert in the release-promotion swap; prevents concurrent promotions from minting duplicate `:Table` nodes. |
| `table_fqn` | index, `:Table(service_name, schema_name, table_name)` | The snapshot writer's `:EXECUTES` match and every fully-qualified descendant/single-table reader. |
| `table_schedule` | index, `:Table(schedule_name)` | `LoadLatestSourceDAG` and schedule-graph scans of `MATCH (:Table {schedule_name})`. |
| `run_schedule` | index, `:Run(schedule_name)` | `ListRuns` (`MATCH (:Run {schedule_name})`). |

`table_uid_unique` will refuse to create if duplicate `:Table {unique_id}` nodes already exist. Before first rollout against an existing graph, collapse duplicates **without losing run history**. A bare `DETACH DELETE` of the duplicates would also delete their `(:Run)-[:EXECUTES]->` relationships and erase the task outcomes recorded against them, so the run-history edges are repointed onto the kept node first:

```cypher
MATCH (t:Table)
WHERE t.unique_id IS NOT NULL
WITH t.unique_id AS uid, collect(t) AS nodes
WHERE size(nodes) > 1
WITH nodes[0] AS keep, nodes[1..] AS dups
UNWIND dups AS dup
  // Repoint every run-history edge from the duplicate onto the kept node,
  // carrying its status/timestamp properties. OPTIONAL so duplicates that
  // carry no history are still removed by the DETACH DELETE below.
  OPTIONAL MATCH (r:Run)-[e:EXECUTES]->(dup)
  FOREACH (_ IN CASE WHEN e IS NULL THEN [] ELSE [1] END |
    MERGE (r)-[k:EXECUTES]->(keep)
    SET k += properties(e)
  )
  DETACH DELETE dup
```

`:Table-[:DEPENDS_ON]->:Table` topology edges need not be repointed — they are rebuilt wholesale by the next `release.promoted:v1` swap. If the deployment has APOC available, `CALL apoc.refactor.mergeNodes(nodes, {properties:"discard", mergeRels:true})` is an equivalent one-liner.

### Run aggregate (`orchestrator/domain/run`)

The write-side of node-completion processing is the `Run` aggregate root. It owns `RunID`, `ScheduleName`, `Status`, the counters above (`TotalNodes`, `TerminalCount`, `Version`), and an operation-scoped `map[NodeKey]*RunNode` subgraph. Methods:

- `NewRun(runID, scheduleName, nodes) *Run` — constructs the aggregate from its initial node set; called once after `Snapshot` materialises the `:EXECUTES` edges.
- `CompleteNode(key, status) ([]DomainEvent, error)` — transitions the target node, enforces cascade-skip on `FAILED`, computes immediate unblocks on `SUCCEEDED`/`SKIPPED`, and emits `RunFinalized` when `TerminalCount == TotalNodes`.

Domain events live in `orchestrator/domain/run/events.go`:

| Event | Meaning |
|---|---|
| `NodeUnblocked` | Every upstream of an immediate-downstream node is now terminal; the orchestrator outbox writes `query.model:v1` for the unblocked node. |
| `NodeCascadeSkipped` | A `PENDING` downstream was forced to `SKIPPED` by an upstream `FAILED`; the orchestrator outbox writes `task.status.updated:v1` (`cascade_task_skipped`) so `state` updates the task row. |
| `RunFinalized` | All nodes have reached a terminal status inside the aggregate. Neo4j `:Run.terminal_status` and `:Run.completed_at` are written by `AggregateRepository.Save`. For runs that produce no `node.updated:v1` traffic (e.g. full-inherited rebases and cancelled runs), a separate `run.finalized:v1` consumer projects state's authoritative terminal outcome — succeeded, failed, or cancelled — onto the same Neo4j fields. |

Ports (`orchestrator/domain/run/ports.go`):

- `AggregateRepository` (write-side) — `Rehydrate(ctx, runID, scope) (*Run, error)` reconstitutes an operation-scoped subgraph; `Save(ctx, *Run) error` persists node statuses, `total_nodes`/`terminal_count`/`failed_count`/`version`, and `:Run.terminal_status`/`completed_at` when finalised. `Save` checks `Version` against the loaded value (with `COALESCE(run.version, 0)` to admit legacy in-flight runs) and returns `ErrVersionConflict` on mismatch; the handler retries from `Rehydrate`. `terminal_status`/`completed_at` are first-writer-wins so state's authoritative `run.finalized:v1` projection cannot be overwritten by a later aggregate save.
- `RunQueryPort` (read-side, CQRS) — `GetScheduleGraph`, `ListRuns(ctx, scheduleName, limit, offset) ([]*RunSummary, total int, err)`, `GetRunGraph`, `GetRunTopologyGeneration`, `ListActiveRuns`.
- `ListScheduleTopologies` is wired through the adapter-internal `ScheduleAndRunListReader` seam in `orchestrator/adapters/grpc/handlers.go`, satisfied by the same `OrchestratorQueryRepository`. It is not part of the domain port surface because no application/handler code consumes it — only the gRPC adapter.

`Scope` is a sealed interface with two variants that the adapter uses to scope the Cypher read:

| Hint | Subgraph loaded |
|---|---|
| `ScopeFull` | Every `:EXECUTES` edge for the run (used at initialisation only). |
| `ScopeNodeCompletion{Key, Status}` | `Status="FAILED"` → target + full transitive downstream; `Status="SUCCEEDED"`/`SKIPPED` → target + immediate downstream + each downstream's upstreams. |

Adapter implementations:
- `orchestrator/adapters/neo4j/run_aggregate_repository.go` — `RunAggregateRepository` implements `AggregateRepository`. Also owns `DeleteExpiredRuns` for the sweeper. `Save` persists all loaded node statuses in a single `UNWIND` round trip, keyed on the full `(service_name, schema_name, table_name)` identity so two services sharing `schema.table` in one run keep distinct `:EXECUTES.status`.
- `orchestrator/adapters/neo4j/orchestrator_query_repository.go` — `OrchestratorQueryRepository` implements `RunQueryPort`.

### Snapshot — domain ports, adapters, and application service

The snapshot pipeline follows a strict layered model:

**Domain ports** (`orchestrator/domain/snapshot/ports.go`):
- `TopologyReader` — read-only port for snapshot selectors; implementations live in the Neo4j adapter and are bound to a single Neo4j `ManagedTransaction`. Descendant walks are **batched**: `DescendantsInLatestTopologyBatch`, `DescendantsInSourceRunBatch`, `ImmediateDescendantsInLatestTopologyBatch`, and `ImmediateDescendantsInSourceRunBatch` each take a slice of start FQNs and return a `map[FQN][]FQN` from one `UNWIND` round trip, so a selector pass over R seed nodes issues O(1) reads instead of R.
- `SnapshotWriter` — write-only port; `WriteRunAndExecutesEdges` MERGEs the `:Run` node (with the metadata-source rule from the table above) and creates one `:EXECUTES` edge per projection entry.
- `TxRunner` — opens a single Neo4j write transaction and hands the caller a paired `TopologyReader` + `SnapshotWriter` scoped to that tx. The caller's `fn` commits on `nil`, rolls back on error.

**Adapter implementations** (`orchestrator/adapters/neo4j/`):
- `TopologyReader` implemented in `topology_reader.go` — all Cypher reads for snapshot policies (DAG loading, source task loading, descendant walks, single-table lookups) live here.
- `SnapshotWriter` implemented in `snapshot_writer.go` — all Cypher writes for run+EXECUTES materialisation live here.
- `SnapshotTxRunner` implemented in `snapshot_tx_runner.go` — opens one Neo4j session/write tx, instantiates the paired reader+writer, and runs the caller's function.

**Application service** (`orchestrator/service/snapshotsvc/Service`):
- `Snapshot(ctx, Params)` — calls `p.Selector.SelectTasks(ctx, reader, p)`, checks the `cancelled_schedules` guard for the run's schedule, then `writer.WriteRunAndExecutesEdges(ctx, p, projection)` inside a single `TxRunner.Run` call. Returns the full projection so handlers can build outbox events without re-reading Neo4j.
- When the schedule is already cancelled at snapshot time, `Params.Cancelled` is set and the writer's `MERGE … ON CREATE` stamps `terminal_status='cancelled'` + `completed_at` on the new `:Run`. This closes the cancel-before-snapshot race: a run cancelled before its `:Run` exists (so the `run.finalized:v1` projection found nothing to finalize) is created already-terminal and never enters the active set.
- Constructed via `snapshotsvc.NewService(snapshotTxRunner, cancelledSchedulesRepo, logger)` in `main.go`.

**Handler interface** (`orchestrator/service/handlers/deps.go`):
- `handlers.SnapshotService` is a narrow handler-local interface `{ Snapshot(ctx, snapshot.Params) ([]snapshot.TaskProjection, error) }`, satisfied by `*snapshotsvc.Service`. Defined here so handler tests can substitute a fake.
- Four handlers receive a `SnapshotService`: `HandleSchedulerStartedHandler`, `HandleRerunHandler`, `HandleRebaseHandler`, `HandleSingleNodeRunHandler`. `HandleNodeCompleted` does not snapshot.

The `Params` struct (defined in `orchestrator/domain/snapshot/`) carries `RunID`, `ScheduleName`, `Kind`, `SourceRunID`, plus a `Selector` interface that decides which tasks land in the projection and with which `(initial_status, image_tag, manifest_version, inherited_from_task_id?)`.

Four selectors live in `orchestrator/domain/snapshot/`, are pure Go, and read all topology data through the `TopologyReader` port:

| Selector | Used by | Source of topology | Per-task projection |
|---|---|---|---|
| `LatestFullDAG` | `HandleSchedulerStarted` (cron / trigger) | latest `:Table`s for the schedule + upstream dbt-seeds | every task PENDING with the latest `image_tag` + `manifest_version`; `ReadyToDispatch` marks the dispatch frontier (nodes with no in-DAG upstream — seeds first, else roots), computed from one batched immediate-descendants read |
| `SourcePinnedDAG` | `HandleRerun` | source `:Run`'s `:EXECUTES` set | non-SUCCEEDED source tasks + their descendants within the source's pinned `:EXECUTES` set → rebased PENDING with **source's** pinned `(image_tag, manifest_version)`; everything else → InitialStatus preserved from source with `inherited_from_task_id` pointing at the source's executed `task_id` (root-resolved) |
| `SingleNode` | `HandleSingleNodeRun` | exactly one node | latest mode reads metadata from `:TopologyRoot` + the `:Table`; `snapshot_of_run` mode reads metadata from the source `:Run`'s `:EXECUTES` edge for that node |
| `RebasePartition` | `HandleRebase` | rebase set ∪ inherit set against latest `:Table`s | rebased rows = PENDING with **latest** metadata; inherited rows = SUCCEEDED with **source's** pinned metadata + root-resolved `inherited_from_task_id` (always points at the lineage-root executed `task_id` — chain depth ≤ 1 even for rebase-of-rebase) |

**Dispatch frontier.** Each PENDING projection row carries `ReadyToDispatch`, computed identically across all paths (cron `LatestFullDAG`, rerun `SourcePinnedDAG`, rebase `RebasePartition`). A PENDING node is on the frontier (`ReadyToDispatch = true`) unless it has an **immediate** (one-hop) PENDING/rebased upstream in the run — i.e. it is a direct in-run dependent of another not-yet-satisfied node. For the cron path this is exactly seeds-first-else-roots: seeds have no upstream and dispatch immediately, roots that depend on a seed wait. The selectors compute this from immediate `DEPENDS_ON` edges (`ImmediateDescendantsIn{LatestTopology,SourceRun}Batch`), deliberately *not* transitive descendants: the run aggregate only unblocks/cascade-skips along immediate in-run edges, so a node blocked via a transitive-only path (its connecting node absent from the run) would never be reached and would stall PENDING forever. `DispatchDerivedRun` emits a `query.model:v1` for the frontier rows only. Blocked rebased rows wait for the run aggregate: as a frontier node completes, `CompleteNode` emits `NodeUnblocked` for newly-ready downstream nodes (→ `query.model:v1`) or cascade-skips them when the upstream fails. This mirrors the fresh-run roots-only dispatch and ensures a re-pended SKIPPED node does not run until its upstream succeeds — and is skipped again if it re-fails.

### Postgres (`continuo_orchestrator`)

| Table | Purpose |
|---|---|
| `message_processing` | Inbound dedup: one row per consumed Redis message, scoped by `(message_id, stream_name)`; tracks state (`processing` / `completed` / `acked`) |
| `orchestrator_outbox` | Canonical transactional outbox — each write-time side effect is a separate row with a JSONB payload; `pkg/outbox.Processor` polls and publishes to the typed Redis stream per row. Each batch pipelines its XADDs over one Redis connection and flips the successful subset in one `UPDATE … WHERE id = ANY(...)`; a full batch immediately drains the next before sleeping to the next tick. Every XADD caps its stream at `MaxLen 10000` (approximate `~`). **Caveat:** approximate trimming can drop the oldest entries before a lagging consumer group reads them; 10000 is the accepted bound |
| `topology_state` | Singleton row holding the monotonic `topology_generation` counter |
| `cancelled_schedules` | Schedule IDs cancelled by an upstream control-plane signal; consulted to short-circuit terminal-state processing for already-cancelled runs |

All `orchestrator_outbox` rows conform to the canonical schema: `id`, `message_processing_id` (nullable), `aggregate_type`, `aggregate_id`, `event_type`, `payload` (JSONB), `stream_name`, `status`, `retry_count`, `max_retries`, `created_at`, `processed_at`, `error_message`.

### Adapter-replaceable ports

All ports the service layer depends on for adapter-replaceable storage live in `orchestrator/domain/repository/port.go`:

| Port | Storage adapter |
|---|---|
| `OutboxRepository` | Postgres |
| `MessageProcessingRepository` | Postgres |
| `CancelledSchedulesRepository` | Postgres |
| `TopologyStateRepository` | Postgres |
| `TopologyRepository` | Neo4j |
| `ReleasePromotionRepository` | Neo4j |

One narrow exception is allowed to import adapter packages directly: `service/handlers/release_promoted_handler_integration_test.go` wires the real Postgres and Neo4j adapters against a live database. Production handlers and unit-test fakes hold only `repository.*` types. The `UnitOfWork` interface is declared in `service/uow/uow.go`; its concrete implementation (`PostgresUnitOfWork`) lives in `adapters/postgres/unit_of_work.go`.

The orchestrator wires one long-lived `PostgresUnitOfWork` instance per consumer and reuses it for every inbound message. `Commit` and `Rollback` therefore clear the transaction state unconditionally — including when the underlying commit fails — so a single failed commit cannot wedge the consumer. A handler's deferred `Rollback` runs after a failed `Commit` and finds the transaction already finished; `sql.ErrTxDone` is treated as a successful no-op there, and the next `Begin` on the same instance starts cleanly.

Read-side ports specific to the CQRS query path (`RunReader`, `TopologyStateReader`) are defined where they are consumed — `service/queries/run_query_service.go` — and intentionally not promoted into `domain/repository/`.

## Background loops

Goroutines started in `main.go` run for the process lifetime:
- **Run sweeper** (`internal/sweeper`) — deletes `:Run` nodes older than the retention window.
- **Dispatch watchdog** (`service/watchdog`, every `ORCHESTRATOR_WATCHDOG_INTERVAL_SECONDS`, default 60) — cancels schedules whose dispatch has silently stalled. Each tick computes a cutoff of `now − ORCHESTRATOR_WATCHDOG_NO_PROGRESS_MINUTES` and issues a single `ListStuckCandidates(cutoff)` RPC to state, which answers with one indexed server-side query: the active (`pending|running`) runs that have at least one task, no task in `running`, and a most-recent task older than the cutoff. The watchdog then calls state's `CancelSchedule` for each candidate. This is O(1) RPCs per tick (no per-schedule fan-out) and considers **all** of a run's tasks, so a long-running task anywhere in a wide run never makes it falsely stuck. The watchdog speaks only domain-typed ports (`ports.StuckScheduleReader`, `ports.ScheduleCanceller`), both implemented by `adapters/grpc.StuckScheduleAdapter`; no gRPC/proto wire types enter the application layer. Every tick runs under a `context.WithTimeout` bounded by the interval.
- **Reconciler** (`internal/reconciler`, every `ORCHESTRATOR_RECONCILER_INTERVAL_SECONDS`, default 60) — converges the `:Run` projection to state's authoritative status: it lists active `:Run`s (`completed_at IS NULL`), reads each run's status from state through the orchestrator-owned `ports.RunStatusReader` (implemented by `adapters/grpc.RunStatusReader` over the state gRPC client's `GetScheduler`), and `FinalizeRun`s any that state already reports terminal. It acts only on runs that exist in Neo4j and are terminal in state, so retention-deleted or never-snapshotted runs are never touched. Each tick runs under a `context.WithTimeout` bounded by the interval. This is the ordering-independent backstop for finalizations missed or raced ahead of a run's snapshot.
- **Cancelled-schedules sweeper** — deletes `cancelled_schedules` rows past their TTL.
- **Retention sweeper** (`pkg/outbox.RetentionSweeper`, default hourly, `RETENTION_SWEEP_INTERVAL_MINUTES`) — prunes, using DB-clock cutoffs, `orchestrator_outbox` rows with `status='processed'` and terminal (`completed`/`acked`) `message_processing` dedup rows older than the retention window (`RETENTION_DAYS`, default 7); `processing` rows are never purged. Each delete is bounded by a per-statement `LIMIT` loop. All knobs default safely — no configuration required.

## Inbound Interfaces

### Redis consumers

| Stream | Group | Handler |
|---|---|---|
| `scheduler.started:v1` | `orchestrator_scheduler_started` | `HandleSchedulerStarted` — runs `Snapshot(LatestFullDAG)`, creates EXECUTES edges with pre-assigned task UUIDs, produces `run.entries.dispatched:v1` + `query.model:v1` for the dispatch frontier (`ReadyToDispatch` rows: seeds first, else roots) |
| `node.updated:v1` | `orchestrator_node_updated` | `HandleNodeCompleted` — updates EXECUTES status, unlocks downstream nodes (SUCCEEDED → `query.model:v1` for newly-ready nodes; FAILED → cascade-skip downstream + emit `task.status.updated:v1` for skipped tasks) |
| `release.promoted:v1` | `orchestrator-release-promoted` | `ReleasePromotedHandler` — swaps the `:Table` topology via `ReleasePromotionRepository.PromoteRelease`, increments `topology_generation`, writes `:TopologyRoot` service_metadata, then emits `schedules.loaded:v1` |
| `trigger.rerun:v1` | `orchestrator_rerun` | `HandleRerun` — runs `Snapshot(SourcePinnedDAG{})` against the **new** `:Run` minted by state's `TriggerRerun`, projecting the source's pinned DAG with non-SUCCEEDED tasks + their descendants as rebased PENDING and the rest as inherited at source's stored status; produces `run.entries.dispatched:v1` (full projection) + `query.model:v1` for the rebase **dispatch frontier** only |
| `trigger.rebase:v1` | `orchestrator-rebase` | `HandleRebaseHandler` — runs `Snapshot(RebasePartition)` against the new `:Run`; projects rebase_set ∪ inherit_set against the latest topology; produces `run.entries.dispatched:v1` (full projection) + `query.model:v1` for the rebase **dispatch frontier** only |
| `trigger.single_node_run:v1` | `orchestrator_single_node_run` | `HandleSingleNodeRunHandler` — runs `Snapshot(SingleNode)` and dispatches the one task; see details below |

Each consumer is wired as a `parser → handler` binding under `adapters/redis/`: the parser extracts and validates the message's scalar fields defensively and returns an `events.ErrPermanent`-wrapped error on any malformed field (missing/non-string value, bad UUID, or cross-field rule violation), which the stream consumer ACKs and drops so a single poison message cannot crash-loop the process.

### gRPC server — `OrchestratorQuery` (port 50052)

| Method | Description |
|---|---|
| `GetScheduleGraph` | Returns all Table nodes and DEPENDS_ON edges for a schedule, plus `topology_generation` (current `:TopologyRoot.topology_generation`; `0` means "generation unknown", same contract as `GetRunGraphResponse.latest_topology_generation`). The topology shape is served from an in-process LRU cache keyed by `(schedule_name, topology_generation)` (see "Topology-shape cache" below). |
| `ListRuns` | Returns a page of completed Run nodes for a schedule, newest first. Paginated via `page_size` (clamped to `[1, 200]`, default 50 when unset) and `page_offset` (negatives treated as 0); the response carries `total_count` (completed runs matching the schedule, independent of the page window). A single `count(run)` query and a `SKIP/LIMIT` page query share the same `MATCH` so the filter cannot drift. |
| `GetRunGraph` | Returns nodes and EXECUTES edges for a specific run, with per-node status. Also returns `run_topology_generation` (stamped on the `:Run` node at `Snapshot` time; `0` means "drift unknown" — not "no drift") and `latest_topology_generation` (current `topology_state.topology_generation` Postgres singleton). |
| `ListActiveRunDrifts` | Returns one `ActiveRunDrift` row per schedule that has an in-flight run (`schedule_name`, `run_id`, `run_topology_generation`) plus the orchestrator's current `latest_topology_generation`. "In-flight" means `completed_at IS NULL` on the `:Run` node — a property stamped by the `run.finalized:v1` projection for all terminal outcomes (succeeded, failed, cancelled). The underlying `ListActiveRuns` query orders results by `schedule_name`, then `created_at DESC`; `RunQueryService.ListActiveRunDrifts` keeps the single newest in-flight run per schedule, so each schedule contributes at most one drift row to the response. Consumed by e2e tests as an active-run state probe. |
| `ListScheduleTopologies` | Returns one entry per schedule with at least one active `:Table`: `schedule_name`, `node_count`, `last_updated_at = max(:Table.last_updated_at)`. Backs the ui-service homepage `Topology` tab tile grid. |

### HTTP (port 8087)

- `GET /health` — liveness probe; returns 200 while the process can serve HTTP.
- `GET /ready` — readiness probe backed by a liveness registry. Returns 200 only
  when every registered background worker (each Redis stream consumer plus the
  outbox processor) is live and the cached Redis/Postgres dependency probes (5s
  TTL) pass; otherwise 503. A consumer goroutine that returns a non-nil error
  (a genuine exit, distinct from the clean `nil` return on context cancel)
  marks itself unhealthy in the registry, flipping `/ready` to 503 so Kubernetes
  stops routing to the degraded pod and restarts it.

### Graceful shutdown

On SIGTERM/SIGINT the lifecycle manager runs an ordered sequence bounded by
`SHUTDOWN_GRACE` (default 15s): (1) stop intake by cancelling the root context so
consumers and the outbox processor return after the in-flight message; (2) drain
— wait on a WaitGroup for those tracked goroutines to return, capped at the
grace period; (3) close infra — run the registered shutdown handlers (gRPC/HTTP
servers, Neo4j, Postgres, Redis) against a fresh live context derived from
`context.Background()`, never the just-cancelled root context. `main` blocks on
the lifecycle completion channel, so there is no fixed sleep.

## Outbound Interfaces

### Redis producer (via outbox)

| Stream | Trigger |
|---|---|
| `query.model:v1` | One message per newly-ready downstream node after a SUCCEEDED node is processed; for rerun/rebase only the rebased rows on the **dispatch frontier** get an initial `query.model:v1` (a rebased row whose upstream is itself rebased waits until that upstream completes; inherited rows are already SUCCEEDED at dispatch and never enter the executor pipeline) |
| `schedules.loaded:v1` | Produced by `ReleasePromotedHandler` after a successful topology swap (schedule names list + service_metadata + topology_generation) |
| `run.entries.dispatched:v1` | Produced by every `Snapshot`-driven handler (`HandleSchedulerStarted`, `HandleRerun`, `HandleRebase`, `HandleSingleNodeRun`) after the projection is materialised. Carries all task entries with pre-assigned UUIDs, plus per-task `Status` (defaults `"pending"`, `"succeeded"` for inherited rows) and `InheritedFromTaskID` (empty for non-inherited; root-resolved source `task_id` for inherited). Each `DispatchedTask` stamps `MaxRetries = pkg/events.DefaultTaskMaxRetries (= 2)` so state's `task_tracker.max_retries` matches the k8s retry budget. |
| `run.entries.dispatch_failed:v1` | Produced by `HandleSingleNodeRunHandler` on `snapshot.ErrTargetNotFound`, and by `HandleSingleNodeRunHandler`, `HandleRerunHandler`, `HandleRebaseHandler`, and `HandleSchedulerStartedHandler` on `snapshot.ErrEmptyProjection`. Symmetric counterpart of `run.entries.dispatched:v1`: same `scheduler_tracker` target, opposite outcome. State row-locks the row, marks status=`failed`, emits `run.finalized:v1`. |

### No gRPC calls to `state`

Orchestrator no longer calls `state` gRPC for any internal writes. All state mutations flow through the Redis event pipeline.

## Processing Logic

### On `scheduler.started:v1` — HandleSchedulerStarted

1. Parses `scheduler.started:v1` into `SchedulerStartedCmd{ ScheduleID, ScheduleName, Kind, SourceRunID }`.
2. Creates a `Run` node in Neo4j via `snapshotService.Snapshot(ctx, Params{...})` with selector `LatestFullDAG`, stamping `:Run.kind`, `:Run.source_run_id` (when non-nil), and copying `topology_generation` + `service_metadata` from `:TopologyRoot` (since `source_run_id` is empty for cron/trigger).
3. `SnapshotWriter.WriteRunAndExecutesEdges` creates `EXECUTES` edges (all initially PENDING) with pre-assigned task UUIDs stored on the edge.
4. Produces `run.entries.dispatched:v1` via outbox with: `run_id`, `schedule_name`, `manifest_versions`, full task entry list (each with `task_id`, node coordinates, `node_type`, `service_name`, `Status="pending"`, `InheritedFromTaskID=""`).
5. Dispatches the initial frontier — every projection row the `LatestFullDAG` selector marked `ReadyToDispatch` (no in-DAG upstream: seeds first, else roots) gets a `query.model:v1`. This frontier is computed by the selector from the DAG edges in one batched read; the handler does **not** issue a second Neo4j query for root/seed classification. The run aggregate dispatches the rest via `NodeUnblocked` as upstreams complete.

`state` consumes `run.entries.dispatched:v1` to create task rows, set `total_task_count`, and mark the run as initialized.

When `Snapshot(LatestFullDAG)` returns `snapshot.ErrEmptyProjection` (a schedule whose topology has zero active `:Table` nodes), the handler emits `run.entries.dispatch_failed:v1` with `reason=empty_projection` and the run finalises as `failed`.

### On `release.promoted:v1` — ReleasePromotedHandler

Receives the full promoted topology for a release (each node carries its `image_tag`, already joined by `release-controller`). The handler runs in one Postgres UoW transaction:

1. Dedup on `message_processing`, keyed `(message_id, release.promoted:v1)`; a secondary uniqueness key on the upstream outbox entry ID catches a re-XADD under a fresh Redis message ID.
2. `ReleasePromotionRepository.PromoteRelease` performs the atomic Neo4j topology swap in a single Neo4j transaction (retire-then-orphan-cleanup; see "Topology swap pattern" below). It short-circuits (`changed=false`) when the `:Meta {key:'current_release'}` singleton already records this `release_id`.
3. If `changed=true`, increment `topology_state.topology_generation`; otherwise read the current value. Either way, write the per-service `service_metadata` and the generation onto `:TopologyRoot` (idempotent MERGE).
4. Always write a `schedules.loaded:v1` outbox entry with the schedule names, `service_metadata`, and `topology_generation`. The `event_id` is a deterministic UUID v5 of `(namespace, release_id)`, so re-emissions from idempotent redeliveries are deduplicated by state's `ScheduleCatalogHandler`.

Schedule graph reads and new run snapshots only consider `active=true` `Table` nodes, while historical `Run` graphs remain intact through their `EXECUTES` edges.

### On `trigger.rerun:v1` — HandleRerun

The rerun entry point materialises a fresh `Snapshot(SourcePinnedDAG)` against a newly-minted `:Run`. `state.RerunHandler.TriggerRerun` (gRPC) writes a `trigger.rerun:v1` outbox row in the same Postgres tx that **inserts a new** `scheduler_tracker` row (`kind='rerun'`, `source_run_id=<src>`); the source row is left untouched at its terminal state. The orchestrator consumes the resulting Redis message and runs `HandleRerun` in one Postgres UoW transaction:

1. Dedup on `message_processing`.
2. Build `Params{ RunID, ScheduleName, Kind: "rerun", SourceRunID: src }` with selector `SourcePinnedDAG{}`.
3. `snapshotService.Snapshot(ctx, params)` — `SourcePinnedDAG.SelectTasks` reads the source `:Run`'s `:EXECUTES` set via `TopologyReader`, seeds the rebase set with all non-SUCCEEDED source tasks, grows it via `DescendantsInSourceRun`, and classifies the rest as **inherited** (carried forward with the source's stored status and `task_id` resolved to its lineage root via `inherited_from_task_id`). `SnapshotWriter.WriteRunAndExecutesEdges` MERGEs the new `:Run` (inheriting `topology_generation` + `service_metadata` from source `:Run`, NOT from `:TopologyRoot`) and writes one `:EXECUTES` edge per projected task, all with the source's pinned `image_tag` + `manifest_version`.
4. orchestrator outbox writes (same tx):
   - 1× `run.entries.dispatched:v1` with the FULL projection — both rebased (Status="pending") and inherited (Status="succeeded", `InheritedFromTaskID=<root>`) rows. State's `RunEntriesDispatchedHandler` creates `task_tracker` rows honouring per-task status, and may auto-rollup directly to terminal if every task is already terminal.
   - N× `query.model:v1` for the rebase **dispatch frontier** only — rebased rows whose every upstream is inherited-SUCCEEDED. Rebased rows behind another rebased upstream are dispatched later by the run aggregate (`NodeUnblocked`) or cascade-skipped if that upstream re-fails. Inherited rows are already SUCCEEDED and never enter the executor pipeline.

The source `:Run` and its `task_tracker` rows are never mutated.

### On `trigger.rebase:v1` — HandleRebase

Entry point for rebase from a terminal `FAILED`/`CANCELLED` run. `state.RebaseHandler.TriggerRebase` (gRPC) writes a `trigger.rebase:v1` outbox row in the same Postgres tx that inserts a new `scheduler_tracker` row (`kind='rebase'`, `source_run_id=<src>`). The handler runs in one Postgres UoW transaction:

1. Dedup on `message_processing`.
2. Build `Params{ RunID: newRunID, ScheduleName, Kind: "rebase", SourceRunID: src }` with selector `RebasePartition{Source: src}`.
3. `snapshotService.Snapshot(ctx, params)` — `RebasePartition.SelectTasks` reads the source `:Run`'s `:EXECUTES` set + the latest `:Table`s via `TopologyReader` and computes:
   - **rebase_set**: every source task with status ≠ SUCCEEDED, plus their descendants in the latest topology, plus new arrivals (nodes in latest not in source).
   - **inherit_set**: SUCCEEDED tasks in source that still exist in latest, minus rebase_set.
   - **drop_set**: tasks in source absent from latest (silently dropped).
   Rebased rows project as PENDING with **latest** `image_tag` + `manifest_version`. Inherited rows project as SUCCEEDED with the **source's** pinned pair plus a root-resolved `inherited_from_task_id` (the projector resolves transitively, so chain depth stays ≤ 1 even for rebase-of-rebase). `SnapshotWriter.WriteRunAndExecutesEdges` MERGEs the new `:Run` (inheriting `topology_generation` + `service_metadata` from source `:Run`) and writes the projected `:EXECUTES` edges.
4. orchestrator outbox writes (same tx):
   - 1× `run.entries.dispatched:v1` with the FULL projection — rebased (`Status="pending"`) + inherited (`Status="succeeded"`, `InheritedFromTaskID=<root>`).
   - N× `query.model:v1` for the rebase **dispatch frontier** only (rebased rows whose every upstream is inherited-SUCCEEDED; the rest follow via `NodeUnblocked`/cascade-skip).

If `rebase_set ∩ inherit_set` is empty (selector returns zero entries — should never happen given upstream eligibility checks but guarded defensively), the handler still emits `run.entries.dispatched:v1` with an empty list and lets state's auto-rollup terminate the run.

### Shared helpers

Both consumer handlers (`HandleRerun`, `HandleRebase`) delegate the projection-to-outbox pipeline to `service/handlers/dispatch_derived_run.go`. The helper takes the materialised projection and emits the run-level `run.entries.dispatched:v1` outbox entry plus one `query.model:v1` entry per PENDING row. Inherited terminal rows (`FAILED` / `CANCELLED` / `SKIPPED`) round-trip their status verbatim — coercing them to `pending` would create `task_tracker` rows the executor never runs. `EmitDispatchFailed` in `service/handlers/dispatch_failed.go` writes a `run.entries.dispatch_failed:v1` outbox entry when a selector returns `ErrEmptyProjection`.

### On `trigger.single_node_run:v1` — HandleSingleNodeRunHandler

Entry point for a single-node ad-hoc run. The handler runs in one Postgres UoW transaction:

1. Dedup on `message_processing`.
2. `snapshotService.Snapshot(ctx, Params{...})` with selector `SingleNode{Target, MetadataSource, SourceRunID?}` — creates a `:Run` node and exactly one `EXECUTES` edge with a pre-assigned task UUID.
   - **latest mode** (`metadata_source=latest`): selector reads metadata from `:TopologyRoot` + the current `:Table` node; new `:Run` inherits topology fields from `:TopologyRoot`.
   - **stale mode** (`metadata_source=snapshot_of_run`): selector reads metadata from the source `:Run`'s `EXECUTES` edge for the same node (preserving the original run's `image_tag` and `manifest_version`); new `:Run` inherits `topology_generation` + `service_metadata` from the source `:Run` instead of `:TopologyRoot`.
3. On `ErrTargetNotFound` (node absent in Neo4j): orchestrator outbox writes `run.entries.dispatch_failed:v1` for the synthesised run — no further dispatch. State's `RunEntriesDispatchFailedConsumer` row-locks the synthesised `scheduler_tracker`, marks it `failed`, and writes `run.finalized:v1`. Idempotent on already-terminal rows.
4. On success, orchestrator outbox writes (same tx):
   - 1× `run.entries.dispatched:v1` with the single task entry — consumed by `state` to create the `task_tracker` row, set `total_task_count=1`, and mark `init_status=completed, status=RUNNING`.
   - 1× `query.model:v1` for the single target node — consumed by `executor-controller` to launch the K8s job.

There is exactly one task and no pre-existing run graph; the handler does not touch the `Run` aggregate.

### On `node.updated:v1` — HandleNodeCompleted

```
1. Open Postgres transaction (UoW)

2. Dedup: INSERT IF NOT EXISTS into message_processing
   -> if state is completed/acked or in-flight on another instance: ack and return

3. Cancelled-schedule guard: if cancelled_schedules contains the schedule_id,
   mark message_processing=completed, commit, and return (no aggregate mutation)

4. Aggregate loop (retried on ErrVersionConflict):
   a. agg, err := runs.Rehydrate(ctx, runID, ScopeNodeCompletion{Key, Status})
   b. events, err := agg.CompleteNode(key, status)
      - on ErrNodeAlreadyTerminal: idempotent re-delivery — mark completed and commit
   c. err := runs.Save(ctx, agg)
      - on ErrVersionConflict: continue (re-enter loop from step a)
   d. Translate each domain event into an outbox entry (same Postgres tx):
        NodeUnblocked     -> query.model:v1            (node_ready_for_execution)
        NodeCascadeSkipped -> task.status.updated:v1   (cascade_task_skipped)
        RunFinalized      -> no outbox entry; :Run.terminal_status and
                             :Run.completed_at are written by runs.Save

5. Update message_processing state -> completed; commit transaction
```

`runs.Save` writes `:Run.terminal_status` / `:Run.completed_at` when the aggregate finalises internally. A separate Redis consumer on `run.finalized:v1` projects state's authoritative terminal outcome — succeeded, failed, or cancelled — onto the same fields whenever a run's terminal transition is not produced by the aggregate. This covers full-inherited rebases that never publish `node.updated:v1` events and cancelled runs, which emit `run.finalized:v1` (status `cancelled`) from state's `Run.Cancel()` rather than through the task-completion path. State remains the source of truth for run terminality; orchestrator's role on this stream is read-only persistence.

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer (`scheduler.started:v1`) | Reads and dispatches to HandleSchedulerStarted handler |
| Redis consumer (`node.updated:v1`) | Reads and dispatches to HandleNodeCompleted handler |
| Redis consumer (`release.promoted:v1`) | Reads and dispatches to ReleasePromotedHandler |
| Redis consumer (`trigger.rerun:v1`) | Reads and dispatches to HandleRerun handler |
| Redis consumer (`trigger.rebase:v1`) | Reads and dispatches to HandleRebase handler |
| Redis consumer (`trigger.single_node_run:v1`) | Reads and dispatches to HandleSingleNodeRunHandler |
| Redis consumer (`run.finalized:v1`) | Projects state's terminal outcome (succeeded, failed, or cancelled) onto Neo4j `:Run.completed_at` / `terminal_status`. Covers runs that produce no `node.updated:v1` traffic (full-inherited rebases, cancelled runs). |
| Outbox processor (`pkg/outbox.Processor`) | Polls `orchestrator_outbox` for pending entries; publishes each row to its `stream_name` via `orchestrator/adapters/publisher.OutboxPublisher` |
| RunSweeper | Periodically deletes expired `Run` nodes (and their `EXECUTES` edges) older than `retention_days` |

## gRPC Callers

| Service | Methods used |
|---|---|
| `ui-service` | `GetScheduleGraph`, `ListRuns`, `GetRunGraph`, `ListScheduleTopologies` |
| `continuo CLI` | `GetScheduleGraph` |

Orchestrator calls no external gRPC services.

## Read-side query services (`service/queries/`)

CQRS read-side application services live under `orchestrator/service/queries/`,
mirroring the existing write-side `service/handlers/` package. Query services
compose read-side stores; command handlers compose write-side stores.

`RunQueryService` is the first orchestrator component to join Neo4j and
Postgres on the read path:

- `GetRunGraph(runID)` — returns the run's nodes and edges from Neo4j (via
  `RunQueryPort`, implemented by `OrchestratorQueryRepository`) plus
  `:Run.topology_generation` and the latest
  `topology_state.topology_generation` from Postgres. Backs the rerun
  confirmation modal in ui-service.
- `ListActiveRunDrifts()` — surfaces the single newest in-flight run per schedule plus the latest `topology_state.topology_generation`. "In-flight" means `completed_at IS NULL`; `run.finalized:v1` stamps `completed_at` for all terminal outcomes (succeeded, failed, cancelled), so a cancelled run leaves the active set once the projection arrives. The underlying `ListActiveRuns` query (Neo4j adapter) orders by `schedule_name`, then `created_at DESC`; `RunQueryService` keeps the head row per schedule. Used by e2e tests as a probe for orchestrator-side active-run state.

`topology_state` (Postgres) is now read by both:
- the **write path** — `ReleasePromotedHandler` calls
  `TopologyStateRepository.IncrementGeneration` to allocate the next
  monotonic value, then stamps it onto `:TopologyRoot` on each promotion
  that changes the current release.
- the **read path** — `RunQueryService` calls `TopologyStateRepository.GetGeneration`
  on each query to expose the current value. The port lives at
  `orchestrator/domain/repository/port.go`; the Postgres adapter is the only
  current implementation.

### Topology-shape cache

`GetScheduleGraph` returns the immutable topology *shape* (Table nodes +
DEPENDS_ON edges) for a schedule. That shape is fixed for a given
`topology_generation`: a manifest load or release promotion bumps the
generation, producing a new shape under a new key. The Neo4j adapter wraps the
schedule reader in `CachingScheduleGraphReader`
(`orchestrator/adapters/neo4j/schedule_graph_cache.go`), a bounded LRU (32
entries) keyed by `(schedule_name, topology_generation)`.

On each call the decorator probes the current generation
(`TopologyStateReader.GetGeneration`, a cheap Postgres single-row read) to form
the key. A hit returns the cached shape without touching Neo4j; a miss runs the
shape query once and stores it. A bumped generation yields a new key, naturally
invalidating stale shapes — no explicit eviction is needed. If the generation
probe fails, the call falls back to a direct uncached read, so a transient
Postgres error degrades performance, not correctness.

**Cache boundary:** only the immutable shape is cached. `GetRunGraph` overlays
live `:EXECUTES.status` per node and is intentionally **not** routed through the
cache, so run status is always fresh.

### Drift contract

Per-run `topology_generation = 0` means **drift unknown** (the run was created
before topology tracking, or the property is unset). Consumers MUST render
this distinctly from "no drift" — typically as "topology version unknown for
this run". `:Run` nodes with the property set carry a value `>= 1`; the latest
is monotonically incremented before any `:Table` stamping, so the invariant is
`run.topology_generation <= latest_topology_generation`. Inversions are logged
as warnings by `RunQueryService` but otherwise pass through unmodified.

## Topology swap pattern

`ReleasePromotionRepository.PromoteRelease` (invoked by `ReleasePromotedHandler`) replaces the `:Table` topology graph using a **retire-then-orphan-cleanup** pattern, not a truncate-and-load. Every `:Table` node may have incoming `:Run-[:EXECUTES]->Table` edges from active or historical runs; a blunt `MATCH (n:Table) DETACH DELETE n` would erase that history and strand in-flight runs. The swap runs in a single Neo4j transaction:

1. **Retire** — `MATCH (t:Table) WHERE NOT t.unique_id IN $new SET t.active = false, t.retired_at = $now`. Nodes stay in the graph; their `:Run` edges still resolve.
2. **Upsert** — `MERGE (existing:Table {unique_id: ...}) SET ..., active = true, retired_at = NULL`. Reactivates any node that's back in the new topology.
3. **Clear outgoing edges** on upserted nodes so dependencies can be rebuilt: `MATCH (a:Table {unique_id: ...})-[r:DEPENDS_ON]->() DELETE r`.
4. **Rebuild `:DEPENDS_ON`** between nodes in the new topology. References to `unique_id`s outside the candidate set are silently skipped (matches dbt's compile semantics for cross-service dependencies).
5. **Orphan cleanup** — `MATCH (t:Table) WHERE COALESCE(t.active, true) = false AND NOT EXISTS { (:Run)-[:EXECUTES]->(t) } DETACH DELETE t`. Removes only the `:Table` nodes no run still references.

Before any of these steps, the transaction reads the `:Meta {key:'current_release'}` singleton; if its `release_id` already matches the incoming release it commits empty and returns `changed=false` (idempotent redelivery). After the swap it MERGEs the `:Meta` singleton with the new `release_id`. Each upserted `:Table` node is stamped with `release_id`.

Reference impl: `orchestrator/adapters/neo4j/release_promotion_repository.go` (`PromoteRelease`).

**Design constraint for any topology-write path:** any handler that replaces `:Table` topology in this orchestrator MUST follow the same pattern. Truncate-and-load is not safe in this graph because it destroys `:Run-[:EXECUTES]->:Table` run history.

When the Neo4j Go driver binds `time.Time` parameters into Cypher, it uses `Location().String()` as a timezone identifier and rejects `"Local"`. Always pass `now.UTC()` at the boundary.

## Reliability Patterns

- **Inbound dedup**: `message_processing` keyed by `(message_id, stream_name)`; INSERT IF NOT EXISTS prevents double-processing. The composite key is required because Redis Streams assign IDs per-stream, so a single publisher can emit two messages to two streams in the same millisecond and produce identical message IDs that must not collide.
- **Neo4j updates are outside the Postgres tx**: topology and status writes are idempotent; if the tx fails the message will be redelivered
- **Snapshot reconciliation**: the promoted topology on `release.promoted:v1` is treated as authoritative; nodes missing from it are retired from the current topology automatically (and deleted once no `:Run` references them)
- **Pre-assigned task UUIDs**: task IDs are committed to Neo4j EXECUTES edges before `run.entries.dispatched:v1` is produced; the outbox processor reads them at publish time, ensuring consistent IDs across retries
- **No state gRPC dependency**: orchestrator is fully decoupled from the state write path; all state mutations go through events
- **RunSweeper**: deletion is best-effort; a sweep failure is logged and retried on the next tick
