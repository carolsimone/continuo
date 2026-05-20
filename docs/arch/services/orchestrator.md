# orchestrator

## Purpose

`orchestrator` owns the dependency topology in Neo4j, handles run initialization and node completion events, and serves gRPC queries for the UI.

It is responsible for:
- ingesting topology from manifest-controller (via `manifest.loaded:v1`)
- initializing run snapshots on scheduler start (via `scheduler.started:v1`)
- processing node completion events, unlocking downstream nodes (via `node.updated:v1`)
- producing task dispatch events for the executor pipeline
- serving read queries for schedule graphs, run listings, and run graphs (gRPC `OrchestratorQuery` service)

## Owned Storage

### Neo4j

| Entity | Description |
|---|---|
| `Table` node | One per model/seed; carries topology metadata, `last_updated_at`, and an `active` flag for current-topology reconciliation |
| `Run` node | One per schedule run; carries `terminal_status`, `created_at`, `completed_at`, `kind`, `source_run_id`, `topology_generation`, `total_nodes`, `terminal_count`, `version` |
| `DEPENDS_ON` relationship | Directed edge from downstream to upstream `Table` |
| `EXECUTES` relationship | Directed edge from `Run` to `Table`; carries per-run `status`, pre-assigned `task_id` UUID, per-task `image_tag` + `manifest_version`, and (for rebase-projected inherited rows only) an optional `inherited_from_task_id` property pointing to the root executed `task_id` in the source lineage |

Task UUIDs are pre-assigned when the `EXECUTES` edges are created during run snapshot initialization. This allows `run.entries.dispatched:v1` to carry canonical task IDs without a round-trip to `state`.

#### `:Run` node properties

| Property | Type | Notes |
|---|---|---|
| `terminal_status` | string | Set at run completion |
| `created_at` | datetime | Stamped at `Snapshot` time |
| `completed_at` | datetime | Stamped at run finalization |
| `topology_generation` | int64 | Stamped at `Snapshot` time. Derived runs (`rerun`, `rebase`, stale-mode `single_node_run`) copy it from the source `:Run`; fresh runs (`cron`, `trigger`, latest-mode `single_node_run`) copy it from `:TopologyRoot`. `0` means drift unknown. |
| `service_metadata` | map | Same source-vs-`:TopologyRoot` rule as `topology_generation` — derived runs inherit the pair from the source `:Run`; fresh runs read it from `:TopologyRoot`. |
| `kind` | string | Mirrors `scheduler_tracker.kind` (`cron`, `trigger`, `rerun`, `rebase`, `single_node_run`). Stamped at `Snapshot` time via `ON CREATE SET run.kind = $kind` / `ON MATCH SET run.kind = COALESCE(run.kind, $kind)` — the original kind survives idempotent replay. Reads use `COALESCE(r.kind, "cron")` as a defensive default. |
| `source_run_id` | string (UUID) | Optional; set via Cypher `FOREACH` only when non-nil. Reads use `COALESCE(r.source_run_id, "")` for the absent case. NULL for `cron`/`trigger` runs. **Drives the topology-metadata source decision:** when present, the new `:Run` inherits `topology_generation` + `service_metadata` from the referenced source run; when absent, both are read from `:TopologyRoot`. |
| `total_nodes` | int | Number of `:EXECUTES` edges materialised at `Snapshot` time. Used by the `Run` aggregate to detect terminal-count == total-count finalization. |
| `terminal_count` | int | Number of `:EXECUTES` edges currently in a terminal status (`SUCCEEDED`, `FAILED`, `SKIPPED`, `CANCELLED`). Incremented by `AggregateRepository.Save` as nodes complete; equal to `total_nodes` when the run finalises. |
| `failed_count` | int | Number of `:EXECUTES` edges that transitioned to `FAILED` directly (cascade-skipped nodes do not count). Drives the aggregate's terminal-status decision when `terminal_count == total_nodes` so finalisation works even when the failed node is outside the currently loaded subgraph. |
| `version` | int | Optimistic-concurrency token. Incremented on every aggregate mutation (`CompleteNode`); `AggregateRepository.Save` compares against `COALESCE(run.version, 0)` and returns `ErrVersionConflict` on mismatch. |

### Run aggregate (`orchestrator/domain/run`)

The write-side of node-completion processing is the `Run` aggregate root. It owns `RunID`, `ScheduleName`, `Status`, the counters above (`TotalNodes`, `TerminalCount`, `Version`), and an operation-scoped `map[NodeKey]*RunNode` subgraph. Methods:

- `NewRun(runID, scheduleName, nodes) *Run` — constructs the aggregate from its initial node set; called once after `Snapshot` materialises the `:EXECUTES` edges.
- `CompleteNode(key, status) ([]DomainEvent, error)` — transitions the target node, enforces cascade-skip on `FAILED`, computes immediate unblocks on `SUCCEEDED`/`SKIPPED`, and emits `RunFinalized` when `TerminalCount == TotalNodes`.

Domain events live in `orchestrator/domain/run/events.go`:

| Event | Meaning |
|---|---|
| `NodeUnblocked` | Every upstream of an immediate-downstream node is now terminal; the orchestrator outbox writes `query.model:v1` for the unblocked node. |
| `NodeCascadeSkipped` | A `PENDING` downstream was forced to `SKIPPED` by an upstream `FAILED`; the orchestrator outbox writes `task.status.updated:v1` (`cascade_task_skipped`) so `state` updates the task row. |
| `RunFinalized` | All nodes have reached a terminal status inside the aggregate. Neo4j `:Run.terminal_status` and `:Run.completed_at` are written by `AggregateRepository.Save`. For runs that produce no `node.updated:v1` traffic (e.g. full-inherited rebases), a separate `run.finalized:v1` consumer projects state's authoritative scheduler outcome onto the same Neo4j fields. |

Ports (`orchestrator/domain/run/ports.go`):

- `AggregateRepository` (write-side) — `Rehydrate(ctx, runID, scope) (*Run, error)` reconstitutes an operation-scoped subgraph; `Save(ctx, *Run) error` persists node statuses, `total_nodes`/`terminal_count`/`failed_count`/`version`, and `:Run.terminal_status`/`completed_at` when finalised. `Save` checks `Version` against the loaded value (with `COALESCE(run.version, 0)` to admit legacy in-flight runs) and returns `ErrVersionConflict` on mismatch; the handler retries from `Rehydrate`. `terminal_status`/`completed_at` are first-writer-wins so state's authoritative `run.finalized:v1` projection cannot be overwritten by a later aggregate save.
- `RunQueryPort` (read-side, CQRS) — `GetScheduleGraph`, `ListRuns`, `GetRunGraph`, `GetRunTopologyGeneration`, `ListActiveRuns`, `GetScheduleInitNodes`.

`Scope` is a sealed interface with two variants that the adapter uses to scope the Cypher read:

| Hint | Subgraph loaded |
|---|---|
| `ScopeFull` | Every `:EXECUTES` edge for the run (used at initialisation only). |
| `ScopeNodeCompletion{Key, Status}` | `Status="FAILED"` → target + full transitive downstream; `Status="SUCCEEDED"`/`SKIPPED` → target + immediate downstream + each downstream's upstreams. |

Adapter implementations:
- `orchestrator/adapters/neo4j/run_aggregate_repository.go` — `RunAggregateRepository` implements `AggregateRepository`. Also owns `DeleteExpiredRuns` for the sweeper.
- `orchestrator/adapters/neo4j/orchestrator_query_repository.go` — `OrchestratorQueryRepository` implements `RunQueryPort`, including `GetScheduleInitNodes` used by `HandleSchedulerStarted` to identify root and seed nodes.

### Snapshot — domain ports, adapters, and application service

The snapshot pipeline follows a strict layered model:

**Domain ports** (`orchestrator/domain/snapshot/ports.go`):
- `TopologyReader` — read-only port for snapshot selectors; implementations live in the Neo4j adapter and are bound to a single Neo4j `ManagedTransaction`.
- `SnapshotWriter` — write-only port; `WriteRunAndExecutesEdges` MERGEs the `:Run` node (with the metadata-source rule from the table above) and creates one `:EXECUTES` edge per projection entry.
- `TxRunner` — opens a single Neo4j write transaction and hands the caller a paired `TopologyReader` + `SnapshotWriter` scoped to that tx. The caller's `fn` commits on `nil`, rolls back on error.

**Adapter implementations** (`orchestrator/adapters/neo4j/`):
- `TopologyReader` implemented in `topology_reader.go` — all Cypher reads for snapshot policies (DAG loading, source task loading, descendant walks, single-table lookups) live here.
- `SnapshotWriter` implemented in `snapshot_writer.go` — all Cypher writes for run+EXECUTES materialisation live here.
- `SnapshotTxRunner` implemented in `snapshot_tx_runner.go` — opens one Neo4j session/write tx, instantiates the paired reader+writer, and runs the caller's function.

**Application service** (`orchestrator/service/snapshotsvc/Service`):
- `Snapshot(ctx, Params)` — calls `p.Selector.SelectTasks(ctx, reader, p)` then `writer.WriteRunAndExecutesEdges(ctx, p, projection)` inside a single `TxRunner.Run` call. Returns the full projection so handlers can build outbox events without re-reading Neo4j.
- Constructed via `snapshotsvc.NewService(snapshotTxRunner, logger)` in `main.go`.

**Handler interface** (`orchestrator/service/handlers/deps.go`):
- `handlers.SnapshotService` is a narrow handler-local interface `{ Snapshot(ctx, snapshot.Params) ([]snapshot.TaskProjection, error) }`, satisfied by `*snapshotsvc.Service`. Defined here so handler tests can substitute a fake.
- Five handlers receive a `SnapshotService`: `InitializeRunHandler`, `HandleSchedulerStartedHandler`, `HandleRerunHandler`, `HandleRebaseHandler`, `HandleSingleNodeRunHandler`. `HandleNodeCompleted` does not snapshot.

The `Params` struct (defined in `orchestrator/domain/snapshot/`) carries `RunID`, `ScheduleName`, `Kind`, `SourceRunID`, plus a `Selector` interface that decides which tasks land in the projection and with which `(initial_status, image_tag, manifest_version, inherited_from_task_id?)`.

Four selectors live in `orchestrator/domain/snapshot/`, are pure Go, and read all topology data through the `TopologyReader` port:

| Selector | Used by | Source of topology | Per-task projection |
|---|---|---|---|
| `LatestFullDAG` | `HandleSchedulerStarted` (cron / trigger) | latest `:Table`s for the schedule + upstream dbt-seeds | every task PENDING with the latest `image_tag` + `manifest_version` |
| `SourcePinnedDAG` | `HandleRerun` | source `:Run`'s `:EXECUTES` set | non-SUCCEEDED source tasks + their descendants within the source's pinned `:EXECUTES` set → rebased PENDING with **source's** pinned `(image_tag, manifest_version)`; everything else → InitialStatus preserved from source with `inherited_from_task_id` pointing at the source's executed `task_id` (root-resolved) |
| `SingleNode` | `HandleSingleNodeRun` | exactly one node | latest mode reads metadata from `:TopologyRoot` + the `:Table`; `snapshot_of_run` mode reads metadata from the source `:Run`'s `:EXECUTES` edge for that node |
| `RebasePartition` | `HandleRebase` | rebase set ∪ inherit set against latest `:Table`s | rebased rows = PENDING with **latest** metadata; inherited rows = SUCCEEDED with **source's** pinned metadata + root-resolved `inherited_from_task_id` (always points at the lineage-root executed `task_id` — chain depth ≤ 1 even for rebase-of-rebase) |

### Postgres (`continuo_orchestrator`)

| Table | Purpose |
|---|---|
| `message_processing` | Inbound dedup: one row per consumed Redis message, scoped by `(message_id, stream_name)`; tracks state (`processing` / `completed` / `acked`) |
| `orchestrator_outbox` | Canonical transactional outbox — each write-time side effect is a separate row with a JSONB payload; `pkg/outbox.Processor` polls and publishes to the typed Redis stream per row |
| `topology_state` | Singleton row holding the monotonic `topology_generation` counter |
| `cancelled_schedules` | Schedule IDs cancelled by an upstream control-plane signal; consulted to short-circuit terminal-state processing for already-cancelled runs |
| `rejected_topology_messages` | Forensics for permanently-rejected `manifest.loaded:v1` payloads |

All `orchestrator_outbox` rows conform to the canonical schema: `id`, `message_processing_id` (nullable), `aggregate_type`, `aggregate_id`, `event_type`, `payload` (JSONB), `stream_name`, `status`, `retry_count`, `max_retries`, `created_at`, `processed_at`, `error_message`.

### Adapter-replaceable ports

All ports the service layer depends on for adapter-replaceable storage live in `orchestrator/domain/repository/port.go`:

| Port | Storage adapter |
|---|---|
| `OutboxRepository` | Postgres |
| `MessageProcessingRepository` | Postgres |
| `CancelledSchedulesRepository` | Postgres |
| `RejectedTopologyRepository` | Postgres |
| `TopologyStateRepository` | Postgres |
| `TopologyRepository` | Neo4j |

Two narrow exceptions are allowed to import adapter packages directly: `service/uow/uow.go` (composition root for transactional repositories) and `service/handlers/ingest_topology_integration_test.go` (integration test wiring the real Postgres adapter against a live database). Production handlers and unit-test fakes hold only `repository.*` types.

Read-side ports specific to the CQRS query path (`RunReader`, `TopologyStateReader`) are defined where they are consumed — `service/queries/run_query_service.go` — and intentionally not promoted into `domain/repository/`.

## Inbound Interfaces

### Redis consumers

| Stream | Group | Handler |
|---|---|---|
| `scheduler.started:v1` | `orchestrator_scheduler_started` | `HandleSchedulerStarted` — runs `Snapshot(LatestFullDAG)`, creates EXECUTES edges with pre-assigned task UUIDs, produces `run.entries.dispatched:v1` + `query.model:v1` for seed/root nodes |
| `node.updated:v1` | `orchestrator_node_updated` | `HandleNodeCompleted` — updates EXECUTES status, unlocks downstream nodes (SUCCEEDED → `query.model:v1` for newly-ready nodes; FAILED → cascade-skip downstream + emit `task.status.updated:v1` for skipped tasks) |
| `manifest.loaded:v1` | `orchestrator_manifest_loaded` | `IngestTopology` — applies the full manifest snapshot, rewrites `DEPENDS_ON`, retires missing `Table` nodes, then emits `schedules.loaded:v1` |
| `trigger.rerun:v1` | `orchestrator_rerun` | `HandleRerun` — runs `Snapshot(SourcePinnedDAG{})` against the **new** `:Run` minted by state's `TriggerRerun`, projecting the source's pinned DAG with non-SUCCEEDED tasks + their descendants as rebased PENDING and the rest as inherited at source's stored status; produces `run.entries.dispatched:v1` (full projection) + `query.model:v1` for rebased rows only |
| `trigger.rebase:v1` | `orchestrator-rebase` | `HandleRebaseHandler` — runs `Snapshot(RebasePartition)` against the new `:Run`; projects rebase_set ∪ inherit_set against the latest topology; produces `run.entries.dispatched:v1` (full projection) + `query.model:v1` for rebased rows only |
| `initialize.run:v1` | `orchestrator_initialize_run` | `InitializeRunHandler` — secondary entry point (no active producer); runs `Snapshot(LatestFullDAG)` + dispatch, same shape as `HandleSchedulerStarted` |
| `trigger.single_node_run:v1` | `orchestrator_single_node_run` | `HandleSingleNodeRunHandler` — runs `Snapshot(SingleNode)` and dispatches the one task; see details below |

### gRPC server — `OrchestratorQuery` (port 50052)

| Method | Description |
|---|---|
| `GetScheduleGraph` | Returns all Table nodes and DEPENDS_ON edges for a schedule |
| `ListRuns` | Returns Run nodes for a schedule, newest first |
| `GetRunGraph` | Returns nodes and EXECUTES edges for a specific run, with per-node status. Also returns `run_topology_generation` (stamped on the `:Run` node at `Snapshot` time; `0` means "drift unknown" — not "no drift") and `latest_topology_generation` (current `topology_state.topology_generation` Postgres singleton). |
| `ListActiveRunDrifts` | Returns one `ActiveRunDrift` row per `is_running=true` schedule (`schedule_name`, `run_id`, `run_topology_generation`) plus the orchestrator's current `latest_topology_generation`. Drives the dashboard's per-schedule active-run drift indicator without forcing the UI to call `GetRunGraph` for every active schedule. |

### HTTP (port 8087)

- `GET /health` — liveness probe only

## Outbound Interfaces

### Redis producer (via outbox)

| Stream | Trigger |
|---|---|
| `query.model:v1` | One message per newly-ready downstream node after a SUCCEEDED node is processed; for rerun/rebase only the **rebased** rows get a `query.model:v1` (inherited rows are already SUCCEEDED at dispatch and never enter the executor pipeline) |
| `schedules.loaded:v1` | Produced by IngestTopology after successful topology load (schedule names list) |
| `run.entries.dispatched:v1` | Produced by every `Snapshot`-driven handler (`HandleSchedulerStarted`, `HandleRerun`, `HandleRebase`, `HandleSingleNodeRun`) after the projection is materialised. Carries all task entries with pre-assigned UUIDs, root/seed node lists, plus per-task `Status` (defaults `"pending"`, `"succeeded"` for inherited rows) and `InheritedFromTaskID` (empty for non-inherited; root-resolved source `task_id` for inherited). Each `DispatchedTask` stamps `MaxRetries = pkg/events.DefaultTaskMaxRetries (= 2)` so state's `task_tracker.max_retries` matches the k8s retry budget. |
| `run.entries.dispatch_failed:v1` | Produced by `HandleSingleNodeRunHandler` on `snapshot.ErrTargetNotFound`, and by `HandleSingleNodeRunHandler`, `HandleRerunHandler`, `HandleRebaseHandler`, and `HandleSchedulerStartedHandler` on `snapshot.ErrEmptyProjection`. Symmetric counterpart of `run.entries.dispatched:v1`: same `scheduler_tracker` target, opposite outcome. State row-locks the row, marks status=`failed`, emits `run.finalized:v1`. |

### No gRPC calls to `state`

Orchestrator no longer calls `state` gRPC for any internal writes. All state mutations flow through the Redis event pipeline.

## Processing Logic

### On `scheduler.started:v1` — HandleSchedulerStarted

1. Parses `scheduler.started:v1` into `SchedulerStartedCmd{ ScheduleID, ScheduleName, Kind, SourceRunID }`.
2. Creates a `Run` node in Neo4j via `snapshotService.Snapshot(ctx, Params{...})` with selector `LatestFullDAG`, stamping `:Run.kind`, `:Run.source_run_id` (when non-nil), and copying `topology_generation` + `service_metadata` from `:TopologyRoot` (since `source_run_id` is empty for cron/trigger).
3. `SnapshotWriter.WriteRunAndExecutesEdges` creates `EXECUTES` edges (all initially PENDING) with pre-assigned task UUIDs stored on the edge.
4. Identifies root and seed nodes.
5. Produces `run.entries.dispatched:v1` via outbox with: `run_id`, `schedule_name`, `manifest_versions`, full task entry list (each with `task_id`, node coordinates, `node_type`, `service_name`, `Status="pending"`, `InheritedFromTaskID=""`).

`state` consumes `run.entries.dispatched:v1` to create task rows, set `total_task_count`, and mark the run as initialized.

When `Snapshot(LatestFullDAG)` returns `snapshot.ErrEmptyProjection` (a schedule whose topology has zero active `:Table` nodes), the handler emits `run.entries.dispatch_failed:v1` with `reason=empty_projection` and the run finalises as `failed`.

### On `manifest.loaded:v1` — IngestTopology

Receives a JSON payload of topology nodes representing the full current manifest snapshot. Within one Neo4j write transaction it:

1. upserts every current `Table` node and rewrites its outgoing `DEPENDS_ON` edges
2. marks any previously-active `Table` node missing from the payload as `active=false`
3. deletes inactive `Table` nodes only when they are no longer referenced by any `Run` snapshot

Schedule graph reads and new run snapshots only consider `active=true` `Table` nodes, while historical `Run` graphs remain intact through their `EXECUTES` edges. Deduplication still keys off the Redis message ID via `message_processing`.

### On `trigger.rerun:v1` — HandleRerun

The rerun entry point materialises a fresh `Snapshot(SourcePinnedDAG)` against a newly-minted `:Run`. `state.RerunHandler.TriggerRerun` (gRPC) writes a `trigger.rerun:v1` outbox row in the same Postgres tx that **inserts a new** `scheduler_tracker` row (`kind='rerun'`, `source_run_id=<src>`); the source row is left untouched at its terminal state. The orchestrator consumes the resulting Redis message and runs `HandleRerun` in one Postgres UoW transaction:

1. Dedup on `message_processing`.
2. Build `Params{ RunID, ScheduleName, Kind: "rerun", SourceRunID: src }` with selector `SourcePinnedDAG{}`.
3. `snapshotService.Snapshot(ctx, params)` — `SourcePinnedDAG.SelectTasks` reads the source `:Run`'s `:EXECUTES` set via `TopologyReader`, seeds the rebase set with all non-SUCCEEDED source tasks, grows it via `DescendantsInSourceRun`, and classifies the rest as **inherited** (carried forward with the source's stored status and `task_id` resolved to its lineage root via `inherited_from_task_id`). `SnapshotWriter.WriteRunAndExecutesEdges` MERGEs the new `:Run` (inheriting `topology_generation` + `service_metadata` from source `:Run`, NOT from `:TopologyRoot`) and writes one `:EXECUTES` edge per projected task, all with the source's pinned `image_tag` + `manifest_version`.
4. orchestrator outbox writes (same tx):
   - 1× `run.entries.dispatched:v1` with the FULL projection — both rebased (Status="pending") and inherited (Status="succeeded", `InheritedFromTaskID=<root>`) rows. State's `RunEntriesDispatchedHandler` creates `task_tracker` rows honouring per-task status, and may auto-rollup directly to terminal if every task is already terminal.
   - N× `query.model:v1` for the **rebased rows only** (inherited rows are already SUCCEEDED and never enter the executor pipeline).

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
   - N× `query.model:v1` for rebased rows only.

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

`runs.Save` writes `:Run.terminal_status` / `:Run.completed_at` when the aggregate finalises internally. A separate Redis consumer on `run.finalized:v1` projects state's authoritative outcome onto the same fields whenever a run's terminal transition is not produced by the aggregate — primarily full-inherited rebases that never publish `node.updated:v1` events. State remains the source of truth for `terminal_task_count == total_task_count`; orchestrator's role on this stream is read-only persistence.

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer (`scheduler.started:v1`) | Reads and dispatches to HandleSchedulerStarted handler |
| Redis consumer (`node.updated:v1`) | Reads and dispatches to HandleNodeCompleted handler |
| Redis consumer (`manifest.loaded:v1`) | Reads and dispatches to IngestTopology handler |
| Redis consumer (`trigger.rerun:v1`) | Reads and dispatches to HandleRerun handler |
| Redis consumer (`trigger.rebase:v1`) | Reads and dispatches to HandleRebase handler |
| Redis consumer (`trigger.single_node_run:v1`) | Reads and dispatches to HandleSingleNodeRunHandler |
| Redis consumer (`initialize.run:v1`) | Secondary entry path; runs `Snapshot(LatestFullDAG)` + dispatch. No active producer. |
| Redis consumer (`run.finalized:v1`) | Projects state's terminal scheduler outcome onto Neo4j `:Run.completed_at` / `terminal_status`. Active fallback for runs that produce no `node.updated:v1` traffic. |
| Outbox processor (`pkg/outbox.Processor`) | Polls `orchestrator_outbox` for pending entries; publishes each row to its `stream_name` via `orchestrator/adapters/publisher.OutboxPublisher` |
| RunSweeper | Periodically deletes expired `Run` nodes (and their `EXECUTES` edges) older than `retention_days` |

## gRPC Callers

| Service | Methods used |
|---|---|
| `ui-service` | `GetScheduleGraph`, `ListRuns`, `GetRunGraph`, `ListActiveRunDrifts` |
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
- `ListActiveRunDrifts()` — returns every `:Run` with `completed_at IS NULL`
  plus the latest `topology_state.topology_generation`. Backs the dashboard
  schedule list in ui-service.

`topology_state` (Postgres) is now read by both:
- the **write path** — `IngestTopologyHandler.IncrementGeneration` allocates
  the next monotonic value before stamping every `:Table` node with it.
- the **read path** — `RunQueryService` calls `TopologyStateRepository.GetGeneration`
  on each query to expose the current value. The port lives at
  `orchestrator/domain/repository/port.go`; the Postgres adapter is the only
  current implementation.

### Drift contract

Per-run `topology_generation = 0` means **drift unknown** (the run was created
before topology tracking, or the property is unset). Consumers MUST render
this distinctly from "no drift" — typically as "topology version unknown for
this run". `:Run` nodes with the property set carry a value `>= 1`; the latest
is monotonically incremented before any `:Table` stamping, so the invariant is
`run.topology_generation <= latest_topology_generation`. Inversions are logged
as warnings by `RunQueryService` but otherwise pass through unmodified.

## Reliability Patterns

- **Inbound dedup**: `message_processing` keyed by `(message_id, stream_name)`; INSERT IF NOT EXISTS prevents double-processing. The composite key is required because Redis Streams assign IDs per-stream, so a single publisher can emit two messages to two streams in the same millisecond and produce identical message IDs that must not collide.
- **Neo4j updates are outside the Postgres tx**: topology and status writes are idempotent; if the tx fails the message will be redelivered
- **Snapshot reconciliation**: `manifest.loaded:v1` is treated as authoritative; nodes missing from the latest payload are retired from the current topology automatically
- **Pre-assigned task UUIDs**: task IDs are committed to Neo4j EXECUTES edges before `run.entries.dispatched:v1` is produced; the outbox processor reads them at publish time, ensuring consistent IDs across retries
- **No state gRPC dependency**: orchestrator is fully decoupled from the state write path; all state mutations go through events
- **RunSweeper**: deletion is best-effort; a sweep failure is logged and retried on the next tick
