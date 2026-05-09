# orchestrator

## Purpose

`orchestrator` is the merged replacement for the former `graph`, `dependency-controller`, and `startup-controller` services. It owns the dependency topology in Neo4j, handles run initialization and node completion events, and serves gRPC queries for the UI.

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
| `Run` node | One per schedule run; carries `terminal_status`, `created_at`, `completed_at`, `kind`, `source_run_id`, `topology_generation` |
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

The `run.Repository` interface exposes the following Neo4j read methods used during rerun and rebase handling:
- `GetNodeType(ctx, schemaName, tableName) (string, error)` — reads `node_type` from the current `Table` node (queries by `schema_name` property)
- `GetNodeServiceName(ctx, schemaName, tableName) (string, error)` — reads `service_name` from the current `Table` node (queries by `schema_name` property)

### `Snapshot` — unified plan/materialise routine

`RunRepository` exposes a single materialisation entry point:

```go
RunRepository.Snapshot(ctx context.Context, params Params) error
```

The `Params` struct (defined in `orchestrator/domain/snapshot/`) carries `RunID`, `ScheduleName`, `Kind`, `SourceRunID`, plus a `Selector` interface that decides which tasks land in the projection and with which `(initial_status, image_tag, manifest_version, inherited_from_task_id?)`. Materialisation is a single Cypher write tx that MERGEs the `:Run` (with the metadata-source rule from the table above) and creates one `:EXECUTES` edge per `Selector` entry.

Four selectors live in `orchestrator/domain/snapshot/`:

| Selector | Used by | Source of topology | Per-task projection |
|---|---|---|---|
| `LatestFullDAG` | `HandleSchedulerStarted` (cron / trigger) | latest `:Table`s for the schedule + upstream dbt-seeds | every task PENDING with the latest `image_tag` + `manifest_version` |
| `SourcePinnedDAG` | `HandleRerun` | source `:Run`'s `:EXECUTES` set | target node + non-SUCCEEDED descendants → rebased PENDING with **source's** pinned metadata; everything else → SUCCEEDED inherit with the same pinned pair + `inherited_from_task_id` pointing at the source's executed `task_id` |
| `SingleNode` | `HandleSingleNodeRun` | exactly one node | latest mode reads metadata from `:TopologyRoot` + the `:Table`; `snapshot_of_run` mode reads metadata from the source `:Run`'s `:EXECUTES` edge for that node |
| `RebasePartition` | `HandleRebase` | rebase set ∪ inherit set against latest `:Table`s | rebased rows = PENDING with **latest** metadata; inherited rows = SUCCEEDED with **source's** pinned metadata + root-resolved `inherited_from_task_id` (always points at the lineage-root executed `task_id` — chain depth ≤ 1 even for rebase-of-rebase) |

### Postgres (`continuo_orchestrator`)

| Table | Purpose |
|---|---|
| `message_processing` | Inbound dedup: one row per consumed Redis message ID; tracks state (`processing` / `completed` / `acked`) |
| `outbox` | Outbound dispatch intents: one row per downstream node ready for execution |
| `published_messages` | Outbound idempotency: records `(outbox_entry_id, redis_message_id)` after successful publish |

## Inbound Interfaces

### Redis consumers

| Stream | Group | Handler |
|---|---|---|
| `scheduler.started:v1` | `orchestrator_scheduler_started` | `HandleSchedulerStarted` — runs `Snapshot(LatestFullDAG)`, creates EXECUTES edges with pre-assigned task UUIDs, produces `run.entries.dispatched:v1` + `query.model:v1` for seed/root nodes |
| `node.updated:v1` | `orchestrator_node_updated` | `HandleNodeCompleted` — updates EXECUTES status, unlocks downstream nodes (SUCCEEDED → `query.model:v1` for newly-ready nodes; FAILED → cascade-skip downstream + emit `task.status.updated:v1` for skipped tasks) |
| `manifest.loaded:v1` | `orchestrator_manifest_loaded` | `IngestTopology` — applies the full manifest snapshot, rewrites `DEPENDS_ON`, retires missing `Table` nodes, then emits `schedules.loaded:v1` |
| `trigger.rerun:v1` | `orchestrator_rerun` | `HandleRerun` — runs `Snapshot(SourcePinnedDAG)` against the **new** `:Run` minted by state's `TriggerRerun`, projecting the source's full pinned DAG with target + non-SUCCEEDED descendants as rebased PENDING and everything else as SUCCEEDED inherits; produces `run.entries.dispatched:v1` (full projection) + `query.model:v1` for rebased rows only |
| `trigger.rebase:v1` | `orchestrator-rebase` (env: `REBASE_GROUP`) | `HandleRebaseHandler` — runs `Snapshot(RebasePartition)` against the new `:Run`; projects rebase_set ∪ inherit_set against the latest topology; produces `run.entries.dispatched:v1` (full projection) + `query.model:v1` for rebased rows only |
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
| `run.entries.dispatch_failed:v1` | Produced by HandleSingleNodeRunHandler when `ErrTargetNotFound`. Symmetric counterpart of `run.entries.dispatched:v1`: same `scheduler_tracker` target, opposite outcome. State row-locks the row, marks status=`failed`, emits `run.finalized:v1`. |

### No gRPC calls to `state`

Orchestrator no longer calls `state` gRPC for any internal writes. All state mutations flow through the Redis event pipeline.

## Processing Logic

### On `scheduler.started:v1` — HandleSchedulerStarted

1. Parses `scheduler.started:v1` into `SchedulerStartedCmd{ ScheduleID, ScheduleName, Kind, SourceRunID }`.
2. Creates a `Run` node in Neo4j via `RunRepository.Snapshot(ctx, Params{...})` with selector `LatestFullDAG`, stamping `:Run.kind`, `:Run.source_run_id` (when non-nil), and copying `topology_generation` + `service_metadata` from `:TopologyRoot` (since `source_run_id` is empty for cron/trigger).
3. Materialiser creates `EXECUTES` edges (all initially PENDING) with pre-assigned task UUIDs stored on the edge.
4. Identifies root and seed nodes.
5. Produces `run.entries.dispatched:v1` via outbox with: `run_id`, `schedule_name`, `manifest_versions`, full task entry list (each with `task_id`, node coordinates, `node_type`, `service_name`, `Status="pending"`, `InheritedFromTaskID=""`).

`state` consumes `run.entries.dispatched:v1` to create task rows, set `total_task_count`, and mark the run as initialized.

### On `manifest.loaded:v1` — IngestTopology

Receives a JSON payload of topology nodes representing the full current manifest snapshot. Within one Neo4j write transaction it:

1. upserts every current `Table` node and rewrites its outgoing `DEPENDS_ON` edges
2. marks any previously-active `Table` node missing from the payload as `active=false`
3. deletes inactive `Table` nodes only when they are no longer referenced by any `Run` snapshot

Schedule graph reads and new run snapshots only consider `active=true` `Table` nodes, while historical `Run` graphs remain intact through their `EXECUTES` edges. Deduplication still keys off the Redis message ID via `message_processing`.

### On `trigger.rerun:v1` — HandleRerun

The rerun entry point materialises a fresh `Snapshot(SourcePinnedDAG)` against a newly-minted `:Run`. `state.RerunHandler.TriggerRerun` (gRPC) writes a `trigger.rerun:v1` outbox row in the same Postgres tx that **inserts a new** `scheduler_tracker` row (`kind='rerun'`, `source_run_id=<src>`); the source row is left untouched at its terminal state. The orchestrator consumes the resulting Redis message and runs `HandleRerun` in one Postgres UoW transaction:

1. Dedup on `message_processing`.
2. Build `Params{ RunID: newRunID, ScheduleName, Kind: "rerun", SourceRunID: src }` with selector `SourcePinnedDAG{Source: src, Target: (service, schema, table)}`.
3. `RunRepository.Snapshot(ctx, params)` — selector reads the source `:Run`'s `:EXECUTES` set, classifies each task as either **rebased** (target + transitively-downstream non-SUCCEEDED) or **inherited** (everything else, carried forward as SUCCEEDED with the source's `task_id` resolved to its lineage root via `inherited_from_task_id`). Materialiser MERGEs the new `:Run` (inheriting `topology_generation` + `service_metadata` from source `:Run`, NOT from `:TopologyRoot`) and writes one `:EXECUTES` edge per projected task, all with the source's pinned `image_tag` + `manifest_version`.
4. orchestrator outbox writes (same tx):
   - 1× `run.entries.dispatched:v1` with the FULL projection — both rebased (Status="pending") and inherited (Status="succeeded", `InheritedFromTaskID=<root>`) rows. State's `RunEntriesDispatchedHandler` creates `task_tracker` rows honouring per-task status, and may auto-rollup directly to terminal if every task is already terminal.
   - N× `query.model:v1` for the **rebased rows only** (inherited rows are already SUCCEEDED and never enter the executor pipeline).

The source `:Run` and its `task_tracker` rows are never mutated.

### On `trigger.rebase:v1` — HandleRebase

Entry point for rebase from a terminal `FAILED`/`CANCELLED` run. `state.RebaseHandler.TriggerRebase` (gRPC) writes a `trigger.rebase:v1` outbox row in the same Postgres tx that inserts a new `scheduler_tracker` row (`kind='rebase'`, `source_run_id=<src>`). The handler runs in one Postgres UoW transaction:

1. Dedup on `message_processing`.
2. Build `Params{ RunID: newRunID, ScheduleName, Kind: "rebase", SourceRunID: src }` with selector `RebasePartition{Source: src}`.
3. `RunRepository.Snapshot(ctx, params)` — selector reads the source `:Run`'s `:EXECUTES` set + the latest `:Table`s for the schedule and computes:
   - **rebase_set**: every source task with status ≠ SUCCEEDED, plus their descendants in the latest topology, plus new arrivals (nodes in latest not in source).
   - **inherit_set**: SUCCEEDED tasks in source that still exist in latest, minus rebase_set.
   - **drop_set**: tasks in source absent from latest (silently dropped).
   Rebased rows project as PENDING with **latest** `image_tag` + `manifest_version`. Inherited rows project as SUCCEEDED with the **source's** pinned pair plus a root-resolved `inherited_from_task_id` (the projector resolves transitively, so chain depth stays ≤ 1 even for rebase-of-rebase). Materialiser MERGEs the new `:Run` (inheriting `topology_generation` + `service_metadata` from source `:Run`) and writes the projected `:EXECUTES` edges.
4. orchestrator outbox writes (same tx):
   - 1× `run.entries.dispatched:v1` with the FULL projection — rebased (`Status="pending"`) + inherited (`Status="succeeded"`, `InheritedFromTaskID=<root>`).
   - N× `query.model:v1` for rebased rows only.

If `rebase_set ∩ inherit_set` is empty (selector returns zero entries — should never happen given upstream eligibility checks but guarded defensively), the handler still emits `run.entries.dispatched:v1` with an empty list and lets state's auto-rollup terminate the run.

### On `trigger.single_node_run:v1` — HandleSingleNodeRunHandler

Entry point for a single-node ad-hoc run. The handler runs in one Postgres UoW transaction:

1. Dedup on `message_processing`.
2. Neo4j: `RunRepository.Snapshot(ctx, Params{...})` with selector `SingleNode{Target, MetadataSource, SourceRunID?}` — creates a `:Run` node and exactly one `EXECUTES` edge with a pre-assigned task UUID.
   - **latest mode** (`metadata_source=latest`): selector reads metadata from `:TopologyRoot` + the current `:Table` node; new `:Run` inherits topology fields from `:TopologyRoot`.
   - **stale mode** (`metadata_source=snapshot_of_run`): selector reads metadata from the source `:Run`'s `EXECUTES` edge for the same node (preserving the original run's `image_tag` and `manifest_version`); new `:Run` inherits `topology_generation` + `service_metadata` from the source `:Run` instead of `:TopologyRoot`.
3. On `ErrTargetNotFound` (node absent in Neo4j): orchestrator outbox writes `run.entries.dispatch_failed:v1` for the synthesised run — no further dispatch. State's `RunEntriesDispatchFailedConsumer` row-locks the synthesised `scheduler_tracker`, marks it `failed`, and writes `run.finalized:v1`. Idempotent on already-terminal rows.
4. On success, orchestrator outbox writes (same tx):
   - 1× `run.entries.dispatched:v1` with the single task entry — consumed by `state` to create the `task_tracker` row, set `total_task_count=1`, and mark `init_status=completed, status=RUNNING`.
   - 1× `query.model:v1` for the single target node — consumed by `executor-controller` to launch the K8s job.

No rerun-descended helpers (`GetSkippedDownstreamTaskIDs`, `ResetSkippedDownstreamToPending`) are called; there is exactly one task and no pre-existing run graph to reset.

### On `node.updated:v1` — HandleNodeCompleted

```
1. Dedup check: insert into message_processing (INSERT IF NOT EXISTS)
   -> if already completed/acked: skip and return

2. Open Postgres transaction
   a. UpdateNodeStatus in Neo4j (outside tx - idempotent)
   b. If status == SUCCEEDED:
      - GetReadyDownstream from Neo4j
      - For each ready node:
          - Read pre-assigned task_id from EXECUTES edge in Neo4j
          - ComputeJobName(service, schema, table, scheduleID) — includes 8-char schedule_id suffix to prevent cross-run K8s job name collisions
          - Write outbox entry (query.model:v1)
      - Update message_processing state -> completed
   c. Commit transaction
```

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer (`scheduler.started:v1`) | Reads and dispatches to HandleSchedulerStarted handler |
| Redis consumer (`node.updated:v1`) | Reads and dispatches to HandleNodeCompleted handler |
| Redis consumer (`manifest.loaded:v1`) | Reads and dispatches to IngestTopology handler |
| Redis consumer (`trigger.rerun:v1`) | Reads and dispatches to HandleRerun handler |
| Redis consumer (`trigger.rebase:v1`) | Reads and dispatches to HandleRebase handler. Stream/group configurable via env: `REBASE_STREAM` (default `trigger.rebase:v1`), `REBASE_GROUP` (default `orchestrator-rebase`). |
| Redis consumer (`trigger.single_node_run:v1`) | Reads and dispatches to HandleSingleNodeRunHandler |
| Redis consumer (`initialize.run:v1`) | Secondary entry path; runs `Snapshot(LatestFullDAG)` + dispatch. No active producer. |
| Outbox processor | Polls outbox for pending entries; publishes to `query.model:v1`, `run.entries.dispatched:v1`, `run.entries.dispatch_failed:v1`, `schedules.loaded:v1`; records in `published_messages` |
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
  `CompositeRunRepository`) plus `:Run.topology_generation` and the latest
  `topology_state.topology_generation` from Postgres. Backs the rerun
  confirmation modal in ui-service.
- `ListActiveRunDrifts()` — returns every `:Run` with `completed_at IS NULL`
  plus the latest `topology_state.topology_generation`. Backs the dashboard
  schedule list in ui-service.

`topology_state` (Postgres) is now read by both:
- the **write path** — `IngestTopologyHandler.IncrementGeneration` allocates
  the next monotonic value before stamping every `:Table` node with it.
- the **read path** — `RunQueryService` calls `TopologyStateRepository.GetGeneration`
  on each query to expose the current value.

### Drift contract

Per-run `topology_generation = 0` means **drift unknown** (the run was created
before topology tracking, or the property is unset). Consumers MUST render
this distinctly from "no drift" — typically as "topology version unknown for
this run". `:Run` nodes with the property set carry a value `>= 1`; the latest
is monotonically incremented before any `:Table` stamping, so the invariant is
`run.topology_generation <= latest_topology_generation`. Inversions are logged
as warnings by `RunQueryService` but otherwise pass through unmodified.

## Reliability Patterns

- **Inbound dedup**: `message_processing` keyed by Redis message ID; INSERT IF NOT EXISTS prevents double-processing
- **Outbound idempotency**: `published_messages` tracks published outbox entries; republishing is safe
- **Neo4j updates are outside the Postgres tx**: topology and status writes are idempotent; if the tx fails the message will be redelivered
- **Snapshot reconciliation**: `manifest.loaded:v1` is treated as authoritative; nodes missing from the latest payload are retired from the current topology automatically
- **Pre-assigned task UUIDs**: task IDs are committed to Neo4j EXECUTES edges before `run.entries.dispatched:v1` is produced; the outbox processor reads them at publish time, ensuring consistent IDs across retries
- **No state gRPC dependency**: orchestrator is fully decoupled from the state write path; all state mutations go through events
- **RunSweeper**: deletion is best-effort; a sweep failure is logged and retried on the next tick
