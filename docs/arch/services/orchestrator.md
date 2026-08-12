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
| `Table` node | One per model/seed; carries topology metadata (`schema_name`, `table_name`, `service_name`, `node_type`, `test_count`, `schedule_name`, `image_tag`, `original_file_path`), the node's current `content_hash`, `last_updated_at`, an `active` flag for current-topology reconciliation, and per-node provenance properties (`last_commit_sha`, `last_repo`, `last_changed_at`, `last_release_id`) that record the most recent release in which the node's `content_hash` changed. `content_hash` is refreshed on every promotion, changed or not, and is what the code-version path compares a release's code bundle against. `test_count` is the number of dbt tests declared for the node, carried on `release.promoted:v1` and defaulted to `0` when absent (`COALESCE(t.test_count, 0)` on read). |
| `Run` node | One per schedule run; carries `terminal_status`, `created_at`, `completed_at`, `kind`, `source_run_id`, `operation`, `topology_generation`, `total_nodes`, `terminal_count`, `version` |
| `DEPENDS_ON` relationship | Directed edge from downstream to upstream `Table` |
| `EXECUTES` relationship | Directed edge from `Run` to `Table`; carries per-run `status`, pre-assigned `task_id` UUID, per-task `image_tag` + `manifest_version`, a `content_hash` pinning which code version the run executed, and (for rebase-projected inherited rows only) an optional `inherited_from_task_id` property pointing to the root executed `task_id` in the source lineage. `content_hash` comes from the snapshot projection and shares its origin with `image_tag`: the latest `:Table` for a fresh run, the SOURCE run's edge for a rerun, a rebase-inherited row, or a `snapshot_of_run` task. Reading the live `:Table` for a derived run would record code the run never executed |
| `NodeVersion` node | One immutable code state of a node, identified by (`unique_id`, `content_hash`); chained by `PREVIOUS` and pointed at from its `:Table` by `CURRENT`. See "Code-version history — graph model" below. |
| `CodeUnit` / `CodeUnitVersion` node | The same pattern for a shared-code unit (a dbt macro today, a Python module later), keyed by a service-namespaced `unit_id` and its source `checksum` |
| `CURRENT` relationship | Points a `Table` at its current `NodeVersion`, or a `CodeUnit` at its current `CodeUnitVersion`; carries the `promoted_at` and `release_id` at which the pointer was set |
| `code_version_promoted_at` property | The per-node ordering watermark, on `:Table` and on `:CodeUnit`. Holds the newest promotion time seen for that node, advanced on every release that mentions it — including one whose code is unchanged. Every pointer and chain write is guarded on it, and writing it takes the node's write lock, which serialises concurrent promotions |
| `PREVIOUS` relationship | Chains a version to the one it superseded; written only when the version is created, which keeps the chain acyclic across a revert |
| `USES_CODE` relationship | Points a `NodeVersion` at every `CodeUnitVersion` in scope when that code ran — the transitive closure, so "the model did not change but its hash flipped" resolves to the exact unit that did |

Task UUIDs are pre-assigned when the `EXECUTES` edges are created during run snapshot initialization. This allows `run.entries.dispatched:v1` to carry canonical task IDs without a round-trip to `state`.

#### `:Run` node properties

| Property | Type | Notes |
|---|---|---|
| `terminal_status` | string | Set at run completion. Always stored in the canonical lowercase form (`succeeded`, `failed`, `cancelled`, `skipped`) regardless of which writer stamps it: `AggregateRepository.Save` normalizes the uppercase in-memory aggregate status, `FinalizeRun` lowercases the `run.finalized:v1` wire value, and the snapshot writer stamps `cancelled` for a cancel-before-snapshot run. Casing translation is owned entirely by the neo4j adapter (`run_status_codec.go`): it maps the domain enum to the stored lowercase form on write and back to the enum on rehydration, so the domain `RunStatus.IsTerminal()` is a plain exact comparison over `SUCCEEDED`/`FAILED`/`CANCELLED`/`SKIPPED` with no storage-casing knowledge. |
| `created_at` | datetime | Stamped at `Snapshot` time |
| `completed_at` | datetime | Stamped when the run reaches any terminal outcome (succeeded, failed, cancelled, or skipped) |
| `topology_generation` | int64 | Stamped at `Snapshot` time. Derived runs (`rerun`, `rebase`, stale-mode `single_node_run`) copy it from the source `:Run`; fresh runs (`cron`, `trigger`, latest-mode `single_node_run`) copy it from `:TopologyRoot`. `0` means drift unknown. |
| `service_metadata` | map | Same source-vs-`:TopologyRoot` rule as `topology_generation` — derived runs inherit the pair from the source `:Run`; fresh runs read it from `:TopologyRoot`. |
| `kind` | string | Mirrors `scheduler_tracker.kind` (`cron`, `trigger`, `rerun`, `rebase`, `single_node_run`). Stamped at `Snapshot` time via `ON CREATE SET run.kind = $kind` / `ON MATCH SET run.kind = COALESCE(run.kind, $kind)` — the original kind survives idempotent replay. Reads use `COALESCE(r.kind, "cron")` as a defensive default. |
| `operation` | string | The dbt verb the run applies to its nodes: `""` (default, `run`), `test`, or `build`. Stamped at `Snapshot` time from `Params.Operation`, carried on `scheduler.started:v1` / `trigger.single_node_run:v1`, and for derived runs (rerun/rebase) resolved from the source run via `SnapshotService.SourceOperation` before the `Snapshot` call, so a rerun/rebase inherits its source's operation. `AggregateRepository`'s rehydrate reads it back (`COALESCE(run.operation, '')`) into `Run.Operation` and, for `operation="test"` only, discards every loaded node's upstream/downstream edges before handing the subgraph to the `Run` aggregate — a whole-DAG test run is an edgeless flat fan-out, so `CompleteNode` must never unblock or cascade-skip across the shared `:Table` `DEPENDS_ON` topology. `operation="build"` keeps its edges and runs the normal dependency-ordered, cascade-skip path exactly like a plain run; `Run.Operation` is stamped onto every `NodeUnblocked` event so a node dispatched later via the downstream-unblock path runs the same dbt verb (`dbt build`) as the run's initial frontier. |
| `initiated_by` | string | The user who initiated the run, or the `system` sentinel for cron / platform-initiated runs. Carried on the `scheduler.started:v1` / `trigger.*:v1` event payloads (parsed via the shared `optionalInitiatedBy` helper, defaulting absent values to `system`), threaded through `snapshot.Params.InitiatedBy`, and stamped `ON CREATE`. Mirrors `state`'s `scheduler_tracker.initiated_by` for full cross-service provenance. |
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
| `node_version_unique` | constraint, `:NodeVersion(unique_id, content_hash)` unique | Makes a code version's identity its (node, code) pair, so a redelivered release MERGEs onto the version it already wrote instead of minting a duplicate. |
| `node_version_uid` | index, `:NodeVersion(unique_id)` | History lookups that start from a node's `unique_id` rather than from its `:Table` — the path that still works after a retired node's `:Table` is deleted. |
| `code_unit_unique` | constraint, `:CodeUnit(unit_id)` unique | The `MERGE (:CodeUnit {unit_id})` upsert during version ingestion. |
| `code_unit_version_unique` | constraint, `:CodeUnitVersion(unit_id, checksum)` unique | The same identity guarantee for a shared-code unit's source state. |

After the DDL is applied and the indexes are online, `InitSchema` runs a set of idempotent data migrations. The current one folds any legacy uppercase `:Run.terminal_status` (`SUCCEEDED`/`FAILED`, stamped by an earlier aggregate writer) down to the canonical lowercase form so the UI never reads a mixed casing: `MATCH (r:Run) WHERE r.terminal_status IN ['SUCCEEDED','FAILED'] SET r.terminal_status = toLower(r.terminal_status)`. Re-running it is a no-op once every row is lowercase.

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
| `NodeUnblocked` | Every upstream of an immediate-downstream node is now terminal; the orchestrator outbox writes `query.model:v1` for the unblocked node, carrying the run's `Operation` (`""`/`test`/`build`) so a node dispatched via unblock runs the same dbt verb as the run's initial frontier. |
| `NodeCascadeSkipped` | A `PENDING` downstream was forced to `SKIPPED` by an upstream `FAILED`; the orchestrator outbox writes `task.status.updated:v1` (`cascade_task_skipped`) so `state` updates the task row. |
| `RunFinalized` | All nodes have reached a terminal status inside the aggregate. Neo4j `:Run.terminal_status` and `:Run.completed_at` are written by `AggregateRepository.Save`. For runs that produce no `node.updated:v1` traffic (e.g. full-inherited rebases and cancelled runs), a separate `run.finalized:v1` consumer projects state's authoritative terminal outcome — succeeded, failed, cancelled, or skipped — onto the same Neo4j fields. |

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
- `orchestrator/adapters/neo4j/run_aggregate_repository.go` — `RunAggregateRepository` implements `AggregateRepository`. Also owns `DeleteExpiredRuns` for the sweeper. Both the `ScopeNodeCompletion` rehydrate target match and the `Save` write match on the full `(service_name, schema_name, table_name)` identity so two services sharing a `schema.table` name in one run resolve to distinct `:Table` targets: `Save` persists all loaded node statuses in a single `UNWIND` round trip keyed on that 4-tuple identity, and rehydrate loads only the addressed service's target and its neighbourhood rather than both same-named nodes.
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
- Three handler implementations receive a `SnapshotService`: `HandleSchedulerStartedHandler`, `DerivedRunHandler` (one parameterized implementation behind both the rerun and rebase triggers — `NewHandleRerunHandler`/`NewHandleRebaseHandler` configure it with the kind, stream, and selector), and `HandleSingleNodeRunHandler`. `HandleNodeCompleted` does not snapshot.

The `Params` struct (defined in `orchestrator/domain/snapshot/`) carries `RunID`, `ScheduleName`, `Kind`, `SourceRunID`, `Operation` (`""` | `"run"` | `"test"` | `"build"`), plus a `Selector` interface that decides which tasks land in the projection and with which `(initial_status, image_tag, manifest_version, inherited_from_task_id?)`. All four selectors read `Operation`: `LatestFullDAG` and `SingleNode` branch their dispatch/gating logic directly on it (test-only edgeless fan-out / zero-test gate); `SourcePinnedDAG` and `RebasePartition` only inspect it to return `ErrRerunOfTestUnsupported` for a source run whose stored operation is `"test"` — a `build` or default `""` source is accepted unchanged and simply flows through as the derived run's own `Operation`.

Four selectors live in `orchestrator/domain/snapshot/`, are pure Go, and read all topology data through the `TopologyReader` port:

| Selector | Used by | Source of topology | Per-task projection |
|---|---|---|---|
| `LatestFullDAG` | `HandleSchedulerStarted` (cron / trigger) | latest `:Table`s for the schedule + upstream dbt-seeds | every task PENDING with the latest `image_tag` + `manifest_version`; `ReadyToDispatch` marks the dispatch frontier (nodes with no in-DAG upstream — seeds first, else roots), computed from one batched immediate-descendants read. When `Operation == "test"` this ordering is skipped entirely: the projection keeps only nodes with a **known, positive** `test_count` and every kept row is `ReadyToDispatch = true` — a flat, edgeless fan-out where `dbt test` runs independently per node with no blocking frontier. A node whose `test_count` is a known zero **or unset** (topology written before `test_count` capture) is excluded, so a test run never dispatches a `dbt test` that would no-op on a node it cannot confirm has tests. The `SingleNode` selector below applies the identical rule (only a known, positive `test_count` is runnable), so single-node and whole-DAG test runs gate consistently. `Operation == "build"` (and the default `""`/`"run"`) takes the normal dependency-ordered path: no node is dropped for `test_count == 0`, and `dbt build --select <node>` runs in the same seeds-first-else-roots frontier order as a plain run, with failed nodes cascade-skipping their descendants via the run aggregate's usual blocked/unblocked bookkeeping. |
| `SourcePinnedDAG` | `HandleRerun` | source `:Run`'s `:EXECUTES` set | non-SUCCEEDED source tasks + their descendants within the source's pinned `:EXECUTES` set → rebased PENDING with **source's** pinned `(image_tag, manifest_version)`; everything else → InitialStatus preserved from source with `inherited_from_task_id` pointing at the source's executed `task_id` (root-resolved). Before calling `Snapshot`, `DerivedRunHandler` resolves the source run's operation via `SnapshotService.SourceOperation` (backed by `TopologyReader.SourceRunOperation`) and threads it into `Params.Operation`; the selector returns `ErrRerunOfTestUnsupported` when `p.Operation == "test"` — a rerun carries no per-task operation, so it cannot safely reissue `dbt test`. A source `operation == "build"` proceeds normally: `Params.Operation` carries `build` through the projection into the derived run's frontier `query.model:v1` dispatch, so the rebuilt tasks re-run `dbt build`. |
| `SingleNode` | `HandleSingleNodeRun` | exactly one node | latest mode reads metadata from `:TopologyRoot` + the `:Table`; `snapshot_of_run` mode reads metadata from the source `:Run`'s `:EXECUTES` edge for that node. When `Operation == "test"` and the resolved node's `test_count` is not a known positive value — a known zero, or an unset count (topology predating `test_count` capture) — the selector returns `ErrNoTests` instead of a projection: there is no test we can confirm exists to run, and a re-release backfills a concrete count to make a genuinely-tested node runnable. `Operation == "build"` has no equivalent gate: a node with zero tests is still built. `snapshot_of_run` mode is unaffected by the `SourcePinnedDAG`/`RebasePartition` rejection below: a single-node run legitimately allows a test-operation source. |
| `RebasePartition` | `HandleRebase` | rebase set ∪ inherit set against latest `:Table`s | rebased rows = PENDING with **latest** metadata; inherited rows = SUCCEEDED with **source's** pinned metadata + root-resolved `inherited_from_task_id` (always points at the lineage-root executed `task_id` — chain depth ≤ 1 even for rebase-of-rebase). Same operation resolution and guard as `SourcePinnedDAG`: `DerivedRunHandler` resolves the source's operation via `SnapshotService.SourceOperation` and the selector rejects with `ErrRerunOfTestUnsupported` when it is `test`; a source `operation == "build"` is inherited and carried through to the rebase frontier's `query.model:v1` dispatch. |

**Dispatch frontier.** Each PENDING projection row carries `ReadyToDispatch`, computed identically across all paths (cron `LatestFullDAG`, rerun `SourcePinnedDAG`, rebase `RebasePartition`). A PENDING node is on the frontier (`ReadyToDispatch = true`) unless it has an **immediate** (one-hop) PENDING/rebased upstream in the run — i.e. it is a direct in-run dependent of another not-yet-satisfied node. For the cron path this is exactly seeds-first-else-roots: seeds have no upstream and dispatch immediately, roots that depend on a seed wait. The selectors compute this from immediate `DEPENDS_ON` edges (`ImmediateDescendantsIn{LatestTopology,SourceRun}Batch`), deliberately *not* transitive descendants: the run aggregate only unblocks/cascade-skips along immediate in-run edges, so a node blocked via a transitive-only path (its connecting node absent from the run) would never be reached and would stall PENDING forever. `DispatchDerivedRun` emits a `query.model:v1` for the frontier rows only. Blocked rebased rows wait for the run aggregate: as a frontier node completes, `CompleteNode` emits `NodeUnblocked` for newly-ready downstream nodes (→ `query.model:v1`) or cascade-skips them when the upstream fails. This mirrors the fresh-run roots-only dispatch and ensures a re-pended SKIPPED node does not run until its upstream succeeds — and is skipped again if it re-fails.

### Postgres (`continuo_orchestrator`)

| Table | Purpose |
|---|---|
| `message_processing` | Inbound dedup: one row per consumed Redis message, scoped by `(message_id, stream_name)`; tracks state (`processing` / `completed` / `acked`) |
| `orchestrator_outbox` | Canonical transactional outbox — each write-time side effect is a separate row with a JSONB payload; `pkg/outbox.Processor` polls and publishes to the typed Redis stream per row. Each batch pipelines its XADDs over one Redis connection and flips the successful subset in one `UPDATE … WHERE id = ANY(...)`; a full batch immediately drains the next before sleeping to the next tick. Every XADD caps its stream at `MaxLen 10000` (approximate `~`). **Caveat:** approximate trimming can drop the oldest entries before a lagging consumer group reads them; 10000 is the accepted bound |
| `topology_state` | Singleton row holding the monotonic `topology_generation` counter |
| `cancelled_schedules` | Schedule IDs cancelled by an upstream control-plane signal; consulted to short-circuit terminal-state processing for already-cancelled runs |

All `orchestrator_outbox` rows conform to the canonical schema: `id`, `message_processing_id` (nullable), `aggregate_type`, `aggregate_id`, `event_type`, `payload` (JSONB), `stream_name`, `status`, `retry_count`, `max_retries`, `created_at`, `processed_at`, `error_message`, `next_attempt_at` (nullable; `NULL` means due now — see `docs/arch/05-error-classification.md` §Outbox processor resilience).

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
| `CodeVersionRepository` | Neo4j |

Technical collaborators that are not domain concepts live in `orchestrator/service/ports/` instead: `CodeBundleReader` (S3 adapter, `adapters/s3`) reads a release's code-bundle document and returns it decoded, surfacing `ErrBundleNotFound` for an absent object and `ErrBundleMalformed` for one that cannot be interpreted, so the handler — not the adapter — decides which failures are retryable.

One narrow exception is allowed to import adapter packages directly: `service/handlers/release_promoted_handler_integration_test.go` wires the real Postgres and Neo4j adapters against a live database. Production handlers and unit-test fakes hold only `repository.*` types. The `UnitOfWork` interface is declared in `service/uow/uow.go`; its concrete implementation (`PostgresUnitOfWork`) lives in `adapters/postgres/unit_of_work.go`.

The orchestrator wires one long-lived `PostgresUnitOfWork` instance per consumer and reuses it for every inbound message. `Commit` and `Rollback` therefore clear the transaction state unconditionally — including when the underlying commit fails — so a single failed commit cannot wedge the consumer. A handler's deferred `Rollback` runs after a failed `Commit` and finds the transaction already finished; `sql.ErrTxDone` is treated as a successful no-op there, and the next `Begin` on the same instance starts cleanly.

Read-side ports specific to the CQRS query path (`RunReader`, `TopologyStateReader`) are defined where they are consumed — `service/queries/run_query_service.go` — and intentionally not promoted into `domain/repository/`.

## Background loops

Goroutines started in `main.go` run for the process lifetime:
- **Run sweeper** (`internal/sweeper`) — deletes `:Run` nodes older than the retention window.
- **Dispatch watchdog** (`service/watchdog`, every `ORCHESTRATOR_WATCHDOG_INTERVAL_SECONDS`, default 60) — cancels schedules whose dispatch has silently stalled. Each tick computes a cutoff of `now − ORCHESTRATOR_WATCHDOG_NO_PROGRESS_MINUTES` and issues a single `ListStuckCandidates(cutoff)` RPC to state, which answers with one indexed server-side query: the active (`pending|running`) runs that have at least one task, no task in `running`, and a most-recent task older than the cutoff. The watchdog then calls state's `CancelSchedule` for each candidate. This is O(1) RPCs per tick (no per-schedule fan-out) and considers **all** of a run's tasks, so a long-running task anywhere in a wide run never makes it falsely stuck. The watchdog speaks only domain-typed ports (`ports.StuckScheduleReader`, `ports.ScheduleCanceller`), both implemented by `adapters/grpc.StuckScheduleAdapter`; no gRPC/proto wire types enter the application layer. Every tick runs under a `context.WithTimeout` bounded by the interval.
- **Reconciler** (`internal/reconciler`, every `ORCHESTRATOR_RECONCILER_INTERVAL_SECONDS`, default 60) — converges the `:Run` projection to state's authoritative status: it lists active `:Run`s (`completed_at IS NULL`), reads each run's status from state through the orchestrator-owned `ports.RunStatusReader` (implemented by `adapters/grpc.RunStatusReader` over the state gRPC client's `GetScheduler`), and `FinalizeRun`s any that state already reports terminal. It acts only on runs that exist in Neo4j and are terminal in state, so retention-deleted or never-snapshotted runs are never touched. Each tick runs under a `context.WithTimeout` bounded by the interval. This is the ordering-independent backstop for finalizations missed or raced ahead of a run's snapshot.
- **Cancelled-schedules sweeper** — deletes `cancelled_schedules` rows past their TTL. The cutoff is computed in SQL (`cancelled_at < NOW() - make_interval(...)`), comparing the DB-stamped `cancelled_at` against the DB clock so the sweep is immune to host/DB clock skew.
- **Retention sweeper** (`pkg/outbox.RetentionSweeper`, default hourly, `RETENTION_SWEEP_INTERVAL_MINUTES`) — prunes, using DB-clock cutoffs, `orchestrator_outbox` rows with `status='processed'` and terminal (`completed`/`acked`) `message_processing` dedup rows older than the retention window (`RETENTION_DAYS`, default 7); `processing` rows are never purged. A `message_processing` row is additionally excluded from deletion while any `orchestrator_outbox` row still references it via `message_processing_id`, regardless of that outbox row's status — this covers a dead-lettered (`status='failed'`) row, which retention never removes (only `processed` rows age out), as well as a `pending`/`scheduled` row awaiting publish; without the exclusion, the dedup delete would violate `orchestrator_outbox_message_processing_id_fkey`. The excluded row becomes eligible again once its last referencing outbox row is gone. Each delete is bounded by a per-statement `LIMIT` loop. All knobs default safely — no configuration required.

## Inbound Interfaces

### Redis consumers

| Stream | Group | Handler |
|---|---|---|
| `scheduler.started:v1` | `orchestrator_scheduler_started` | `HandleSchedulerStarted` — runs `Snapshot(LatestFullDAG)`, creates EXECUTES edges with pre-assigned task UUIDs, produces `run.entries.dispatched:v1` + `query.model:v1` for the dispatch frontier (`ReadyToDispatch` rows: seeds first, else roots) |
| `node.updated:v1` | `orchestrator_node_updated` | `HandleNodeCompleted` — updates EXECUTES status, unlocks downstream nodes (SUCCEEDED → `query.model:v1` for newly-ready nodes; FAILED → cascade-skip downstream + emit `task.status.updated:v1` for skipped tasks) |
| `release.promoted:v1` | `orchestrator-release-promoted` | `ReleasePromotedHandler` — swaps the `:Table` topology via `ReleasePromotionRepository.PromoteRelease`, refreshes each node's `content_hash`, stamps per-node provenance (`last_commit_sha`, `last_repo`, `last_changed_at`, `last_release_id`) on changed nodes only, increments `topology_generation`, writes `:TopologyRoot` service_metadata, then emits `schedules.loaded:v1` |
| `trigger.rerun:v1` | `orchestrator_rerun` | `DerivedRunHandler` (rerun config) — runs `Snapshot(SourcePinnedDAG{})` against the **new** `:Run` minted by state's `TriggerRerun`, projecting the source's pinned DAG with non-SUCCEEDED tasks + their descendants as rebased PENDING and the rest as inherited at source's stored status; produces `run.entries.dispatched:v1` (full projection) + `query.model:v1` for the rebase **dispatch frontier** only |
| `trigger.rebase:v1` | `orchestrator-rebase` | `DerivedRunHandler` (rebase config) — runs `Snapshot(RebasePartition)` against the new `:Run`; projects rebase_set ∪ inherit_set against the latest topology; produces `run.entries.dispatched:v1` (full projection) + `query.model:v1` for the rebase **dispatch frontier** only. Shares one handler implementation with the rerun trigger, differing only in selector/kind/stream |
| `trigger.single_node_run:v1` | `orchestrator_single_node_run` | `HandleSingleNodeRunHandler` — runs `Snapshot(SingleNode)` and dispatches the one task; see details below |
| `release.promoted:v1` | `orchestrator-release-promoted-versions` | `ReleasePromotedVersionsHandler` — reads the release's code-bundle document from S3 and records the `:NodeVersion` / `:CodeUnitVersion` history behind the topology via `CodeVersionRepository.WriteVersions`; no outbox emission |
| `trigger.promoted_seeds:v1` | `orchestrator-promoted-seeds` | `HandlePromotedSeedsRunHandler` — projects the run `state` created for a promoted release. Snapshots with the `NodeSet` selector over the seeds named on the message, then emits one `run.entries.dispatched:v1` carrying every task and one `query.model:v1` per task |

Each consumer is wired as a `parser → handler` binding under `adapters/redis/`: the parser extracts and validates the message's scalar fields defensively and returns an `events.ErrPermanent`-wrapped error on any malformed field (missing/non-string value, bad UUID, or cross-field rule violation), which the stream consumer ACKs and drops so a single poison message cannot crash-loop the process.

### gRPC server — `OrchestratorQuery` (port 50052)

| Method | Description |
|---|---|
| `GetScheduleGraph` | Returns all Table nodes and DEPENDS_ON edges for a schedule, plus `topology_generation` (current `:TopologyRoot.topology_generation`; `0` means "generation unknown", same contract as `GetRunGraphResponse.latest_topology_generation`). The topology shape is served from an in-process LRU cache keyed by `(schedule_name, topology_generation)` (see "Topology-shape cache" below). |
| `ListRuns` | Returns a page of completed Run nodes for a schedule, newest first. Paginated via `page_size` (clamped to `[1, 200]`, default 50 when unset) and `page_offset` (negatives treated as 0); the response carries `total_count` (completed runs matching the schedule, independent of the page window). A single `count(run)` query and a `SKIP/LIMIT` page query share the same `MATCH` so the filter cannot drift. |
| `GetRunGraph` | Returns nodes and EXECUTES edges for a specific run, with per-node status. Also returns `run_topology_generation` (stamped on the `:Run` node at `Snapshot` time; `0` means "drift unknown" — not "no drift") and `latest_topology_generation` (current `topology_state.topology_generation` Postgres singleton). |
| `ListActiveRunDrifts` | Returns one `ActiveRunDrift` row per schedule that has an in-flight run (`schedule_name`, `run_id`, `run_topology_generation`) plus the orchestrator's current `latest_topology_generation`. "In-flight" means `completed_at IS NULL` on the `:Run` node — a property stamped by the `run.finalized:v1` projection for all terminal outcomes (succeeded, failed, cancelled, skipped). The underlying `ListActiveRuns` query orders results by `schedule_name`, then `created_at DESC`; `RunQueryService.ListActiveRunDrifts` keeps the single newest in-flight run per schedule, so each schedule contributes at most one drift row to the response. Consumed by e2e tests as an active-run state probe. |
| `ListScheduleTopologies` | Returns one entry per schedule with at least one active `:Table`: `schedule_name`, `node_count`, `last_updated_at = max(:Table.last_updated_at)`. Backs the ui-service homepage `Topology` tab tile grid. |
| `GetNodeAncestry` | Returns a node and its transitive upstream ancestry (outgoing `:DEPENDS_ON`) for a given `:Table` `unique_id`, up to a configurable `max_depth`. Only active nodes are traversed — every node on a returned path is active, so retired tables (kept for run history) and any ancestor reachable only through a retired node are excluded, reflecting the current topology. Each result entry includes per-node provenance (`last_commit_sha`, `last_repo`, `last_changed_at`, `last_release_id`) and `file_path`, ranked by change-recency (`last_changed_at` DESC, nulls last). Returns `NOT_FOUND` for an unknown or inactive node. |
| `GetNode` | Returns per-node topology metadata — `node_type`, `test_count`, `test_count_known` — for a single active `:Table` addressed by `(service_name, schema_name, table_name)`. `test_count_known` is `false` when the node predates `test_count` capture (the property is unset on the `:Table`). Only a known, positive `test_count` is runnable for a `test` operation: an unset count and a known zero both gate (both single-node and whole-DAG test runs enforce this identically). Backs the UI's decision on whether to offer a single-node `test` operation — it offers `test` only when `test_count_known` and `test_count > 0`. Returns `NOT_FOUND` for an unknown or inactive node. Not cached — it is a single-row lookup and `test_count` can change on the next release promotion. |
| `GetNodeVersions` | Returns a node's recorded code-version history, newest first (ordered by `promoted_at`). `limit` <= 0 defaults to 20, clamped to a max of 200. Returns `NOT_FOUND` for an unknown `unique_id`; a known node with no recorded history returns an empty `versions` list. |
| `GetNodeVersionDiff` | Renders a server-side diff between two named versions of one node, addressed by `from_seq`/`to_seq` (`version_seq` — a stable per-node addressing handle, not a chronological ordering; neither is required to be the newer of the pair). Returns `NOT_FOUND` for an unknown `unique_id`, or for a `from_seq`/`to_seq` that node has no recorded version for. |
| `GetUpstreamChanges` | Returns the node's transitive ancestors' most recent code change, most-recently-changed first. Capped at the 5 most-recently-changed ancestors, and each diff independently capped at 8 KiB with `truncated` set on the diffs that were cut — both caps are contract (prompt-size stability for consumers), not tuning. `depth` <= 0 defaults to 3 hops server-side; any value above 10 is rejected as `INVALID_ARGUMENT`. `since`, an optional RFC3339 timestamp, excludes ancestors whose newest version predates it. Returns `NOT_FOUND` for an unknown `unique_id`; a node with no changed ancestors (or none surviving the `since` filter) returns an empty `changes` list. |
| `GetCodeUnitVersions` | Returns a shared-code unit's version chain, newest first. Exactly one of `unit_id`/`unique_id` must be set: `unit_id` queries one unit's chain directly; `unique_id` resolves the node's current units first (via its `:CURRENT` `:NodeVersion`'s `:USES_CODE` edges) and returns each of their chains concatenated in resolution order. `limit` <= 0 defaults to 20, clamped to a max of 200. Returns `NOT_FOUND` for an unknown `unit_id`/`unique_id`; a known unit with no recorded history returns an empty `versions` list. |
| `GetNodeRunHistory` | Returns runs that executed the node, newest first (ordered by `:Run.created_at`). Status and `content_hash` come from the `:EXECUTES` edge — the code that specific run executed; the rest comes from the `:Run` node. `limit` <= 0 defaults to 20, clamped to a max of 200. Returns `NOT_FOUND` for an unknown `unique_id`; a known node with no runs yet returns an empty `runs` list. |

### Code-version read paths

`GetNodeVersions`, `GetNodeVersionDiff`, `GetUpstreamChanges`, `GetCodeUnitVersions`, and `GetNodeRunHistory` (`orchestrator/service/queries/code_version_query_service.go`, backed by `orchestrator/adapters/neo4j/code_version_query_repository.go`) read the code-version graph described above. A few contract points hold across all five:

- **Chain walk starts at `:CURRENT`, but only to compute `is_current`.** The version rows themselves are matched by `unique_id` directly against `:NodeVersion` (the `node_version_uid` index backs this lookup), not by following `:CURRENT`/`:PREVIOUS` from the `:Table`. So a retired node — its `:Table` deleted by topology-swap orphan cleanup, its versions still present — still returns its full history; every row just carries `is_current = false` since there is no `:Table` to point a `:CURRENT` edge from.
- **Ordering is by `promoted_at`, never `version_seq` or `:PREVIOUS`.** `promoted_at` is when the release that introduced the code was promoted, which is chronologically true regardless of ingestion order. `version_seq` is assigned `max+1` at ingestion time, so a late-arriving older release gets the *highest* seq — it is a stable per-node addressing handle (what `GetNodeVersionDiff`'s `from_seq`/`to_seq` address), not a chronological ordering. `:PREVIOUS` is written only for a version its writing release actually created, so a late older release has no `:PREVIOUS` link at all and cannot be used to order or enumerate the chain.
- **The 5-ancestor and 8 KiB-per-diff caps on `GetUpstreamChanges` are contract, not tuning.** remediation's prompt builder inherits them from the path this replaced, so consumers size prompts against both caps together, not just the top-level result count.
- **Absence of history is a degraded answer, not an error.** A known node/unit with no recorded versions, an unknown-ancestor-changes case, or a node with no runs yet all return an empty list with an OK status. Only an *unknown* `unique_id`/`unit_id` — no `:Table` and no matching version at all — is `NOT_FOUND`.
- **List limits clamp uniformly**: `limit` <= 0 defaults to 20; any value above 200 is clamped to 200 (`GetNodeVersions`, `GetCodeUnitVersions`, `GetNodeRunHistory`).
- **`GetUpstreamChanges`'s `depth` is server-validated, not clamped**: <= 0 defaults to 3 hops; a value above 10 is rejected with `INVALID_ARGUMENT` rather than silently capped, so a caller that requests too much finds out rather than getting a quietly smaller answer.

### HTTP (port 8087)

- `GET /health` — liveness probe; returns 200 while the process can serve HTTP.
- `GET /ready` — readiness probe backed by a liveness registry. Returns 200 only
  when every registered background worker (each Redis stream consumer plus the
  outbox processor) is live and the cached dependency probes pass —
  Redis/Postgres (5s TTL); otherwise 503. A consumer goroutine that returns a
  non-nil error (a genuine exit, distinct from the clean `nil` return on
  context cancel) marks itself unhealthy in the registry, flipping `/ready` to
  503 so Kubernetes stops routing to the degraded pod and restarts it.

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
| `query.model:v1` | One message per newly-ready downstream node after a SUCCEEDED node is processed; for rerun/rebase only the rebased rows on the **dispatch frontier** get an initial `query.model:v1` (a rebased row whose upstream is itself rebased waits until that upstream completes; inherited rows are already SUCCEEDED at dispatch and never enter the executor pipeline). Carries `operation` (`""`, `"test"`, or `"build"`) on every dispatch path: the initial frontier (`Params.Operation`), the downstream unblock path (`Run.Operation`, read back on rehydrate and stamped onto `NodeUnblocked`), and the derived-run frontier (`DerivedRunDispatch.Operation`, resolved from the source run via `SnapshotService.SourceOperation`) — so `executor-controller` resolves `dbt test`/`dbt build` instead of `dbt run`/`seed`/`snapshot` accordingly, on whichever path dispatched the node. |
| `schedules.loaded:v1` | Produced by `ReleasePromotedHandler` after a successful topology swap (schedule names list + service_metadata + topology_generation) |
| `run.entries.dispatched:v1` | Produced by every `Snapshot`-driven handler (`HandleSchedulerStarted`, `DerivedRunHandler` for rerun/rebase, `HandleSingleNodeRun`) after the projection is materialised. Carries all task entries with pre-assigned UUIDs, plus per-task `Status` (defaults `"pending"`, `"succeeded"` for inherited rows) and `InheritedFromTaskID` (empty for non-inherited; root-resolved source `task_id` for inherited). Each `DispatchedTask` stamps `MaxRetries = pkg/events.DefaultTaskMaxRetries (= 2)` so state's `task_tracker.max_retries` matches the k8s retry budget. |
| `run.entries.dispatch_failed:v1` | Produced by `HandleSingleNodeRunHandler` on `snapshot.ErrTargetNotFound` (`reason=target_not_found`) or `snapshot.ErrNoTests` (`reason=no_tests` — single-node `Operation="test"` against a node with no known-positive `test_count`: a known zero or an unset count); by `HandleSchedulerStartedHandler` on `snapshot.ErrNoTests` as well (`reason=no_tests` — a whole-DAG `Operation="test"` run whose schedule has no nodes with a known-positive `test_count`: `LatestFullDAG` filters out every known-zero and unset-count node before dispatch, and an all-gated result surfaces `ErrNoTests`, not an empty-projection error); by `HandleSingleNodeRunHandler`, `DerivedRunHandler` (rerun and rebase), and `HandleSchedulerStartedHandler` on `snapshot.ErrEmptyProjection` (`reason=empty_projection` — a whole-DAG `Operation="run"` schedule with zero active `:Table` nodes, or the equivalent empty-projection case for rerun/rebase; the `test`-mode all-gated case never reaches this branch, since the selector returns `ErrNoTests` first) — all sites emit it through the shared `EmitDispatchFailed` helper; by `DerivedRunHandler` (rerun and rebase) on `snapshot.ErrRerunOfTestUnsupported` (`reason=rerun_of_test_unsupported` — `SourcePinnedDAG`/`RebasePartition` reject a source `:Run.operation == "test"`, since a rerun/rebase projection carries no per-task operation and cannot safely reissue `dbt test`); and by `HandleSchedulerStartedHandler` and `HandleNodeCompletedHandler` when a dispatch-frontier or unblocked node carries an unparseable `node_type` (`reason=invalid_node_type` — a permanent defect, so the run fails fast rather than stalling until the watchdog cancels it). Symmetric counterpart of `run.entries.dispatched:v1`: same `scheduler_tracker` target, opposite outcome. State row-locks the row and finalises via `MarkDispatchTerminal`, emitting `run.finalized:v1`: the benign `no_tests` reason marks status=`skipped`; every other reason marks status=`failed`. |

### S3

| Operation | Detail |
|---|---|
| `GetObject` | Reads the code-bundle document named by `release.promoted:v1`'s `code_bundle_uri` (`code-bundles/<release_id>/bundle.json`), once per promoted release on the version-ingestion consumer group. Orchestrator does not own the bucket and never writes to it. |

Requires `S3_ENDPOINT_URL`, `S3_BUCKET`, and `AWS_DEFAULT_REGION` at start-up (all supplied by the shared ConfigMap under Helm). Startup fails closed on a missing required value, rather than booting an orchestrator that silently records no code history. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are injected by the chart's deployment template for every storage-reading service, independently of the replaceable `services` list, so an upgrade carrying an older values file keeps its credentials. Both are optional: when they are absent the S3 client falls back to the SDK's default credential chain, which is what an IAM-role or workload-identity install relies on. The object read is bounded at 64 MiB before decoding, and an oversized document is treated as a permanent failure.

### No gRPC calls to `state`

Orchestrator no longer calls `state` gRPC for any internal writes. All state mutations flow through the Redis event pipeline.

## Processing Logic

### On `scheduler.started:v1` — HandleSchedulerStarted

1. Parses `scheduler.started:v1` into `SchedulerStartedCmd{ ScheduleID, ScheduleName, Kind, SourceRunID, Operation }`.
2. Creates a `Run` node in Neo4j via `snapshotService.Snapshot(ctx, Params{...})` with selector `LatestFullDAG`, stamping `:Run.kind`, `:Run.operation`, `:Run.source_run_id` (when non-nil), and copying `topology_generation` + `service_metadata` from `:TopologyRoot` (since `source_run_id` is empty for cron/trigger).
3. `SnapshotWriter.WriteRunAndExecutesEdges` creates `EXECUTES` edges (all initially PENDING) with pre-assigned task UUIDs stored on the edge.
4. Produces `run.entries.dispatched:v1` via outbox with: `run_id`, `schedule_name`, `manifest_versions`, full task entry list (each with `task_id`, node coordinates, `node_type`, `service_name`, `Status="pending"`, `InheritedFromTaskID=""`).
5. Dispatches the initial frontier. For `Operation=""` (run) or `Operation="build"`, the frontier is every projection row the `LatestFullDAG` selector marked `ReadyToDispatch` (no in-DAG upstream: seeds first, else roots) — computed by the selector from the DAG edges in one batched read; the handler does **not** issue a second Neo4j query for root/seed classification. Build behaves exactly like run here: no node is dropped for `test_count == 0`, and every dispatched `query.model:v1` carries `operation="build"` so `dbt build --select <node>` runs in place of `dbt run`/`seed`/`snapshot`. For `Operation="test"` the selector already filtered the projection down to nodes with a known, positive `test_count` (known-zero and unset counts excluded) and marked every row `ReadyToDispatch`, so the whole flat set dispatches at once — no frontier ordering, no `NodeUnblocked` follow-up. On the run/build path, the run aggregate dispatches any remaining PENDING rows via `NodeUnblocked` as upstreams complete, cascade-skipping descendants of a `FAILED` node exactly as a plain run does (never triggered for a test run, since its rehydrated nodes carry no upstream/downstream edges — see `:Run.operation` above).

`state` consumes `run.entries.dispatched:v1` to create task rows, set `total_task_count`, and mark the run as initialized.

When `Snapshot(LatestFullDAG)` returns `snapshot.ErrEmptyProjection` (a whole-DAG `Operation="run"` schedule whose topology has zero active `:Table` nodes), the handler emits `run.entries.dispatch_failed:v1` with `reason=empty_projection` and the run finalises as `failed`. The equivalent all-gated case for `Operation="test"` — every node's `test_count` is a known zero or unset, so `LatestFullDAG` filters the projection down to nothing — takes a different path: the selector returns `snapshot.ErrNoTests` instead, the handler emits `reason=no_tests`, and the run finalises as `skipped`, since a schedule with no confirmable tests is a benign outcome, not a failure.

Before writing the dispatched entry, the handler validates the `node_type` of every seed/root dispatch node. An unparseable `node_type` is a permanent topology defect (the node could never be dispatched), so the handler emits `run.entries.dispatch_failed:v1` with `reason=invalid_node_type` instead of the dispatched entry, and the run finalises as `failed`. `HandleNodeCompletedHandler` applies the same fail-fast rule when completing a node unblocks a downstream node whose `node_type` is unparseable.

### On `release.promoted:v1` — ReleasePromotedHandler

Receives the full promoted topology for a release (each node carries its `image_tag` joined by `release-controller`, the top-level `repo`, `commit_sha`, and `promoted_at` provenance fields, and a per-node `changed` flag indicating whether that node's `content_hash` differed from the prior prod). The handler runs in one Postgres UoW transaction:

1. Dedup on `message_processing`, keyed `(message_id, release.promoted:v1)`; a secondary uniqueness key on the upstream outbox entry ID catches a re-XADD under a fresh Redis message ID.
2. `ReleasePromotionRepository.PromoteRelease` performs the atomic Neo4j topology swap in a single Neo4j transaction (retire-then-orphan-cleanup; see "Topology swap pattern" below). Every upserted node has its `content_hash` refreshed from the event, changed or not — the code-version path compares that property against each release's code bundle, so a stale value would read as a permanent gap. Nodes with `changed=true` additionally have `last_commit_sha`, `last_repo`, `last_changed_at`, and `last_release_id` stamped from the release's provenance; unchanged nodes retain their prior values. It short-circuits (`changed=false`) when the `:Meta {key:'current_release'}` singleton already records this `release_id`.
3. If `changed=true`, increment `topology_state.topology_generation`; otherwise read the current value. Either way, write the per-service `service_metadata` and the generation onto `:TopologyRoot` (idempotent MERGE).
4. Always write a `schedules.loaded:v1` outbox entry with the schedule names, `service_metadata`, and `topology_generation`. The `event_id` is a deterministic UUID v5 of `(namespace, release_id)`, so re-emissions from idempotent redeliveries are deduplicated by state's `ScheduleCatalogHandler`.

`release.promoted:v1` has two orchestrator consumer groups. `ReleasePromotedHandler` (group `orchestrator-release-promoted`) performs the topology swap and emits `schedules.loaded:v1`. `ReleasePromotedVersionsHandler` (group `orchestrator-release-promoted-versions`) records code-version history from the release's code bundle (see "Code-version history" below). The two are decoupled — a failure in the version path does not block the topology swap, and the swap transaction never waits on object storage.

Building the seeds a release changed starts here and completes in three hops. `ReleasePromotedHandler` writes `release.seeds.pending:v1` **inside the swap transaction**, listing each changed seed with the `node_type` and `image_tag` this release pinned to it. `state` consumes that, mints the run, and emits `trigger.promoted_seeds:v1`; orchestrator consumes that in turn and projects the run's tasks. The ordering is not incidental: the snapshot writer `MATCH`es `:Table` nodes, so a run projected before the swap committed would attach no `:EXECUTES` edges, and re-reading metadata later would let a subsequent promotion substitute its own image. The seeds are built by an ordinary run, so a failed seed build is retried, recorded, and visible like any other task.

Schedule graph reads and new run snapshots only consider `active=true` `Table` nodes, while historical `Run` graphs remain intact through their `EXECUTES` edges.

### On `release.promoted:v1` (versions) — ReleasePromotedVersionsHandler

Records what code each node runs and how it changed over time, behind the `:Table` topology. Nothing on the run path reads this history, so the group is free to trail the topology swap and retry.

1. Dedup on `message_processing`, scoped by the consumer-group name `orchestrator-release-promoted-versions` so its rows stay independent of the other two groups on the same stream.
2. An empty `code_bundle_uri` — a release promoted before manifest-controller wrote bundles — is logged and acknowledged: no retry can produce a bundle that was never written.
3. `CodeBundleReader.Fetch` reads and decodes the bundle from S3. An absent or unreachable object is a plain error, so the message stays in the pending-entries list and is retried (then quarantined at the consumer's delivery ceiling). A bundle that decodes badly — malformed JSON, an unknown `contract_version`, a node with an empty `content_hash` — is wrapped in `events.ErrPermanent`, so the consumer acknowledges and drops it with a loud log.
4. Each bundle node is turned into a write request: code sanitized through `pkg/sanitize`, `compiled_code` above 256 KiB truncated on a rune boundary and flagged `compiled_truncated` with a warning, the resolved config encoded as canonical `config_json`, and the node's direct shared-code ids expanded into their transitive closure so the recorded edges name every unit whose source folds into the node's `shared_code_hash`.
5. `CodeVersionRepository.WriteVersions` decides what to write **against the graph**: a version is recorded where the bundle's `content_hash` differs from the hash on the node's `:CURRENT` version. The event's `changed` flags never trigger a write; they set the `healed` qualifier only (`healed = bootstrap OR NOT changed`), marking a version whose commit and release stamps are approximate because the release that carried it into the graph is not the one that authored it. Comparing against the graph is what lets any later release converge a graph that missed a write.
6. Bundle nodes with no `:Table` are reported back, and the response separates two cases using `:Meta {key:'current_release'}`. If the graph names a different release and its `updated_at` is OLDER than this promotion, the swap has not landed yet: the handler returns a plain error and retries, which is how this group trails the swap. If `updated_at` is NEWER, the topology has moved past this release and those nodes were retired and deleted in between — no retry can bring them back, so their versions are written unattached (`backfilled: true`, no `:CURRENT`) and the message is acknowledged. They stay reachable by `unique_id` and re-attach if the node returns. Once the graph is at this release, an unmatched node is simply logged as absent from the promoted topology.

Failures leave no dedup row (the Postgres transaction rolls back), so a retry reprocesses the message; the graph writes are idempotent, so the replay is a no-op for everything already recorded.

#### Code-version history — graph model

```
(:Table {content_hash})──[:CURRENT {promoted_at, release_id}]──▶(:NodeVersion)──[:PREVIOUS]──▶(:NodeVersion)…
(:NodeVersion)──[:USES_CODE]──▶(:CodeUnitVersion)
(:CodeUnit {unit_id})──[:CURRENT {promoted_at, release_id}]──▶(:CodeUnitVersion)──[:PREVIOUS]──▶…
```

`:NodeVersion` carries `unique_id`, the four hashes (`content_hash`, `source_hash`, `shared_code_hash`, `config_hash`), `runtime`, `raw_code`, `compiled_code`, `compiled_truncated`, `config_json`, the provenance stamp (`repo`, `commit_sha`, `release_id`, `promoted_at`), a per-node monotonic `version_seq`, and the `healed` / `backfilled` provenance qualifiers. `:CodeUnitVersion` carries `unit_id`, `checksum`, `source`, and the same provenance stamp; its `unit_id` is service-namespaced (`<service>:<unit id>`) exactly as the bundle emits it, so two services' copies of a same-named dbt macro never collide.

Two invariants hold the model together:

- **A version is immutable once written.** Every property, `:PREVIOUS` included, is set under `ON CREATE`. A node that reverts to earlier code re-points `:CURRENT` at the version that already exists rather than rewriting that version's chain — rewriting it would close the chain into a cycle and hang every chain walk. The intermediate version stays reachable by `unique_id`, which the `node_version_uid` index backs.
- **Ordering is decided in the database, never from a pre-write read.** Each node carries a `code_version_promoted_at` watermark; a release advances it for every node it mentions, then guards its pointer and chain writes on it inside the same statement. Three things follow. A release that finds the code unchanged still moves the watermark, so a delayed intermediate release cannot later satisfy a stale guard and drag `:CURRENT` backwards. Two replicas promoting different releases serialise on the watermark's node write lock, so an older promotion can never delete a newer pointer and install a stale one. And a late older release still records its version, but may not link it as superseding the newer code that is current, which would reverse the chain's order.

A retired node's `:Table` is deleted by the topology swap's orphan cleanup, taking the `:CURRENT` edge with it. The version nodes survive as free-standing nodes, and if the node returns to the topology the pointer reattaches to the version that already exists — no duplicate, and `version_seq` unchanged.

Writes are batched (100 nodes per explicit transaction), and each batch reads the graph's state for its nodes, writes the shared-code versions the changed nodes reference, then the node versions, their chain and `:USES_CODE` edges, and finally the pointer moves.

### On `trigger.rerun:v1` — HandleRerun

The rerun entry point materialises a fresh `Snapshot(SourcePinnedDAG)` against a newly-minted `:Run`. `state.RerunHandler.TriggerRerun` (gRPC) writes a `trigger.rerun:v1` outbox row in the same Postgres tx that **inserts a new** `scheduler_tracker` row (`kind='rerun'`, `source_run_id=<src>`); the source row is left untouched at its terminal state. The orchestrator consumes the resulting Redis message and runs `HandleRerun` in one Postgres UoW transaction:

1. Dedup on `message_processing`.
2. Resolves the source run's operation via `snapshotService.SourceOperation(ctx, src)` (reads `:Run.operation` through `TopologyReader.SourceRunOperation`), then builds `Params{ RunID, ScheduleName, Kind: "rerun", SourceRunID: src, Operation: op }` with selector `SourcePinnedDAG{}` — a rerun inherits its source's operation (`""`, `test`, or `build`).
3. `snapshotService.Snapshot(ctx, params)` — `SourcePinnedDAG.SelectTasks` checks `p.Operation`; if it is `"test"`, the selector returns `ErrRerunOfTestUnsupported` and the handler emits `run.entries.dispatch_failed:v1` (`reason=rerun_of_test_unsupported`) instead of continuing — rerunning a test run is not supported, since the projection carries no per-task operation and would silently run `dbt run` in place of `dbt test`; the caller must trigger a fresh `node test` instead. For `p.Operation` `""` or `"build"` the selector proceeds: it reads the source `:Run`'s `:EXECUTES` set via `TopologyReader`, seeds the rebase set with all non-SUCCEEDED source tasks, grows it via `DescendantsInSourceRun`, and classifies the rest as **inherited** (carried forward with the source's stored status and `task_id` resolved to its lineage root via `inherited_from_task_id`). `SnapshotWriter.WriteRunAndExecutesEdges` MERGEs the new `:Run` (inheriting `topology_generation` + `service_metadata` from source `:Run`, NOT from `:TopologyRoot`, and stamping `operation = op`) and writes one `:EXECUTES` edge per projected task, all with the source's pinned `image_tag` + `manifest_version`.
4. orchestrator outbox writes (same tx, via `DispatchDerivedRun` with `Operation: op`):
   - 1× `run.entries.dispatched:v1` with the FULL projection — both rebased (Status="pending") and inherited (Status="succeeded", `InheritedFromTaskID=<root>`) rows. State's `RunEntriesDispatchedHandler` creates `task_tracker` rows honouring per-task status, and may auto-rollup directly to terminal if every task is already terminal.
   - N× `query.model:v1` for the rebase **dispatch frontier** only — rebased rows whose every upstream is inherited-SUCCEEDED. Rebased rows behind another rebased upstream are dispatched later by the run aggregate (`NodeUnblocked`) or cascade-skipped if that upstream re-fails. Inherited rows are already SUCCEEDED and never enter the executor pipeline. Each dispatched entry carries `operation = op`, so a rerun of a build re-dispatches `dbt build` for every rebased task.

The source `:Run` and its `task_tracker` rows are never mutated.

### On `trigger.rebase:v1` — HandleRebase

Entry point for rebase from a terminal `FAILED`/`CANCELLED` run. `state.RebaseHandler.TriggerRebase` (gRPC) writes a `trigger.rebase:v1` outbox row in the same Postgres tx that inserts a new `scheduler_tracker` row (`kind='rebase'`, `source_run_id=<src>`). The handler runs in one Postgres UoW transaction:

1. Dedup on `message_processing`.
2. Resolves the source run's operation via `snapshotService.SourceOperation(ctx, src)` (reads `:Run.operation` through `TopologyReader.SourceRunOperation`), then builds `Params{ RunID: newRunID, ScheduleName, Kind: "rebase", SourceRunID: src, Operation: op }` with selector `RebasePartition{Source: src}` — a rebase inherits its source's operation (`""`, `test`, or `build`).
3. `snapshotService.Snapshot(ctx, params)` — `RebasePartition.SelectTasks` checks `p.Operation`; if it is `"test"`, the selector returns `ErrRerunOfTestUnsupported` and the handler emits `run.entries.dispatch_failed:v1` (`reason=rerun_of_test_unsupported`) instead of continuing — same rationale as `SourcePinnedDAG`. For `p.Operation` `""` or `"build"` the selector proceeds: it reads the source `:Run`'s `:EXECUTES` set + the latest `:Table`s via `TopologyReader` and computes:
   - **rebase_set**: every source task with status ≠ SUCCEEDED, plus their descendants in the latest topology, plus new arrivals (nodes in latest not in source).
   - **inherit_set**: SUCCEEDED tasks in source that still exist in latest, minus rebase_set.
   - **drop_set**: tasks in source absent from latest (silently dropped).
   Rebased rows project as PENDING with **latest** `image_tag` + `manifest_version`. Inherited rows project as SUCCEEDED with the **source's** pinned pair plus a root-resolved `inherited_from_task_id` (the projector resolves transitively, so chain depth stays ≤ 1 even for rebase-of-rebase). `SnapshotWriter.WriteRunAndExecutesEdges` MERGEs the new `:Run` (inheriting `topology_generation` + `service_metadata` from source `:Run`, and stamping `operation = op`) and writes the projected `:EXECUTES` edges.
4. orchestrator outbox writes (same tx, via `DispatchDerivedRun` with `Operation: op`):
   - 1× `run.entries.dispatched:v1` with the FULL projection — rebased (`Status="pending"`) + inherited (`Status="succeeded"`, `InheritedFromTaskID=<root>`).
   - N× `query.model:v1` for the rebase **dispatch frontier** only (rebased rows whose every upstream is inherited-SUCCEEDED; the rest follow via `NodeUnblocked`/cascade-skip), each carrying `operation = op` so a rebase of a build re-dispatches `dbt build`.

If `rebase_set ∩ inherit_set` is empty (selector returns zero entries — should never happen given upstream eligibility checks but guarded defensively), the handler still emits `run.entries.dispatched:v1` with an empty list and lets state's auto-rollup terminate the run.

### Shared helpers

Both consumer handlers (`HandleRerun`, `HandleRebase`) delegate the projection-to-outbox pipeline to `service/handlers/dispatch_derived_run.go`. The helper takes the materialised projection and emits the run-level `run.entries.dispatched:v1` outbox entry plus one `query.model:v1` entry per PENDING row. Inherited terminal rows (`FAILED` / `CANCELLED` / `SKIPPED`) round-trip their status verbatim — coercing them to `pending` would create `task_tracker` rows the executor never runs. `EmitDispatchFailed` in `service/handlers/dispatch_failed.go` writes a `run.entries.dispatch_failed:v1` outbox entry when a selector returns `ErrEmptyProjection` or (for rerun/rebase) `ErrRerunOfTestUnsupported`.

### On `trigger.single_node_run:v1` — HandleSingleNodeRunHandler

Entry point for a single-node ad-hoc run. The handler runs in one Postgres UoW transaction:

1. Dedup on `message_processing`.
2. `snapshotService.Snapshot(ctx, Params{..., Operation})` with selector `SingleNode{Target, MetadataSource, SourceRunID?}` — creates a `:Run` node and exactly one `EXECUTES` edge with a pre-assigned task UUID.
   - **latest mode** (`metadata_source=latest`): selector reads metadata from `:TopologyRoot` + the current `:Table` node; new `:Run` inherits topology fields from `:TopologyRoot`.
   - **stale mode** (`metadata_source=snapshot_of_run`): selector reads metadata from the source `:Run`'s `EXECUTES` edge for the same node (preserving the original run's `image_tag` and `manifest_version`); new `:Run` inherits `topology_generation` + `service_metadata` from the source `:Run` instead of `:TopologyRoot`.
3. On `ErrTargetNotFound` (node absent in Neo4j) or `ErrNoTests` (`Operation="test"` and the resolved node has no known-positive `test_count` — a known zero or an unset count, so no confirmable test to run): orchestrator outbox writes `run.entries.dispatch_failed:v1` for the synthesised run — `reason=target_not_found` or `reason=no_tests` respectively, no further dispatch. State's `RunEntriesDispatchFailedConsumer` row-locks the synthesised `scheduler_tracker` and finalises it via `MarkDispatchTerminal`, writing `run.finalized:v1`: `reason=no_tests` marks it `skipped` (a benign non-failure), `reason=target_not_found` marks it `failed`. Idempotent on already-terminal rows.
4. On success, orchestrator outbox writes (same tx):
   - 1× `run.entries.dispatched:v1` with the single task entry — consumed by `state` to create the `task_tracker` row, set `total_task_count=1`, and mark `init_status=completed, status=RUNNING`.
   - 1× `query.model:v1` (carrying `operation`) for the single target node — consumed by `executor-controller` to launch the K8s job, running `dbt test --select <node>` when `operation="test"` or `dbt build --select <node>` when `operation="build"`, in place of `dbt run`/`seed`/`snapshot`.

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

`runs.Save` writes `:Run.terminal_status` / `:Run.completed_at` when the aggregate finalises internally, normalizing the uppercase in-memory aggregate status to the canonical lowercase stored form at the write boundary so every writer agrees. A separate Redis consumer on `run.finalized:v1` projects state's authoritative terminal outcome — succeeded, failed, cancelled, or skipped — onto the same fields whenever a run's terminal transition is not produced by the aggregate. This covers full-inherited rebases that never publish `node.updated:v1` events and cancelled runs, which emit `run.finalized:v1` (status `cancelled`) from state's `Run.Cancel()` rather than through the task-completion path. State remains the source of truth for run terminality; orchestrator's role on this stream is read-only persistence.

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer (`scheduler.started:v1`) | Reads and dispatches to HandleSchedulerStarted handler |
| Redis consumer (`node.updated:v1`) | Reads and dispatches to HandleNodeCompleted handler |
| Redis consumer (`release.promoted:v1`) | Reads and dispatches to ReleasePromotedHandler |
| Redis consumer (`trigger.rerun:v1`) | Reads and dispatches to HandleRerun handler |
| Redis consumer (`trigger.rebase:v1`) | Reads and dispatches to HandleRebase handler |
| Redis consumer (`trigger.single_node_run:v1`) | Reads and dispatches to HandleSingleNodeRunHandler |
| Redis consumer (`release.promoted:v1` versions) | Reads `release.promoted:v1` on consumer group `orchestrator-release-promoted-versions`; dispatches to `ReleasePromotedVersionsHandler` |
| Redis consumer (`trigger.promoted_seeds:v1`) | Reads `trigger.promoted_seeds:v1` on consumer group `orchestrator-promoted-seeds`; dispatches to `HandlePromotedSeedsRunHandler` |
| Redis consumer (`run.finalized:v1`) | Projects state's terminal outcome (succeeded, failed, cancelled, or skipped) onto Neo4j `:Run.completed_at` / `terminal_status`. Covers runs that produce no `node.updated:v1` traffic (full-inherited rebases, cancelled runs). |
| Outbox processor (`pkg/outbox.Processor`) | Polls `orchestrator_outbox` for pending entries; publishes each row to its `stream_name` via `orchestrator/adapters/publisher.OutboxPublisher` |
| RunSweeper | Periodically deletes expired `Run` nodes (and their `EXECUTES` edges) older than `retention_days` |

## gRPC Callers

| Service | Methods used |
|---|---|
| `ui-service` | `GetScheduleGraph`, `ListRuns`, `GetRunGraph`, `ListScheduleTopologies`, `GetNode` |
| `continuo CLI` | `GetScheduleGraph`, `GetNodeVersions`, `GetNodeVersionDiff`, `GetUpstreamChanges`, `GetCodeUnitVersions`, `GetNodeRunHistory` |

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
- `ListActiveRunDrifts()` — surfaces the single newest in-flight run per schedule plus the latest `topology_state.topology_generation`. "In-flight" means `completed_at IS NULL`; `run.finalized:v1` stamps `completed_at` for all terminal outcomes (succeeded, failed, cancelled, skipped), so a cancelled run leaves the active set once the projection arrives. The underlying `ListActiveRuns` query (Neo4j adapter) orders by `schedule_name`, then `created_at DESC`; `RunQueryService` keeps the head row per schedule. Used by e2e tests as a probe for orchestrator-side active-run state.

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
2. **Upsert** — `MERGE (existing:Table {unique_id: ...}) SET ..., active = true, retired_at = NULL`. Reactivates any node that's back in the new topology. For each node whose `changed` flag is `true`, the provenance properties `last_commit_sha`, `last_repo`, `last_changed_at`, and `last_release_id` are also stamped via a `FOREACH` guard; unchanged nodes retain their prior provenance values (null until first change for nodes predating this feature).
3. **Clear outgoing edges** on upserted nodes so dependencies can be rebuilt: `MATCH (a:Table {unique_id: ...})-[r:DEPENDS_ON]->() DELETE r`.
4. **Rebuild `:DEPENDS_ON`** between nodes in the new topology. References to `unique_id`s outside the candidate set are silently skipped (matches dbt's compile semantics for cross-service dependencies).
5. **Orphan cleanup** — `MATCH (t:Table) WHERE COALESCE(t.active, true) = false AND NOT EXISTS { (:Run)-[:EXECUTES]->(t) } DETACH DELETE t`. Removes only the `:Table` nodes no run still references.

Before any of these steps, the transaction reads the `:Meta {key:'current_release'}` singleton; if its `release_id` already matches the incoming release it commits empty and returns `changed=false` (idempotent redelivery). After the swap it MERGEs the `:Meta` singleton with the new `release_id`. Each upserted `:Table` node is stamped with `release_id`.

Reference impl: `orchestrator/adapters/neo4j/release_promotion_repository.go` (`PromoteRelease`).

**Design constraint for any topology-write path:** any handler that replaces `:Table` topology in this orchestrator MUST follow the same pattern. Truncate-and-load is not safe in this graph because it destroys `:Run-[:EXECUTES]->:Table` run history.

When the Neo4j Go driver binds `time.Time` parameters into Cypher, it uses `Location().String()` as a timezone identifier and rejects `"Local"`. Always pass `now.UTC()` at the boundary.

## Reliability Patterns

- **Inbound dedup**: `message_processing` keyed by `(message_id, stream_name)`; INSERT IF NOT EXISTS prevents double-processing. The composite key is required because Redis Streams assign IDs per-stream, so a single publisher can emit two messages to two streams in the same millisecond and produce identical message IDs that must not collide. When multiple consumer groups read the same stream, each group scopes its dedup row by its consumer-group name rather than the stream name, so the two groups' rows remain distinct. Single-consumer streams continue to use the stream name.
- **Neo4j updates are outside the Postgres tx**: topology and status writes are idempotent; if the tx fails the message will be redelivered
- **Snapshot reconciliation**: the promoted topology on `release.promoted:v1` is treated as authoritative; nodes missing from it are retired from the current topology automatically (and deleted once no `:Run` references them)
- **Pre-assigned task UUIDs**: task IDs are committed to Neo4j EXECUTES edges before `run.entries.dispatched:v1` is produced; the outbox processor reads them at publish time, ensuring consistent IDs across retries
- **No state gRPC dependency**: orchestrator is fully decoupled from the state write path; all state mutations go through events
- **RunSweeper**: deletion is best-effort; a sweep failure is logged and retried on the next tick
