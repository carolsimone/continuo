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
| `Run` node | One per schedule run; carries `terminal_status`, `created_at`, `completed_at` |
| `DEPENDS_ON` relationship | Directed edge from downstream to upstream `Table` |
| `EXECUTES` relationship | Directed edge from `Run` to `Table`; carries per-run `status` and pre-assigned `task_id` UUID |

Task UUIDs are pre-assigned when the `EXECUTES` edges are created during run snapshot initialization. This allows `run.entries.dispatched:v1` to carry canonical task IDs without a round-trip to `state`.

The `run.Repository` interface exposes the following Neo4j read methods used during rerun handling:
- `GetNodeType(ctx, schemaName, tableName) (string, error)` — reads `node_type` from the current `Table` node (queries by `schema_name` property)
- `GetNodeServiceName(ctx, schemaName, tableName) (string, error)` — reads `service_name` from the current `Table` node (queries by `schema_name` property)

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
| `scheduler.started:v1` | `orchestrator_scheduler_started` | `HandleSchedulerStarted` — initializes run snapshot, creates EXECUTES edges with pre-assigned task UUIDs, produces `run.entries.dispatched:v1` |
| `node.updated:v1` | `orchestrator_node_updated` | `HandleNodeCompleted` — updates EXECUTES status, unlocks downstream nodes |
| `manifest.loaded:v1` | `orchestrator_manifest_loaded` | `IngestTopology` — applies the full manifest snapshot, rewrites `DEPENDS_ON`, retires missing `Table` nodes, then emits `schedules.loaded:v1` |
| `initialize.run:v1` | `orchestrator_initialize_run` | `HandleRerun` — resets target/downstream nodes and produces `run.rerun.dispatched:v1` when a rerun target is present |

### gRPC server — `OrchestratorQuery` (port 50052)

| Method | Description |
|---|---|
| `GetScheduleGraph` | Returns all Table nodes and DEPENDS_ON edges for a schedule |
| `ListRuns` | Returns Run nodes for a schedule, newest first |
| `GetRunGraph` | Returns nodes and EXECUTES edges for a specific run, with per-node status. Also returns `run_topology_generation` (stamped on the `:Run` node at SnapshotGraph time; `0` means "drift unknown" — not "no drift") and `latest_topology_generation` (current `topology_state.topology_generation` Postgres singleton). |
| `ListActiveRunDrifts` | Returns one `ActiveRunDrift` row per `is_running=true` schedule (`schedule_name`, `run_id`, `run_topology_generation`) plus the orchestrator's current `latest_topology_generation`. Drives the dashboard's per-schedule active-run drift indicator without forcing the UI to call `GetRunGraph` for every active schedule. |

### HTTP (port 8087)

- `GET /health` — liveness probe only

## Outbound Interfaces

### Redis producer (via outbox)

| Stream | Trigger |
|---|---|
| `query.model:v1` | One message per newly-ready downstream node after a SUCCEEDED node is processed |
| `schedules.loaded:v1` | Produced by IngestTopology after successful topology load (schedule names list) |
| `run.entries.dispatched:v1` | Produced by HandleSchedulerStarted after run snapshot is created; carries all task entries with pre-assigned UUIDs, root/seed node lists |
| `run.rerun.dispatched:v1` | Produced by HandleRerun; carries the rerun scope and target task IDs for state to reset |

### No gRPC calls to `state`

Orchestrator no longer calls `state` gRPC for any internal writes. All state mutations flow through the Redis event pipeline.

## Processing Logic

### On `scheduler.started:v1` — HandleSchedulerStarted

1. Creates a `Run` node in Neo4j.
2. Creates `EXECUTES` edges (all initially PENDING) with pre-assigned task UUIDs stored on the edge.
3. Identifies root and seed nodes.
4. Produces `run.entries.dispatched:v1` via outbox with: `run_id`, `schedule_name`, `manifest_versions`, full task entry list (each with `task_id`, node coordinates, `node_type`, `service_name`).

`state` consumes `run.entries.dispatched:v1` to create task rows, set `total_task_count`, and mark the run as initialized.

### On `manifest.loaded:v1` — IngestTopology

Receives a JSON payload of topology nodes representing the full current manifest snapshot. Within one Neo4j write transaction it:

1. upserts every current `Table` node and rewrites its outgoing `DEPENDS_ON` edges
2. marks any previously-active `Table` node missing from the payload as `active=false`
3. deletes inactive `Table` nodes only when they are no longer referenced by any `Run` snapshot

Schedule graph reads and new run snapshots only consider `active=true` `Table` nodes, while historical `Run` graphs remain intact through their `EXECUTES` edges. Deduplication still keys off the Redis message ID via `message_processing`.

### On `initialize.run:v1` (rerun path) — HandleRerun

When `initialize.run:v1` carries a `rerun_target`:

1. Gets transitive downstream nodes from Neo4j via `GetTransitiveDownstream`.
2. Resets the target node and any FAILED downstream nodes to PENDING in Neo4j via `UpdateNodeStatus`.
3. Reads `node_type` for the target from Neo4j via `GetNodeType`.
4. Reads `service_name` for the target from Neo4j via `GetNodeServiceName`.
5. Builds the `run.rerun.dispatched:v1` payload with a `target_nodes` list. For the rerun target node:
   - `service_name` = current graph value
   - `schedule_name` = schedule name from the rerun command
   - FAILED downstream nodes carry their current graph `service_name`.
6. Writes the payload to the `run.rerun.dispatched:v1` outbox entry.

`state` consumes `run.rerun.dispatched:v1` to reset the target task(s) to PENDING.

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
| Redis consumer (`initialize.run:v1`) | Reads and dispatches to HandleRerun handler |
| Outbox processor | Polls outbox for pending entries; publishes to `query.model:v1`, `run.entries.dispatched:v1`, `run.rerun.dispatched:v1`; records in `published_messages` |
| RunSweeper | Periodically deletes expired `Run` nodes (and their `EXECUTES` edges) older than `retention_days` |

## gRPC Callers

| Service | Methods used |
|---|---|
| `ui-service` | `GetScheduleGraph`, `ListRuns`, `GetRunGraph`, `ListActiveRunDrifts` |
| `continuo CLI` | `GetScheduleGraph` |

Orchestrator calls no external gRPC services.

## Reliability Patterns

- **Inbound dedup**: `message_processing` keyed by Redis message ID; INSERT IF NOT EXISTS prevents double-processing
- **Outbound idempotency**: `published_messages` tracks published outbox entries; republishing is safe
- **Neo4j updates are outside the Postgres tx**: topology and status writes are idempotent; if the tx fails the message will be redelivered
- **Snapshot reconciliation**: `manifest.loaded:v1` is treated as authoritative; nodes missing from the latest payload are retired from the current topology automatically
- **Pre-assigned task UUIDs**: task IDs are committed to Neo4j EXECUTES edges before `run.entries.dispatched:v1` is produced; the outbox processor reads them at publish time, ensuring consistent IDs across retries
- **No state gRPC dependency**: orchestrator is fully decoupled from the state write path; all state mutations go through events
- **RunSweeper**: deletion is best-effort; a sweep failure is logged and retried on the next tick
