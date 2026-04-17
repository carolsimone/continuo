# orchestrator

## Purpose

`orchestrator` is the merged replacement for the former `graph` and `dependency-controller` services. It owns the dependency topology in Neo4j, handles run initialization and node completion events, and serves gRPC queries for the UI.

It is responsible for:
- ingesting topology from manifest-controller (via `manifest.loaded:v1`)
- initializing run snapshots (via `initialize.run:v1`)
- processing node completion events, unlocking downstream nodes, and finalizing schedule runs (via `node.updated:v1`)
- serving read queries for schedule graphs, run listings, and run graphs (gRPC `OrchestratorQuery` service)

## Owned Storage

### Neo4j

| Entity | Description |
|---|---|
| `Table` node | One per model/seed; carries topology metadata and `last_updated_at` |
| `Run` node | One per schedule run; carries `terminal_status`, `created_at`, `completed_at` |
| `DEPENDS_ON` relationship | Directed edge from downstream to upstream `Table` |
| `EXECUTES` relationship | Directed edge from `Run` to `Table`; carries per-run `status` |

The `run.Repository` interface exposes the following Neo4j read methods used during rerun handling:
- `GetNodeType(ctx, schema, tableName) (string, error)` — reads `node_type` from the current `Table` node
- `GetNodeServiceName(ctx, schema, tableName) (string, error)` — reads `service_name` from the current `Table` node

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
| `node.updated:v1` | `orchestrator_node_updated` | `HandleNodeCompleted` — updates EXECUTES status, unlocks downstream, finalizes runs |
| `manifest.loaded:v1` | `orchestrator_manifest_loaded` | `IngestTopology` — upserts Table nodes and DEPENDS_ON edges |
| `initialize.run:v1` | `orchestrator_initialize_run` | `InitializeRun` — creates Run node and EXECUTES edges for normal startup; `HandleRerun` — resets target/downstream nodes and produces `rerun.ready:v1` when a rerun target is present |

### gRPC server — `OrchestratorQuery` (port 50052)

| Method | Description |
|---|---|
| `GetScheduleGraph` | Returns all Table nodes and DEPENDS_ON edges for a schedule |
| `ListRuns` | Returns Run nodes for a schedule, newest first |
| `GetRunGraph` | Returns nodes and EXECUTES edges for a specific run, with per-node status |

### HTTP (port 8087)

- `GET /health` — liveness probe only

## Outbound Interfaces

### Redis producer (via outbox)

| Stream | Trigger |
|---|---|
| `query.model:v1` | One message per newly-ready downstream node after a SUCCEEDED node is processed |
| `schedules.loaded:v1` | Produced by IngestTopology after successful topology load (schedule names list) |
| `run.initialized:v1` | Produced by InitializeRun after run snapshot is created (run_id, schedule info, node lists) |
| `rerun.ready:v1` | Produced by HandleRerun when handling a rerun target (rerun scope + target info; `service_name` comes from the current graph for K8s dispatch) |

### gRPC to `state`

| Method | When called |
|---|---|
| `GetTaskByScheduleAndNode` | For each ready downstream node, to retrieve pre-registered task ID |
| `GetSchedulerInitStatus` | Before finalization, to guard against premature terminal write during rerun setup |
| `UpdateScheduler` | To write SUCCEEDED or FAILED on the scheduler run when the graph drains |

## Processing Logic

### On `manifest.loaded:v1` — IngestTopology

Receives a JSON payload of topology nodes. For each node, upserts the `Table` node and rewrites `DEPENDS_ON` edges in Neo4j. Deduplicates by Redis message ID via `message_processing` table.

### On `initialize.run:v1` — InitializeRun

Creates a `Run` node and `EXECUTES` edges (all initially PENDING) for a schedule run. If the message contains rerun target fields, delegates to the `HandleRerun` handler (see below). Otherwise produces `run.initialized:v1` with root/seed node lists for `startup-controller` to dispatch.

### On `initialize.run:v1` (rerun path) — HandleRerun

When `initialize.run:v1` carries a `rerun_target`, the `HandleRerun` handler takes over:

1. Gets transitive downstream nodes from Neo4j via `GetTransitiveDownstream`.
2. Resets the target node and any FAILED downstream nodes to PENDING in Neo4j via `UpdateNodeStatus`.
3. Reads `node_type` for the target from Neo4j via `GetNodeType` — uses the current graph value (reflects any fixes applied since the original run).
4. Reads `service_name` for the target from Neo4j via `GetNodeServiceName` — uses the current graph value for K8s image dispatch.
5. Builds the `rerun.ready:v1` payload with a `target_nodes` list. For the rerun target node:
   - `service_name` = current graph value (used for both K8s job dispatch and task lookup in `state`)
   - `schedule_name` = schedule name from the rerun command
   - FAILED downstream nodes carry their current graph `service_name`.
6. Writes the payload to the `rerun.ready:v1` outbox entry.

### On `node.updated:v1` — HandleNodeCompleted

```
1. Dedup check: insert into message_processing (INSERT IF NOT EXISTS)
   -> if already completed/acked: skip and return

2. Open Postgres transaction
   a. UpdateNodeStatus in Neo4j (outside tx - idempotent)
   b. If status == SUCCEEDED:
      - GetReadyDownstream from Neo4j
      - For each ready node:
          - GetTaskByScheduleAndNode from state
          - ComputeJobName
          - Write outbox entry
      - Update message_processing state -> completed
   c. Commit transaction

3. After commit, if status is terminal (SUCCEEDED or FAILED):
   a. GetSchedulerInitStatus from state
      -> if not "completed": skip finalization (rerun guard)
   b. CheckScheduleCompletion from Neo4j
      -> if not complete: return
   c. UpdateScheduler in state -> SUCCEEDED or FAILED
   d. FinalizeRun in Neo4j -> stamp Run node with terminal status and timestamp
```

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer (`node.updated:v1`) | Reads and dispatches to HandleNodeCompleted handler |
| Redis consumer (`manifest.loaded:v1`) | Reads and dispatches to IngestTopology handler |
| Redis consumer (`initialize.run:v1`) | Reads and dispatches to InitializeRun handler |
| Outbox processor | Polls outbox for pending entries; publishes to `query.model:v1`; records in `published_messages` |
| RunSweeper | Periodically deletes expired `Run` nodes (and their `EXECUTES` edges) older than `retention_days` |

## gRPC Callers

| Service | Methods used |
|---|---|
| `ui-service` | `GetScheduleGraph`, `ListRuns`, `GetRunGraph` |

Orchestrator calls no external gRPC services except `state`.

## Reliability Patterns

- **Inbound dedup**: `message_processing` keyed by Redis message ID; INSERT IF NOT EXISTS prevents double-processing
- **Outbound idempotency**: `published_messages` tracks published outbox entries; republishing is safe
- **Neo4j updates are outside the Postgres tx**: topology and status writes are idempotent; if the tx fails the message will be redelivered
- **Finalization is post-commit and best-effort**: if `checkAndFinalizeSchedule` fails, the error is logged but the message is still ACKed
- **Rerun guard**: finalization is skipped if `initialization_status != "completed"` to prevent overwriting RUNNING with FAILED while a rerun is in progress
- **RunSweeper**: deletion is best-effort; a sweep failure is logged and retried on the next tick
