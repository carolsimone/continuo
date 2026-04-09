# graph

## Purpose

`graph` owns the dependency topology and per-run execution projection in Neo4j.

It answers:
- what depends on what (topology)
- which nodes are roots or seed-roots for a run (init)
- which downstream nodes are ready to execute (runtime)
- whether a schedule run is complete (runtime)
- what the run graph looked like historically (UI / history)
- which upstream nodes are stale or not yet fresh (freshness checks)

## Owned Storage

Neo4j only. No Postgres, no Redis.

| Entity | Description |
|---|---|
| `Table` node | One per model/seed; carries topology metadata and `last_updated_at` |
| `Run` node | One per schedule run; carries `terminal_status`, `created_at`, `completed_at` |
| `DEPENDS_ON` relationship | Directed edge from downstream → upstream `Table` |
| `EXECUTES` relationship | Directed edge from `Run` → `Table`; carries per-run `status` |

`Table` node fields include: `table_name`, `schema_name`, `service_name`, `owner`, `schedule_name`, `node_type`, `criticality`, `manifest_version`, `last_updated_at`, `created_at`.

Criticality enum: `REGULATORY`, `CORE`, `SECONDARY`.

## Inbound Interfaces

### gRPC — `GraphService` (port 50052)

#### Topology writes

| Method | Description |
|---|---|
| `CreateNode` | Upsert a `Table` node and rewrite its `DEPENDS_ON` edges; creates upstream placeholder nodes via `MERGE` if they don't exist |
| `UpdateNodeTimestamp` | Set `Table.last_updated_at = now()` after a successful execution |

#### Freshness / staleness queries (used by manifest-controller / external)

| Method | Description |
|---|---|
| `GetStaleRootNodes` | Returns root `Table` nodes not updated within `hours_threshold` hours |
| `GetDownstreamDependencies` | Returns downstream deps of a node up to `max_depth`; includes depth per dependency |
| `CheckUpstreamFreshness` | Returns whether all upstreams of a node were updated within `freshness_minutes` (default 30); lists stale upstreams |

#### Run snapshot and lifecycle

| Method | Description |
|---|---|
| `SnapshotGraph` | Creates a `Run` node and `EXECUTES` edges (all initially `PENDING`) for a schedule run |
| `UpdateNodeStatus` | Writes `EXECUTES.status` for a specific `(run_id, node)` |
| `FinalizeRun` | Sets `Run.completed_at` and `Run.terminal_status` |

#### Startup support

| Method | Description |
|---|---|
| `GetScheduleInitNodes` | Returns root and seed-root nodes for the initialization phase of a run |
| `GetTransitiveDownstream` | Returns all reachable downstream nodes (non-SUCCEEDED) from a given node; used to reset downstream on rerun |

#### Runtime dependency support

| Method | Description |
|---|---|
| `GetReadyDownstream` | Returns downstream nodes whose all upstreams are SUCCEEDED — i.e. ready to execute |
| `CheckScheduleCompletion` | Returns whether all `EXECUTES` edges for a run are in a terminal status |

#### History and UI support

| Method | Description |
|---|---|
| `GetScheduleGraph` | Returns all `Table` nodes and `DEPENDS_ON` edges for a schedule, including cross-service upstream placeholders |
| `ListRuns` | Returns `Run` nodes for a schedule, newest first |
| `GetRunGraph` | Returns nodes and `EXECUTES` edges for a specific run, with per-node status |

> **Note on `Status` field in `TableNode`**: populated only by `GetTransitiveDownstream` and `GetRunGraph`. Empty string for all other RPCs.

### HTTP (port 8081)

- `GET /health` — liveness probe only

## Outbound Interfaces

None. Graph does not call any other service or publish to Redis.

## Background Loops

| Loop | Description |
|---|---|
| `RunSweeper` | Periodically calls `DeleteExpiredRuns` on Neo4j; deletes `Run` nodes (and their `EXECUTES` edges) older than `retention_days`; interval configurable |

## gRPC Callers

| Service | Methods used |
|---|---|
| `startup-controller` | `SnapshotGraph`, `GetScheduleInitNodes`, `GetTransitiveDownstream`, `UpdateNodeStatus` |
| `dependency-controller` | `GetReadyDownstream`, `CheckScheduleCompletion`, `UpdateNodeStatus`, `FinalizeRun` |
| `manifest-controller` | `CreateNode` |
| `ui-service` | `GetScheduleGraph`, `ListRuns`, `GetRunGraph` |

Graph calls no external gRPC services.

## Reliability Notes

- Graph is not the task-state authority — `state` owns task and scheduler status.
- `EXECUTES.status` is a projection pushed in by callers (`startup-controller`, `dependency-controller`). If a status update is missed, the run projection will be stale.
- `GetReadyDownstream` and `CheckScheduleCompletion` read from `EXECUTES.status`, so correctness of these queries depends on callers keeping the projection up to date.
- `RunSweeper` deletion is best-effort; a sweep failure is logged and retried on the next tick.
