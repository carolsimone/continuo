# ui-service

## Purpose

`ui-service` is an HTTP facade and React frontend for operators.

It provides:
- a real-time dashboard of all schedules and their last-run status
- a detail view per schedule run: DAG topology, node statuses, task list, execution history
- a per-node detail page: recent run history for a single dbt node, with trigger controls
- S3 log proxying: fetches pod logs from S3 and streams them to the browser
- rerun triggering: proxies `POST /api/schedulers/:id/rerun` to the `TriggerRerun` gRPC method on `state`
- rebase triggering: proxies `POST /api/schedulers/:id/rebase` to the `TriggerRebase` gRPC method on `state`
- single-node run triggering: proxies `POST /api/nodes/:service/:schema/:table/run` to the `TriggerSingleNodeRun` gRPC method on `state`
- schedule triggering: proxies `POST /api/schedules/:name/trigger` to the `TriggerSchedule` gRPC method on `state`
- graph update triggering: publishes `update.graph:v1` to Redis via `POST /api/graph/update`

It owns no storage.

**Runtime**: Node.js / Express / TypeScript (port 8090).

## Owned Storage

None.

## Inbound Interfaces

### HTTP server (port 8090)

#### Schedule API

| Route | Method | Backend |
|---|---|---|
| `/api/schedules` | GET | `ListAllSchedules` → state gRPC. Returns the schedule catalog (name, cron, status, last run summary). |
| `/api/schedules/:name/graph` | GET | `GetScheduleGraph` → orchestrator gRPC. Response includes `topology_generation` (current `:TopologyRoot.topology_generation`; `0` = unknown). |
| `/api/schedules/:name/runs` | GET | `ListRuns` → orchestrator gRPC |
| `/api/schedules/:name/trigger` | POST | `TriggerSchedule` → state gRPC |
| `/api/graph/update` | POST | Publishes `update.graph:v1` → Redis |

#### Topology API

| Route | Method | Backend |
|---|---|---|
| `/api/topology/schedules` | GET | `ListScheduleTopologies` → orchestrator gRPC. Returns one entry per schedule with at least one active `:Table`: `{schedule_name, node_count, last_updated_at}`. Backs the homepage `Topology` tab tile grid. |

#### Run / scheduler API

| Route | Method | Backend |
|---|---|---|
| `/api/runs/:run_id/graph` | GET | `GetRunGraph` → orchestrator gRPC. Returns the run's nodes/edges plus `run_topology_generation` + `latest_topology_generation`. Powers drift display on both the schedules dashboard cards and the schedule detail header. |
| `/api/schedulers/:id` | GET | `GetScheduler` → state gRPC |
| `/api/schedulers/:id/tasks` | GET | `ListTasks` → state gRPC (page_size=200) |
| `/api/schedulers/:id/executions` | GET | `ListTaskExecutions` → state gRPC (page_size=500) |
| `/api/schedulers/:id/rerun` | POST | `TriggerRerun` → state gRPC |
| `/api/schedulers/:id/rebase` | POST | `TriggerRebase` → state gRPC. Body is ignored; the run ID from the URL is used as `source_run_id`. |

#### Node API

| Route | Method | Backend |
|---|---|---|
| `/api/nodes/:service/:schema/:table/runs` | GET | `ListNodeRuns` → state gRPC. Returns the last 50 task instances that executed on the node, most recent first. |
| `/api/nodes/:service/:schema/:table/run` | POST | `TriggerSingleNodeRun` → state gRPC. Body `{}` → `metadata_source=latest`; body `{"source_run_id": "<uuid>"}` → `metadata_source=snapshot_of_run`. |

#### Log proxy

| Route | Method | Backend |
|---|---|---|
| `/api/task-executions/:id/logs?key=<s3_key>` | GET | S3 `GetObject` — streams log content as `text/plain` |

#### Frontend

In production mode, `dist/` (built React SPA) is served as static files; all unmatched routes serve `index.html` (SPA fallback).

## Outbound Interfaces

### gRPC to `state` (`STATE_GRPC_ADDR`, default `localhost:50051`)

| Method | Route that calls it |
|---|---|
| `ListAllSchedules` | `GET /api/schedules` |
| `GetScheduler` | `GET /api/schedulers/:id` |
| `ListTasks` | `GET /api/schedulers/:id/tasks` |
| `ListTaskExecutions` | `GET /api/schedulers/:id/executions` |
| `ListNodeRuns` | `GET /api/nodes/:service/:schema/:table/runs` |
| `TriggerRerun` | `POST /api/schedulers/:id/rerun` |
| `TriggerRebase` | `POST /api/schedulers/:id/rebase` |
| `TriggerSingleNodeRun` | `POST /api/nodes/:service/:schema/:table/run` |
| `TriggerSchedule` | `POST /api/schedules/:name/trigger` |

### gRPC to `orchestrator` (`ORCHESTRATOR_GRPC_ADDR`, default `localhost:50052`)

| Method | Route that calls it |
|---|---|
| `GetScheduleGraph` | `GET /api/schedules/:name/graph` |
| `ListRuns` | `GET /api/schedules/:name/runs` |
| `GetRunGraph` | `GET /api/runs/:run_id/graph` (used both directly and by per-card drift polling on the dashboard) |
| `ListScheduleTopologies` | `GET /api/topology/schedules` |

### S3

| Operation | Route | Description |
|---|---|---|
| `GetObject` | `GET /api/task-executions/:id/logs` | Fetches log by `key` query param; proxies content to browser |

On S3 error: returns HTTP 502 with `{ error: "Failed to fetch log from storage" }`.

### Redis (`REDIS_URL`)

| Operation | Route | Description |
|---|---|---|
| `update.graph:v1` | `POST /api/graph/update` | Publishes graph reload command with `source` field (`s3` or `local`); endpoint requires `Authorization: Bearer $GRAPH_UPDATE_TOKEN` when `GRAPH_UPDATE_TOKEN` is set |

#### Graph Update Callers

`POST /api/graph/update` is called in two contexts:

- **Production deploys**: the CI deploy workflow SSHes into the cluster host and runs a one-shot `kubectl run` curl pod inside the `continuo` namespace. The pod sends the HTTP POST with `Authorization: Bearer $GRAPH_UPDATE_TOKEN` and exits.
- **Local development**: `dbt/update-graph.sh` calls the endpoint directly.

## What It Reads

| Data | Source |
|---|---|
| Schedule catalog + last-run summary | `state.ListAllSchedules` |
| Scheduler run details | `state.GetScheduler` |
| Task list for a run | `state.ListTasks` |
| Task execution history | `state.ListTaskExecutions` |
| Recent run history for a single node | `state.ListNodeRuns` |
| Schedule topology (all nodes + edges) | `orchestrator.GetScheduleGraph` |
| Run list (historical) | `orchestrator.ListRuns` |
| Per-run graph with node statuses + per-run/latest topology generation | `orchestrator.GetRunGraph` |
| Pod logs | S3 (via `log_s3_key` from task execution records) |

## What It Writes

| Data | Target |
|---|---|
| Rerun trigger (reset failed task + downstream) | `state.TriggerRerun` via `POST /api/schedulers/:id/rerun` |
| Rebase trigger (re-execute failed/cancelled tasks + new arrivals against latest topology) | `state.TriggerRebase` via `POST /api/schedulers/:id/rebase` |
| Single-node run trigger (one-task ad-hoc run for a specific dbt node) | `state.TriggerSingleNodeRun` via `POST /api/nodes/:service/:schema/:table/run` |
| Schedule trigger (start full DAG run) | `state.TriggerSchedule` via `POST /api/schedules/:name/trigger` |
| Graph update command | Redis `update.graph:v1` stream via `POST /api/graph/update` |

## Data Transformations

- **Status normalization**: proto enum prefixes (`SCHEDULER_STATUS_`, `TASK_STATUS_`) are stripped and values are lowercased before returning to the client (e.g. `SCHEDULER_STATUS_RUNNING` → `"running"`).
- **Timestamp conversion**: proto `Timestamp` (seconds + nanos) is converted to ISO 8601 strings.
- **Node ID construction**: graph node IDs are constructed as `{service_name}.{schema_name}.{table_name}`.

## DAG Panel Source

- **Primary**: `/api/runs/:run_id/graph` — uses the run snapshot created by `orchestrator`; includes per-node `EXECUTES.status` from the live execution projection.
- **Fallback**: `/api/schedules/:name/graph` — topology view without run status; used when no run snapshot exists yet.
- When a run snapshot includes node statuses, the DAG renderer uses those directly and only falls back to `state` task rows for the same node when both are present.
- **Latest mode** (`/schedule/:name/latest`): `DetailPage` is rendered with `mode="latest"`; `resolveActiveGraph` short-circuits to the topology graph (`/api/schedules/:name/graph`, polled every 5s). A `topology v<N>` chip is rendered in the page header using `:TopologyRoot.topology_generation`. The run-centric drift chip (`source N gen behind latest`) is suppressed in this mode — it compares a selected run's source generation to the latest topology, which is meaningless when the canvas already shows the latest topology. The chip remains active inside `RerunFailedModal`, where the user is acting on a specific run. Triggers from this route still work; the orchestrator pins the generation at snapshot-write time.

## Frontend Architecture

- React SPA (TypeScript + Vite)
- `DashboardPage`: two URL-routed tabs under the page header — `Runs` (default, `/`) shows the `SchedulerCard` list, and `Topology` (`/?tab=topology`) shows the `SnapshotTile` grid. Both data sources poll every 5 seconds regardless of active tab: `/api/schedules` feeds the `Runs` tab and the `Runs` count pill; `/api/topology/schedules` feeds the `Topology` tab and its count pill. Each snapshot tile navigates to `/schedule/:name/latest`.
- `SchedulerCard`: displays schedule name, running status, cron expression, last run time and progress; polls `/api/schedulers/:last_run_id/tasks` for task progress and `/api/runs/:last_run_id/graph` for topology-drift information (both every 5 s); shows a warning strip when the last run's `run_topology_generation` is older than the orchestrator's `latest_topology_generation`, matching the drift logic used on the schedule detail page; includes a "Trigger run" button to start a full DAG run (disabled while a run is active) and a "Cancel" button while a run is in flight
- `DetailPage`: two-column layout — left column shows the `Dependency Graph` (`DAGPanel`); right column is a single `.detail-card` whose header is a panel-level tab bar with two URL-routed tabs (`Nodes` default, `Past Runs` via `?panel=runs`). Both modes (`/schedule/:name` and `/schedule/:name/latest`) inherit the structure. Includes Rerun and Rebase buttons for terminal runs with drift badge when topology generation differs.
- `DAGPanel`: renders graph topology using run graph or schedule graph
- `PastRunsPanel`: lists historical runs from `orchestrator.ListRuns`
- `NodeDetailPage`: per-node detail page; fetches recent run history via `GET /api/nodes/:service/:schema/:table/runs`; provides a "Trigger run" control that opens `RunSourcePickerDialog` to select between latest metadata and a pinned source run
- `RunSourcePickerDialog`: modal for choosing `metadata_source` (`latest` or `snapshot_of_run`) before calling `POST /api/nodes/:service/:schema/:table/run`

## Reliability Notes

- Mostly read-only; write-side effects are `TriggerRerun` (via `POST /api/schedulers/:id/rerun`), `TriggerRebase` (via `POST /api/schedulers/:id/rebase`), `TriggerSingleNodeRun` (via `POST /api/nodes/:service/:schema/:table/run`), `TriggerSchedule` (via `POST /api/schedules/:name/trigger`), and `POST /api/graph/update` (publishes `update.graph:v1` to Redis). All trigger calls delegate atomicity and error semantics to `state`.
- gRPC errors are surfaced as HTTP 500 with the gRPC error message.
- S3 errors are surfaced as HTTP 502.
- `log_s3_key` is stored by `k8s-controller` on task execution records; the UI does not resolve or generate S3 keys itself.
- `ListAllSchedules` reads from `schedule_catalog`; a schedule not in the catalog (e.g. activated before the catalog was populated) will not appear in the dashboard until the catalog is updated.
