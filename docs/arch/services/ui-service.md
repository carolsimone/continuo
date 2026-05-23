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
| `/api/schedules/:name/graph` | GET | `GetScheduleGraph` → orchestrator gRPC |
| `/api/schedules/:name/runs` | GET | `ListRuns` → orchestrator gRPC |
| `/api/schedules/:name/trigger` | POST | `TriggerSchedule` → state gRPC |
| `/api/graph/update` | POST | `XADD update.graph:v1` → Redis |

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

### S3

| Operation | Route | Description |
|---|---|---|
| `GetObject` | `GET /api/task-executions/:id/logs` | Fetches log by `key` query param; proxies content to browser |

On S3 error: returns HTTP 502 with `{ error: "Failed to fetch log from storage" }`.

### Redis (`REDIS_URL`)

| Operation | Route | Description |
|---|---|---|
| `XADD update.graph:v1` | `POST /api/graph/update` | Publishes graph reload command with `source` field (`s3` or `local`) |

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

## Frontend Architecture

- React SPA (TypeScript + Vite)
- `DashboardPage`: polls `/api/schedules` every 5 seconds; shows schedule cards
- `SchedulerCard`: displays schedule name, running status, cron expression, last run time and progress; polls `/api/schedulers/:last_run_id/tasks` for task progress and `/api/runs/:last_run_id/graph` for topology-drift information (both every 5 s); shows a warning strip when the last run's `run_topology_generation` is older than the orchestrator's `latest_topology_generation`, matching the drift logic used on the schedule detail page; includes a "Trigger run" button to start a full DAG run (disabled while a run is active) and a "Cancel" button while a run is in flight
- `DetailPage`: shows DAG panel, nodes panel, past runs panel for a selected run; includes Rerun and Rebase buttons for terminal runs with drift badge when topology generation differs
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
