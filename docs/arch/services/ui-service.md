# ui-service

## Purpose

`ui-service` is an HTTP facade and React frontend for operators.

It provides:
- a real-time dashboard of all schedules and their last-run status
- a detail view per schedule run: DAG topology, node statuses, task list, execution history
- S3 log proxying: fetches pod logs from S3 and streams them to the browser
- rerun triggering: proxies `POST /api/schedulers/:id/rerun` to the `TriggerRerun` gRPC method on `state`

It owns no storage.

**Runtime**: Node.js / Express / TypeScript (port 8090).

## Owned Storage

None.

## Inbound Interfaces

### HTTP server (port 8090)

#### Schedule API

| Route | Method | Backend |
|---|---|---|
| `/api/schedules` | GET | `ListAllSchedules` → state gRPC |
| `/api/schedules/:name/graph` | GET | `GetScheduleGraph` → graph gRPC |
| `/api/schedules/:name/runs` | GET | `ListRuns` → graph gRPC |

#### Run / scheduler API

| Route | Method | Backend |
|---|---|---|
| `/api/runs/:run_id/graph` | GET | `GetRunGraph` → graph gRPC |
| `/api/schedulers/:id` | GET | `GetScheduler` → state gRPC |
| `/api/schedulers/:id/tasks` | GET | `ListTasks` → state gRPC (page_size=200) |
| `/api/schedulers/:id/executions` | GET | `ListTaskExecutions` → state gRPC (page_size=500) |
| `/api/schedulers/:id/rerun` | POST | `TriggerRerun` → state gRPC |

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
| `TriggerRerun` | `POST /api/schedulers/:id/rerun` |

### gRPC to `graph` (`GRAPH_GRPC_ADDR`, default `localhost:50052`)

| Method | Route that calls it |
|---|---|
| `GetScheduleGraph` | `GET /api/schedules/:name/graph` |
| `ListRuns` | `GET /api/schedules/:name/runs` |
| `GetRunGraph` | `GET /api/runs/:run_id/graph` |

### S3

| Operation | Route | Description |
|---|---|---|
| `GetObject` | `GET /api/task-executions/:id/logs` | Fetches log by `key` query param; proxies content to browser |

On S3 error: returns HTTP 502 with `{ error: "Failed to fetch log from storage" }`.

## What It Reads

| Data | Source |
|---|---|
| Schedule catalog + last-run summary | `state.ListAllSchedules` |
| Scheduler run details | `state.GetScheduler` |
| Task list for a run | `state.ListTasks` |
| Task execution history | `state.ListTaskExecutions` |
| Schedule topology (all nodes + edges) | `graph.GetScheduleGraph` |
| Run list (historical) | `graph.ListRuns` |
| Per-run graph with node statuses | `graph.GetRunGraph` |
| Pod logs | S3 (via `log_s3_key` from task execution records) |

## What It Writes

| Data | Target |
|---|---|
| Rerun trigger (reset failed task + downstream) | `state.TriggerRerun` via `POST /api/schedulers/:id/rerun` |

## Data Transformations

- **Status normalization**: proto enum prefixes (`SCHEDULER_STATUS_`, `TASK_STATUS_`) are stripped and values are lowercased before returning to the client (e.g. `SCHEDULER_STATUS_RUNNING` → `"running"`).
- **Timestamp conversion**: proto `Timestamp` (seconds + nanos) is converted to ISO 8601 strings.
- **Node ID construction**: graph node IDs are constructed as `{service_name}.{schema_name}.{table_name}`.

## DAG Panel Source

- **Primary**: `/api/runs/:run_id/graph` — uses the run snapshot created by `startup-controller → graph.SnapshotGraph`; includes per-node `EXECUTES.status` from the live execution projection.
- **Fallback**: `/api/schedules/:name/graph` — topology view without run status; used when no run snapshot exists yet.
- When a run snapshot includes node statuses, the DAG renderer uses those directly and only falls back to `state` task rows for the same node when both are present.

## Frontend Architecture

- React SPA (TypeScript + Vite)
- `DashboardPage`: polls `/api/schedules` every 5 seconds; shows schedule cards
- `SchedulerCard`: displays schedule name, running status, cron expression, last run time and progress
- `DetailPage`: shows DAG panel, nodes panel, past runs panel for a selected run
- `DAGPanel`: renders graph topology using run graph or schedule graph
- `PastRunsPanel`: lists historical runs from `graph.ListRuns`

## Reliability Notes

- Mostly read-only; the only write-side effect is `TriggerRerun` (via `POST /api/schedulers/:id/rerun`), which resets a failed task and its downstream in `state`.
- gRPC errors are surfaced as HTTP 500 with the gRPC error message.
- S3 errors are surfaced as HTTP 502.
- `log_s3_key` is stored by `k8s-controller` on task execution records; the UI does not resolve or generate S3 keys itself.
- `ListAllSchedules` reads from `schedule_catalog`; a schedule not in the catalog (e.g. activated before the catalog was populated) will not appear in the dashboard until the catalog is updated.
