# ui-service

## Purpose

`ui-service` is an HTTP facade and React frontend for operators.

It provides:
- a real-time dashboard of all schedules and their last-run status
- a detail view per schedule run: DAG topology, node statuses, task list, execution history
- a per-node detail page: recent run history for a single dbt node, with trigger controls
- a Releases tab on the homepage: live prod release, in-flight candidate, and paginated release history
- a release detail page: per-node validation results with an inline dbt log viewer
- S3 log proxying: fetches pod logs from S3 and streams them to the browser (used by both task executions and release validation logs)
- rerun triggering: proxies `POST /api/schedulers/:id/rerun` to the `TriggerRerun` gRPC method on `state`
- rebase triggering: proxies `POST /api/schedulers/:id/rebase` to the `TriggerRebase` gRPC method on `state`
- single-node run triggering: proxies `POST /api/nodes/:service/:schema/:table/run` to the `TriggerSingleNodeRun` gRPC method on `state`
- schedule triggering: proxies `POST /api/schedules/:name/trigger` to the `TriggerSchedule` gRPC method on `state`
- a chat panel backed by `/ws/chat` (enabled only when `CHAT_BRIDGE_ENABLED=true`): a WebSocket (WS) endpoint that exposes a Large Language Model (LLM) agent which inspects schedule status, task status, and dependency graphs via the `continuo` CLI (Command-Line Interface); mutating commands are blocked by a deny-list, and the endpoint is gated off outside local development

It owns no storage and constructs no Redis client.

**Runtime**: Node.js / Express / TypeScript (port 8090).

## Chat Bridge

### Overview

`ui-service` exposes a `/ws/chat` WebSocket (WS) endpoint that is attached to its HTTP server only when the environment variable `CHAT_BRIDGE_ENABLED=true` is set. The endpoint is OFF by default, including in the production image (which runs `node dist-server/index.js` without the flag). Local development enables it via the `dev` npm script. Operating the bridge in a shared or production environment additionally requires the `claude` and `continuo` binaries present in the runtime image with Claude credentials, plus authentication, origin checks, connection limits, and Application Programming Interface (API) budget quotas on the endpoint — none of which are provided today.

Each incoming WebSocket connection receives one dedicated headless `claude` subprocess. The subprocess runs in streaming-JSON mode:

```
claude -p --input-format stream-json --output-format stream-json --verbose
```

Read-only behavior is enforced by a deny-list, not the allow-list. In headless `claude -p` mode the `--allowedTools` list does not act as a default-deny — tool calls are auto-approved — so the intended read surface (`Bash(continuo schedule status:*)`, `Bash(continuo schedule list:*)`, `Bash(continuo schedule graph:*)`, `Bash(continuo describe:*)`) is documentation of intent rather than a boundary. The boundary is `--disallowedTools`, which Claude Code does honor: `Bash(continuo schedule trigger:*)` is denied, so the mutating command cannot run. The system prompt additionally instructs read-only behavior as defense in depth. This is best-effort confinement for local development only: the subprocess is not sandboxed against arbitrary shell, so it runs with the developer's privileges. That, together with the absence of authentication, is why the endpoint is gated off outside local development. The agent inspects the system by shelling out to the `continuo` CLI, which in turn reads `state` and `orchestrator` over gRPC (Remote Procedure Call). The `claude` process itself has no direct gRPC or Redis connections.

The spawned `claude` process (and the `continuo` CLI it invokes) receive `CONTINUO_STATE_ADDR` and `CONTINUO_ORCHESTRATOR_ADDR` in their environment, mapped from the ui-service server's `STATE_GRPC_ADDR` and `ORCHESTRATOR_GRPC_ADDR`, so the CLI reaches the same `state` and `orchestrator` gRPC endpoints the ui-service uses.

### Process lifetime and session continuity

One subprocess is created per WebSocket connection. The bridge captures the Claude session ID from the first response. The subprocess signals termination exactly once whether it exits normally or fails to spawn; on the next user turn the bridge respawns it and passes `--resume <session_id>` so the conversation resumes without loss of context. A `new_chat` message from the client terminates any existing subprocess, clears the session ID, and starts a fresh conversation. The browser chat socket reconnects automatically with capped exponential backoff, reusing the stored session ID, after a disconnect.

### Message contract

**Client → server messages** (JSON over WebSocket):

| `type` | Payload | Meaning |
|---|---|---|
| `user_message` | `{ "text": string }` | User turn to relay to the `claude` subprocess |
| `new_chat` | `{}` | Reset the current conversation and start a new one |

**Server → client messages** (JSON over WebSocket):

| `type` | Payload | Meaning |
|---|---|---|
| `session` | `{ "sessionId": string }` | Captured Claude session ID; sent once after the first response |
| `tool` | `{ "command": string }` | Tool call in flight (for UI progress indication) |
| `text` | `{ "text": string }` | Assistant text for the current turn (emitted at whole-message granularity, not token-by-token) |
| `final` | `{ "text": string }` | Complete assistant response, marking the turn as done |
| `error` | `{ "code": string, "message": string }` | Bridge or subprocess error |

### Scope and constraints

A deny-list (`--disallowedTools`) blocks the mutating `continuo schedule trigger`; the agent is steered to the read-only surface below by the system prompt and the documented allow-list. The `continuo` CLI commands surfaced are:

| Command | Data read |
|---|---|
| `schedule list` | All schedules and their last-run status |
| `schedule status <name>` | Per-node task status of a schedule's latest run |
| `schedule graph <name>` | Dependency graph (nodes and edges) |
| `describe` | Machine-readable command catalog |

No backend service contracts, storage ownership, Redis Streams flows, or gRPC interfaces change as a result of the chat bridge. The bridge introduces no new outbound connections beyond what the `continuo` CLI already makes to `state` and `orchestrator`.

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

#### Release API

| Route | Method | Backend |
|---|---|---|
| `/api/releases` | GET | Proxies `GET /releases` on release-controller (passes through `status`, `limit`, `cursor` query params). Returns paginated history. |
| `/api/releases/current-prod` | GET | Proxies `GET /current-prod` on release-controller. |
| `/api/releases/:id` | GET | Proxies `GET /releases/{id}` on release-controller. Returns full detail including `per_node_results`. |
| `/api/releases/log?key=<s3_key_or_uri>` | GET | S3 `GetObject` — streams dbt log content as `text/plain`. Accepts `s3://<bucket>/<key>` URIs or bare keys. |

#### Log proxy

| Route | Method | Backend |
|---|---|---|
| `/api/task-executions/:id/logs?key=<s3_key>` | GET | S3 `GetObject` — streams log content as `text/plain` |

#### Chat WebSocket

| Route | Protocol | Description |
|---|---|---|
| `/ws/chat` | WebSocket | Chat bridge. Attached only when `CHAT_BRIDGE_ENABLED=true`; absent by default. Each connection spawns one `claude` subprocess. See "Chat Bridge" section for the full message contract. |

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

### HTTP to `release-controller` (`RELEASE_CONTROLLER_URL`, default `http://release-controller:8088`)

| Route proxied | BFF route |
|---|---|
| `GET /releases` | `GET /api/releases` |
| `GET /releases/{id}` | `GET /api/releases/:id` |
| `GET /current-prod` | `GET /api/releases/current-prod` |

HTTP errors from release-controller are forwarded with their status code; network errors return HTTP 502.

### S3

| Operation | Route | Description |
|---|---|---|
| `GetObject` | `GET /api/task-executions/:id/logs` | Fetches task-execution log by `key` query param; proxies content to browser |
| `GetObject` | `GET /api/releases/log` | Fetches dbt validation log by `key` query param (accepts `s3://` URI or bare key); proxies content to browser |

On S3 error: returns HTTP 502 with `{ error: "Failed to fetch log from storage" }`.

`ui-service` makes no Redis connection; it reaches backends through gRPC (`state`, `orchestrator`), HTTP (`release-controller`), and S3 (log proxy) only.

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
| Paginated release history | `release-controller GET /releases` |
| Release detail (per-node validation results) | `release-controller GET /releases/{id}` |
| Current production release + topology snapshot | `release-controller GET /current-prod` |
| Pod logs | S3 (via `log_s3_key` from task execution records) |
| dbt validation logs | S3 (via `dbt_log_uri` from per-node validation results) |

## What It Writes

| Data | Target |
|---|---|
| Rerun trigger (reset failed task + downstream) | `state.TriggerRerun` via `POST /api/schedulers/:id/rerun` |
| Rebase trigger (re-execute failed/cancelled tasks + new arrivals against latest topology) | `state.TriggerRebase` via `POST /api/schedulers/:id/rebase` |
| Single-node run trigger (one-task ad-hoc run for a specific dbt node) | `state.TriggerSingleNodeRun` via `POST /api/nodes/:service/:schema/:table/run` |
| Schedule trigger (start full DAG run) | `state.TriggerSchedule` via `POST /api/schedules/:name/trigger` |

## Data Transformations

- **Status normalization**: proto enum prefixes (`SCHEDULER_STATUS_`, `TASK_STATUS_`) are stripped and values are lowercased before returning to the client (e.g. `SCHEDULER_STATUS_RUNNING` → `"running"`).
- **Timestamp conversion**: proto `Timestamp` (seconds + nanos) is converted to ISO 8601 strings.
- **Node ID construction**: graph node IDs are constructed as `{service_name}.{schema_name}.{table_name}`.

## DAG Panel Source

- **Primary**: `/api/runs/:run_id/graph` — uses the run snapshot created by `orchestrator`; includes per-node `EXECUTES.status` from the live execution projection.
- **Fallback**: `/api/schedules/:name/graph` — topology view without run status; used when no run snapshot exists yet.
- When a run snapshot includes node statuses, the DAG renderer uses those directly and only falls back to `state` task rows for the same node when both are present.
- **Latest mode** (`/schedule/:name/latest`): `DetailPage` is rendered with `mode="latest"`; `resolveActiveGraph` short-circuits to the topology graph (`/api/schedules/:name/graph`, polled every 5s). A `topology v<N>` chip is rendered in the page header using `:TopologyRoot.topology_generation`. The run-centric drift chip (`source N gen behind latest`) is suppressed in this mode — it compares a selected run's source generation to the latest topology, which is meaningless when the canvas already shows the latest topology. The chip remains active inside `RerunFailedModal`, where the user is acting on a specific run. Triggers from this route still work; the orchestrator pins the generation at snapshot-write time. In latest mode the canvas paints every node with neutral idle styling (white fill, solid grey border) regardless of any past run's task statuses, and the graph badge is fixed to `catalog`. The `/api/schedulers/:lastRunId/tasks` and `/executions` endpoints are not polled in this mode.

## Frontend Architecture

- React SPA (TypeScript + Vite)
- `DashboardPage`: three URL-routed tabs under the page header — `Runs` (default, `/?tab=runs`) shows the `SchedulerCard` list, `Topology` (`/?tab=topology`) shows the `SnapshotTile` grid, and `Releases` (`/?tab=releases`) shows `ReleasesPanel`. Schedule and topology data sources poll every 5 seconds regardless of active tab: `/api/schedules` feeds the `Runs` tab and the `Runs` count pill; `/api/topology/schedules` feeds the `Topology` tab and its count pill. Each snapshot tile navigates to `/schedule/:name/latest`.
- `ReleasesPanel`: displays the live `current_prod` release, the first in-flight candidate (parsing or validating), and a paginated release history list with status filtering. Fetches `/api/releases/current-prod` and `/api/releases` (with optional `status` and `cursor` params); supports load-more pagination via `next_cursor`. Each release row links to `ReleaseDetailPage`.
- `ReleaseDetailPage`: full detail view for a single release at `/releases/:id`. Fetches `/api/releases/:id`. Shows status, bootstrap flag, reject reason, and a per-node validation results table. Each row with a `dbt_log_uri` includes an inline log viewer that fetches `/api/releases/log?key=<uri>` on demand.
- `SchedulerCard`: displays schedule name, running status, cron expression, last run time and progress; polls `/api/schedulers/:last_run_id/tasks` for task progress and `/api/runs/:last_run_id/graph` for topology-drift information (both every 5 s); shows a warning strip when the last run's `run_topology_generation` is older than the orchestrator's `latest_topology_generation`, matching the drift logic used on the schedule detail page; includes a "Trigger run" button to start a full DAG run (disabled while a run is active) and a "Cancel" button while a run is in flight
- `DetailPage`: two-column layout — left column shows the `Dependency Graph` (`DAGPanel`). Right column branches on mode. In `/schedule/:name` the column header is a panel-level tab bar with two URL-routed tabs (`Nodes` default, `Past Runs` via `?panel=runs`). In `/schedule/:name/latest` the column is a single `.detail-card` with a `.section-header` titled `Past Runs` followed by `PastRunsPanel` — the panel tab strip is omitted because only one panel remains, per `.claude/design-guideline/ui.md`. Includes Rerun and Rebase buttons for terminal runs with drift badge when topology generation differs.
- `DAGPanel`: renders graph topology using run graph or schedule graph
- `PastRunsPanel`: lists historical runs from `orchestrator.ListRuns`
- `NodeDetailPage`: per-node detail page; fetches recent run history via `GET /api/nodes/:service/:schema/:table/runs`; provides a "Trigger run" control that opens `RunSourcePickerDialog` to select between latest metadata and a pinned source run
- `RunSourcePickerDialog`: modal for choosing `metadata_source` (`latest` or `snapshot_of_run`) before calling `POST /api/nodes/:service/:schema/:table/run`

## Reliability Notes

- Mostly read-only; write-side effects are `TriggerRerun` (via `POST /api/schedulers/:id/rerun`), `TriggerRebase` (via `POST /api/schedulers/:id/rebase`), `TriggerSingleNodeRun` (via `POST /api/nodes/:service/:schema/:table/run`), and `TriggerSchedule` (via `POST /api/schedules/:name/trigger`). All trigger calls delegate atomicity and error semantics to `state`.
- gRPC errors are surfaced as HTTP 500 with the gRPC error message.
- S3 errors are surfaced as HTTP 502.
- `log_s3_key` is stored by `k8s-controller` on task execution records; the UI does not resolve or generate S3 keys itself.
- `ListAllSchedules` reads from `schedule_catalog`; a schedule not in the catalog (e.g. activated before the catalog was populated) will not appear in the dashboard until the catalog is updated.
