# ui-service

## Purpose

`ui-service` is an HTTP facade and React frontend for operators.

It provides:
- OpenID Connect (OIDC) login with role-based authorization: every `/api` route requires an authenticated session, and mutations plus the chat WebSocket (WS) require the `operator` role (see "Authentication & Authorization")
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
- a chat panel backed by `/ws/chat` (enabled only when `CHAT_BRIDGE_ENABLED=true`): a WebSocket (WS) endpoint that exposes a Large Language Model (LLM) agent which inspects schedule status, task status, and dependency graphs via the `continuo` CLI (Command-Line Interface); mutating commands are blocked by a deny-list, the WebSocket upgrade is operator-only, and the endpoint is gated off outside local development

Its only datastore is Redis, used in `AUTH_MODE=oidc` for server-side login sessions under plain `uisession:` keys (not Redis Streams). It owns no Postgres or Neo4j storage.

**Runtime**: Node.js / Express / TypeScript (port 8090).

## Authentication & Authorization

`ui-service` is the only HTTP edge of the system; it authenticates every browser user as an OIDC (OpenID Connect) relying party and enforces role-based authorization on every request.

### Modes

`AUTH_MODE` is required; there is no unauthenticated mode — a missing or invalid value fails boot.

| Mode | Behavior |
|---|---|
| `oidc` | Production. Full OIDC login flow against an external Identity Provider (IdP); sessions stored in Redis. |
| `dev` | Local development and the standard end-to-end (e2e) suites (the docker-compose `ui` service). Every request carries the fixed placeholder identity `dev\|local` with the `operator` role; a loud warning is logged at boot. |

### Login flow (`src/server/auth/`)

Authorization Code + PKCE (Proof Key for Code Exchange) via the `openid-client` library. The issuer is discovered from `AUTH_OIDC_ISSUER_URL` at boot, retried with backoff; if discovery never succeeds the process exits and the platform restarts it.

| Route | Method | Behavior |
|---|---|---|
| `/auth/login` | GET | Redirects to the IdP authorization endpoint. State, nonce, and the PKCE verifier travel in a 10-minute HttpOnly pending cookie. |
| `/auth/callback` | GET | Validates state + nonce, resolves the role, creates the Redis session, sets the session cookie, and redirects to the sanitized `returnTo` (same-origin relative paths only). |
| `/auth/logout` | POST | Destroys the Redis session, clears the cookie, and returns the IdP end-session redirect when the IdP advertises one. |
| `/auth/me` | GET | Current identity (`userId`, `email`, `name`, `role`) for the React shell; 401 when unauthenticated. |

### Sessions

- Server-side in Redis under `uisession:<256-bit-random-id>` keys — plain keys with TTLs, not Redis Streams, so `pkg/streams/contract.yaml` is unaffected.
- The browser holds only an opaque HttpOnly SameSite=Lax cookie `continuo_session` (Secure when `AUTH_PUBLIC_URL` is https).
- Sliding idle TTL (`AUTH_SESSION_IDLE_TTL`, default `8h`) capped by an absolute lifetime (`AUTH_SESSION_MAX_TTL`, default `24h`).
- Deleting the Redis key revokes the session instantly.
- IdP tokens are used once at the callback and discarded; per-request checks touch only Redis.

### Roles

Two roles: `viewer` (read) and `operator` (viewer + mutations + chat). Resolution happens once, at login:

1. The IdP groups claim (`AUTH_GROUPS_CLAIM`, default `groups`) is mapped through `AUTH_ROLE_MAPPING` (comma-separated `group=role` pairs); the strongest matching role wins. Group membership comes from the IdP directory and is not gated on email verification.
2. Email overrides: `AUTH_OPERATOR_EMAILS` / `AUTH_VIEWER_EMAILS`. These lists are applied only when the ID token asserts a verified email (`email_verified` claim is `true` or `"true"`); an unverified or absent `email_verified` falls through to step 3. This prevents privilege escalation through unverified email aliases.
3. `AUTH_DEFAULT_ROLE` (default `none` — login is denied).

### Request gating

Enforced by middleware wired in `createApp`; method-based, so new endpoints are safe by default:

| Surface | Requirement |
|---|---|
| `/healthz` | Public — Kubernetes probe target. |
| `/auth/*` | Public — login machinery. |
| Static SPA shell | Public — holds no data and renders a sign-in page when `/auth/me` returns 401. |
| `GET /api/*` | Any authenticated user. |
| `POST`/`PUT`/`PATCH`/`DELETE` `/api/*` | `operator` role (403 otherwise). |
| `/ws/chat` upgrade | `operator` only; rejected with HTTP 401 before any WebSocket is established. Browser requests whose `Origin` header does not match the deployment origin (derived from `AUTH_PUBLIC_URL`) are additionally rejected with HTTP 403. Requests with no `Origin` header (non-browser clients) pass. The origin check is skipped in `dev` mode. |

### CSRF (Cross-Site Request Forgery)

SameSite=Lax cookies plus an Origin-header check on mutating `/api` requests: a browser `Origin` that does not match `AUTH_PUBLIC_URL`'s origin is rejected with 403. Requests without an `Origin` header pass through to the auth gate — they carry no ambient cookie and are not CSRF-able.

### Error body contract

All auth error responses use the shape `{ "error": "<human-readable string>", "code": "<machine token>" }`. The `error` string is suitable for the client to render directly; the `code` value is a stable token for programmatic handling (e.g. `unauthenticated`, `forbidden`, `csrf_rejected`, `auth_unavailable`). The terminal error handler is status-aware: an upstream client error carrying an HTTP status below 500 (for example, malformed JSON rejected by the body parser at status 400) is preserved at that status with `code: "bad_request"`. Only errors with no HTTP status, or with a 5xx status, indicate a backend outage (Redis or the IdP unreachable) and are returned as `503` with `code: "auth_unavailable"`.

### Audit

One structured JSON line to stdout per auth event (login success/failure/denied, logout, `role_denied`, `csrf_rejected`, `ws_denied`) and per mutating `/api` call (user, role, method, path, outcome status).

### Failure modes

- Redis unreachable → 503 fail-closed; a request is never passed through unauthenticated.
- IdP unreachable after boot → new logins fail; active sessions keep working, because per-request checks touch only Redis.

### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `AUTH_MODE` | required | `oidc` or `dev` |
| `AUTH_OIDC_ISSUER_URL` | required (oidc) | OIDC issuer URL for discovery |
| `AUTH_OIDC_CLIENT_ID` | required (oidc) | OIDC client ID |
| `AUTH_OIDC_CLIENT_SECRET` | required (oidc) | OIDC client secret |
| `AUTH_PUBLIC_URL` | required (oidc) | Externally visible base URL; drives the redirect URI, the cookie `Secure` flag, and the CSRF origin |
| `AUTH_OIDC_SCOPES` | `openid email profile` | Requested scopes |
| `AUTH_GROUPS_CLAIM` | `groups` | ID-token claim holding group names |
| `AUTH_ROLE_MAPPING` | empty | `group=role` pairs |
| `AUTH_OPERATOR_EMAILS` / `AUTH_VIEWER_EMAILS` | empty | Per-email role overrides |
| `AUTH_DEFAULT_ROLE` | `none` | Role when nothing matches (`none` = login denied) |
| `AUTH_SESSION_IDLE_TTL` | `8h` | Sliding session idle TTL |
| `AUTH_SESSION_MAX_TTL` | `24h` | Absolute session lifetime cap |
| `REDIS_URL` | required (oidc) | Session store connection |

Deployment surfaces:

- The docker-compose `ui` service runs `AUTH_MODE=dev`.
- The `auth-e2e` compose profile (dex + `ui-auth`) runs the real OIDC flow for the dedicated e2e suite (`tests/e2e/auth_oidc_test.go`).
- Helm (`deploy/app`) runs `AUTH_MODE=oidc`. Per-deployment identity settings (issuer URL, client ID, public URL, operator/viewer email allowlists, role mapping) come from `global.auth.*`, which the deployment template expands into the `AUTH_*` env via sentinels; the committed defaults are empty so a deploy fails closed until they are set in the on-box secret values file. The client secret lands in the chart credentials Secret via `global.auth.oidcClientSecret`, and `REDIS_URL` expands from `__REDIS_URL_FROM_AUTH__`. Kubernetes probes target `/healthz`.

Operator setup, including configuring an identity provider and a no-domain Google loopback flow for testing, is documented in `deploy/app/AUTH.md`.

## Chat Bridge

### Overview

`ui-service` exposes a `/ws/chat` WebSocket (WS) endpoint that is attached to its HTTP server only when the environment variable `CHAT_BRIDGE_ENABLED=true` is set. The endpoint is OFF by default, including in the production image (which runs `node dist-server/index.js` without the flag). Local development enables it via the `dev` npm script. The same flag is surfaced to the browser via `GET /api/features` (`{ "chatBridgeEnabled": boolean }`); the client reads it on load and only mounts the chat panel — and only opens the `/ws/chat` socket — when the bridge is enabled, so the default/production configuration shows no chat panel rather than a permanently disconnected one. The endpoint upgrade is authenticated: only a session with the `operator` role may open `/ws/chat`; anything else is rejected with HTTP 401 before the WebSocket is established (audited as `ws_denied`). Browser upgrade requests whose `Origin` header does not match the deployment origin (derived from `AUTH_PUBLIC_URL`) are rejected with HTTP 403 before authentication is attempted, preventing cross-site WebSocket hijacking. Requests with no `Origin` header (non-browser clients) are not subject to this check. The origin check is skipped in `dev` mode. Operating the bridge in a shared or production environment additionally requires the `claude` and `continuo` binaries present in the runtime image with Claude credentials, plus connection limits and Application Programming Interface (API) budget quotas on the endpoint — none of which are provided today.

Each incoming WebSocket connection receives one dedicated headless `claude` subprocess. The subprocess runs in streaming-JSON mode:

```
claude -p --input-format stream-json --output-format stream-json --verbose
```

Read-only behavior is enforced by a deny-list, not the allow-list. In headless `claude -p` mode the `--allowedTools` list does not act as a default-deny — tool calls are auto-approved — so the intended read surface (`Bash(continuo schedule status:*)`, `Bash(continuo schedule list:*)`, `Bash(continuo schedule graph:*)`, `Bash(continuo describe:*)`) is documentation of intent rather than a boundary. The boundary is `--disallowedTools`, which Claude Code does honor: `Bash(continuo schedule trigger:*)` is denied, so the mutating command cannot run. The system prompt additionally instructs read-only behavior as defense in depth. This is best-effort confinement for local development only: the subprocess is not sandboxed against arbitrary shell, so it runs with the developer's privileges. That unsandboxed subprocess is why the endpoint stays gated off outside local development even though the upgrade itself is operator-only. The agent inspects the system by shelling out to the `continuo` CLI, which in turn reads `state` and `orchestrator` over gRPC (Remote Procedure Call). The `claude` process itself has no direct gRPC or Redis connections.

The spawned `claude` process (and the `continuo` CLI it invokes) receive `CONTINUO_STATE_ADDR` and `CONTINUO_ORCHESTRATOR_ADDR` in their environment, mapped from the ui-service server's `STATE_GRPC_ADDR` and `ORCHESTRATOR_GRPC_ADDR`, so the CLI reaches the same `state` and `orchestrator` gRPC endpoints the ui-service uses.

### Process lifetime and session continuity

One subprocess is created per WebSocket connection. The bridge captures the Claude session ID from the first response. The subprocess signals termination exactly once whether it exits normally or fails to spawn; on the next user turn the bridge respawns it and passes `--resume <session_id>` so the conversation resumes without loss of context. A `new_chat` message from the client terminates any existing subprocess, clears the session ID, and starts a fresh conversation. The browser chat socket reconnects automatically with capped exponential backoff, reusing the stored session ID, after a disconnect.

### Message contract

**Client → server messages** (JSON over WebSocket):

| `type` | Payload | Meaning |
|---|---|---|
| `user_message` | `{ "text": string }` | User turn to relay to the `claude` subprocess |
| `new_chat` | `{}` | Reset the current conversation and start a new one |

Incoming frames are validated before use: anything that is not valid JSON, or that decodes to a non-object (e.g. `null`, a number, an array), is dropped silently, as is a `user_message` whose `text` is not a string. A malformed frame can therefore never throw an uncaught exception and tear down the server.

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

Redis, in `AUTH_MODE=oidc` only: server-side login sessions under plain `uisession:<256-bit-random-id>` keys with TTLs. These are ordinary keys, not Redis Streams — `pkg/streams/contract.yaml` is unaffected. In `AUTH_MODE=dev` no Redis client is constructed. No Postgres, no Neo4j.

## Inbound Interfaces

### HTTP server (port 8090)

All `/api` routes below require an authenticated session; mutating methods (every `POST` below) additionally require the `operator` role. See "Authentication & Authorization".

#### Health

| Route | Method | Backend |
|---|---|---|
| `/healthz` | GET | None — returns `{ "ok": true }`. Public; Kubernetes liveness/readiness probes target it. |

#### Auth

| Route | Method | Backend |
|---|---|---|
| `/auth/login` | GET | Redirect to the IdP authorization endpoint (OIDC Authorization Code + PKCE). |
| `/auth/callback` | GET | OIDC callback — validates state + nonce, resolves the role, creates the Redis session. |
| `/auth/logout` | POST | Destroys the Redis session; returns the IdP end-session redirect when advertised. |
| `/auth/me` | GET | Current identity for the SPA; 401 when unauthenticated. |

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

#### Feature flags

| Route | Method | Backend |
|---|---|---|
| `/api/features` | GET | Returns `{ "chatBridgeEnabled": boolean }`, reflecting `CHAT_BRIDGE_ENABLED`. The SPA reads this on load to decide whether to mount the chat panel and open `/ws/chat`. |

#### Chat WebSocket

| Route | Protocol | Description |
|---|---|---|
| `/ws/chat` | WebSocket | Chat bridge. Attached only when `CHAT_BRIDGE_ENABLED=true`; absent by default. Browser upgrades whose `Origin` does not match `AUTH_PUBLIC_URL`'s origin are rejected with HTTP 403 before authentication. The upgrade is additionally operator-only — unauthenticated or non-operator upgrades are rejected with HTTP 401. Each connection spawns one `claude` subprocess. See "Chat Bridge" section for the full message contract. |

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

### Redis (`REDIS_URL`, `AUTH_MODE=oidc` only)

| Operation | Purpose |
|---|---|
| `GET` / `SET ... EX` / `EXPIRE` / `DEL` on `uisession:<id>` | Server-side session records: created at `/auth/callback`, read and TTL-refreshed on every authenticated request and on the `/ws/chat` upgrade, deleted on logout or expiry. Plain keys, not streams. |

### OIDC Identity Provider (`AUTH_OIDC_ISSUER_URL`, `AUTH_MODE=oidc` only)

HTTP(S) to the IdP: issuer discovery at boot (retried with backoff; the process exits if discovery never succeeds) and the token-endpoint exchange during `/auth/callback`. IdP downtime after boot blocks only new logins; active sessions keep working.

Beyond the session keyspace above, `ui-service` reaches backends through gRPC (`state`, `orchestrator`), HTTP (`release-controller`, the IdP), and S3 (log proxy); it neither produces nor consumes Redis Streams.

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
| Login session records | Redis `uisession:<id>` keys (oidc mode) |

## What It Writes

| Data | Target |
|---|---|
| Rerun trigger (reset failed task + downstream) | `state.TriggerRerun` via `POST /api/schedulers/:id/rerun` |
| Rebase trigger (re-execute failed/cancelled tasks + new arrivals against latest topology) | `state.TriggerRebase` via `POST /api/schedulers/:id/rebase` |
| Single-node run trigger (one-task ad-hoc run for a specific dbt node) | `state.TriggerSingleNodeRun` via `POST /api/nodes/:service/:schema/:table/run` |
| Schedule trigger (start full DAG run) | `state.TriggerSchedule` via `POST /api/schedules/:name/trigger` |
| Login session records (create at callback, sliding-TTL refresh per request, delete on logout) | Redis `uisession:<id>` keys (oidc mode) |

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
- Auth shell: `useAuth` bootstraps identity from `GET /auth/me` on load and flips to unauthenticated when any later `/api` call returns 401 (server-side session expiry); the unauthenticated state renders `SignInPage` (which links to `/auth/login`) instead of the app; the authenticated state renders a `UserMenu` (the user's email and a sign-out action via `POST /auth/logout`). The chat panel mounts only for `operator` users (and only when the chat bridge is enabled).
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
- Auth fails closed: a session-store (Redis) error returns 503 `auth_unavailable`; a request is never passed through unauthenticated.
- Kubernetes liveness/readiness probes target `GET /healthz`, which is public and bypasses auth; `GET /` serves the SPA shell.
- `log_s3_key` is stored by `k8s-controller` on task execution records; the UI does not resolve or generate S3 keys itself.
- `ListAllSchedules` reads from `schedule_catalog`; a schedule not in the catalog (e.g. activated before the catalog was populated) will not appear in the dashboard until the catalog is updated.
