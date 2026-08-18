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
- single-node run triggering: proxies `POST /api/nodes/:service/:schema/:table/run` to the `TriggerSingleNodeRun` gRPC method on `state`, carrying the caller's chosen run/test/build `operation`
- schedule triggering: proxies `POST /api/schedules/:name/trigger` to the `TriggerSchedule` gRPC method on `state`, carrying the caller's chosen run/test/build `operation`
- per-node topology metadata: proxies `GET /api/nodes/:service/:schema/:table/meta` to the `GetNode` gRPC method on `orchestrator`, used to decide whether a node's single-node `test` operation is meaningful
- a **Remediation** tab (5th dashboard tab): lists fix proposals from `remediation-agent` with diff, rationale, confidence, and source lineage; `operator` users can click **Create PR** to open a GitHub pull request applying the corrected real-source SQL; `viewer` users see the surface read-only. ui-service holds the GitHub App write credential, reads the corrected source from S3, and records the PR result back to remediation-agent over gRPC. Each proposal's `pr_state` is rendered as a colored chip once it reaches a terminal outcome — `merged` or `rejected` — mirrored from GitHub by remediation-agent's PR-outcome reconciler; non-terminal `pr_state` values render as plain text. The release detail page carries a lightweight back-link to any associated proposal.
- a chat panel backed by `/ws/chat` (enabled only when `CHAT_BRIDGE_ENABLED=true`): an operator-only WebSocket (WS) endpoint that relays browser messages over a bidirectional gRPC stream to `agent-runner`, which runs the LLM (Large Language Model) tool-use loop; the WebSocket upgrade is operator-only and the endpoint is gated off by default

Every mutation proxied to `state` (trigger, cancel, rerun, rebase, single-node run) forwards the authenticated user's id as the `x-continuo-user-id` gRPC metadata header so `state` and `orchestrator` record the initiating user for full provenance. The header key and the `userMetadata(req)` helper live in `src/server/grpc-client.ts`; the key value matches `pkg/identity.MetadataKey` on the Go side. An unauthenticated request records the `system` sentinel.

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
- Helm (`deploy/continuo`) runs `AUTH_MODE=oidc`. Per-deployment identity settings (issuer URL, client ID, public URL, operator/viewer email allowlists, role mapping) come from `auth.*`, which the deployment template expands into the `AUTH_*` env via sentinels; the committed defaults are empty so a deploy fails closed until they are set in the on-box secret values file. The client secret lands in the chart credentials Secret via `auth.oidcClientSecret`, and `REDIS_URL` expands from `__REDIS_URL_FROM_AUTH__`. Kubernetes probes target `/healthz`.

Operator setup, including configuring an identity provider and a no-domain Google loopback flow for testing, is documented in `deploy/AUTH.md`.

## Chat Relay

### Overview

`ui-service` exposes a `/ws/chat` WebSocket (WS) endpoint that is attached to its HTTP server only when the environment variable `CHAT_BRIDGE_ENABLED=true` is set. The endpoint is OFF by default, including in the production image. The same flag is surfaced to the browser via `GET /api/features` (`{ "chatBridgeEnabled": boolean }`); the client reads it on load and only mounts the chat panel — and only opens the `/ws/chat` socket — when the feature is enabled.

The endpoint upgrade is authenticated: only a session with the `operator` role may open `/ws/chat`; anything else is rejected with HTTP 401 before the WebSocket is established (audited as `ws_denied`). Browser upgrade requests whose `Origin` header does not match the deployment origin (derived from `AUTH_PUBLIC_URL`) are rejected with HTTP 403 before authentication is attempted, preventing cross-site WebSocket hijacking. Requests with no `Origin` header (non-browser clients) are not subject to this check. The origin check is skipped in `dev` mode.

Each incoming WebSocket connection is relayed 1:1 onto a bidirectional gRPC `AgentChat.Chat` stream to `agent-runner` (`AGENT_RUNNER_GRPC_ADDR`, default `localhost:50053`). `ui-service` forwards the authenticated `user_id` in the first `Open` event written to the gRPC stream (carrying `user_id` and the optional `thread_id` query parameter). The browser-to-ui-service leg is WebSocket (JSON frames); the ui-service-to-agent-runner leg is gRPC bidi streaming. `ui-service` performs no LLM calls and holds no agent state; it is a transport relay.

### Message contract

**Client → server messages** (JSON over WebSocket, relayed as `ClientEvent` proto to agent-runner):

| `type` | Payload | Meaning |
|---|---|---|
| `user_message` | `{ "text": string }` | User turn forwarded to the agent loop in agent-runner |
| `new_chat` | `{}` | Start a fresh conversation thread |
| `confirm_response` | `{ "actionId": string, "approved": boolean }` | Approve (`true`) or deny (`false`) a pending mutating tool call |
| `interrupt` | `{}` | Cancel the in-flight turn |

Incoming frames are validated before use: anything that is not valid JSON, or that decodes to a non-object (e.g. `null`, a number, an array), is dropped silently.

**Server → client messages** (JSON over WebSocket, translated from `ServerEvent` proto from agent-runner):

| `type` | Payload | Meaning |
|---|---|---|
| `thread` | `{ "threadId": string }` | Thread UUID, emitted after session creation or resume |
| `history` | `{ "messages": array }` | Prior conversation history on thread resume |
| `tool` | `{ "command": string }` | Human-readable CLI command string for an upcoming tool execution |
| `text` | `{ "text": string }` | Streaming agent text delta for the current turn |
| `final` | `{ "text": string }` | Complete agent response, marking the turn as done |
| `confirm_request` | `{ "actionId": string, "summary": string }` | Mutating tool pending human confirmation |
| `error` | `{ "code": string, "message": string }` | Agent or relay error |

### Scope and constraints

`ui-service` is a pure transport relay for chat. All agent logic, tool execution, LLM calls, confirmation gating, and conversation persistence are owned by `agent-runner`. `ui-service` introduces no new storage, no Redis Streams, and no direct connections to `state`, `orchestrator`, or LLM providers as a result of the chat relay.

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
| `/api/schedules/:name/trigger` | POST | `TriggerSchedule` → state gRPC. Body `{ "operation": "" \| "run" \| "test" \| "build" }` (default `""`, meaning `run`); `"run"` is normalized to `""` on the wire. An unrecognized value returns 400. |

#### Topology API

| Route | Method | Backend |
|---|---|---|
| `/api/topology/schedules` | GET | `ListScheduleTopologies` → orchestrator gRPC. Returns one entry per schedule with at least one active `:Table`: `{schedule_name, node_count, last_updated_at}`. Backs the homepage `Topology` tab tile grid. |

#### Run / scheduler API

| Route | Method | Backend |
|---|---|---|
| `/api/runs/:run_id/graph` | GET | `GetRunGraph` → orchestrator gRPC. Returns the run's nodes/edges plus `run_topology_generation` + `latest_topology_generation`. Powers drift display on both the schedules dashboard cards and the schedule detail header. |
| `/api/schedulers/:id` | GET | `GetScheduler` → state gRPC |
| `/api/schedulers/:id/tasks` | GET | `ListTasks` → state gRPC. Paged via `?limit` (default 200, max 500) and `?offset`. Responds `{ total_count, tasks }`. |
| `/api/schedulers/:id/executions` | GET | `ListTaskExecutions` → state gRPC. Paged via `?limit` (default 200, max 200, matching state's `maxTaskExecutionsPageSize`) and `?offset`. Responds `{ total_count, executions }`. |
| `/api/schedulers/:id/rerun` | POST | `TriggerRerun` → state gRPC |
| `/api/schedulers/:id/rebase` | POST | `TriggerRebase` → state gRPC. Body is ignored; the run ID from the URL is used as `source_run_id`. |

#### Node API

| Route | Method | Backend |
|---|---|---|
| `/api/nodes/:service/:schema/:table/runs` | GET | `ListNodeRuns` → state gRPC. Returns the last 50 task instances that executed on the node, most recent first. |
| `/api/nodes/:service/:schema/:table/run` | POST | `TriggerSingleNodeRun` → state gRPC. Body `{}` → `metadata_source=latest`; body `{"source_run_id": "<uuid>"}` → `metadata_source=snapshot_of_run`. Body also accepts `operation` (`"" \| "run" \| "test" \| "build"`, default `""`/`run`; `"run"` normalizes to `""`); an unrecognized value returns 400. |
| `/api/nodes/:service/:schema/:table/meta` | GET | `GetNode` → orchestrator gRPC. Returns `{ node_type, test_count, test_count_known }` for the node. `NodeDetailPage` reads this to disable the single-node `test` operation when `test_count_known` is `true` and `test_count` is `0`. |

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

#### Remediation API

| Route | Method | Auth | Backend |
|---|---|---|---|
| `/api/remediation/proposals` | GET | authenticated | `ListProposals` → remediation-agent gRPC. Returns proposals ordered `created_at DESC`. Supports `status` and `pr_state` filter query params for the inbox view. |
| `/api/remediation/proposals/:id` | GET | authenticated | `GetProposal` → remediation-agent gRPC. |
| `/api/remediation/proposals/:id/pull-request` | POST | `operator` | `BeginPullRequest` → claim; S3 read of `proposed_sql_uri`; GitHub App: create branch + commit file + open PR; `RecordPullRequest` on success or `FailPullRequest` on GitHub error. Returns `{ pr_url }`. Returns 409 when the proposal is already `opening`/`open` (with existing `pr_url`). Returns 422 when `source_resolved=false`. |

The Create PR route requires the GitHub App to be provisioned (`GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY`, `GITHUB_APP_INSTALLATION_ID`); without them it returns a clear configuration-error response while the read/list paths remain functional.

#### Chat WebSocket

| Route | Protocol | Description |
|---|---|---|
| `/ws/chat` | WebSocket | Chat relay. Attached only when `CHAT_BRIDGE_ENABLED=true`; absent by default. Browser upgrades whose `Origin` does not match `AUTH_PUBLIC_URL`'s origin are rejected with HTTP 403 before authentication. The upgrade is additionally operator-only — unauthenticated or non-operator upgrades are rejected with HTTP 401. Each connection is relayed 1:1 onto a bidirectional gRPC `AgentChat.Chat` stream to `agent-runner`. See "Chat Relay" section for the full message contract. |

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
| `GetNode` | `GET /api/nodes/:service/:schema/:table/meta` |

### gRPC to `agent-runner` (`AGENT_RUNNER_GRPC_ADDR`, default `localhost:50053`)

| Method | Trigger |
|---|---|
| `AgentChat.Chat` (bidirectional streaming) | `/ws/chat` WebSocket connection (operator-only, `CHAT_BRIDGE_ENABLED=true`) |

Each WebSocket connection opens one bidirectional `AgentChat.Chat` gRPC stream. The authenticated `user_id` is forwarded in the first `Open` event written to the stream (alongside the optional `thread_id` taken from the WebSocket URL query parameter). WebSocket frames are translated to `ClientEvent` proto messages on the request side; `ServerEvent` proto messages are translated back to JSON WebSocket frames on the response side.

### gRPC to `remediation-agent` (`REMEDIATION_AGENT_GRPC_ADDR`, default `localhost:50054`)

| Method | Route that calls it |
|---|---|
| `ListProposals` | `GET /api/remediation/proposals` |
| `GetProposal` | `GET /api/remediation/proposals/:id` |
| `BeginPullRequest` | `POST /api/remediation/proposals/:id/pull-request` (claim step) |
| `RecordPullRequest` | `POST /api/remediation/proposals/:id/pull-request` (after GitHub PR created) |
| `FailPullRequest` | `POST /api/remediation/proposals/:id/pull-request` (on GitHub error, to reset `pr_state` to `failed`) |

### GitHub App (write credential, minted per-request)

ui-service holds the GitHub App ID, private key, and installation ID (`GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY`, `GITHUB_APP_INSTALLATION_ID`). For each Create PR request it mints a short-lived (~1h) installation token and performs:

1. Fetch `main` branch HEAD SHA (to base the new branch on).
2. Create branch `remediation/<release_id>/<node_id>-attempt<n>` (deterministic; a 422 "Reference already exists" is treated as idempotent).
3. Create or update `file_path` with the corrected source SQL read from S3.
4. Open a PR (`base=main`, `head=<branch>`). A 422 "PR already exists for head" is handled by looking up and returning the existing PR.

The App is installed on the single dbt-demo repo only (`contents:write` + `pull-requests:write`). It never calls merge or delete APIs and never targets `main` directly. `main` branch protection (require PR + review) is the final gate.

Implemented via the `PullRequestCreator` interface in `ui-service/src/server/github/pull-request-creator.ts`.

Before use, `GITHUB_APP_PRIVATE_KEY` is passed through `normalizePemPrivateKey` (`ui-service/src/server/github/private-key.ts`), which reconstructs a well-formed PEM regardless of how its line breaks arrived — real newlines, `\n` escapes, spaces, CRLF, or a mix. Some encodings fold every line break into a space with no parse error at all (a quoted rather than block-scalar YAML value is the common cause), producing a key whose material is intact but that no PEM parser will accept. `resolveGithubAppPullRequestCreator` then runs a startup signing check (`crypto.createSign('RSA-SHA256')` over a dummy payload, no network) on the normalized key before constructing the Octokit client. A key that fails this check is treated as unconfigured for the route's purposes (`prCreator` stays undefined, the route reports 503) but is logged as a startup error naming the env var and the likely YAML cause — distinct from a deployment where the App is intentionally not set up, which logs nothing. On a GitHub API failure during PR creation, the route logs the proposal id and the error's `status`/`message` (never the raw error object, which for Octokit errors can carry the Authorization header used to mint the installation token) before calling `FailPullRequest`.

The private key reaches the container differently per environment. In Kubernetes it comes from the `githubAppPrivateKey` secret reference in `deploy/continuo/values.yaml`. Under docker compose it is read from `${GITHUB_APP_PRIVATE_KEY}`, which `scripts/ensure-dev-env.sh` populates in a git-ignored `.env` with a freshly generated throwaway RSA key. A generated key is required rather than a blank or fake value because the startup signing check above rejects anything that is not a valid, well-formed PEM before the Octokit client is ever constructed, which would leave `prCreator` undefined and the Create PR route reporting itself as unconfigured. The generated key authenticates nothing: local runs point `GITHUB_API_BASE_URL` at the `stub-github` service.

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

Beyond the session keyspace above, `ui-service` reaches backends through gRPC (`state`, `orchestrator`, `agent-runner`), HTTP (`release-controller`, the IdP), and S3 (log proxy); it neither produces nor consumes Redis Streams.

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
| Per-node topology metadata (`node_type`, `test_count`, `test_count_known`) | `orchestrator.GetNode` |
| Paginated release history | `release-controller GET /releases` |
| Release detail (per-node validation results) | `release-controller GET /releases/{id}` |
| Current production release + topology snapshot | `release-controller GET /current-prod` |
| Pod logs | S3 (via `log_s3_key` from task execution records) |
| dbt validation logs | S3 (via `dbt_log_uri` from per-node validation results) |
| Remediation proposals (list + detail, incl. PR state) | `remediation-agent.ListProposals` / `GetProposal` |
| Corrected real-source SQL (for PR creation) | S3 (`proposed_sql_uri` → `.source.sql`) |
| Login session records | Redis `uisession:<id>` keys (oidc mode) |

## What It Writes

| Data | Target |
|---|---|
| Rerun trigger (reset failed task + downstream) | `state.TriggerRerun` via `POST /api/schedulers/:id/rerun` |
| Rebase trigger (re-execute failed/cancelled tasks + new arrivals against latest topology) | `state.TriggerRebase` via `POST /api/schedulers/:id/rebase` |
| Single-node run trigger (one-task ad-hoc run for a specific dbt node, carrying the selected `operation`) | `state.TriggerSingleNodeRun` via `POST /api/nodes/:service/:schema/:table/run` |
| Schedule trigger (start full DAG run, carrying the selected `operation`) | `state.TriggerSchedule` via `POST /api/schedules/:name/trigger` |
| PR claim (BeginPullRequest), PR record (RecordPullRequest), PR failure reset (FailPullRequest) | `remediation-agent` gRPC (operator-only) |
| GitHub pull request (branch + file commit + PR open) | GitHub App API (operator-only, single dbt-demo repo) |
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
- `DashboardPage`: five URL-routed tabs under the page header — `Runs` (default, `/?tab=runs`) shows the `SchedulerCard` list, `Topology` (`/?tab=topology`) shows the `SnapshotTile` grid, `Releases` (`/?tab=releases`) shows `ReleasesPanel`, `Nodes` (`/?tab=nodes`), and `Remediation` (`/?tab=remediation`) shows the `RemediationPage` proposal inbox. Schedule and topology data sources poll every 5 seconds regardless of active tab: `/api/schedules` feeds the `Runs` tab and the `Runs` count pill; `/api/topology/schedules` feeds the `Topology` tab and its count pill. Each snapshot tile navigates to `/schedule/:name/latest`. The `Remediation` tab carries a count badge showing the number of proposals awaiting a human (`status='proposed'` AND `pr_state IN ('', 'failed')`).
- `ReleasesPanel`: displays the live `current_prod` release, the first in-flight candidate (parsing or validating), and a paginated release history list with status filtering. Fetches `/api/releases/current-prod` and `/api/releases` (with optional `status` and `cursor` params); supports load-more pagination via `next_cursor`. Each release row links to `ReleaseDetailPage`. The history table carries a `Reason` column: a rejected release shows its `reject_reason` as the humanized failed-stage label (Compilation / Seed build / Validation); every other row renders a muted `—`.
- `ReleaseDetailPage`: full detail view for a single release at `/releases/:id`. Fetches `/api/releases/:id` and, while the release status is non-terminal (anything other than `promoted`/`rejected`/`superseded`), self-schedules another fetch every 5s so the per-node validation results table — which the backend projects incrementally during `validating` — grows live without a manual refresh; polling stops as soon as a terminal status is observed. Shows status, bootstrap flag, and — for a rejected release — an error strip carrying the humanized failed-stage reject reason (matching the `Releases` list) with the persisted `reject_detail` appended after an em dash when the release row carries one (empty for a rejection whose reject path supplied no detail, and for every release row written before the `reject_detail` column existed). Also shows the per-node validation results table grouped by stage (Compilation/Seed/Validation). Each row with a `dbt_log_uri` includes an inline log viewer that fetches `/api/releases/log?key=<uri>` on demand, and a status-aware FIX cell (see below) that surfaces the "Generating fix…" chip and then the "Proposed fix available →" link for a healable failed node.
- `SchedulerCard`: displays schedule name, running status, cron expression, last run time and progress; polls `/api/schedulers/:last_run_id/tasks` for task progress, walking every page via `fetchAllPages` so the progress total reflects the whole run rather than the first page, and `/api/runs/:last_run_id/graph` for topology-drift information (both every 5 s); shows a warning strip when the last run's `run_topology_generation` is older than the orchestrator's `latest_topology_generation`, matching the drift logic used on the schedule detail page; includes an Operation `<select>` (Run / Test / Build, default Run) next to a verb-aware trigger button ("Trigger run" / "Trigger test" / "Trigger build") that starts a whole-DAG run in the selected operation (button disabled while a run is active), plus a "Cancel" button while a run is in flight
- `DetailPage`: two-column layout — left column shows the `Dependency Graph` (`DAGPanel`). Right column branches on mode. In `/schedule/:name` the column header is a panel-level tab bar with two URL-routed tabs (`Nodes` default, `Past Runs` via `?panel=runs`). In `/schedule/:name/latest` the column is a single `.detail-card` with a `.section-header` titled `Past Runs` followed by `PastRunsPanel` — the panel tab strip is omitted because only one panel remains, per `.claude/design-guideline/ui.md`. Includes Rerun and Rebase buttons for terminal runs with drift badge when topology generation differs.
- **Service-level grouping** (run mode only): both panels default to service granularity, sharing one `expandedServices` set owned by `DetailPage`. The graph collapses each service (the first segment of `{service}.{schema}.{table}` node ids) into a single vertex carrying a rolled-up status (`failed` > `running` > `pending` > `skipped` > `succeeded` > `cancelled`) and a node count; the nodes table groups rows under collapsible per-service headers with the same rollup. Clicking a service vertex or a group header toggles that service in both panels; selecting a node (graph click, table click, or search) auto-expands its service; collapsing the selected node's service clears the selection; clearing the search box also clears the selection, while the graph card mounting does not — a node selected in the `Nodes` table while the graph is still loading stays selected once the graph appears. Cross-service model dependencies are projected onto service vertices and deduped; intra-service edges between two collapsed endpoints are dropped as self-loops. The service-level projection may be cyclic even though the model graph is a DAG — dagre lays it out by internally reversing back-edges, so cycles render rather than crash. Each service gets a stable accent color (assigned by sorted service name in `service-helpers.ts`) shown identically on graph vertices, expanded model nodes (left border), and table group headers. A graph with a single service always renders expanded. Latest mode keeps the flat catalog canvas and is not grouped.
- `DAGPanel`: renders graph topology using run graph or schedule graph; in service view it renders the mixed graph of collapsed service vertices and expanded model nodes described above; the canvas auto-fits the whole graph the first time it renders, and thereafter whenever the graph's node set changes, the selection is cleared, or the container resizes — each with nothing searched or selected, since an active search or selection suppresses every auto-fit after the first so the view is not pulled away from what the user is looking at
- **Manual node placement**: node positions come from a dagre layout recomputed whenever the topology, task statuses or selection change — which includes every 5-second graph poll in latest mode. Positions the operator drags a node (or service vertex) to are held in `DAGPanel` state keyed by canvas node id and overlaid on each recomputed layout, so an arrangement built to isolate a few services is not undone by the next poll; nodes that were never dragged keep following the automatic layout. The overrides live for as long as the panel is mounted and are not persisted, so a reload starts from the computed layout. A `Reset layout` button appears in the search strip once at least one node has been moved; it drops every override and re-fits the view, unconditionally — an explicit reset is not subject to the search/selection auto-fit suppression above.
- `PastRunsPanel`: lists historical runs from `orchestrator.ListRuns`
- `NodeDetailPage`: per-node detail page; fetches recent run history via `GET /api/nodes/:service/:schema/:table/runs` and node metadata via `GET /api/nodes/:service/:schema/:table/meta`; exposes an Operation `<select>` (Run / Test / Build, default Run) alongside a "Trigger run" control that opens `RunSourcePickerDialog` to select between latest metadata and a pinned source run. The `test` option is disabled, with an inline info strip explaining why, when `/meta` reports `test_count_known=true` and `test_count=0`; selecting `test` on such a node falls back to `run` automatically. Whole-DAG concerns don't apply here — the gate is per-node only.
- `RunSourcePickerDialog`: modal for choosing `metadata_source` (`latest` or `snapshot_of_run`) before calling `POST /api/nodes/:service/:schema/:table/run` with the page's selected `operation`
- `RemediationPage`: the Remediation tab surface. Lists all proposals from `/api/remediation/proposals`, each row expandable to a detail card showing diff, rationale, confidence, `source_resolved`, and origin (release + node). `operator` users see an active **Create PR** button (disabled with tooltip when `source_resolved=false`); `viewer` users see the surface read-only. Clicking Create PR calls `POST /api/remediation/proposals/:id/pull-request` and displays the returned `pr_url` as a link. A proposal's `pr_state` renders as a `pr-chip` badge once it reaches the terminal `merged` or `rejected` outcome (`ProposalDTO.pr_closed_at` carries the RFC 3339 close timestamp); any other `pr_state` value renders as plain text. `ReleaseDetailPage`'s per-node FIX cell is status-aware, driven by polling `/api/remediation/proposals` every 5s. Each failed node is bucketed by its proposals' `status`: a `generating` row (fix in flight) renders a disabled "Generating fix…" chip; a `proposed` row renders the "Proposed fix available →" back-link (which dominates a concurrent generating row from an earlier attempt); a node with only terminal-but-blank outcomes (`skipped`/`failed`/`escalated`) or no proposal renders nothing. Polling stops once every failed node has a ready `proposed` fix and is capped at 3 minutes so unhealable failures do not poll indefinitely. This lets the chip and then the link surface without a manual refresh: the disabled chip appears while remediation-agent generates the fix and swaps to the link as soon as the proposal is persisted.

## Reliability Notes

- Mostly read-only; write-side effects are `TriggerRerun` (via `POST /api/schedulers/:id/rerun`), `TriggerRebase` (via `POST /api/schedulers/:id/rebase`), `TriggerSingleNodeRun` (via `POST /api/nodes/:service/:schema/:table/run`), and `TriggerSchedule` (via `POST /api/schedules/:name/trigger`). All trigger calls delegate atomicity and error semantics to `state`. The Create PR route (`POST /api/remediation/proposals/:id/pull-request`) is the only path that calls an external write API (GitHub); on GitHub error the handler calls `FailPullRequest` on remediation-agent to reset `pr_state` to `failed` before returning an error to the browser.
- gRPC errors are surfaced as HTTP 500 with the gRPC error message.
- S3 errors are surfaced as HTTP 502.
- Auth fails closed: a session-store (Redis) error returns 503 `auth_unavailable`; a request is never passed through unauthenticated.
- Kubernetes liveness/readiness probes target `GET /healthz`, which is public and bypasses auth; `GET /` serves the SPA shell.
- `log_s3_key` is stored by `k8s-controller` on task execution records; the UI does not resolve or generate S3 keys itself.
- `ListAllSchedules` reads from `schedule_catalog`; a schedule not in the catalog (e.g. activated before the catalog was populated) will not appear in the dashboard until the catalog is updated.
