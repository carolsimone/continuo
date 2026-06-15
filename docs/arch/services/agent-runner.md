# agent-runner

## Purpose

`agent-runner` is a cluster-internal Go service that runs a provider-agnostic LLM (Large Language Model) tool-use loop on behalf of authenticated operators. It exposes a single gRPC service, `AgentChat`, with a bidirectional streaming `Chat` RPC. Browsers never connect to it directly; `ui-service` relays each operator's `/ws/chat` WebSocket connection onto one `AgentChat.Chat` stream.

It provides:
- a persistent conversation interface: each operator message drives one turn of the LLM tool-use loop
- tool execution via the bundled `continuo` CLI binary (read-only tools run immediately; mutating tools require explicit operator confirmation before execution)
- conversation persistence: threads, messages, and pending tool confirmations are stored in the `continuo_agent` Postgres database
- a background retention job that deletes threads idle past a configurable period and optionally archives each thread to S3 before deletion

**Runtime**: Go (port 50053 gRPC, port 8091 health). Cluster-internal — not exposed outside the service mesh.

## LLM Provider

The LLM provider is operator-configured per deployment. Three provider types are supported, selected by `LLM_PROVIDER`:

| `LLM_PROVIDER` | Backend |
|---|---|
| `anthropic` | Anthropic Messages API (`LLM_API_KEY`, `LLM_MODEL`) |
| `openai` | OpenAI API (`LLM_API_KEY`, `LLM_MODEL`) |
| `openai-compatible` | Any OpenAI-compatible endpoint (`LLM_API_KEY`, `LLM_MODEL`, `LLM_BASE_URL`) |

All provider communication is outbound HTTPS. agent-runner holds no Redis connections and participates in no Redis Streams.

## Tool Execution

At boot, agent-runner runs `continuo describe` and builds its tool catalog from the output. Every tool the LLM may call is derived from the CLI's self-description; adding a CLI command makes it available to the agent without changes to agent-runner.

When the LLM requests a tool call, agent-runner:

1. Validates the tool name against the catalog (unknown tools are rejected).
2. Validates the arguments against the per-command schema (malformed or injected flags are rejected).
3. Classifies the tool as read-only or mutating based on the CLI annotation in `continuo describe`.
4. For read-only tools: spawns the `continuo` binary via direct argv exec (no shell) and streams the result back.
5. For mutating tools: persists a `pending_actions` row in Postgres, emits a `confirm_request` event to the client, and waits. Execution only proceeds on an explicit `confirm_response` with `approved: true`; `approved: false` discards the action.

The `continuo` CLI subprocess reaches `state` (port 50051) and `orchestrator` (port 50052) over their public gRPC interfaces. agent-runner holds no direct connections to those services and imports none of their internals.

## Owned Storage

Postgres database `continuo_agent`:

| Table | Contents |
|---|---|
| `threads` | One row per conversation thread: `id` (UUID PK), `user_id`, `created_at`, `updated_at` |
| `messages` | Full turn history: `id` (UUID PK), `thread_id` FK, `seq`, role (`user` / `assistant` / `tool_call` / `tool_result`), `content` (JSONB), `created_at` |
| `pending_actions` | Tool calls awaiting human confirmation: `id` (UUID PK), `thread_id` FK, `tool`, `args` (JSONB), `summary`, `status` (`pending` / `approved` / `denied` / `expired`), `created_at`, `expires_at` |

The retention job runs on a configurable interval and deletes threads whose `updated_at` is older than `RETENTION_DAYS`. When `RETENTION_ARCHIVE_S3=true`, each thread is written to S3 at `chat-archive/<user_id>/<thread_id>.json` before deletion.

## Inbound Interfaces

### gRPC server (port 50053)

| Service | RPC | Description |
|---|---|---|
| `AgentChat` | `Chat(stream ClientEvent) returns (stream ServerEvent)` | Bidirectional streaming chat. The authenticated `user_id` arrives in the first `Open` event on the request stream (sent by `ui-service` immediately after the stream is established). Each request stream carries `ClientEvent` messages; responses are `ServerEvent` messages. |

#### Health

Port 8091: HTTP health endpoints `/health` (liveness) and `/ready` (readiness); Kubernetes liveness/readiness probes target these paths.

### ClientEvent types (inbound stream)

| Type | Payload | Meaning |
|---|---|---|
| `open` | `user_id`, `thread_id` | MUST be the first event; identifies the user and optionally resumes an existing thread |
| `user_message` | `text` | User turn; drives one iteration of the LLM tool-use loop |
| `new_chat` | — | Start a fresh thread (abandons the current thread context) |
| `confirm_response` | `action_id`, `approved` | Approve (`approved: true`) or deny (`approved: false`) a pending mutating tool call |
| `interrupt` | — | Cancel the in-flight turn |

### ServerEvent types (outbound stream)

| Type | Payload | Meaning |
|---|---|---|
| `thread` | `thread_id` | Thread UUID, emitted immediately after session creation or resume |
| `history` | `messages` | Prior conversation messages on thread resume |
| `tool` | `command` | Human-readable CLI command string for an upcoming tool execution |
| `text` | `text` | Streaming assistant text delta for the current turn |
| `final` | `text` | Complete assistant response; marks the turn as done |
| `confirm_request` | `action_id`, `summary` | Mutating tool pending human confirmation |
| `error` | `code`, `message` | Agent or execution error |

## Outbound Interfaces

### LLM provider (HTTPS egress)

| Provider | Endpoint |
|---|---|
| `anthropic` | `https://api.anthropic.com` (Anthropic Messages API) |
| `openai` | `https://api.openai.com` |
| `openai-compatible` | `LLM_BASE_URL` (operator-supplied) |

### `continuo` CLI subprocess (tool execution)

The `continuo` binary is bundled in the agent-runner container image. It is invoked via direct argv exec (no shell) for each tool call. The subprocess inherits `CONTINUO_STATE_ADDR` and `CONTINUO_ORCHESTRATOR_ADDR` from agent-runner's environment.

| Subprocess call | Purpose |
|---|---|
| `continuo describe` | Boot-time catalog build |
| `continuo <tool> <args...>` | Per-tool-call execution |

### S3 (optional)

| Operation | Key pattern | Trigger |
|---|---|---|
| `PutObject` | `chat-archive/<user_id>/<thread_id>.json` | Retention job, when `RETENTION_ARCHIVE_S3=true` |

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `LLM_PROVIDER` | required | `anthropic`, `openai`, or `openai-compatible` |
| `LLM_API_KEY` | required | API key for the LLM provider |
| `LLM_MODEL` | required | Model identifier (e.g. `claude-opus-4-5`, `gpt-4o`) |
| `LLM_BASE_URL` | required (`openai-compatible`) | Base URL for the OpenAI-compatible endpoint |
| `POSTGRES_DSN` | required | Connection string for `continuo_agent` |
| `GRPC_PORT` | `50053` | gRPC listen port |
| `HEALTH_PORT` | `8091` | HTTP health listen port |
| `CONTINUO_STATE_ADDR` | required | gRPC address of the `state` service (forwarded to CLI subprocess) |
| `CONTINUO_ORCHESTRATOR_ADDR` | required | gRPC address of the `orchestrator` service (forwarded to CLI subprocess) |
| `RETENTION_DAYS` | `30` | Threads idle longer than this are deleted by the retention job |
| `RETENTION_ARCHIVE_S3` | `false` | When `true`, each thread is archived to S3 (`S3_BUCKET`) before deletion |

## Dependencies

| Dependency | How reached |
|---|---|
| `state` | Via `continuo` CLI subprocess over public gRPC (port 50051) |
| `orchestrator` | Via `continuo` CLI subprocess over public gRPC (port 50052) |
| LLM provider | HTTPS egress |
| `continuo_agent` Postgres | Direct connection via `POSTGRES_DSN` |
| S3 | AWS SDK `PutObject` (optional, retention-job only) |

agent-runner holds no direct gRPC stubs for `state` or `orchestrator` and imports none of their source packages. All system reads happen through the `continuo` CLI subprocess, which uses only those services' public interfaces.

## Reliability Notes

- The gRPC `AgentChat.Chat` stream is held open for the duration of a WebSocket connection. If the stream is interrupted, `ui-service` reconnects and the client resumes the thread; on reconnect the agent replays the stored message history.
- A pending tool confirmation can be resumed across a reconnect, but is never executed off a stale confirmation. When the stream drops while awaiting approval, the `pending_actions` row is left `pending` (not expired) and the tool call is persisted without a result. On thread resume, if a still-`pending` action whose `ConfirmTTL` window has not passed exists, agent-runner re-emits its `confirm_request`; an approval arriving outside an in-flight turn then runs the tool, persists the result, and continues the turn, while a denial records the refusal. The mutating tool still runs only on a fresh, live, explicit approval — the resume just re-offers the confirmation instead of forcing the user to re-ask. An in-session interrupt (as opposed to a disconnect) still resolves the action as `expired`, and once the `ConfirmTTL` window passes a disconnected action is no longer resumable.
- Missing `LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL`, `POSTGRES_DSN`, `CONTINUO_STATE_ADDR`, or `CONTINUO_ORCHESTRATOR_ADDR` cause the process to exit before accepting any traffic (`pkg/config.Validator`).
- LLM provider errors are returned to the client as `error` ServerEvents; they do not crash the stream.
