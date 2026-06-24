# remediation-agent

## Purpose

`remediation-agent` acts on healable validation failures surfaced by the `remediation` classifier. It consumes `remediation.requested:v1` — one trigger per failing dbt node — fetches the candidate SQL and dbt execution log from S3, assembles a prompt with ranked upstream ancestry, forces a single-shot `propose_fix` tool call against a configured LLM provider, and persists the result. For each successful proposal it enqueues a pointer-only `remediation.proposed:v1` trigger so a downstream approver can review and apply the fix. Every invocation — whether it produces a proposal, is skipped, is escalated, or fails — is recorded in Postgres so no trigger is invisible.

**Runtime**: Go service. HTTP `/healthz` on port 8092. Depends on Postgres (`continuo_remediation_agent`), Redis, S3, and the orchestrator gRPC endpoint (`GetNodeAncestry`, port 50052).

## Owned Storage

Postgres database `continuo_remediation_agent`. Tables:

| Table | Purpose |
|---|---|
| `proposal` | One row per attempt. Records `source`, `release_id`, `node_id`, `error_signature`, `attempt`, `status` (`proposed`, `skipped`, `failed`, `escalated`), `confidence`, `rationale`, `proposed_sql_uri`, `diff_uri`, `model`, and `created_at`. Unique on `(release_id, node_id, attempt)`. A secondary index on `(source, node_id, error_signature)` supports the attempt-count lookup. |
| `remediation_agent_outbox` | Transactional outbox; one row per `remediation.proposed:v1` trigger, drained by the outbox publisher. Status: `pending`, `processed`, `failed`. |
| `message_processing` | Shared shape consumed by `pkg/messageprocessing`; FK target of `remediation_agent_outbox.message_processing_id`. |

The `proposal` table is append-only: all outcomes, including escalations, skips, and LLM failures, are recorded so the full attempt history is queryable.

## Inbound Interfaces

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `remediation.requested:v1` | `remediation-agent-remediation-requested` | Emitted by the `remediation` classifier for each healable failing node. Each message drives one `ProposeFix` invocation. |

## Outbound Interfaces

### Redis producer

| Stream | Consumed by | Emitted when |
|---|---|---|
| `remediation.proposed:v1` | (approval surface) | The LLM returns non-empty proposed SQL for the node. |

All events are written to `remediation_agent_outbox` inside the same transaction as the `proposal` row insert and published with a deterministic `event_id` for consumer-side dedup.

### gRPC calls to `orchestrator`

| Method | Purpose |
|---|---|
| `OrchestratorQuery.GetNodeAncestry` | Fetch ranked upstream ancestors for the failing node. Best-effort: a failure degrades gracefully (no ancestors forwarded to the prompt) rather than failing the invocation. |

Exposes no gRPC services of its own.

## Data Flow

### On `remediation.requested:v1` — per failing node

```
1. Decode trigger: extract source, release_id, node_id, error_signature,
   category, dbt_log_uri, candidate_sql_uri, repo, commit_sha.

2. Count prior attempts for (source, node_id, error_signature).
   - attempts >= MaxAttempts (default 3): insert proposal(status=escalated),
     emit nothing, done.

3. candidate_sql_uri is empty (e.g. seed nodes): insert proposal(status=skipped),
   emit nothing, done.

4. Fetch candidate SQL from S3 at candidate_sql_uri (required; error is transient).
5. Fetch dbt log from S3 at dbt_log_uri.
   - If not found: rawLog = "" (log unavailable path).
   - If transient S3 error: return error (message stays in PEL, retried).
6. Pass rawLog through LogSanitizer → dbtLog.
7. Call orchestrator GetNodeAncestry(node_id) → ancestors (best-effort; degrades
   to empty on error).

8. Assemble ProposeRequest from (node_id, error_signature, candidateSQL, dbtLog,
   repo, commit_sha, ancestors).

9. Forced single-shot LLM tool call:
   - Provider: anthropic (Anthropic API, HTTPS) or openai-compatible (e.g. stub-llm in dev/e2e).
   - The LLM is forced to call the propose_fix tool; no streaming; result is parsed
     from the tool arguments.
   - On transient LLM error: return error (message retried via PEL).

10. ProposedSQL is empty: insert proposal(status=failed), emit nothing, done.

11. Write proposed SQL to S3: proposed-fix/<release_id>/<node_id>/attempt-<attempt>.sql
    Write unified diff to S3: proposed-fix/<release_id>/<node_id>/attempt-<attempt>.diff
    The attempt number is part of the key so a later attempt never overwrites an
    earlier attempt's artifacts that a prior proposal row still references.

12. Open one Postgres transaction:
    a. Claim the inbound message in message_processing (keyed on the Redis
       message id and the upstream outbox_entry_id); if the claim conflicts the
       trigger was already handled, so roll back and ACK without re-proposing.
    b. Insert proposal(status=proposed, confidence, rationale, proposed_sql_uri,
       diff_uri, model).
    c. Enqueue remediation_agent_outbox row (stream=remediation.proposed:v1,
       message_processing_id = the claim row, event_id = deterministic SHA1 UUID
       keyed on release_id+"|"+node_id+"|"+attempt).
13. Commit.
```

### Outbox publisher

A background goroutine drains `remediation_agent_outbox` rows with `status='pending'` and XADDs each to its stream, injecting `outbox_entry_id` for downstream dedup.

## LLM Integration

The `LLMProvider` port is backed by one of three adapters selected at boot via `LLM_PROVIDER`:

| Value | Target | Notes |
|---|---|---|
| `anthropic` | Anthropic API (`https://api.anthropic.com`) | Model from `LLM_MODEL` env var (e.g. `claude-opus-4-8`). |
| `openai` | OpenAI API (`https://api.openai.com`) | Model from `LLM_MODEL`. |
| `openai-compatible` | Operator-supplied endpoint (`LLM_BASE_URL`) | Used for local stub-llm in dev and e2e environments; model from `LLM_MODEL`. |

Each adapter issues a single non-streaming HTTP request with the `propose_fix` tool forced — the LLM must invoke it. The adapter parses the `proposed_sql`, `rationale`, `confidence`, and `suspected_root_cause_node` fields from the tool-call arguments and returns a `ProposeResult`. If the response contains no tool call (or no choices), the adapter returns an error; the handler propagates it so the Redis message is redelivered and retried. If the tool call is present but `proposed_sql` is empty, the adapter returns a zero-value `ProposeResult` without error; the handler detects the empty field and records the attempt as `failed` with no outbox emission.

## LogSanitizer Seam

The `LogSanitizer` port sits between the raw S3 log fetch and the prompt assembly step. The deployed implementation is currently pass-through: it returns the dbt log string unchanged. The seam exists so a redacting implementation can be dropped in without touching the handler or prompt-assembly logic.

## Payload Shape (`remediation.proposed:v1`)

The trigger is pointer-only: it carries no SQL text, no log content, and no warehouse data. Consumers fetch the artifacts from S3 using the supplied URIs.

| Field | Description |
|---|---|
| `event_id` | Deterministic SHA1 UUID keyed on `release_id\|node_id\|attempt`. Stable on redelivery. |
| `source` | Origin pipeline. Currently always `validation`. |
| `release_id` | The release identifier from the inbound trigger. |
| `node_id` | The unique_id of the failing dbt node. |
| `error_signature` | Release-stable normalized dedup key from the classifier (SHA-256 hex). |
| `proposed_sql_uri` | S3 URI of the proposed SQL file (`proposed-fix/<release_id>/<node_id>/attempt-<attempt>.sql`). |
| `diff_uri` | S3 URI of the unified diff (`proposed-fix/<release_id>/<node_id>/attempt-<attempt>.diff`). |
| `rationale` | Short rationale from the LLM (no warehouse data). |
| `confidence` | `low`, `medium`, or `high`. |
| `suspected_root_cause_node` | Optional node_id the LLM identified as the root cause. |
| `model` | The LLM model identifier used for this proposal. |
| `attempt` | Monotonically increasing attempt number for this `(release_id, node_id)`. |
| `proposed_at` | RFC 3339 timestamp of the proposal. |

## Attempt Cap and Escalation

For each `(source, node_id, error_signature)` triple the service enforces a cap (default `REMEDIATION_AGENT_MAX_ATTEMPTS=3`). Before any S3 fetch or LLM call, the handler counts existing `proposal` rows matching the triple. If the count is already at or above the cap, it inserts a `proposal(status=escalated)` row and emits nothing. The trigger is consumed and ACKed; escalation is auditable in the `proposal` table.

## Consumer Reliability

- **Inbound idempotency**: the write transaction first claims the inbound message in `message_processing`, keyed on both the Redis message id and the upstream `outbox_entry_id`. The first key catches a Redis replay (a message redelivered after the work committed but before the ACK); the second catches an outbox republish (the classifier re-emitting the same row with a fresh Redis message id). On either conflict the transaction rolls back and the message is ACKed, so a redelivered trigger produces no second `proposal` row and no second `remediation.proposed` emit. A transient error before commit rolls the claim back with the rest of the work, so the message stays in the PEL for a clean retry. Permanent decode failures (malformed payload) are ACKed by returning nil (not retried).
- **Transactional consistency**: the `message_processing` claim, the `proposal` row insert, and the `remediation_agent_outbox` enqueue are performed in one transaction. The LLM call and S3 writes happen before the transaction opens, so no transaction is held across the external call. A crash between the proposal insert and the outbox enqueue cannot occur — both commit together or not at all.
- **Outbox dedup**: the `remediation.proposed:v1` entry carries a deterministic `event_id` (SHA1 UUID on `release_id|node_id|attempt`) so a redelivered downstream consumer can detect and suppress duplicates.

## Non-Responsibilities

`remediation-agent` generates proposals only. It does not:

- Create GitHub pull requests or open code review branches.
- Auto-apply or merge any proposed SQL change.
- Fetch or parse git repository content.
- Expose a UI or any gRPC service to external callers.
- Track whether a proposal was accepted or resulted in a passing release.

All downstream actions — review, approval, PR creation — belong to services that consume `remediation.proposed:v1`.

## Background Loops

| Loop | Description |
|---|---|
| `remediation.requested:v1` consumer | Dispatches each inbound message to the `ProposeFix` handler. |
| Outbox publisher | Drains `remediation_agent_outbox` and XADDs each pending row to `remediation.proposed:v1`. |

## Key Code Paths

| Concern | Path |
|---|---|
| Proposal entity + unified diff | `remediation-agent/domain/proposal/proposal.go` |
| Prompt assembly | `remediation-agent/domain/prompt/` |
| Event payload + deterministic IDs | `remediation-agent/domain/event/remediation_proposed.go` |
| Application handler | `remediation-agent/service/handlers/propose_fix.go` |
| Port interfaces | `remediation-agent/service/ports/` |
| Postgres UoW + proposal repo | `remediation-agent/adapters/postgres/` |
| S3 evidence reader + artifact writer | `remediation-agent/adapters/s3/` |
| gRPC ancestry client | `remediation-agent/adapters/grpc/ancestry_client.go` |
| Anthropic LLM adapter | `remediation-agent/adapters/llm/anthropic.go` |
| OpenAI-compatible LLM adapter | `remediation-agent/adapters/llm/openai.go` |
| Pass-through log sanitizer | `remediation-agent/adapters/sanitizer/passthrough.go` |
| Redis consumer + outbox publisher | `remediation-agent/adapters/redis/` |
| DB migrations | `db/migration/remediation_agent/V1__init_remediation_agent.sql` |
