# remediation-agent

## Purpose

`remediation-agent` acts on healable failures surfaced by the `remediation` classifier, across all three failure sources: `validation`, `compile`, and `seed_build`. It consumes `remediation.requested:v1` — one trigger per failing dbt node — and produces a fix proposal. Validation failures carry a pre-compiled candidate SQL and use a two-step LLM flow (candidate diagnosis, then real-source fix). Compile and seed_build failures carry no candidate SQL; the agent reads the offending source file directly and fixes it in a single LLM step from the dbt error and the real source. For each successful proposal it enqueues a pointer-only `remediation.proposed:v1` trigger so a downstream approver can review and apply the fix. Every invocation — whether it produces a proposal, is skipped, is escalated, or fails — is recorded in Postgres so no trigger is invisible. The agent never writes to or creates branches in any git repository; proposal application is a human action.

**Runtime**: Go service. HTTP `/healthz` on port 8092. gRPC `RemediationProposals` server on port 50054. Depends on Postgres (`continuo_remediation_agent`), Redis, S3, the orchestrator gRPC endpoint (`GetNodeAncestry`, port 50052), and the GitHub Contents API (read-only).

## Owned Storage

Postgres database `continuo_remediation_agent`. Tables:

| Table | Purpose |
|---|---|
| `proposal` | One row per attempt. Records `source`, `release_id`, `node_id`, `error_signature`, `attempt`, `status` (`proposed`, `skipped`, `failed`, `escalated`), `confidence`, `rationale`, `proposed_sql_uri`, `diff_uri`, `candidate_fix_sql_uri`, `candidate_fix_diff_uri`, `source_resolved`, `model`, `created_at`, and source-location columns (`repo`, `commit_sha`, `file_path` — populated when `source_resolved=true`). PR-tracking columns: `pr_url`, `pr_number`, `pr_state` (lifecycle: `'' → opening → open` or `failed`), `pr_opened_at`, `pr_opened_by`. Unique on `(release_id, node_id, attempt)`. A secondary index on `(source, node_id, error_signature)` supports the attempt-count lookup. |
| `remediation_agent_outbox` | Transactional outbox; one row per `remediation.proposed:v1` trigger, drained by the outbox publisher. Status: `pending`, `processed`, `failed`. |
| `message_processing` | Shared shape consumed by `pkg/messageprocessing`; FK target of `remediation_agent_outbox.message_processing_id`. |

The `proposal` table is append-only: all outcomes, including escalations, skips, and LLM failures, are recorded so the full attempt history is queryable.

## Inbound Interfaces

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `remediation.requested:v1` | `remediation-agent-remediation-requested` | Emitted by the `remediation` classifier for each healable failing node. Each message drives one `ProposeFix` invocation. |

### gRPC server — `RemediationProposals` (port 50054)

Exposes proposal data and the PR lifecycle to ui-service. Handlers are thin and delegate to application services; all persistence goes through the `ProposalRepository` port.

| Method | Description |
|---|---|
| `ListProposals(filter)` | Returns proposals ordered `created_at DESC`, all fields including `pr_*`. Supports filtering by `status` and/or `pr_state` (inbox view: `status='proposed'` AND `pr_state IN ('', 'failed')`). |
| `GetProposal(id)` | Returns a single proposal. Returns `NOT_FOUND` when the id is unknown. |
| `BeginPullRequest(id)` | Atomic compare-and-set: transitions `pr_state` from `''` or `'failed'` to `'opening'`, allowed only when `source_resolved=true`. Returns `{repo, commit_sha, file_path, proposed_sql_uri, branch_name, release_id, node_id, rationale, confidence, diff_uri, model}` on success. Returns `FAILED_PRECONDITION` with the existing `pr_url` when the proposal is already `opening` or `open`; also returns `FAILED_PRECONDITION` when `source_resolved=false`. This is the single-winner idempotency guard that prevents concurrent duplicate PRs. |
| `RecordPullRequest(id, pr_url, pr_number, opened_by)` | Sets `pr_state='open'`, `pr_url`, `pr_number`, `pr_opened_at=now()`, `pr_opened_by`. Emits `remediation.pr_opened:v1` via the transactional outbox. |
| `FailPullRequest(id)` | Resets `pr_state` from `'opening'` to `'failed'`, making the action retryable. Called by ui-service when the GitHub step errors after a successful `BeginPullRequest` claim. |

## Outbound Interfaces

### Redis producer

| Stream | Consumed by | Emitted when |
|---|---|---|
| `remediation.proposed:v1` | (approval surface) | The LLM returns non-empty proposed SQL for the node. |
| `remediation.pr_opened:v1` | (no consumer; audit seam for future close-loop) | `RecordPullRequest` is called; payload is pointer-only: `proposal_id`, `release_id`, `node_id`, `pr_url`, `pr_number`, `opened_by`, `opened_at`. |

All events are written to `remediation_agent_outbox` inside the same transaction as the `proposal` row insert and published with a deterministic `event_id` for consumer-side dedup.

### gRPC calls to `orchestrator`

| Method | Purpose |
|---|---|
| `OrchestratorQuery.GetNodeAncestry` | Called twice per successful proposal. First call (depth > 0): fetch ranked upstream ancestors for the prompt; best-effort, degrades to empty list on failure. Second call (depth 0, self-node): extract `file_path` for the failing node so Step 2 can locate the real model source; also best-effort. |

Its own inbound gRPC surface (`RemediationProposals`) is described in the inbound interfaces section above.

### Outbound HTTP — GitHub Contents API

| Endpoint | Purpose |
|---|---|
| `GET /repos/{repo}/contents/{path}?ref={commit_sha}` | Read-only fetch of the real dbt model source file at the release commit. `Accept: application/vnd.github.raw+json`. Authenticated with `Authorization: Bearer <GITHUB_TOKEN>` when the token is set; unauthenticated otherwise. 404 → `ErrSourceNotFound` (degrades to candidate proposal). Any non-2xx or network error also degrades gracefully. |

The `{path}` is formed by joining the service's `repo_path` from `service_repos.yaml` with the dbt node's `original_file_path` from the orchestrator topology. The `{repo}` comes from the `remediation.requested:v1` trigger payload. No write requests are issued. The agent holds no GitHub write permissions.

`GITHUB_TOKEN` is injected at deploy time from the chart-managed secret `continuo-app-credentials` (key `GITHUB_TOKEN`, sourced from `global.github.token` in Helm values). No out-of-band secret mechanism is used.

## Data Flow

### On `remediation.requested:v1` — per failing node

```
1. Decode trigger: extract source, release_id, node_id, error_signature,
   category, dbt_log_uri, candidate_sql_uri, file_path, repo, commit_sha.

2. Count prior attempts for (source, node_id, error_signature).
   - attempts >= MaxAttempts (default 3): insert proposal(status=escalated),
     emit nothing, done.

3. source is compile or seed_build: run the source-only branch (below) and done.
   These sources carry no candidate SQL.

4. source is validation and candidate_sql_uri is empty: insert
   proposal(status=skipped), emit nothing, done.

── Source-only branch (compile / seed_build) ──────────────────────────────────

S1. Fetch dbt log from S3 at dbt_log_uri (not-found → ""; transient error → retry),
    sanitize via LogSanitizer.
S2. Resolve the project-relative source path and owning service:
    - compile: file_path is on the trigger; service_name = node_id (the synthetic
      service id). Ancestry is bypassed.
    - seed_build: file_path empty → resolve via orchestrator GetNodeAncestry(node_id)
      (node_id is a real dbt node). On ancestry error: proposal(status=skipped), done.
S3. Look up service_name in the service→repo mapping (SERVICE_REPO_MAP_PATH). Missing
    file path / service name / mapping: proposal(status=skipped), done.
S4. Read the real source from GitHub Contents API at <repo_path>/<file_path>. Read
    error: proposal(status=skipped), done.
S5. Single forced propose_fix LLM tool call from {sanitized dbt log, real source}
    (no candidate SQL is fabricated). LLM transient error → retry.
S6. proposed_sql empty or unchanged from the source: proposal(status=failed), done.
S7. Write source artifacts attempt-<n>.source.sql / .source.diff; insert
    proposal(status=proposed, source_resolved=true) with repo/commit_sha/file_path,
    emit remediation.proposed:v1. Persist as in the shared transaction below.

── Validation two-step ─────────────────────────────────────────────────────────

4v. Fetch candidate SQL from S3 at candidate_sql_uri (required; error is transient).
5. Fetch dbt log from S3 at dbt_log_uri.
   - If not found: rawLog = "" (log unavailable path).
   - If transient S3 error: return error (message stays in PEL, retried).
6. Pass rawLog through LogSanitizer → dbtLog.
7. Call orchestrator GetNodeAncestry(node_id) → ranked upstream ancestors
   (best-effort; degrades to empty list on error).

── Step 1: candidate-based diagnosis ─────────────────────────────────────────

8. Assemble ProposeRequest from (node_id, error_signature, candidateSQL, dbtLog,
   repo, commit_sha, ancestors).

9. Forced single-shot LLM tool call (propose_fix):
   - Provider: anthropic (Anthropic API, HTTPS) or openai-compatible
     (e.g. stub-llm in dev/e2e).
   - The LLM must invoke the propose_fix tool; no streaming; result is parsed
     from the tool arguments (proposed_sql, rationale, confidence,
     suspected_root_cause_node).
   - On transient LLM error: return error (message retried via PEL).

10. proposed_sql is empty: insert proposal(status=failed), emit nothing, done.

11. Write candidate artifacts to S3 (unconditionally; audit trail):
      proposed-fix/<release_id>/<node_id>/attempt-<attempt>.sql
      proposed-fix/<release_id>/<node_id>/attempt-<attempt>.diff
    These become candidate_fix_sql_uri / candidate_fix_diff_uri in the proposal row.
    Default: proposed_sql_uri / diff_uri point here (source_resolved=false).

── Step 2: real-source fix ────────────────────────────────────────────────────

12. Call orchestrator GetNodeAncestry(node_id, depth=0) → file_path (original_file_path)
    of the failing node itself.
    - file_path empty or call fails: skip Step 2, keep candidate proposal
      (logged warning; no error returned).
    - Look up the node's service_name in the service→repo mapping loaded from
      SERVICE_REPO_MAP_PATH (service_repos.yaml). If the service is unmapped or
      SERVICE_REPO_MAP_PATH is empty: skip Step 2, keep candidate proposal.
    - Build the GitHub content path as: <repo_path>/<original_file_path>.

13. Read real model source from GitHub Contents API:
      GET /repos/{repo}/contents/{path}?ref={commit_sha}
    where {repo} comes from the trigger and {path} is constructed in step 12.
    - 404, empty GITHUB_TOKEN, unmapped service, or any HTTP/network error: skip
      Step 2, keep candidate proposal (logged warning; no error returned).

14. Forced single-shot LLM tool call (propose_fix) with the real source and the
    Step-1 rationale as context. Result is a corrected real-source SQL.
    - LLM error or empty result: skip Step 2, keep candidate proposal
      (logged warning; no error returned).

15. Write real-source artifacts to S3:
      proposed-fix/<release_id>/<node_id>/attempt-<attempt>.source.sql
      proposed-fix/<release_id>/<node_id>/attempt-<attempt>.source.diff
    Promote: proposed_sql_uri / diff_uri now point at the source artifacts
    (source_resolved=true).

── Persist ────────────────────────────────────────────────────────────────────

16. Open one Postgres transaction:
    a. Claim the inbound message in message_processing (keyed on the Redis
       message id and the upstream outbox_entry_id); if the claim conflicts the
       trigger was already handled, so roll back and ACK without re-proposing.
    b. Insert proposal(status=proposed, confidence, rationale, proposed_sql_uri,
       diff_uri, candidate_fix_sql_uri, candidate_fix_diff_uri,
       source_resolved, model, repo, commit_sha, file_path). repo/commit_sha
       come from the trigger; file_path is the path built in step 12 (non-empty
       only when source_resolved=true; empty string otherwise).
    c. Enqueue remediation_agent_outbox row (stream=remediation.proposed:v1,
       message_processing_id = the claim row, event_id = deterministic SHA1 UUID
       keyed on release_id+"|"+node_id+"|"+attempt).
17. Commit.
```

The degrade-don't-fail design means any failure in Step 2 (missing file_path, GitHub read error, empty token, empty LLM result) silently falls back to the candidate proposal. The trigger is never lost or retried due to a Step-2 failure.

### Outbox publisher

A background goroutine drains `remediation_agent_outbox` rows with `status='pending'` and XADDs each to its stream, injecting `outbox_entry_id` for downstream dedup.

## LLM Integration

The `LLMProvider` port is backed by one of three adapters selected at boot via `LLM_PROVIDER`:

| Value | Target | Notes |
|---|---|---|
| `anthropic` | Anthropic API (`https://api.anthropic.com`) | Model from `LLM_MODEL` env var (e.g. `claude-haiku-4-5`). |
| `openai` | OpenAI API (`https://api.openai.com`) | Model from `LLM_MODEL`. |
| `openai-compatible` | Operator-supplied endpoint (`LLM_BASE_URL`) | Used for local stub-llm in dev and e2e environments; model from `LLM_MODEL`. |

Each successful proposal triggers two non-streaming LLM calls through the same adapter:

1. **Step 1 (candidate diagnosis)**: the adapter is given the candidate SQL, sanitized dbt log, and ranked upstream ancestors. It forces the `propose_fix` tool call, which returns `proposed_sql`, `rationale`, `confidence`, and `suspected_root_cause_node`.
2. **Step 2 (real-source fix)**: the adapter receives the real model source (fetched from GitHub) and the Step-1 rationale. It forces the same `propose_fix` tool call to produce corrected real-source SQL. This call is made only when Step 2 succeeds; a failure or empty result falls back silently to the Step-1 candidate proposal.

If the Step-1 response contains no tool call (or no choices), the adapter returns an error; the handler propagates it so the Redis message is redelivered and retried. If the tool call is present but `proposed_sql` is empty, the adapter returns a zero-value `ProposeResult` without error; the handler detects the empty field and records the attempt as `failed` with no outbox emission.

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
| `proposed_sql_uri` | S3 URI of the best available proposed SQL. Points to the real-source artifact (`attempt-<n>.source.sql`) when `source_resolved=true`; falls back to the candidate artifact (`attempt-<n>.sql`) when `source_resolved=false`. |
| `diff_uri` | S3 URI of the unified diff corresponding to `proposed_sql_uri` (`attempt-<n>.source.diff` or `attempt-<n>.diff`). |
| `source_resolved` | `true` when Step 2 succeeded and the URIs above point at the real-source artifacts; `false` when only the candidate proposal is available. |
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

`remediation-agent` generates proposals and exposes their lifecycle over gRPC. It does not:

- Create GitHub pull requests or open code review branches. PR creation is performed by ui-service, which holds the GitHub App write credential.
- Write to, commit to, or push any git repository. GitHub access is read-only.
- Auto-apply or merge any proposed SQL change.
- Track PR state beyond `open` (merged/closed webhook/polling is out of scope for this service).
- Track whether a proposal was accepted or resulted in a passing release.

All code-change decisions — review, approval, and PR creation — are human actions.

## Background Loops

| Loop | Description |
|---|---|
| `remediation.requested:v1` consumer | Dispatches each inbound message to the `ProposeFix` handler. |
| Outbox publisher | Drains `remediation_agent_outbox` and XADDs each pending row to `remediation.proposed:v1` or `remediation.pr_opened:v1` depending on the row's stream field. |

## Configuration Reference

| Env var | Required | Default | Description |
|---|---|---|---|
| `POSTGRES_HOST` | yes | — | Postgres host |
| `POSTGRES_USER` | yes | — | Postgres user |
| `POSTGRES_PASSWORD` | yes | — | Postgres password |
| `POSTGRES_DB` | no | `continuo_remediation_agent` | Database name |
| `POSTGRES_PORT` | no | `5432` | Postgres port |
| `DB_SSLMODE` | no | `disable` | Postgres SSL mode |
| `REDIS_ADDR` | yes | — | Redis address (via `pkg/config.LoadRedis`) |
| `REDIS_PASSWORD` | yes | — | Redis password; process refuses to start if missing |
| `LLM_PROVIDER` | yes | — | `anthropic`, `openai`, or `openai-compatible` |
| `LLM_MODEL` | yes | — | Model identifier (e.g. `claude-haiku-4-5`) |
| `LLM_API_KEY` | no | `""` | API key for `anthropic`/`openai` providers |
| `LLM_BASE_URL` | conditional | `""` | Base URL; required when `LLM_PROVIDER=openai-compatible` |
| `GITHUB_TOKEN` | no | `""` | Read-only fine-grained PAT with `Contents: Read` on the dbt repo. In Helm, sourced from `global.github.token` in the chart-managed secret `continuo-app-credentials`. Empty disables Step-2 source fetch; the agent degrades to the candidate proposal. |
| `SERVICE_REPO_MAP_PATH` | no | `""` | Path to `service_repos.yaml`, which maps each dbt service name to its project root within the source repo. In Helm, set to `/etc/continuo/service_repos.yaml` and backed by the `continuo-app-service-repos` ConfigMap (built from `deploy/app/files/service_repos.yaml`). In docker-compose (dev/e2e), bind-mounted from `remediation-agent/config/service_repos.yaml`. Empty disables the lookup and causes Step 2 to degrade to the candidate proposal. |
| `GITHUB_BASE_URL` | no | `https://api.github.com` | GitHub REST API root; override for e2e stub (`stub-github`) |
| `CONTINUO_ORCHESTRATOR_ADDR` | no | `orchestrator:50052` | Orchestrator gRPC endpoint |
| `REMEDIATION_AGENT_HTTP_PORT` | no | `8092` | `/healthz` port |
| `REMEDIATION_AGENT_GRPC_PORT` | no | `50054` | `RemediationProposals` gRPC server port |
| `REMEDIATION_AGENT_MAX_ATTEMPTS` | no | `3` | Per-`(source, node_id, error_signature)` attempt cap |

## Key Code Paths

| Concern | Path |
|---|---|
| Proposal entity + unified diff | `remediation-agent/domain/proposal/proposal.go` |
| Prompt assembly (candidate + real-source) | `remediation-agent/domain/prompt/` |
| Event payloads + deterministic IDs | `remediation-agent/domain/event/` (proposed + pr_opened) |
| Application handler (two-step flow) | `remediation-agent/service/handlers/propose_fix.go` |
| PR lifecycle application service (claim/record/fail + outbox) | `remediation-agent/service/` |
| Port interfaces | `remediation-agent/service/ports/` |
| Postgres UoW + proposal repo (incl. CAS for BeginPR) | `remediation-agent/adapters/postgres/` |
| S3 evidence reader + artifact writer | `remediation-agent/adapters/s3/` |
| gRPC ancestry client | `remediation-agent/adapters/grpc/ancestry_client.go` |
| gRPC `RemediationProposals` server | `remediation-agent/adapters/grpc/server.go` |
| GitHub read-only source reader | `remediation-agent/adapters/github/source_reader.go` |
| Service→repo map config loader | `remediation-agent/config.go` (reads `SERVICE_REPO_MAP_PATH`, parses `service_repos.yaml`) |
| Service→repo map file (dev + e2e) | `remediation-agent/config/service_repos.yaml` |
| Service→repo map file (Helm chart) | `deploy/app/files/service_repos.yaml` (rendered into `continuo-app-service-repos` ConfigMap, mounted at `/etc/continuo`) |
| Anthropic LLM adapter | `remediation-agent/adapters/llm/anthropic.go` |
| OpenAI-compatible LLM adapter | `remediation-agent/adapters/llm/openai.go` |
| Pass-through log sanitizer | `remediation-agent/adapters/sanitizer/passthrough.go` |
| Redis consumer + outbox publisher | `remediation-agent/adapters/redis/` |
| DB migrations | `db/migration/remediation_agent/V1__init_remediation_agent.sql`, `V2__source_fix.sql` |
