# remediation-agent

## Purpose

`remediation-agent` acts on healable failures surfaced by the `remediation` classifier, across all three failure sources: `validation`, `compile`, and `seed_build`. It consumes `remediation.requested:v1` — one trigger per failing dbt node — and produces a fix proposal. A shared driver (`ProposeFix`) owns the attempt cap, inbound dedup, the dbt-log fetch, persistence, and the outbox emit; it dispatches each trigger to a per-error-class `Fixer` that decides which source files to read, which prompt to send, and how to interpret the model's answer. Validation failures carry a pre-compiled candidate SQL and use a two-step LLM flow (candidate diagnosis, then real-source fix). Compile failures carry no candidate SQL; the agent reads the offending file named in the trigger and, for a `.sql` file, also gathers its co-located `schema.yml` siblings and the service's `dbt_project.yml`, then asks the model to pick and correct the one file that needs to change in a single LLM call. Seed-build failures read the failing CSV and ask the model for a corrected CSV in a single LLM call, with an honest failed-not-proposed outcome when the bad value cannot be inferred. For each successful proposal the driver enqueues a pointer-only `remediation.proposed:v1` trigger so a downstream approver can review and apply the fix. Every invocation — whether it produces a proposal, is skipped, is escalated, or fails — is recorded in Postgres so no trigger is invisible. The agent never writes to or creates branches in any git repository; proposal application is a human action.

**Runtime**: Go service. HTTP `/healthz` on port 8092. gRPC `RemediationProposals` server on port 50054. Depends on Postgres (`continuo_remediation_agent`), Redis, S3, the orchestrator gRPC endpoint (`GetNodeAncestry`, port 50052), and the GitHub Contents API (read-only).

## Owned Storage

Postgres database `continuo_remediation_agent`. Tables:

| Table | Purpose |
|---|---|
| `proposal` | One row per attempt. Records `source` (`validation`, `compile`, or `seed_build`), `release_id`, `node_id`, `error_signature`, `attempt`, `status` (`proposed`, `skipped`, `failed`, `escalated`), `confidence`, `rationale`, `proposed_sql_uri`, `diff_uri`, `source_resolved`, `model`, `created_at`, and source-location columns (`repo`, `commit_sha`, `file_path` — populated when `source_resolved=true`). `candidate_fix_sql_uri`/`candidate_fix_diff_uri` are populated only for `validation` proposals (the Step-1 fix applied to the pre-compiled candidate SQL); always empty for `compile`/`seed_build`, which have no candidate SQL. PR-tracking columns: `pr_url`, `pr_number`, `pr_state` (lifecycle: `'' → opening → open` or `failed`), `pr_opened_at`, `pr_opened_by`. Unique on `(release_id, node_id, attempt)`. A secondary index on `(source, node_id, error_signature)` supports the attempt-count lookup. |
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
| `remediation.proposed:v1` | (approval surface) | The dispatched `Fixer` produces a `status=proposed` outcome for the node (validation, compile, or seed_build). |
| `remediation.pr_opened:v1` | (no consumer; audit seam for future close-loop) | `RecordPullRequest` is called; payload is pointer-only: `proposal_id`, `release_id`, `node_id`, `pr_url`, `pr_number`, `opened_by`, `opened_at`. |

All events are written to `remediation_agent_outbox` inside the same transaction as the `proposal` row insert and published with a deterministic `event_id` for consumer-side dedup.

### gRPC calls to `orchestrator`

| Method | Purpose |
|---|---|
| `OrchestratorQuery.GetNodeAncestry` (via the `AncestryClient.NodeContext` port) | For validation proposals: called once, returning the failing node's own `file_path`, its `service_name`, and its ranked upstream ancestors together; best-effort, degrades to an empty/absent result on failure (Step 1 proceeds without ancestor context; Step 2 is skipped). For seed_build: called only as a fallback when `file_path` or `service` is absent on the trigger; an error or empty result skips the proposal. For compile: not called — the offending file path comes from the trigger and the service is the trigger's `node_id`. |

Its own inbound gRPC surface (`RemediationProposals`) is described in the inbound interfaces section above.

### Outbound HTTP — GitHub Contents API

| Endpoint | Purpose |
|---|---|
| `GET /repos/{repo}/contents/{path}?ref={commit_sha}` | Read-only fetch of one file's raw text at the release commit. `Accept: application/vnd.github.raw+json`. Authenticated with `Authorization: Bearer <GITHUB_TOKEN>` when the token is set; unauthenticated otherwise. A 404 maps to `ErrSourceNotFound`; each fixer treats this as a definitive skip (no retry). Any other non-2xx status or network error is returned to the caller as a transient failure so the trigger is redelivered. Response bodies over 1 MiB are rejected rather than silently truncated. |
| `GET /repos/{repo}/contents/{dir}?ref={commit_sha}` | Read-only directory listing. `Accept: application/vnd.github+json`. Returns the repo-relative paths of the files (not sub-directories) directly under `{dir}`. Used only by the compile fixer to find `.yml`/`.yaml` siblings co-located with a failing `.sql` model; a 404 or any error is swallowed (this context is best-effort) and the read is simply skipped. |

The `{path}` for the offending file is formed by joining the owning service's `repo_path` (from `service_repos.yaml`, keyed by service name) with the dbt-project-relative source path. For compile, the service key is the trigger's `node_id` (the synthetic service id) and the file path comes directly from the trigger. For seed_build, the file path and service are threaded from the candidate topology on the trigger, falling back to orchestrator `GetNodeAncestry` when either is absent. For validation, the offending file's path and service come from the single `GetNodeAncestry` call. The `{repo}` comes from the trigger payload in all cases. No write requests are issued anywhere in this service; it holds no GitHub write permissions.

`ReadFile` itself always returns the same shape of error (`ErrSourceNotFound` on 404, a wrapped error otherwise); how a caller reacts differs by class. The compile and seed fixers treat their offending-file read as load-bearing: a 404 is a definitive skip, any other error is transient and the trigger is redelivered. The compile fixer's extra context reads (co-located `.yml`/`.yaml` files, `dbt_project.yml`, via `ListDir`) swallow every error, including 404s, since that context is optional. Validation's Step-2 real-source read is best-effort at a higher level: any error there — 404 or otherwise — degrades silently to the Step-1 candidate proposal rather than causing a retry, because Step 1 already produced a usable (if lower-fidelity) result.

`GITHUB_TOKEN` is injected at deploy time from the chart-managed secret `continuo-app-credentials` (key `GITHUB_TOKEN`, sourced from `global.github.token` in Helm values). No out-of-band secret mechanism is used.

## Data Flow

### Shared driver — `ProposeFix`

The driver in `service/handlers/propose_fix.go` runs for every trigger regardless of error class. It owns everything that is not class-specific; the class-specific work is delegated to a `Fixer`.

```
1. Decode trigger: extract source, release_id, node_id, error_signature,
   category, dbt_log_uri, candidate_sql_uri, file_path, service, repo, commit_sha.

2. Count prior attempts for (source, node_id, error_signature).
   - attempts >= MaxAttempts (default 3): insert proposal(status=escalated),
     emit nothing, done.

3. Resolve the Fixer for the trigger's source via fixer.For(source): compileFixer,
   seedFixer, or validationFixer. An unrecognized source is a programming error —
   the classifier only ever emits the three known values — and is returned loudly,
   not swallowed.

4. Fetch the dbt log from S3 at dbt_log_uri (not-found → "" for a log-unavailable
   proposal; any other error → return, message stays in the PEL and is retried),
   then sanitize it once via LogSanitizer. The sanitized log is handed to every
   Fixer; none of them re-fetch or re-sanitize it.

5. Call fx.Propose(ctx, services, input) — see the per-class flows below.
   A returned error means a transient failure (LLM error or a non-404 source
   read); the driver returns it unchanged so the trigger is redelivered.

6. Persist (shared for every class, one Postgres transaction):
   a. Claim the inbound message in message_processing (keyed on the Redis
      message id and the upstream outbox_entry_id); if the claim conflicts the
      trigger was already handled, so roll back and ACK without re-proposing.
   b. Insert the proposal row returned by the Fixer (status, confidence,
      rationale, proposed_sql_uri, diff_uri, source_resolved, model, and —
      for validation only — candidate_fix_sql_uri/candidate_fix_diff_uri).
      repo/commit_sha/file_path are populated only when the Fixer reports
      source_resolved=true.
   c. When the Fixer's outcome is status=proposed, enqueue a
      remediation_agent_outbox row (stream=remediation.proposed:v1,
      message_processing_id = the claim row, event_id = deterministic SHA1
      UUID keyed on release_id+"|"+node_id+"|"+attempt).
   d. Commit.
```

### Compile fixer

`compileFixer` (`service/fixer/compile.go`) makes exactly one LLM call and lets the model choose which of several shown files to correct.

```
1. Empty file_path on the trigger (a project-level compile error with no
   models/ path in the log): proposal(status=skipped), done.
2. Look up the trigger's node_id (the synthetic service id for a compile
   failure) in the service→repo mapping. Unmapped: proposal(status=skipped), done.
3. Read the offending file at <repo_path>/<file_path>.
   - 404: proposal(status=skipped), done (definitive; not retried).
   - Any other error: return it (transient; message redelivered).
4. When the offending file's name ends in .sql, best-effort-gather extra context
   (every failure here is swallowed; the offending file alone is still sent):
   - List the offending file's directory via the GitHub Contents API directory
     listing; read every co-located .yml/.yaml sibling found.
   - Read the service's dbt_project.yml.
5. Single forced propose_fix LLM tool call showing every gathered file (offending
   file plus any context files) and the sanitized dbt compile error. The model
   returns target_file (which shown file to change) and proposed_content (that
   file's complete corrected content).
   - LLM transient error → retry.
6. Interpret the result:
   - target_file is empty: default to the offending file.
   - target_file is not one of the files shown to the model: proposal(status=skipped)
     — never open a PR against a path the agent never read.
   - proposed_content is empty or identical to target_file's original content:
     proposal(status=failed).
   - Otherwise: proposed outcome.
7. On a proposed outcome, diff the corrected content against target_file's
   original content and write proposed-fix/<release_id>/<node_id>/attempt-<n>.source.sql
   and .source.diff (these keys are used for every source-file fix regardless of
   whether target_file is a .sql or a .yml). Insert
   proposal(status=proposed, source_resolved=true, file_path=target_file),
   emit remediation.proposed:v1.
```

### Seed fixer

`seedFixer` (`service/fixer/seed.go`) also makes exactly one LLM call, scoped to a single failing CSV.

```
1. Resolve file_path and service:
   - Primary: both are threaded from the candidate topology on the trigger.
   - Fallback: either is empty — call orchestrator
     GetNodeAncestry(node_id) to resolve them. Ancestry error, or both still
     empty after the fallback: proposal(status=skipped), done.
2. Look up service in the service→repo mapping. Unmapped: proposal(status=skipped), done.
3. Read the CSV at <repo_path>/<file_path>.
   - 404: proposal(status=skipped), done (definitive; not retried).
   - Any other error: return it (transient; message redelivered).
4. Single forced propose_fix LLM tool call with the CSV content and the sanitized
   dbt seed error. The prompt is CSV-specific: it names the three concrete failure
   shapes (a stray comma inside an unquoted text field, a malformed row with the
   wrong column count, a value that does not match its column type) and instructs
   the model to return the CSV unchanged with low confidence when a bad value
   cannot be inferred from the file and error alone.
   - LLM transient error → retry.
5. Interpret the result: proposed_content empty or identical to the original CSV
   → proposal(status=failed) — an honest "can't infer the value" answer produces
   no proposal, not a false-positive fix. Otherwise: proposed outcome.
6. On a proposed outcome, diff and write proposed-fix/<release_id>/<node_id>/attempt-<n>.source.sql
   and .source.diff (same key shape as compile, even though the content is CSV).
   Insert proposal(status=proposed, source_resolved=true, file_path=<the CSV path>),
   emit remediation.proposed:v1.
```

### Validation fixer — two-step flow

`validationFixer` (`service/fixer/validation.go`) is the one class that carries a pre-compiled candidate SQL and still runs two LLM calls: a first diagnosis against that candidate, then a best-effort second pass that applies the diagnosis to the real model source.

```
1. Empty candidate_sql_uri on the trigger: proposal(status=skipped), done.
2. Fetch the candidate SQL from S3 at candidate_sql_uri (required; any error is
   transient and the trigger is redelivered).
3. Call orchestrator GetNodeAncestry(node_id) once. It returns the failing node's
   own file_path, its service_name, and its ranked upstream ancestors together.
   Best-effort: on error, proceed with no ancestors and empty file_path/service_name
   (Step 1 still runs; Step 2 is skipped below).

── Step 1: candidate-based diagnosis ─────────────────────────────────────────

4. Assemble a ProposeRequest from (node_id, error_signature, candidateSQL,
   sanitized dbt log, repo, commit_sha, ancestors).
5. Forced single-shot LLM tool call (propose_fix): the LLM must invoke the tool;
   no streaming; result parsed from the tool arguments (proposed_sql, rationale,
   confidence, suspected_root_cause_node).
   - Transient LLM error → retry.
6. proposed_sql is empty: proposal(status=failed), emit nothing, done.
7. Write candidate artifacts to S3 (unconditionally; audit trail — this is the
   LLM's fix applied to the pre-compiled candidate SQL, not the real model source):
     proposed-fix/<release_id>/<node_id>/attempt-<attempt>.sql
     proposed-fix/<release_id>/<node_id>/attempt-<attempt>.diff
   These become candidate_fix_sql_uri / candidate_fix_diff_uri. Default:
   proposed_sql_uri / diff_uri point here (source_resolved=false).

── Step 2: real-source fix ────────────────────────────────────────────────────

8. file_path or service_name from step 3 is empty: skip Step 2, keep the
   candidate proposal (logged warning; no error returned).
9. Look up service_name in the service→repo mapping. Unmapped: skip Step 2,
   keep the candidate proposal.
10. Read the real model source from GitHub Contents API at
    <repo_path>/<file_path> (repo/commit_sha from the trigger). Any error
    (404, network, non-2xx): skip Step 2, keep the candidate proposal
    (logged warning; no error returned — unlike compile/seed, a Step-2 read
    failure never causes a retry).
11. Forced single-shot LLM tool call (propose_fix) with the real source — passed
    through the same LogSanitizer.Sanitize seam used for the dbt log — and the
    Step-1 rationale as context. Result is a corrected real-source SQL.
    - LLM error, empty result, an unchanged result, or a low-confidence result
      (confidence == "low"): skip Step 2, keep the candidate proposal
      (logged warning; no error returned).
12. Write real-source artifacts to S3:
      proposed-fix/<release_id>/<node_id>/attempt-<attempt>.source.sql
      proposed-fix/<release_id>/<node_id>/attempt-<attempt>.source.diff
    Promote: proposed_sql_uri / diff_uri now point at the source artifacts
    (source_resolved=true).
13. Build the proposal: confidence, rationale, model, and suspected_root_cause_node
    all come from the Step-1 result. source_resolved, repo, commit_sha, and
    file_path reflect whether Step 2 succeeded (step 12) or was skipped (steps
    8–11), keeping the Step-1 candidate proposal in the latter case.
```

The degrade-don't-fail design means any failure in Step 2 (missing file_path, GitHub read error, empty LLM result, unchanged result, low-confidence result) silently falls back to the candidate proposal. The trigger is never lost or retried due to a Step-2 failure — only Step 1 and the offending-file reads in the compile/seed fixers are load-bearing enough to cause a retry.

### Outbox publisher

A background goroutine drains `remediation_agent_outbox` rows with `status='pending'` and XADDs each to its stream, injecting `outbox_entry_id` for downstream dedup.

## LLM Integration

The `LLMProvider` port is backed by one of three adapters selected at boot via `LLM_PROVIDER`:

| Value | Target | Notes |
|---|---|---|
| `anthropic` | Anthropic API (`https://api.anthropic.com`) | Model from `LLM_MODEL` env var (e.g. `claude-haiku-4-5`). |
| `openai` | OpenAI API (`https://api.openai.com`) | Model from `LLM_MODEL`. |
| `openai-compatible` | Operator-supplied endpoint (`LLM_BASE_URL`) | Used for local stub-llm in dev and e2e environments; model from `LLM_MODEL`. |

Every adapter forces the same `propose_fix` tool on every call — no streaming, no free-form text response — but the number of calls and which tool fields are populated differ per error class:

- **Validation**: two non-streaming calls per proposal. Step 1 is given the candidate SQL, sanitized dbt log, and ranked upstream ancestors, and returns `proposed_sql`, `rationale`, `confidence`, and `suspected_root_cause_node`. Step 2 is given the real model source and the Step-1 rationale, and returns a corrected `proposed_sql`. Step 2 is made only when the file path and service resolve; a failure, empty result, unchanged result, or low-confidence result falls back silently to the Step-1 candidate proposal.
- **Compile**: one non-streaming call per proposal. The adapter is given every gathered file (the offending file plus any co-located `.yml`/`.yaml` siblings and `dbt_project.yml`) and the sanitized dbt compile error, and returns `target_file` (which shown file to change), `proposed_content` (that file's complete corrected content), `rationale`, `confidence`, and `suspected_root_cause_node`.
- **Seed**: one non-streaming call per proposal. The adapter is given the failing CSV and the sanitized dbt seed error, and returns `proposed_content` (the complete corrected CSV), `rationale`, and `confidence`. The seed prompt has no `suspected_root_cause_node` field — a bad seed value has no upstream node to blame.

The `ProposeResult` struct carries all four possible fields (`proposed_sql`, `proposed_content`, `target_file`, plus `rationale`/`confidence`/`suspected_root_cause_node`/`model`) regardless of class; each fixer reads only the fields its prompt asked for. Both the Anthropic and the OpenAI-compatible adapter parse `target_file` and `proposed_content` from the tool-call arguments alongside the pre-existing `proposed_sql`.

If a response contains no tool call (or no choices), the adapter returns an error; the caller propagates it so the Redis message is redelivered and retried. If the tool call is present but the class-relevant content field (`proposed_sql` for validation, `proposed_content` for compile/seed) is empty or unchanged from the original, the fixer records the attempt as `failed` with no outbox emission (for validation Step 1 this also aborts the proposal entirely; for validation Step 2, compile, and seed this is a normal empty/unchanged outcome as described above).

## LogSanitizer Seam

The `LogSanitizer` port sits between the raw S3 log fetch and prompt assembly; the driver runs every fetched dbt log through it once. Validation's Step 2 also reuses the same seam to sanitize the real model source before it is sent to the LLM. The deployed implementation is currently pass-through: it returns its input unchanged. The seam exists so a redacting implementation can be dropped in without touching the handler or fixer logic.

## Payload Shape (`remediation.proposed:v1`)

The trigger is pointer-only: it carries no SQL/CSV text, no log content, and no warehouse data. Consumers fetch the artifacts from S3 using the supplied URIs.

| Field | Description |
|---|---|
| `event_id` | Deterministic SHA1 UUID keyed on `release_id\|node_id\|attempt`. Stable on redelivery. |
| `source` | Origin pipeline: `validation`, `compile`, or `seed_build`. |
| `release_id` | The release identifier from the inbound trigger. |
| `node_id` | The unique_id of the failing dbt node. |
| `error_signature` | Release-stable normalized dedup key from the classifier (SHA-256 hex). |
| `proposed_sql_uri` | S3 URI of the best available proposed fix content. For compile and seed_build, always the source artifact (`attempt-<n>.source.sql`, containing the corrected file's content whether it is SQL, YAML, or CSV). For validation, points to the real-source artifact (`attempt-<n>.source.sql`) when `source_resolved=true`; falls back to the candidate artifact (`attempt-<n>.sql`) when `source_resolved=false`. |
| `diff_uri` | S3 URI of the unified diff corresponding to `proposed_sql_uri` (`attempt-<n>.source.diff` or, for a validation candidate fallback, `attempt-<n>.diff`). |
| `source_resolved` | `true` when the URIs above point at a real-source artifact. Always `true` for a compile or seed_build proposal. For validation, `true` only when Step 2 succeeded; `false` when only the Step-1 candidate proposal is available. |
| `rationale` | Short rationale from the LLM (no warehouse data). |
| `confidence` | `low`, `medium`, or `high`. |
| `suspected_root_cause_node` | Optional node_id the LLM identified as the root cause. Populated by validation and compile; never set by seed_build. |
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
| `GITHUB_TOKEN` | no | `""` | Read-only fine-grained PAT with `Contents: Read` on the dbt repo. In Helm, sourced from `global.github.token` in the chart-managed secret `continuo-app-credentials`. When empty, requests to the Contents API are sent unauthenticated (subject to GitHub's lower unauthenticated rate limit) rather than failing outright. |
| `SERVICE_REPO_MAP_PATH` | no | `""` | Path to `service_repos.yaml`, which maps each dbt service name to its project root within the source repo. In Helm, set to `/etc/continuo/service_repos.yaml` and backed by the `continuo-app-service-repos` ConfigMap (built from `deploy/app/files/service_repos.yaml`). In docker-compose (dev/e2e), bind-mounted from `remediation-agent/config/service_repos.yaml`. Empty (or a service name absent from the map) means every fixer's source read has no repo path to resolve: compile and seed proposals are skipped, and validation's Step 2 degrades to the Step-1 candidate proposal. |
| `GITHUB_BASE_URL` | no | `https://api.github.com` | GitHub REST API root; override for e2e stub (`stub-github`) |
| `CONTINUO_ORCHESTRATOR_ADDR` | no | `orchestrator:50052` | Orchestrator gRPC endpoint |
| `REMEDIATION_AGENT_HTTP_PORT` | no | `8092` | `/healthz` port |
| `REMEDIATION_AGENT_GRPC_PORT` | no | `50054` | `RemediationProposals` gRPC server port |
| `REMEDIATION_AGENT_MAX_ATTEMPTS` | no | `3` | Per-`(source, node_id, error_signature)` attempt cap |

## Key Code Paths

| Concern | Path |
|---|---|
| Proposal entity + unified diff | `remediation-agent/domain/proposal/proposal.go` |
| Prompt assembly (validation candidate + real-source, compile, seed) | `remediation-agent/domain/prompt/prompt.go` (`Assemble`, `AssembleSourceFix`, `AssembleCompileFix`, `AssembleSeedFix`) |
| Event payloads + deterministic IDs | `remediation-agent/domain/event/` (proposed + pr_opened) |
| Shared driver — attempt cap, dedup, log fetch, persistence, outbox emit | `remediation-agent/service/handlers/propose_fix.go` |
| Per-error-class fixers — `Fixer` interface, `For` factory, shared single-shot pipeline | `remediation-agent/service/fixer/fixer.go` |
| Compile fixer (offending file + co-located YAML/`dbt_project.yml` context, one LLM call) | `remediation-agent/service/fixer/compile.go` |
| Seed fixer (CSV read, one LLM call) | `remediation-agent/service/fixer/seed.go` |
| Validation fixer (two-step candidate + real-source flow) | `remediation-agent/service/fixer/validation.go` |
| PR lifecycle application service (claim/record/fail + outbox) | `remediation-agent/service/` |
| Port interfaces | `remediation-agent/service/ports/` |
| Postgres UoW + proposal repo (incl. CAS for BeginPR) | `remediation-agent/adapters/postgres/` |
| S3 evidence reader + artifact writer | `remediation-agent/adapters/s3/` |
| gRPC ancestry client | `remediation-agent/adapters/grpc/ancestry_client.go` |
| gRPC `RemediationProposals` server | `remediation-agent/adapters/grpc/server.go` |
| GitHub read-only source reader (file read + directory list) | `remediation-agent/adapters/github/source_reader.go` |
| Service→repo map config loader | `remediation-agent/config.go` (reads `SERVICE_REPO_MAP_PATH`, parses `service_repos.yaml`) |
| Service→repo map file (dev + e2e) | `remediation-agent/config/service_repos.yaml` |
| Service→repo map file (Helm chart) | `deploy/app/files/service_repos.yaml` (rendered into `continuo-app-service-repos` ConfigMap, mounted at `/etc/continuo`) |
| Anthropic LLM adapter | `remediation-agent/adapters/llm/anthropic.go` |
| OpenAI-compatible LLM adapter | `remediation-agent/adapters/llm/openai.go` |
| Pass-through log sanitizer | `remediation-agent/adapters/sanitizer/passthrough.go` |
| Redis consumer + outbox publisher | `remediation-agent/adapters/redis/` |
| DB migrations | `db/migration/remediation_agent/V1__init_remediation_agent.sql`, `V2__source_fix.sql` |
