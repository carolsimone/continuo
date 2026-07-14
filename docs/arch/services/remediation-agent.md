# remediation-agent

## Purpose

`remediation-agent` acts on healable failures surfaced by the `remediation` classifier, across all three failure sources: `validation`, `compile`, and `seed_build`. It consumes `remediation.requested:v1` — one trigger per failing dbt node — and produces a fix proposal. A shared driver (`ProposeFix`) owns the attempt cap, inbound dedup, persistence, and the outbox emit; it dispatches each trigger to a per-error-class `Fixer` that decides which source files to read, whether it needs the dbt log (each class fetches and sanitizes it itself, only when needed), which prompt to send, and how to interpret the model's answer. Validation failures carry a pre-compiled candidate SQL and use a two-step LLM flow (candidate diagnosis, then real-source fix). Compile failures carry no candidate SQL; the agent reads the offending file named in the trigger and, for a `.sql` file, also gathers its co-located `schema.yml` siblings and the service's `dbt_project.yml`, then asks the model to pick and correct the one file that needs to change in a single LLM call. Seed-build failures read the failing CSV and ask the model for a corrected CSV in a single LLM call, with an honest failed-not-proposed outcome when the bad value cannot be inferred. For each successful proposal the driver enqueues a pointer-only `remediation.proposed:v1` trigger so a downstream approver can review and apply the fix. Every invocation — whether it produces a proposal, is skipped, is escalated, or fails — is recorded in Postgres so no trigger is invisible. The agent never writes to or creates branches in any git repository; proposal application is a human action.

**Runtime**: Go service. HTTP `/healthz` on port 8092. gRPC `RemediationProposals` server on port 50054. Depends on Postgres (`continuo_remediation_agent`), Redis, S3, the orchestrator gRPC endpoint (`GetNodeAncestry`, port 50052), and GitHub, exclusively via read-only GETs against the Contents API (source file/directory reads), the commit API (upstream-change diffs for validation), and the Pulls API (PR status polling for the reconciler).

## Owned Storage

Postgres database `continuo_remediation_agent`. Tables:

| Table | Purpose |
|---|---|
| `proposal` | One row per attempt. Records `source` (`validation`, `compile`, or `seed_build`), `release_id`, `node_id`, `error_signature`, `attempt`, `status`, `confidence`, `rationale`, `proposed_sql_uri`, `diff_uri`, `source_resolved`, `model`, `created_at`, and source-location columns (`repo`, `commit_sha`, `file_path` — populated when `source_resolved=true`). `status` lifecycle: a row is written `generating` (in flight, just before the model is called) and then finalized to one terminal state — `proposed`, `skipped`, `failed`, or `escalated`. `candidate_fix_sql_uri`/`candidate_fix_diff_uri` are populated only for `validation` proposals (the Step-1 fix applied to the pre-compiled candidate SQL); always empty for `compile`/`seed_build`, which have no candidate SQL. PR-tracking columns: `pr_url`, `pr_number`, `pr_state`, `pr_opened_at`, `pr_opened_by`, `pr_closed_at`. `pr_state` lifecycle: `'' → opening → open → merged | rejected`, with `opening → failed` as a retryable error path; `merged` and `rejected` are terminal. A PR reopened on GitHub after reaching `merged`/`rejected` is not tracked — the reconciler only watches `open` rows. Unique on `(release_id, source, node_id, attempt)`; the terminal write upserts on this key so it finalizes the in-flight generating row. A secondary index on `(source, node_id, error_signature)` supports the attempt-count lookup, which counts terminal rows only (`status <> 'generating'`). |
| `remediation_agent_outbox` | Transactional outbox; one row per emitted event (`remediation.proposed:v1`, `remediation.pr_opened:v1`, or `remediation.pr_closed:v1`), drained by the outbox publisher. Status: `pending`, `processed`, `failed`. |
| `message_processing` | Shared shape consumed by `pkg/messageprocessing`; FK target of `remediation_agent_outbox.message_processing_id`. |

The `proposal` table records one row per attempt: it is written `generating` when the model call begins and finalized in place to a terminal outcome. All terminal outcomes — proposed, escalations, skips, and LLM failures — are recorded so the full attempt history is queryable.

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
| `remediation.pr_opened:v1` | (no consumer; audit seam) | `RecordPullRequest` is called; payload is pointer-only: `proposal_id`, `release_id`, `node_id`, `pr_url`, `pr_number`, `opened_by`, `opened_at`. |
| `remediation.pr_closed:v1` | (no consumer; audit seam) | The PR-outcome reconciler observes a terminal GitHub PR state and `RecordOutcome` performs the CAS `open → merged | rejected`; payload is pointer-only: `proposal_id`, `release_id`, `node_id`, `pr_url`, `pr_number`, `outcome` (`merged` or `rejected`), `closed_at` (RFC 3339). `event_id` is a deterministic SHA1 UUID keyed on `release_id|node_id|attempt`, distinct from the `pr_opened` id derived from the same triple. |

All events are written to `remediation_agent_outbox` inside the same transaction as the `proposal` row insert and published with a deterministic `event_id` for consumer-side dedup.

### gRPC calls to `orchestrator`

| Method | Purpose |
|---|---|
| `OrchestratorQuery.GetNodeAncestry` (via the `AncestryClient.NodeContext` port) | For validation proposals: called once, returning the failing node's own `file_path`, its `service_name`, and its ranked upstream ancestors together (each carrying `last_repo`/`last_commit_sha`/`last_changed_at` when the ancestor has changed); best-effort, degrades to an empty/absent result on failure (Step 1 proceeds without ancestor context; Step 2 is skipped). The returned ancestors also drive a best-effort upstream-diff gather: up to the 5 most-recently-changed eligible ancestors are read via the GitHub commit API and their diffs are embedded in the Step-1 prompt. For seed_build: called only as a fallback when `file_path` or `service` is absent on the trigger; an error or empty result skips the proposal. For compile: not called — the offending file path comes from the trigger and the service is the trigger's `node_id`. |

Its own inbound gRPC surface (`RemediationProposals`) is described in the inbound interfaces section above.

### Outbound HTTP — GitHub Contents & commit API

| Endpoint | Purpose |
|---|---|
| `GET /repos/{repo}/contents/{path}?ref={commit_sha}` | Read-only fetch of one file's raw text at the release commit. `Accept: application/vnd.github.raw+json`. Authenticated with `Authorization: Bearer <GITHUB_TOKEN>` when the token is set; unauthenticated otherwise. A 404 maps to `ErrSourceNotFound`. For the compile and seed fixers' offending-file read, a 404 is a definitive skip (no retry) and any other non-2xx status or network error is returned to the caller as a transient failure so the trigger is redelivered. The validation fixer's Step-2 real-source read instead treats any error — 404 or otherwise — as a silent degrade to the Step-1 candidate proposal, with no retry. Response bodies over 1 MiB are rejected rather than silently truncated. |
| `GET /repos/{repo}/contents/{dir}?ref={commit_sha}` | Read-only directory listing. `Accept: application/vnd.github+json`. Returns the repo-relative paths of the files (not sub-directories) directly under `{dir}`. Used only by the compile fixer to find `.yml`/`.yaml` siblings co-located with a failing `.sql` model; a 404 or any error is swallowed (this context is best-effort) and the read is simply skipped. |
| `GET /repos/{repo}/commits/{sha}` | Read-only fetch of one commit's changed files and their unified diffs. `Accept: application/vnd.github+json`. Returns the commit's `files[]`, each with a `patch`; the adapter (`SourceReader.CommitFileDiff(repo, sha, path)`) returns the patch for the file matching `path`. A 404, a commit that did not touch `path`, or a file GitHub returns without a `patch` all map to `ErrSourceNotFound`. For a commit whose file list spans multiple pages, the adapter follows the `Link` header's `rel="next"` until the file is found or the pages are exhausted, so a target on a later page is not falsely reported as unchanged. Used only by the validation fixer, to fetch the diff of the recent change to each of its most-recently-changed upstream ancestors; every per-ancestor read is best-effort — an error is logged and that ancestor is skipped, never retried, and a total failure simply leaves the Step-1 prompt at its metadata-only content. |

The `{path}` for the offending file is formed by joining the owning service's `repo_path` (from `service_repos.yaml`, keyed by service name) with the dbt-project-relative source path. For compile, the service key is the trigger's `node_id` (the synthetic service id) and the file path comes directly from the trigger. For seed_build, the file path and service are threaded from the candidate topology on the trigger, falling back to orchestrator `GetNodeAncestry` when either is absent. For validation, the offending file's path and service come from the single `GetNodeAncestry` call; each upstream ancestor's diff path is instead formed by joining its own `service_name`'s `repo_path` with its own `file_path`, read at its own `last_repo`/`last_commit_sha` rather than the failing node's `{repo}`/`commit_sha`. No write requests are issued anywhere in this service; it holds no GitHub write permissions.

`ReadFile` and `CommitFileDiff` both always return the same shape of error (`ErrSourceNotFound` on 404 or an absent/empty result, a wrapped error otherwise); how a caller reacts differs by class. The compile and seed fixers treat their offending-file read as load-bearing: a 404 is a definitive skip, any other error is transient and the trigger is redelivered. The compile fixer's extra context reads (co-located `.yml`/`.yaml` files, `dbt_project.yml`, via `ListDir`) swallow every error, including 404s, since that context is optional. Validation's Step-2 real-source read is best-effort at a higher level: any error there — 404 or otherwise — degrades silently to the Step-1 candidate proposal rather than causing a retry, because Step 1 already produced a usable (if lower-fidelity) result. The validation fixer's upstream-diff reads (`CommitFileDiff`) are best-effort at the same level as the compile fixer's extra context: an ancestor is eligible only when it carries a non-empty `last_commit_sha`, `last_repo`, and `file_path` and its `service_name` has a known `service_repos.yaml` mapping (an ancestor that never changed carries no stamped commit and is naturally excluded); at most 5 eligible ancestors, most-recently-changed first, are attempted — the cap bounds fetch attempts, not successful reads, so a GitHub outage over a wide ancestry cannot issue an unbounded number of calls — and each fetched diff is run through the `LogSanitizer` and then truncated to roughly 8 KiB before being embedded in the prompt.

`GITHUB_TOKEN` is injected at deploy time from the chart-managed secret `continuo-app-credentials` (key `GITHUB_TOKEN`, sourced from `global.github.token` in Helm values). No out-of-band secret mechanism is used.

### Outbound HTTP — GitHub Pulls API

| Endpoint | Purpose |
|---|---|
| `GET /repos/{repo}/pulls/{number}` | Read-only fetch of one pull request's current state, used by the PR-outcome reconciler. `Accept: application/vnd.github+json`. Authenticated the same way as the Contents API calls above. Any non-2xx response (including 404) is an error: the caller leaves the proposal untouched and retries on the next reconcile pass. On success, `state="closed"` with `merged=true` maps to outcome `merged`; `state="closed"` with `merged=false` maps to outcome `rejected`; `state="open"` is not yet actionable and the row is left for a later pass. |

This is the only GitHub write-adjacent surface remediation-agent reads from besides source files; the request is still a GET, so GitHub access remains exclusively read-only.

## Data Flow

### Shared driver — `ProposeFix`

The driver in `service/handlers/propose_fix.go` runs for every trigger regardless of error class. It owns everything that is not class-specific; the class-specific work is delegated to a `Fixer`.

```
1. Decode trigger: extract source, release_id, node_id, error_signature,
   category, dbt_log_uri, candidate_sql_uri, file_path, service, repo, commit_sha.

1a. Read-only dedup pre-check (no write): before any row is written, check
    whether this trigger was already handled, on either dedup axis scoped to the
    consuming stream — a message_processing row matching (message_id, stream_name)
    OR (outbox_entry_id, stream_name). If found, ACK and return without writing
    anything. This
    keeps a re-emitted completed trigger (fresh Redis message id, same
    outbox_entry_id) from minting a phantom in-flight 'generating' row for a
    fresh attempt number that the terminal claim would then abandon.

2. Count prior TERMINAL attempts for (source, node_id, error_signature). In-flight
   'generating' rows are excluded, so the in-progress attempt neither inflates the
   cap nor shifts the attempt number across a redelivery.
   - attempts >= MaxAttempts (default 3): insert proposal(status=escalated),
     emit nothing, done. (This path is before markGenerating, so an unhealable
     escalated node never shows the "Generating fix…" indicator.)

3. Resolve the Fixer for the trigger's source via fixer.For(source): compileFixer,
   seedFixer, or validationFixer. An unrecognized source is a programming error —
   the classifier only ever emits the three known values — and is returned loudly,
   not swallowed.

3a. markGenerating(attempt): in its own committed transaction, insert an in-flight
    proposal(status=generating) row for this attempt (idempotent — ON CONFLICT on
    (release_id, source, node_id, attempt) DO NOTHING, so a redelivery of an
    in-flight attempt re-uses the row). This is the explicit "fix in flight" signal
    the release UI reads to show a disabled "Generating fix…" chip while the model
    runs. A Fixer that then skips internally finalizes the row to a terminal state
    (a brief generating→blank flicker for that case is accepted).

4. Call fx.Propose(ctx, services, input) — see the per-class flows below. The
   trigger's dbt_log_uri is passed through unfetched: each Fixer reads and
   sanitizes the dbt log itself (via the shared loadDBTLog helper: not-found → ""
   for a log-unavailable proposal; any other error → return, message stays in the
   PEL and is retried) only when its error class needs the log, and only after it
   has decided to produce a fix. This keeps the read off the paths that skip
   before proposing anything — most importantly a validation trigger with no
   candidate SQL, which must skip even when its log URI is transiently unreadable.
   A returned error means a transient failure (LLM error, a non-404 source read,
   or a non-not-found log read); the driver returns it unchanged so the trigger is
   redelivered.

5. Persist the terminal outcome (shared for every class, one Postgres transaction):
   a. Claim the inbound message in message_processing (keyed on the Redis
      message id and the upstream outbox_entry_id); if the claim conflicts the
      trigger was already handled, so roll back and ACK without re-proposing.
   b. Upsert the proposal row returned by the Fixer on (release_id, source,
      node_id, attempt): this finalizes the in-flight 'generating' row from
      step 3a in place (status, confidence, rationale, proposed_sql_uri, diff_uri,
      source_resolved, model, and — for validation only —
      candidate_fix_sql_uri/candidate_fix_diff_uri). repo/commit_sha/file_path are
      populated only when the Fixer reports source_resolved=true.
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
5. Fetch and sanitize the dbt compile error via loadDBTLog (only now, once the
   gather above has committed to producing a fix; not-found → "", any other error
   is transient and redelivers), then make a single forced propose_fix LLM tool
   call showing every gathered file — each run through the LogSanitizer first, so
   raw source never leaves for the LLM — and that error. The model returns
   target_file (which shown file to change) and proposed_content (that file's
   complete corrected content).
   - LLM transient error → retry.
6. Interpret the result:
   - Resolve target_file to exactly one shown file. An exact path match wins; a
     differently-rooted spelling (e.g. the model returns models/x.sql for a file
     shown as services/svc/models/x.sql) is accepted only when it is an
     unambiguous path-suffix match of a single shown file; an empty target_file
     resolves to the offending file only when it is the sole file shown.
   - No shown file resolves (unknown path, ambiguous suffix, or empty target with
     several files shown): proposal(status=skipped) — never open a PR against a
     path the agent never read, and never guess which of several files the fix
     was meant for.
   - proposed_content is empty or identical to the resolved file's original
     content: proposal(status=failed).
   - confidence is low (the model's signal it could not determine a safe fix):
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
4. Fetch and sanitize the dbt seed error via loadDBTLog (only now, once the CSV
   read above has succeeded; not-found → "", any other error is transient and
   redelivers), then make a single forced propose_fix LLM tool call with the CSV
   content — run through the LogSanitizer first, so raw seed rows never leave for
   the LLM — and that error. The prompt is CSV-specific: it names the three concrete failure
   shapes (a stray comma inside an unquoted text field, a malformed row with the
   wrong column count, a value that does not match its column type) and instructs
   the model to return the CSV unchanged with low confidence when a bad value
   cannot be inferred from the file and error alone.
   - LLM transient error → retry.
5. Interpret the result: proposed_content empty or identical to the original CSV,
   or returned with low confidence → proposal(status=failed) — an honest "can't
   infer the value" answer (whether it echoes the CSV or guesses a value it flags
   low-confidence) produces no proposal, not a false-positive fix. Otherwise:
   proposed outcome.
6. On a proposed outcome, diff and write proposed-fix/<release_id>/<node_id>/attempt-<n>.source.sql
   and .source.diff (same key shape as compile, even though the content is CSV).
   Insert proposal(status=proposed, source_resolved=true, file_path=<the CSV path>),
   emit remediation.proposed:v1.
```

### Validation fixer — two-step flow

`validationFixer` (`service/fixer/validation.go`) is the one class that carries a pre-compiled candidate SQL and still runs two LLM calls: a first diagnosis against that candidate, then a best-effort second pass that applies the diagnosis to the real model source.

```
1. Empty candidate_sql_uri on the trigger: proposal(status=skipped), done. This
   is decided before any evidence is fetched, so a transiently unreadable dbt log
   URI cannot turn the intended skip into a redelivery.
2. Fetch the candidate SQL from S3 at candidate_sql_uri (required; any error is
   transient and the trigger is redelivered), then fetch and sanitize the dbt log
   via loadDBTLog (not-found → ""; any other error is transient and redelivered).
   Both reads happen only after the empty-candidate skip above.
3. Call orchestrator GetNodeAncestry(node_id) once. It returns the failing node's
   own file_path, its service_name, and its ranked upstream ancestors together.
   Best-effort: on error, proceed with no ancestors and empty file_path/service_name
   (Step 1 still runs; Step 2 is skipped below).
3a. Best-effort: gather the diff of the recent change to each of the most-
    recently-changed upstream ancestors, up to 5. An ancestor is eligible only
    when it carries a non-empty last_commit_sha, last_repo, and file_path, and
    its service_name has a known service_repos.yaml mapping (an ancestor that
    never changed carries no stamped commit and is naturally excluded). For
    each eligible ancestor, join its service_repos.yaml path with its own
    file_path and read the unified diff via the GitHub commit API
    (GET /repos/{last_repo}/commits/{last_commit_sha}) — the ancestor's own
    repo and commit, not the failing node's. Each diff is truncated to ~8 KiB.
    Any per-ancestor read error is logged and that ancestor is skipped; a
    total failure leaves the Step-1 prompt at its metadata-only content.

── Step 1: candidate-based diagnosis ─────────────────────────────────────────

4. Assemble a ProposeRequest from (node_id, error_signature, candidateSQL,
   sanitized dbt log, repo, commit_sha, ancestors, upstream diffs from 3a).
   The diffs are additive to the per-ancestor metadata bullets already sent —
   both are included whenever present.
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

- **Validation**: two non-streaming calls per proposal. Step 1 is given the candidate SQL, sanitized dbt log, ranked upstream ancestors, and — best-effort, up to 5 most-recently-changed eligible ancestors — the diff of each ancestor's recent upstream change, and returns `proposed_sql`, `rationale`, `confidence`, and `suspected_root_cause_node`. Step 2 is given the real model source and the Step-1 rationale, and returns a corrected `proposed_sql`. Step 2 is made only when the file path and service resolve; a failure, empty result, unchanged result, or low-confidence result falls back silently to the Step-1 candidate proposal.
- **Compile**: one non-streaming call per proposal. The adapter is given every gathered file (the offending file plus any co-located `.yml`/`.yaml` siblings and `dbt_project.yml`) and the sanitized dbt compile error, and returns `target_file` (which shown file to change), `proposed_content` (that file's complete corrected content), `rationale`, `confidence`, and `suspected_root_cause_node`.
- **Seed**: one non-streaming call per proposal. The adapter is given the failing CSV and the sanitized dbt seed error, and returns `proposed_content` (the complete corrected CSV), `rationale`, and `confidence`. The seed prompt has no `suspected_root_cause_node` field — a bad seed value has no upstream node to blame.

The `ProposeResult` struct carries all four possible fields (`proposed_sql`, `proposed_content`, `target_file`, plus `rationale`/`confidence`/`suspected_root_cause_node`/`model`) regardless of class; each fixer reads only the fields its prompt asked for. Both the Anthropic and the OpenAI-compatible adapter parse `target_file` and `proposed_content` from the tool-call arguments alongside `proposed_sql`.

If a response contains no tool call (or no choices), the adapter returns an error; the caller propagates it so the Redis message is redelivered and retried. If the tool call is present but the class-relevant content field (`proposed_sql` for validation, `proposed_content` for compile/seed) is empty or unchanged from the original, the fixer records the attempt as `failed` with no outbox emission (for validation Step 1 this also aborts the proposal entirely; for validation Step 2, compile, and seed this is a normal empty/unchanged outcome as described above).

### Idempotency-keyed response cache

The concrete `LLMProvider` is wrapped at boot by a best-effort caching decorator (`service/llmcache.CachingLLMProvider`) before it enters the fixers. `remediation.requested:v1` is consumed at least once; if the terminal write transaction fails after the LLM call, the *same* trigger is redelivered and the whole handler re-runs, which would re-pay the expensive completion. The decorator makes the external LLM call effectively-once **per trigger**: it keys each request on `sha256(LLM_MODEL ‖ inbound-idempotency-key ‖ canonical-JSON(ProposeRequest))` (hex, prefixed `llmcache:`), returns a cached `ProposeResult` on a hit, and on a miss calls the wrapped provider and caches the result. The model id is folded in so results from different models never collide.

The inbound idempotency key scopes the cache to a specific trigger. The driver derives it from the same identity that drives message-processing dedup — the upstream `outbox_entry_id` when present (stable across a Redis republish of the same logical trigger), otherwise the Redis message id — and threads it to the decorator through the request `context` (`llmcache.ContextWithIdempotencyKey`), so the `LLMProvider` port signature is unchanged and the fixers are untouched. This distinction matters: keying by request content alone would let two genuinely distinct triggers — for example successive remediation attempts for the same failure, which build a byte-identical prompt — collide, replaying an earlier (possibly failed) result and burning the attempt cap without re-consulting the model. With trigger-scoped keys, only a redelivery of the *same* trigger reuses the completion; a new attempt always gets a fresh call. If no idempotency key is present on the context, the decorator cannot tell a redelivery from a new trigger and bypasses the cache (calls the provider directly).

The cache is strictly best-effort and can never break the happy path: a `Get` error is treated as a miss and a `Put` error is swallowed — both are logged, neither is surfaced. Only the wrapped provider's own error propagates. The store is the `LLMResponseCache` port (`service/ports`), implemented by a Redis adapter (`adapters/redis`) over the service's existing Redis client. It stores only the small `ProposeResult` as JSON (never the prompt) under a per-entry TTL (`LLM_CACHE_TTL`, default 1h) and never scans keys. The shared Redis runs `noeviction` — it co-hosts the event streams and the OIDC sessions, which must not be evicted — so the TTL is the cache's only memory bound and is kept short deliberately: the key is scoped to the inbound trigger, so only a same-trigger redelivery (a Redis PEL sweep or outbox re-emit, seconds to minutes) needs to hit within it. A non-positive `LLM_CACHE_TTL` is clamped back to the default at load, so a misconfiguration can never disable expiry. A pathologically delayed redelivery past the TTL misses and re-calls, which is acceptable.

## LogSanitizer Seam

The `LogSanitizer` port sits between the raw S3/GitHub reads and prompt assembly. Every Fixer runs its fetched dbt log through it once (via the shared `loadDBTLog` helper), and every source string sent to the LLM passes through it too: the compile fixer sanitizes each shown file, the seed fixer sanitizes the CSV, validation's Step 2 sanitizes the real model source, and validation's Step-1 upstream-ancestor diffs are sanitized before truncation and embedding. The diff and no-op check always run against the raw content, since the fix is applied to the real file. The deployed implementation is currently pass-through: it returns its input unchanged. The seam exists so a redacting implementation can be dropped in without touching the handler or fixer logic.

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
- **Effectively-once LLM call**: because the whole handler re-runs on a redelivery (the LLM call precedes the write transaction), the external completion is protected by the best-effort idempotency-keyed response cache described under LLM Integration. A redelivery of the same trigger (same inbound idempotency key and request) hits the cache and skips the provider, so a failed terminal commit does not re-pay the expensive call. A genuinely new trigger — including a later remediation attempt for the same failure — uses a different key and always calls the model afresh.
- **Outbox dedup**: the `remediation.proposed:v1` entry carries a deterministic `event_id` (SHA1 UUID on `release_id|node_id|attempt`) so a redelivered downstream consumer can detect and suppress duplicates.

## Non-Responsibilities

`remediation-agent` generates proposals and exposes their lifecycle over gRPC. It does not:

- Create GitHub pull requests or open code review branches. PR creation is performed by ui-service, which holds the GitHub App write credential.
- Write to, commit to, or push any git repository. GitHub access is read-only.
- Auto-apply or merge any proposed SQL change.
- Merge, close, or comment on any pull request; the reconciler only reads PR status via GitHub's Pulls API. It observes GitHub's own merge/close decision, made by human reviewers, and mirrors it onto `pr_state`.
- Track whether a proposal was accepted or resulted in a passing release.

All code-change decisions — review, approval, and PR creation — are human actions.

## Background Loops

| Loop | Description |
|---|---|
| `remediation.requested:v1` consumer | Dispatches each inbound message to the `ProposeFix` handler. |
| Outbox publisher | Drains `remediation_agent_outbox` and XADDs each pending row to `remediation.proposed:v1`, `remediation.pr_opened:v1`, or `remediation.pr_closed:v1` depending on the row's stream field. |
| PR-outcome reconciler | Ticks every `REMEDIATION_PR_POLL_INTERVAL` (default 60s). Each pass lists up to 50 proposals with `pr_state='open'`, oldest-opened first; for each it calls `GET /repos/{repo}/pulls/{number}` (GitHub Pulls API) and maps a merged PR to `merged` and a closed-unmerged PR to `rejected`. A closed PR calls `Service.RecordOutcome`, which performs the single-winner CAS `pr_state: open → merged|rejected` and enqueues the `remediation.pr_closed:v1` outbox row in one transaction; a CAS miss (the row already left `open`) is a no-op. Per-row errors (a failed GitHub read or a failed `RecordOutcome`) are logged and skipped — one bad row never blocks the rest of the batch — and are retried on the next pass. A `401`, or a `403` that is not rate limiting (no `Retry-After` header and `X-RateLimit-Remaining` != 0), signals the token lacks `Pull requests: Read` and is classified as a permission error that flips the reconciler to a degraded state; rate-limited `403`s and `429`s are treated as transient and simply retried. While degraded, an actionable ERROR log (`grant the GitHub token 'Pull requests: Read'`) fires only on the healthy→degraded transition — not every pass — and a subsequent clean read clears it (recovery logged once). This distinguishes a standing permission gap — which stalls the whole close-loop and never self-resolves — from transient errors or genuinely still-open PRs. |

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
| `LLM_CACHE_TTL` | no | `1h` | Per-entry TTL for cached LLM propose results (Go duration). Only needs to cover the same-trigger redelivery window; kept short to bound memory on the shared `noeviction` Redis. A non-positive value is clamped to the `1h` default. |
| `GITHUB_TOKEN` | no | `""` | Read-only fine-grained PAT with `Contents: Read` and `Pull requests: Read` on the dbt repo. In Helm, sourced from `global.github.token` in the chart-managed secret `continuo-app-credentials`. When empty, requests to the Contents API are sent unauthenticated (subject to GitHub's lower unauthenticated rate limit) rather than failing outright. |
| `SERVICE_REPO_MAP_PATH` | no | `""` | Path to `service_repos.yaml`, which maps each dbt service name to its project root within the source repo. In Helm, set to `/etc/continuo/service_repos.yaml` and backed by the `continuo-app-service-repos` ConfigMap (built from `deploy/app/files/service_repos.yaml`). In docker-compose (dev/e2e), bind-mounted from `remediation-agent/config/service_repos.yaml`. Empty (or a service name absent from the map) means every fixer's source read has no repo path to resolve: compile and seed proposals are skipped, and validation's Step 2 degrades to the Step-1 candidate proposal. |
| `GITHUB_BASE_URL` | no | `https://api.github.com` | GitHub REST API root; override for e2e stub (`stub-github`) |
| `CONTINUO_ORCHESTRATOR_ADDR` | no | `orchestrator:50052` | Orchestrator gRPC endpoint |
| `REMEDIATION_AGENT_HTTP_PORT` | no | `8092` | `/healthz` port |
| `REMEDIATION_AGENT_GRPC_PORT` | no | `50054` | `RemediationProposals` gRPC server port |
| `REMEDIATION_AGENT_MAX_ATTEMPTS` | no | `3` | Per-`(source, node_id, error_signature)` attempt cap |
| `REMEDIATION_PR_POLL_INTERVAL` | no | `60s` | Interval between PR-outcome reconciler passes (Go duration). A non-positive value falls back to the default. |

## Key Code Paths

| Concern | Path |
|---|---|
| Proposal entity + unified diff | `remediation-agent/domain/proposal/proposal.go` |
| Prompt assembly (validation candidate + real-source, compile, seed) | `remediation-agent/domain/prompt/prompt.go` (`Assemble`, `AssembleSourceFix`, `AssembleCompileFix`, `AssembleSeedFix`) |
| Event payloads + deterministic IDs | `remediation-agent/domain/event/` (proposed, pr_opened, pr_closed) |
| Shared driver — attempt cap, dedup, persistence, outbox emit (each Fixer fetches its own dbt log) | `remediation-agent/service/handlers/propose_fix.go` |
| Per-error-class fixers — `Fixer` interface, `For` factory, shared single-shot pipeline | `remediation-agent/service/fixer/fixer.go` |
| Compile fixer (offending file + co-located YAML/`dbt_project.yml` context, one LLM call) | `remediation-agent/service/fixer/compile.go` |
| Seed fixer (CSV read, one LLM call) | `remediation-agent/service/fixer/seed.go` |
| Validation fixer (two-step candidate + real-source flow, best-effort upstream-diff gather) | `remediation-agent/service/fixer/validation.go` |
| PR lifecycle application service (claim/record/fail/record-outcome + outbox) | `remediation-agent/service/proposals/service.go` |
| PR-outcome reconciler loop (incl. permission-gap degraded signal) | `remediation-agent/service/proposals/reconciler.go` |
| Port interfaces | `remediation-agent/service/ports/` |
| Postgres UoW + proposal repo (incl. CAS for BeginPR and RecordPROutcome, open-PR listing) | `remediation-agent/adapters/postgres/` |
| S3 evidence reader + artifact writer | `remediation-agent/adapters/s3/` |
| gRPC ancestry client | `remediation-agent/adapters/grpc/ancestry_client.go` |
| gRPC `RemediationProposals` server | `remediation-agent/adapters/grpc/server.go` |
| GitHub read-only source reader (file read + directory list + commit diff) | `remediation-agent/adapters/github/source_reader.go` |
| GitHub read-only PR status reader (Pulls API) | `remediation-agent/adapters/github/pr_status.go` |
| Service→repo map config loader | `remediation-agent/config.go` (reads `SERVICE_REPO_MAP_PATH`, parses `service_repos.yaml`) |
| Service→repo map file (dev + e2e) | `remediation-agent/config/service_repos.yaml` |
| Service→repo map file (Helm chart) | `deploy/app/files/service_repos.yaml` (rendered into `continuo-app-service-repos` ConfigMap, mounted at `/etc/continuo`) |
| Anthropic LLM adapter | `remediation-agent/adapters/llm/anthropic.go` |
| OpenAI-compatible LLM adapter | `remediation-agent/adapters/llm/openai.go` |
| Best-effort LLM response caching decorator | `remediation-agent/service/llmcache/caching_provider.go` |
| Redis LLM response cache adapter | `remediation-agent/adapters/redis/llm_response_cache.go` |
| Pass-through log sanitizer | `remediation-agent/adapters/sanitizer/passthrough.go` |
| Redis consumer + outbox publisher | `remediation-agent/adapters/redis/` |
| DB migrations | `db/migration/remediation_agent/` (`V1__init_remediation_agent.sql` through `V7__pr_close_loop.sql`, including `V3__pr_creation.sql` for the PR-tracking columns and `V6__add_generating_proposal_status.sql` for the in-flight `generating` status) |
