# agent-remediation

## Purpose

`agent-remediation` acts on healable failures surfaced by the `remediation` classifier, across all four failure sources: `validation`, `compile`, `seed_build`, and `duplicate_table`. It consumes `remediation.requested:v1` — one trigger per failing node — and produces a fix proposal. A shared driver (`ProposeFix`) owns the attempt cap, inbound dedup, persistence, and the outbox emit; it dispatches each trigger to a `Fixer` chosen by the trigger's error class and, for a validation failure, the failing node's kind. Each `Fixer` decides which source files to read, whether it needs the dbt log (each class fetches and sanitizes it itself, only when needed), which prompt to send, and how to interpret the model's answer. Every fixer's prompt also carries precedent — how similar past failures (by exact error signature, or the broader category/reason class) were resolved before, read from the orchestrator's failure case base. A dbt node's validation failure carries a pre-compiled candidate SQL and uses a two-step LLM flow: a Step-1 diagnosis against the candidate SQL, the diff of what this release itself changed in the failing model (last promoted version vs. candidate), the diffs of its recently-changed upstreams, and precedent; then a best-effort Step-2 pass that applies the diagnosis to the failing node's real source, resolved primarily from the release's code bundle in object storage. A **python** node's validation failure runs a different lane entirely, because a python node is not a SQL file but an entry in a contract yaml declaring the relations it reads and the columns it produces: the agent fetches the team's repository at the failing commit, searches it for the file declaring the node, asks the model for that file's corrected content, packages the result with the same CLI the team's own release CI runs after a merge, and submits it to release-controller as a **shadow release** — a real release that runs the full parse → candidate-schema → validation pipeline but stops at the terminal `validated` status instead of promoting. That attempt is recorded `verifying` and finalized asynchronously by a reconciler: `proposed` when the shadow release validated the fix, `failed` with the shadow's own error when it did not — and that error becomes the evidence the next attempt is shown. Compile failures carry no candidate SQL; the agent reads the offending file named in the trigger and, for a `.sql` file, also gathers its co-located `schema.yml` siblings and the service's `dbt_project.yml`, then asks the model to pick and correct the one file that needs to change in a single LLM call. Seed-build failures read the failing CSV and ask the model for a corrected CSV in a single LLM call, with an honest failed-not-proposed outcome when the bad value cannot be inferred. Duplicate-table failures carry no dbt log at all — the rejection happens at parse time, before any Job runs — so the agent reads only the claimant the release changed, names the competing producer by service and path without reading its source, and asks the model for a rename in a single LLM call. For each successful proposal the driver — or, for a shadow-verified one, the reconciler — enqueues a pointer-only `remediation.proposed:v1` trigger so a downstream approver can review and apply the fix. Every invocation — whether it produces a proposal, is skipped, is escalated, or fails — is recorded in Postgres so no trigger is invisible. The agent never writes to or creates branches in any git repository; proposal application is a human action.

**Runtime**: Go service. HTTP `/healthz` on port 8092. gRPC `RemediationProposals` server on port 50054. Depends on Postgres (`continuo_agent_remediation`), Redis, S3, the orchestrator gRPC endpoint (four narrow read RPCs — `GetNodeLocation`, `GetUpstreamChanges`, `GetNodeVersions`, `GetPrecedents` — behind one `GraphClient`, port 50052), release-controller's HTTP API (shadow-release submission and verdict polling), and GitHub, exclusively via read-only GETs against the Contents API (source file/directory reads), the repository tarball endpoint (whole-tree fetch for the python lane), and the Pulls API (PR status polling for the outcome reconciler, and by-branch PR lookup for its opening sweep). The service image also carries the `continuo-runtime` CLI, which the packaging adapter runs as a subprocess to merge and hash-fold a corrected contract directory.

## Owned Storage

Postgres database `continuo_agent_remediation`. Tables:

| Table | Purpose |
|---|---|
| `proposal` | One row per attempt. Records `source` (`validation`, `compile`, `seed_build`, or `duplicate_table`), `release_id`, `remediation_round` (default `1`; the release's remediation round this attempt belongs to — a human's "try again" on a rejected release starts a new round with its own attempt budget), `node_id`, `error_signature`, `attempt`, `status`, `confidence`, `rationale`, `proposed_sql_uri`, `diff_uri`, `source_resolved`, `model`, `created_at`, source-location columns (`repo`, `commit_sha`, `file_path` — populated when `source_resolved=true`), and `file_edits` (JSONB, default `'[]'`) — the list of files this attempt changes, each `{path, content_uri, diff_uri}`, populated alongside `repo`/`commit_sha`/`file_path` under the same `source_resolved=true` condition. A row whose `file_edits` is empty and whose `file_path` is non-empty (a row written before this column existed) is read as a single edit synthesized from `file_path`/`proposed_sql_uri`/`diff_uri`, so every reader sees a uniform edits list regardless of which schema version wrote the row. Shadow-verification columns: `shadow_release_id` (the release judging this attempt's fix, empty for every attempt judged synchronously), `verify_error` (why that release rejected the fix — the evidence the next attempt is shown), and `trigger_payload` (JSONB, default `'{}'`: the raw inbound trigger, stored only while an attempt is awaiting a verdict so the reconciler can rebuild it to start the next attempt). `status` lifecycle: a row is written `generating` (in flight, just before the model is called) and then either finalized directly to one terminal state — `proposed`, `skipped`, `failed`, or `escalated` — or, when the fix's correctness can only be judged by running it, written `verifying` and finalized later by the shadow-verify reconciler to `proposed` or `failed`. `verifying` is the one non-terminal status a fixer can return, and it is excluded from the attempt count exactly as `generating` is, so an in-flight verification neither inflates the cap nor shifts the attempt number. `candidate_fix_sql_uri`/`candidate_fix_diff_uri` are populated only for `validation` proposals (the Step-1 fix applied to the pre-compiled candidate SQL); always empty for `compile`/`seed_build`/`duplicate_table`, which have no candidate SQL. PR-tracking columns: `pr_url`, `pr_number`, `pr_state`, `pr_opened_at`, `pr_opened_by`, `pr_closed_at`, `pr_claimed_at`. `pr_state` lifecycle: `'' → opening → open → merged | rejected`, with `opening → failed` as a retryable error path; `merged` and `rejected` are terminal. Entering `opening` requires `source_resolved=true` **and** `status='proposed'`, both carried by the claim's own compare-and-set: a python contract fix is written `verifying` with `source_resolved=true` while a shadow release is judging it, and a rejection changes nothing but the status, so status is the only thing that separates a fix a human may open a PR for from one that is unverified or already refused. `pr_claimed_at` (nullable) is stamped with the claiming wall-clock time whenever `BeginPullRequest` moves a row into `'opening'`, and cleared back to `NULL` on every exit from `'opening'` (`RecordPullRequest`, `FailPullRequest`) — so a row re-claimed after `opening → failed → opening` always carries the second claim's time, and the reconciler's opening sweep can measure a claim's exact age. This clear-on-exit guarantee is enforced at the database boundary, not by each writer individually: a `BEFORE UPDATE` trigger (`proposal_stamp_pr_claimed_at`, `V10__stamp_pr_claimed_at_on_opening.sql` + `V11__clear_pr_claimed_at_on_opening_exit.sql`) stamps `pr_claimed_at` with `clock_timestamp()` whenever a row's `pr_state` is becoming `'opening'` and the column is still `NULL` at that point — so every claim carries a value regardless of which binary version performed the `UPDATE`, including one that predates the column and never sets it itself — and unconditionally clears `pr_claimed_at` back to `NULL` on every transition out of `'opening'`, regardless of what value (if any) the transitioning statement itself wrote to the column. The clear-on-exit clause exists because the fill-when-NULL clause alone is not enough across a rolling upgrade: a binary that predates the column writes its `RecordPullRequest`/`FailPullRequest`-equivalent `UPDATE` without mentioning `pr_claimed_at` at all, so without an unconditional clear the column would keep the exiting claim's stale value in place, and a subsequent re-claim by that same old binary — whose `UPDATE` also never mentions the column — would inherit it instead of getting a fresh stamp. A row that is `'opening'` with `pr_claimed_at IS NULL` is therefore expected only under schema corruption or a manual edit; the sweep still treats it as unmeasurable and never sweeps it, as a defensive backstop. A PR reopened on GitHub after reaching `merged`/`rejected` is not tracked — the reconciler's outcome pass only watches `open` rows (its opening sweep separately watches `opening` rows, to recover or release a stuck claim). Unique on `(release_id, source, node_id, attempt)`; the terminal write upserts on this key so it finalizes the in-flight generating row (`remediation_round` is not part of the conflict key — it is carried onto the row and left unmodified by that upsert, since a redelivery of the same attempt is always the same round). A secondary index on `(release_id, remediation_round, source, node_id, error_signature)` (`idx_proposal_round_node_signature`, `V15__proposal_remediation_round.sql`) supports the attempt-count lookup, which counts terminal rows only (`status NOT IN ('generating','verifying')`) for one release *and* remediation round: the cap is a budget per release per round, so a later release's or a later round's attempts at the same failure are never charged to an earlier release's or round's count. |
| `remediation_agent_outbox` | Transactional outbox; one row per emitted event (`remediation.proposed:v1`, `remediation.pr_opened:v1`, or `remediation.pr_closed:v1`), drained by the outbox publisher. Status: `pending`, `processed`, `failed`. |
| `message_processing` | Shared shape consumed by `pkg/messageprocessing`; FK target of `remediation_agent_outbox.message_processing_id`. |

The `proposal` table records one row per attempt: it is written `generating` when the model call begins and finalized in place to a terminal outcome. All terminal outcomes — proposed, escalations, skips, and LLM failures — are recorded so the full attempt history is queryable.

## Inbound Interfaces

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `remediation.requested:v1` | `agent-remediation-remediation-requested` | Emitted by the `remediation` classifier for each healable failing node. Each message drives one `ProposeFix` invocation. |

### gRPC server — `RemediationProposals` (port 50054)

Exposes proposal data and the PR lifecycle to ui. Handlers are thin and delegate to application services; all persistence goes through the `ProposalRepository` port. Every `Proposal` and `BeginPullRequestResponse` message carries a `repeated FileEdit edits` field, mapped from the domain object's `Edits` list — a nil or empty list produces an absent repeated field, never a list containing a zero-valued element. The single-file fields on the same messages (`file_path`, `proposed_sql_uri`, `diff_uri`) are populated straight from the domain object's own same-named fields, not derived from `edits[0]`: they equal `edits[0]` whenever `edits` is non-empty, and for a validation proposal whose real-source step did not resolve they describe the candidate fix artifact while `edits` is empty. The `Proposal` message also carries `shadow_release_id` and `verify_error`, mapped verbatim from the row: the release that ran this attempt's fix through the validation pipeline (set while the attempt is `verifying` and kept on the `proposed`/`failed` row it became) and why that release rejected it. Both are empty on an attempt judged without a release. They are on the wire because they are what lets a reader connect a proposal to the release deciding it, and say why a failed one failed, instead of showing a status word with the reason unread in the database. It also carries `remediation_round`, mapped verbatim from the row, so a caller can tell which round's attempt budget a given proposal was charged against.

| Method | Description |
|---|---|
| `ListProposals(filter)` | Returns proposals ordered `created_at DESC`, all fields including `pr_*`. Supports filtering by `status`, `pr_state`, and/or `release_id` (inbox view: `status='proposed'` AND `pr_state IN ('', 'failed')`; a release page passes `release_id` alone to see its own remediation history across every round). |
| `GetProposal(id)` | Returns a single proposal. Returns `NOT_FOUND` when the id is unknown. |
| `BeginPullRequest(id)` | Atomic compare-and-set: transitions `pr_state` from `''` or `'failed'` to `'opening'`, allowed only when `source_resolved=true`, and stamps `pr_claimed_at` with the current time read from the service's `Clock` port. Returns `{repo, commit_sha, file_path, proposed_sql_uri, branch_name, release_id, node_id, rationale, confidence, diff_uri, model, claimed_at, edits}` on success — `edits` is the same list carried on the proposal row (or its single-file-field synthesis) — `claimed_at` (RFC3339) is `pr_claimed_at` read back from the row, not the client's own clock, and the caller must present it back to `FailPullRequest` to release this exact claim. Returns `FAILED_PRECONDITION` with the existing `pr_url` when the proposal is already `opening` or `open`; also returns `FAILED_PRECONDITION` when `source_resolved=false`. This is the single-winner idempotency guard that prevents concurrent duplicate PRs. |
| `RecordPullRequest(id, pr_url, pr_number, opened_by)` | Sets `pr_state='open'`, `pr_url`, `pr_number`, `pr_opened_at=now()`, `pr_opened_by`, and clears `pr_claimed_at` back to `NULL`. Emits `remediation.pr_opened:v1` via the transactional outbox. |
| `FailPullRequest(id, claimed_at)` | Compare-and-set: resets `pr_state` from `'opening'` to `'failed'` and clears `pr_claimed_at` back to `NULL`, but only when the row's current `pr_claimed_at` still equals the `claimed_at` argument — the same repository CAS the reconciler's opening sweep uses (`FailStuckOpeningPR`, via `Service.FailStuckClaim`). Called by ui after its own `BeginPullRequest` claim, passing back the `claimed_at` that call returned. Returns `{released: bool}`; `released=false` is not an error — it means the claim already moved on (recorded, or already released by the opening sweep) by the time this call ran, which can happen when the S3/GitHub round trip between `BeginPullRequest` and this call outlives `REMEDIATION_PR_OPENING_GRACE_PERIOD`. Returns `INVALID_ARGUMENT` when `claimed_at` is missing or not RFC3339. |

### HTTP (port 8092)

Two health endpoints backed by the `pkg/liveness` registry, with readiness and
liveness deliberately split:

- `GET /healthz` — readiness. Fails (503) when the `remediation.requested:v1`
  consumer or the outbox publisher has exited with an error, the consumer
  read-loop heartbeat has gone stale, **or** any dependency probe (Redis,
  Postgres) fails. A dependency outage pulls the pod out of the Service
  endpoints so no traffic is routed to it.
- `GET /livez` — liveness. Fails (503) **only** for worker/heartbeat failures —
  dependency probes are excluded, so a Redis/Postgres outage does not restart a
  pod whose consumer is already retrying, while a dead or wedged consumer does.

The Kubernetes `readinessProbe` points at `/healthz` and the `livenessProbe` at
`/livez` (`deploy/continuo/values.yaml`: `probePath: /healthz`,
`livenessPath: /livez`).

## Outbound Interfaces

### Redis producer

| Stream | Consumed by | Emitted when |
|---|---|---|
| `remediation.proposed:v1` | (approval surface) | A fix becomes ready for human review. Two writers reach this point and both build the event through one shared helper, so they cannot drift apart: the driver, when the dispatched `Fixer` returns a `status=proposed` outcome it could judge on the spot (compile, seed_build, duplicate_table, dbt validation); and the shadow-verify reconciler, when the release judging a python contract fix reports `validated`. |
| `remediation.pr_opened:v1` | `orchestrator` (group `orchestrator-remediation-pr-opened-proposals`) | `RecordPullRequest` is called; payload is pointer-only: `proposal_id`, `release_id`, `node_id`, `pr_url`, `pr_number`, `opened_by`, `opened_at`. Orchestrator's case-base proposals consumer records the opened PR as a `:Proposal` node linked from the corresponding `:Rejection`. |
| `remediation.pr_closed:v1` | (no consumer; audit seam) | The PR-outcome reconciler observes a terminal GitHub PR state and `RecordOutcome` performs the CAS `open → merged | rejected`; payload is pointer-only: `proposal_id`, `release_id`, `node_id`, `pr_url`, `pr_number`, `outcome` (`merged` or `rejected`), `closed_at` (RFC 3339). `event_id` is a deterministic SHA1 UUID keyed on `release_id|node_id|attempt`, distinct from the `pr_opened` id derived from the same triple. |

All events are written to `remediation_agent_outbox` inside the same transaction as the `proposal` row insert and published with a deterministic `event_id` for consumer-side dedup.

### gRPC calls to `orchestrator`

`GraphClient` (`adapters/grpc/graph_client.go`) serves four narrow read ports over the orchestrator's `OrchestratorQuery` gRPC service. `Locate`, `UpstreamChanges`, and `CurrentVersion` each degrade a `NOT_FOUND` response to an empty/absent result rather than an error; `Precedents` has no such branch and returns any gRPC error as-is, which is moot in practice since `GetPrecedents` never returns `NOT_FOUND` (an unmatched signature is an empty list — see `services/orchestrator.md`). Any other gRPC error, on any of the four, is returned to the caller, which decides per port whether that is fatal or a best-effort degrade (see below).

| Method | Port | Purpose |
|---|---|---|
| `GetNodeLocation` | `NodeLocator.Locate` | Returns the failing node's `file_path` and `service_name`. Called by the validation fixer as a fallback, for PR targeting and as the location input to its code-bundle/repo source resolution, only when `file_path` is absent from the trigger — release-controller otherwise threads both fields onto the trigger from the candidate topology; best-effort — a failure degrades Step 2 to the candidate-only proposal. Called as a fallback by the seed fixer only when `file_path` or `service` is absent on the trigger; a failure or empty result there skips the proposal. Not called by compile (the offending path comes from the trigger and the service is the trigger's `node_id`) or duplicate_table (path/service are threaded directly from the trigger, since a duplicate-table rejection can name a node that was never promoted into the active topology `GetNodeLocation` serves). |
| `GetUpstreamChanges` | `UpstreamChangeReader.UpstreamChanges` | Returns the failing node's most-recently-changed upstream ancestors with their code and config diffs, most-recent first — server-capped at 5 ancestors and 8 KiB per diff. Called once by the validation fixer for the Step-1 prompt; best-effort, degrades to no upstream section on failure. Not called by any other fixer. |
| `GetNodeVersions` (`current_only=true`) | `VersionReader.CurrentVersion` | Returns the code the failing node runs *now* — the diff baseline for "what this release changed in the failing model." Called once by the validation fixer, and only when its own resolved source is non-empty; best-effort, a failure or absent current version simply omits the own-change-diff section. Not called by any other fixer. |
| `GetPrecedents` | `PrecedentReader.Precedents` | Returns past rejections matching the trigger's `error_signature`, or — when that has no matches — the broader `(category, reason)` pair, filtering out the failure's own rejection. Called by every fixer (`loadPrecedents`) once its own skip checks pass: the shared single-shot pipeline calls it for compile, seed, and duplicate_table; the validation fixer calls it directly. Best-effort; a failure degrades to no precedent section. |

Its own inbound gRPC surface (`RemediationProposals`) is described in the inbound interfaces section above.

### Outbound S3 — code bundle

| Operation | Purpose |
|---|---|
| `GetObject` (`CandidateSourceReader.NodeSource`, `adapters/s3/candidate_source_reader.go`) | Reads the release's code-bundle document at the trigger's `code_bundle_uri` and decodes it via the shared `pkg/codebundle` contract decoder, returning the one failing node's `raw_code`/`runtime` entry after confirming the document's `release_id` matches the trigger's own — a stale or misrouted object could otherwise resolve to another release's bundle naming the same `unique_id`. Called only by the validation fixer, as the primary source of the failing node's real source — used both for the own-change diff and as the base for the Step-2 real-source fix. An empty URI, an absent object, an oversized object, a body that fails `codebundle.Decode` (malformed JSON, an unsupported `contract_version`), a document for a different `release_id`, a document that decodes but carries no entry for the node, or a dbt-runtime entry with an empty `raw_code` — all map to `ErrNotFound`, a permanent miss that falls back to a GitHub repo read (below) rather than redelivering the trigger. Any other error (S3 unreachable, a non-`NoSuchKey` failure reading the object) is transient and propagates, so the trigger is redelivered. |

### Outbound HTTP — GitHub Contents API

| Endpoint | Purpose |
|---|---|
| `GET /repos/{repo}/contents/{path}?ref={commit_sha}` | Read-only fetch of one file's raw text at the release commit. `Accept: application/vnd.github.raw+json`. Authenticated with `Authorization: Bearer <GITHUB_TOKEN>` when the token is set; unauthenticated otherwise. A 404 maps to `ErrSourceNotFound`. For the compile, seed, and duplicate-table fixers' offending-file read, a 404 is a definitive skip (no retry) and any other non-2xx status or network error is returned to the caller as a transient failure so the trigger is redelivered. The validation fixer uses this endpoint only as its code-bundle fallback (see below): any error there — 404 or otherwise — degrades silently to an unresolved source (Step 2 is skipped; the Step-1 candidate proposal stands), with no retry. Response bodies over 1 MiB are rejected rather than silently truncated. |
| `GET /repos/{repo}/contents/{dir}?ref={commit_sha}` | Read-only directory listing. `Accept: application/vnd.github+json`. Returns the repo-relative paths of the files (not sub-directories) directly under `{dir}`. Used only by the compile fixer to find `.yml`/`.yaml` siblings co-located with a failing `.sql` model; a 404 or any error is swallowed (this context is best-effort) and the read is simply skipped. |
| `GET /repos/{repo}/tarball/{commit_sha}` | Read-only fetch of the whole repository tree at the release commit, as a gzipped tar. Same base URL and credential as the Contents API calls. Used only by the python validation fixer, which has no recorded path for the contract yaml declaring the failing node and so must search the tree for it. The archive is extracted into a fresh temporary directory: GitHub's single generated top-level directory is stripped, symlinks are skipped rather than recreated, any entry that is absolute or escapes the extraction root aborts the extraction, and total decompressed bytes are capped at 200 MiB — so an untrusted archive can neither redirect a write outside the directory nor fill the disk. A 404 maps to `ErrSourceNotFound`, which ends the attempt as `skipped` with the reason recorded; any other non-2xx or network error is transient and the trigger is redelivered. The extracted tree is removed as soon as the attempt finishes, and on any failed fetch before returning. |

The `{path}` for the offending file is formed by joining the owning service's `repo_path` (from `service_repos.yaml`, keyed by service name) with the dbt-project-relative source path. For compile, the service key is the trigger's `node_id` (the synthetic service id) and the file path comes directly from the trigger. For seed_build, the file path and service are threaded from the candidate topology on the trigger, falling back to the orchestrator's `GetNodeLocation` when either is absent. For duplicate_table, the file path and service are threaded directly from the trigger's `file_path`/`service` (the rename target release-controller selected), with no orchestrator fallback — a duplicate-table rejection can name a node that was never promoted. For validation, the failing node's source is resolved by `resolveCandidateSource`: the release's code bundle first, keyed by `unique_id` (no path needed); only on a permanent bundle miss does it fall back to this Contents API read, joining `service_repos.yaml`'s prefix for the `service_name` against the `file_path`, both already resolved — from the trigger's own `file_path`/`service` when present, or via `GetNodeLocation` as a fallback otherwise. No write requests are issued anywhere in this service; it holds no GitHub write permissions.

The compile, seed, and duplicate-table fixers treat their offending-file `ReadFile` as load-bearing: a 404 is a definitive skip, any other error is transient and the trigger is redelivered. The compile fixer's extra context reads (co-located `.yml`/`.yaml` files, `dbt_project.yml`, via `ListDir`) swallow every error, including 404s, since that context is optional. The validation fixer's repo-read fallback sits behind the code-bundle read and is best-effort at that higher level: because it is only reached after a permanent bundle miss, *any* error there — 404, network, or otherwise — degrades silently to an unresolved source (no own-change diff; Step 2 skipped) rather than causing a retry. Only a *transient* code-bundle fetch error (not this repo-read fallback) redelivers the trigger.

`GITHUB_TOKEN` is injected at deploy time from the chart-managed secret `continuo-app-credentials` (key `GITHUB_TOKEN`, sourced from `global.github.token` in Helm values). No out-of-band secret mechanism is used.

### Outbound HTTP — GitHub Pulls API

| Endpoint | Purpose |
|---|---|
| `GET /repos/{repo}/pulls/{number}` | Read-only fetch of one pull request's current state, used by the PR-outcome reconciler. `Accept: application/vnd.github+json`. Authenticated the same way as the Contents API calls above. Any non-2xx response (including 404) is an error: the caller leaves the proposal untouched and retries on the next reconcile pass. On success, `state="closed"` with `merged=true` maps to outcome `merged`; `state="closed"` with `merged=false` maps to outcome `rejected`; `state="open"` is not yet actionable and the row is left for a later pass. |
| `GET /repos/{repo}/pulls?head={owner}:{branch}&state=all&per_page=1` | Read-only lookup by head branch, used by the reconciler's opening sweep to find a pull request for a claimed-but-unrecorded proposal. An empty result array means no PR exists for the branch (not an error). Any non-2xx response is an error and the row is retried next pass. |

This is the only GitHub write-adjacent surface agent-remediation reads from besides source files; the request is still a GET, so GitHub access remains exclusively read-only.

### Outbound HTTP — release-controller

The `ReleaseGateway` port (`adapters/releasehttp`) is the python lane's client of release-controller's public HTTP API. It is the only place in this service that issues a non-GET request to another service, and everything it can create is a shadow release — one that never promotes.

| Endpoint | Purpose |
|---|---|
| `POST /releases` | Submits a shadow verification release for a packaged contract fix. The body always carries `kind: "python"`, `bootstrap: false`, and `shadow: true` — a shadow submission never varies them, so none is a caller-supplied field — plus the shadow's own `release_id`, the failing node's `service`, the `image_tag` read back from the original failing release, and that release's `repo`/`commit_sha`. `202 Accepted` is success, including release-controller's idempotent-duplicate case where a row for this release id already exists, so a redelivered attempt resubmits harmlessly. |
| `GET /releases/{shadow_id}` | Polls a submitted shadow release for its verdict. `validated` is a terminal pass; `rejected` is a terminal fail, and its per-node error text is read from each failing validation node's `run_results_uri` through the same S3 evidence reader the fixers use (falling back to the release-level `reject_reason`/`reject_detail` when that structured result is missing or unreadable). Every other status is non-terminal and the read is repeated on the next pass. |
| `GET /releases/{original_id}` | Read twice against the ORIGINAL failing release, for two different fields. Its verdict's per-node errors name every node that release rejected, which is how the fixer finds out whether another node it would package alongside the one it is fixing also failed — a fix that could never be verified is skipped before any model call. And `image_tags[service]` is read before submitting. The trigger carries no image tag, and a shadow release needs one to be accepted; reusing the failing release's tag is safe precisely because a shadow release never promotes, so the tag never reaches the production pointer an image tag otherwise drives. An absent or empty tag for the service is an error and the attempt is retried. |

## Data Flow

### Shared driver — `ProposeFix`

The driver in `service/handlers/propose_fix.go` runs for every trigger regardless of error class. It owns everything that is not class-specific; the class-specific work is delegated to a `Fixer`.

```
1. Decode trigger: extract source, release_id, remediation_round, node_id,
   relation_id, error_signature, category, reason, error_excerpt, dbt_log_uri,
   candidate_artifact_uri, code_bundle_uri, file_path, service, node_type,
   other_service, other_file_path, repo, commit_sha.
   remediation_round is the release's remediation round this trigger belongs
   to; missing or 0 (a trigger predating this field) normalizes to 1, the
   round every rejection starts a release at. It is threaded onto every
   proposal row this trigger produces and onto the remediation.proposed:v1
   event it emits.
   node_type is the failing node's kind (dbt-model, dbt-seed, dbt-snapshot,
   python-model, python-csv), set on duplicate_table and validation triggers.
   It selects the Fixer for a validation trigger — a python-model node goes to
   the python contract fixer, a python-csv node to its own dedicated contract
   fixer — and the duplicate-table fixer skips either python kind on it.
   error_excerpt is the classifier's key error line for this failure, capped at
   4 KiB; it is the python contract fixer's primary evidence, since a python
   validation failure's message is the engine's own text rather than a dbt log
   the fixer can re-read. file_path/service are the node's source location,
   set on seed_build, duplicate_table, and validation triggers, all three from
   the candidate topology. other_service/other_file_path locate the competing
   node that also produces the contested relation. relation_id is the contested
   physical relation itself, distinct from node_id (the target claimant's own
   unique_id) — the two differ whenever the target already carries an alias.
   other_service, other_file_path, and relation_id are all set
   only on a duplicate_table trigger, empty otherwise. reason is the
   classifier's finer-grained rule (e.g. logic:missing_object); with category
   it forms the fallback precedent-lookup key when error_signature has no
   recorded matches. code_bundle_uri locates the release's code-bundle
   document; empty only for compile-stage rejections, which precede the parse
   that produces the bundle (duplicate_table, seed_build, and validation
   rejections all follow a completed parse and carry it), and consumed only
   by the validation fixer to resolve the failing node's real source.

1a. Read-only dedup pre-check (no write): before any row is written, check
    whether this trigger was already handled, on either dedup axis scoped to the
    consuming stream — a message_processing row matching (message_id, stream_name)
    OR (outbox_entry_id, stream_name). If found, ACK and return without writing
    anything. This
    keeps a re-emitted completed trigger (fresh Redis message id, same
    outbox_entry_id) from minting a phantom in-flight 'generating' row for a
    fresh attempt number that the terminal claim would then abandon.

2. Count prior TERMINAL attempts for (release_id, remediation_round, source,
   node_id, error_signature). The release is part of the key: a later release
   is new code, so the same node failing with the same signature under it
   starts a fresh count at attempt 1 instead of inheriting an exhausted one.
   The remediation round is part of the key for the same reason within one
   release: a human's "try again" on a rejected release starts a fresh round
   with its own count, though the attempt number itself keeps incrementing
   across rounds of the same release (attempt is scoped to (release_id,
   source, node_id) alone). In-flight 'generating' and 'verifying' rows are
   excluded, so neither the in-progress attempt nor one still awaiting a
   shadow release's verdict inflates the cap or shifts the attempt number
   across a redelivery.
   - attempts >= MaxAttempts (default 3): insert proposal(status=escalated),
     emit nothing, done. (This path is before markGenerating, so an unhealable
     escalated node never shows the "Generating fix…" indicator.)

3. Resolve the Fixer via fixer.For(source, node_type): compileFixer, seedFixer,
   duplicateTableFixer, or — for a validation trigger — validationFixer for a dbt
   node, pythonValidationFixer when node_type is python-model, and
   csvValidationFixer when node_type is python-csv. node_type selects a lane
   only for validation failures, where the three node kinds need entirely
   different fixes; every other class ignores it and refuses either python
   node kind in its own way. An unrecognized source is a programming error —
   the classifier only ever emits the four known values — and is returned
   loudly, not swallowed.

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
      candidate_fix_sql_uri/candidate_fix_diff_uri). A 'verifying' outcome also
      stores shadow_release_id and the raw inbound trigger in trigger_payload,
      so the reconciler that resolves that release can rebuild the trigger and
      start the next attempt from it; every other status resolves inside this
      call and needs nothing to replay. repo/commit_sha/file_path
      and edits — the same file list, one FileEdit{path, content_uri, diff_uri}
      per changed file — are populated only when the Fixer reports
      source_resolved=true; every single-shot Fixer (compile, seed,
      duplicate_table) always reports source_resolved=true on a proposed
      outcome and writes exactly one edit, while validation's edits list is
      empty whenever its Step 2 real-source fix did not resolve (the Step-1
      candidate proposal stands alone).
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
   is transient and redelivers). Fetch precedent (loadPrecedents: by
   error_signature, falling back to (category, reason) when the signature has
   no matches; best-effort, no section on failure). Then make a single forced
   propose_fix LLM tool call showing every gathered file — each run through the
   LogSanitizer first, so raw source never leaves for the LLM — that error, and
   the precedent section. The model returns target_file (which shown file to
   change) and proposed_content (that file's complete corrected content).
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
   - Fallback: either is empty — call the orchestrator's
     GetNodeLocation(node_id) to resolve them. A gRPC error, or both still
     empty after the fallback: proposal(status=skipped), done.
2. Look up service in the service→repo mapping. Unmapped: proposal(status=skipped), done.
3. Read the CSV at <repo_path>/<file_path>.
   - 404: proposal(status=skipped), done (definitive; not retried).
   - Any other error: return it (transient; message redelivered).
4. Fetch and sanitize the dbt seed error via loadDBTLog (only now, once the CSV
   read above has succeeded; not-found → "", any other error is transient and
   redelivers). Fetch precedent (loadPrecedents, same signature-then-category/
   reason lookup as compile; best-effort). Then make a single forced
   propose_fix LLM tool call with the CSV content — run through the
   LogSanitizer first, so raw seed rows never leave for the LLM — that error,
   and the precedent section. The prompt is CSV-specific: it names the three concrete failure
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

### Duplicate-table fixer

`duplicateTableFixer` (`service/fixer/duplicate_table.go`) resolves a naming collision — two models in the release produce the same warehouse relation — with exactly one LLM call, reusing the same single-file gather/build/interpret shape as the compile fixer (`singleFileInterpret`).

```
1. node_type is python-model or dbt-seed: proposal(status=skipped), done,
   before any read is attempted.
   - python-model: its relation is declared in the service's contract.yaml,
     not in the file file_path names (the contract's script entry, a program
     that produces the relation but does not name it), and this system
     carries no repository path for contract.yaml at all — so reading and
     renaming the script could not fix the collision.
   - dbt-seed: its relation name comes from the CSV filename or the
     project's seed config, never from the CSV's own contents, so the named
     file cannot contain the fix either — and treating an edited CSV as a
     valid rename proposal would mean accepting an edit to the seed's DATA,
     not its identity.
2. Empty file_path or service on the trigger: proposal(status=skipped), done.
3. Look up service in the service→repo mapping. Unmapped: proposal(status=skipped), done.
4. Read the offending file at <repo_path>/<file_path> — the claimant the
   release changed. The competing claimant's source is never read: its
   other_service and other_file_path (from the trigger) are enough for the
   model to choose a distinguishing name.
   - 404: proposal(status=skipped), done (definitive; not retried).
   - Any other error: return it (transient; message redelivered).
5. No dbt log is fetched at any point: a duplicate-relation rejection happens
   at parse time, before any Job runs, so there is none to read. Fetch
   precedent (loadPrecedents, same signature-then-category/reason lookup as
   compile and seed; best-effort).
6. Make a single forced propose_fix LLM tool call naming the contested
   relation (relation_id, not node_id — the two differ once the target
   carries an alias, and naming node_id would tell the model to stop
   producing a relation it may not even write), the competing producer
   (other_service, other_file_path), the offending file's sanitized content,
   and the precedent section, instructing the model to rename only what the
   file produces (alias, configured schema, or model name) without altering
   its logic, columns, or ordering. The model returns target_file and
   proposed_content (no suspected_root_cause_node — a naming collision has no
   upstream node to blame).
   - LLM transient error → retry.
7. Interpret the result via the same singleFileInterpret used by the compile
   fixer: target_file must resolve to the one shown file (exact or unambiguous
   suffix match; empty resolves to it since it is the sole file shown).
   proposed_content empty, identical to the original, or a low-confidence
   result → proposal(status=failed). Otherwise: proposed outcome.
8. On a proposed outcome, diff the corrected content against the original and
   write proposed-fix/<release_id>/<node_id>/attempt-<n>.source.sql and
   .source.diff (same key shape as compile/seed). Insert
   proposal(status=proposed, source_resolved=true, file_path=target_file),
   emit remediation.proposed:v1.
```

When no claimant belongs to the changed service — a bootstrap release, or two already-promoted services colliding while a third is released — `file_path`/`service` on the trigger name a dbt claimant whose source lives in a repository this trigger's `repo`/`commit_sha` do not describe, the read returns `ErrSourceNotFound`, and the fixer skips rather than proposing a change to a file it cannot see; the release page's `reject_detail` already names every claimant so an operator can rename one by hand.

### Validation fixer — two-step flow

`validationFixer` (`service/fixer/validation.go`) is the one class that carries a pre-compiled candidate SQL and still runs two LLM calls: a first diagnosis against that candidate — plus the diff of this release's own change, recent upstream changes, and precedent — then a best-effort second pass that applies the diagnosis to the failing node's real source.

```
0. node_type == python-model on the trigger: proposal(status=skipped), done,
   before anything at all is read. A python node's candidate artifact is a JSON
   validation spec (declared reads + output columns), not SQL, and the code
   bundle records its source as the normalized contract entry rather than the
   script the repository holds — neither is model source a dbt fix can be
   proposed against. The dispatcher routes a python validation trigger to the
   python contract fixer, so this check is normally unreachable; it stays
   because a trigger carrying no node_type at all resolves to this Fixer, and
   the node's kind must still be able to stop it here. The empty-URI check
   below cannot do that job, because a python node's candidate_artifact_uri is
   non-empty.
1. Empty candidate_artifact_uri on the trigger: proposal(status=skipped), done. This
   is decided before any evidence is fetched, so a transiently unreadable dbt log
   URI cannot turn the intended skip into a redelivery. This is the dbt-seed
   case: a seed has no candidate artifact, so there is nothing to fix.
2. Fetch the candidate SQL from S3 at candidate_artifact_uri (required; any error is
   transient and the trigger is redelivered), then fetch and sanitize the dbt log
   via loadDBTLog (not-found → ""; any other error is transient and redelivered).
   Both reads happen only after the two skips above.
3. Resolve the PR target. The trigger's own file_path/service — stamped by
   release-controller from the candidate topology — are used when present. Only
   a trigger carrying no file_path falls back to the orchestrator's
   GetNodeLocation(node_id), which serves the PROMOTED topology: the rejected
   release was never promoted, so that lookup holds nothing for a newly-added
   node and the previous release's path for a node whose candidate moved it.
   The fallback is best-effort: on error, proceed with both empty (source
   resolution below then has no location to fall back on, and Step 2 degrades).
4. Resolve the failing node's real source (resolveCandidateSource): the
   release's code bundle first (CandidateSourceReader.NodeSource, keyed by
   node_id and the trigger's release_id — no path needed). A transient
   bundle-fetch error returns and the trigger is redelivered. A bundle entry
   whose runtime is not "dbt" ends the whole flow with proposal(status=skipped)
   — its raw_code is a contract entry, not model source, and no repo read
   substitutes for it; this backs the node_type guard in step 0 for a trigger
   that carries no node_type. A permanent bundle miss (ErrNotFound — empty
   URI, absent object, a document for a different release_id, no entry for
   this node, or a dbt entry with an empty raw_code) falls back toward a
   GitHub repo read: an empty file_path or service_name from step 3, or a
   service with no entry in service_repos.yaml, degrades silently to an
   unresolved source (""). Once a repo path is actually about to be read,
   extension inference applies only when the trigger carries no node_type: a
   resolved path that doesn't end in ".sql" then ends the flow the same way as
   the runtime check above, with proposal(status=skipped), because with no
   node_type the only non-dbt source this system ever tracks a path for is a
   python node's script. A trigger that does carry a node_type (a dbt model,
   seed, or snapshot — python already ended the flow in step 0) is trusted
   over the extension and always proceeds to the GitHub read at
   <repo_path>/<file_path>, since a dbt snapshot's source can legitimately be a
   ".yml" file rather than ".sql". Any error from that read — 404, network, or
   otherwise — degrades silently to an unresolved source ("").
5. If the resolved source is non-empty, call the orchestrator's
   GetNodeVersions(node_id, current_only=true) for the code the node runs
   *now*, diff it against the resolved source, sanitize, and truncate to
   ~8 KiB — this becomes the own-change-diff section ("what this release
   changed in the failing model"). Best-effort: a lookup error or an absent
   current version (a new node) simply omits the section.
6. Call the orchestrator's GetUpstreamChanges(node_id) for the failing node's
   most-recently-changed upstream ancestors, server-capped at 5 ancestors and
   8 KiB per diff. Best-effort: an error proceeds with no upstream section.
   Each returned diff (code and config) is sanitized before it reaches the
   prompt; the orchestrator's cap is not re-applied client-side.
7. Fetch precedent (loadPrecedents: by error_signature, falling back to
   (category, reason) when the signature has no matches; best-effort, no
   section on failure).

── Step 1: candidate-based diagnosis ─────────────────────────────────────────

8. Assemble a ProposeRequest from (node_id, error_signature, candidateSQL,
   sanitized dbt log, the own-change diff from step 5, the upstream diffs from
   step 6, and the precedent section from step 7).
9. Forced single-shot LLM tool call (propose_fix): the LLM must invoke the tool;
   no streaming; result parsed from the tool arguments (proposed_sql, rationale,
   confidence, suspected_root_cause_node).
   - Transient LLM error → retry.
10. proposed_sql is empty: proposal(status=failed), emit nothing, done.
11. Write candidate artifacts to S3 (unconditionally; audit trail — this is the
    LLM's fix applied to the pre-compiled candidate SQL, not the real model source):
      proposed-fix/<release_id>/<node_id>/attempt-<attempt>.sql
      proposed-fix/<release_id>/<node_id>/attempt-<attempt>.diff
    These become candidate_fix_sql_uri / candidate_fix_diff_uri. Default:
    proposed_sql_uri / diff_uri point here (source_resolved=false).

── Step 2: real-source fix ────────────────────────────────────────────────────

12. The source resolved in step 4 is empty, or file_path/service_name from step 3
    is empty, or service_repos.yaml has no mapping for service_name: skip Step 2,
    keep the candidate proposal (logged warning; no error returned). Step 2
    performs no read of its own — it reuses the source already resolved in
    step 4 (bundle or repo fallback); the repo mapping is needed here only to
    record the file's full repository path on the proposal.
13. Forced single-shot LLM tool call (propose_fix) with the resolved source —
    passed through the same LogSanitizer.Sanitize seam used elsewhere — and the
    Step-1 rationale as context (AssembleSourceFix). Result is a corrected
    version of that source.
    - LLM error, empty result, an unchanged result, or a low-confidence result
      (confidence == "low"): skip Step 2, keep the candidate proposal
      (logged warning; no error returned).
14. Write real-source artifacts to S3:
      proposed-fix/<release_id>/<node_id>/attempt-<attempt>.source.sql
      proposed-fix/<release_id>/<node_id>/attempt-<attempt>.source.diff
    Promote: proposed_sql_uri / diff_uri now point at the source artifacts
    (source_resolved=true).
15. Build the proposal: confidence, rationale, model, and suspected_root_cause_node
    all come from the Step-1 result. source_resolved, repo, commit_sha, and
    file_path reflect whether Step 2 succeeded (step 14) or was skipped (steps
    12–13), keeping the Step-1 candidate proposal in the latter case.
```

The degrade-don't-fail design means any failure in source resolution or Step 2 — a permanent code-bundle miss, a repo-read error, a missing file_path/service_name, no repo mapping, or an empty/unchanged/low-confidence LLM result — silently falls back to the candidate proposal. None of these ever lose or retry the trigger: only a transient code-bundle fetch error (step 4), Step 1 itself (steps 8–10), and the offending-file reads in the compile/seed/duplicate-table fixers are load-bearing enough to cause a retry.

### Python validation fixer — repair the contract, then run it

`pythonValidationFixer` (`service/fixer/python_validation.go`) handles a validation rejection whose failing node is a python node. A python node is not a SQL file: it is an entry in a contract yaml naming its schema and table, the script that produces it, the relations it reads (each with the SQL that selects them), and the `output_columns` it promises. Validation runs that contract against the candidate schema — it bind-checks each declared read and builds the empty typed table from the declared columns — and rejects the node when what the contract promises does not hold. So the fix belongs in that yaml, and it cannot be judged by reading it back, only by running it. This lane therefore ends in a submitted release rather than a finished proposal.

```
1. Split node_id into schema and table (the trailing two dot-separated
   segments). An id naming neither: proposal(status=skipped) with the reason
   recorded, done. An empty service on the trigger: same — the service names
   both the object-storage location the packaged contract is uploaded to and
   the release it is submitted under, so without one there is nothing to
   submit.
2. Fetch the repository tarball at the trigger's repo/commit_sha and extract it
   (RepoArchive.Fetch). A missing repo or commit is permanent:
   proposal(status=skipped) with the reason recorded, since redelivering would
   retry it forever. Any other fetch error is transient and the trigger is
   redelivered. The extracted tree is removed when the attempt finishes.
3. Locate the declaring yaml (ContractLocator.Locate): walk the checkout for every
   *.yml/*.yaml file, parse each as a contract document, and take the one whose
   nodes list contains an entry matching the schema and table, folding case on
   both sides. A file over 1 MiB, or one that does not parse as yaml, is skipped
   with a log rather than failing the search. Zero matches or more than one
   match — the duplicate-node gate upstream should make the latter impossible,
   so seeing it means the repository has drifted from the topology — both end
   the attempt as proposal(status=skipped) with the reason recorded, never a
   guess. The match yields the file's repo-relative path and verbatim text, the
   contract directory holding it, and that directory's parent as the service's
   application root.
4. Check whether a fix for this node can be verified at all. Read the ORIGINAL
   failing release's verdict (ReleaseGateway.Verdict), whose NodeErrors name
   every node that release rejected, and resolve each OTHER failing node id
   against the same checkout with the same ContractLocator search. A node that
   resolves to the SAME contract directory ends the attempt as
   proposal(status=skipped), naming it in the rationale. The classifier emits
   one trigger per failing node, but step 9 packages the whole directory, so
   the shadow release re-runs every node declared in it: a second broken node
   there would reject that release however correct the fix is, the rejection
   would become the next attempt's evidence naming a node the prompt never
   shows the model, and the whole attempt budget would be spent on full
   validation releases that could not have passed — each holding the single
   global release slot. A failing node that no contract yaml declares (a dbt
   model), or one declared in another service's directory, is not packaged by
   this fix and does not block it. A verdict that cannot be read is transient
   and the trigger is redelivered, exactly as the image-tag read of the same
   release in step 12 already is.
5. Assemble evidence. The failure text (the trigger's error_excerpt) and the
   declaring file are the load-bearing pair; every other section degrades to
   its own absence rather than blocking the heal:
   - the full runner log at dbt_log_uri when present,
   - the node's contract entry as the release parsed it, read from the code
     bundle (a non-python runtime there means the trigger and the bundle
     disagree about what this node is, so the section is omitted rather than
     shown as something it is not; a permanent miss omits it too, and only a
     transient fetch error is returned),
   - recently-changed upstream ancestors' contract diffs (GetUpstreamChanges),
   - precedent (loadPrecedents, the same signature-then-category/reason lookup
     every other fixer uses),
   - every earlier attempt at this same failure, oldest first, each with the
     diffs it applied and the error its shadow release reported back. This is
     the section that makes attempt n+1 better informed than attempt n; the
     in-flight attempt's own row is excluded, and no row limit is applied
     because the attempt cap already bounds how many can exist.
   Every string in the prompt passes through the LogSanitizer seam.
6. One forced propose_python_fix LLM tool call returning updated_files — a list
   of {path, content} pairs, each the COMPLETE new content of a file — plus
   rationale and confidence. An empty list is proposal(status=failed).
7. Refuse an unusable answer before anything is written to disk:
   - a path outside the contract directory the model was shown (naming another
     service, traversing upwards, or absolute): proposal(status=failed);
   - the same path returned twice: proposal(status=failed), because applying
     the list leaves the LAST entry on disk — which is what gets packaged and
     verified — while the recorded edits describe an earlier one, so a human
     would approve content that was never the content validated.
8. Apply the files to the checkout, reading each path's prior content first so
   the recorded diffs describe the change against what the repository held.
   Only then package: running the packager before the answer is written would
   verify the contract that already failed.
9. Check that the node survived its own repair, before anything is packaged.
   A shadow release can only reject what the packaged contract still declares,
   so an answer that DELETED or RENAMED the failing node — or DELETED the read
   that could not bind — leaves the release nothing to fail on: it validates,
   and the edit that removed the broken thing is recorded as a proven fix and
   offered to a human. Two checks close that:
   - Every node the answer's own files declared BEFORE it was applied must
     still be declared after, with its identity fields — schema, table, script,
     kind, owner, schedule, criticality — unchanged, and with every read name it
     declared still declared (ContractInspector.Declarations, read on both
     sides of each returned *.yml/*.yaml file). A missing or mutated entry is
     proposal(status=failed) naming the node and the fields that moved; an
     entry that lost a read name is proposal(status=failed) naming the read.
     Read names are compared verbatim, so renaming one is dropping it: the
     name is how the node's script asks for that read, and this fix never
     edits the script. Both sides are compared as a whole rather than file by
     file, so moving an entry between two of the answer's own files — which
     packages identically — is not read as deleting it. What the answer ADDS is
     not refused — a new node, a new read, or a rewritten read's SQL is
     packaged, so the shadow release judges it honestly; only subtraction is
     refused, because it leaves the release less to judge than the failure it
     was asked to repair. A returned file that no longer parses as yaml is
     proposal(status=failed) too, since "declares nothing" and "cannot be
     read" must not be confused.
   - The declaring-file search is re-run over the patched checkout
     (ContractLocator.Locate): the node must still be declared by exactly one
     file, still inside the contract directory being packaged. This covers what
     comparing only the returned files cannot — a second declaration added in a
     new file, which leaves every touched entry intact while making the
     packaged directory declare one node twice. ErrNodeNotDeclared,
     ErrAmbiguousDeclaration, or a different contract directory each end the
     attempt as proposal(status=failed); any other search error is transient
     and the trigger is redelivered.
10. Package with ContractPackager.Merge — a subprocess call to
   `continuo-runtime merge <contractDir> --service <service> --repo-root
   <appRoot> --dialect <dialect> --out <tmp>/contract.yaml`, the same command
   the team's own release CI runs after a merge. The hash fold verifies against
   topology-controller because the tool is the same, not because anything is
   bypassed. --repo-root is the service's own directory, never the enclosing
   repository root: script paths and the in-repo import closure resolve against
   it, so the wrong base folds a different set of files and the release is
   rejected for a hash mismatch unrelated to the fix. --dialect comes from the
   install's configured warehouse engine (see WAREHOUSE_ENGINE below), so the
   contract is rendered under the same rules the pipeline validates it against.
   A nonzero exit — the CLI having read the contract and refused it — is
   deterministic and ends the attempt as proposal(status=failed), keeping the
   CLI's stderr as the rationale. It cannot be retried: the tool answers the
   same way every time, and a redelivery rebuilds the identical contract because
   the model's answer is served from the trigger-keyed idempotency cache, so a
   transient classification would loop until the stream's poison limit dropped
   the message and left the attempt in 'generating' forever. Every other
   packaging failure — a missing binary, a context deadline, a killed process —
   is transient and the trigger is redelivered. The adapter draws the line
   (`ports.ErrContractRejected`), so the fixer never string-matches stderr.
11. Mint the shadow release id — shadow-<original_release_id>-<node_id>-a<n>,
    with every character outside [A-Za-z0-9._-] replaced by a dash. It is
    unique per attempt, legible in every log line and release listing, and
    stable across a redelivery of the same attempt (release-controller's
    submission is idempotent on it). The id is capped at 52 bytes: the release
    it names gets a candidate schema of "_candidate_" plus the id, and
    PostgreSQL truncates an identifier past 63 bytes rather than rejecting it —
    which would cut the attempt suffix and the node name, letting two attempts
    share one schema and validate against each other's leftovers. Past the cap
    the release-and-node middle is shortened and given an 8-hex digest of what
    it held, so the prefix and attempt number stay whole and two nodes whose
    names diverge only past the cut still get separate schemas.
12. Upload the merged contract to <service>/<shadow_id>/contract.yaml — the
    canonical per-release key release-controller reads a python service's
    release artifact from — BEFORE submitting, because release-controller
    reads that object as soon as the submission is accepted.
13. Read the ORIGINAL failing release's image tag for the service
    (ReleaseGateway.ImageTag), then submit the shadow release
    (ReleaseGateway.Submit) under the minted id.
14. Write one audit artifact pair per edited file:
      proposed-fix/<release_id>/<node_id>/attempt-<n>/edit-<i>.content
      proposed-fix/<release_id>/<node_id>/attempt-<n>/edit-<i>.diff
    Edits of one attempt share a directory and are numbered within it, so two
    edits can never write the same key.
15. Return proposal(status=verifying) naming the shadow release, with the edits
    list, source_resolved=true (the edits name real files at a real commit,
    which is what a pull request needs — the shadow release decides whether
    they are right), and the single-file view (file_path/proposed_sql_uri/
    diff_uri) normalized from the first edit.
```

### CSV validation fixer — same shadow-release lane, a contract-only node

`csvValidationFixer` (`service/fixer/csv_validation.go`) handles a validation rejection whose failing node is a python-csv node. A python-csv node has no script at all: it is an entry in a contract yaml naming its schema and table, a single `csv` read whose value is the uri of the file the runtime loads, and the `output_columns` it promises that file to carry. Validation fetches the file's header line and rejects the node when a declared output column is missing from it. The CSV file is the source of truth, so the fix corrects the contract to match it — rename or drop a mis-declared `output_columns` entry, or, only when the evidence shows the uri itself is stale, correct the `csv:` uri.

Steps 1–4 (split node id, fetch the repo, locate the declaring yaml, check no sibling in the same contract directory also failed) are not merely identical in shape to the python-model lane's — `csvValidationFixer.Propose` and `pythonValidationFixer.Propose` both call one function for them, `locateContractForFix`, which returns either the located contract (schema, table, checkout root, and `Located`) or an already-decided `proposal(status=skipped)`/error for the caller to return as-is. Steps 6–15 (one forced LLM tool call, refuse an unusable answer, apply then package, mint and submit the shadow release, write audit artifacts, return `proposal(status=verifying)`) are likewise one shared function, `proposeContractFixViaShadow`. The two lanes differ only in the two seams passed into it: what evidence step 5 shows the model, and what step 9's post-apply guard checks.

- **Step 5 (evidence)** reuses the python lane's evidence assembly (`pythonEvidence`) verbatim — a csv node runs on the same python-runtime image and its code-bundle entry is read the same way — and converts the result to `CsvEvidence`, which mirrors `PythonEvidence` field for field. Only the prompt built from it differs: `propose_csv_fix` (vs. `propose_python_fix`), telling the model the file is the source of truth, that there is no script to preserve, and that the single `csv` read may have its uri corrected but never be deleted or renamed.
- **Step 9 (post-apply guard)** shares its identity check with the python lane — every node the answer's own files declared before the edit must still be declared, with schema, table, script, **kind**, owner, schedule, and criticality unchanged (`identityBreach`, factored out of the python lane's guard as `buildDeclarationMaps` + `identityBreach` so both lanes enforce it identically, and checked *before* either lane's read rule — a re-identified entry is refused on that alone before its reads are even compared). Kind flipping a node's declared kind — most dangerously python-csv into python-model or back — is refused here the same as renaming its table would be, since a flipped kind changes which of these two rule sets governs the node. What differs is the *read* rule applied to the failing node itself: a python-model node's script may perform any number of reads, so `declarationBreach` refuses dropping any of them for every node it checks; a python-csv node has exactly one, always named `csv`, and correcting its value is the expected fix. So for a node that declares a `csv` key, `csvDeclarationBreach` gives dropping that key a csv-specific message naming the read by name — fired first, ahead of the generic one. It does not replace the generic rule: every node, csv node included, is still run through the same blanket "no read may be dropped" rule (`droppedReadBreach`) the python-model lane applies everywhere, including every read of a python-model sibling packaged alongside the csv node, whose script this fix never touches. For the failing csv node itself, which declares exactly one read, the two checks catch the identical breach; the csv-specific message only gets to report it first.

### Shadow-verify reconciler

`service/shadowverify` resolves every attempt left in `verifying`. It ticks every `SHADOW_VERIFY_POLL_INTERVAL` (default 15s), lists up to 20 such proposals oldest-first, and for each reads its shadow release's verdict:

- **Validated** — `MarkVerified` performs the CAS `verifying → proposed` and, in the same transaction, enqueues the `remediation.proposed:v1` outbox row that surfaces the fix for review, so the row and the announcement cannot disagree. A repeated pass over an already-finalized row writes nothing and emits nothing. Because no inbound message drove this write, the outbox entry carries no `message_processing` provenance.
- **Rejected** — `MarkVerifyFailed` performs the CAS `verifying → failed`, recording as `verify_error` the failing node's own error text; when the fixed node itself passed and other nodes did not — a fix that broke something downstream of itself — every failing node is named instead, in a stable order. That transaction commits **before** the next attempt is started, because the driver counts terminal attempts to decide both the next attempt's number and whether the cap has been reached. Only the pass that actually wins the CAS starts a next attempt.
- **Neither** — the release is still working, and the row is left exactly as it is unless it has been RUNNING longer than `SHADOW_VERIFY_TIMEOUT` (default 20m), in which case it is failed with `shadow verification timed out`. The budget is measured from the release's activation moment — the timestamp of its first transition past `received`, read from the verdict — not from when the attempt was recorded. A shadow release joins the same global FIFO queue as every other release and only one release runs at a time, so measuring from the proposal would fail an attempt whose release never ran, and would do the same to every retry behind it. A release still queued (no activation moment yet) is left alone however old the attempt is.

A verdict that cannot be *read* is deliberately not treated as a rejection: the gateway reports a briefly unreachable release-controller and a release id it will never know with the same error, so counting either as a failed attempt would let one outage burn the attempt budget on fixes that were never judged. The read is retried next pass, and the same timeout ends one whose verdict never becomes readable — measured there from when the attempt was recorded, since an unreadable release has no activation moment to measure from and this is the path that must stay bounded regardless: a submission that was lost would otherwise sit in `verifying` for as long as the row exists.

The next attempt is started from the trigger stored on the failed attempt's own row, decoded by the same decoder the stream consumer uses, and given a fresh dedup identity — `shadow-verify:<shadow_release_id>`, with the upstream outbox entry id dropped. Replaying the stored payload verbatim would collide with the claim the first attempt's own transaction already made and return having done nothing, leaving no attempt, no row, and no error to show for it. There is exactly one shadow release per attempt, so this identity is both fresh per attempt and stable across repeated passes over the same one. A next attempt that fails to start is logged rather than retried: the row it would follow is already terminal, so the following pass no longer lists it, and the failure stands as the recorded outcome. The driver commits an in-flight `generating` row of its own just before calling the model, so when it errors here that row is failed with the reason (`FailGenerating` on the (release_id, source, node_id, error_signature) attempt). The release is part of the match: the same node failing the same way under two releases at once shares a source, node id and error signature, so a triple-only match would fail the OTHER release's in-flight attempt and spend one of the attempts that release was allowed. On the stream consumer's path the unacknowledged message is redelivered and reuses that row; nothing redelivers an attempt started from a release's verdict, so without this step the row would report a fix as still being generated indefinitely.

### Artifact keys

The single-file fixers (compile, seed, duplicate_table, and validation's real-source step) write their corrected file and unified diff under the attempt-level keys `proposed-fix/<release_id>/<node_id>/attempt-<n>.source.sql` and `.source.diff` (or, for validation's Step-1 candidate, `attempt-<n>.sql`/`.diff`), and every proposed outcome from them carries exactly one edit built from that pair. The python contract fixer, whose answer is a list of files, writes one pair per edit under `proposed-fix/<release_id>/<node_id>/attempt-<n>/edit-<i>.content` and `.diff`.

### Outbox publisher

A background goroutine drains `remediation_agent_outbox` rows with `status='pending'` and XADDs each to its stream, injecting `outbox_entry_id` for downstream dedup.

## LLM Integration

The `LLMProvider` port is backed by one of three adapters selected at boot via `LLM_PROVIDER`:

| Value | Target | Notes |
|---|---|---|
| `anthropic` | Anthropic API (`https://api.anthropic.com`) | Model from `LLM_MODEL` env var (e.g. `claude-haiku-4-5`). |
| `openai` | OpenAI API (`https://api.openai.com`) | Model from `LLM_MODEL`. |
| `openai-compatible` | Operator-supplied endpoint (`LLM_BASE_URL`) | Used for local stub-llm in dev and e2e environments; model from `LLM_MODEL`. |

Every adapter forces a tool call on every call — no streaming, no free-form text response. Four of the five fixers force `propose_fix`, whose answer describes one file; the python contract fixer forces `propose_python_fix`, whose answer is an `updated_files` array of `{path, content}` objects. Both tools' parameter schemas are rendered by one builder, which gives an array parameter an `items` subschema whose properties are all required, so the model is told an element's exact shape rather than inferring it from the parameter description. The number of calls and which tool fields are populated differ per error class:

- **Python validation** (a validation trigger whose `node_type` is `python-model`): one non-streaming `propose_python_fix` call per attempt. The adapter is given the failure text, the runner log, the node's contract entry as the release parsed it, the verbatim contract yaml declaring it, upstream contract diffs, precedent, and every earlier attempt's diffs and shadow-release error, and returns `updated_files` (each changed file's complete new content), `rationale`, and `confidence`. The prompt constrains the model to what validation actually checks — the node's declared reads, `output_columns`, and config — and forbids touching the fields that identify the node (schema, table, script path, owner, schedule, criticality), since changing one makes it a different node rather than a fixed one. There is no `suspected_root_cause_node` field: the verdict comes from a real release, not from the model's guess.
- **Validation (dbt)**: two non-streaming calls per proposal. Step 1 is given the candidate SQL, sanitized dbt log, the diff of what this release itself changed in the failing model (last promoted version vs. candidate, from the orchestrator's `GetNodeVersions`), the diffs of its recently-changed upstreams (from `GetUpstreamChanges`, server-capped at 5 ancestors/8 KiB each), and precedent, and returns `proposed_sql`, `rationale`, `confidence`, and `suspected_root_cause_node`. Step 2 is given the failing node's resolved real source (the release's code bundle, or a GitHub repo read on a permanent bundle miss) and the Step-1 rationale, and returns a corrected version of it. Step 2 is made only when that source, the file path, and the service all resolve; a failure, empty result, unchanged result, or low-confidence result falls back silently to the Step-1 candidate proposal.
- **Compile**: one non-streaming call per proposal. The adapter is given every gathered file (the offending file plus any co-located `.yml`/`.yaml` siblings and `dbt_project.yml`), the sanitized dbt compile error, and precedent, and returns `target_file` (which shown file to change), `proposed_content` (that file's complete corrected content), `rationale`, `confidence`, and `suspected_root_cause_node`.
- **Seed**: one non-streaming call per proposal. The adapter is given the failing CSV, the sanitized dbt seed error, and precedent, and returns `proposed_content` (the complete corrected CSV), `rationale`, and `confidence`. The seed prompt has no `suspected_root_cause_node` field — a bad seed value has no upstream node to blame.
- **Duplicate table**: one non-streaming call per proposal, made only when the target claimant's `node_type` is `dbt-model` or `dbt-snapshot` — a python or dbt-seed target is skipped before any call (a python node's relation lives in contract.yaml, not the named file; a seed's relation name comes from its CSV filename or project config, not the CSV's own content, and treating an edited CSV as a proposal would mean accepting a data edit). The adapter is given the offending file (the claimant the release changed), the contested relation (`relation_id`, not `node_id` — the two differ once the target carries an alias), the competing producer's `other_service`/`other_file_path` — never the competing file's content — and precedent, and returns `target_file`, `proposed_content` (the complete corrected file, renaming what it produces), `rationale`, and `confidence`. No dbt log is involved (there is none) and no `suspected_root_cause_node` field (a naming collision has no upstream node to blame).

Every fixer's precedent section (`loadPrecedents`, described under gRPC calls to `orchestrator` above) is fetched via `GetPrecedents` and rendered the same way regardless of class: the first few resolved precedents in full (excerpt, resolution diff, fix-PR link), every other match as a one-line mention.

The `ProposeResult` struct carries every possible field (`proposed_sql`, `proposed_content`, `target_file`, `files` — the multi-file answer — plus `rationale`/`confidence`/`suspected_root_cause_node`/`model`) regardless of class; each fixer reads only the fields its prompt asked for. Both the Anthropic and the OpenAI-compatible adapter parse `target_file`, `proposed_content`, and `updated_files` from the tool-call arguments alongside `proposed_sql`.

If a response contains no tool call (or no choices), the adapter returns an error; the caller propagates it so the Redis message is redelivered and retried. If the tool call is present but the class-relevant content field (`proposed_sql` for validation, `proposed_content` for compile/seed/duplicate_table) is empty or unchanged from the original, the fixer records the attempt as `failed` with no outbox emission (for validation Step 1 this also aborts the proposal entirely; for validation Step 2, compile, seed, and duplicate_table this is a normal empty/unchanged outcome as described above).

### Idempotency-keyed response cache

The concrete `LLMProvider` is wrapped at boot by a best-effort caching decorator (`service/llmcache.CachingLLMProvider`) before it enters the fixers. `remediation.requested:v1` is consumed at least once; if the terminal write transaction fails after the LLM call, the *same* trigger is redelivered and the whole handler re-runs, which would re-pay the expensive completion. The decorator makes the external LLM call effectively-once **per trigger**: it keys each request on `sha256(LLM_MODEL ‖ inbound-idempotency-key ‖ canonical-JSON(ProposeRequest))` (hex, prefixed `llmcache:`), returns a cached `ProposeResult` on a hit, and on a miss calls the wrapped provider and caches the result. The model id is folded in so results from different models never collide.

The inbound idempotency key scopes the cache to a specific trigger. The driver derives it from the same identity that drives message-processing dedup — the upstream `outbox_entry_id` when present (stable across a Redis republish of the same logical trigger), otherwise the Redis message id — and threads it to the decorator through the request `context` (`llmcache.ContextWithIdempotencyKey`), so the `LLMProvider` port signature is unchanged and the fixers are untouched. This distinction matters: keying by request content alone would let two genuinely distinct triggers — for example successive remediation attempts for the same failure, which build a byte-identical prompt — collide, replaying an earlier (possibly failed) result and burning the attempt cap without re-consulting the model. With trigger-scoped keys, only a redelivery of the *same* trigger reuses the completion; a new attempt always gets a fresh call. If no idempotency key is present on the context, the decorator cannot tell a redelivery from a new trigger and bypasses the cache (calls the provider directly).

The cache is strictly best-effort and can never break the happy path: a `Get` error is treated as a miss and a `Put` error is swallowed — both are logged, neither is surfaced. Only the wrapped provider's own error propagates. The store is the `LLMResponseCache` port (`service/ports`), implemented by a Redis adapter (`adapters/redis`) over the service's existing Redis client. It stores only the small `ProposeResult` as JSON (never the prompt) under a per-entry TTL (`LLM_CACHE_TTL`, default 1h) and never scans keys. The shared Redis runs `noeviction` — it co-hosts the event streams and the OIDC sessions, which must not be evicted — so the TTL is the cache's only memory bound and is kept short deliberately: the key is scoped to the inbound trigger, so only a same-trigger redelivery (a Redis PEL sweep or outbox re-emit, seconds to minutes) needs to hit within it. A non-positive `LLM_CACHE_TTL` is clamped back to the default at load, so a misconfiguration can never disable expiry. A pathologically delayed redelivery past the TTL misses and re-calls, which is acceptable.

### Prompt logging

The concrete `LLMProvider` is also wrapped by a logging decorator (`service/promptlog.LoggingLLMProvider`), and the boot order is `cache(log(provider))` — the log decorator wraps the raw provider, the cache wraps the log decorator. So a prompt is logged exactly when the model is actually called: a cache hit returns before reaching the log decorator and records nothing, because nothing is fed to the model. Each real call logs one `llm prompt` record at INFO carrying the forced tool name, the system-prompt and user-content byte sizes, and the full `System` and `User` text — the complete prompt sent to the model. The driver stamps the failure identity (`source`, `release_id`, `node_id`, `attempt`) onto the request `context` (`promptlog.ContextWithFailure`) alongside the cache idempotency key, so each logged prompt is correlated to the failure it addresses without changing the `LLMProvider` port signature or touching the fixers. The logged content is exactly what leaves the process for the external model: every source-derived string in the prompt has already passed through the `LogSanitizer` seam during assembly (see below), so logging exposes nothing the request does not already send. Unlike the response cache — which stores only the small `ProposeResult`, never the prompt — this decorator is the one place the assembled prompt itself is captured.

## LogSanitizer Seam

The `LogSanitizer` port sits between the raw S3/GitHub/orchestrator reads and prompt assembly. Every Fixer runs its fetched dbt log through it once (via the shared `loadDBTLog` helper) — duplicate_table skips this step, since it fetches no log — and every source-derived string sent to the LLM passes through it too: the compile fixer sanitizes each shown file, the seed fixer sanitizes the CSV, the duplicate-table fixer sanitizes the offending file, validation's Step 2 sanitizes the resolved real source, and validation's own-change diff and each upstream ancestor's code/config diff are sanitized before they reach the prompt (the own-change diff is also truncated client-side to ~8 KiB after sanitizing; the orchestrator's `GetUpstreamChanges` diffs arrive already capped and are not re-truncated). The python contract fixer sanitizes every section it assembles: the trigger's error excerpt, the runner log, the code bundle's contract entry, the declaring yaml's verbatim text, each upstream diff, each precedent, and each earlier attempt's recorded error and diffs. The diff and no-op checks always run against the raw content, since the fix is applied to the real file. The deployed implementation is currently pass-through: it returns its input unchanged. The seam exists so a redacting implementation can be dropped in without touching the handler or fixer logic.

## Payload Shape (`remediation.proposed:v1`)

The trigger is pointer-only: it carries no SQL/CSV text, no log content, and no warehouse data. Consumers fetch the artifacts from S3 using the supplied URIs.

| Field | Description |
|---|---|
| `event_id` | Deterministic SHA1 UUID keyed on `release_id\|node_id\|attempt`. Stable on redelivery. |
| `source` | Origin pipeline: `validation`, `compile`, `seed_build`, or `duplicate_table`. |
| `release_id` | The release identifier from the inbound trigger. |
| `remediation_round` | The release's remediation round this attempt belongs to; `1` for the rejection itself, incremented by each human "try again". |
| `node_id` | The unique_id of the failing node. |
| `error_signature` | Release-stable normalized dedup key from the classifier (SHA-256 hex). |
| `proposed_sql_uri` | S3 URI of the best available proposed fix content. For compile, seed_build, and duplicate_table, always the source artifact (`attempt-<n>.source.sql`, containing the corrected file's content whether it is SQL, YAML, or CSV). For a dbt validation proposal, points to the real-source artifact (`attempt-<n>.source.sql`) when `source_resolved=true`; falls back to the candidate artifact (`attempt-<n>.sql`) when `source_resolved=false`. For a python contract fix, the first edit's content artifact (`attempt-<n>/edit-0.content`); the full list is on the proposal row and its gRPC projection. |
| `diff_uri` | S3 URI of the unified diff corresponding to `proposed_sql_uri` (`attempt-<n>.source.diff`, a validation candidate fallback's `attempt-<n>.diff`, or a python contract fix's `attempt-<n>/edit-0.diff`). |
| `source_resolved` | `true` when the URIs above point at a real-source artifact. Always `true` for a compile, seed_build, duplicate_table, or python contract proposal. For a dbt validation proposal, `true` only when Step 2 succeeded; `false` when only the Step-1 candidate proposal is available. |
| `rationale` | Short rationale from the LLM (no warehouse data). |
| `confidence` | `low`, `medium`, or `high`. |
| `suspected_root_cause_node` | Optional node_id the LLM identified as the root cause. Populated by validation and compile; never set by seed_build or duplicate_table (neither has an upstream node to blame). |
| `model` | The LLM model identifier used for this proposal. |
| `attempt` | Monotonically increasing attempt number for this `(release_id, node_id)`, spanning every remediation round of the release — it never resets when a round bumps (see below). |
| `proposed_at` | RFC 3339 timestamp of the proposal. |

## Attempt Cap and Escalation

For each `(release_id, remediation_round, source, node_id, error_signature)` key the service enforces a cap (default `AGENT_REMEDIATION_MAX_ATTEMPTS=3`). Before any S3 fetch or LLM call, the handler counts existing terminal `proposal` rows matching the key. If the count is already at or above the cap, it inserts a `proposal(status=escalated)` row and emits nothing. The trigger is consumed and ACKed; escalation is auditable in the `proposal` table.

The cap is a budget per release and per remediation round, not per failure across releases or rounds. Within one release the count grows through the shadow-verification loop, where a python node's rejected fix is retried with the shadow's error as new evidence; a compile, seed, or dbt validation failure is judged synchronously and normally records one attempt per release per round. A later release that fails the same node with the same signature is a new change and gets its own budget — a rejected fix for an earlier release is precedent for it (via the case base), not a spent attempt. `remediation_round` is decoded from the inbound trigger (missing or `0` normalizes to `1`, the round every rejection starts a release at) and is what release-controller bumps when a human asks a rejected release to "try again" — that request replays the same failing nodes' `remediation.requested:v1` triggers with the incremented round, giving each one a fresh attempt budget for the same failure.

The `attempt` number itself, by contrast, is not round-scoped: the `proposal` table's uniqueness (`release_id, source, node_id, attempt`) spans every round of a release, so the handler numbers each attempt from the cumulative TERMINAL count across every round so far (`countAttempts` sums the per-round query from round 1 up to the trigger's own round), not from the round-scoped count the cap check uses. A round's first attempt therefore continues the release's existing sequence — the ordinary case being a round-2 retry of a node round 1 already escalated — rather than restarting at 1 and colliding with (and silently overwriting, via the terminal upsert's `ON CONFLICT`) a row an earlier round already wrote at that attempt number.

## Consumer Reliability

- **Inbound idempotency**: the write transaction first claims the inbound message in `message_processing`, keyed on both the Redis message id and the upstream `outbox_entry_id`. The first key catches a Redis replay (a message redelivered after the work committed but before the ACK); the second catches an outbox republish (the classifier re-emitting the same row with a fresh Redis message id). On either conflict the transaction rolls back and the message is ACKed, so a redelivered trigger produces no second `proposal` row and no second `remediation.proposed` emit. A transient error before commit rolls the claim back with the rest of the work, so the message stays in the PEL for a clean retry. Permanent decode failures (malformed payload) are ACKed by returning nil (not retried).
- **Transactional consistency**: the `message_processing` claim, the `proposal` row insert, and the `remediation_agent_outbox` enqueue are performed in one transaction. The LLM call and S3 writes happen before the transaction opens, so no transaction is held across the external call. A crash between the proposal insert and the outbox enqueue cannot occur — both commit together or not at all.
- **Effectively-once LLM call**: because the whole handler re-runs on a redelivery (the LLM call precedes the write transaction), the external completion is protected by the best-effort idempotency-keyed response cache described under LLM Integration. A redelivery of the same trigger (same inbound idempotency key and request) hits the cache and skips the provider, so a failed terminal commit does not re-pay the expensive call. A genuinely new trigger — including a later remediation attempt for the same failure — uses a different key and always calls the model afresh.
- **Outbox dedup**: the `remediation.proposed:v1` entry carries a deterministic `event_id` (SHA1 UUID on `release_id|node_id|attempt`) so a redelivered downstream consumer can detect and suppress duplicates.

## Non-Responsibilities

`agent-remediation` generates proposals and exposes their lifecycle over gRPC. It does not:

- Create GitHub pull requests or open code review branches. PR creation is performed by ui, which holds the GitHub App write credential.
- Write to, commit to, or push any git repository. GitHub access is read-only.
- Auto-apply or merge any proposed SQL change.
- Merge, close, or comment on any pull request; the PR-outcome reconciler only reads PR status via GitHub's Pulls API. It observes GitHub's own merge/close decision, made by human reviewers, and mirrors it onto `pr_state`.
- Promote anything to production. The one write it issues to another service is `POST /releases` with `shadow: true`, and release-controller stops such a release at `validated`: it never touches `current_prod`, `service_prod`, the Neo4j topology, or the promoted code-version history. Verifying a fix and shipping it stay separate acts, the second still requiring a human-approved pull request.
- Open a pull request via the opening sweep. The sweep only looks an already-created PR up by branch to recover its URL onto a stranded proposal row; the PR itself was created by ui through the GitHub App, exactly as in the normal flow.
- Track whether a proposal was accepted or resulted in a passing release.

All code-change decisions — review, approval, and PR creation — are human actions.

## Background Loops

| Loop | Description |
|---|---|
| `remediation.requested:v1` consumer | Dispatches each inbound message to the `ProposeFix` handler. |
| Outbox publisher | Drains `remediation_agent_outbox` and XADDs each pending row to `remediation.proposed:v1`, `remediation.pr_opened:v1`, or `remediation.pr_closed:v1` depending on the row's stream field. |
| Shadow-verify reconciler | Ticks every `SHADOW_VERIFY_POLL_INTERVAL` (default 15s). Each pass lists up to 20 proposals with `status='verifying'`, oldest first, and reads each one's shadow release through the `ReleaseGateway`. A validated release finalizes the attempt (`verifying → proposed`) and enqueues its `remediation.proposed:v1` outbox row in one transaction; a rejected release finalizes it (`verifying → failed`) with the shadow's per-node error recorded as `verify_error`, commits, and only then starts the next attempt from the trigger stored on the row. A non-terminal release leaves the row untouched until the release has been RUNNING longer than `SHADOW_VERIFY_TIMEOUT` (default 20m, counted from its first transition past `received`, so queued time never spends it); a verdict that cannot be read leaves it untouched until the same budget elapses from when the attempt was recorded, there being no readable activation moment. Either way the attempt is then failed with `shadow verification timed out`. Both transitions are compare-and-set, so a repeated pass over an already-finalized row writes nothing, emits nothing, and starts no second attempt. A next attempt that cannot be started leaves no in-flight row behind: the `generating` row the driver committed for it is failed with the reason. Per-row errors are logged and skipped, so no row's failure ends the pass — though a pass does run each retry's fix pipeline synchronously, so one slow retry delays the rows behind it until the next tick. |
| PR-outcome reconciler | Ticks every `REMEDIATION_PR_POLL_INTERVAL` (default 60s). Each pass lists up to 50 proposals with `pr_state='open'`, oldest-opened first; for each it calls `GET /repos/{repo}/pulls/{number}` (GitHub Pulls API) and maps a merged PR to `merged` and a closed-unmerged PR to `rejected`. A closed PR calls `Service.RecordOutcome`, which performs the single-winner CAS `pr_state: open → merged|rejected` and enqueues the `remediation.pr_closed:v1` outbox row in one transaction; a CAS miss (the row already left `open`) is a no-op. Per-row errors (a failed GitHub read or a failed `RecordOutcome`) are logged and skipped — one bad row never blocks the rest of the batch — and are retried on the next pass. A `401`, or a `403` that is not rate limiting (no `Retry-After` header and `X-RateLimit-Remaining` != 0), signals the token lacks `Pull requests: Read` and is classified as a permission error that flips the reconciler to a degraded state; rate-limited `403`s and `429`s are treated as transient and simply retried. While degraded, an actionable ERROR log (`grant the GitHub token 'Pull requests: Read'`) fires only on the healthy→degraded transition — not every pass — and a subsequent clean read clears it (recovery logged once). This distinguishes a standing permission gap — which stalls the whole close-loop and never self-resolves — from transient errors or genuinely still-open PRs. |
| Opening sweep (same reconciler, same tick) | Recovers `pr_state='opening'` claims left stranded when ui's `RecordPullRequest` call fails after the PR was already created on GitHub (that step is explicitly best-effort; see `ui/src/server/routes/remediation.ts`), and releases genuinely abandoned claims for retry. Each pass reads one page of up to 50 proposals with `pr_state='opening'`, ordered `(created_at, id)` and resuming after the keyset cursor (`repository.OpeningCursor`) the previous pass returned — not always the same oldest 50 rows — each carrying its `pr_claimed_at`; for each row the sweep recomputes the deterministic branch name (`remediation/<release_id>/<node_sanitized>-attempt<n>`, `proposals.BuildBranch`) and calls `GET /repos/{repo}/pulls?head=…` (GitHub Pulls API). A found PR — at any claim age — is recorded via `Service.Record` exactly as the client-side flow would have, closing the gap: a CAS guarded on `pr_state='opening'` (the same guard `RecordPullRequest`'s own call to `Service.Record` uses) transitions `opening → open` with `pr_url`/`pr_number` set and `pr_claimed_at` cleared to `NULL`, so the UI stops inviting a duplicate PR; if ui's own `RecordPullRequest` call for this exact claim reaches the row first, this call's CAS misses and is a no-op, never a second write or a second `pr_opened` event. When no PR is found, the sweep compares `now - pr_claimed_at` (wall-clock, via the reconciler's `Clock` port) directly against `REMEDIATION_PR_OPENING_GRACE_PERIOD`: a claim younger than the grace period is left untouched and retried next pass, while one older than it calls `Service.FailStuckClaim(id, observedClaimedAt)` — the repository's `FailStuckOpeningPR` compare-and-set, guarded on the exact `pr_claimed_at` this pass observed (`opening → failed`, `pr_claimed_at` cleared to `NULL` only on a CAS hit), which ui's claim guard (`pr_state IN ('', 'failed')`) accepts as retryable once it fires. The CAS is what keeps a second reconciler instance (or an operator's retry) from clobbering a claim released and re-claimed since this pass listed it: a miss (row already left `'opening'`, or re-claimed with a different `pr_claimed_at`) is logged and left alone, never treated as an error. `pr_claimed_at` is stamped by `BeginPullRequest` from the service's own clock — or, for a claim taken by a binary that does not set the column itself, by the `proposal_stamp_pr_claimed_at` trigger (see the `proposal` table above) — and cleared on every exit from `'opening'` (`RecordPullRequest`, `FailPullRequest`, `FailStuckOpeningPR`), so a proposal re-claimed after a prior failure (`opening → failed → opening`) always ages from its own, second claim. A row with `pr_claimed_at IS NULL` is left untouched rather than failed, since an unmeasurable claim can never safely be judged stale; a warn log fires on every pass such a row is seen. Age is read from a stored timestamp, so a claim taken moments before a pass runs is never raced: its age is always far short of the grace period regardless of poll interval. A per-row GitHub error leaves the row untouched — an inconclusive read is not a confirmed miss — and does not block the rest of the batch; because the pass has already advanced its cursor past this page before handling any row in it, an error or a left-alone row here never re-anchors the following pass at the same prefix. When a page comes back short of the 50-row limit, the reconciler wraps its cursor back to the start, so a full rotation through every stuck `'opening'` row repeats indefinitely and a handful of persistently unresolvable rows (a standing GitHub error, for instance) can never keep the rows behind them out of every pass. A permission error (401, or a non-rate-limited 403) from the branch lookup feeds the same degraded signal as the outcome loop's `PRStatus` reads (see `updateHealth` below): either loop hitting a permission error marks the reconciler degraded, and a clean read from either loop clears it. |

## Configuration Reference

| Env var | Required | Default | Description |
|---|---|---|---|
| `POSTGRES_HOST` | yes | — | Postgres host |
| `POSTGRES_USER` | yes | — | Postgres user |
| `POSTGRES_PASSWORD` | yes | — | Postgres password |
| `POSTGRES_DB` | no | `continuo_agent_remediation` | Database name |
| `POSTGRES_PORT` | no | `5432` | Postgres port |
| `DB_SSLMODE` | no | `disable` | Postgres SSL mode |
| `REDIS_ADDR` | yes | — | Redis address (via `pkg/config.LoadRedis`) |
| `REDIS_PASSWORD` | yes | — | Redis password; process refuses to start if missing |
| `LLM_PROVIDER` | yes | — | `anthropic`, `openai`, or `openai-compatible` |
| `LLM_MODEL` | yes | — | Model identifier (e.g. `claude-haiku-4-5`) |
| `LLM_API_KEY` | no | `""` | API key for `anthropic`/`openai` providers |
| `LLM_BASE_URL` | conditional | `""` | Base URL; required when `LLM_PROVIDER=openai-compatible` |
| `LLM_CACHE_TTL` | no | `1h` | Per-entry TTL for cached LLM propose results (Go duration). Only needs to cover the same-trigger redelivery window; kept short to bound memory on the shared `noeviction` Redis. A non-positive value is clamped to the `1h` default; a value that is not a Go duration at all fails start-up naming the key. |
| `GITHUB_TOKEN` | no | `""` | Read-only fine-grained PAT with `Contents: Read` and `Pull requests: Read` on the dbt repo. In Helm, sourced from `global.github.token` in the chart-managed secret `continuo-app-credentials`. When empty, requests to the Contents API are sent unauthenticated (subject to GitHub's lower unauthenticated rate limit) rather than failing outright. |
| `SERVICE_REPO_MAP_PATH` | no | `""` | Path to `service_repos.yaml`, which maps each service name (dbt or python) to its project root within that service's own repository — production is a data mesh of one repository per team; the single shared checkout this map implies is the dev/e2e convenience, not the deployed shape. In Helm, set to `/etc/continuo/service_repos.yaml` and backed by the `continuo-app-service-repos` ConfigMap (built from `deploy/continuo/files/service_repos.yaml`). In docker-compose (dev/e2e), bind-mounted from `agent-remediation/config/service_repos.yaml`. Empty (or a service name absent from the map) means compile, seed, and duplicate-table proposals are skipped (their source read always goes through this mapping). For validation it degrades Step 2 to the Step-1 candidate proposal even when the code bundle successfully supplied the source — the mapping is needed to record the file's repository path on the proposal, not only to read it — and it additionally removes validation's GitHub repo-read fallback for the (rarer) case where the code bundle permanently misses the node. |
| `GITHUB_BASE_URL` | no | `https://api.github.com` | GitHub REST API root; override for e2e stub (`stub-github`) |
| `CONTINUO_ORCHESTRATOR_ADDR` | no | `orchestrator:50052` | Orchestrator gRPC endpoint (`GraphClient`'s `GetNodeLocation`/`GetUpstreamChanges`/`GetNodeVersions`/`GetPrecedents` calls) |
| `AGENT_REMEDIATION_HTTP_PORT` | no | `8092` | `/healthz` port |
| `AGENT_REMEDIATION_GRPC_PORT` | no | `50054` | `RemediationProposals` gRPC server port |
| `AGENT_REMEDIATION_MAX_ATTEMPTS` | no | `3` | Per-`(release_id, remediation_round, source, node_id, error_signature)` attempt cap |
| `REMEDIATION_PR_POLL_INTERVAL` | no | `60s` | Interval between PR-outcome reconciler passes (Go duration). A non-positive value falls back to the default; a value that is not a Go duration at all fails start-up naming the key. |
| `REMEDIATION_PR_OPENING_GRACE_PERIOD` | no | `10m` | How long, measured as wall-clock time against the proposal's stored `pr_claimed_at`, the opening sweep waits before releasing a `pr_state='opening'` claim with no matching GitHub PR back to `'failed'`. A non-positive value falls back to the default; a value that is not a Go duration at all fails start-up naming the key. |
| `WAREHOUSE_ENGINE` | no | `postgres` | The warehouse engine this install runs, published to every service on the shared ConfigMap from the chart's `validation.engine`. It resolves to the sqlglot dialect the packaging CLI renders a corrected contract's declared reads under, mirroring topology-controller's own engine→dialect map exactly so a release that service accepts is never packaged here under a dialect it never validated against. An engine with no dialect mapping fails startup rather than packaging under another engine's rules. |
| `RELEASE_CONTROLLER_URL` | no | `http://release-controller:8088` | release-controller's HTTP root, used to submit shadow verification releases and poll their verdicts. |
| `SHADOW_VERIFY_TIMEOUT` | no | `20m` | How long a proposal awaits its shadow release's verdict before the reconciler ends the attempt as failed (Go duration). Measured from when that release left the release queue and started running, so a backlog of other releases never spends it; when no verdict can be read at all, measured from when the attempt was recorded instead. A non-positive value falls back to the default; a value that is not a Go duration at all fails start-up naming the key. |
| `SHADOW_VERIFY_POLL_INTERVAL` | no | `15s` | Interval between shadow-verify reconciler passes (Go duration). A non-positive value falls back to the default, so a misconfiguration can never produce a hot loop; a value that is not a Go duration at all fails start-up naming the key. |

## Key Code Paths

| Concern | Path |
|---|---|
| Proposal entity + unified diff | `agent-remediation/domain/proposal/proposal.go` |
| Prompt assembly (validation candidate + real-source, compile, seed, duplicate table) | `agent-remediation/domain/prompt/prompt.go` (`Assemble`, `AssembleSourceFix`, `AssembleCompileFix`, `AssembleSeedFix`, `AssembleDuplicateTableFix`) |
| Prompt assembly (python contract fix, incl. the prior-attempts section) | `agent-remediation/domain/prompt/python.go` (`AssemblePythonContractFix`) |
| Event payloads + deterministic IDs | `agent-remediation/domain/event/` (proposed, pr_opened, pr_closed) |
| Shared driver — attempt cap, dedup, persistence, outbox emit (each Fixer fetches its own dbt log) | `agent-remediation/service/handlers/propose_fix.go` |
| Per-error-class fixers — `Fixer` interface, `For` factory, shared single-shot pipeline | `agent-remediation/service/fixer/fixer.go` |
| Compile fixer (offending file + co-located YAML/`dbt_project.yml` context, one LLM call) | `agent-remediation/service/fixer/compile.go` |
| Seed fixer (CSV read, one LLM call) | `agent-remediation/service/fixer/seed.go` |
| Duplicate-table fixer (single-file rename, no dbt log, shares `singleFileInterpret` with compile) | `agent-remediation/service/fixer/duplicate_table.go` |
| Validation fixer, dbt node (two-step candidate + real-source flow, best-effort upstream-diff gather) | `agent-remediation/service/fixer/validation.go` |
| Validation fixer, python node (repo checkout, contract repair, packaging, shadow submission) | `agent-remediation/service/fixer/python_validation.go` |
| Contract-yaml search across a repository checkout, and the node-declaration read (identity fields + declared read names) a proposed edit is checked against (`ContractLocator` + `ContractInspector`) | `agent-remediation/adapters/repofs/contract_locator.go` |
| Shadow-verify reconciler loop (verdict polling, CAS finalization, next-attempt start) | `agent-remediation/service/shadowverify/reconciler.go` |
| PR lifecycle application service (claim/record/fail/fail-stuck-claim/record-outcome + outbox) | `agent-remediation/service/proposals/service.go` |
| PR-outcome reconciler loop, incl. permission-gap degraded signal and the opening sweep (recover-or-fail stuck `pr_state='opening'` claims, CAS-guarded fail, cursor-paginated rotation) | `agent-remediation/service/proposals/reconciler.go` |
| Deterministic branch-name builder (`BuildBranch`), shared by `Service.Begin` and the opening sweep so they never drift apart | `agent-remediation/service/proposals/service.go` |
| Port interfaces, incl. `PullRequestBranchFinder` (by-branch PR lookup for the opening sweep) | `agent-remediation/service/ports/` |
| Postgres UoW + proposal repo (incl. CAS for BeginPR, RecordPR, RecordPROutcome, and FailStuckOpeningPR; open-PR listing; keyset-paginated stuck-opening listing) | `agent-remediation/adapters/postgres/` |
| S3 evidence reader + artifact writer | `agent-remediation/adapters/s3/` |
| S3 code-bundle candidate-source reader (validation only; decodes via `pkg/codebundle`) | `agent-remediation/adapters/s3/candidate_source_reader.go` |
| Graph read ports (`NodeLocator`, `UpstreamChangeReader`, `VersionReader`, `PrecedentReader`) | `agent-remediation/service/ports/graph_reader.go` |
| Candidate-source port | `agent-remediation/service/ports/candidate_source_reader.go` |
| gRPC graph client (`GetNodeLocation`/`GetUpstreamChanges`/`GetNodeVersions`/`GetPrecedents` over `OrchestratorQuery`) | `agent-remediation/adapters/grpc/graph_client.go` |
| gRPC `RemediationProposals` server | `agent-remediation/adapters/grpc/proposals_server.go` |
| GitHub read-only source reader (file read + directory list) | `agent-remediation/adapters/github/source_reader.go` |
| GitHub read-only repository archive (tarball fetch + hardened extraction) | `agent-remediation/adapters/github/repo_archive.go` |
| Contract packaging adapter (`continuo-runtime merge` subprocess) | `agent-remediation/adapters/packaging/cli_packager.go` |
| release-controller HTTP gateway (shadow submit, verdict poll, image-tag read) | `agent-remediation/adapters/releasehttp/gateway.go` |
| GitHub read-only PR status reader (Pulls API: by-number status for the outcome reconciler, by-branch lookup for the opening sweep) | `agent-remediation/adapters/github/pr_status.go` |
| Service→repo map config loader | `agent-remediation/config.go` (reads `SERVICE_REPO_MAP_PATH`, parses `service_repos.yaml`) |
| Service→repo map file (dev + e2e) | `agent-remediation/config/service_repos.yaml` |
| Service→repo map file (Helm chart) | `deploy/continuo/files/service_repos.yaml` (rendered into `continuo-app-service-repos` ConfigMap, mounted at `/etc/continuo`) |
| Anthropic LLM adapter | `agent-remediation/adapters/llm/anthropic.go` |
| OpenAI-compatible LLM adapter | `agent-remediation/adapters/llm/openai.go` |
| Best-effort LLM response caching decorator | `agent-remediation/service/llmcache/caching_provider.go` |
| Redis LLM response cache adapter | `agent-remediation/adapters/redis/llm_response_cache.go` |
| Pass-through log sanitizer | `agent-remediation/adapters/sanitizer/passthrough.go` |
| Redis consumer + outbox publisher | `agent-remediation/adapters/redis/` |
| DB migrations | `db/migration/agent_remediation/` (`V1__init_remediation_agent.sql` through `V15__proposal_remediation_round.sql`), including `V3__pr_creation.sql` for the PR-tracking columns, `V6__add_generating_proposal_status.sql` for the in-flight `generating` status, `V9__proposal_pr_claimed_at.sql` for the `pr_claimed_at` column, `V10__stamp_pr_claimed_at_on_opening.sql` for the `proposal_stamp_pr_claimed_at` trigger's fill-when-NULL clause, `V11__clear_pr_claimed_at_on_opening_exit.sql` for the same trigger's unconditional clear-on-exit clause, `V12__proposal_file_edits.sql` for the `file_edits` column, `V13__proposal_verification.sql` for the `verifying` status and the `shadow_release_id`/`verify_error`/`trigger_payload` columns, `V14__proposal_attempts_per_release.sql` for the `(release_id, source, node_id, error_signature)` index behind the per-release attempt count, and `V15__proposal_remediation_round.sql` for the `remediation_round` column and its `(release_id, remediation_round, source, node_id, error_signature)` index behind the per-round attempt count |
