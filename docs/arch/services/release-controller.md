# release-controller

## Purpose

`release-controller` owns the dbt blue/green candidate-release lifecycle: it gates every candidate behind a validation of the changed nodes, their downstream descendants, and their full transitive upstream closure across service boundaries — making every validation self-contained — and only swaps production (topology, schedules, image tags) when validation passes. It holds the `current_prod` pointer — the single source of truth for what is live — and orchestrates the release state machine across topology-controller, executor-controller, and orchestrator via Redis streams. The pipeline serves two kinds of run — a candidate release a team's CI posts, and a fix-verification run agent-remediation posts to find out whether a proposed fix holds — and one FIFO queue serialises both, so a verification run and a candidate release never build against the same candidate schema at the same time.

**Runtime**: Go service. Exposes an HTTP API and consumes/produces Redis streams; persists to its own Postgres database via a transactional outbox.

## Owned Storage

Postgres (its own database). Tables:

| Table | Purpose |
|---|---|
| `release_pipeline_runs` | One row per run of the pipeline, of either kind. `run_id` is the primary key; `run_kind` (`candidate` or `verification`) says which. Shared columns, present and meaningful for both kinds: `status`, `manifest_kind` (`dbt` or `python`, set at creation and immutable thereafter — it decides whether the run takes the compile leg at all, see Processing Logic), `changed_service` (the single service this delta belongs to), the per-service `image_tags` map assembled at activation, `candidate_topology`, `code_bundle_uri` (S3 URI of the run's code-bundle contract document, set from the `manifest.loaded.candidate:v1` parse result), `validation_node_ids`, `fail_reason` (a machine-readable token) alongside `fail_detail` (the operator-facing explanation — the same string carried as `error_detail` on `release.rejected:v1` for a candidate; empty when the fail path supplied none), `per_node_results` (tagged by `stage` — `compile`, `seed_build`, or `validation` — so results from all three legs are accumulated and independently addressable on one run), `failing_nodes`, `transitions` (history), and `created_at`. Candidate-only columns, meaningless (and reset to their zero value) on a verification row: the immutable provenance columns `repo` (GitHub owner/name) and `commit_sha` (full SHA) captured at receipt, `bootstrap` (immutable; promotes without validation when set), `remediation_round` (default `1`; how many times a human has asked this rejected candidate to "try again," capped in code at `MaxRemediationRounds = 3`), and `rejection_payload` (JSONB, nullable) — the exact `release.rejected:v1` payload this candidate emitted at its (healable) rejection, kept so a later round can replay it verbatim instead of re-deriving it from the aggregate; set at every rejection that carries a `per_node`-shaped payload — every reason from the compile leg (`compile_failed`, `parse_rehearsal_failed`, `artifact_upload_failed`), `duplicate_table`, `seed_build_failed`, and `validation_failed`, regardless of whether that particular reason is itself retryable — and left `NULL` for `parse_failed`, which is not healable. Verification-only columns, meaningless on a candidate row: `verifies_release_id` (the rejected candidate this run verifies a fix for), `attempt` (which attempt of that candidate's remediation this run belongs to; parsed from the trailing `-a<n>` of `run_id` when not supplied directly), and `source_overlay_uri` (default `''`; the S3 URI of the source-overlay tarball a dbt verification run's compile and seed-build legs lay over the service project, so the run compiles a proposed fix rather than the committed source — always empty for a python verification, which is verified by its packaged contract instead). A `release_pipeline_runs_kind_status_check` constraint pins kind to status: a `candidate` row's status is never `passed` or `failed`; a `verification` row's status is never `promoted`, `rejected`, or `superseded` — so a run can only ever reach the terminal statuses that belong to its own kind. `status` itself is constrained to `received`, `compiling`, `parsing`, `seed_building`, `validating` (shared by both kinds), `promoted`, `rejected`, `superseded` (candidate terminals), and `passed`, `failed` (verification terminals). Four indexes serve the table: `idx_release_pipeline_runs_active` (on `created_at`, filtered to the five non-terminal statuses) backs the FIFO's single-active-run query for either kind; `idx_release_pipeline_runs_kind_created` (`run_kind, created_at DESC, run_id DESC`) and `idx_release_pipeline_runs_kind_status_created` (`run_kind, status, created_at DESC, run_id DESC`) back `GET /releases` and `GET /verification-runs`, both of which always filter on kind; `idx_release_pipeline_runs_verifies` (`verifies_release_id`, filtered to `run_kind = 'verification'`) backs `GET /verification-runs?verifies=`, the release page's "Verification runs" section. |
| `current_prod` | Singleton row: the promoted `release_id` and its `topology_snapshot` (the live topology). Only a candidate ever writes this row. |
| `service_prod` | One row per service (dbt or python) — the live per-service production pointer: `{service_name, release_id, manifest_s3_key, manifest_kind, image_tag, updated_at}`. `manifest_kind` (`dbt` or `python`) records how that service's canonical artifact is authored and parsed, so `AssembleManifestSet` can carry the right `kind` forward onto the next release's `manifest_keys` entry for that service without re-deriving it. Records which manifest key and image tag are currently live for each service; the full production manifest set is reconstructed by collecting every service's pointer at activation time. |
| `release_controller_outbox` | Transactional outbox; one row per produced event, drained by the outbox publisher. |
| `message_processing` | Inbound dedup ledger (`outbox_entry_id` / message id) for idempotent consumption. |

The `topology_snapshot` is the live topology as a list of nodes (`unique_id`, `schema_name`, `table_name`, `resolved_relation_id`, `service_name`, `node_type`, `content_hash`, `image_tag`, `upstream_unique_ids`, `schedule`); the per-node `content_hash` comparison against it determines which nodes a new candidate must validate. `resolved_relation_id` is the physical relation the node's build actually writes (its dbt alias, when it has one, else the same name as `unique_id`'s own table segment); it is what `DuplicateClaims` groups on, not `unique_id`, so it survives into the snapshot even though nothing else reads it there. When a new candidate is promoted, its candidate topology (carrying `content_hash` + joined `image_tag`) replaces the snapshot, forming the change-detection base for the next release. The `candidate_artifact_uri` field is stored in `releases.candidate_topology` (as a JSONB field) during validation but is stripped on promotion — it is transient validation data and is not carried into `current_prod`. `code_bundle_uri` is a release-level column, not a per-node topology field; it is set once from the parse result and is carried forward unchanged onto `release.promoted:v1` rather than stripped.

## Inbound Interfaces

### HTTP

| Route | Purpose |
|---|---|
| `POST /releases` | Accept a candidate release for a single service. Body: `{service, release_id, image_tag, repo, commit_sha, bootstrap?, kind?}`. `repo` (GitHub owner/name) and `commit_sha` (full SHA) are required; missing either returns 400. `kind` is optional (`"dbt"` or `"python"`); absent or empty defaults to `"dbt"`, and any other value returns 400. Idempotent on `release_id`; an id that already names a verification run returns `409`. `bootstrap:true` promotes without validation (see Processing Logic). `source_overlay_uri` and `verifies_release_id` are **refused (400)**: those are fix-verification concepts and belong on `POST /verification-runs` instead — a body carrying either is answered with a clear error naming the right endpoint, rather than the values being silently ignored. |
| `GET /releases/{id}` | Full candidate detail: `{release_id, status, changed_service, manifest_kind, transitions, validation_node_ids, reject_reason, reject_detail, failing_nodes, per_node_results, image_tags, bootstrap, repo, commit_sha, remediation_round}`. `manifest_kind` (`dbt` or `python`) says how the artifact is parsed and therefore which pipeline legs the release takes; the UI previews the remaining stages from it. `404` when the id is unknown **or names a verification run** — a verification run is read through `GET /verification-runs/{id}` instead. `reject_detail` is the operator-facing explanation of the rejection (empty string when the release is not rejected, or when the reject path supplied none). `per_node_results` is an array of `{stage, node_id, status, dbt_log_uri, run_results_uri, duration_ms, file_path?}` accumulated across all pipeline legs; the `stage` field (`compile`, `seed_build`, or `validation`) identifies which leg produced each entry. For the compile leg the single entry's `node_id` is the service name; `file_path` is non-empty when the failure maps to a specific source file. `duration_ms` is populated for the `compile` and `seed_build` legs; the `validation` stage's per-node results come from the incremental `kind=node` projections on `validation.result:v1`, which do not carry a duration, so `duration_ms` is absent (zero) for those entries. `remediation_round` (default `1`) is how many times a human has asked this release to "try again" after a rejection; the UI's release page shows it in the status pill once it exceeds 1. |
| `POST /releases/{id}/retry-remediation` | Start another remediation round on a rejected candidate: replays the candidate's stored `release.rejected:v1` payload, tagged with the incremented `remediation_round`, on `remediation.retry_requested:v1`. `202 {release_id, remediation_round}` on success. `404 {error: "not_found"}` when the id does not exist, or names a verification run (a verification run is not a release, so it is reported the same as an unknown id — in practice this branch is defensive: a verification's terminal statuses are `passed`/`failed`, never `rejected`, so the `not_rejected` refusal below already screens every real verification run out first). `409 {error}` when refused: `not_rejected` (the run is not currently `rejected`), `not_healable` (the stored reason is not one of `compile_failed`/`seed_build_failed`/`validation_failed`/`duplicate_table`), `not_retryable` (no stored `rejection_payload` — the release predates the column), `rounds_exhausted` (`remediation_round` is already at the cap of 3), `retry_in_progress` (the current round has not yet produced a proposal row — its trigger has not reached agent-remediation yet, or the classifier dropped it), or `proposal_open` (some node's latest attempt in the current round is still generating/verifying/proposed, or any attempt from any round has a PR that is opening/open/merged — the response also carries `proposal_id`/`pr_url` when known). `502 {error: "proposal_reader_unavailable"}` when the gRPC read to agent-remediation fails outright, as distinct from a `proposal_open` refusal. `500 {error: "internal"}` on any other failure (logged server-side). See Processing Logic → `RetryRemediation` for the full rule order. |
| `GET /releases` | Paginated **candidate** history, newest-first — a verification run never appears here. Query params: `status` (optional exact-match filter), `limit` (default 20; values that are unparseable, non-positive, or exceed 100 fall back to the default of 20), `cursor` (opaque keyset cursor). Response: `{"releases":[{release_id, status, created_at, resolved_at, node_count, bootstrap, reject_reason, repo, commit_sha}], "next_cursor":"<opaque or empty>"}`. `resolved_at` is non-empty once a release reaches a terminal status. `repo` and `commit_sha` are the release's stored provenance, carried on each row so the UI can resolve the commit author for the Releases tab without a per-release detail fetch. |
| `POST /verification-runs` | Accept a fix-verification run for a single edited service. Body: `{run_id, service, image_tag, kind, verifies_release_id, attempt, source_overlay_uri?}`. `run_id`, `service`, `image_tag`, `kind` (`"dbt"` or `"python"` — required, with no default: an absent kind is a caller bug), `verifies_release_id`, and `attempt` (must be ≥ 1) are all required; missing or invalid any of them returns 400. `source_overlay_uri` is optional and accepted only when `kind` is `"dbt"` (400 otherwise) — a python service is verified by its packaged contract yaml instead, uploaded under the run's own canonical manifest key, and carries no overlay. Idempotent on `run_id`; an id that already names a candidate release returns `409`. It is posted by `agent-remediation`; nothing else submits one. `202 {run_id, status: "received"}` on success. |
| `GET /verification-runs/{id}` | Full verification-run detail: `{run_id, status, changed_service, verifies_release_id, attempt, created_at, activated_at, finished_at, transitions, validation_node_ids, failing_nodes, fail_reason, fail_detail, per_node_results, image_tags, manifest_kind}`. `per_node_results` keeps the same shape as a candidate's so the UI renders both kinds with one table. `activated_at` and `finished_at` are `""` until the run has left the queue / reached a terminal status. `404` when the id is unknown or names a candidate release. |
| `GET /verification-runs?verifies=<release_id>` | The verification runs that verify one rejected candidate, newest first: `{"runs":[{run_id, status, service, attempt, created_at, activated_at, finished_at, fail_reason}]}`. `verifies` is required — `400` when it is absent. This listing exists for the release detail page's "Verification runs" section; there is no unfiltered listing of every verification run. Unpaginated: bounded by rounds × attempts × edited services, which never grows large. |
| `GET /pipeline` | What the pipeline is doing right now, across both kinds: `{"active": null}` when the queue is idle, otherwise `{"active": {run_id, run_kind, status, service, since}}`, with `verifies_release_id` and `attempt` added to that object only when `run_kind` is `"verification"`. `since` is the run's `activated_at`. It is the one read that spans both kinds, so an operator (or the UI's in-flight strip) can see a verification run holding the pipeline slot that queued candidate releases are waiting behind. |
| `GET /current-prod` | The current promoted release + topology snapshot. |
| `GET /healthz` | Readiness, backed by the `pkg/liveness` registry. Fails (503) when any registered worker (the stream consumers, the outbox publisher) has exited with an error, any consumer read-loop heartbeat has gone stale, **or** any dependency probe (Redis, Postgres) fails; a dependency outage pulls the pod out of the Service endpoints without restarting it, since a restart does not fix a downstream outage. |
| `GET /livez` | Liveness, backed by the same registry but **workers + heartbeats only** (no dependency probes), so a Redis/Postgres outage does not restart a pod whose consumers are already retrying, while a dead or wedged consumer does. |

The HTTP port is 8088. The Kubernetes `readinessProbe` points at `/healthz` and
the `livenessProbe` at `/livez` (`deploy/continuo/values.yaml`:
`probePath: /healthz`, `livenessPath: /livez`).

`POST /releases` is the production entry point for a deploy: each team's CI uploads only its own service's compiled artifact to the canonical S3 key — `s3://<bucket>/<service>/<release_id>/manifest.json` for a `dbt` service (the compile leg produces it) or `.../contract.yaml` for a `python` service (the domain repo's own CI compiles and uploads it directly, before this POST, since there is no compile leg for python) — and posts a single-service candidate here. `kind` on the request selects which; the canonical key itself is derived from it via `CanonicalManifestKey`, never supplied by the caller. The request carries no manifest-key list — release-controller assembles the full per-service set at activation time from `service_prod` (see Processing Logic). It also carries no changed-node list — release-controller derives it. The CI caller supplies `repo` (`github.repository`, e.g. `org/continuo-demo`) and `commit_sha` (`github.sha`) as required provenance fields; release-controller stores them verbatim and exposes them on `GET /releases/{id}`.

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `compile.completed:v1` | `release-controller-compile-completed` | Compile job result from executor-controller. |
| `manifest.loaded.candidate:v1` | `release-controller-manifest-loaded-candidate` | Resolved candidate topology (or a parse failure) from topology-controller. |
| `seed.build.completed:v1` | `release-controller-seed-build-completed` | Aggregate seed-build results from executor-controller. |
| `validation.result:v1` | `release-controller-validation-result` | Unified validation-leg stream from executor-controller. Carries per-node outcomes (`kind=node`, one per node as it settles) and the terminal decision (`kind=complete`, emitted last), all under one `aggregate_id` so a single in-order consumer sees every node before the decision. |

## Outbound Interfaces

### Redis producer

| Stream | Consumed by | Emitted when |
|---|---|---|
| `compile.requested:v1` | executor-controller | A run activates and its `manifest_kind` is `dbt`; the changed service's dbt compile is needed to produce a fresh manifest. Payload: `{release_id, service, image_tag, bucket, candidate_schema, source_overlay_uri?}` — the key stays `release_id` on the wire regardless of run kind. `candidate_schema` (`"_candidate_" + SanitizeSchemaSuffix(run_id)`) is always set, driving the compile Job's parse-export/rehearsal gate (see the executor-controller doc's `CreateCompileJob`). `source_overlay_uri` is threaded verbatim from the run row and is non-empty only for a dbt verification run verifying a proposed fix; it makes the compile Job lay that overlay over the team's project before dbt runs. |
| `release.requested:v1` | topology-controller | A run becomes ready to parse: a `dbt` run transitions from Compiling to Parsing (on `compile.completed:v1`); a `python` run, which has no compile leg, transitions directly from Received to Parsing at activation. Either way it carries the assembled `manifest_keys` set (the changed service plus every other service's current prod manifest, each entry's `kind` explicit) to parse. |
| `seed.build.requested:v1` | executor-controller | A parsed run has new/changed dbt-seed nodes that need building into the candidate schema before validation. Payload: `{release_id, mode, candidate_schema, image_tags, seeds, seed_ids_in_order, source_overlay_uri?}` — `source_overlay_uri` is threaded from the run row exactly as on `compile.requested:v1` and is present only for a dbt verification run verifying a proposed fix, so the seed Job loads the proposed CSV rather than the committed one. |
| `validation.requested:v1` | executor-controller | A run has changed nodes to validate. |
| `release.promoted:v1` | orchestrator | A candidate is promoted to production. Only a candidate ever emits this; a verification run never does, whatever its outcome. Payload: `{release_id, topology, image_tags, repo, commit_sha, promoted_at, candidate_schema, code_bundle_uri, bootstrap}`. Each entry in `topology` carries the standard node fields plus a `changed` boolean that is `true` when the node's `content_hash` differs from the prior `current_prod` (or when `current_prod` was empty). The top-level `repo`, `commit_sha`, and `promoted_at` are the source-change provenance for this release; orchestrator stamps them onto each changed `:Table` node. `code_bundle_uri` is the release's code-bundle S3 URI carried through unchanged from the parse result; `bootstrap` reflects whether this release skipped validation. Orchestrator's version-ingestion consumer group reads the bundle at that URI and uses `bootstrap`, together with each node's `changed` flag, to mark whether a recorded code version's commit stamp is exact or approximate. |
| `release.rejected:v1` | `remediation` (group `remediation-release-rejected`) | A candidate fails at any pipeline stage. **Candidate-only**: a verification run's failure never rides this stream, whatever caused it (see `pipeline.run.finished:v1` below). The payload is uniform across all three legs: `{release_id, stage, reason, repo, commit_sha, code_bundle_uri, failing_nodes, per_node[{node_id, status, dbt_log_uri, run_results_uri}]}`. `stage` is `compile`, `seed_build`, or `validation`; `reason` is `compile_failed`, `seed_build_failed`, or `validation_failed`. For the compile leg, the single `per_node` entry's `node_id` is the service name (a synthetic compile unit, not a dbt node); for validation, entries additionally carry `candidate_artifact_uri`, `node_type`, `file_path`, `service`, and — on every non-ok entry — `changed_ancestors`, the node's transitive upstream ancestors this release changed, each as `{node_id, file_path, service}` with the location this candidate declares (see `handleValidationFailed` below). The stage-less `duplicate_table` rejection also stamps `code_bundle_uri`. The field is the release's code-bundle S3 URI, carried from the release aggregate exactly as on `release.promoted:v1`: empty for a compile-stage rejection, which precedes the parse that produces the bundle, and set for duplicate_table, seed_build, and validation, all of which follow a completed parse. The remediation classifier discriminates by `stage`, fetches the dbt log from S3 for each failing entry, and emits ONE `remediation.requested:v2` trigger carrying every healable failure of the rejection. |
| `pipeline.run.finished:v1` | executor-controller (group `executor-pipeline-run-finished`) | A run of either kind reaches a terminal status — every one of them, not only the healable ones: `promoted`/`rejected`/`superseded` for a candidate, `passed`/`failed` for a verification. Payload: `{run_id, run_kind, outcome, service, candidate_schema, verifies_release_id, attempt, finished_at}`. Every field is always present regardless of kind — a candidate carries an empty `verifies_release_id` and a zero `attempt` rather than omitting the keys, so one consumer decodes the same shape whichever kind ended. `outcome` is the run's terminal `status` value. `candidate_schema` is always named (`CandidateSchemaFor(run_id)`); executor-controller drops that schema on receipt whatever the outcome, and dropping a schema that no longer exists is a no-op. Every terminal path in Processing Logic writes this row in the same transaction as the status change, so the announcement and the terminal status can never disagree. |
| `remediation.retry_requested:v1` | `remediation` (group `remediation-retry-requested`) | A human retries a rejected release via `POST /releases/{id}/retry-remediation`. Payload: the release's own stored `release.rejected:v1` payload (see above), replayed byte-for-byte except for one added top-level field, `remediation_round` (the incremented round). `event_id` is deterministic on `(release_id, round)` (`remediation-retry:<release_id>:<round>`), so redelivery is a no-op down the outbox. The remediation classifier decodes and classifies it exactly as it does `release.rejected:v1` — see `docs/arch/services/remediation.md`. |

All events are written to the outbox inside the same transaction as the state change and published with an injected `outbox_entry_id` for consumer-side dedup.

### gRPC calls to `agent-remediation`

`RetryRemediation` calls agent-remediation's `RemediationProposals.ListProposals({release_id})` (through `adapters/grpc.ProposalsClient`, implementing the domain-owned `ports.ProposalReader`) before starting a new round, to check the release is not already a live remediation. Each returned proposal carries `remediation_round` (0 on a row recorded before the field existed, read the same as round 1), its owning-service groups (`pr_services`), and one `pull_requests` entry per owning service — a batched attempt opens one pull request per service it edits, so its PR state is a set, not a single value. The client keeps that set on `ports.ProposalSummary.PullRequests` (`PRServices` for the group list); the singular `pr_state`/`pr_url` mirror the first service's row and are used only as a legacy fallback (`EffectivePRs` synthesizes a single entry from them for a pre-split proposal that carries no `pull_requests`). The read splits into two checks: first, among proposals belonging to the release's *current* round, each node's *latest* attempt (by `attempt` number — an earlier attempt superseded by a later one on the same node does not count) must not be `generating`/`verifying`, and must not be `proposed` unless it is a **dead end** — every owning service's pull request has been rejected. A `proposed` attempt with any owning service still lacking a pull request, or holding one in any non-rejected state, still has a fix that could land and blocks the retry. Second, across *every* round, no attempt may have *any* per-service pull request that is `opening`/`open`/`merged` — one service's still-open PR blocks a new round even when a sibling service's PR was rejected. When several attempts qualify the refusal names the lowest node id (deterministic), and its `pr_url` points at that attempt's first non-rejected pull request so a human lands on a PR that can still move. A round with no proposal rows at all — the round's trigger has not reached agent-remediation yet, or the classifier dropped it — refuses with `ErrRetryInProgress` (HTTP `409 retry_in_progress`) rather than proceeding, since there is nothing yet for a retry to supersede; this is what stops a second `POST` in the seconds after the first from spending a second round before the first round's proposal row exists. Either open-attempt check refuses with `ErrProposalOpen` (HTTP `409 proposal_open`). The client dials `AGENT_REMEDIATION_GRPC_ADDR` (Helm `global.agentRemediationGrpcAddr`, mirrored onto the shared ConfigMap and `docker-compose.yml`; default `agent-remediation:50054`) once at process startup with an insecure (in-cluster) credential, the same pattern every other internal gRPC client in this repository uses; an invalid address fails boot, while an unreachable service surfaces as HTTP `502 proposal_reader_unavailable` on the first retry that calls it, rather than at startup — so the caller can distinguish "no, something is in flight" from "release-controller could not find out." On a cluster whose CNI enforces NetworkPolicy the chart's default-deny would block this edge, so the `allow-agent-remediation-grpc` policy admits `release-controller → agent-remediation:50054` explicitly.

## Processing Logic

Runs share one FIFO queue: one run — candidate or verification, whichever is oldest — is active (`compiling`, `parsing`, `seed_building`, or `validating`) at a time; on each terminal outcome the queue advances the next queued run regardless of kind. The five non-terminal statuses are shared: `received` → `compiling` → `parsing` → `seed_building` → `validating`.

### Two kinds of run

A run's kind is fixed at receipt by which endpoint it is posted to — `POST /releases` always creates a `candidate`, `POST /verification-runs` always creates a `verification` — and nothing later changes it. Every leg of the pipeline (compile, parse, seed build, validate) runs identically for both kinds; the handlers read `Run.Kind()` only at the points below where the two truly diverge.

**Terminal rules.** `Fail` is one method serving both kinds: called from every failure path in this section (compile, duplicate-table, unbuildable-cross-service-upstream, seed-build, validation), it routes to the kind-appropriate terminal status internally, so no failure handler branches on kind itself.

| Kind | Passing transition | Passing status | Failing status (via `Fail`) |
|---|---|---|---|
| candidate | `Promote` (from `validating`) | `promoted` | `rejected` |
| verification | `Pass` (from `validating`) | `passed` | `failed` |

A `bootstrap` candidate reaches `promoted` straight from `validating` without a real validation leg (see Activation guard below); a verification is never bootstrap. `superseded` is a candidate-only terminal status the schema reserves alongside `promoted`/`rejected`; no path in this pipeline currently assigns it.

**What gets promoted.** Only a candidate calls `promoteToProduction`: it writes `current_prod`, upserts the changed service's `service_prod` pointer, and emits `release.promoted:v1`. A verification calls neither — it never reads or writes `current_prod` or `service_prod` and never emits a release event, so proving a fix never touches what is live. `concludeValidated` is the one function every "the candidate topology passed" path calls; it branches on kind exactly once (`finishVerification` vs. `promoteToProduction`), so no path can promote a verification by taking a different route to production.

**What gets assembled.** Every manifest-set assembly goes through `assembleFor` (`service/handlers/assemble_release.go`), which starts from the same single-service assembly for both kinds: the changed service's fresh manifest key plus every OTHER service's live `service_prod` pointer. For a verification naming `verifies_release_id`, `assembleFor` additionally reads that rejected candidate and swaps ITS changed service from its production pointer to its own candidate manifest key (`CanonicalManifestKey(bucket, service, verifies_release_id, kind)`) and image tag — so the verification is judged against the graph the failure actually occurred in, not that service's unrelated production code. This matters whenever the fix lands in a different service than the one whose release was rejected. Both assembly sites call it — queue advance (also where a python verification emits `release.requested:v1`) and the compile result (where a dbt run's manifest keys are emitted) — so a verification's two legs cannot disagree about which manifest a service contributes. A verified release that cannot be read, or that never parsed far enough to hold a candidate topology, is logged and the assembly falls back to the plain production set: the verification still runs, against production, a weaker check rather than a stuck one. Only the verified release's OWN delta is restored this way — sibling edits the same remediation attempt made in other services are not co-verified, because one run is one service's delta; the follow-up candidate that runs once every fix PR is merged remains the gate that judges the whole change together.

**What counts as changed.** Every run's validation seed set is the candidate nodes whose `content_hash` differs from `current_prod` (`changedNodeIDsFor`, `service/handlers/handle_parsed_manifest.go`). A candidate, and a verification naming no `verifies_release_id`, stop there. A verification naming `verifies_release_id` instead re-validates a node only if it differs from BOTH `current_prod` AND the verified release's own candidate topology — the INTERSECTION of two diffs. A fix spanning two services submits one verification per edited service, so each verification assembles the OTHER edited service's node unchanged, still carrying its not-yet-fixed failure at the exact `content_hash` it had in the rejected candidate: that node differs from `current_prod` but is byte-identical to the rejected candidate, so it falls out of the intersection and this verification never re-checks it. The fix's own edited node differs from production (old or absent) and from the rejected candidate (broken), so it is always in the intersection and always validated. An unrelated node another candidate promoted since the rejection matches `current_prod` — even though it still differs from the rejected candidate's now-stale copy — so it too falls out of the intersection and is never dragged into a fix-only verification. When the verified release is unreadable or has no candidate topology, `changedNodeIDsFor` falls back to the plain `current_prod` diff, the same graceful degradation `assembleFor` takes.

**The overlay.** A verification is submitted for either manifest kind, and the kind decides what it actually runs. A `python` verification reads the packaged contract yaml `agent-remediation` uploaded under the run's own canonical key and carries no overlay. A `dbt` verification runs the real compile leg against the team's image, and the proposed source reaches it through `source_overlay_uri`: the S3 URI of a gzipped tar of project-relative files, accepted only with `kind: "dbt"` on `POST /verification-runs` (`ReceiveVerificationInput.validate` rejects it otherwise), stored on the run row, and threaded verbatim onto `compile.requested:v1` at queue advance and onto `seed.build.requested:v1` when the parsed run has seeds to build. Executor-controller turns it into an `overlay` init container plus a staging prologue on every team-image init container of the compile Job, which copies the team project into a writable `/work` emptyDir, lays the proposed files over the copy, and runs dbt there (see `services/executor-controller.md`), so the manifest the run parses, the candidate SQL it rewrites, and the validation Jobs it runs all describe the proposed fix rather than the committed source. Nothing in this service reads the tarball; it is a pointer it carries.

**The identity pass.** An empty validation seed set is a trivial pass for both kinds, but the two kinds land on different terminals. A candidate with nothing to validate promotes directly: emitting an empty `validation.requested:v1` would be refused by executor-controller as a permanent parse error, leaving no `validation.completed:v1` and blocking the queue indefinitely, so an empty candidate diff trivially passes the gate. A verification whose non-empty candidate has every node matching production takes the same trivial pass and ends `passed` without ever touching `current_prod`: that candidate IS production's own already-proven code, so the fix it carries is proven by identity with that baseline — the shape of a fix that restores a compile-broken model to its promoted content, already proved by compiling it, with nothing left to measure.

**The empty-topology failure.** The one case the identity pass does not cover: a verification whose packaged candidate declares no node at all. It built and checked nothing and can prove nothing, so `failVerificationNothingToValidate` fails it with `fail_reason: "nothing_to_validate"` (no `stage`, no per-node results) rather than reporting an unmeasured fix as verified.

**Terminal announcement.** Every terminal transition, of either kind, on every path in this section, writes a `pipeline.run.finished:v1` outbox row (`enqueueRunFinished`) in the same transaction as the status change, naming the run's candidate schema so executor-controller can drop it whatever the outcome. A candidate additionally writes `release.rejected:v1` on a healable-shaped rejection (`emitReleaseRejected`; a no-op when called for a verification) and `release.promoted:v1` on promotion. A verification writes neither of those — its failure is never a release rejection, whatever caused it, so remediation derives no evidence and opens no heal trigger from it; its only announcement is `pipeline.run.finished:v1`.

### On `POST /releases`
Create a `Received` candidate for the single submitted service (idempotent: an existing `release_id` naming a candidate is a no-op; one already naming a verification run is a `409` conflict). The candidate records its `changed_service`, that service's `image_tag`, its `manifest_kind` (`dbt` or `python`, default `dbt`), and the immutable provenance (`repo`, `commit_sha`) supplied by the caller. `source_overlay_uri` and `verifies_release_id` on the body are rejected (400) before any of this: those fields belong to `POST /verification-runs`. The queue advance then promotes the next `Received` run — of either kind — to active, and a candidate's `manifest_kind` decides how it proceeds:
- `dbt`: transition to `Compiling`, emit `compile.requested:v1` — CI's manifest still needs a fresh dbt compile before it can be parsed.
- `python`: no compile leg exists — CI already compiled and uploaded the `contract.yaml` artifact before this POST — so the release transitions straight to `Parsing` and emits `release.requested:v1` directly, with the manifest set assembled at this same activation (see Manifest-set assembly below).

### On `POST /verification-runs`
Create a `Received` verification for the single named service (idempotent: an existing `run_id` naming a verification is a no-op; one already naming a candidate is a `409` conflict). The verification records its `changed_service`, `image_tag`, `manifest_kind`, `verifies_release_id`, `attempt`, and — for a `dbt` service only — `source_overlay_uri`. It then takes the same queue advance as a candidate, decided by the same `manifest_kind` switch (compile leg for `dbt`, straight to `Parsing` for `python`); see Two kinds of run above for where the two kinds' handling of the shared legs actually differs.

### Manifest-set assembly (at activation, not receipt)
Assembly happens at activation — when the queue advance promotes a release out of `Received` — not at receipt. Under the FIFO queue an earlier release may promote and change another service's pointer before this one activates, so the set must be read from live state at the moment of activation. The queue advance runs this full assembly (every other service's `service_prod` row, combined with the changed service's new canonical key and submitted image tag) for every release regardless of kind, and always persists the resulting per-service image tags onto the release (`SetAssembledImageTags`) — a `dbt` release needs the changed service's own tag for its `compile.requested:v1` payload, but the full map is stored either way. What differs by kind is the `manifest_keys` result of that computation: a `python` release, which has no compile leg, emits it immediately in `release.requested:v1`; a `dbt` release's activation-time `manifest_keys` are not emitted at all — a fresh `service_prod` read (same assembly logic) is performed a second time when the release advances from `Compiling` to `Parsing` on `compile.completed:v1` (see below), because an earlier-queued release may have promoted and changed another service's pointer while this one was compiling, and it is *that* second computation's `manifest_keys` that go out on `release.requested:v1`. Either way, the canonical key is `CanonicalManifestKey`: `.../manifest.json` for a `dbt` service, `.../contract.yaml` for `python`; the changed service's prior `service_prod` pointer, if any, is replaced rather than duplicated. The resulting `manifest_keys` list — one `{service, s3_uri, kind}` entry per service, `kind` explicit on every entry — is emitted in the `release.requested:v1` payload `{release_id, manifest_keys}`.

### Activation guard
Before a release activates, the queue advance verifies that every service live in the `current_prod` topology snapshot is covered by a `service_prod` pointer (or is this release's changed service). If a live service has no pointer, the release stays `Received`, nothing is emitted, and a warning naming the uncovered services is logged; a later queue advance retries automatically. This protects the transition from a fully-populated `current_prod` with an empty `service_prod` table: without coverage, assembly would omit the unpointered services and promotion would retire them. `service_prod` is populated from the existing `current_prod` snapshot by the `seed-service-prod` command (shipped in the service image), which records each service's existing manifest key verbatim and is idempotent. `manifest_kind` is derived per service from its own snapshot nodes, not hardcoded: any node whose `node_type` is python-family (`python-model` or `python-csv`) makes the service `python`, otherwise `dbt`. A service whose nodes mix python and non-python types fails the whole seed before writing anything — a service is always a single kind.

### On `compile.completed:v1`
This section applies to `dbt` releases only: a `python` release never emits `compile.requested:v1` (see the `POST /releases` and Manifest-set assembly sections above), so it never produces this event either.
```
status=failed:
  decode per_node (one entry per compile unit; node_id = service name)
  RecordStageResults("compile", per_node results)
  reason = compileRejection(per_node):
     any entry's failed_container ∈ {parse-prod, parse-candidate} → "parse_rehearsal_failed"
     any entry's failed_container == "upload" → "artifact_upload_failed"
     otherwise (failed_container empty or "compile") → "compile_failed"
  Reject(reason, failing_nodes=[node_id of failed entries])
  emit release.rejected:v1 {release_id, stage="compile", reason, failing_nodes,
       per_node[{node_id, status, dbt_log_uri, run_results_uri}], repo, commit_sha,
       code_bundle_uri=""} (empty: no parse has happened yet)
       (stage stays "compile" for all three reasons; the failed_container
        attribution is what discriminates parse_rehearsal_failed and
        artifact_upload_failed — continuo-internal, never a model defect — from
        compile_failed. remediation excludes both from heal evidence: a
        rehearsal-gate miss is a project *property*, not something a model
        edit can fix, and an artifact-upload failure is continuo-internal.)
  advance queue
status=ok:
  RecordStageResults("compile", per_node results)
  TransitionFromCompiling (Compiling → Parsing)
  re-assemble manifest set from live service_prod (same logic as activation)
  emit release.requested:v1 {release_id, manifest_keys}
  advance queue
```

### On `manifest.loaded.candidate:v1`
```
status=failed → Reject(reason=parse_failed), emit release.rejected:v1, advance queue
status=ok:
  join per-service image_tags into the candidate topology
  DuplicateClaims(topology) non-empty →
  Reject(reason=duplicate_table), emit release.rejected:v1, advance queue, return
      (DuplicateClaims runs two independent checks and merges their results:
       - relation collisions: nodes grouped by resolved_relation_id — the physical relation
         each node's build actually writes — falling back to unique_id for a node whose
         resolved_relation_id is empty. Two nodes with different unique_ids that alias to the
         same relation collide here.
       - identity collisions: nodes grouped by unique_id, reported only when the group is NOT
         relation-homogeneous (a relation-homogeneous group is already reported above, for the
         same members). Two nodes that share a unique_id but resolve to different relations
         collide here — unique_id is the identity key for every downstream lookup keyed on it
         (the code bundle, the candidate object key, this service's own topology walks, the
         orchestrator's :Table MERGE), so sharing it silently erases one of the two nodes
         regardless of whether their resolved relations also collide.
       error_detail names every claim, worded per kind ("<relation> is produced by ..." for a
       relation collision, "unique_id <id> is declared by ..." for an identity one) so an
       operator can tell which remedy applies — an alias rename clears a relation collision but
       can never clear an identity one, since it changes resolved_relation_id, not unique_id.
       failing_nodes lists every claimant's own unique_id across every claim, deduplicated.
       per_node carries a rename proposal's target/competitor pair ONLY for a two-claimant
       RELATION collision — a rename can only ever fix one competitor (so a three-or-more-way
       relation collision gets none either), and no rename a fixer can express changes a
       unique_id (so an identity collision NEVER gets one, regardless of claimant count). Such
       a claim is still rejected and fully named in error_detail/failing_nodes, but contributes
       no per_node entry, and remediation opens no trigger for it; if every claim in the
       release is either an identity collision or a three-or-more-way relation collision,
       per_node is empty. relation_id on a per_node entry carries the contested relation
       itself, separate from node_id (the target claimant's own unique_id) — the two differ
       whenever the target already carries an alias.)
  if release.bootstrap: promote directly, skipping validation
      (record candidate topology, update current_prod, upsert the changed
       service's service_prod pointer, transition to Promoted,
       emit release.promoted:v1); advance queue, return
  load current_prod.topology_snapshot
  derive changed = candidate nodes whose content_hash differs from current_prod, or are new
             # for a verification naming verifies_release_id: the INTERSECTION of two
             # diffs — nodes that differ from current_prod AND from that release's
             # candidate topology. A sibling's not-yet-fixed failure differs from
             # current_prod but matches the rejected candidate (excluded); a node
             # promoted since the rejection matches current_prod but differs from the
             # stale candidate (excluded); only the fix's own edit differs from both
             # (checked). Falls back to the current_prod-only diff when the verified
             # release is unreadable or has no candidate topology.
  inSet = DescendantsClosure(candidate, changed) ∪ FullAncestorsClosure(candidate, that)
          # changed + their downstream + full transitive upstream closure across service boundaries
  for each inSet node: if any of its direct upstreams is absent from the candidate topology:
      Reject(reason=unbuildable_cross_service_upstream), emit release.rejected:v1, advance queue, return
  for each inSet node: upstream_node_ids = inSet ∩ direct upstreams of node (intra- and cross-service)
  if inSet is empty:
      if run.kind == verification and the candidate topology is empty:
          Fail(reason=nothing_to_validate, no stage, no per_node) → Failed,
            emit pipeline.run.finished:v1 only
      else if run.kind == verification:
          trivial pass (the candidate is production's own already-proven code):
            transition to Validating then Passed; current_prod untouched, no release event
      else:
          promote directly (nothing to validate trivially passes the gate):
            update current_prod, transition to Promoted, emit release.promoted:v1
  else if inSet has changed seeds:
      emit seed.build.requested:v1, transition to SeedBuilding
  else:
      transition to Validating, emit validation.requested:v1
        (mode=validation, candidate_schema=_candidate_<release_id>,
         nodes carry upstream_node_ids = all in-set upstreams, intra- and cross-service,
         candidate_artifact_uri = the S3 URI for the node's candidate artifact
           (rewritten SQL for a dbt node, a validation spec for a python node),
         and validation_op = validationOpFor(node, changedClosureSet))
  advance queue
```
`DuplicateClaims` walks the candidate topology for any `unique_id` claimed by more than one node — a relation two nodes both write, the second silently overwriting the first the moment either lands in `current_prod` — and runs above every branch of this handler that can promote: the bootstrap short-circuit immediately below, the nothing-to-validate short-circuit further down, the seed-build leg's own promotion (`handle_seed_build_result.go`), and the post-validation promotion (`handle_validation_result.go`). One check here covers all four paths. The check is service-agnostic: two nodes in the same service collide exactly as two nodes in different services do, so `per_node[]` on the rejection carries a `service`/`file_path` (the rename target), the target's `node_type`, and an `other_service`/`other_file_path` (the competing claimant) — the service name alone does not disambiguate a same-service collision, and `node_type` lets remediation tell a python target (whose relation is declared in the service's contract.yaml, not in `file_path`) apart from a dbt one without its own topology lookup.

`validationOpFor` picks the executor's per-node build strategy from the node's kind and whether it is in the changed closure: a changed-closure dbt node gets `build_from_sql` (candidate SQL already rewritten to the candidate schema); a changed-closure python node gets `build_from_columns` (a JSON spec of declared reads + output columns, since there is no SQL to build from); every other in-set node — an unchanged upstream, of either kind — gets `clone_from_prod` regardless, since it carries no candidate artifact to build from.

An empty in-set is a trivial pass for a candidate and, with one exception, for a verification too. A candidate with nothing to validate promotes directly because emitting an empty `validation.requested:v1` would be refused by executor-controller as a permanent parse error, leaving no `validation.completed:v1` and blocking the queue indefinitely. A verification whose non-empty candidate has every node matching production takes the same trivial pass and ends `passed` without touching `current_prod`: that candidate is production's own already-proven code, so the fix it carries is proven by identity with that baseline. This is the shape of a fix that restores a compile-broken model to its promoted content — the verification has already proved the fix by compiling it, and there is nothing left to measure. The exception is a verification whose packaged candidate declares no node at all: it built and checked nothing and can prove nothing, so it fails with `reason=nothing_to_validate` rather than being reported as verified. That failure carries no `stage` and no `per_node` entries, is never a release rejection, and travels only on `pipeline.run.finished:v1`, so remediation derives no failure evidence from it; agent-remediation's reconciler reads the failed status through `GET /verification-runs/{id}` and records the attempt as failed.

A `bootstrap:true` release skips validation entirely: it records the candidate topology, seeds `current_prod`, and promotes directly. This is the initial cutover (or a trusted re-baseline) against an empty or mismatched `current_prod`. A non-bootstrap release against an empty snapshot instead treats every candidate node as changed and validates the whole topology.

### On `seed.build.completed:v1`
```
status=failed:
  decode per_node (one entry per failed seed node)
  RecordStageResults("seed_build", per_node results)
  Reject(reason=seed_build_failed, failing_nodes=[node_id of failed entries])
  emit release.rejected:v1 {release_id, stage="seed_build", reason, failing_nodes,
       per_node[{node_id, status, dbt_log_uri, run_results_uri}], repo, commit_sha,
       code_bundle_uri, candidate_schema}
  advance queue
status=ok:
  RecordStageResults("seed_build", per_node results)
  compute the filtered validation set: all recorded validation ids minus the just-built seeds
     (seeds already live in the candidate schema and are not validated)
  TransitionFromSeedBuilding (SeedBuilding → Validating), narrowing the persisted
     validation_node_ids to that filtered set so the terminal decision expects exactly the
     nodes the executor emits per-node results for
  emit validation.requested:v1 for the same filtered set (single source: persisted set and emit derive from one computation)
  advance queue
  edge case: if excluding the built seeds leaves an empty validation set, promote directly
```

### On `validation.result:v1`

A single consumer reads both message kinds off one stream. A `kind=node` message projects that node's outcome into the release's `per_node_results` read model (loads the release `FOR UPDATE`, upserts the one node). A `kind=complete` message carries only the decision (`release_id`, `aggregate_status`, `candidate_schema`) and decides by reading back the stored `per_node_results` for the validation stage together with the authoritative `aggregate_status`: it rejects if any *stored* per-node result is non-ok or `aggregate_status` is not ok. A node absent from the store is not counted as failing — for that node the decision rests on `aggregate_status` alone, so the terminal message does not need every `kind=node` message to have already been consumed — there is no completeness barrier.
```
on kind=node: upsert per_node_results[node_id] (stage="validation")
   Nodes skipped by a failed upstream are included: executor-controller emits a
   status="skipped" projection for them even though they never ran.
on kind=complete:
load release FOR UPDATE, read stored per_node_results where stage="validation"
missing-node fallback: a node can be absent from per_node_results either because its
   projection hasn't been consumed yet (each per-node row publishes under its own
   distinct aggregate_id, separate from the terminal's deterministic aggregate_id, so
   per-node rows publish in parallel and relative delivery order isn't guaranteed) or
   because its write was permanently dropped. Either way, do not block or fabricate the node as failing: log
   the missing ids and decide from the authoritative aggregate_status, which reflects
   that node's real outcome regardless of projection visibility. Only stored, non-ok
   nodes count toward failing, so a missing node with aggregate_status ok still promotes.
all stored (and present) nodes ok and aggregate_status ok → handleValidationOK:
   verification → transition to Passed and save; nothing else happens (see below)
   candidate → update current_prod to this release's candidate topology,
      upsert the changed service's service_prod pointer (canonical key + image tag + release id),
      transition to Promoted, emit release.promoted:v1
any stored node not ok (failed or skipped) / aggregate_status not ok → Fail(reason=validation_failed):
   candidate → Rejected, emit release.rejected:v1 {release_id, stage="validation", reason, failing_nodes,
        per_node[{node_id, status, dbt_log_uri, run_results_uri, candidate_artifact_uri,
                  node_type, file_path, service,
                  changed_ancestors[{node_id, file_path, service}]}], repo, commit_sha,
        code_bundle_uri}
   verification → Failed; the same per_node payload is built (see below) but never emitted —
        emitReleaseRejected is a no-op for a verification, so only pipeline.run.finished:v1 announces it
        (per_node sourced from the stored read model, enriched from the candidate topology with each
         node's candidate_artifact_uri, node_type, original_file_path, and service_name — the location
         has to come from the candidate because a rejected release is never promoted, so nothing
         downstream can resolve it from the promoted topology.
         changed_ancestors is computed once per rejection and stamped on every non-ok entry:
         DerivedChangedNodeIDs(candidate, current_prod) gives the release's changed set, and
         release.ChangedAncestors(candidate, node_id, changedSet) intersects it with the node's
         full transitive upstream closure — across service boundaries — excluding the node itself
         and sorted by id. Each entry carries the ancestor's id plus the original_file_path and
         service_name THIS candidate declares for it, for the same reason the failing node's own
         location does: a rejected release is never promoted, and an ancestor it renamed or moved
         is still at its old path in the promoted topology, so an upstream fix routed by that
         topology would edit a file that no longer holds the node. It is what lets the remediation
         agent see that several failures descend from one changed node and repair that node once
         instead of once per victim; an ok node gets no list, since nothing about it needs
         explaining)
advance queue
```

Promotion is shared by the validation-passed path, the bootstrap short-circuit, and the nothing-to-validate short-circuit: all point `current_prod` at the candidate topology, upsert the changed service's `service_prod` pointer (its canonical manifest key, image tag, and this `release_id`), transition the release to `Promoted`, and emit `release.promoted:v1`. The candidate topology (carrying `content_hash` + joined `image_tag`) becomes the new snapshot, so the next release's change-detection diff is correct, and the refreshed pointer is what the next release for any other service assembles against.

A verification takes none of that. Every path in this section that reaches a validated candidate topology calls `concludeValidated`, which checks `Kind()` once and routes a verification to `finishVerification` (transitions to `Passed` and saves) instead of `promoteToProduction` — so the current-prod read, the pointer upsert, the changed-node derivation, and the `release.promoted:v1` emit are simply never reached for one, rather than being made conditional inside `promoteToProduction` itself. The branch lives in the one function every "validated" path funnels through, so no path can promote a verification by taking a different route to production. Promotion telemetry is likewise not recorded for one: nothing was promoted.

Before updating `current_prod`, `promoteToProduction` computes the set of changed node IDs by calling `DerivedChangedNodeIDs` against the pre-update `current_prod` snapshot (bootstrap emits all nodes as changed). Each node in the `release.promoted:v1` topology carries a `changed` boolean reflecting membership in that set; orchestrator's topology-swap handler refreshes every node's `:Table.content_hash` regardless of this flag, but reads it to decide which seeds to build into production. The event's top-level `repo`, `commit_sha`, and `promoted_at` fields taken from the release's provenance are consumed by orchestrator's separate version-ingestion handler, which records them as each written `:NodeVersion`'s own provenance stamp.

### On `POST /releases/{id}/retry-remediation`

Unlike every other transition in this section, this one is not driven by an inbound stream message through the FIFO queue advance — it is a synchronous HTTP request triggered by a human clicking "Try again" on a dead-end rejected release, handled inline against a `FOR UPDATE`-locked release row (`RetryRemediation`, `service/handlers/retry_remediation.go`). It never touches the release's own `status` (a retried release stays `rejected`) or the FIFO queue; the only effects are the round bump and the outbox row.

```
load run FOR UPDATE
run absent → 404 not_found
run.status != rejected → 409 not_rejected
    (a verification's status is never "rejected" — Fail always lands it on
    "failed" — so a verification run's id is refused here already, before the
    kind check below is ever reached in practice)
run.kind != candidate → 404 not_found
    (defensive: a verification is not a release, reported the same as an
    unknown id; unreachable through this endpoint under normal operation for
    the reason above)
run.fail_reason not in {compile_failed, seed_build_failed, validation_failed,
    duplicate_table} → 409 not_healable
run.rejection_payload empty (no payload was ever stored for this rejection —
    the release predates the column, or the rejection reason never stores one) →
    409 not_retryable
run.remediation_round >= MaxRemediationRounds (3) → 409 rounds_exhausted
ListProposals(release_id) via agent-remediation gRPC (see "gRPC calls to
    agent-remediation" above):
    RPC error → 502 proposal_reader_unavailable
    no proposal belongs to release.remediation_round → 409 retry_in_progress
    among that round's proposals, any node's latest attempt is
        generating/verifying → 409 proposal_open {proposal_id, pr_url}
        proposed, unless every owning service's PR is rejected (a dead end) →
            409 proposal_open {proposal_id, pr_url}
        (a proposed attempt whose every per-service PR was closed without
        merging is a dead end and does not block; one still lacking a PR, or
        holding a non-rejected one, still blocks)
    among every round's proposals, any attempt has any per-service PR that is
        opening/open/merged → 409 proposal_open {proposal_id, pr_url}
    otherwise, in one transaction:
        round = StartRemediationRound(now)  # remediation_round + 1; records a
            "remediation_retry" transition
        Save(release)
        decode stored rejection_payload, set top-level remediation_round=round,
            re-encode
        outbox insert: stream=remediation.retry_requested:v1, deterministic id
            remediation-retry:<release_id>:<round>, payload = the re-encoded
            rejection
        commit
        202 {release_id, remediation_round=round}
```

The refusal order matters: existence, then rejection state (which also screens out every verification run), then the defensive kind check, then healability, then whether there is anything to replay, then the round cap, and only then the one check that costs a network call (`ListProposals`) — so a request that a purely local read can already refuse never reaches agent-remediation at all. The round is capped in code (`release.MaxRemediationRounds = 3`), not in the chart or an env var: raising it means shipping a new release-controller binary, an explicit choice against runaway "try again" chains rather than a per-install tunable. The `retry_in_progress` check exists because a round's proposal rows are written asynchronously by agent-remediation, after `remediation.retry_requested:v1` is consumed — a second `POST` in the window between the round bump and that first row landing would otherwise see an empty current-round proposal set and read it as "nothing open," spending a second round and racing the first at the agent.

## Consumer Reliability

- Four consumer groups (`compile.completed:v1`, `manifest.loaded.candidate:v1`, `seed.build.completed:v1`, `validation.result:v1`) run in the same process; each maintains its own offset.
- Inbound messages are deduped via `message_processing` (idempotent on the upstream `outbox_entry_id`), so a redelivery is absorbed.
- A permanent parse-decode failure (or an unrecognised `validation.result:v1` kind) is ACKed (logged, not retried); transient errors are not ACKed and replay. The `kind=complete` decision reads the stored `per_node_results` projections together with the authoritative `aggregate_status` column, so it never needs to defer: a `kind=node` message that hasn't been consumed yet or a permanently-dropped projection write both leave a node absent from `per_node_results`, and for that node the decision falls back to `aggregate_status` alone rather than blocking the release or the queue; only a node that is present and non-ok in the store counts toward the reject.
- A message on any of the inbound streams whose `release_id` no longer has a `release_pipeline_runs` row (pruned, or reclaimed from a previous consumer for a deleted run) is logged and dropped rather than processed. The repository's `Get` returns no row as `(nil, nil)`, so all handlers nil-check the aggregate before use; without that guard a reclaimed message for a missing run would crash the consumer on startup.
- State changes and the outbox row are written in one transaction; the outbox publisher drains rows and XADDs them, injecting `outbox_entry_id` for downstream dedup.

## Background Loops

| Loop | Description |
|---|---|
| Outbox publisher | Drains `release_controller_outbox` and XADDs each row to its stream. |
| `compile.completed:v1` consumer | Dispatches to the compile-result handler. |
| `manifest.loaded.candidate:v1` consumer | Dispatches to the parsed-manifest handler. |
| `seed.build.completed:v1` consumer | Dispatches to the seed-build-result handler. |
| `validation.result:v1` consumer | Routes by kind: `node` → per-node projection handler, `complete` → terminal validation-result handler, then advances the queue. |
| Retention (`PruneFinishedRuns`) | Runs on the janitor interval (`RELEASE_JANITOR_INTERVAL`, default 24h). Deletes terminal runs of either kind — candidate (`promoted`, `rejected`, `superseded`) or verification (`passed`, `failed`) — whose creation timestamp is older than `RELEASE_RETENTION_DAYS` (default 90 days). Never deletes the run referenced by `current_prod` or by any `service_prod` pointer (a verification, which writes neither, is never protected by this exception — only a candidate ever is). For each pruned run, also deletes the `candidate-sql/<run_id>/` and `code-bundles/<run_id>/` S3 prefixes (soft-fail — a delete error does not abort the prune). |

## S3 Behavior

| Operation | Description |
|---|---|
| `DeleteObjects` | Delete the `candidate-sql/<run_id>/` prefix for each pruned run during retention; soft-fail (a delete error is logged but does not abort the prune). |
| `DeleteObjects` | Delete the `code-bundles/<run_id>/` prefix for each pruned run during retention; soft-fail (a delete error is logged but does not abort the prune). |

The S3 bucket is not managed by release-controller's Helm chart. A native S3 lifecycle expiry rule (30 days) on the `candidate-sql/` prefix is configured via a one-time bootstrap for production and the minio-init compose service for development; the prune-time delete is the primary reclaim path there, with the lifecycle rule as backstop. The `code-bundles/` prefix carries the same 30-day lifecycle rule, configured the same way, with the prune-time delete as its primary reclaim path and the lifecycle rule as backstop.

## gRPC Callers

None — release-controller is not called via gRPC by any service.

## Reliability Notes

- Idempotent on `release_id`: a redelivered `POST /releases` or re-promotion is a no-op; `release.promoted:v1` carries a deterministic aggregate id so orchestrator dedups re-emissions.
- Change detection relies on each candidate node carrying a non-empty `content_hash` (topology-controller emits a fold of its own source checksum, its transitive macro checksums, and its resolved-config fingerprint (`source_hash`/`shared_code_hash`/`config_hash`), with deterministic fallbacks so it is never empty). An empty-vs-empty hash would skip validation; this is structurally avoided upstream. Because macros and resolved config are folded in, a shared-macro edit or a config-only change (e.g. a materialization bump in `dbt_project.yml`) changes the hash of every affected node and re-runs them through the validation gate even when that node's own `.sql` is untouched.
- The first release into an empty `current_prod` is submitted with `bootstrap:true`: it promotes without validation and seeds `current_prod` from the candidate topology, giving subsequent releases a change-detection base. A bootstrap promotes whatever topology it carries, so it must be a trusted one.
