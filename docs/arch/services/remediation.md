# remediation

## Purpose

`remediation` is the triage gate for failed nodes in the blue/green validation pipeline. It consumes `release.rejected:v1`, fetches the execution log from S3 for each failing node, and deterministically sorts the failure into a category. Every classification decision — whether the outcome is to emit a trigger or to drop the failure — is recorded in Postgres so that no dropped failure is invisible. For each healable failure, the service enqueues a `remediation.requested:v1` trigger so a downstream agent can investigate and propose a fix. Rejections of shadow verification releases are the one class routed on something other than the failure itself: they are recorded and then dropped, because they report on a fix the agent is already waiting for rather than on a change anyone shipped. It also consumes `remediation.retry_requested:v1` — release-controller's replay of a rejected release's stored rejection, tagged with an incremented `remediation_round`, produced when a human asks the release to "try again" — through the exact same classification handler, so a retry walks the same path as the original rejection, one round later.

**Runtime**: Go service. HTTP `/healthz` on port 8090. Depends on Postgres (`continuo_remediation`), Redis, and S3.

## Owned Storage

Postgres database `continuo_remediation`. Tables:

| Table | Purpose |
|---|---|
| `classification_decision` | One row per classified node per stage per remediation round. Records `source` (`validation`, `compile`, `seed_build`, or `duplicate_table`), `release_id`, `remediation_round` (default `1`; the release's remediation round this decision belongs to — a human's "try again" on a rejected release starts a new round), `node_id`, `category`, `error_signature`, `decision` (`emit` or `drop`), `reason` (matched rule, e.g. `infra:connection_refused`, or `shadow_verification` for a rejected fix-verification release), `dbt_log_uri`, and `created_at`. Natural key `(source, release_id, remediation_round, node_id)` gives idempotency: a redelivered rejection or retry neither re-records nor re-emits, while a later round classifies the same node into its own fresh row rather than colliding with an earlier round's. |
| `remediation_outbox` | Transactional outbox; one row per `remediation.requested:v1` trigger, drained by the outbox publisher. |
| `message_processing` | FK target of `remediation_outbox.message_processing_id` (canonical outbox table shape). Not used for inbound consumer dedup; inbound idempotency is enforced by the `classification_decision` natural key `(source, release_id, remediation_round, node_id)`. |

The `classification_decision` table is append-only and auditable: it contains every decision, including dropped infra failures, so the full triage history is queryable without needing downstream systems.

## Inbound Interfaces

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `release.rejected:v1` | `remediation-release-rejected` | Emitted by release-controller when a release fails at any pipeline stage (compile, seed_build, or validation). The `stage` field in the payload determines how each failing node is classified. Each message carries one or more failing nodes (one for compile, one per seed for seed_build, one per model for validation); the handler processes each node independently. A `reason` of `parse_rehearsal_failed` or `artifact_upload_failed` (both `stage="compile"`) is excluded entirely at the inbound adapter, before any evidence is built — see "Parse-export-leg exclusion" below. |
| `remediation.retry_requested:v1` | `remediation-retry-requested` | Emitted by release-controller when a human retries a rejected release (`POST /releases/{id}/retry-remediation`). The payload is release-controller's own stored `release.rejected:v1` payload, replayed verbatim plus a top-level `remediation_round`; it is decoded and classified by the exact same handler as `release.rejected:v1` (`classifyRejectionMessages` builds both consumers over one shared handler), one round later. |

### HTTP (port 8090)

Two health endpoints backed by the `pkg/liveness` registry, with readiness and
liveness deliberately split:

- `GET /healthz` — readiness. Fails (503) when the `release.rejected:v1`
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
| `remediation.requested:v1` | `agent-remediation` (group `agent-remediation-remediation-requested`) | A classified node is healable (`category != infra_transient`), the rejected release is not a shadow verification, and the node has not been seen before within its remediation round (first insertion in `classification_decision` for that `(source, release_id, remediation_round, node_id)` key) — a later round reclassifies the same node as a fresh first-insertion, independent of an earlier round's row. |

All events are written to the outbox inside the same transaction as the `classification_decision` insert and published with a deterministic `event_id` for consumer-side dedup.

Calls no gRPC services. Exposes no gRPC services.

## Failure Taxonomy

The domain classifier (`remediation/domain/failure/classify.go`) applies a fixed, ordered rule set. Precedence: `infra_transient` is checked first so a genuine infrastructure failure is never mislabelled; then `test`, then `logic`; anything unmatched falls to `unknown`. The classifier is pure and never errors.

| Category | `decision` | Signal |
|---|---|---|
| `infra_transient` | `drop` | Unambiguously infrastructure: connection refused, could not connect to database, OOMKilled, ImagePullBackOff / back-off pulling image, InvalidAccessKeyId / AccessDenied (S3 credentials). Exactly four hard-drop families. |
| `test` | `emit` | dbt test assertion failures: "failure in test", "configured to fail if". |
| `logic` | `emit` | SQL/model defects: relation/object does not exist, compilation error, syntax error, missing ref, type mismatch, ambiguous column. |
| `unknown` | `emit` | Everything else — including the ambiguous resource/permission class (statement timeout, permission denied, deadlock, generic out-of-memory) and an unreachable log (`unknown:log_unavailable`). |

**Under-drop policy**: only the four confidently infrastructure signal families are dropped. All ambiguous cases, including signals that might be infra-related but could also be a model problem, fall through to `unknown` and are emitted. Uncertainty flows to the agent; only confident infra is silenced.

**Shadow verifications are dropped regardless of category.** A rejection whose payload carries `shadow: true` came from a release `agent-remediation` posted to verify a fix proposal, not from a change anyone shipped. Its decision is recorded with the category and signature the rules produced — so the row still says truthfully what failed — but overridden to `decision=drop`, `reason=shadow_verification`, and nothing is emitted. The agent learns that verdict by polling the release it submitted; routing it back through the classifier as well would remediate a failed fix attempt with another fix attempt and never terminate.

A log that cannot be fetched from S3 (URI not found) is treated as an empty log and classified `unknown:log_unavailable` with `decision=emit`.

A `duplicate_table` rejection never reaches this table: `ClassifyDuplicateTable` returns a fixed `logic`/`emit` classification directly from the evidence, with no log fetch at all — the rejection happens at parse time, before any Job runs, so there is no dbt log to scan.

## Error Signature Normalization

The signature is derived from one piece of the log: the key error line (`keyErrorLine`, `remediation/domain/failure/classify.go`), which is also the `error_excerpt` carried on the trigger and recorded in the case base. It is the first line mentioning "error" or "failure" (else the first non-blank line), with ANSI colour codes removed — except when that line is dbt's lead-in `[ERROR]: Encountered an error:`, which precedes every error dbt reports and says nothing about the failure. Keyed on the lead-in, every failure a node ever raised would share one signature, which is how unrelated compile errors once exhausted the agent's attempt cap and polluted precedent lookup. When the error line is the lead-in, the key is the message block after it: its consecutive non-blank lines up to the next timestamped log line, at most three, joined with a space — for a compile failure, `Compilation Error in model <name> (models/<name>.sql) <detail> line N`. A log that ends at the lead-in keys on the lead-in itself. Fixture tests under `remediation/domain/failure/testdata/` pin the key line for verbatim logs captured from real dbt Jobs, and the compile e2e test asserts the excerpt on a live rejection.

`NormalizeSignature` (`remediation/domain/failure/signature.go`) then produces a stable dedup key from that text. It strips the volatile parts that vary release-to-release:

- Candidate schema names (`candidate_<hex>`  → `candidate_schema`)
- UUIDs and invocation IDs
- ISO timestamps and clock times
- Row counts ("got 14 results" → "got N results")
- Source line/column positions
- Remaining standalone digit runs

After stripping, it folds the category with the normalized error text and returns a SHA-256 hex digest. The same underlying error in two different releases yields an identical `error_signature`, enabling the downstream agent to correlate recurring failures across releases.

## Key Flows

### Parse-export-leg exclusion

Before any `FailureEvidence` is built, the inbound adapter (`release_rejected_binding.go::evidenceFromRejected`) checks the payload's `reason`: `parse_rehearsal_failed` and `artifact_upload_failed` both short-circuit to an empty evidence list, so the consumer ACKs the message having produced nothing at all — no `classification_decision` row, no `remediation.requested:v1` trigger. Both reasons are `stage="compile"`, but neither is a model defect: a parse-rehearsal miss is a project *property* (partial parsing disabled, or an `env_var()` read at parse time that differs between compile and run pods) and an artifact-upload failure is continuo-internal: no change to the dbt project fixes either, so proposing a heal would misdirect the agent onto SQL that was never the problem. `compile_failed`, `seed_build_failed`, and `validation_failed` resolve to their pipeline leg; the stage-less `duplicate_table` resolves to its own source. Every other stage-less reason — `parse_failed` and `unbuildable_cross_service_upstream` — reaches `sourceFromPayload` and is dropped there (`ok=false`), producing no evidence, no `classification_decision` row, and no trigger: neither is a model defect a heal proposal could fix.

### On `release.rejected:v1` and `remediation.retry_requested:v1` — per failing node

Both streams decode to the identical payload shape — `remediation.retry_requested:v1` is release-controller's own stored `release.rejected:v1` payload replayed with one field added, the top-level `remediation_round` — so `classifyRejectionMessages` builds one handler shared by both consumer groups (`remediation-release-rejected` and `remediation-retry-requested`); nothing below the inbound adapter can tell which stream a message arrived on.

```
0. Exclude reason=parse_rehearsal_failed / artifact_upload_failed entirely
   (see "Parse-export-leg exclusion" above) — ACK with no evidence built.
1. Parse the event; extract stage ("compile" | "seed_build" | "validation") and,
   for each entry in per_node where status="failed":
   build FailureEvidence {source (derived from stage), release_id,
   remediation_round, node_id, dbt_log_uri, run_results_uri,
   candidate_artifact_uri, repo, commit_sha, code_bundle_uri}.
   remediation_round is the payload's top-level field, absent (and thus zero)
   on the original release.rejected:v1 rejection; ClassifyFailure normalizes
   an unset or zero round to 1 before anything downstream reads it, so round
   1 and "no round field at all" are handled identically.
   When `stage` is absent, the `reason` field is used as the source fallback.
   For source=duplicate_table, FilePath/Service (the rename target),
   OtherService/OtherFilePath (the competing claimant), and RelationID (the
   contested physical relation, distinct from node_id whenever the target
   carries an alias) are threaded straight from the payload's per_node entry,
   the same way seed_build's are (step 2c). release-controller emits a
   per_node entry for a duplicate_table claim only for a two-claimant
   RELATION collision — a rename proposal can only ever target one
   competitor, and no rename can clear an identity collision (two nodes
   sharing a unique_id) at all — so a three-or-more-way relation collision or
   any identity collision has no per_node entry and this loop never visits
   it: no FailureEvidence, no classification_decision row, and no
   remediation.requested:v1 trigger for that claim, even though it still
   appears in the release's error_detail and failing_nodes.
1b. For source=duplicate_table: skip steps 2–2c entirely — the rejection
    happens at parse time, before any Job runs, so there is no dbt log or
    run_results to fetch — and classify directly via
    ClassifyDuplicateTable(ev), which keys the signature on ev.RelationID
    (falling back to ev.NodeID when empty, for a trigger predating the
    field), straight to step 3's Category/Signature/Decision/Reason. Keying
    on the relation rather than the target's own node id matters because the
    target flips between releases (Target prefers whichever service the
    release actually changed): keying on node id would fork one recurring
    collision into two signatures the moment the changed service alternates.
2. Fetch dbt log text from S3 at dbt_log_uri.
   - If not found: logText = "" (→ unknown:log_unavailable).
   - If transient S3 error: return error (message stays in PEL, retried).
2b. If run_results_uri is set: fetch + parse the structured validation result
    (schema_version/status/message/failures/unique_id) via `pkg/validationresult`,
    the shared Go implementation of this wire contract (also used by
    k8s-controller). A candidate is accepted only when its schema_version
    matches the contract and its status is one of success | error | fail |
    skipped; anything else — no run_results, no matching JSON object, a wrong
    schema_version, or an unsupported status — is treated the same as a parse
    failure. A transient S3 error returns (retried); a parse failure logs and
    leaves structured=nil (text-log fallback).
2c. Source-path resolution by stage:
    - compile: call ExtractDbtFilePath(logText) to derive the offending
      project-relative source file path from the dbt error output (the log
      names the file as “models/…​.sql”). Compile failures use a synthetic
      service-name NodeID (not a real dbt node) so the log is the only source.
      Result is threaded into FailureEvidence.FilePath.
    - seed_build: the release.rejected:v1 per-node payload carries file_path
      and service from the candidate topology (OriginalFilePath and ServiceName
      on release.Node). These are parsed by the release_rejected_binding adapter
      into FailureEvidence.FilePath and FailureEvidence.Service, and forwarded
      into the remediation.requested:v1 trigger. This allows the agent to
      locate the source file without a `GetNodeLocation` round trip to
      orchestrator, which is critical for newly-added seeds that exist only in
      the candidate release and are therefore absent from the promoted
      topology `GetNodeLocation` serves.
    - validation: the release.rejected:v1 per-node payload carries node_type,
      file_path, and service from the candidate topology (NodeType,
      OriginalFilePath and ServiceName on release.Node), decoded by the same
      release_rejected_binding fields as seed_build's and forwarded onto the
      trigger. node_type is what lets the agent pick the right lane before any
      read: a python node's validation failure is repaired in the contract yaml
      declaring it, not in a SQL file, and its candidate artifact is a JSON
      validation spec, so the agent's empty-artifact skip never fires for it.
      file_path/service give
      the agent the path THIS candidate declares, which `GetNodeLocation` cannot:
      a rejected release is never promoted, so the promoted topology it serves
      holds nothing for a newly-added node and the previous release's path for a
      node whose candidate moved it.
3. ClassifyWithStructured(ev, structured, logText) → Category, Signature, Decision,
   Reason, Excerpt (pure, deterministic). Prefers the structured record: status=fail
   → test; status=error → message through the infra/logic rules. Falls back to the
   text-log Classify path when structured is nil or carries no message. Excerpt is
   the key error line the signature was derived from, capped at 4 KiB.
3a. Shadow override: when the rejected release carries shadow=true, the decision
   recorded in step 4a becomes decision=drop, reason=shadow_verification,
   regardless of what the rules concluded. The category and error signature are
   still the rules' own, so the row remains a truthful record of what the failure
   was — only the routing changes. A shadow release verifies a fix proposal and
   never promotes, so its rejection means the proposed fix did not work;
   emitting a trigger for it would hand a failed fix attempt back as a fresh
   failure to heal, and the loop would never end. The decision is still written,
   because no drop in this service is invisible.
4. Open transaction:
   a. Upsert classification_decision (source, release_id, remediation_round, node_id).
      - If already exists (redelivery, or a re-read of this exact round): inserted=false
        → skip enqueue, commit, done. A later round's row has a different
        remediation_round, so it is never mistaken for an earlier round's decision.
   b. If inserted && decision == emit:
      - Build RemediationRequested payload: pointer-first (the full log stays
        behind dbt_log_uri, the failing code behind code_bundle_uri), carrying
        the classifier's reason, remediation_round, and capped error_excerpt inline.
      - Enqueue remediation_outbox row (stream=remediation.requested:v1,
        event_id = RemediationEventID(release_id, node_id, remediation_round):
        round <= 1 → SHA1(namespace, release_id+"|"+node_id), reproducing every
        id minted before rounds existed; round >= 2 → the same SHA1 input with
        a "|round|<n>" suffix, so a retry's trigger is never mistaken for the
        rejection's original one).
5. Commit.
```

### Outbox publisher

A background goroutine drains `remediation_outbox` rows with `status='pending'` and XADDs each to its stream, injecting `outbox_entry_id` for downstream dedup.

## Payload Shape (`remediation.requested:v1`)

The trigger is pointer-first: the full log stays behind `dbt_log_uri` and the failing code behind `code_bundle_uri`, and the agent fetches and redacts the full log before any LLM call. The one piece of error text carried inline is `error_excerpt` — the classifier's key error line, capped at 4 KiB — kept so the orchestrator can record the failure as queryable precedent in its case base.

| Field | Description |
|---|---|
| `event_id` | Deterministic SHA1 UUID keyed on `release_id\|node_id` for `remediation_round <= 1` — reproducing the id every trigger minted before rounds existed. For `remediation_round >= 2` (a human's "try again"), a `\|round\|<n>` suffix is folded in, so a retry's trigger for the same node is never mistaken for an earlier round's. Stable on redelivery. |
| `source` | Origin pipeline. One of `validation`, `compile`, `seed_build`, or `duplicate_table`. |
| `release_id` | The rejected release identifier. |
| `remediation_round` | The release's remediation round this trigger belongs to: `1` for the rejection itself, `+1` per human "try again" on the release. |
| `node_id` | The unique_id of the failing dbt node — for duplicate_table, the target claimant's own unique_id. |
| `relation_id` | Duplicate_table only: the contested physical relation, threaded from `per_node[].relation_id`. Distinct from `node_id` — the two differ whenever the target claimant carries an alias. Used for the classification signature and the remediation agent's rename prompt, both of which need the relation, not the target's own declared name. Empty for every other source. |
| `category` | `logic`, `test`, or `unknown`. |
| `error_signature` | Release-stable normalized dedup key (SHA-256 hex). |
| `reason` | The matched classifier rule, e.g. `logic:missing_object`, `infra:connection_refused`. |
| `error_excerpt` | The classifier's key error line, capped at 4 KiB; empty when no log text exists. Kept for the orchestrator's failure-precedent case base. |
| `code_bundle_uri` | S3 URI of the rejected release's code-bundle document, threaded from `release.rejected:v1`'s top-level `code_bundle_uri`. Set for duplicate_table, validation, and seed_build rejections. Empty for compile-stage rejections, which precede the parse that produces the bundle. |
| `dbt_log_uri` | S3 URI of the full dbt execution log. |
| `candidate_artifact_uri` | S3 URI of the node's candidate artifact — rewritten SQL for a dbt node, a validation spec for a python node (candidate-schema form; omitted for seeds and compile failures). |
| `file_path` | Project-relative source file path. Non-empty for compile failures (extracted from the dbt log) and for seed_build, validation, and duplicate_table failures (threaded from `per_node[].file_path`, which release-controller stamps from the candidate topology's `OriginalFilePath`; for duplicate_table it is the rename target's file). When present, the agent bypasses the `GetNodeLocation` (orchestrator) lookup — which is required for correctness, not just economy: that lookup serves the promoted topology, and a rejected release is never promoted. |
| `service` | Owning dbt service name for the failing node. Non-empty for seed_build, validation, and duplicate_table failures (threaded from `per_node[].service`, stamped from the candidate topology's `ServiceName`; for duplicate_table it is the rename target's service). Empty for compile, where NodeID is the service. |
| `node_type` | The failing node's kind (`dbt-model`, `dbt-seed`, `dbt-snapshot`, `python-model`, or `python-csv`), threaded from `per_node[].node_type`. Non-empty for validation and duplicate_table failures. It is what lets the agent decide a python node's handling without a topology lookup of its own: a validation failure on a `python-model` node routes to the python contract fixer and on a `python-csv` node to its own dedicated contract fixer — both repair the yaml declaring the node and verify the repair with a shadow release — while the duplicate-table fixer skips either python kind outright because its relation is declared in the service's contract.yaml, not in `file_path`. Empty for compile and seed_build. |
| `other_service` | Duplicate_table only: the competing claimant's service name — the node that also produces the contested relation (`relation_id`). Empty for every other source. |
| `other_file_path` | Duplicate_table only: the competing claimant's file path. Carried alongside `other_service` because two nodes in the *same* service can collide, where the service name alone identifies nothing. Empty for every other source. |
| `repo` | GitHub owner/name from the originating release. |
| `commit_sha` | Full commit SHA from the originating release. |
| `classified_at` | RFC 3339 timestamp of classification. |

## Consumer Reliability

- Inbound idempotency is enforced by the natural key `(source, release_id, remediation_round, node_id)` in `classification_decision`. A redelivered `release.rejected:v1` or `remediation.retry_requested:v1` message produces the same outcome: the Upsert detects the existing row for that round (`inserted=false`) and skips re-enqueue. `remediation_round` is part of the key precisely so a retry's replay of the same node is not swallowed by round 1's row. The `message_processing` table is present as the FK target of `remediation_outbox.message_processing_id` and is not used for inbound consumer dedup.
- A transient S3 fetch error is not wrapped in `ErrPermanent` and causes the message to stay in the PEL for retry. A permanent decode failure (malformed payload) is ACKed by returning nil from the handler (not retried).
- The `classification_decision` insert and the `remediation_outbox` enqueue are performed in one transaction, so a crash between them cannot produce a trigger without a decision record, or a decision record without a trigger.

## Non-Responsibilities

`remediation` is a triage gate only. It does not:

- Hold or query any proposed fix, patch, or rewrite.
- Track which remediations succeeded or failed.
- Rank, prioritize, or deduplicate triggers across releases.
- Consume production-run failures.
- Own any lifecycle past emitting the trigger.

All solution state — proposals, what-worked history, agent conversations — belongs to downstream services.

## Background Loops

| Loop | Description |
|---|---|
| Outbox publisher | Drains `remediation_outbox` and XADDs each pending row to `remediation.requested:v1`. |
| `release.rejected:v1` consumer | Dispatches each rejected-release message to the `ClassifyFailure` handler. |
| `remediation.retry_requested:v1` consumer | Dispatches each retried-release message to the same `ClassifyFailure` handler, tagged with the incremented `remediation_round`. |

## Key Code Paths

| Concern | Path |
|---|---|
| Domain model (evidence, categories, decisions, source constants) | `remediation/domain/failure/evidence.go` |
| Deterministic classifier (incl. stage dispatch, ExtractDbtFilePath, and ClassifyDuplicateTable) | `remediation/domain/failure/classify.go` |
| Signature normalization | `remediation/domain/failure/signature.go` |
| Trigger payload (incl. FilePath and RemediationRound fields) | `remediation/domain/event/remediation_requested.go` |
| Application handler (stage discrimination, FilePath derivation, round normalization) | `remediation/service/handlers/classify_failure.go` |
| Inbound adapter (stage→source mapping, reason fallback, `release.rejected:v1` shape) | `remediation/adapters/redis/release_rejected_binding.go` |
| `remediation.retry_requested:v1` consumer (shares the inbound adapter's handler) | `remediation/adapters/redis/remediation_retry_binding.go` |
| Decision repository port | `remediation/domain/repository/decision_repository.go` |
| Postgres adapter | `remediation/adapters/postgres/decision_repository.go` |
| S3 log reader | `remediation/adapters/s3/log_reader.go` |
| Redis ingress + outbox publisher | `remediation/adapters/redis/` |
| DB migrations | `db/migration/remediation/V1__init_remediation.sql` through `V4__classification_decision_round.sql` (the latter adds the `remediation_round` column and widens the natural-key unique constraint to `(source, release_id, remediation_round, node_id)`) |
