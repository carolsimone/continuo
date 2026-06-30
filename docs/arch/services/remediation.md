# remediation

## Purpose

`remediation` is the triage gate for failed dbt nodes in the blue/green validation pipeline. It consumes `release.rejected:v1`, fetches the dbt execution log from S3 for each failing node, and deterministically sorts the failure into a category. Every classification decision — whether the outcome is to emit a trigger or to drop the failure — is recorded in Postgres so that no dropped failure is invisible. For each healable failure, the service enqueues a `remediation.requested:v1` trigger so a downstream agent can investigate and propose a fix.

**Runtime**: Go service. HTTP `/healthz` on port 8090. Depends on Postgres (`continuo_remediation`), Redis, and S3.

## Owned Storage

Postgres database `continuo_remediation`. Tables:

| Table | Purpose |
|---|---|
| `classification_decision` | One row per classified node per stage. Records `source` (`validation`, `compile`, or `seed_build`), `release_id`, `node_id`, `category`, `error_signature`, `decision` (`emit` or `drop`), `reason` (matched rule, e.g. `infra:connection_refused`), `dbt_log_uri`, and `created_at`. Natural key `(source, release_id, node_id)` gives idempotency: a redelivered rejection neither re-records nor re-emits. |
| `remediation_outbox` | Transactional outbox; one row per `remediation.requested:v1` trigger, drained by the outbox publisher. |
| `message_processing` | FK target of `remediation_outbox.message_processing_id` (canonical outbox table shape). Not used for inbound consumer dedup; inbound idempotency is enforced by the `classification_decision` natural key `(source, release_id, node_id)`. |

The `classification_decision` table is append-only and auditable: it contains every decision, including dropped infra failures, so the full triage history is queryable without needing downstream systems.

## Inbound Interfaces

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `release.rejected:v1` | `remediation-release-rejected` | Emitted by release-controller when a release fails at any pipeline stage (compile, seed_build, or validation). The `stage` field in the payload determines how each failing node is classified. Each message carries one or more failing nodes (one for compile, one per seed for seed_build, one per model for validation); the handler processes each node independently. |

## Outbound Interfaces

### Redis producer

| Stream | Consumed by | Emitted when |
|---|---|---|
| `remediation.requested:v1` | `remediation-agent` (group `remediation-agent-remediation-requested`) | A classified node is healable (`category != infra_transient`) and has not been seen before (first insertion in `classification_decision`). |

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

A log that cannot be fetched from S3 (URI not found) is treated as an empty log and classified `unknown:log_unavailable` with `decision=emit`.

## Error Signature Normalization

`NormalizeSignature` (`remediation/domain/failure/signature.go`) produces a stable dedup key for each failure. It strips the volatile parts that vary release-to-release:

- Candidate schema names (`candidate_<hex>`  → `candidate_schema`)
- UUIDs and invocation IDs
- ISO timestamps and clock times
- Row counts ("got 14 results" → "got N results")
- Source line/column positions
- Remaining standalone digit runs

After stripping, it folds the category with the normalized error text and returns a SHA-256 hex digest. The same underlying error in two different releases yields an identical `error_signature`, enabling the downstream agent to correlate recurring failures across releases.

## Key Flows

### On `release.rejected:v1` — per failing node

```
1. Parse the event; extract stage ("compile" | "seed_build" | "validation") and,
   for each entry in per_node where status="failed":
   build FailureEvidence {source (derived from stage), release_id, node_id,
   dbt_log_uri, run_results_uri, candidate_sql_uri, repo, commit_sha}.
   When `stage` is absent, the `reason` field is used as the source fallback.
2. Fetch dbt log text from S3 at dbt_log_uri.
   - If not found: logText = "" (→ unknown:log_unavailable).
   - If transient S3 error: return error (message stays in PEL, retried).
2b. If run_results_uri is set: fetch + parse the structured validation result
    (status/message/failures/unique_id). A transient S3 error returns (retried);
    a parse failure logs and leaves structured=nil (text-log fallback).
2c. For compile sources only: call ExtractDbtFilePath(logText) to derive the
    offending project-relative source file path from the dbt error output (the
    log names the file as "models/…​.sql" or similar). The result is threaded into
    FailureEvidence.FilePath and later into the remediation.requested:v1 trigger
    so the agent can read the real source file directly. Compile failures use a
    synthetic service-name NodeID (not a real dbt node), so the log is the only
    source of the path. Seed_build and validation failures have a real dbt node
    unique_id; the agent resolves the source file via the ancestry node lookup
    (NodeContext) and FilePath is left empty at this stage.
3. ClassifyWithStructured(ev, structured, logText) → Category, Signature, Decision,
   Reason (pure, deterministic). Prefers the structured record: status=fail → test;
   status=error → message through the infra/logic rules. Falls back to the text-log
   Classify path when structured is nil or carries no message.
4. Open transaction:
   a. Upsert classification_decision (source, release_id, node_id).
      - If already exists (redelivery): inserted=false → skip enqueue, commit, done.
   b. If inserted && category.Healable():
      - Build RemediationRequested payload (pointer-only: no error text).
      - Enqueue remediation_outbox row (stream=remediation.requested:v1,
        event_id = SHA1(namespace, release_id+"|"+node_id)).
5. Commit.
```

### Outbox publisher

A background goroutine drains `remediation_outbox` rows with `status='pending'` and XADDs each to its stream, injecting `outbox_entry_id` for downstream dedup.

## Payload Shape (`remediation.requested:v1`)

The trigger is pointer-only: it contains no error text, no stack traces, no raw log content. The agent fetches the full log from S3 and performs redaction before any LLM call.

| Field | Description |
|---|---|
| `event_id` | Deterministic SHA1 UUID keyed on `release_id\|node_id`. Stable on redelivery. |
| `source` | Origin pipeline. One of `validation`, `compile`, or `seed_build`. |
| `release_id` | The rejected release identifier. |
| `node_id` | The unique_id of the failing dbt node. |
| `category` | `logic`, `test`, or `unknown`. |
| `error_signature` | Release-stable normalized dedup key (SHA-256 hex). |
| `dbt_log_uri` | S3 URI of the full dbt execution log. |
| `candidate_sql_uri` | S3 URI of the candidate SQL file (candidate-schema form; omitted for seeds and compile failures). |
| `file_path` | Project-relative source file path; non-empty for compile failures only (the log names the offending file, e.g. `models/order_items.sql`). Empty for seed_build and validation failures — the agent derives the source file from the orchestrator ancestry node lookup using `node_id`. |
| `repo` | GitHub owner/name from the originating release. |
| `commit_sha` | Full commit SHA from the originating release. |
| `classified_at` | RFC 3339 timestamp of classification. |

## Consumer Reliability

- Inbound idempotency is enforced by the natural key `(source, release_id, node_id)` in `classification_decision`. A redelivered `release.rejected:v1` message produces the same outcome: the Upsert detects the existing row (`inserted=false`) and skips re-enqueue. The `message_processing` table is present as the FK target of `remediation_outbox.message_processing_id` and is not used for inbound consumer dedup.
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

## Key Code Paths

| Concern | Path |
|---|---|
| Domain model (evidence, categories, decisions, source constants) | `remediation/domain/failure/evidence.go` |
| Deterministic classifier (incl. stage dispatch + ExtractDbtFilePath) | `remediation/domain/failure/classify.go` |
| Signature normalization | `remediation/domain/failure/signature.go` |
| Trigger payload (incl. FilePath field) | `remediation/domain/event/remediation_requested.go` |
| Application handler (stage discrimination, FilePath derivation) | `remediation/service/handlers/classify_failure.go` |
| Inbound adapter (stage→source mapping, reason fallback) | `remediation/adapters/redis/release_rejected_binding.go` |
| Decision repository port | `remediation/domain/repository/decision_repository.go` |
| Postgres adapter | `remediation/adapters/postgres/decision_repository.go` |
| S3 log reader | `remediation/adapters/s3/log_reader.go` |
| Redis ingress + outbox publisher | `remediation/adapters/redis/` |
| DB migrations | `db/migration/remediation/V1__init_remediation.sql` |
