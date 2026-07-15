# release-controller

## Purpose

`release-controller` owns the dbt blue/green candidate-release lifecycle: it gates every candidate behind a validation of the changed nodes, their downstream descendants, and their full transitive upstream closure across service boundaries — making every validation self-contained — and only swaps production (topology, schedules, image tags) when validation passes. It holds the `current_prod` pointer — the single source of truth for what is live — and orchestrates the release state machine across manifest-controller, executor-controller, and orchestrator via Redis streams.

**Runtime**: Go service. Exposes an HTTP API and consumes/produces Redis streams; persists to its own Postgres database via a transactional outbox.

## Owned Storage

Postgres (its own database). Tables:

| Table | Purpose |
|---|---|
| `releases` | One row per candidate release: status (`received`, `compiling`, `parsing`, `seed_building`, `validating`, `promoted`, `rejected`, `superseded`), the `changed_service` this delta belongs to, the per-service image tags assembled at activation, candidate topology, validation node ids, per-node results (tagged by `stage` — `compile`, `seed_build`, or `validation` — so results from all three legs are accumulated and independently addressable on a single release), transition history, and the immutable provenance columns `repo` (GitHub owner/name) and `commit_sha` (full SHA) captured at receipt. |
| `current_prod` | Singleton row: the promoted `release_id` and its `topology_snapshot` (the live topology). |
| `service_prod` | One row per dbt service — the live per-service production pointer: `{service_name, release_id, manifest_s3_key, image_tag, updated_at}`. Records which manifest key and image tag are currently live for each service; the full production manifest set is reconstructed by collecting every service's pointer at activation time. |
| `release_controller_outbox` | Transactional outbox; one row per produced event, drained by the outbox publisher. |
| `message_processing` | Inbound dedup ledger (`outbox_entry_id` / message id) for idempotent consumption. |

The `topology_snapshot` is the live topology as a list of nodes (`unique_id`, `schema_name`, `table_name`, `service_name`, `node_type`, `content_hash`, `image_tag`, `upstream_unique_ids`, `schedule`); the per-node `content_hash` comparison against it determines which nodes a new candidate must validate. When a new candidate is promoted, its candidate topology (carrying `content_hash` + joined `image_tag`) replaces the snapshot, forming the change-detection base for the next release. The `candidate_sql_uri` field is stored in `releases.candidate_topology` (as a JSONB field) during validation but is stripped on promotion — it is transient validation data and is not carried into `current_prod`.

## Inbound Interfaces

### HTTP

| Route | Purpose |
|---|---|
| `POST /releases` | Accept a candidate release for a single dbt service. Body: `{service, release_id, image_tag, repo, commit_sha, bootstrap?}`. `repo` (GitHub owner/name) and `commit_sha` (full SHA) are required; missing either returns 400. Idempotent on `release_id`. `bootstrap:true` promotes without validation (see Processing Logic). |
| `GET /releases/{id}` | Full release detail: `{release_id, status, changed_service, transitions, validation_node_ids, reject_reason, failing_nodes, per_node_results, image_tags, bootstrap, repo, commit_sha}`. `per_node_results` is an array of `{stage, node_id, status, dbt_log_uri, run_results_uri, duration_ms, file_path?}` accumulated across all pipeline legs; the `stage` field (`compile`, `seed_build`, or `validation`) identifies which leg produced each entry. For the compile leg the single entry's `node_id` is the service name; `file_path` is non-empty when the failure maps to a specific source file. `duration_ms` is populated for the `compile` and `seed_build` legs; the `validation` stage's per-node results come from the incremental `validation.node.result:v1` projection, which does not carry a duration, so `duration_ms` is absent (zero) for those entries. |
| `GET /releases` | Paginated release history, newest-first. Query params: `status` (optional exact-match filter), `limit` (default 20; values that are unparseable, non-positive, or exceed 100 fall back to the default of 20), `cursor` (opaque keyset cursor). Response: `{"releases":[{release_id, status, created_at, resolved_at, node_count, bootstrap, reject_reason}], "next_cursor":"<opaque or empty>"}`. |
| `GET /current-prod` | The current promoted release + topology snapshot. |
| `GET /healthz` | Liveness. |

`POST /releases` is the production entry point for a deploy: each team's CI uploads only its own service's compiled manifest to the canonical S3 key `s3://<bucket>/<service>/<release_id>/manifest.json` and posts a single-service candidate here. The request carries no manifest-key list — release-controller assembles the full per-service set at activation time from `service_prod` (see Processing Logic). It also carries no changed-node list — release-controller derives it. The CI caller supplies `repo` (`github.repository`, e.g. `org/continuo-dbt-demo`) and `commit_sha` (`github.sha`) as required provenance fields; release-controller stores them verbatim and exposes them on `GET /releases/{id}`.

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `compile.completed:v1` | `release-controller-compile-completed` | Compile job result from executor-controller. |
| `manifest.loaded.candidate:v1` | `release-controller-manifest-loaded-candidate` | Resolved candidate topology (or a parse failure) from manifest-controller. |
| `seed.build.completed:v1` | `release-controller-seed-build-completed` | Aggregate seed-build results from executor-controller. |
| `validation.node.result:v1` | `release-controller-validation-node-result` | Per-node validation outcome from executor-controller, projected incrementally into the release's `per_node_results` read model as each node settles (loads the release `FOR UPDATE`, upserts the one node). |
| `validation.completed:v1` | `release-controller-validation-completed` | Terminal validation decision from executor-controller; the per-node content was projected earlier via `validation.node.result:v1`, and a completeness barrier defers the decision until every expected node is stored. |

## Outbound Interfaces

### Redis producer

| Stream | Consumed by | Emitted when |
|---|---|---|
| `compile.requested:v1` | executor-controller | A new release activates; the changed service's dbt compile is needed to produce a fresh manifest. |
| `release.requested:v1` | manifest-controller | A release transitions from Compiling to Parsing; carries the re-assembled `manifest_keys` set (the changed service plus every other service's current prod manifest) to parse. |
| `seed.build.requested:v1` | executor-controller | A parsed release has new/changed dbt-seed nodes that need building into the candidate schema before validation. |
| `validation.requested:v1` | executor-controller | A candidate has changed nodes to validate. |
| `release.promoted:v1` | orchestrator | A release is promoted to production. Payload: `{release_id, topology, image_tags, repo, commit_sha, promoted_at}`. Each entry in `topology` carries the standard node fields plus a `changed` boolean that is `true` when the node's `content_hash` differs from the prior `current_prod` (or when `current_prod` was empty). The top-level `repo`, `commit_sha`, and `promoted_at` are the source-change provenance for this release; orchestrator stamps them onto each changed `:Table` node. |
| `release.rejected:v1` | `remediation` (group `remediation-release-rejected`) | A release fails at any pipeline stage. The payload is uniform across all three legs: `{release_id, stage, reason, repo, commit_sha, failing_nodes, per_node[{node_id, status, dbt_log_uri, run_results_uri}]}`. `stage` is `compile`, `seed_build`, or `validation`; `reason` is `compile_failed`, `seed_build_failed`, or `validation_failed`. For the compile leg, the single `per_node` entry's `node_id` is the service name (a synthetic compile unit, not a dbt node); for validation, entries additionally carry `candidate_sql_uri`. The remediation classifier discriminates by `stage`, fetches the dbt log from S3 for each failing entry, and emits a `remediation.requested:v1` trigger for each healable failure. |

All events are written to the outbox inside the same transaction as the state change and published with an injected `outbox_entry_id` for consumer-side dedup.

Calls no gRPC services.

## Processing Logic

Releases run a FIFO queue: one release is active (`compiling`, `parsing`, `seed_building`, or `validating`) at a time; on each terminal outcome the queue advances the next queued release. The full state machine is: `received` → `compiling` → `parsing` → `seed_building` → `validating` → (`promoted` | `rejected` | `superseded`).

### On `POST /releases`
Create a `Received` release for the single submitted service (idempotent: an existing `release_id` is a no-op). The release records its `changed_service`, that service's `image_tag`, and the immutable provenance (`repo`, `commit_sha`) supplied by the caller. The queue advance promotes the next `Received` release to `Compiling` and emits `compile.requested:v1`.

### Manifest-set assembly (at activation, not receipt)
Assembly happens when a release transitions from `Compiling` to `Parsing`, not when it is received. Under the FIFO queue an earlier release may promote and change another service's pointer before this one activates, so the set must be read from live state at the moment of activation. The queue advance reads every other service's `service_prod` row and combines them with the changed service's new canonical key (`s3://<bucket>/<changed_service>/<release_id>/manifest.json`) and its submitted image tag. The changed service's prior pointer, if any, is replaced rather than duplicated. The assembled per-service image tags are persisted on the release; the resulting `manifest_keys` list — one `{service, s3_uri}` entry per service — is emitted in the `release.requested:v1` payload `{release_id, manifest_keys}`.

### Activation guard
Before a release activates, the queue advance verifies that every service live in the `current_prod` topology snapshot is covered by a `service_prod` pointer (or is this release's changed service). If a live service has no pointer, the release stays `Received`, nothing is emitted, and a warning naming the uncovered services is logged; a later queue advance retries automatically. This protects the transition from a fully-populated `current_prod` with an empty `service_prod` table: without coverage, assembly would omit the unpointered services and promotion would retire them. `service_prod` is populated from the existing `current_prod` snapshot by the `seed-service-prod` command (shipped in the service image), which records each service's existing manifest key verbatim and is idempotent.

### On `compile.completed:v1`
```
status=failed:
  decode per_node (one entry per compile unit; node_id = service name)
  RecordStageResults("compile", per_node results)
  Reject(reason=compile_failed, failing_nodes=[node_id of failed entries])
  emit release.rejected:v1 {release_id, stage="compile", reason, failing_nodes,
       per_node[{node_id, status, dbt_log_uri, run_results_uri}], repo, commit_sha}
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
  if release.bootstrap: promote directly, skipping validation
      (record candidate topology, update current_prod, upsert the changed
       service's service_prod pointer, transition to Promoted,
       emit release.promoted:v1); advance queue, return
  load current_prod.topology_snapshot
  derive changed = candidate nodes whose content_hash differs from prod, or are new
  inSet = DescendantsClosure(candidate, changed) ∪ FullAncestorsClosure(candidate, that)
          # changed + their downstream + full transitive upstream closure across service boundaries
  for each inSet node: if any of its direct upstreams is absent from the candidate topology:
      Reject(reason=unbuildable_cross_service_upstream), emit release.rejected:v1, advance queue, return
  for each inSet node: upstream_node_ids = inSet ∩ direct upstreams of node (intra- and cross-service)
  if inSet is empty:
      promote directly (nothing to validate trivially passes the gate):
        update current_prod, transition to Promoted, emit release.promoted:v1
  else if inSet has changed seeds:
      emit seed.build.requested:v1, transition to SeedBuilding
  else:
      transition to Validating, emit validation.requested:v1
        (mode=validation, candidate_schema=_candidate_<release_id>,
         nodes carry upstream_node_ids = all in-set upstreams, intra- and cross-service,
         and candidate_sql_uri = the S3 URI for the node's rewritten SQL)
  advance queue
```
A `bootstrap:true` release skips validation entirely: it records the candidate topology, seeds `current_prod`, and promotes directly. This is the initial cutover (or a trusted re-baseline) against an empty or mismatched `current_prod`. A non-bootstrap release against an empty snapshot instead treats every candidate node as changed and validates the whole topology.

### On `seed.build.completed:v1`
```
status=failed:
  decode per_node (one entry per failed seed node)
  RecordStageResults("seed_build", per_node results)
  Reject(reason=seed_build_failed, failing_nodes=[node_id of failed entries])
  emit release.rejected:v1 {release_id, stage="seed_build", reason, failing_nodes,
       per_node[{node_id, status, dbt_log_uri, run_results_uri}], repo, commit_sha, candidate_schema}
  advance queue
status=ok:
  RecordStageResults("seed_build", per_node results)
  compute the filtered validation set: all recorded validation ids minus the just-built seeds
     (seeds already live in the candidate schema and are not validated)
  TransitionFromSeedBuilding (SeedBuilding → Validating), narrowing the persisted
     validation_node_ids to that filtered set so the completeness barrier expects exactly the
     nodes the executor emits per-node results for
  emit validation.requested:v1 for the same filtered set (single source: persisted set and emit derive from one computation)
  advance queue
  edge case: if excluding the built seeds leaves an empty validation set, promote directly
```

### On `validation.completed:v1`

The terminal event carries only the decision (`release_id`, `aggregate_status`, `candidate_schema`); the per-node content was already projected into the release's `per_node_results` read model by the incremental `validation.node.result:v1` stream. The handler loads the release `FOR UPDATE` (serializing against in-flight per-node upserts) and reads the stored validation-stage results rather than any per-node array on the event.
```
load release FOR UPDATE, read stored per_node_results where stage="validation"
completeness barrier: if any id in validation_node_ids has no stored validation result →
   return error so the message redelivers (the terminal event overtook a per-node event on
   its separate stream); resolves once every expected node's validation.node.result:v1
   projection has been delivered and applied — each projection is written transactionally
   with the node's settle, so under normal delivery this clears within redelivery latency.
   Nodes skipped by a failed upstream are included: executor-controller emits a
   status="skipped" projection for them even though they never ran.
bounded escape: the barrier still depends on outbox delivery of those projection rows, so a
   permanently-lost row (e.g. its retries were exhausted during a sustained Redis outage)
   would otherwise hang the release in validating forever, blocking the single-ActiveRelease
   queue. The age of the terminal event is read from its Redis message ID (the millisecond
   prefix, stable across redeliveries, so no extra state). Once that age exceeds a bounded
   grace (5m ≈ 2–3 PEL redeliveries, far beyond normal sub-second catch-up) the barrier stops
   waiting, logs the un-projected nodes, and decides from the authoritative aggregate_status.
   Missing nodes are never fabricated as failing: only stored, non-ok nodes count toward
   failing, so an escape with aggregate_status ok still promotes.
all stored (and present) nodes ok and aggregate_status ok → handleValidationOK:
   update current_prod to this release's candidate topology,
   upsert the changed service's service_prod pointer (canonical key + image tag + release id),
   transition to Promoted, emit release.promoted:v1
any stored node not ok (failed or skipped) / aggregate_status not ok → Reject(reason=validation_failed),
   emit release.rejected:v1 {release_id, stage="validation", reason, failing_nodes,
        per_node[{node_id, status, dbt_log_uri, run_results_uri, candidate_sql_uri}], repo, commit_sha}
        (per_node sourced from the stored read model, enriched with each node's candidate_sql_uri)
advance queue
```

Promotion is shared by the validation-passed path, the bootstrap short-circuit, and the nothing-to-validate short-circuit: all point `current_prod` at the candidate topology, upsert the changed service's `service_prod` pointer (its canonical manifest key, image tag, and this `release_id`), transition the release to `Promoted`, and emit `release.promoted:v1`. The candidate topology (carrying `content_hash` + joined `image_tag`) becomes the new snapshot, so the next release's change-detection diff is correct, and the refreshed pointer is what the next release for any other service assembles against.

Before updating `current_prod`, `promoteToProduction` computes the set of changed node IDs by calling `DerivedChangedNodeIDs` against the pre-update `current_prod` snapshot (bootstrap emits all nodes as changed). Each node in the `release.promoted:v1` topology carries a `changed` boolean reflecting membership in that set. The event also carries top-level `repo`, `commit_sha`, and `promoted_at` fields taken from the release's provenance; orchestrator uses these to stamp `:Table.last_commit_sha`, `:Table.last_repo`, `:Table.last_changed_at`, and `:Table.last_release_id` on changed nodes only.

## Consumer Reliability

- Five consumer groups (`compile.completed:v1`, `manifest.loaded.candidate:v1`, `seed.build.completed:v1`, `validation.node.result:v1`, `validation.completed:v1`) run in the same process; each maintains its own offset.
- Inbound messages are deduped via `message_processing` (idempotent on the upstream `outbox_entry_id`), so a redelivery is absorbed.
- A permanent parse-decode failure is ACKed (logged, not retried); transient errors are not ACKed and replay. The `validation.completed:v1` completeness barrier deliberately uses this replay path: it returns a transient error until every expected per-node result is stored, so the terminal decision normally never runs on a partial projection. This defer is bounded — once the terminal event is older than a fixed grace (dated from its Redis message ID), the barrier escapes and decides from the authoritative `aggregate_status`, so a permanently-lost projection row can no longer hang the release or the queue.
- A message on any of the inbound streams whose `release_id` no longer has a `releases` row (pruned, or reclaimed from a previous consumer for a deleted release) is logged and dropped rather than processed. The repository's `Get` returns no row as `(nil, nil)`, so all handlers nil-check the aggregate before use; without that guard a reclaimed message for a missing release would crash the consumer on startup.
- State changes and the outbox row are written in one transaction; the outbox publisher drains rows and XADDs them, injecting `outbox_entry_id` for downstream dedup.

## Background Loops

| Loop | Description |
|---|---|
| Outbox publisher | Drains `release_controller_outbox` and XADDs each row to its stream. |
| `compile.completed:v1` consumer | Dispatches to the compile-result handler. |
| `manifest.loaded.candidate:v1` consumer | Dispatches to the parsed-manifest handler. |
| `seed.build.completed:v1` consumer | Dispatches to the seed-build-result handler. |
| `validation.node.result:v1` consumer | Dispatches to the per-node validation-result handler (incremental projection). |
| `validation.completed:v1` consumer | Dispatches to the validation-result handler. |
| Retention | Runs on the janitor interval (`RELEASE_JANITOR_INTERVAL`, default 24h). Deletes terminal releases (promoted, rejected, superseded) whose creation timestamp is older than `RELEASE_RETENTION_DAYS` (default 90 days). Never deletes the release referenced by `current_prod` or by any `service_prod` pointer. For each pruned release, also deletes the `candidate-sql/<release_id>/` S3 prefix (soft-fail — a delete error does not abort the prune; the S3 lifecycle expiry rule is the backstop). |

## S3 Behavior

| Operation | Description |
|---|---|
| `DeleteObjects` | Delete the `candidate-sql/<release_id>/` prefix for each pruned release during retention; soft-fail (a delete error is logged but does not abort the prune). |

The S3 bucket is not managed by release-controller's Helm chart. A native S3 lifecycle expiry rule (30 days) on the `candidate-sql/` prefix is configured via a one-time bootstrap for production and the LocalStack init script for development. The prune-time delete is the primary reclaim path; the lifecycle rule is the backstop.

## gRPC Callers

None — release-controller is not called via gRPC by any service.

## Reliability Notes

- Idempotent on `release_id`: a redelivered `POST /releases` or re-promotion is a no-op; `release.promoted:v1` carries a deterministic aggregate id so orchestrator dedups re-emissions.
- Change detection relies on each candidate node carrying a non-empty `content_hash` (manifest-controller emits a macro-aware fingerprint: dbt's per-node checksum folded with its transitive macro source checksums, with a deterministic fallback). An empty-vs-empty hash would skip validation; this is structurally avoided upstream. Because macros are folded in, a shared-macro edit changes the hash of every dependent node and re-runs them through the validation gate.
- The first release into an empty `current_prod` is submitted with `bootstrap:true`: it promotes without validation and seeds `current_prod` from the candidate topology, giving subsequent releases a change-detection base. A bootstrap promotes whatever topology it carries, so it must be a trusted one.
