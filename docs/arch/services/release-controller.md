# release-controller

## Purpose

`release-controller` owns the dbt blue/green candidate-release lifecycle: it gates every candidate behind a validation of the changed nodes, their downstream descendants, and their full transitive upstream closure across service boundaries — making every validation self-contained — and only swaps production (topology, schedules, image tags) when validation passes. It holds the `current_prod` pointer — the single source of truth for what is live — and orchestrates the release state machine across manifest-controller, executor-controller, and orchestrator via Redis streams.

**Runtime**: Go service. Exposes an HTTP API and consumes/produces Redis streams; persists to its own Postgres database via a transactional outbox.

## Owned Storage

Postgres (its own database). Tables:

| Table | Purpose |
|---|---|
| `releases` | One row per candidate release: status (`received`, `compiling`, `parsing`, `seed_building`, `validating`, `promoted`, `rejected`, `superseded`), the release's `kind` (`dbt` or `python`, set from the POST body and immutable thereafter — it decides whether the release runs the compile leg at all, see Processing Logic), the `changed_service` this delta belongs to, the per-service image tags assembled at activation, candidate topology, the `code_bundle_uri` (S3 URI of the release's code-bundle contract document, set from the `manifest.loaded.candidate:v1` parse result), validation node ids, `reject_reason` (a machine-readable token) alongside `reject_detail` (the operator-facing explanation of that rejection — the same string carried as `error_detail` on `release.rejected:v1`; empty when the reject path supplied none, and always empty for rows written before this column existed), per-node results (tagged by `stage` — `compile`, `seed_build`, or `validation` — so results from all three legs are accumulated and independently addressable on a single release), transition history, and the immutable provenance columns `repo` (GitHub owner/name) and `commit_sha` (full SHA) captured at receipt. |
| `current_prod` | Singleton row: the promoted `release_id` and its `topology_snapshot` (the live topology). |
| `service_prod` | One row per service (dbt or python) — the live per-service production pointer: `{service_name, release_id, manifest_s3_key, manifest_kind, image_tag, updated_at}`. `manifest_kind` (`dbt` or `python`) records how that service's canonical artifact is authored and parsed, so `AssembleManifestSet` can carry the right `kind` forward onto the next release's `manifest_keys` entry for that service without re-deriving it. Records which manifest key and image tag are currently live for each service; the full production manifest set is reconstructed by collecting every service's pointer at activation time. |
| `release_controller_outbox` | Transactional outbox; one row per produced event, drained by the outbox publisher. |
| `message_processing` | Inbound dedup ledger (`outbox_entry_id` / message id) for idempotent consumption. |

The `topology_snapshot` is the live topology as a list of nodes (`unique_id`, `schema_name`, `table_name`, `service_name`, `node_type`, `content_hash`, `image_tag`, `upstream_unique_ids`, `schedule`); the per-node `content_hash` comparison against it determines which nodes a new candidate must validate. When a new candidate is promoted, its candidate topology (carrying `content_hash` + joined `image_tag`) replaces the snapshot, forming the change-detection base for the next release. The `candidate_artifact_uri` field is stored in `releases.candidate_topology` (as a JSONB field) during validation but is stripped on promotion — it is transient validation data and is not carried into `current_prod`. `code_bundle_uri` is a release-level column, not a per-node topology field; it is set once from the parse result and is carried forward unchanged onto `release.promoted:v1` rather than stripped.

## Inbound Interfaces

### HTTP

| Route | Purpose |
|---|---|
| `POST /releases` | Accept a candidate release for a single service. Body: `{service, release_id, image_tag, repo, commit_sha, bootstrap?, kind?}`. `repo` (GitHub owner/name) and `commit_sha` (full SHA) are required; missing either returns 400. `kind` is optional (`"dbt"` or `"python"`); absent or empty defaults to `"dbt"`, and any other value returns 400. Idempotent on `release_id`. `bootstrap:true` promotes without validation (see Processing Logic). |
| `GET /releases/{id}` | Full release detail: `{release_id, status, changed_service, transitions, validation_node_ids, reject_reason, reject_detail, failing_nodes, per_node_results, image_tags, bootstrap, repo, commit_sha}`. `reject_detail` is the operator-facing explanation of the rejection (empty string when the release is not rejected, or when the reject path supplied none). `per_node_results` is an array of `{stage, node_id, status, dbt_log_uri, run_results_uri, duration_ms, file_path?}` accumulated across all pipeline legs; the `stage` field (`compile`, `seed_build`, or `validation`) identifies which leg produced each entry. For the compile leg the single entry's `node_id` is the service name; `file_path` is non-empty when the failure maps to a specific source file. `duration_ms` is populated for the `compile` and `seed_build` legs; the `validation` stage's per-node results come from the incremental `kind=node` projections on `validation.result:v1`, which do not carry a duration, so `duration_ms` is absent (zero) for those entries. |
| `GET /releases` | Paginated release history, newest-first. Query params: `status` (optional exact-match filter), `limit` (default 20; values that are unparseable, non-positive, or exceed 100 fall back to the default of 20), `cursor` (opaque keyset cursor). Response: `{"releases":[{release_id, status, created_at, resolved_at, node_count, bootstrap, reject_reason}], "next_cursor":"<opaque or empty>"}`. |
| `GET /current-prod` | The current promoted release + topology snapshot. |
| `GET /healthz` | Readiness, backed by the `pkg/liveness` registry. Fails (503) when any registered worker (the stream consumers, the outbox publisher) has exited with an error, any consumer read-loop heartbeat has gone stale, **or** any dependency probe (Redis, Postgres) fails; a dependency outage pulls the pod out of the Service endpoints without restarting it, since a restart does not fix a downstream outage. |
| `GET /livez` | Liveness, backed by the same registry but **workers + heartbeats only** (no dependency probes), so a Redis/Postgres outage does not restart a pod whose consumers are already retrying, while a dead or wedged consumer does. |

The HTTP port is 8088. The Kubernetes `readinessProbe` points at `/healthz` and
the `livenessProbe` at `/livez` (`deploy/continuo/values.yaml`:
`probePath: /healthz`, `livenessPath: /livez`).

`POST /releases` is the production entry point for a deploy: each team's CI uploads only its own service's compiled artifact to the canonical S3 key — `s3://<bucket>/<service>/<release_id>/manifest.json` for a `dbt` service (the compile leg produces it) or `.../contract.yaml` for a `python` service (the domain repo's own CI compiles and uploads it directly, before this POST, since there is no compile leg for python) — and posts a single-service candidate here. `kind` on the request selects which; the canonical key itself is derived from it via `CanonicalManifestKey`, never supplied by the caller. The request carries no manifest-key list — release-controller assembles the full per-service set at activation time from `service_prod` (see Processing Logic). It also carries no changed-node list — release-controller derives it. The CI caller supplies `repo` (`github.repository`, e.g. `org/continuo-dbt-demo`) and `commit_sha` (`github.sha`) as required provenance fields; release-controller stores them verbatim and exposes them on `GET /releases/{id}`.

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `compile.completed:v1` | `release-controller-compile-completed` | Compile job result from executor-controller. |
| `manifest.loaded.candidate:v1` | `release-controller-manifest-loaded-candidate` | Resolved candidate topology (or a parse failure) from manifest-controller. |
| `seed.build.completed:v1` | `release-controller-seed-build-completed` | Aggregate seed-build results from executor-controller. |
| `validation.result:v1` | `release-controller-validation-result` | Unified validation-leg stream from executor-controller. Carries per-node outcomes (`kind=node`, one per node as it settles) and the terminal decision (`kind=complete`, emitted last), all under one `aggregate_id` so a single in-order consumer sees every node before the decision. |

## Outbound Interfaces

### Redis producer

| Stream | Consumed by | Emitted when |
|---|---|---|
| `compile.requested:v1` | executor-controller | A new release activates; the changed service's dbt compile is needed to produce a fresh manifest. Payload: `{release_id, service, image_tag, bucket, candidate_schema}` — `candidate_schema` (`"_candidate_" + SanitizeSchemaSuffix(release_id)`) is always set, driving the compile Job's parse-export/rehearsal gate (see the executor-controller doc's `CreateCompileJob`). |
| `release.requested:v1` | manifest-controller | A release becomes ready to parse: a `dbt` release transitions from Compiling to Parsing (on `compile.completed:v1`); a `python` release, which has no compile leg, transitions directly from Received to Parsing at activation. Either way it carries the assembled `manifest_keys` set (the changed service plus every other service's current prod manifest, each entry's `kind` explicit) to parse. |
| `seed.build.requested:v1` | executor-controller | A parsed release has new/changed dbt-seed nodes that need building into the candidate schema before validation. |
| `validation.requested:v1` | executor-controller | A candidate has changed nodes to validate. |
| `release.promoted:v1` | orchestrator | A release is promoted to production. Payload: `{release_id, topology, image_tags, repo, commit_sha, promoted_at, candidate_schema, code_bundle_uri, bootstrap}`. Each entry in `topology` carries the standard node fields plus a `changed` boolean that is `true` when the node's `content_hash` differs from the prior `current_prod` (or when `current_prod` was empty). The top-level `repo`, `commit_sha`, and `promoted_at` are the source-change provenance for this release; orchestrator stamps them onto each changed `:Table` node. `code_bundle_uri` is the release's code-bundle S3 URI carried through unchanged from the parse result; `bootstrap` reflects whether this release skipped validation. Neither field is consumed by orchestrator today. |
| `release.rejected:v1` | `remediation` (group `remediation-release-rejected`) | A release fails at any pipeline stage. The payload is uniform across all three legs: `{release_id, stage, reason, repo, commit_sha, failing_nodes, per_node[{node_id, status, dbt_log_uri, run_results_uri}]}`. `stage` is `compile`, `seed_build`, or `validation`; `reason` is `compile_failed`, `seed_build_failed`, or `validation_failed`. For the compile leg, the single `per_node` entry's `node_id` is the service name (a synthetic compile unit, not a dbt node); for validation, entries additionally carry `candidate_artifact_uri`. The remediation classifier discriminates by `stage`, fetches the dbt log from S3 for each failing entry, and emits a `remediation.requested:v1` trigger for each healable failure. |

All events are written to the outbox inside the same transaction as the state change and published with an injected `outbox_entry_id` for consumer-side dedup.

Calls no gRPC services.

## Processing Logic

Releases run a FIFO queue: one release is active (`compiling`, `parsing`, `seed_building`, or `validating`) at a time; on each terminal outcome the queue advances the next queued release. The full state machine is: `received` → `compiling` → `parsing` → `seed_building` → `validating` → (`promoted` | `rejected` | `superseded`).

### On `POST /releases`
Create a `Received` release for the single submitted service (idempotent: an existing `release_id` is a no-op). The release records its `changed_service`, that service's `image_tag`, its `kind` (`dbt` or `python`, default `dbt`), and the immutable provenance (`repo`, `commit_sha`) supplied by the caller. The queue advance then promotes the next `Received` release to active, and its `kind` decides how:
- `dbt`: transition to `Compiling`, emit `compile.requested:v1` — CI's manifest still needs a fresh dbt compile before it can be parsed.
- `python`: no compile leg exists — CI already compiled and uploaded the `contract.yaml` artifact before this POST — so the release transitions straight to `Parsing` and emits `release.requested:v1` directly, with the manifest set assembled at this same activation (see Manifest-set assembly below).

### Manifest-set assembly (at activation, not receipt)
Assembly happens at activation — when the queue advance promotes a release out of `Received` — not at receipt. Under the FIFO queue an earlier release may promote and change another service's pointer before this one activates, so the set must be read from live state at the moment of activation. The queue advance runs this full assembly (every other service's `service_prod` row, combined with the changed service's new canonical key and submitted image tag) for every release regardless of kind, and always persists the resulting per-service image tags onto the release (`SetAssembledImageTags`) — a `dbt` release needs the changed service's own tag for its `compile.requested:v1` payload, but the full map is stored either way. What differs by kind is the `manifest_keys` result of that computation: a `python` release, which has no compile leg, emits it immediately in `release.requested:v1`; a `dbt` release's activation-time `manifest_keys` are not emitted at all — a fresh `service_prod` read (same assembly logic) is performed a second time when the release advances from `Compiling` to `Parsing` on `compile.completed:v1` (see below), because an earlier-queued release may have promoted and changed another service's pointer while this one was compiling, and it is *that* second computation's `manifest_keys` that go out on `release.requested:v1`. Either way, the canonical key is `CanonicalManifestKey`: `.../manifest.json` for a `dbt` service, `.../contract.yaml` for `python`; the changed service's prior `service_prod` pointer, if any, is replaced rather than duplicated. The resulting `manifest_keys` list — one `{service, s3_uri, kind}` entry per service, `kind` explicit on every entry — is emitted in the `release.requested:v1` payload `{release_id, manifest_keys}`.

### Activation guard
Before a release activates, the queue advance verifies that every service live in the `current_prod` topology snapshot is covered by a `service_prod` pointer (or is this release's changed service). If a live service has no pointer, the release stays `Received`, nothing is emitted, and a warning naming the uncovered services is logged; a later queue advance retries automatically. This protects the transition from a fully-populated `current_prod` with an empty `service_prod` table: without coverage, assembly would omit the unpointered services and promotion would retire them. `service_prod` is populated from the existing `current_prod` snapshot by the `seed-service-prod` command (shipped in the service image), which records each service's existing manifest key verbatim and is idempotent. `manifest_kind` is derived per service from its own snapshot nodes, not hardcoded: any node with `node_type` `python-model` makes the service `python`, otherwise `dbt`. A service whose nodes mix python and non-python types fails the whole seed before writing anything — a service is always a single kind.

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
       per_node[{node_id, status, dbt_log_uri, run_results_uri}], repo, commit_sha}
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
         candidate_artifact_uri = the S3 URI for the node's candidate artifact
           (rewritten SQL for a dbt node, a validation spec for a python node),
         and validation_op = validationOpFor(node, changedClosureSet))
  advance queue
```
`validationOpFor` picks the executor's per-node build strategy from the node's kind and whether it is in the changed closure: a changed-closure dbt node gets `build_from_sql` (candidate SQL already rewritten to the candidate schema); a changed-closure python node gets `build_from_columns` (a JSON spec of declared reads + output columns, since there is no SQL to build from); every other in-set node — an unchanged upstream, of either kind — gets `clone_from_prod` regardless, since it carries no candidate artifact to build from.

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
   update current_prod to this release's candidate topology,
   upsert the changed service's service_prod pointer (canonical key + image tag + release id),
   transition to Promoted, emit release.promoted:v1
any stored node not ok (failed or skipped) / aggregate_status not ok → Reject(reason=validation_failed),
   emit release.rejected:v1 {release_id, stage="validation", reason, failing_nodes,
        per_node[{node_id, status, dbt_log_uri, run_results_uri, candidate_artifact_uri}], repo, commit_sha}
        (per_node sourced from the stored read model, enriched with each node's candidate_artifact_uri)
advance queue
```

Promotion is shared by the validation-passed path, the bootstrap short-circuit, and the nothing-to-validate short-circuit: all point `current_prod` at the candidate topology, upsert the changed service's `service_prod` pointer (its canonical manifest key, image tag, and this `release_id`), transition the release to `Promoted`, and emit `release.promoted:v1`. The candidate topology (carrying `content_hash` + joined `image_tag`) becomes the new snapshot, so the next release's change-detection diff is correct, and the refreshed pointer is what the next release for any other service assembles against.

Before updating `current_prod`, `promoteToProduction` computes the set of changed node IDs by calling `DerivedChangedNodeIDs` against the pre-update `current_prod` snapshot (bootstrap emits all nodes as changed). Each node in the `release.promoted:v1` topology carries a `changed` boolean reflecting membership in that set. The event also carries top-level `repo`, `commit_sha`, and `promoted_at` fields taken from the release's provenance; orchestrator uses these to stamp `:Table.last_commit_sha`, `:Table.last_repo`, `:Table.last_changed_at`, and `:Table.last_release_id` on changed nodes only.

## Consumer Reliability

- Four consumer groups (`compile.completed:v1`, `manifest.loaded.candidate:v1`, `seed.build.completed:v1`, `validation.result:v1`) run in the same process; each maintains its own offset.
- Inbound messages are deduped via `message_processing` (idempotent on the upstream `outbox_entry_id`), so a redelivery is absorbed.
- A permanent parse-decode failure (or an unrecognised `validation.result:v1` kind) is ACKed (logged, not retried); transient errors are not ACKed and replay. The `kind=complete` decision reads the stored `per_node_results` projections together with the authoritative `aggregate_status` column, so it never needs to defer: a `kind=node` message that hasn't been consumed yet or a permanently-dropped projection write both leave a node absent from `per_node_results`, and for that node the decision falls back to `aggregate_status` alone rather than blocking the release or the queue; only a node that is present and non-ok in the store counts toward the reject.
- A message on any of the inbound streams whose `release_id` no longer has a `releases` row (pruned, or reclaimed from a previous consumer for a deleted release) is logged and dropped rather than processed. The repository's `Get` returns no row as `(nil, nil)`, so all handlers nil-check the aggregate before use; without that guard a reclaimed message for a missing release would crash the consumer on startup.
- State changes and the outbox row are written in one transaction; the outbox publisher drains rows and XADDs them, injecting `outbox_entry_id` for downstream dedup.

## Background Loops

| Loop | Description |
|---|---|
| Outbox publisher | Drains `release_controller_outbox` and XADDs each row to its stream. |
| `compile.completed:v1` consumer | Dispatches to the compile-result handler. |
| `manifest.loaded.candidate:v1` consumer | Dispatches to the parsed-manifest handler. |
| `seed.build.completed:v1` consumer | Dispatches to the seed-build-result handler. |
| `validation.result:v1` consumer | Routes by kind: `node` → per-node projection handler, `complete` → terminal validation-result handler, then advances the queue. |
| Retention | Runs on the janitor interval (`RELEASE_JANITOR_INTERVAL`, default 24h). Deletes terminal releases (promoted, rejected, superseded) whose creation timestamp is older than `RELEASE_RETENTION_DAYS` (default 90 days). Never deletes the release referenced by `current_prod` or by any `service_prod` pointer. For each pruned release, also deletes the `candidate-sql/<release_id>/` and `code-bundles/<release_id>/` S3 prefixes (soft-fail — a delete error does not abort the prune). |

## S3 Behavior

| Operation | Description |
|---|---|
| `DeleteObjects` | Delete the `candidate-sql/<release_id>/` prefix for each pruned release during retention; soft-fail (a delete error is logged but does not abort the prune). |
| `DeleteObjects` | Delete the `code-bundles/<release_id>/` prefix for each pruned release during retention; soft-fail (a delete error is logged but does not abort the prune). |

The S3 bucket is not managed by release-controller's Helm chart. A native S3 lifecycle expiry rule (30 days) on the `candidate-sql/` prefix is configured via a one-time bootstrap for production and the LocalStack init script for development; the prune-time delete is the primary reclaim path there, with the lifecycle rule as backstop. The `code-bundles/` prefix carries the same 30-day lifecycle rule, configured the same way, with the prune-time delete as its primary reclaim path and the lifecycle rule as backstop.

## gRPC Callers

None — release-controller is not called via gRPC by any service.

## Reliability Notes

- Idempotent on `release_id`: a redelivered `POST /releases` or re-promotion is a no-op; `release.promoted:v1` carries a deterministic aggregate id so orchestrator dedups re-emissions.
- Change detection relies on each candidate node carrying a non-empty `content_hash` (manifest-controller emits a fold of its own source checksum, its transitive macro checksums, and its resolved-config fingerprint (`source_hash`/`shared_code_hash`/`config_hash`), with deterministic fallbacks so it is never empty). An empty-vs-empty hash would skip validation; this is structurally avoided upstream. Because macros and resolved config are folded in, a shared-macro edit or a config-only change (e.g. a materialization bump in `dbt_project.yml`) changes the hash of every affected node and re-runs them through the validation gate even when that node's own `.sql` is untouched.
- The first release into an empty `current_prod` is submitted with `bootstrap:true`: it promotes without validation and seeds `current_prod` from the candidate topology, giving subsequent releases a change-detection base. A bootstrap promotes whatever topology it carries, so it must be a trusted one.
