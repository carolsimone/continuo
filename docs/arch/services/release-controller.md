# release-controller

## Purpose

`release-controller` owns the dbt blue/green candidate-release lifecycle: it gates every candidate behind a validation of the changed nodes, their downstream descendants, and their full transitive upstream closure across service boundaries — making every validation self-contained — and only swaps production (topology, schedules, image tags) when validation passes. It holds the `current_prod` pointer — the single source of truth for what is live — and orchestrates the release state machine across topology-controller, executor-controller, and orchestrator via Redis streams.

**Runtime**: Go service. Exposes an HTTP API and consumes/produces Redis streams; persists to its own Postgres database via a transactional outbox.

## Owned Storage

Postgres (its own database). Tables:

| Table | Purpose |
|---|---|
| `releases` | One row per candidate release: status (`received`, `compiling`, `parsing`, `seed_building`, `validating`, `promoted`, `rejected`, `superseded`, `validated`), the release's `kind` (`dbt` or `python`, set from the POST body and immutable thereafter — it decides whether the release runs the compile leg at all, see Processing Logic), the `changed_service` this delta belongs to, the per-service image tags assembled at activation, candidate topology, the `code_bundle_uri` (S3 URI of the release's code-bundle contract document, set from the `manifest.loaded.candidate:v1` parse result), validation node ids, `reject_reason` (a machine-readable token) alongside `reject_detail` (the operator-facing explanation of that rejection — the same string carried as `error_detail` on `release.rejected:v1`; empty when the reject path supplied none, and always empty for rows written before this column existed), per-node results (tagged by `stage` — `compile`, `seed_build`, or `validation` — so results from all three legs are accumulated and independently addressable on a single release), transition history, the immutable `shadow` boolean (set from the POST body, default `false`, marking a release intended for verification rather than production promotion), the immutable provenance columns `repo` (GitHub owner/name) and `commit_sha` (full SHA) captured at receipt, `remediation_round` (default `1`; how many times a human has asked this rejected release to "try again," capped in code at `MaxRemediationRounds = 3`), `source_overlay_uri` (default `''`; the S3 URI of the source-overlay tarball a shadow release's compile leg lays over the service project, so the release compiles a proposed fix rather than the committed source — always empty for a non-shadow release, which the `POST /releases` validation enforces), and `rejection_payload` (JSONB, nullable) — the exact `release.rejected:v1` payload this release emitted at its (healable) rejection, kept so a later round can replay it verbatim instead of re-deriving it from the aggregate. `rejection_payload` is set at every rejection that carries a `per_node`-shaped payload — every reason from the compile leg (`compile_failed`, `parse_rehearsal_failed`, `artifact_upload_failed`), `duplicate_table`, `seed_build_failed`, and `validation_failed` — regardless of whether that particular reason is itself retryable; it is left `NULL` for `parse_failed` and the shadow-only `nothing_to_validate` rejection, neither of which is healable, and for any release rejected before this column existed. |
| `current_prod` | Singleton row: the promoted `release_id` and its `topology_snapshot` (the live topology). |
| `service_prod` | One row per service (dbt or python) — the live per-service production pointer: `{service_name, release_id, manifest_s3_key, manifest_kind, image_tag, updated_at}`. `manifest_kind` (`dbt` or `python`) records how that service's canonical artifact is authored and parsed, so `AssembleManifestSet` can carry the right `kind` forward onto the next release's `manifest_keys` entry for that service without re-deriving it. Records which manifest key and image tag are currently live for each service; the full production manifest set is reconstructed by collecting every service's pointer at activation time. |
| `release_controller_outbox` | Transactional outbox; one row per produced event, drained by the outbox publisher. |
| `message_processing` | Inbound dedup ledger (`outbox_entry_id` / message id) for idempotent consumption. |

The `topology_snapshot` is the live topology as a list of nodes (`unique_id`, `schema_name`, `table_name`, `resolved_relation_id`, `service_name`, `node_type`, `content_hash`, `image_tag`, `upstream_unique_ids`, `schedule`); the per-node `content_hash` comparison against it determines which nodes a new candidate must validate. `resolved_relation_id` is the physical relation the node's build actually writes (its dbt alias, when it has one, else the same name as `unique_id`'s own table segment); it is what `DuplicateClaims` groups on, not `unique_id`, so it survives into the snapshot even though nothing else reads it there. When a new candidate is promoted, its candidate topology (carrying `content_hash` + joined `image_tag`) replaces the snapshot, forming the change-detection base for the next release. The `candidate_artifact_uri` field is stored in `releases.candidate_topology` (as a JSONB field) during validation but is stripped on promotion — it is transient validation data and is not carried into `current_prod`. `code_bundle_uri` is a release-level column, not a per-node topology field; it is set once from the parse result and is carried forward unchanged onto `release.promoted:v1` rather than stripped.

## Inbound Interfaces

### HTTP

| Route | Purpose |
|---|---|
| `POST /releases` | Accept a candidate release for a single service. Body: `{service, release_id, image_tag, repo, commit_sha, bootstrap?, kind?, shadow?, source_overlay_uri?}`. `repo` (GitHub owner/name) and `commit_sha` (full SHA) are required; missing either returns 400. `kind` is optional (`"dbt"` or `"python"`); absent or empty defaults to `"dbt"`, and any other value returns 400. Idempotent on `release_id`. `bootstrap:true` promotes without validation (see Processing Logic). `shadow` is optional (default `false`) and is stored immutably: a shadow release runs the normal pipeline but ends at the terminal `validated` status instead of promoting (see Processing Logic). It is posted by `agent-remediation` to verify a proposed fix; nothing else submits one. `source_overlay_uri` is optional and is the S3 URI of a gzipped tar of project-relative source files the compile leg lays over the service's checked-in project before running, so the release verifies a proposed fix instead of the committed source. It is **accepted only together with `shadow: true`** — a production release always compiles exactly what is committed, and a body carrying it without `shadow` returns 400. |
| `GET /releases/{id}` | Full release detail: `{release_id, status, changed_service, transitions, validation_node_ids, reject_reason, reject_detail, failing_nodes, per_node_results, image_tags, bootstrap, shadow, repo, commit_sha, remediation_round}`. `reject_detail` is the operator-facing explanation of the rejection (empty string when the release is not rejected, or when the reject path supplied none). `per_node_results` is an array of `{stage, node_id, status, dbt_log_uri, run_results_uri, duration_ms, file_path?}` accumulated across all pipeline legs; the `stage` field (`compile`, `seed_build`, or `validation`) identifies which leg produced each entry. For the compile leg the single entry's `node_id` is the service name; `file_path` is non-empty when the failure maps to a specific source file. `duration_ms` is populated for the `compile` and `seed_build` legs; the `validation` stage's per-node results come from the incremental `kind=node` projections on `validation.result:v1`, which do not carry a duration, so `duration_ms` is absent (zero) for those entries. `remediation_round` (default `1`) is how many times a human has asked this release to "try again" after a rejection; the UI's release page shows it in the status pill once it exceeds 1. |
| `POST /releases/{id}/retry-remediation` | Start another remediation round on a rejected release: replays the release's stored `release.rejected:v1` payload, tagged with the incremented `remediation_round`, on `remediation.retry_requested:v1`. `202 {release_id, remediation_round}` on success. `404 {error: "not_found"}` when the release does not exist. `409 {error}` when refused: `not_rejected` (release is not currently `rejected`), `not_healable` (release is a shadow release, or its stored reason is not one of `compile_failed`/`seed_build_failed`/`validation_failed`/`duplicate_table`), `not_retryable` (no stored `rejection_payload` — the release predates the column), `rounds_exhausted` (`remediation_round` is already at the cap of 3), `retry_in_progress` (the current round has not yet produced a proposal row — its trigger has not reached agent-remediation yet, or the classifier dropped it), or `proposal_open` (some node's latest attempt in the current round is still generating/verifying/proposed, or any attempt from any round has a PR that is opening/open/merged — the response also carries `proposal_id`/`pr_url` when known). `502 {error: "proposal_reader_unavailable"}` when the gRPC read to agent-remediation fails outright, as distinct from a `proposal_open` refusal. `500 {error: "internal"}` on any other failure (logged server-side). See Processing Logic → `RetryRemediation` for the full rule order. |
| `GET /releases` | Paginated release history, newest-first. Query params: `status` (optional exact-match filter), `limit` (default 20; values that are unparseable, non-positive, or exceed 100 fall back to the default of 20), `cursor` (opaque keyset cursor). Response: `{"releases":[{release_id, status, created_at, resolved_at, node_count, bootstrap, shadow, reject_reason}], "next_cursor":"<opaque or empty>"}`. `resolved_at` is non-empty once a release reaches any terminal status, including `validated`. |
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
| `compile.requested:v1` | executor-controller | A new release activates; the changed service's dbt compile is needed to produce a fresh manifest. Payload: `{release_id, service, image_tag, bucket, candidate_schema, source_overlay_uri?}` — `candidate_schema` (`"_candidate_" + SanitizeSchemaSuffix(release_id)`) is always set, driving the compile Job's parse-export/rehearsal gate (see the executor-controller doc's `CreateCompileJob`). `source_overlay_uri` is threaded verbatim from the release row and is non-empty only for a dbt shadow release verifying a proposed fix; it makes the compile Job lay that overlay over the team's project before dbt runs. |
| `release.requested:v1` | topology-controller | A release becomes ready to parse: a `dbt` release transitions from Compiling to Parsing (on `compile.completed:v1`); a `python` release, which has no compile leg, transitions directly from Received to Parsing at activation. Either way it carries the assembled `manifest_keys` set (the changed service plus every other service's current prod manifest, each entry's `kind` explicit) to parse. |
| `seed.build.requested:v1` | executor-controller | A parsed release has new/changed dbt-seed nodes that need building into the candidate schema before validation. Payload: `{release_id, mode, candidate_schema, image_tags, seeds, seed_ids_in_order, source_overlay_uri?}` — `source_overlay_uri` is threaded from the release row exactly as on `compile.requested:v1` and is present only for a dbt shadow release verifying a proposed fix, so the seed Job loads the proposed CSV rather than the committed one. |
| `validation.requested:v1` | executor-controller | A candidate has changed nodes to validate. |
| `release.promoted:v1` | orchestrator | A release is promoted to production. Payload: `{release_id, topology, image_tags, repo, commit_sha, promoted_at, candidate_schema, code_bundle_uri, bootstrap}`. Each entry in `topology` carries the standard node fields plus a `changed` boolean that is `true` when the node's `content_hash` differs from the prior `current_prod` (or when `current_prod` was empty). The top-level `repo`, `commit_sha`, and `promoted_at` are the source-change provenance for this release; orchestrator stamps them onto each changed `:Table` node. `code_bundle_uri` is the release's code-bundle S3 URI carried through unchanged from the parse result; `bootstrap` reflects whether this release skipped validation. Orchestrator's version-ingestion consumer group reads the bundle at that URI and uses `bootstrap`, together with each node's `changed` flag, to mark whether a recorded code version's commit stamp is exact or approximate. |
| `release.rejected:v1` | `remediation` (group `remediation-release-rejected`) | A release fails at any pipeline stage. The payload is uniform across all three legs: `{release_id, stage, reason, repo, commit_sha, code_bundle_uri, shadow, failing_nodes, per_node[{node_id, status, dbt_log_uri, run_results_uri}]}`. `shadow` mirrors the release's own flag, so the classifier can tell a real failure from a rejected fix verification. `stage` is `compile`, `seed_build`, or `validation`; `reason` is `compile_failed`, `seed_build_failed`, or `validation_failed`. For the compile leg, the single `per_node` entry's `node_id` is the service name (a synthetic compile unit, not a dbt node); for validation, entries additionally carry `candidate_artifact_uri`, `node_type`, `file_path`, `service`, and — on every non-ok entry — `changed_ancestors`, the node's transitive upstream ancestors this release changed, each as `{node_id, file_path, service}` with the location this candidate declares (see `handleValidationFailed` below). The stage-less `duplicate_table` rejection also stamps `code_bundle_uri`. The field is the release's code-bundle S3 URI, carried from the release aggregate exactly as on `release.promoted:v1`: empty for a compile-stage rejection, which precedes the parse that produces the bundle, and set for duplicate_table, seed_build, and validation, all of which follow a completed parse. The remediation classifier discriminates by `stage`, fetches the dbt log from S3 for each failing entry, and emits ONE `remediation.requested:v2` trigger carrying every healable failure of the rejection. |
| `remediation.retry_requested:v1` | `remediation` (group `remediation-retry-requested`) | A human retries a rejected release via `POST /releases/{id}/retry-remediation`. Payload: the release's own stored `release.rejected:v1` payload (see above), replayed byte-for-byte except for one added top-level field, `remediation_round` (the incremented round). `event_id` is deterministic on `(release_id, round)` (`remediation-retry:<release_id>:<round>`), so redelivery is a no-op down the outbox. The remediation classifier decodes and classifies it exactly as it does `release.rejected:v1` — see `docs/arch/services/remediation.md`. |

All events are written to the outbox inside the same transaction as the state change and published with an injected `outbox_entry_id` for consumer-side dedup.

### gRPC calls to `agent-remediation`

`RetryRemediation` calls agent-remediation's `RemediationProposals.ListProposals({release_id})` (through `adapters/grpc.ProposalsClient`, implementing the domain-owned `ports.ProposalReader`) before starting a new round, to check the release is not already a live remediation. Each returned proposal carries `remediation_round` (0 on a row recorded before the field existed, read the same as round 1). The read splits into two checks: first, among proposals belonging to the release's *current* round, each node's *latest* attempt (by `attempt` number — an earlier attempt superseded by a later one on the same node does not count) must not be `generating`/`verifying`/`proposed`; second, across *every* round, no attempt's `pr_state` may be `opening`/`open`/`merged`. A round with no proposal rows at all — the round's trigger has not reached agent-remediation yet, or the classifier dropped it — refuses with `ErrRetryInProgress` (HTTP `409 retry_in_progress`) rather than proceeding, since there is nothing yet for a retry to supersede; this is what stops a second `POST` in the seconds after the first from spending a second round before the first round's proposal row exists. Either open-attempt check refuses with `ErrProposalOpen` (HTTP `409 proposal_open`). The client dials `AGENT_REMEDIATION_GRPC_ADDR` (Helm `global.agentRemediationGrpcAddr`, mirrored onto the shared ConfigMap and `docker-compose.yml`; default `agent-remediation:50054`) once at process startup with an insecure (in-cluster) credential, the same pattern every other internal gRPC client in this repository uses; an invalid address fails boot, while an unreachable service surfaces as HTTP `502 proposal_reader_unavailable` on the first retry that calls it, rather than at startup — so the caller can distinguish "no, something is in flight" from "release-controller could not find out."

## Processing Logic

Releases run a FIFO queue: one release is active (`compiling`, `parsing`, `seed_building`, or `validating`) at a time; on each terminal outcome the queue advances the next queued release. The full state machine is: `received` → `compiling` → `parsing` → `seed_building` → `validating` → (`promoted` | `validated` | `rejected` | `superseded`).

`validated` is the terminal status of a **shadow release**: one posted with `shadow: true`, which runs exactly the same path as any other release — parse, candidate-schema build, the real validation Jobs — and then stops. Where a normal release calls `promoteToProduction` on validation success, a shadow release transitions to `validated` and saves, so it never reads or writes `current_prod`, never writes `service_prod`, never emits `release.promoted:v1`, and therefore never reaches the Neo4j topology swap or the promoted code-version history the orchestrator builds from that event. It is not otherwise special: it takes its turn in the same FIFO queue as every other release (bounded in practice by the submitter's own per-release attempt cap), builds and tears down its own candidate schema through the same lifecycle, records the same per-node results, and is pruned by the same retention job.

A shadow release is submitted for either kind, and the kind decides what it actually runs. A `python` shadow reads the packaged contract yaml `agent-remediation` uploaded under the shadow's own canonical key. A `dbt` shadow runs the real compile leg against the team's image, and the proposed source reaches it through `source_overlay_uri`: the S3 URI of a gzipped tar of project-relative files, accepted only on a shadow (`ReceiveCandidateInput.validate` rejects it otherwise), stored on the release row, and threaded verbatim onto `compile.requested:v1` at queue advance and onto `seed.build.requested:v1` when the parsed release has seeds to build. Executor-controller turns it into an `overlay` init container plus a `cp -R /shared/overlay/. ./` prefix on every team-image init container of the compile Job (see `services/executor-controller.md`), so the manifest the release parses, the candidate SQL it rewrites, and the validation Jobs it runs all describe the proposed fix rather than the committed source. Nothing in this service reads the tarball; it is a pointer it carries. On failure a shadow release is rejected like any other and emits `release.rejected:v1` — carrying `shadow: true`, which is what lets the remediation classifier record the rejection without healing it (see `services/remediation.md`). It is also rejected, rather than passed, when its candidate diff against production is empty: `validated` means the fix was validated, so it is never reached with an empty validation set (see the change-detection gate below).

### On `POST /releases`
Create a `Received` release for the single submitted service (idempotent: an existing `release_id` is a no-op). The release records its `changed_service`, that service's `image_tag`, its `kind` (`dbt` or `python`, default `dbt`), and the immutable provenance (`repo`, `commit_sha`) supplied by the caller. The queue advance then promotes the next `Received` release to active, and its `kind` decides how:
- `dbt`: transition to `Compiling`, emit `compile.requested:v1` — CI's manifest still needs a fresh dbt compile before it can be parsed.
- `python`: no compile leg exists — CI already compiled and uploaded the `contract.yaml` artifact before this POST — so the release transitions straight to `Parsing` and emits `release.requested:v1` directly, with the manifest set assembled at this same activation (see Manifest-set assembly below).

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
  derive changed = candidate nodes whose content_hash differs from prod, or are new
  inSet = DescendantsClosure(candidate, changed) ∪ FullAncestorsClosure(candidate, that)
          # changed + their downstream + full transitive upstream closure across service boundaries
  for each inSet node: if any of its direct upstreams is absent from the candidate topology:
      Reject(reason=unbuildable_cross_service_upstream), emit release.rejected:v1, advance queue, return
  for each inSet node: upstream_node_ids = inSet ∩ direct upstreams of node (intra- and cross-service)
  if inSet is empty:
      if release.shadow:
          Reject(reason=nothing_to_validate, no stage, no per_node), emit release.rejected:v1
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

An empty in-set is a trivial pass for a normal release and a rejection for a shadow one. A normal release with nothing to validate promotes directly because emitting an empty `validation.requested:v1` would be refused by executor-controller as a permanent parse error, leaving no `validation.completed:v1` and blocking the queue indefinitely. A shadow release exists only to measure a proposed fix by validating it, and its terminal `validated` status is the sole evidence a reviewer is shown before merging that fix — so an empty in-set, which validates nothing, is rejected with `reason=nothing_to_validate` rather than reported as verified. It happens when the proposed fix changed nothing the candidate topology can see (a node's `content_hash` folds source, shared code, and resolved config, so an edit touching none of them leaves it identical to production) or when the packaged candidate declares no node at all. The rejection carries no `stage` and no `per_node` entries, so remediation derives no failure evidence from it; agent-remediation's shadow-verify reconciler reads the rejected status through `GET /releases/{id}` and records the attempt as failed.

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
   shadow release → transition to Validated and save; nothing else happens (see below)
   otherwise → update current_prod to this release's candidate topology,
      upsert the changed service's service_prod pointer (canonical key + image tag + release id),
      transition to Promoted, emit release.promoted:v1
any stored node not ok (failed or skipped) / aggregate_status not ok → Reject(reason=validation_failed),
   emit release.rejected:v1 {release_id, stage="validation", reason, failing_nodes, shadow,
        per_node[{node_id, status, dbt_log_uri, run_results_uri, candidate_artifact_uri,
                  node_type, file_path, service,
                  changed_ancestors[{node_id, file_path, service}]}], repo, commit_sha,
        code_bundle_uri}
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

A shadow release takes none of that. `promoteToProduction` checks the release's immutable `shadow` flag first and, when it is set, transitions to `Validated` and saves — so the current-prod read, the pointer upsert, the changed-node derivation, and the `release.promoted:v1` emit are all skipped rather than made conditional downstream. The gate lives in the one function every promoting path funnels through, so no path can promote a shadow release by taking a different route to production. Promotion telemetry is likewise not recorded for one: nothing was promoted.

Before updating `current_prod`, `promoteToProduction` computes the set of changed node IDs by calling `DerivedChangedNodeIDs` against the pre-update `current_prod` snapshot (bootstrap emits all nodes as changed). Each node in the `release.promoted:v1` topology carries a `changed` boolean reflecting membership in that set; orchestrator's topology-swap handler refreshes every node's `:Table.content_hash` regardless of this flag, but reads it to decide which seeds to build into production. The event's top-level `repo`, `commit_sha`, and `promoted_at` fields taken from the release's provenance are consumed by orchestrator's separate version-ingestion handler, which records them as each written `:NodeVersion`'s own provenance stamp.

### On `POST /releases/{id}/retry-remediation`

Unlike every other transition in this section, this one is not driven by an inbound stream message through the FIFO queue advance — it is a synchronous HTTP request triggered by a human clicking "Try again" on a dead-end rejected release, handled inline against a `FOR UPDATE`-locked release row (`RetryRemediation`, `service/handlers/retry_remediation.go`). It never touches the release's own `status` (a retried release stays `rejected`) or the FIFO queue; the only effects are the round bump and the outbox row.

```
load release FOR UPDATE
release absent → 404 not_found
release.status != rejected → 409 not_rejected
release.shadow → 409 not_healable
release.reject_reason not in {compile_failed, seed_build_failed, validation_failed,
    duplicate_table} → 409 not_healable
release.rejection_payload empty (no payload was ever stored for this rejection —
    the release predates the column, or the rejection reason never stores one) →
    409 not_retryable
release.remediation_round >= MaxRemediationRounds (3) → 409 rounds_exhausted
ListProposals(release_id) via agent-remediation gRPC (see "gRPC calls to
    agent-remediation" above):
    RPC error → 502 proposal_reader_unavailable
    no proposal belongs to release.remediation_round → 409 retry_in_progress
    among that round's proposals, any node's latest attempt is
        generating/verifying/proposed → 409 proposal_open {proposal_id, pr_url}
        (a proposed attempt whose PR was closed without merging is terminal,
        not open, and does not block a new round)
    among every round's proposals, any attempt's PR is
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

The refusal order matters: existence, then rejection state, then shadow, then healability, then whether there is anything to replay, then the round cap, and only then the one check that costs a network call (`ListProposals`) — so a request that a purely local read can already refuse never reaches agent-remediation at all. The round is capped in code (`release.MaxRemediationRounds = 3`), not in the chart or an env var: raising it means shipping a new release-controller binary, an explicit choice against runaway "try again" chains rather than a per-install tunable. The `retry_in_progress` check exists because a round's proposal rows are written asynchronously by agent-remediation, after `remediation.retry_requested:v1` is consumed — a second `POST` in the window between the round bump and that first row landing would otherwise see an empty current-round proposal set and read it as "nothing open," spending a second round and racing the first at the agent.

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
| Retention | Runs on the janitor interval (`RELEASE_JANITOR_INTERVAL`, default 24h). Deletes terminal releases (promoted, rejected, superseded, validated) whose creation timestamp is older than `RELEASE_RETENTION_DAYS` (default 90 days). Never deletes the release referenced by `current_prod` or by any `service_prod` pointer. For each pruned release, also deletes the `candidate-sql/<release_id>/` and `code-bundles/<release_id>/` S3 prefixes (soft-fail — a delete error does not abort the prune). |

## S3 Behavior

| Operation | Description |
|---|---|
| `DeleteObjects` | Delete the `candidate-sql/<release_id>/` prefix for each pruned release during retention; soft-fail (a delete error is logged but does not abort the prune). |
| `DeleteObjects` | Delete the `code-bundles/<release_id>/` prefix for each pruned release during retention; soft-fail (a delete error is logged but does not abort the prune). |

The S3 bucket is not managed by release-controller's Helm chart. A native S3 lifecycle expiry rule (30 days) on the `candidate-sql/` prefix is configured via a one-time bootstrap for production and the minio-init compose service for development; the prune-time delete is the primary reclaim path there, with the lifecycle rule as backstop. The `code-bundles/` prefix carries the same 30-day lifecycle rule, configured the same way, with the prune-time delete as its primary reclaim path and the lifecycle rule as backstop.

## gRPC Callers

None — release-controller is not called via gRPC by any service.

## Reliability Notes

- Idempotent on `release_id`: a redelivered `POST /releases` or re-promotion is a no-op; `release.promoted:v1` carries a deterministic aggregate id so orchestrator dedups re-emissions.
- Change detection relies on each candidate node carrying a non-empty `content_hash` (topology-controller emits a fold of its own source checksum, its transitive macro checksums, and its resolved-config fingerprint (`source_hash`/`shared_code_hash`/`config_hash`), with deterministic fallbacks so it is never empty). An empty-vs-empty hash would skip validation; this is structurally avoided upstream. Because macros and resolved config are folded in, a shared-macro edit or a config-only change (e.g. a materialization bump in `dbt_project.yml`) changes the hash of every affected node and re-runs them through the validation gate even when that node's own `.sql` is untouched.
- The first release into an empty `current_prod` is submitted with `bootstrap:true`: it promotes without validation and seeds `current_prod` from the candidate topology, giving subsequent releases a change-detection base. A bootstrap promotes whatever topology it carries, so it must be a trusted one.
