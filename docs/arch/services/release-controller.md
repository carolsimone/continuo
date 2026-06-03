# release-controller

## Purpose

`release-controller` owns the dbt blue/green candidate-release lifecycle: it gates every candidate behind a `dbt --empty` validation against the changed-and-downstream models and only swaps production (topology, schedules, image tags) when validation passes. It holds the `current_prod` pointer — the single source of truth for what is live — and orchestrates the release state machine across manifest-controller, executor-controller, and orchestrator via Redis streams.

**Runtime**: Go service. Exposes an HTTP API and consumes/produces Redis streams; persists to its own Postgres database via a transactional outbox.

## Owned Storage

Postgres (its own database). Tables:

| Table | Purpose |
|---|---|
| `releases` | One row per candidate release: status, image tags, manifests URI, candidate topology, validation node ids, per-node results, transition history. |
| `current_prod` | Singleton row: the promoted `release_id`, the `manifests_uri` that release was submitted with, and its `topology_snapshot` (the live topology). |
| `release_controller_outbox` | Transactional outbox; one row per produced event, drained by the outbox publisher. |
| `message_processing` | Inbound dedup ledger (`outbox_entry_id` / message id) for idempotent consumption. |

The `topology_snapshot` is the live topology as a list of nodes (`unique_id`, `schema_name`, `table_name`, `service_name`, `node_type`, `content_hash`, `image_tag`, `upstream_unique_ids`, `schedule`); the per-node `content_hash` comparison against it determines which nodes a new candidate must validate. When a new candidate is promoted, its candidate topology (carrying `content_hash` + joined `image_tag`) replaces the snapshot, forming the change-detection base for the next release.

## Inbound Interfaces

### HTTP

| Route | Purpose |
|---|---|
| `POST /releases` | Accept a candidate release. Body: `{release_id, image_tags, manifests_uri, bootstrap?}`. Idempotent on `release_id`. `bootstrap:true` promotes without validation (see Processing Logic). |
| `GET /releases/{id}` | Full release detail: `{release_id, status, transitions, validation_node_ids, reject_reason, failing_nodes, per_node_results, image_tags, manifests_uri, bootstrap}`. `per_node_results` is an array of `{node_id, status, dbt_log_uri, duration_ms}`. |
| `GET /releases` | Paginated release history, newest-first. Query params: `status` (optional exact-match filter), `limit` (default 20; values that are unparseable, non-positive, or exceed 100 fall back to the default of 20), `cursor` (opaque keyset cursor). Response: `{"releases":[{release_id, status, created_at, resolved_at, node_count, bootstrap, reject_reason}], "next_cursor":"<opaque or empty>"}`. |
| `GET /current-prod` | The current promoted release + topology snapshot. |
| `GET /healthz` | Liveness. |

`POST /releases` is the production entry point for a deploy: the CI deploy workflow uploads the per-release manifests to S3 (`releases/<id>/manifests/<service>/manifest_v1.json`) and posts the release here. It does not carry a changed-node list — release-controller derives it (see Processing Logic).

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `manifest.loaded.candidate:v1` | `release-controller-manifest-loaded-candidate` | Resolved candidate topology (or a parse failure) from manifest-controller. |
| `validation.completed:v1` | `release-controller-validation-completed` | Aggregate per-node validation results from executor-controller. |

## Outbound Interfaces

### Redis producer

| Stream | Consumed by | Emitted when |
|---|---|---|
| `release.requested:v1` | manifest-controller | A release becomes active and needs its manifests parsed. |
| `validation.requested:v1` | executor-controller | A candidate has changed nodes to validate. |
| `release.promoted:v1` | orchestrator | A release is promoted to production. |
| `release.rejected:v1` | (observers) | A release fails parsing or validation. |

All events are written to the outbox inside the same transaction as the state change and published with an injected `outbox_entry_id` for consumer-side dedup.

Calls no gRPC services.

## Processing Logic

Releases run a FIFO queue: one release is active (parsing or validating) at a time; on each terminal outcome the queue advances the next queued release.

### On `POST /releases`
Create a `Received` release (idempotent: an existing `release_id` is a no-op). The queue advance promotes the next `Received` release to `Parsing` and emits `release.requested:v1`.

### On `manifest.loaded.candidate:v1`
```
status=failed → Reject(reason=parse_failed), emit release.rejected:v1, advance queue
status=ok:
  join per-service image_tags into the candidate topology
  if release.bootstrap: promote directly, skipping validation
      (record candidate topology, update current_prod, transition to Promoted,
       emit release.promoted:v1); advance queue, return
  load current_prod.topology_snapshot
  derive changed = candidate nodes whose content_hash differs from prod, or are new
  for each changed node: if it has a new cross-service upstream absent from current_prod:
      Reject(reason=new_cross_service_upstream), emit release.rejected:v1, advance queue, return
  inSet = DescendantsClosure(candidate, changed) ∪ AncestorsClosure(intra-service, inSet)
          # changed + their downstream + transitive intra-service ancestors of each in-set node
  for each inSet node: upstream_node_ids = inSet ∩ same-service direct upstreams of node
  if inSet is empty:
      promote directly (nothing to validate trivially passes the gate):
        update current_prod, transition to Promoted, emit release.promoted:v1
  else:
      transition to Validating, emit validation.requested:v1
        (mode=validation, candidate_schema=_candidate_<release_id>,
         nodes carry upstream_node_ids)
  advance queue
```
A `bootstrap:true` release skips validation entirely: it records the candidate topology, seeds `current_prod`, and promotes directly. This is the initial cutover (or a trusted re-baseline) against an empty or mismatched `current_prod`, where normal validation would reject every cross-service upstream as new. A non-bootstrap release against an empty snapshot instead treats every candidate node as changed and validates the whole topology.

### On `validation.completed:v1`
```
all nodes ok and none missing → handleValidationOK:
   update current_prod to this release's candidate topology,
   transition to Promoted, emit release.promoted:v1
any node failed / missing / aggregate not ok → Reject(reason=validation_failed),
   emit release.rejected:v1
advance queue
```

Promotion is shared by the validation-passed path and the nothing-to-validate short-circuit: both point `current_prod` at the candidate topology, transition the release to `Promoted`, and emit `release.promoted:v1`. The candidate topology (carrying `content_hash` + joined `image_tag`) becomes the new snapshot, so the next release's change-detection diff is correct.

## Consumer Reliability

- Two consumer groups (`manifest.loaded.candidate:v1`, `validation.completed:v1`) run in the same process; each maintains its own offset.
- Inbound messages are deduped via `message_processing` (idempotent on the upstream `outbox_entry_id`), so a redelivery is absorbed.
- A permanent parse-decode failure is ACKed (logged, not retried); transient errors are not ACKed and replay.
- A `manifest.loaded.candidate:v1` or `validation.completed:v1` message whose `release_id` no longer has a `releases` row (pruned, or reclaimed from a previous consumer for a deleted release) is logged and dropped rather than processed. The repository's `Get` returns no row as `(nil, nil)`, so both handlers nil-check the aggregate before use; without that guard a reclaimed message for a missing release would crash the consumer on startup.
- State changes and the outbox row are written in one transaction; the outbox publisher drains rows and XADDs them, injecting `outbox_entry_id` for downstream dedup.

## Background Loops

| Loop | Description |
|---|---|
| Outbox publisher | Drains `release_controller_outbox` and XADDs each row to its stream. |
| `manifest.loaded.candidate:v1` consumer | Dispatches to the parsed-manifest handler. |
| `validation.completed:v1` consumer | Dispatches to the validation-result handler. |
| Retention | Runs on the janitor interval (`RELEASE_JANITOR_INTERVAL`, default 24h). Deletes terminal releases (promoted, rejected, superseded) whose creation timestamp is older than `RELEASE_RETENTION_DAYS` (default 90 days). Never deletes the release referenced by `current_prod`. |

## gRPC Callers

None — release-controller is not called via gRPC by any service.

## Reliability Notes

- Idempotent on `release_id`: a redelivered `POST /releases` or re-promotion is a no-op; `release.promoted:v1` carries a deterministic aggregate id so orchestrator dedups re-emissions.
- Change detection relies on each candidate node carrying a non-empty `content_hash` (manifest-controller emits dbt's per-node checksum, with a deterministic fallback). An empty-vs-empty hash would skip validation; this is structurally avoided upstream.
- The first release into an empty `current_prod` is submitted with `bootstrap:true`: it promotes without validation and seeds `current_prod` from the candidate topology, giving subsequent releases a change-detection base. A bootstrap promotes whatever topology it carries, so it must be a trusted one.
