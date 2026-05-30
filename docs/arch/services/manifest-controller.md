# manifest-controller

## Purpose

`manifest-controller` is the topology ingestion service. It loads dbt `manifest.json` files, resolves cross-service dependencies, and publishes the resolved topology to one of two consumers depending on which stream triggered the load:

- the **graph-update flow** publishes to `orchestrator` via `manifest.loaded:v1` (the live topology that backs the production graph), and
- the **candidate-parse flow** publishes back to `release-controller` via `manifest.loaded.candidate:v1` (a per-release topology parsed from a release-specific S3 prefix, used to validate a candidate release before promotion).

Both flows run concurrently in one process, each driven by its own Redis consumer.

**Runtime**: Python 3.12 / uv. Not a Go service.

## Owned Storage

None. Does not own Postgres, Neo4j, or S3.

On the graph-update flow, an intermediate registry CSV is written to the local filesystem during processing (via `RegistryRepository`) and used within the same request to resolve cross-service dependencies. It does not persist across restarts as a source of truth. The candidate-parse flow builds the equivalent registry in memory only and persists nothing.

## Inbound Interfaces

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `update.graph:v1` | `manifest-controller-update-graph` | Triggers a full manifest load of the live topology; consumed as a consumer group (`xreadgroup`) |
| `release.requested:v1` | `manifest-controller-release-requested` | Triggers a per-release manifest load from a release-specific S3 prefix; consumed as a consumer group (`xreadgroup`) |

`update.graph:v1` required message field:
- `source`: `"local"` or `"s3"` — determines manifest source

Unknown `source` values are treated as errors; the message is **not ACKed** and will be replayed.

`release.requested:v1` required message field:
- `payload`: JSON `{release_id, manifests_uri}` where `manifests_uri` is an `s3://bucket/prefix/` URI pointing at the release's manifests. A missing/invalid payload, or one missing `release_id`/`manifests_uri`, is treated as an error; the message is **not ACKed** and will be replayed.

### HTTP

None (no HTTP interface; runs as `tail -f /dev/null` in dev; started manually or via `start-services.sh`).

## Outbound Interfaces

### Redis producer

| Stream | Description |
|---|---|
| `manifest.loaded:v1` | Published after a successful `update.graph:v1` load; consumed by `orchestrator` |
| `manifest.loaded.candidate:v1` | Published after a `release.requested:v1` load (success or failure); consumed by `release-controller` |

`manifest.loaded:v1` is a single Redis field `payload` containing JSON: a list of topology node objects with their resolved upstream dependencies. The `orchestrator` consumes this and upserts `Table` nodes and `DEPENDS_ON` edges in Neo4j, then publishes `schedules.loaded:v1` to `state` with the schedule names list.

`manifest.loaded.candidate:v1` is a single Redis field `payload` containing JSON. On success: `{release_id, status: "ok", topology}` where `topology` is a list of nodes (`unique_id`, `schema_name`, `table_name`, `service_name`, `node_type`, `content_hash`, `image_tag`, `upstream_unique_ids`, `schedule`). `node_type` is the dbt resource type (`dbt-model`, `dbt-seed`, or `dbt-snapshot`). `content_hash` is dbt's per-node source checksum (`checksum.checksum` from the manifest node); `release-controller` diffs it against the prod snapshot to derive the changed-node set for the validation gate, detecting model SQL, seed CSV, and snapshot changes uniformly. `image_tag` is left empty — `release-controller` joins in the per-service image tags it received from CI. On failure: `{release_id, status: "failed", error_class, error_detail}`. `release-controller` uses this to transition a release from parsing to validating, or to mark it failed.

Calls no gRPC services.

## Processing Logic

### On `update.graph:v1`

```
Parse source field → choose LocalManifestSource or S3ManifestSource

Pass 1 — Parse
  For each manifest file (local: /manifests/*.json, s3: listed + downloaded):
    parse_manifest(path, version) → list[ManifestNode]
  Flatten into all_nodes

Pass 2 — Build registry
  Construct NodeRegistry from all_nodes (table_name, schema_name, service_name, owner)
  Save registry to local filesystem CSV (for cross-service dep lookup)
  Build lookup dict: (schema_name, table_name) → NodeRegistryEntry

Pass 3 — Resolve deps
  For each node in all_nodes:
    resolve_upstream_deps(node, lookup):
      - parse compiled_sql via sqlglot
      - skip CTEs
      - skip qualified self-references
      - raise UnqualifiedTableReferenceError on unqualified references
      - skip tables not in registry (external/source tables)
      - dbt seeds ARE in registry -> seed refs resolve as upstream deps

Publish manifest.loaded:v1 (all nodes with resolved deps as JSON payload)

ACK message
```

On any exception: message is **not ACKed**; it stays in the group PEL and is retried by the reclaim sweep (see Consumer Reliability).

### On `release.requested:v1`

```
Decode payload {release_id, manifests_uri}
parse_s3_uri(manifests_uri) → (bucket, prefix)
Build an S3 source scoped to that bucket + per-release prefix

list_manifests() from the per-release prefix
  No manifests → publish status=ok with empty topology, ACK

Pass 1 — Parse
  For each manifest file: parse_manifest(path, version, image_tag) → list[ManifestNode]
  Malformed manifest — invalid JSON, a missing top-level `nodes` key, or an
    invalid node shape (missing `schema`/`fqn`, empty `fqn`) → publish
    status=failed (error_class=MalformedManifest), ACK

Pass 2 — Build registry (in memory only; no CSV persisted)
  Build lookup dict: (schema_name, table_name) → NodeRegistryEntry

Pass 3 — Resolve deps and shape candidate topology
  For each node: resolve_upstream_deps(node, lookup) (same sqlglot rules as the graph-update flow)
    UnqualifiedTableReferenceError → publish status=failed (error_class=UnqualifiedTableReference), ACK
  Shape each node as {unique_id, schema_name, table_name, service_name, node_type, content_hash, image_tag, upstream_unique_ids, schedule}

Publish manifest.loaded.candidate:v1 status=ok with the topology, ACK
```

This flow differs from the graph-update flow in three ways: it does not run the publish-boundary `image_tag` validator (`image_tag` is empty by design and joined in by `release-controller`); it persists no registry; and it reports parse/resolve failures back as a `status=failed` business signal rather than failing silently.

Failure-handling distinction: a parse or resolve failure that re-delivery cannot fix (malformed manifest JSON or node shape, unresolvable reference) is published as `status=failed` and the message is ACKed — replaying it would not help. A transient infrastructure failure (S3 read error, Redis publish error) propagates so the message is **not ACKed**; it stays in the group PEL and is retried by the reclaim sweep (see Consumer Reliability).

### Dependency resolution rules (sqlglot)

| Case | Behavior |
|---|---|
| CTE alias | Skipped |
| Qualified self-reference | Skipped |
| Unqualified table reference | `UnqualifiedTableReferenceError` raised → node fails to load |
| Table not in registry | Skipped (external/source table) |
| Table in registry | Resolved as `UpstreamDep` |
| dbt seed reference | Resolved as `UpstreamDep` (seeds are registered in pass 2) |

## S3 Behavior

| Operation | Description |
|---|---|
| `list_objects_v2` | List manifest files under a prefix. The graph-update flow uses the env-derived `{env}/manifest/` prefix; the candidate flow uses the per-release prefix parsed from `manifests_uri` |
| `download_file` | Download each manifest JSON to a temp path |
| `get_object` | Read the per-service `service_metadata.json` sidecar for `image_tag` (absent on the candidate path, so `image_tag` stays empty) |

No S3 writes.

## Consumer Reliability

- Both consumers (`update.graph:v1` and `release.requested:v1`) run in the same process on separate daemon threads and share a single connection-pooled `redis` client; each maintains its own consumer-group offset.
- Consumer group is created with `id="0"` (reads from the beginning on first create; `BUSYGROUP` error on re-create is ignored)
- Consumer name is `consumer-{random_hex}` (unique per process restart)
- `NOGROUP` error recovery: consumer group is recreated and the loop retries after 3 seconds
- Message is ACKed only after the handler returns without raising; a failure leaves the message in the group PEL
- Each loop iteration first runs an `XAUTOCLAIM` reclaim sweep before reading new (`>`) messages. Because consumer names are random per restart, a message left pending by a transient failure (or by a crashed consumer) would otherwise never be re-read. The sweep claims and re-dispatches any message idle longer than the reclaim window (60s — wide enough never to steal a live peer's in-flight work), so transient failures are retried instead of stranding the release; a still-failing message stays pending for the next sweep
- The main thread parks on both consumer threads. On `SIGTERM` the process exits and the daemon threads are abandoned; Kubernetes restarts the pod.

## Background Loops

| Loop | Description |
|---|---|
| `update.graph:v1` consumer loop | Blocking `xreadgroup` (1s block, batch of 10); dispatches to `ManifestHandler` |
| `release.requested:v1` consumer loop | Blocking `xreadgroup` (1s block, batch of 10); dispatches to `CandidateManifestHandler` |

## gRPC Callers

None -- manifest-controller is not called via gRPC by any service.

## Reliability Notes

- No local outbox -- if the Redis publish of `manifest.loaded:v1` fails, the message is not ACKed and the entire load is replayed.
- The candidate flow has no per-message dedup store. A `release.requested:v1` redelivered after a successful publish causes a second `manifest.loaded.candidate:v1`; `release-controller` handles this idempotently (the candidate transition only applies while the release is still parsing, and a duplicate is logged and ACKed).
- An unresolvable reference in pass 3 aborts the whole load: on the graph-update flow the exception is not ACKed and the message replays; on the candidate flow it is reported as `status=failed` and ACKed.
- `update.graph:v1` is published by `ui-service` via `POST /api/graph/update`. In production the deploy CI workflow triggers this endpoint through a one-shot `kubectl run` curl pod inside the `continuo` namespace. In local development it is reached via `dbt/update-graph.sh`.
