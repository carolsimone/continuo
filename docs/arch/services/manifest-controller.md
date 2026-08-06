# manifest-controller

## Purpose

`manifest-controller` parses candidate dbt manifests and resolves cross-service dependencies for a release. It consumes `release.requested:v1`, downloads each `manifest.json` named by the event's explicit `manifest_keys` list (one per service), resolves dependencies, and publishes the resolved candidate topology back to `release-controller` via `manifest.loaded.candidate:v1` (used to validate a candidate release before promotion).

The service runs a single Redis consumer.

**Runtime**: Python 3.12 / uv. Not a Go service.

## Owned Storage

None. Does not own Postgres or Neo4j.

Writes candidate SQL objects to S3 under the key prefix `candidate-sql/<release_id>/<unique_id>.sql`. Does not own the bucket; the S3 bucket is shared with `k8s-controller` (logs) and `release-controller` (prune-time delete of the same prefix). S3 writes are the only durable side-effect of the candidate parse; the in-memory cross-service registry is rebuilt from scratch for each `release.requested:v1` message and persisted nowhere.

## Inbound Interfaces

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `release.requested:v1` | `manifest-controller-release-requested` | Triggers a per-release manifest load from an explicit list of S3 object keys; consumed as a consumer group (`xreadgroup`) |

`release.requested:v1` required message field:
- `payload`: JSON `{release_id, manifest_keys}` where `manifest_keys` is a list of `{service, s3_uri}` entries — one per service — each `s3_uri` an `s3://bucket/<service>/<release_id>/manifest.json` URI. A missing/invalid payload, or one missing `release_id`/`manifest_keys`, or one whose keys span more than one bucket, is treated as an error; the message is **not ACKed** and will be replayed.

### HTTP

None (no HTTP interface; runs as `tail -f /dev/null` in dev; started manually or via `start-services.sh`).

## Outbound Interfaces

### Redis producer

| Stream | Description |
|---|---|
| `manifest.loaded.candidate:v1` | Published after a `release.requested:v1` load (success or failure); consumed by `release-controller` |

`manifest.loaded.candidate:v1` is a single Redis field `payload` containing JSON. On success: `{release_id, status: "ok", topology}` where `topology` is a list of nodes (`unique_id`, `schema_name`, `table_name`, `service_name`, `node_type`, `test_count`, `content_hash`, `image_tag`, `original_file_path`, `upstream_unique_ids`, `schedule`, `candidate_sql_uri`). `node_type` is the dbt resource type (`dbt-model`, `dbt-seed`, or `dbt-snapshot`). `test_count` is the number of dbt tests attached to the node: the parser's second pass walks every `resource_type: test` manifest node once and attributes it to `attached_node` when the manifest sets it (generic tests), else to each of `depends_on.nodes` that resolves to a tracked node (singular tests), counting each test at most once per target. `release-controller` carries `test_count` through unchanged onto `release.promoted:v1`, where `orchestrator` persists it as `:Table.test_count`. `content_hash` is a macro-aware fingerprint: dbt's per-node source checksum (`checksum.checksum` from the manifest node) folded together with the source checksums of every macro the node transitively depends on (resolved from the manifest's `macros` map via `depends_on.macros`, following macro→macro edges). A node with no macro dependencies keeps its verbatim dbt checksum. `release-controller` diffs it against the prod snapshot to derive the changed-node set for the validation gate, detecting model SQL, seed CSV, snapshot, and shared-macro changes uniformly — a macro edit re-validates every node that depends on it even when those nodes' own `.sql` is untouched. `candidate_sql_uri` is an `s3://` URI pointing to the object at `candidate-sql/<release_id>/<unique_id>.sql`, which holds the node's compiled SQL with every schema-qualified reference that resolves to a known graph node rewritten (via sqlglot) to the candidate schema (`_candidate_<release>`). Seeds carry an empty URI because they have no SELECT to rewrite. An S3 upload failure for any node is fatal: the handler publishes `status=failed` and ACKs — no partial or dangling URI is ever emitted. `image_tag` is left empty — `release-controller` joins in the per-service image tags it assembled for the release. On failure: `{release_id, status: "failed", error_class, error_detail}`. `release-controller` uses this to transition a release from parsing to validating, or to mark it failed.

Calls no gRPC services.

## Processing Logic

### On `release.requested:v1`

```
Decode payload {release_id, manifest_keys}
For each entry: parse_s3_uri(s3_uri) → (bucket, key); strip trailing slash
  All entries must share one bucket (otherwise error, not ACKed)
Build an S3 source scoped to that bucket + the explicit key list

list_manifests() downloads exactly the listed keys (no S3 listing)
  No keys → publish status=ok with empty topology, ACK

Pass 1 — Parse and validate against the declared service
  For each manifest file: parse_manifest(path, version, image_tag) → list[ManifestNode]
  Malformed manifest — invalid JSON, a missing top-level `nodes` key, or an
    invalid node shape (missing `schema`/`fqn`, empty `fqn`) → publish
    status=failed (error_class=MalformedManifest), ACK
  Each manifest key declares the service it belongs to (manifest_keys[].service).
  The manifest is validated against that declared service:
    Zero model/seed nodes → publish status=failed (error_class=EmptyManifest), ACK
    Any node whose service_name differs from the declared service →
      publish status=failed (error_class=ServiceMismatch), ACK
  This rejects an empty or wrong-service upload before it can enter the candidate
  topology — without it, such a candidate would promote with the declared
  service's nodes missing and silently retire that service.

Pass 2 — Build registry (in memory only; no CSV persisted)
  Build lookup dict: (schema_name, table_name) → NodeRegistryEntry

Pass 3 — Resolve deps, rewrite SQL, upload to S3, and shape candidate topology
  For each node: resolve_upstream_deps(node, lookup) (sqlglot rules below)
    UnqualifiedTableReferenceError → publish status=failed (error_class=UnqualifiedTableReference), ACK
    InvalidCompiledSqlError (a dependency SQL does not parse as SQL) → publish status=failed (error_class=InvalidCompiledSql), ACK
  For each node: rewrite_to_candidate_schema(node.candidate_sql, lookup, candidate_schema)
    Rewrites every schema-qualified reference whose (schema, table) pair is in the registry
    to the candidate schema using sqlglot; CTE aliases, unqualified refs, and tables
    not in the registry are left unchanged; seeds carry an empty candidate_sql (and an empty
    dependency_sqls) → candidate_sql_uri=""
  For each non-seed node: upload rewritten SQL to S3 at candidate-sql/<release_id>/<unique_id>.sql
    → upload failure: publish status=failed (error_class=CandidateSqlUploadFailed), ACK (fatal — no partial URI emitted)
    → success: candidate_sql_uri = s3://<bucket>/candidate-sql/<release_id>/<unique_id>.sql
  Shape each node as {unique_id, schema_name, table_name, service_name, node_type, test_count, content_hash, image_tag, original_file_path, upstream_unique_ids, schedule, candidate_sql_uri}

Publish manifest.loaded.candidate:v1 status=ok with the topology, ACK
```

The flow leaves `image_tag` empty by design (`release-controller` joins the per-service tags it assembled for the release onto the candidate topology); it builds its registry in memory and persists nothing; and it reports parse/resolve failures back as a `status=failed` business signal rather than failing silently.

Failure-handling distinction: a parse or resolve failure that re-delivery cannot fix (malformed manifest JSON or node shape, an empty or wrong-service manifest, an unresolvable reference, compiled SQL that does not parse) is published as `status=failed` and the message is ACKed — replaying it would not help. A transient infrastructure failure (S3 read error, Redis publish error) propagates so the message is **not ACKed**; it stays in the group PEL and is retried by the reclaim sweep (see Consumer Reliability).

### Dependency resolution rules (sqlglot)

| Case | Behavior |
|---|---|
| CTE alias | Skipped |
| Qualified self-reference | Skipped |
| Unqualified table reference | `UnqualifiedTableReferenceError` raised → node fails to load |
| Table not in registry | Skipped (external/source table) |
| Table in registry | Resolved as `UpstreamDep` |
| dbt seed reference | Resolved as `UpstreamDep` (seeds are registered in pass 2) |
| A dependency SQL does not parse under the configured engine's dialect (e.g. an un-suppressed Jinja expression leaking literal text, such as a trailing comma inside `{{ config(...) }}` rendering as `('',)`, or an unterminated string literal failing the tokenizer) | `InvalidCompiledSqlError` raised → node fails to load |

The dependency resolver parses each entry of `dependency_sqls`; the candidate-schema rewriter parses `candidate_sql` and re-renders it. Both use the sqlglot dialect of the warehouse engine the install targets, so engine-specific syntax (e.g. Postgres' `ARRAY[...] @> ARRAY[...]`) resolves normally on that engine.

The engine comes from `WAREHOUSE_ENGINE`, which the chart sets on the shared ConfigMap from `validation.engine` (`postgres` by default). `main.py` resolves it to a dialect once at boot and injects it into `CandidateManifestHandler`, which passes it to both the resolver and the rewriter — so the SQL uploaded to S3 is in the dialect the validation runner will execute against. An engine with no dialect mapping fails `config.validate()` at startup rather than silently emitting another engine's SQL: sqlglot's `postgres` dialect renders a cast as `CAST(x AS TEXT)`, which Trino rejects.

## S3 Behavior

| Operation | Description |
|---|---|
| `download_file` | Download each manifest JSON named in `manifest_keys` to a temp path; no S3 listing is performed |
| `PutObject` | Upload the rewritten candidate SQL for each non-seed node to `candidate-sql/<release_id>/<unique_id>.sql`; failure is fatal and aborts the load |

## Consumer Reliability

- The `release.requested:v1` consumer runs on a daemon thread with a connection-pooled `redis` client and maintains its own consumer-group offset.
- Consumer group is created with `id="0"` (reads from the beginning on first create; `BUSYGROUP` error on re-create is ignored)
- Consumer name is `consumer-{random_hex}` (unique per process restart)
- `NOGROUP` error recovery: consumer group is recreated and the loop retries after 3 seconds
- Message is ACKed only after the handler returns without raising; a failure leaves the message in the group PEL
- Each loop iteration first runs an `XAUTOCLAIM` reclaim sweep before reading new (`>`) messages. Because consumer names are random per restart, a message left pending by a transient failure (or by a crashed consumer) would otherwise never be re-read. The sweep claims and re-dispatches any message idle longer than the reclaim window (60s — wide enough never to steal a live peer's in-flight work), so transient failures are retried instead of stranding the release; a still-failing message stays pending for the next sweep
- The main thread parks on the consumer thread. On `SIGTERM` the process exits and the daemon thread is abandoned; Kubernetes restarts the pod.

## Background Loops

| Loop | Description |
|---|---|
| `release.requested:v1` consumer loop | Blocking `xreadgroup` (1s block, batch of 10); dispatches to `CandidateManifestHandler` |

## gRPC Callers

None -- manifest-controller is not called via gRPC by any service.

## Reliability Notes

- No local outbox -- if the Redis publish of `manifest.loaded.candidate:v1` fails, the message is not ACKed and the entire load is replayed.
- The candidate flow has no per-message dedup store. A `release.requested:v1` redelivered after a successful publish causes a second `manifest.loaded.candidate:v1`; `release-controller` handles this idempotently (the candidate transition only applies while the release is still parsing, and a duplicate is logged and ACKed).
- An unresolvable reference in pass 3 aborts the load: it is reported as `status=failed` and ACKed, since replaying it would not help.
- Compiled SQL that fails to tokenize or parse (`sqlglot.errors.TokenError` / `sqlglot.errors.ParseError`) in pass 3 is likewise a permanent failure: it is reported as `status=failed` (error_class=InvalidCompiledSql) and ACKed rather than left for the reclaim sweep, which is reserved for transient infrastructure failures.
- An S3 upload failure in pass 3 is also fatal: it is reported as `status=failed` and ACKed. No partial URI is ever emitted — either all nodes' SQL objects are uploaded and the topology carries complete `candidate_sql_uri` values, or the entire load is aborted.
- Releases enter through `release-controller`'s `POST /releases`, which emits `release.requested:v1` for this service to parse.
