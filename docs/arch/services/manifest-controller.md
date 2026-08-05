# manifest-controller

## Purpose

`manifest-controller` parses candidate dbt manifests and resolves cross-service dependencies for a release. It consumes `release.requested:v1`, downloads each `manifest.json` named by the event's explicit `manifest_keys` list (one per service), resolves dependencies, and publishes the resolved candidate topology back to `release-controller` via `manifest.loaded.candidate:v1` (used to validate a candidate release before promotion).

The service runs a single Redis consumer.

**Runtime**: Python 3.12 / uv. Not a Go service.

## Owned Storage

None. Does not own Postgres or Neo4j.

Writes candidate SQL objects to S3 under the key prefix `candidate-sql/<release_id>/candidate_<unique_id>.sql`, plus one code-bundle contract document per release at `code-bundles/<release_id>/bundle.json`. Does not own the bucket; the S3 bucket is shared with `k8s-controller` (logs) and `release-controller` (prune-time delete of both prefixes). S3 writes are the only durable side-effect of the candidate parse; the in-memory cross-service registry is rebuilt from scratch for each `release.requested:v1` message and persisted nowhere.

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

`manifest.loaded.candidate:v1` is a single Redis field `payload` containing JSON. On success: `{release_id, status: "ok", topology, code_bundle_uri}` where `topology` is a list of nodes (`unique_id`, `schema_name`, `table_name`, `service_name`, `node_type`, `test_count`, `content_hash`, `image_tag`, `upstream_unique_ids`, `schedule`, `candidate_sql_uri`). `node_type` is the dbt resource type (`dbt-model`, `dbt-seed`, or `dbt-snapshot`). `test_count` is the number of dbt tests attached to the node: the parser's second pass walks every `resource_type: test` manifest node once and attributes it to `attached_node` when the manifest sets it (generic tests), else to each of `depends_on.nodes` that resolves to a tracked node (singular tests), counting each test at most once per target. `release-controller` carries `test_count` through unchanged onto `release.promoted:v1`, where `orchestrator` persists it as `:Table.test_count`.

`content_hash` is `"sha256:" + sha256(source_hash|shared_code_hash|config_hash)` — a fold of three independently-computed components, so a change to any one of them flips the whole fingerprint:
- `source_hash` is dbt's per-node source checksum (`checksum.checksum` from the manifest node); a node dbt did not checksum falls back to a sha256 of its `raw_code`/`compiled_code`, or failing that a stable JSON dump of the node, so it is never empty.
- `shared_code_hash` is a fold of the source checksums of every macro the node transitively depends on (resolved from the manifest's `macros` map via `depends_on.macros`, following macro→macro edges); `""` for a node with no macro dependencies.
- `config_hash` is a sha256 of the node's resolved `config`, minus the `meta`, `docs`, `description`, `grants`, and `tags` keys, so an out-of-file config change (a materialization or other setting from `dbt_project.yml` / `schema.yml`) flips the hash even when the node's own `.sql` file is untouched.

`release-controller` diffs `content_hash` against the prod snapshot to derive the changed-node set for the validation gate, detecting model SQL, seed CSV, snapshot, shared-macro, and config changes uniformly — a macro edit or a config change re-validates every node that depends on it even when those nodes' own `.sql` is untouched.

`candidate_sql_uri` is an `s3://` URI pointing to the object at `candidate-sql/<release_id>/candidate_<unique_id>.sql`, which holds the node's compiled SQL with every schema-qualified reference that resolves to a known graph node rewritten (via sqlglot) to the candidate schema (`_candidate_<release>`). Seeds carry an empty URI because they have no SELECT to rewrite. An S3 upload failure for any node is fatal: the handler publishes `status=failed` and ACKs — no partial or dangling URI is ever emitted. `image_tag` is left empty — `release-controller` joins in the per-service image tags it assembled for the release.

`code_bundle_uri` is a top-level `s3://` URI pointing to a single code-bundle contract document for the whole release, uploaded to `code-bundles/<release_id>/bundle.json` (`contract_version: 1`). The document is `{contract_version, release_id, nodes, shared_code}`: `nodes` is keyed by the same `schema_name.table_name` unique_id and carries, per node, `runtime` (`"dbt"`), `raw_code`, `compiled_code`, the node's resolved `config`, the three hash components (`source_hash`, `shared_code_hash`, `config_hash`), `content_hash`, and `code_unit_ids` (the node's direct macro dependency ids); `shared_code` maps each referenced macro id to `{source, checksum, depends_on}`. Nothing in the system consumes this document today — it exists so a downstream consumer never needs to parse a dbt manifest directly. An empty-manifest release (no manifest files found) publishes `code_bundle_uri: ""` along with an empty topology. A code-bundle upload failure is fatal in the same way as a candidate-SQL upload failure: the handler publishes `status=failed` (`error_class=CodeBundleUploadFailed`) and ACKs; the bundle is built and uploaded only after every node's candidate SQL has uploaded successfully, so a bundle failure never leaves a partially-uploaded topology.

On failure: `{release_id, status: "failed", error_class, error_detail}`. `release-controller` uses this to transition a release from parsing to validating, or to mark it failed.

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
    InvalidCompiledSqlError (compiled_sql does not parse as SQL) → publish status=failed (error_class=InvalidCompiledSql), ACK
  For each node: rewrite_to_candidate_schema(compiled_sql, lookup, candidate_schema)
    Rewrites every schema-qualified reference whose (schema, table) pair is in the registry
    to the candidate schema using sqlglot; CTE aliases, unqualified refs, and tables
    not in the registry are left unchanged; seeds carry empty compiled_sql → candidate_sql_uri=""
  For each non-seed node: upload rewritten SQL to S3 at candidate-sql/<release_id>/candidate_<unique_id>.sql
    → upload failure: publish status=failed (error_class=CandidateSqlUploadFailed), ACK (fatal — no partial URI emitted)
    → success: candidate_sql_uri = s3://<bucket>/candidate-sql/<release_id>/candidate_<unique_id>.sql
  Shape each node as {unique_id, schema_name, table_name, service_name, node_type, content_hash, image_tag, upstream_unique_ids, schedule, candidate_sql_uri}

Build the release's code bundle {contract_version, release_id, nodes, shared_code} and upload it to
  code-bundles/<release_id>/bundle.json
  → upload failure: publish status=failed (error_class=CodeBundleUploadFailed), ACK
  → success: code_bundle_uri = s3://<bucket>/code-bundles/<release_id>/bundle.json

Publish manifest.loaded.candidate:v1 status=ok with the topology and code_bundle_uri, ACK
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
| `compiled_sql` does not parse as PostgreSQL (e.g. an un-suppressed Jinja expression leaking literal text, such as a trailing comma inside `{{ config(...) }}` rendering as `('',)`, or an unterminated string literal failing the tokenizer) | `InvalidCompiledSqlError` raised → node fails to load |

Both the dependency resolver and the candidate-schema rewriter parse `compiled_sql` with sqlglot's `postgres` dialect — the only warehouse this system targets — so postgres-specific syntax (e.g. `ARRAY[...] @> ARRAY[...]`) resolves normally.

## S3 Behavior

| Operation | Description |
|---|---|
| `download_file` | Download each manifest JSON named in `manifest_keys` to a temp path; no S3 listing is performed |
| `PutObject` | Upload the rewritten candidate SQL for each non-seed node to `candidate-sql/<release_id>/candidate_<unique_id>.sql`; failure is fatal and aborts the load |
| `PutObject` | Upload one code-bundle contract document per release to `code-bundles/<release_id>/bundle.json`; failure is fatal and aborts the load |

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
- A code-bundle upload failure is likewise fatal, for the same reason: it is reported as `status=failed` (`error_class=CodeBundleUploadFailed`) and ACKed rather than published with a dangling `code_bundle_uri`. The bundle upload runs only after every node's candidate SQL has already uploaded successfully.
- Releases enter through `release-controller`'s `POST /releases`, which emits `release.requested:v1` for this service to parse.
