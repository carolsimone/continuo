# manifest-controller

## Purpose

`manifest-controller` parses each service's candidate release artifact — a dbt `manifest.json` or a python service's `contract.yaml` — and resolves cross-service dependencies for a release. It consumes `release.requested:v1`, downloads the artifact named by each of the event's explicit `manifest_keys` entries (one per service; the entry's `kind` selects which parser reads it), resolves dependencies, and publishes the resolved candidate topology back to `release-controller` via `manifest.loaded.candidate:v1` (used to validate a candidate release before promotion).

The service runs a single Redis consumer.

**Runtime**: Python 3.12 / uv. Not a Go service.

## Owned Storage

None. Does not own Postgres or Neo4j.

Writes each node's candidate artifact to S3 under the key prefix `candidate-sql/<release_id>/candidate_<unique_id>.<sql|json>` — `.sql` for a dbt node's rewritten compiled SQL, `.json` for a python node's validation spec — plus one code-bundle contract document per release at `code-bundles/<release_id>/bundle.json`. Does not own the bucket; the S3 bucket is shared with `k8s-controller` (logs) and `release-controller` (prune-time delete of both prefixes). S3 writes are the only durable side-effect of the candidate parse; the in-memory cross-service registry is rebuilt from scratch for each `release.requested:v1` message and persisted nowhere.

## Inbound Interfaces

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `release.requested:v1` | `manifest-controller-release-requested` | Triggers a per-release load of each service's dbt manifest or python contract, named by an explicit list of S3 object keys; consumed as a consumer group (`xreadgroup`) |

`release.requested:v1` required message field:
- `payload`: JSON `{release_id, manifest_keys}` where `manifest_keys` is a list of `{service, s3_uri, kind}` entries — one per service — each `s3_uri` an `s3://bucket/<service>/<release_id>/manifest.json` (dbt) or `s3://bucket/<service>/<release_id>/contract.yaml` (python) URI. `kind` is `"dbt"` or `"python"` and selects the parser for that entry; a producer that predates python support omits it, which defaults to `"dbt"`. A missing/invalid payload, one missing `release_id`/`manifest_keys`, an entry with a missing/empty `service`, or entries whose keys span more than one bucket, is treated as a payload-decode error; the message is **not ACKed** and will be replayed. A `kind` value this build cannot parse is not a decode error — it is threaded through and reported as a permanent per-entry parse failure instead (see Processing Logic).

### HTTP

None (no HTTP interface; runs as `tail -f /dev/null` in dev; started manually or via `start-services.sh`).

## Outbound Interfaces

### Redis producer

| Stream | Description |
|---|---|
| `manifest.loaded.candidate:v1` | Published after a `release.requested:v1` load (success or failure); consumed by `release-controller` |

`manifest.loaded.candidate:v1` is a single Redis field `payload` containing JSON. On success: `{release_id, status: "ok", topology, code_bundle_uri}` where `topology` is a list of nodes (`unique_id`, `schema_name`, `table_name`, `service_name`, `node_type`, `test_count`, `content_hash`, `image_tag`, `original_file_path`, `upstream_unique_ids`, `schedule`, `candidate_artifact_uri`). `unique_id` is `"<schema_name>.<table_name>"` lowercased; the declared `schema_name`/`table_name` fields on the node keep their original spelling (they render into SQL and DDL, where the declared spelling is what addresses the relation), but the identity key folds case because the warehouse lookups that resolve a reference to a node already fold case (`NodeRegistry.to_lookup`, `service/rewriter.py`) — two declarations differing only in case name one relation and must not mint two identities. Each node's `upstream_unique_ids` entries are derived the same way (`UpstreamDep.unique_id`), so a downstream reader comparing a node's own `unique_id` against another node's `upstream_unique_ids` never misses a match over a case difference. `node_type` is `dbt-model`, `dbt-seed`, or `dbt-snapshot` for a dbt node (dbt's resource type), or `python-model` for a python node. `test_count` is the number of dbt tests attached to the node: the parser's second pass walks every `resource_type: test` manifest node once and attributes it to `attached_node` when the manifest sets it (generic tests), else to each of `depends_on.nodes` that resolves to a tracked node (singular tests), counting each test at most once per target. `release-controller` carries `test_count` through unchanged onto `release.promoted:v1`, where `orchestrator` persists it as `:Table.test_count`.

`content_hash` is `"sha256:" + sha256(source_hash|shared_code_hash|config_hash)` — a fold of three independently-computed components, so a change to any one of them flips the whole fingerprint:
- `source_hash` is dbt's per-node source checksum (`checksum.checksum` from the manifest node); a node dbt did not checksum falls back to a sha256 of its `raw_code`/`compiled_code`, or failing that a stable JSON dump of the node, so it is never empty.
- `shared_code_hash` is a fold of the source checksums of every macro the node transitively depends on (resolved from the manifest's `macros` map via `depends_on.macros`, following macro→macro edges); `""` for a node with no macro dependencies.
- `config_hash` is a sha256 of the node's resolved `config`, minus the `meta`, `docs`, `description`, `grants`, and `tags` keys, so an out-of-file config change (a materialization or other setting from `dbt_project.yml` / `schema.yml`) flips the hash even when the node's own `.sql` file is untouched.

`release-controller` diffs `content_hash` against the prod snapshot to derive the changed-node set for the validation gate, detecting model SQL, seed CSV, snapshot, shared-macro, and config changes uniformly — a macro edit or a config change re-validates every node that depends on it even when those nodes' own `.sql` is untouched.

`candidate_artifact_uri` is an `s3://` URI pointing to the object a node's validation Job fetches to build that node — one kind-agnostic key for both runtimes; which shape the object has follows from the node's `node_type`. It is built and uploaded by a per-runtime artifact builder (`DbtSqlArtifactBuilder` or `PythonSpecArtifactBuilder`), selected by the node's `runtime`. For a dbt model or snapshot, the object is the node's `candidate_sql` (dbt's `compiled_code` for the node) uploaded to `candidate-sql/<release_id>/candidate_<unique_id>.sql`, with every schema-qualified reference that resolves to a known graph node rewritten (via sqlglot) to the candidate schema (`_candidate_<release>`); dbt seeds carry an empty URI because they have no SELECT to rewrite. For a python node, the object is a validation spec uploaded to `candidate-sql/<release_id>/candidate_<unique_id>.json` — `{reads, output_columns, config}` — where `reads` is the node's declared reads (from its contract, sorted by read name), each rewritten to the candidate schema the same way; `output_columns` and `config` are the contract's declared output shape and physical-layout config, carried through unchanged. Both rewrites leave a qualified self-reference on the production schema (the node's own table does not yet exist under the candidate schema at read time). An S3 upload failure for any node is fatal: the handler publishes `status=failed` (`error_class=CandidateArtifactUploadFailed`) and ACKs — no partial or dangling URI is ever emitted. `image_tag` is left empty — `release-controller` joins in the per-service image tags it assembled for the release.

`code_bundle_uri` is a top-level `s3://` URI pointing to a single code-bundle contract document for the whole release, uploaded to `code-bundles/<release_id>/bundle.json` (`contract_version: 1`). The document is `{contract_version, release_id, nodes, shared_code}`: `nodes` is keyed by the same `schema_name.table_name` unique_id and carries, per node, `runtime` (`"dbt"` or `"python"`), `raw_code`, `compiled_code` (the node's `candidate_sql`, i.e. its compiled SQL before any candidate-schema rewrite — empty for a python node, which has no compiled SQL), the node's resolved `config`, the three hash components (`source_hash`, `shared_code_hash`, `config_hash`), `content_hash`, and `code_unit_ids` (the node's direct macro dependency ids). For a dbt node, `raw_code` is dbt's own pre-compilation source; for a python node it is a deterministic pretty-printed JSON dump of the parsed contract entry (hash fields excluded) — the node's script itself never reaches manifest-controller, so this normalized entry is what the code bundle, and the remediation LLM reading it, see as the node's source. `shared_code` maps each referenced macro id to `{source, checksum, depends_on}`; a python contract carries no shared-code units, so a python-only release contributes nothing to this map. Every shared-code unit id — both `shared_code` map keys and each node's/unit's `code_unit_ids`/`depends_on` entries — is namespaced by service as `<service>:<dbt-macro-id>` (e.g. `service-a:macro.svc.m1`), using the manifest's declared service (falling back to the manifest's own node service when undeclared). This keeps two manifests that pin different versions of the same dbt package (and so ship a same-named macro with different source) from colliding on one bundle entry: each service's copy is recorded separately, and every node's `code_unit_ids` points at the copy its own manifest actually hashed against. Namespacing makes cross-manifest collisions on a unit id impossible by construction — per-node hashes (`shared_code_hash`/`content_hash`) are unaffected by this, since they fold the macro's source content within one manifest and never depend on the id string. The document is consumed by `orchestrator`, which reads it once per promoted release (carried forward as `release.promoted:v1`'s `code_bundle_uri`) to record node code-version history in the graph. It exists so that consumer never needs to parse a dbt manifest directly — manifest-controller stays the only component in the system that reads dbt's own artifact format. An empty-manifest release (no manifest files found) publishes `code_bundle_uri: ""` along with an empty topology. A release whose manifests *are* found but resolve to zero tracked nodes (e.g. an undeclared-service manifest, where the `EmptyManifest` check is skipped) still builds and uploads a bundle — `nodes: {}` — and publishes that bundle's real `code_bundle_uri` alongside the empty topology, rather than the empty-string sentinel. A code-bundle upload failure is fatal in the same way as a candidate-artifact upload failure: the handler publishes `status=failed` (`error_class=CodeBundleUploadFailed`) and ACKs; the bundle is built and uploaded only after every node's candidate artifact has uploaded successfully, so a bundle failure never leaves a partially-uploaded topology.

On failure: `{release_id, status: "failed", error_class, error_detail}`. `release-controller` uses this to transition a release from parsing to validating, or to mark it failed.

Calls no gRPC services.

## Processing Logic

### On `release.requested:v1`

```
Decode payload {release_id, manifest_keys}
For each entry: parse_s3_uri(s3_uri) → (bucket, key); strip trailing slash; carry entry.kind (default "dbt")
  All entries must share one bucket (otherwise error, not ACKed)
Build an S3 source scoped to that bucket + the explicit key list

list_manifests() downloads exactly the listed keys (no S3 listing)
  No keys → publish status=ok with empty topology, ACK

Pass 1 — Parse and validate against the declared service
  For each manifest file: parser_for(file.kind) selects the per-kind parser
    Unrecognized kind → publish status=failed (error_class=UnknownManifestKind), ACK
    kind=dbt: parse_manifest(path, version, image_tag) → (list[ManifestNode], shared_code)
      Malformed manifest — invalid JSON, a missing top-level `nodes` key, or an
        invalid node shape (missing `schema`/`fqn`, empty `fqn`) → publish
        status=failed (error_class=MalformedManifest), ACK
    kind=python: parse_python_contract(path, version, image_tag) → (list[ManifestNode], {})
      Malformed contract — invalid yaml, an unknown/missing top-level or entry
        field, an invalid entry shape, or a content_hash that does not equal
        the fold of its own source_hash/shared_code_hash/config_hash → publish
        status=failed (error_class=MalformedContract), ACK
    Both parsers produce the same ManifestNode shape, so every later pass is kind-blind.
  Each manifest key declares the service it belongs to (manifest_keys[].service).
  The manifest is validated against that declared service, for either kind:
    Zero nodes (dbt: "no model/seed nodes"; python: "declares no nodes") →
      publish status=failed (error_class=EmptyManifest), ACK
    Any node whose service_name differs from the declared service →
      publish status=failed (error_class=ServiceMismatch), ACK
  This rejects an empty or wrong-service upload before it can enter the candidate
  topology — without it, such a candidate would promote with the declared
  service's nodes missing and silently retire that service.

Pass 2 — Build registry (in memory only; no CSV persisted)
  Build lookup dict: (schema_name, table_name) → NodeRegistryEntry, from every
  parsed node regardless of kind

Pass 3 — Resolve deps, build each node's candidate artifact, upload to S3, and shape the topology
  For each node: resolve_upstream_deps(node, lookup, dialect=dialect) (sqlglot rules below;
    dependency_sqls holds compiled SQL for a dbt node, declared reads for a python node)
    UnqualifiedTableReferenceError → publish status=failed (error_class=UnqualifiedTableReference), ACK
    InvalidCompiledSqlError (a dependency SQL does not parse under the configured dialect) → publish status=failed (error_class=InvalidCompiledSql), ACK
  For each node: artifact_builders[node.runtime].build(node, ctx)
    No builder registered for the node's runtime → publish status=failed (error_class=UnsupportedRuntime), ACK
      (unreachable with a correctly wired composition root; a fail-closed guard, not a live path)
    dbt (DbtSqlArtifactBuilder): rewrite_to_candidate_schema(node.candidate_sql, lookup, candidate_schema, dialect=dialect),
      then upload the rewritten SQL to candidate-sql/<release_id>/candidate_<unique_id>.sql;
      seeds carry an empty candidate_sql (and empty dependency_sqls) → candidate_artifact_uri=""
    python (PythonSpecArtifactBuilder): rewrite each declared read the same way, then upload
      {reads, output_columns, config} to candidate-sql/<release_id>/candidate_<unique_id>.json
    Both rewrites: schema-qualified references whose (schema, table) pair is in the registry move
      to the candidate schema using sqlglot; CTE aliases, unqualified refs, tables not in the
      registry, and a qualified self-reference are all left unchanged
    → upload failure: publish status=failed (error_class=CandidateArtifactUploadFailed), ACK (fatal — no partial URI emitted)
    → success: candidate_artifact_uri = s3://<bucket>/candidate-sql/<release_id>/candidate_<unique_id>.<sql|json>
  Shape each node as {unique_id, schema_name, table_name, resolved_relation_id, service_name, node_type, test_count, content_hash, image_tag, original_file_path, upstream_unique_ids, schedule, candidate_artifact_uri}
    (resolved_relation_id = "<schema>.<resolved name>", lowercased, mirroring unique_id's own
     derivation; "resolved name" is a dbt node's alias when it overrides one, else its declared
     name — for a python node, always its declared table, since a contract has no alias concept.
     unique_id is keyed on the DECLARED name; this is keyed on the RESOLVED one, so release-controller's
     duplicate-relation gate can see two differently-named nodes that alias to the same physical
     table, which unique_id alone would miss)

Build the release's code bundle {contract_version, release_id, nodes, shared_code} and upload it to
  code-bundles/<release_id>/bundle.json
  → upload failure: publish status=failed (error_class=CodeBundleUploadFailed), ACK
  → success: code_bundle_uri = s3://<bucket>/code-bundles/<release_id>/bundle.json

Publish manifest.loaded.candidate:v1 status=ok with the topology and code_bundle_uri, ACK
```

The flow leaves `image_tag` empty by design (`release-controller` joins the per-service tags it assembled for the release onto the candidate topology); it builds its registry in memory and persists nothing; and it reports parse/resolve failures back as a `status=failed` business signal rather than failing silently.

Failure-handling distinction: a parse or resolve failure that re-delivery cannot fix (malformed manifest JSON or node shape, a malformed python contract, an unrecognized `kind`, an empty or wrong-service manifest, an unresolvable reference, compiled SQL that does not parse) is published as `status=failed` and the message is ACKed — replaying it would not help. A transient infrastructure failure (S3 read error, Redis publish error) propagates so the message is **not ACKed**; it stays in the group PEL and is retried by the reclaim sweep (see Consumer Reliability).

### Dependency resolution rules (sqlglot)

These rules apply uniformly to both kinds' `dependency_sqls` — a dbt node's compiled SQL and a python node's declared reads are resolved by the same `resolve_upstream_deps`, so a malformed python read fails the release exactly like malformed dbt compiled SQL.

| Case | Behavior |
|---|---|
| CTE alias | Skipped |
| Qualified self-reference | Skipped |
| Unqualified table reference | `UnqualifiedTableReferenceError` raised → node fails to load |
| Table not in registry | Skipped (external/source table) |
| Table in registry | Resolved as `UpstreamDep` |
| dbt seed reference | Resolved as `UpstreamDep` (seeds are registered in pass 2) |
| A dependency SQL does not parse under the configured engine's dialect (e.g. an un-suppressed Jinja expression leaking literal text, such as a trailing comma inside `{{ config(...) }}` rendering as `('',)`, or an unterminated string literal failing the tokenizer) | `InvalidCompiledSqlError` raised → node fails to load |

The dependency resolver parses each entry of `dependency_sqls`; the candidate-schema rewriter parses `candidate_sql` (dbt) or each declared read (python) and re-renders it. Both use the sqlglot dialect of the warehouse engine the install targets, so engine-specific syntax (e.g. Postgres' `ARRAY[...] @> ARRAY[...]`) resolves normally on that engine.

The engine comes from `WAREHOUSE_ENGINE`, which the chart sets on the shared ConfigMap from `validation.engine` (`postgres` by default). `main.py` resolves it to a dialect once at boot and injects it into `CandidateManifestHandler`, which passes it to both the resolver and the rewriter — so the SQL uploaded to S3 is in the dialect the validation runner will execute against. An engine with no dialect mapping fails `config.validate()` at startup rather than silently emitting another engine's SQL: sqlglot's `postgres` dialect renders a cast as `CAST(x AS TEXT)`, which Trino rejects.

## S3 Behavior

| Operation | Description |
|---|---|
| `download_file` | Download each artifact (dbt `manifest.json` or python `contract.yaml`) named in `manifest_keys` to a temp path; no S3 listing is performed |
| `PutObject` | Upload the rewritten candidate SQL for each non-seed dbt node to `candidate-sql/<release_id>/candidate_<unique_id>.sql`; failure is fatal and aborts the load |
| `PutObject` | Upload the validation spec for each python node to `candidate-sql/<release_id>/candidate_<unique_id>.json`; failure is fatal and aborts the load |
| `PutObject` | Upload one code-bundle contract document per release to `code-bundles/<release_id>/bundle.json`; failure is fatal and aborts the load |

## Consumer Reliability

- The `release.requested:v1` consumer runs on a daemon thread with a connection-pooled `redis` client and maintains its own consumer-group offset.
- Consumer group is created with `id="0"` (reads from the beginning on first create; `BUSYGROUP` error on re-create is ignored)
- The initial group creation waits out a Redis it cannot reach yet, retrying every 3 seconds for up to 60 seconds before giving up. A cold start can beat its own Redis — on Kubernetes the Service DNS name may not resolve, under compose the server may not be accepting connections — and letting that error end the process costs more than the wait, since CrashLoopBackOff then holds the pod down on an exponential delay well past the point Redis is ready. The wait is bounded rather than infinite because the health server only starts once the consumer is constructed: waiting forever on a genuinely unreachable Redis would leave a process that serves no probe and consumes nothing, with nothing to signal it. Only connection and timeout failures are retried; any other error is permanent and raised on the first attempt
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
- An S3 upload failure in pass 3 is also fatal: it is reported as `status=failed` and ACKed. No partial URI is ever emitted — either every node's candidate artifact (SQL or spec) is uploaded and the topology carries complete `candidate_artifact_uri` values, or the entire load is aborted.
- A code-bundle upload failure is likewise fatal, for the same reason: it is reported as `status=failed` (`error_class=CodeBundleUploadFailed`) and ACKed rather than published with a dangling `code_bundle_uri`. The bundle upload runs only after every node's candidate artifact has already uploaded successfully.
- Releases enter through `release-controller`'s `POST /releases`, which emits `release.requested:v1` for this service to parse.
