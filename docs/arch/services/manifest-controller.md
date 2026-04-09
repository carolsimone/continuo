# manifest-controller

## Purpose

`manifest-controller` is the topology ingestion service. It loads dbt `manifest.json` files into the graph and tells `state` which schedule names currently exist.

It is the only service that writes topology to `graph` (`CreateNode`).

**Runtime**: Python 3.12 / uv. Not a Go service.

## Owned Storage

None. Does not own Postgres, Neo4j, or S3.

An intermediate registry CSV is written to the local filesystem during processing (via `RegistryRepository`) and used within the same request to resolve cross-service dependencies. It does not persist across restarts as a source of truth.

## Inbound Interfaces

### Redis consumer

| Stream | Group | Description |
|---|---|---|
| `update.graph:v1` | `manifest-controller` | Triggers a full manifest load; consumed as a consumer group (`xreadgroup`) |

Required message field:
- `source`: `"local"` or `"s3"` — determines manifest source

Unknown `source` values are treated as errors; the message is **not ACKed** and will be replayed.

### HTTP

None (no HTTP interface; runs as `tail -f /dev/null` in dev; started manually or via `start-services.sh`).

## Outbound Interfaces

### Redis producer

| Stream | Description |
|---|---|
| `schedules.loaded:v1` | Published after every successful manifest load; consumed by `state` |

`schedules.loaded:v1` is a single Redis field `payload` containing JSON:
- `event_id` — UUID generated per load
- `schedule_names` — deduplicated list of `schedule_name` values across all loaded nodes
- `manifest_versions` — `{ service_name: manifest_version }` map (last version per service wins)

> **Empty schedule list**: if no manifests are found or no nodes have a `schedule_name`, an empty list is published. `state`'s `SoftDeleteAbsent([])` will soft-delete **all** active catalog entries. The manifest source must always return nodes with schedule names populated.

### gRPC to `graph`

| Method | Description |
|---|---|
| `CreateNode` | One call per node; upserts `Table` node and rewrites `DEPENDS_ON` edges |

Calls no other gRPC services.

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
  Build lookup dict: (schema, table) → NodeRegistryEntry

Pass 3 — Resolve deps + load
  For each node in all_nodes:
    resolve_upstream_deps(node, lookup):
      - parse compiled_sql via sqlglot
      - skip CTEs
      - skip qualified self-references
      - raise UnqualifiedTableReferenceError on unqualified references
      - skip tables not in registry (external/source tables)
      - dbt seeds ARE in registry → seed refs resolve as upstream deps
    graph_client.create_node(node)  ← gRPC CreateNode
    collect manifest_versions[service_name] = manifest_version

Collect schedule_names = deduplicated {n.schedule_name for n in all_nodes if n.schedule_name}
Publish schedules.loaded:v1

ACK message
```

On any exception: message is **not ACKed** → replayed on next poll.

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
| `list_objects_v2` | List manifest files in configured S3 bucket/prefix |
| `download_file` | Download each manifest JSON to a temp path |

No S3 writes.

## Consumer Reliability

- Consumer group is created with `id="0"` (reads from the beginning on first create; `BUSYGROUP` error on re-create is ignored)
- Consumer name is `consumer-{random_hex}` (unique per process restart)
- `NOGROUP` error recovery: consumer group is recreated and the loop retries after 3 seconds
- Message is ACKed only after full successful handle; failures leave the message in the PEL for redelivery

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer loop | Blocking `xreadgroup` (1s block, batch of 10); dispatches to `ManifestHandler` |

## gRPC Callers

None — manifest-controller is not called via gRPC by any service.

## Reliability Notes

- No local outbox — if the gRPC calls to `graph` succeed but Redis publish of `schedules.loaded:v1` fails, the message is not ACKed and the entire load is replayed. `CreateNode` is idempotent (upsert).
- Per-node failures in pass 3 are logged and counted but do not abort the load — other nodes continue. The failed node will not appear in the graph.
- `update.graph:v1` producer is external to the repo in production (typically a CI/deploy pipeline or e2e test trigger).
