# Streams Contract

The Redis Stream names and consumer-group names used across continuo services
are declared in a single YAML file: `pkg/streams/contract.yaml`.

A Go generator at `pkg/streams/cmd/gen-streams/` reads this file and produces:

- `pkg/streams/streams.gen.go` — Go constants for every stream and group.
- `pkg/streams/streams_test_access.gen.go` — Test-only accessor used by the
  contract integrity test.
- `manifest-controller/streams_contract.py` — Python constants for the
  manifest-controller service (filtered to streams it produces or consumes).

## Naming policy

- **Stream name**: lowercase, dot-separated, ending with `:vN`. Example:
  `node.updated:v1`. The version is part of the identity; a v2 stream is a
  separate entry.
- **Consumer group name**: lowercase kebab-case, no underscores. Example:
  `orchestrator-node-updated`. The format is `<service>-<topic>`.
- **Go stream constant**: PascalCase including version suffix. Example:
  `NodeUpdatedV1`.
- **Go group constant**: PascalCase, service-prefixed. Example:
  `OrchestratorNodeUpdated`.
- **Python constants**: `SCREAMING_SNAKE_CASE` of the Go identifier.

## Adding a stream or consumer group

1. Edit `pkg/streams/contract.yaml`. Add a new `streams:` entry with `name`,
   `const`, `description`, `producers`, and any `consumers`.
2. Run `go generate ./pkg/streams/...` from the repo root.
3. Commit the YAML and all regenerated files together.

CI fails if `go generate` would produce a diff that isn't committed.

## Service usage

- **Go**: `import streams "github.com/carolsimone/continuo/pkg/streams"`, then
  reference constants directly: `streams.NodeUpdatedV1`,
  `streams.OrchestratorNodeUpdated`.
- **Python (manifest-controller)**: `from streams_contract import UPDATE_GRAPH_V1, MANIFEST_UPDATE_GRAPH`.
- Service `Config` structs do not carry stream or group fields. Stream and
  group names are passed to `pkg/redis.NewStreamConsumer` at the wiring site in
  `main.go`, so the constant only appears there.

## Consumer identity and throughput

`pkg/redis.NewStreamConsumer` derives a **stable per-pod consumer name**
(`<group>-<hostname>`, falling back to a time-seeded name only when the
hostname is unavailable). Because the name is stable across restarts, a
restarted process re-attaches to its own pending-entry list (PEL) instead of
minting a fresh consumer every boot — the consumer-group registry no longer
grows an orphaned entry per restart. A dead pod's PEL is still recovered by the
`XAUTOCLAIM` reclaim sweep (see `docs/arch/05-error-classification.md`).

Throughput within a single consumer is tuned by `WithWorkerPool(n,
aggregateKeyField)`:

- **Default `n = 1`** — strictly-serial processing, identical to the original
  behaviour. Every binding opts into parallelism deliberately.
- **`n > 1`** — each read batch is sharded across `n` lanes by a hash of the
  message's `aggregateKeyField` value (for example `schedule_id`). All messages
  for one aggregate land on the same lane, so per-aggregate ordering is
  preserved (same key → same lane → FIFO) while distinct aggregates process in
  parallel. A message missing that field hashes to a single stable lane, so it
  is never reordered; it just forgoes parallelism. The full batch completes
  before the next read, so ack-after-success and PEL semantics are unchanged.
  Sizing note: per-aggregate `SELECT … FOR UPDATE` serialization in the write
  store means `n > 1` only adds throughput across aggregates, so size `n`
  against concurrently-active aggregates rather than raw message rate.

## Out of scope

- Stream payload schemas — protobuf owns these.
- Per-environment overrides for stream or group names — names are code-level
  identifiers, not deployment-tunable.

## Operator migration

When a stream or consumer group is renamed, any pre-existing Redis instance
carries the old group with potentially pending entries. To clean up:

```bash
REDIS_URL=redis://:continuo@<host>:6379 ./scripts/migrate-consumer-groups.sh
```

The script issues `XGROUP DESTROY` for each deprecated group. Idempotent.
