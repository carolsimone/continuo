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

## Out of scope

- Stream payload schemas — protobuf owns these.
- Consumer names within a group — runtime-generated, not in the registry.
- Per-environment overrides for stream or group names — names are code-level
  identifiers, not deployment-tunable.

## Operator migration

When a stream or consumer group is renamed, any pre-existing Redis instance
carries the old group with potentially pending entries. To clean up:

```bash
REDIS_URL=redis://:continuo@<host>:6379 ./scripts/migrate-consumer-groups.sh
```

The script issues `XGROUP DESTROY` for each deprecated group. Idempotent.
