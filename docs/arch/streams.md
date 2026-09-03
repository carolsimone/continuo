# Streams Contract

The Redis Stream names and consumer-group names used across continuo services
are declared in a single YAML file: `pkg/streams/contract.yaml`.

A Go generator at `pkg/streams/cmd/gen-streams/` reads this file and produces:

- `pkg/streams/streams.gen.go` — Go constants for every stream and group.
- `pkg/streams/streams_test_access.gen.go` — Test-only accessor used by the
  contract integrity test.
- `topology-controller/streams_contract.py` — Python constants for the
  topology-controller service (filtered to streams it produces or consumes).

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
- **Python (topology-controller)**: `from streams_contract import UPDATE_GRAPH_V1, MANIFEST_UPDATE_GRAPH`.
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

## Notable stream payload conventions

A few streams carry a `stage` discriminator that changes how consumers process
the message. These are documented here as cross-cutting facts; full payload
schemas live in the service dossiers.

**`release.requested:v1`** — emitted by release-controller once a release is
ready to parse: a `dbt` release emits it on transitioning from Compiling to
Parsing (`compile.completed:v1` ok path); a `python` release has no compile
leg, so it emits it directly on activating from Received to Parsing. The
payload is `{release_id, manifest_keys}`; each `manifest_keys` entry is
`{service, s3_uri, kind}`, with `kind` (`"dbt"` | `"python"`) explicit on
every entry — release-controller always sets it, never omits it — selecting
which of topology-controller's two parsers reads that entry's artifact: a dbt
`manifest.json` or a python service's `contract.yaml`. topology-controller's
decoder tolerates an absent `kind` by defaulting to `"dbt"` regardless, so a
future or third-party producer that omits the field still parses. See
`docs/arch/services/topology-controller.md` for the per-kind parse and
failure behavior.

**`release.rejected:v1`** — **candidate-only**: emitted by release-controller for every
terminal rejection of a candidate release, regardless of which leg failed. A
fix-verification run's failure never rides this stream, whatever caused it —
its only announcement is `pipeline.run.finished:v1` (below). The payload always includes:
`release_id`, `stage` (`compile` | `seed_build` | `validation`; absent for parse-phase rejections), `reason`,
`repo`, `commit_sha`, `code_bundle_uri`, `failing_nodes`, and `per_node[]` (each entry: `node_id`,
`status`, `dbt_log_uri`, optional `run_results_uri`). Validation entries
additionally carry `candidate_artifact_uri` plus the candidate topology's
`node_type`, `file_path`, and `service` for that node; seed_build entries carry
`file_path`/`service` from the same source, and the payload carries
`candidate_schema`.
Consumers must not assume `stage` is always `validation`
— all three legs reuse this single stream. The remediation classifier
(group `remediation-release-rejected`) triages the failing set;
executor-controller (group `executor-release-rejected`) consumes it too, as an
idempotent candidate-schema teardown backstop, dropping `candidate_schema` when
the payload carries one and it is not already reclaimed by the
`pipeline.run.finished:v1` or `validation.result:v1` path.
`release.promoted:v1` has the same executor teardown backstop
(group `executor-release-promoted`). See `docs/arch/services/release-controller.md`
for the full per-leg payload shape.

**`pipeline.run.finished:v1`** — emitted by release-controller for every terminal
status of every pipeline run, of either kind: `promoted` | `rejected` | `superseded`
for a candidate release, `passed` | `failed` for a fix-verification run. The payload
is `{run_id, run_kind, outcome, service, candidate_schema, verifies_release_id,
attempt, finished_at}`; every field is always present regardless of kind — a
candidate carries an empty `verifies_release_id` and a zero `attempt` rather than
omitting the keys, so one consumer decodes the same shape whichever kind ended.
`candidate_schema` is always named, so the sole consumer — executor-controller
(group `executor-pipeline-run-finished`) — can drop that schema on receipt
whatever the outcome; a drop of a schema already gone (the `validation.result:v1`
teardown got there first) is a no-op. See `docs/arch/services/release-controller.md`
and `docs/arch/services/executor-controller.md` for the full behavior.

**`remediation.retry_requested:v1`** — emitted by release-controller when a
human asks a rejected release to "try again" (`POST
/releases/{id}/retry-remediation`). The payload is the release's own stored
`release.rejected:v1` payload — release-controller keeps the exact bytes it
emitted at the healable rejection on the `releases.rejection_payload` column
— replayed verbatim with one field added: the incremented
`remediation_round`. `remediation` consumes it on its own consumer group
(`remediation-retry-requested`) through the same handler that reads
`release.rejected:v1`, since the two payloads share one shape; classifying it
re-runs the identical per-node triage one round later. The request that
produces this event is refused before it is ever published unless the
release is `rejected`, its stored reason is healable (`compile_failed`,
`seed_build_failed`, `validation_failed`, or `duplicate_table`), it has a
stored rejection payload at all (a release rejected before this column
existed, or rejected for the non-healable reason `parse_failed`, has none), its round is below the cap
(`MaxRemediationRounds = 3`), and agent-remediation's `ListProposals` reports
no attempt still in flight, proposed, or already carrying an
opening/open/merged PR for the release. See
`docs/arch/services/release-controller.md` (`RetryRemediation`) and
`docs/arch/services/remediation.md` for the full behavior.

**`remediation.requested:v2`** — emitted by remediation **once per rejected
release per remediation round**, carrying every healable failing node of that
rejection in a `nodes[]` array. The release, not the node, is the unit of
remediation: one trigger becomes one fix attempt, one proposal, and one pull
request downstream. Release-level fields (`source`, `release_id`,
`remediation_round`, `repo`, `commit_sha`, `code_bundle_uri`,
`classified_at`) sit at the top level; each `nodes[]` entry carries its own
`category`, `error_signature`, `reason`, `error_excerpt` (the classifier's key
error line, capped at 4 KiB), `dbt_log_uri`, `candidate_artifact_uri`,
`file_path`/`service`/`node_type`, the duplicate-relation fields, and
`changed_ancestors` (each `{node_id, file_path, service}`). The payload stays pointer-first — the full log lives
behind each node's `dbt_log_uri` and the failing code behind
`code_bundle_uri` (threaded from `release.rejected:v1`'s top-level
`code_bundle_uri`; empty for compile-stage rejections, which precede the parse
that produces the bundle) — so the orchestrator's failure-precedent case base
can record every rejection in the batch without pulling the full log or code
inline. `agent-remediation` decodes all of it: `reason`, together with
`category`, is its fallback precedent-lookup key (`GetPrecedents`) when the
exact `error_signature` has no recorded match; `code_bundle_uri` is the
validation fixer's primary source for a failing node's real code (falling back
to a GitHub repo read only on a permanent bundle miss); and
`changed_ancestors` is what lets it group same-signature failures that
descend from one changed ancestor and repair that ancestor once — and each
entry's `file_path`/`service` are the location the REJECTED release's candidate
declares for the ancestor, which is the file the fix edits: an ancestor this
release renamed or moved still sits at its old path in the promoted graph. `file_path`
is derived from the dbt log for `compile` sources and threaded from the
candidate topology for `seed_build`, `validation`, and `duplicate_table`; an
absent one falls back to the orchestrator's `GetNodeLocation` RPC. See
`docs/arch/services/remediation.md` for the full payload shape.

**`outbox.dead_letter:v1`** — emitted by every outbox-owning service's
`pkg/outbox.Processor` for a terminal outbox row: a permanent payload error,
or a transient error whose retry budget was exhausted. Producers: `state`,
`orchestrator`, `executor-controller`, `k8s-controller`, `release-controller`,
`remediation`, `agent-remediation`. The payload carries
`original_event_type`, `original_stream`, `original_aggregate_id`,
`failure_kind` (`permanent` | `transient_exhausted`), `error`, `attempts`,
`failed_outbox_id`. It is an operational DLQ signal, distinct from domain
`<event>.failed:v1` compensation events; no consumer is wired to it today. See
`docs/arch/05-error-classification.md` §Outbox processor resilience for the
classification and backoff mechanics.

**`remediation.pr_closed:v1`** — emitted by agent-remediation when its
PR-outcome reconciler observes a terminal GitHub pull request state (merged,
or closed without merge) for a proposal whose `pr_state` is `open`; the CAS
`open → merged | rejected` and this outbox row commit in the same transaction.
The payload is pointer-only: `proposal_id`, `release_id`, `node_id`,
`resolved_node_ids` (every failing node the PR addressed, sorted; `node_id` is
that set's representative), `service`, `pr_url`, `pr_number`, `outcome`
(`merged` or `rejected`), `closed_at`, and, on a merged outcome, `edits[]`
(each `{path, target_node_id, amended, diff}` — whether a human amended that
edit before merge, and the amendment diff). `event_id` is a deterministic SHA1
UUID derived from `(release_id, attempt, service)` — one owning-service PR of a
split proposal per id, with the legacy service `""` reproducing the pre-split
`(release_id, attempt)` id byte-for-byte — under a namespace distinct from the
`remediation.pr_opened:v1` id, so the two events never collide. Orchestrator's
case-base provenance consumer (group
`orchestrator-remediation-pr-closed-provenance`) records the resolution: it
stamps the `:PullRequest`'s terminal `pr_state`/`closed_at`, and for a merged
outcome draws `[:RESOLVED_BY {amended, service}]` from every resolved node's
`:Rejection` to the shared `:Proposal` and `[:EDITED {path, amended, diff,
service}]` from that `:Proposal` to each edit's `:Table`. See
`docs/arch/services/agent-remediation.md` for the full payload shape.

## Out of scope

- Stream payload schemas — service dossiers document them; see `docs/arch/services/`.
- Per-environment overrides for stream or group names — names are code-level
  identifiers, not deployment-tunable.

## Operator migration

When a stream or consumer group is renamed, any pre-existing Redis instance
carries the old group with potentially pending entries. To clean up:

```bash
REDIS_URL=redis://:continuo@<host>:6379 ./scripts/migrate-consumer-groups.sh
```

The script issues `XGROUP DESTROY` for each deprecated group. Idempotent.
