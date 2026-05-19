# Error Classification

> **Status:** load-bearing — describes the contract by which services
> distinguish permanent (non-retryable) failures from transient ones.

## `pkg/events.ErrPermanent`

Single sentinel `var ErrPermanent = errors.New("permanent: retry will not help")`
in `pkg/events/errors.go`. Handlers wrap it via
`fmt.Errorf("%w: <reason>", events.ErrPermanent)` when input is
deterministically bad — validation failures, structurally corrupt data,
schema mismatches. Consumers and outbox processors recognise it via
`errors.Is`.

The wrap chain is preserved through `errors.Join` (verified by
`pkg/events/errors_test.go::TestErrPermanent_RecognisedInsideJoinedError`),
so handlers can aggregate a transient I/O error with a permanent
validation error and the recogniser still classifies the result as
permanent.

## Who emits it

| Site | When | Wrapping idiom |
|---|---|---|
| `orchestrator/service/handlers/ingest_topology.go` (`validateTopologyNodes`) | any node has empty `image_tag` | `fmt.Errorf("%w: image_tag empty for N node(s): ...", events.ErrPermanent, ...)` |
| `executor-controller/adapters/k8s/client.go:177` (`buildPodSpec`) | `image_tag` missing on dispatch | `fmt.Errorf("%w: image_tag missing from job params for service %s", events.ErrPermanent, params.ServiceName)` |
| `state/adapters/redis/*_binding.go` (each binding) | per-stream parser returns a parse/validation failure (malformed payload, missing required field, bad UUID, unknown enum value) | `fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)` after the binding's parser returns an error |

The Python counterpart (`manifest-controller/service/validators.py`)
raises `ManifestValidationError` instead of wrapping a sentinel — Python
doesn't share the Go sentinel by design — but the message format is kept
symmetric (`image_tag empty for N node(s): svc/schema/table, ..., ...and N more`)
so logs across the two layers are visually consistent.

Add new emitters to this table as they land.

## Who recognises it

| Site | Behaviour on `errors.Is(err, events.ErrPermanent)` |
|---|---|
| `pkg/redis/streamconsumer.go` (`readAndProcess` and `reclaimPending`) | log ERROR, ACK, continue — drops the message from the PEL under both first-delivery AND periodic reclaim. **Read path** retries plain (non-`ErrPermanent`) errors inline via `invokeWithRetry` (backoff schedule `0 / 100ms / 500ms / 2s`, ~2.6s budget); if every attempt fails the message is left un-ACKed in the PEL. **Reclaim path** is single-shot: the handler is called exactly once per claimed entry, and on failure the entry stays in the PEL — the next periodic sweep (every `reclaimInterval`) is the retry cadence. This split keeps the read loop responsive when a large backlog of pending entries is being swept. The sweep itself uses `XAUTOCLAIM` (cursor-paged, one round-trip per 100 entries) gated by a `MinIdle` threshold (default 30s, overridable via `WithReclaimMinIdle`) so a multi-replica deployment cannot have one replica steal an in-flight message that a peer is actively retrying. `ErrPermanent` ACKs immediately on either path. Used by every Go service's Redis ingest path. |
| `executor-controller/service/handlers/outbox_processor.go` (`processEntry`) | call `MarkTaskTerminallyFailed` (publishes `task.status.updated:v1` FAILED + `node.updated:v1` FAILED + marks outbox failed), return `errPermanentFailure` so `ProcessBatch` skips retry-increment |

The local `errPermanentFailure` sentinel in `outbox_processor.go` is
**flow control** between `processEntry` and `ProcessBatch` — it tells
`ProcessBatch` that `MarkFailed` has already been called and the retry
counter must not be incremented. It is unrelated to `events.ErrPermanent`,
which is the cross-service classification key.

## When NOT to use it

- **Transient errors** (network, timeout, k8s API quota, redis blip).
  These must keep the existing no-ACK + retry behaviour. On the **read
  path** the consumer retries them inline within a bounded backoff budget
  (~2.6s); if that budget is exhausted the message stays in the PEL. On
  the **reclaim path** the handler is single-shot — successive periodic
  sweeps are the retry cadence, so a flapping handler does not
  head-of-line-block the read loop during a sweep.
- **Errors where the cause is unknown.** Wrap only when you're sure
  retry will never succeed.
- **Programmer errors** (panics, nil pointer derefs). Let the runtime
  handle those — wrapping with a sentinel hides the stack trace.

## Forensics

When the orchestrator's `IngestTopologyHandler` rejects a payload, it
writes a row to `rejected_topology_messages` (Postgres, V7 migration)
with `message_id`, `reason`, and the raw `payload` JSON. The insert is
non-transactional and best-effort: a failed insert MUST NOT turn a
permanent error into a transient one (that would loop the message in
the Redis pending list under XCLAIM redelivery). See `docs/arch/02-interaction-matrix.md`
for the local durable state inventory.

The manifest-controller logs structured `event=manifest_publish_rejected`
at ERROR with `missing_image_tag_count`, `total_node_count`, and
`offenders` — no Postgres sink (manifest-controller owns no durable
state by design).

## See also

- `docs/arch/03-sequence-flows.md` §3 (permanent fast-path note in the
  retry/terminal flow) and §8 (dispatch watchdog termination).
- `docs/arch/04-service-ownership.md` (per-service invariants under
  orchestrator, executor-controller, and manifest-controller).
- `docs/arch/02-interaction-matrix.md` (ACK-on-permanent annotations on
  `update.graph:v1` and `manifest.loaded:v1`).
