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
| `pkg/redis/streamconsumer.go` (`readAndProcess` and `reclaimPending`) | log ERROR, ACK, continue — drops the message from the PEL under both first-delivery AND periodic reclaim. Plain (non-`ErrPermanent`) errors are left in the PEL so the reclaim ticker retries them. Used by every Go service's Redis ingest path. |
| `executor-controller/adapters/publisher/outbox_publisher.go` (`Publish`) | wraps the invalid-payload error with `errors.Join(err, pkgevents.ErrPermanent)` so `pkg/outbox.Processor` exhausts the retry budget immediately and invokes `TerminalFailureHook` (publishes `task.status.updated:v1` FAILED + `node.updated:v1` FAILED), then calls `MarkFailed` |

## When NOT to use it

- **Transient errors** (network, timeout, k8s API quota, redis blip).
  These must keep the existing no-ACK + retry behaviour so the system
  self-heals at the next reclaim cycle.
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
