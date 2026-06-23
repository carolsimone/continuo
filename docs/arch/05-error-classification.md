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
| `executor-controller/adapters/k8s/client.go:177` (`buildPodSpec`) | `image_tag` missing on dispatch | `fmt.Errorf("%w: image_tag missing from job params for service %s", events.ErrPermanent, params.ServiceName)` |
| `state/adapters/redis/*_binding.go` and `orchestrator/adapters/redis/*_parser.go` (each parser/binding) | per-stream parser returns a parse/validation failure (malformed payload, missing required field, bad UUID, unknown enum value, cross-field rule violation) | the parser returns `fmt.Errorf("%w: <reason>", events.ErrPermanent, …)` and the binding propagates it (logs ERROR + returns the error) so the consumer ACKs and drops the poison message |

Add new emitters to this table as they land.

## Who recognises it

| Site | Behaviour on `errors.Is(err, events.ErrPermanent)` |
|---|---|
| `pkg/redis/streamconsumer.go` (`readAndProcess` and `reclaimPending`) | log ERROR, ACK, continue — drops the message from the PEL under both first-delivery AND periodic reclaim. **Read path** retries plain (non-`ErrPermanent`) errors inline via `invokeWithRetry` (backoff schedule `0 / 100ms`, one quick retry then park, ~100ms budget); if both attempts fail the message is left un-ACKed in the PEL and the reclaim sweep redelivers it. Keeping the inline budget short stops one transiently-failing message from head-of-line-blocking its lane for seconds. **Reclaim path** is single-shot: the handler is called exactly once per claimed entry, and on failure the entry stays in the PEL — the next periodic sweep (every `reclaimInterval`) is the retry cadence. This split keeps the read loop responsive when a large backlog of pending entries is being swept. The sweep itself uses `XAUTOCLAIM` (cursor-paged, one round-trip per 100 entries) gated by a `MinIdle` threshold (default 30s, overridable via `WithReclaimMinIdle`) so a multi-replica deployment cannot have one replica steal an in-flight message that a peer is actively retrying. Successful and `ErrPermanent`-dropped IDs are ACKed in one pipelined `XAck` per read batch / reclaim page (ack-after-success unchanged — only resolved IDs are batched). `ErrPermanent` ACKs immediately on either path. Both paths invoke the handler through `safeInvoke`, which recovers any handler panic and converts it into a plain (non-`ErrPermanent`) error — a panicking handler can no longer unwind through the loop and crash the process, and the message is left in the PEL for the next sweep just like any transient failure. Used by every Go service's Redis ingest path. |
| `executor-controller/service/deployer/dispatcher.go` (`dispatchRow`) | call `writeFailed` (writes `task.status.updated:v1` FAILED + `node.updated:v1` FAILED as `executor_outbox` rows, marks `executor_deployments` row `failed`) |

## When NOT to use it

- **Transient errors** (network, timeout, k8s API quota, redis blip).
  These must keep the existing no-ACK + retry behaviour. On the **read
  path** the consumer makes one quick inline retry (~100ms budget); if
  both attempts fail the message stays in the PEL. On
  the **reclaim path** the handler is single-shot — successive periodic
  sweeps are the retry cadence, so a flapping handler does not
  head-of-line-block the read loop during a sweep.
- **Errors where the cause is unknown.** Wrap only when you're sure
  retry will never succeed.
- **Programmer errors** (panics, nil pointer derefs). Let the runtime
  handle those — wrapping with a sentinel hides the stack trace.

## Remediation service: dbt failure taxonomy

The `remediation` service applies a separate, domain-level classification to failed dbt nodes (distinct from the `ErrPermanent` sentinel above, which governs message-consumer routing). Its classifier (`remediation/domain/failure/classify.go`) deterministically sorts each failure into one of four categories:

| Category | Routing decision | Signals |
|---|---|---|
| `infra_transient` | `drop` (not emitted) | Exactly four families: connection refused / could not connect to database; OOMKilled; ImagePullBackOff / back-off pulling image; InvalidAccessKeyId / AccessDenied (S3 credentials). |
| `test` | `emit` | dbt test assertion failures. |
| `logic` | `emit` | SQL/model defects (missing relation, compilation error, syntax error, missing ref, type mismatch, ambiguous column). |
| `unknown` | `emit` | Everything else, including the ambiguous resource/permission class (statement timeout, permission denied, deadlock, out-of-memory) and an unreachable log. |

**Under-drop policy**: only the four confidently-infra signal families are dropped. Ambiguous cases — signals that could be either infrastructure or a model problem — fall through to `unknown` and are emitted. Uncertainty flows to the heal agent; only confident infrastructure failures are silenced.

The `remediation` consumer does use `ErrPermanent` at the transport layer: a malformed `release.rejected:v1` payload is wrapped with `ErrPermanent` and ACKed (dropped from the PEL); a transient S3 fetch error is not wrapped and causes the message to stay in the PEL for retry.

## See also

- `docs/arch/03-sequence-flows.md` §3 (permanent fast-path note in the
  retry/terminal flow) and §8 (dispatch watchdog termination).
- `docs/arch/04-service-ownership.md` (per-service invariants under
  orchestrator, executor-controller, and manifest-controller).
- `docs/arch/services/remediation.md` (full remediation service documentation).
