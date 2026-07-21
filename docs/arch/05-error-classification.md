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
| `pkg/redis/streamconsumer.go` (`readAndProcess` and `reclaimPending`) | log ERROR, ACK, continue — drops the message from the PEL under both first-delivery AND periodic reclaim. **Read path** retries plain (non-`ErrPermanent`) errors inline via `invokeWithRetry` (backoff schedule `0 / 100ms`, one quick retry then park, ~100ms budget); if both attempts fail the message is left un-ACKed in the PEL and the reclaim sweep redelivers it. Keeping the inline budget short stops one transiently-failing message from head-of-line-blocking its lane for seconds. **Reclaim path** is single-shot: the handler is called exactly once per claimed entry, and on failure the entry stays in the PEL — the next periodic sweep (every `reclaimInterval`) is the retry cadence. This split keeps the read loop responsive when a large backlog of pending entries is being swept. The sweep itself uses `XAUTOCLAIM` (cursor-paged, one round-trip per 100 entries) gated by a `MinIdle` threshold (default 30s, overridable via `WithReclaimMinIdle`) so a multi-replica deployment cannot have one replica steal an in-flight message that a peer is actively retrying. Successful and `ErrPermanent`-dropped IDs are ACKed in one pipelined `XAck` per read batch / reclaim page (ack-after-success unchanged — only resolved IDs are batched). `ErrPermanent` ACKs immediately on either path. Both paths invoke the handler through `safeInvoke`, which recovers any handler panic and converts it into a plain (non-`ErrPermanent`) error — a panicking handler can no longer unwind through the loop and crash the process, and the message is left in the PEL for the next sweep just like any transient failure. Beyond `ErrPermanent`, the consumer also classifies startup consumer-group bootstrap errors, quarantines poison messages past a delivery bound, and carries a per-handler timeout plus a liveness heartbeat — see **Stream consumer resilience** below. Used by every Go service's Redis ingest path. |
| `executor-controller/service/deployer/dispatcher.go` (`dispatchRow`) | call `writeFailed` (writes `task.status.updated:v1` FAILED + `node.updated:v1` FAILED as `executor_outbox` rows, marks `executor_deployments` row `failed`) |

## Stream consumer resilience

Beyond the `ErrPermanent` routing above, `pkg/redis/streamconsumer.go` layers
three consumer-resilience behaviours that also bear on failure handling:

- **Startup consumer-group bootstrap classification.** `Start` creates the
  consumer group in a retry loop rather than returning on the first failure, so
  a Redis outage overlapping process start does not permanently kill the
  consumer. The bootstrap error is classified: a **permanent** bootstrap error —
  a Redis reply-error code of `WRONGTYPE` (the stream key exists as a non-stream
  type) or `unknown command` / `unknown subcommand` (the server does not
  implement `XGROUP`) — makes `Start` return the error, which the caller records
  via `liveness.Registry.WorkerExited`, so it surfaces on the readiness endpoint
  and is logged loudly instead of retrying forever behind a green health check.
  Everything else is retried with backoff: every network/connection error and
  every transient server state (connection refused, i/o timeout, `LOADING`,
  `CLUSTERDOWN`), and the auth-class codes `WRONGPASS` / `NOAUTH` / `NOPERM`
  (which can be a password-rotation or ACL-propagation race, so restarting the
  fleet on them is wrong). Classification uses `goredis.HasErrorPrefix` against
  the RESP reply-error code, so a wrapped error or a non-Redis error whose text
  merely contains a token is never misclassified.

- **Poison-message quarantine (reclaim/PEL path).** A message that keeps failing
  transiently — a handler bug, a payload the handler can never process, or a
  handler that repeatedly exceeds its timeout (`context.DeadlineExceeded`, which
  is **not** `ErrPermanent`) — would otherwise cycle in the PEL forever, never
  ACKed. The reclaim path reads the PEL delivery counter (`XPendingExt`
  `RetryCount`) and, once a message has been redelivered past `maxDeliveries`
  (5), ACK-drops it with a loud ERROR log — the same visibility and effect as an
  `ErrPermanent` drop. This is the general safety net for any repeatedly-failing
  message, not only timeouts.

- **Per-handler timeout + liveness heartbeat.** Each handler invocation runs
  under a bounded context deadline (`SetHandlerTimeout` / `WithHandlerTimeout`),
  so a hung handler eventually returns control to the loop; a timeout logs
  distinctly from a generic transient failure. The consumer maintains a
  `lastActivity` heartbeat advanced once per read-loop iteration and once per
  handler attempt (so a batch, or a single slow-but-bounded handler, keeps it
  fresh), exposed via `Healthy(maxStale)`. Each service wires `Healthy` into a
  `liveness.Registry` worker probe with a stale budget larger than the handler
  timeout, so a wedged (non-iterating) consumer goroutine trips liveness and
  restarts the pod while legitimate in-flight work never does.
  `pkgoutbox.Processor` carries the same `Healthy(maxStale)` heartbeat for the
  outbox publisher loop.

- **Dead-letter backlog probe.** A wedged-loop heartbeat cannot see a *live*
  processor that is simply failing every row it publishes (e.g. a bad
  payload or a permanently misconfigured downstream) — each row still goes
  terminal ('failed') and the loop keeps ticking. `pkgoutbox.Processor`
  exposes `DeadLetterBacklog(ctx)`, which counts rows with `status = 'failed'`
  (the terminal state; the dead-letter row it wrote alongside each one stays
  `'pending'` and is not counted). Every service that runs an outbox
  processor wires this into a `liveness.Registry.AddProbe("outbox_dead_letters",
  30*time.Second, ...)` that fails liveness only when the backlog is
  non-zero. The probe swallows its own query error (returns nil) rather than
  failing liveness on a transient DB hiccup — the `Healthy` heartbeat above
  already covers a genuinely wedged loop.

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

The `remediation` service applies a separate, domain-level classification to failed dbt nodes across all three pipeline stages: `compile`, `seed_build`, and `validation`. This is distinct from the `ErrPermanent` sentinel above, which governs message-consumer routing. The classifier (`remediation/domain/failure/classify.go`) deterministically sorts each failure into one of four categories regardless of which stage produced it:

| Category | Routing decision | Signals |
|---|---|---|
| `infra_transient` | `drop` (not emitted) | Exactly four families: connection refused / could not connect to database; OOMKilled; ImagePullBackOff / back-off pulling image; InvalidAccessKeyId / AccessDenied (S3 credentials). |
| `test` | `emit` | dbt test assertion failures. |
| `logic` | `emit` | SQL/model defects (relation/object does not exist, compilation error, syntax error, missing ref, type mismatch, ambiguous column). |
| `unknown` | `emit` | Everything else, including the ambiguous resource/permission class (statement timeout, permission denied, deadlock, out-of-memory) and an unreachable log. |

**Pre-classification exclusion.** A `release.rejected:v1` message whose `reason` is `parse_rehearsal_failed` or `artifact_upload_failed` (both `stage="compile"`, from the compile Job's parse-export/rehearsal gate) never reaches the classifier at all: the inbound adapter builds no `FailureEvidence` for either reason, so no `classification_decision` row is written and no `remediation.requested:v1` trigger is emitted. Both are continuo-internal or project-property failures, never a model defect, so classifying them would put a fixable-looking label on something no model change can fix.

**Stage sources.** The classifier receives each failure annotated with a `source` field (`validation`, `compile`, or `seed_build`) derived from the `stage` field of the incoming `release.rejected:v1` event. For `compile` and `seed_build` sources the classifier additionally calls `ExtractDbtFilePath` against the dbt log to derive the project-relative source file path (e.g. `models/order_items.sql`); this `file_path` is threaded into the `remediation.requested:v1` trigger so the downstream agent can read the real source file without querying orchestrator ancestry. For `validation` sources `file_path` is empty at this layer; the agent resolves it via `GetNodeAncestry`.

**Under-drop policy**: only the four confidently-infra signal families are dropped. Ambiguous cases — signals that could be either infrastructure or a model problem — fall through to `unknown` and are emitted. Uncertainty flows to the heal agent; only confident infrastructure failures are silenced.

**Structured-first signal.** Each per-node entry on `release.rejected:v1` carries an optional `run_results_uri` — the S3 key of a structured validation-result record (`status` in dbt's `success | error | fail | skipped` vocabulary, plus `message`, `failures`, `unique_id`) emitted by the validation pod and uploaded by k8s-controller. When present, `ClassifyWithStructured` (`remediation/domain/failure/classify.go`) keys off it: a `fail` status is deterministically `test` (no heuristic); an `error` status routes the structured `message` through the same infra/logic substring rules used for the text log. When `run_results_uri` is absent, empty, or unfetchable — or the structured record carries no message — classification falls back to parsing the dbt text log at `dbt_log_uri`. The category vocabulary and routing decisions above are identical on either path.

The remediation binding ACKs a malformed `release.rejected:v1` payload by returning nil from the handler (it does not use `ErrPermanent`); a transient S3 fetch error (for either the text log or the structured result) is returned unwrapped so the message stays in the PEL for retry.

## See also

- `docs/arch/03-sequence-flows.md` §3 (permanent fast-path note in the
  retry/terminal flow) and §8 (dispatch watchdog termination).
- `docs/arch/04-service-ownership.md` (per-service invariants under
  orchestrator, executor-controller, and manifest-controller).
- `docs/arch/services/remediation.md` (full remediation service documentation).
