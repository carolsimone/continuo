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
| `state/adapters/publisher/outbox_publisher.go` (`Publish`) | generic `json.Unmarshal(entry.Payload, &fields)` into a `map[string]interface{}` fails on malformed JSON — no per-type switch, no unknown-event_type detection | `fmt.Errorf("%w: unmarshal outbox row: %v", events.ErrPermanent, err)` |
| `orchestrator`, `executor-controller`, `k8s-controller` outbox publishers (`adapters/*/outbox_publisher.go`, `Publish`) | decode a typed event out of `entry.Payload` per `event_type`; a `json.Unmarshal` failure against a *known* `event_type`, or an out-of-range numeric field (`pkg/num.Int32`) on an otherwise well-formed known payload, is deterministic — the row can never succeed no matter how many times it is retried | `fmt.Errorf("%w: unmarshal <event>: %v", events.ErrPermanent, err)` / `fmt.Errorf("%w: <event> payload: %v", events.ErrPermanent, err)` |
| `orchestrator`, `executor-controller`, `k8s-controller` outbox publishers (`adapters/*/outbox_publisher.go`, `Publish`) | the type-switch's `default:` case — an `event_type` the publisher does not recognise — is **not** wrapped in `ErrPermanent`. During a rolling deployment an old replica can dequeue a row for an `event_type` only a newer (already-deployed) replica knows how to publish; treating it as permanent would dead-letter a row that a peer replica can handle moments later. The row is retried through its normal backoff budget instead | `fmt.Errorf("<service> publisher: unknown event_type %q (retryable — a newer replica may handle it during a rolling upgrade)", entry.EventType)` |
| All 7 outbox publishers' `Publish`, dead-letter branch | `pkgoutbox.DeadLetterValues` fails to decode a dead-letter row's own JSON payload — the publisher's own construction, so a decode failure is a bug, not an infra blip | `fmt.Errorf("%w: dead-letter values: %v", events.ErrPermanent, err)` |

Add new emitters to this table as they land.

## Who recognises it

| Site | Behaviour on `errors.Is(err, events.ErrPermanent)` |
|---|---|
| `pkg/redis/streamconsumer.go` (`readAndProcess` and `reclaimPending`) | log ERROR, ACK, continue — drops the message from the PEL under both first-delivery AND periodic reclaim. **Read path** retries plain (non-`ErrPermanent`) errors inline via `invokeWithRetry` (backoff schedule `0 / 100ms`, one quick retry then park, ~100ms budget); if both attempts fail the message is left un-ACKed in the PEL and the reclaim sweep redelivers it. Keeping the inline budget short stops one transiently-failing message from head-of-line-blocking its lane for seconds. **Reclaim path** is single-shot: the handler is called exactly once per claimed entry, and on failure the entry stays in the PEL — the next periodic sweep (every `reclaimInterval`) is the retry cadence. This split keeps the read loop responsive when a large backlog of pending entries is being swept. The sweep itself uses `XAUTOCLAIM` (cursor-paged, one round-trip per 100 entries) gated by a `MinIdle` threshold (default 30s, overridable via `WithReclaimMinIdle`) so a multi-replica deployment cannot have one replica steal an in-flight message that a peer is actively retrying. Successful and `ErrPermanent`-dropped IDs are ACKed in one pipelined `XAck` per read batch / reclaim page (ack-after-success unchanged — only resolved IDs are batched). `ErrPermanent` ACKs immediately on either path. Both paths invoke the handler through `safeInvoke`, which recovers any handler panic and converts it into a plain (non-`ErrPermanent`) error — a panicking handler can no longer unwind through the loop and crash the process, and the message is left in the PEL for the next sweep just like any transient failure. Beyond `ErrPermanent`, the consumer also classifies startup consumer-group bootstrap errors, quarantines poison messages past a delivery bound, and carries a per-handler timeout plus a liveness heartbeat — see **Stream consumer resilience** below. Used by every Go service's Redis ingest path. |
| `executor-controller/service/deployer/dispatcher.go` (`dispatchRow`) | call `writeFailed` (writes `task.status.updated:v1` FAILED + `node.updated:v1` FAILED as `executor_outbox` rows, marks `executor_deployments` row `failed`) |
| `pkg/outbox.Processor` (`processBatchOnce`) | a publish error matching `ErrPermanent` is terminal on attempt #1 — zero retries. Any other error is treated as transient and rescheduled with backoff instead. See **Outbox processor resilience** below. |

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

- **Drop notification seam (`WithOnDrop` / `DropHandler`).** A dropped message —
  poison quarantine or `ErrPermanent` — is ACKed away silently apart from the log,
  which orphans any in-flight state a handler committed before it started failing.
  A consumer may register a `DropHandler` via `WithOnDrop`; the consumer invokes
  it at every drop site (read-path permanent, reclaim permanent, reclaim poison)
  with the message and the cause, so the owning service can finalize that state —
  but only **after the ACK is confirmed** (`ackFn`/`ackBatch` returned no error).
  A failed XACK leaves the message in the PEL to be reprocessed, so notifying then
  would finalize in-flight state for a message that was never actually dropped;
  in agent-remediation the newly-terminal row would let the same trigger spend
  another attempt. It is best-effort and off the critical path: nil by default
  (drops behave exactly as before), invoked with panic recovery, and its outcome
  never changes whether the message is ACKed. `agent-remediation` uses it to fail the in-flight
  `generating` proposal row a dropped `remediation.requested:v2` trigger leaves
  behind (see `docs/arch/services/agent-remediation.md`, "Recovering a dropped
  trigger's in-flight row").

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

## Outbox processor resilience

`pkg/outbox.Processor` (`processBatchOnce`) splits a publish failure it
receives from `Publisher.Publish` into two classes and handles each
differently:

- **Permanent** (`errors.Is(err, events.ErrPermanent)`) — the payload is
  deterministically bad for a *known* `event_type` (an undecodable JSON
  payload, or an out-of-range numeric field on an otherwise well-formed
  payload); retrying can never succeed. The row goes terminal on attempt #1 —
  `retry_count` is left untouched, since bumping it would misrepresent a
  single dead-on-arrival row as a row that was actually retried.
- **Transient** (everything else — Redis `XADD`/connection errors, timeouts,
  and an `event_type` the publisher's type-switch does not recognise) —
  treated as recoverable. An unrecognised `event_type` is deliberately
  transient rather than permanent: during a rolling deployment an old replica
  can dequeue a row for an `event_type` only a newer (already-deployed)
  replica knows how to publish, so dead-lettering it on attempt #1 would
  discard a row a peer replica could have handled moments later. The row is
  rescheduled via `ScheduleRetry`, which bumps `retry_count`, stamps
  `next_attempt_at = clock_timestamp() + backoff(attempt)`, records the error,
  and moves the row to the `scheduled` status. `clock_timestamp()` (the
  statement wall-clock) is used rather than `NOW()` (fixed at transaction
  start) so a batch whose publish attempts outlast the first backoff interval
  still schedules a real delay instead of a deadline already in the past. A
  backed-off row goes terminal only once `retry_count + 1 >= MaxRetries`.

**Due-gate, scheduled status, and backoff curve.** `GetPendingBatch` selects
rows with `status IN ('pending', 'scheduled')` that are due —
`next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()`. A fresh
`pending` row (never attempted, `next_attempt_at` NULL) is always eligible; a
transiently-failed `scheduled` row becomes eligible only once its backoff
elapses. The distinct `scheduled` status is what keeps the change safe under a
rolling deployment: a previous-version replica's reader filters on
`status = 'pending'` and so never reclaims a row a newer replica has backed
off, so it cannot retry that row every tick and exhaust its budget before the
backoff elapses. The delay is capped exponential
growth — `base * 2^(attempt-1)` clamped to a maximum — defaulting to a
1-second base and a 5-minute cap. The small base keeps the first retry about a
poll tick away so a brief blip recovers quickly (which matters under
per-aggregate FIFO, where a scheduled head withholds its younger siblings until
it publishes), while the exponential growth still spaces out attempts during a
sustained outage. `DefaultMaxRetries` is 13, giving a transient row roughly 20
minutes of retry window before the budget is exhausted and it goes terminal —
long enough to ride out a real Redis outage or restart without manual
intervention.

**Mandatory durable dead-letter.** Every terminal outcome — permanent, or
transient with the retry budget exhausted — writes a dead-letter row in the
same transaction that marks the original row `failed`, so the signal is
durable even if the process crashes immediately afterward. The dead-letter
row is itself an ordinary outbox row targeting the single canonical stream
`outbox.dead_letter:v1` (`streams.OutboxDeadLetterV1`), so it publishes
through the same processor loop as any other row — immediately if Redis is
reachable, or once Redis heals. Its payload (`pkgoutbox.DeadLetterPayload`)
carries `original_event_type`, `original_stream`, `original_aggregate_id`,
`failure_kind` (`permanent` or `transient_exhausted`), `error`, `attempts`,
and `failed_outbox_id`. A loop guard — the dead-letter row's
`aggregate_type`/`event_type` are set to the sentinel `outbox_dead_letter` —
stops a dead-letter row from ever being dead-lettered itself. An optional
`TerminalFailureHook`, on a service that wires one, still fires as a
best-effort additional side effect alongside the mandatory dead-letter write;
it is not the delivery mechanism.

**Operational, not domain.** `outbox.dead_letter:v1` is an
infrastructure/operational signal — "this outbox row could not be
delivered" — distinct from a domain `<event>.failed:v1` compensation event,
which represents a business-meaningful terminal outcome (e.g. a task that
failed after exhausting its k8s retry budget). It has no consumer today; it
exists for a future alerting/redrive service. Producers are every
outbox-owning service's `pkg/outbox.Processor`: `state`, `orchestrator`,
`executor-controller`, `k8s-controller`, `release-controller`, `remediation`,
`agent-remediation`.

**Dead-letter backlog visibility.** A wedged-loop heartbeat (see **Stream
consumer resilience** above) cannot see a *live* processor that is simply
failing every row it publishes (e.g. a bad payload or a permanently
misconfigured downstream) — each row still goes terminal (`'failed'`) and the
loop keeps ticking. `pkgoutbox.Processor` exposes `DeadLetterBacklog(ctx)`,
which counts rows with `status = 'failed'` (the terminal state; the
dead-letter row it wrote alongside each one stays `'pending'` until it
publishes, and is not counted). This is deliberately **not** wired into any
service's readiness or liveness probe: all replicas of a service share the
same outbox table, so gating pod health on the backlog would take the whole
Service out of rotation over a single stuck row — a data/ops condition that
needs a human or a dead-letter consumer to redrive, not a pod restart or an
endpoint pull. `DeadLetterBacklog` — backed by the repository's
`CountTerminal` — exists as an API seam for that future consumer. Visibility
today is the durable `outbox.dead_letter:v1` event plus a structured `ERROR`
log (`"Outbox entry dead-lettered"`, with `failure_kind`, `entry_id`,
`original_event_type`, `original_stream`, `attempts`, `error`) emitted at the
moment a row goes terminal.

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

## Remediation service: release failure taxonomy

The `remediation` service applies a separate, domain-level classification to failed nodes across all three pipeline stages: `compile`, `seed_build`, and `validation`. This is distinct from the `ErrPermanent` sentinel above, which governs message-consumer routing. The same four categories cover dbt and python nodes alike — a python node's validation failure arrives as the warehouse engine's own error text, which the substring rules below read exactly as they read a dbt log's. The classifier (`remediation/domain/failure/classify.go`) deterministically sorts each failure into one of four categories regardless of which stage or node kind produced it:

| Category | Routing decision | Signals |
|---|---|---|
| `infra_transient` | `drop` (not emitted) | Exactly four families: connection refused / could not connect to database; OOMKilled; ImagePullBackOff / back-off pulling image; InvalidAccessKeyId / AccessDenied (S3 credentials). |
| `test` | `emit` | dbt test assertion failures. |
| `logic` | `emit` | SQL/model defects (relation/object does not exist, compilation error, syntax error, missing ref, type mismatch, ambiguous column). |
| `unknown` | `emit` | Everything else, including the ambiguous resource/permission class (statement timeout, permission denied, deadlock, out-of-memory) and an unreachable log. |

**Pre-classification exclusion.** A `release.rejected:v1` message whose `reason` is `parse_rehearsal_failed` or `artifact_upload_failed` (both `stage="compile"`, from the compile Job's parse-export/rehearsal gate) never reaches the classifier at all: the inbound adapter builds no `FailureEvidence` for either reason, so no `classification_decision` row is written and no `remediation.requested:v2` trigger is emitted. Both are continuo-internal or project-property failures, never a model defect, so classifying them would put a fixable-looking label on something no model change can fix.

**Stage sources.** The classifier receives each failure annotated with a `source` field (`validation`, `compile`, `seed_build`, or `duplicate_table`) derived from the `stage` field of the incoming `release.rejected:v1` event; the stage-less `duplicate_table` rejection instead derives its source from the `reason` field. For `compile` sources the classifier additionally calls `ExtractDbtFilePath` against the dbt log to derive the project-relative source file path (e.g. `models/order_items.sql`); for `seed_build`, `validation`, and `duplicate_table` sources release-controller stamps `file_path`/`service` onto each `per_node` entry from the candidate topology instead. Either way the path is threaded into the `remediation.requested:v2` trigger so the downstream agent can read the real source file without an orchestrator round trip — which matters for correctness, not only economy: `GetNodeLocation` serves the *promoted* topology, and a rejected release is never promoted. Only a node whose trigger carries no `file_path` falls back to that RPC.

Classification is per node, but emission is per release: once every failing node of one rejection has been classified and recorded, the classifier emits a **single** `remediation.requested:v2` message carrying all the healable ones in `nodes[]`, plus each node's `changed_ancestors` (the ancestors this release changed that the failure descends from, each with the file path and service the candidate declares, stamped by release-controller). That is what lets the agent see two failures as one cause and repair them with one edit.

**Under-drop policy**: only the four confidently-infra signal families are dropped. Ambiguous cases — signals that could be either infrastructure or a model problem — fall through to `unknown` and are emitted. Uncertainty flows to the heal agent; only confident infrastructure failures are silenced.

**Shadow-verification override.** One drop is decided outside the category rules entirely. A `release.rejected:v1` message whose `shadow` field is true came from a fix-verification release `agent-remediation` posted — a release that runs the full validation pipeline but never promotes — so its rejection reports on a proposed fix, not on a change anyone shipped. The classifier still runs the rules and still writes a `classification_decision` row carrying the category and error signature they produced, so the failure is recorded truthfully and no drop is invisible; it then overrides the routing to `decision=drop`, `reason=shadow_verification`, and emits nothing. The agent learns the same verdict by polling the release it submitted. Emitting here as well would hand a failed fix attempt back to the agent as a fresh failure to heal, and the loop would have no exit.

**Structured-first signal.** Each per-node entry on `release.rejected:v1` carries an optional `run_results_uri` — the S3 key of a structured validation-result record (`status` in dbt's `success | error | fail | skipped` vocabulary, plus `message`, `failures`, `unique_id`) emitted by the validation pod and uploaded by k8s-controller. When present, `ClassifyWithStructured` (`remediation/domain/failure/classify.go`) keys off it: a `fail` status is deterministically `test` (no heuristic); an `error` status routes the structured `message` through the same infra/logic substring rules used for the text log. When `run_results_uri` is absent, empty, or unfetchable — or the structured record carries no message — classification falls back to parsing the dbt text log at `dbt_log_uri`. The category vocabulary and routing decisions above are identical on either path.

The remediation binding ACKs a malformed `release.rejected:v1` payload by returning nil from the handler (it does not use `ErrPermanent`); a transient S3 fetch error (for either the text log or the structured result) is returned unwrapped so the message stays in the PEL for retry.

### `python-csv` failure classes

A `python-csv` node's contract declares a single `csv` read (an `s3://` or `https://` uri) and the `output_columns` it promises. Its failures fall into six distinct classes, at three different points in the pipeline — most never reach the `remediation` classifier above at all, since only the validation-stage classes do:

| When | Failure | Classification |
|---|---|---|
| CI / topology-controller parse | A malformed `csv:` uri (not `s3://` or `https://`), or a `reads` block that is not exactly `{csv: <uri>}` | Permanent contract error — the release never reaches validation; `topology-controller`'s parser rejects the entry outright (see `docs/arch/services/topology-controller.md`). |
| Validation | A declared `output_columns` entry missing from the CSV's own header line | Validation failure → `release.rejected:v1` (`stage=validation`) → the `remediation` classifier → `remediation.requested:v2` → `csvValidationFixer` (see `docs/arch/services/agent-remediation.md`). |
| Validation | The declared uri is unreachable when the validation pod range-fetches its header, with an error text that does **not** match one of `remediation`'s four hard-drop `infraRules` families (e.g. a 404/`NoSuchKey`, DNS failure, or timeout) | Validation failure, same routing as above — an accepted trade-off: an unreachable source at validation time surfaces as an ordinary validation rejection rather than a distinct infra category, since a bad or since-moved uri is functionally a contract defect from the operator's perspective. |
| Validation | The declared uri is unreachable with an S3 `AccessDenied`/`InvalidAccessKeyId` response, or a TCP connection refused, when the validation pod range-fetches its header | **Not routed to `csvValidationFixer` at all.** `remediation`'s `ClassifyWithStructured`/`Classify` (`remediation/domain/failure/classify.go`) match `infraRules` — the same substrings a platform-infra outage would produce — before the test/logic rules ever run, so this is classified `infra_transient`/`decision=drop`: no `remediation.requested:v2` is emitted, and the failure is only visible as a `classification_decision` audit row. This is deliberate, not an oversight: an `AccessDenied` on an operator-supplied source is not something a contract-yaml fix can resolve (it needs an IAM/bucket-policy change outside the release), and a refused connection reports a network condition rather than a URI defect, so silencing both matches the same under-drop policy the platform's own infra signals already get. The trade-off is that a csv source whose bucket policy or firewall genuinely changed gets no fix proposal and no operator-facing signal beyond that audit row. |
| Run | The uri is unreachable, or the fetched file fails to parse as CSV, when the run pod loads it | Existing `HarnessError` classes (`ReadError`/`LoadError`/`ConformError` — the same deterministic error-class vocabulary every python-family run failure reports via its structured result block; see **Structured-result column** in `docs/arch/services/state.md`). Not a distinct csv category — the harness treats a csv read failure the same way it treats any other declared-read failure. |
| Validation (warning) / Run (policy-dependent) | The CSV header carries columns beyond the declared `output_columns` | **Warning only** at the validation gate — extra undeclared columns do not fail validation. **At run time**, however, extras are governed by the contract's own `extra_columns` policy, whose default is `raise`: a release can pass its validation gate green and still fail on its first real run if the live file carries extra columns and the author never set `extra_columns: warn`. This asymmetry is deliberate (validation warns so the author has visibility without blocking a release over columns that may be transient), but it means a green gate is not a run guarantee for this one failure mode — an author who wants the extra-columns case to never fail a run must set `extra_columns: warn` explicitly. |

**SSRF / S3-credential posture (accepted, not accidental).** An `https://` csv source is fetched directly by the run pod — the same pod that, for `python-csv` nodes only, carries S3 credential env vars (see `docs/arch/services/executor-controller.md`, **python-family nodes**). The executor attaches those credentials unconditionally, regardless of the declared uri's scheme, because it knows only the node type, not the contract's uri. This means a `python-csv` pod fetching an operator-supplied `https://` uri does so from a process that also holds S3 credentials in its environment. The accepted mitigation is upstream, not runtime: contract authorship (and therefore uri authorship) goes through code review before it reaches production, and the runtime itself performs no HTTPS authentication or destination allow-listing in v1 — that is explicitly out of scope for the first release. This is a documented decision, not an oversight: the alternative (a network-isolated fetch sidecar with no S3 credentials, or an explicit destination allow-list) is deferred to a later hardening pass if an install's threat model requires it.

## See also

- `docs/arch/03-sequence-flows.md` §3 (permanent fast-path note in the
  retry/terminal flow) and §8 (dispatch watchdog termination).
- `docs/arch/04-service-ownership.md` (per-service invariants under
  orchestrator, executor-controller, and topology-controller).
- `docs/arch/services/remediation.md` (full remediation service documentation).
