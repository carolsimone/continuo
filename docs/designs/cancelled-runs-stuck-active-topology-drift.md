# Cancelled Runs Stuck "Active" → Spurious Topology-Drift Badges — Design

**Status:** Draft for review
**Author:** generated 2026-05-21 via systematic-debugging session against the live dev cluster
**Related work:** `docs/superpowers/specs/2026-05-06-topology-drift-detection-design.md` (the drift feature this defect lives in); commits `c20f3cd` / `735727a` (May 13 — removed then restored the `run.finalized:v1` consumer for inherited-only runs); `220651b` (Apr 16 — graph + dependency-controller merged into orchestrator).

---

## 1. Symptom

On the dashboard (`DashboardPage` → `/api/schedules`), every schedule with history shows a drift strip reading **"? topology version unknown"**, even schedules whose latest run *succeeded* minutes ago at the current topology generation. `seed`, which has no rows in the topology projection, correctly shows no strip.

The strip should read `source N gen behind latest` when an in-flight run is pinned to an older topology generation, and be hidden when the schedule has no in-flight run (`drift-helpers.ts` → `getDriftState`/`getDriftBadge`; `SchedulerCard.tsx`).

## 2. Evidence (live dev cluster, 2026-05-21)

`/api/schedules` returns `latest_topology_generation = 89` and, per schedule:

| schedule | active_run_id | active_run_topology_generation | rendered |
|---|---|---|---|
| e2e-schedule | `73b2b2e9…` | 0 | "topology version unknown" |
| e2e-schedule-failure | `a4ed2efc…` | 0 | "topology version unknown" |
| seed | null | null | (strip hidden) ✓ |

Neo4j `:Run` nodes (`completed_at IS NULL` = "active"):

```
sched                  run        created                completed              term         gen
e2e-schedule           ab9f2d2e   2026-04-21T17:52:44Z   NULL                   NULL         NULL
e2e-schedule           dc13ff09   2026-04-22T13:47:48Z   NULL                   NULL         NULL
e2e-schedule           9147e6f6   2026-04-22T14:01:22Z   NULL                   NULL         NULL
e2e-schedule           c7de51b2   2026-04-27T16:40:54Z   NULL                   NULL         NULL
e2e-schedule-failure   a4ed2efc   2026-04-29T15:22:18Z   NULL                   NULL         0
e2e-schedule           73b2b2e9   2026-04-29T15:23:45Z   NULL                   NULL         0
e2e-schedule           b5b13e04   2026-05-21T14:32:59Z   2026-05-21T14:34:44Z   succeeded    89  ← today, finalized OK
e2e-schedule           26dfdcb3   2026-05-21T14:35:24Z   2026-05-21T14:36:39Z   succeeded    89  ← today, finalized OK
```

state's authoritative `scheduler_tracker` for the six stuck `run_id`s — **all `cancelled`, all `completed_at` NULL**:

```
run        status      created               completed_at   tasks
ab9f2d2e   cancelled   2026-04-21 17:52:43   (null)         4/12
dc13ff09   cancelled   2026-04-22 13:47:48   (null)         8/12
9147e6f6   cancelled   2026-04-22 14:01:22   (null)         8/12
c7de51b2   cancelled   2026-04-27 16:40:54   (null)         8/12
a4ed2efc   cancelled   2026-04-29 15:22:17   (null)         0/7
73b2b2e9   cancelled   2026-04-29 15:23:45   (null)         0/12
```

**What the evidence nails down:**

1. **Live completion works.** Today's two `succeeded` runs have `completed_at`, `terminal_status='succeeded'`, `gen=89` (correct latest). The SUCCEEDED/FAILED projection path is healthy — this is *not* a regression from the orchestrator merge.
2. **Every stuck run is `cancelled`**, in *both* stores' eyes, and **neither store set `completed_at`** for it. state records `cancelled_at` instead; Neo4j has nothing.
3. Two zombies (`a4ed2efc`, `73b2b2e9`) were cancelled at `0/N` tasks — they emitted zero terminal `node.updated:v1` events, so the orchestrator's node-completion aggregate could never have finalized them even in principle.

## 3. Root cause — the cancel transition bypasses the finalization routine

The relevant state machine is in **`state`**, not the orchestrator. `state/domain/aggregate/run/status.go` already models cancellation as terminal:

```go
const (
	SchedulerStatusPending, SchedulerStatusRunning,
	SchedulerStatusSucceeded, SchedulerStatusFailed,
	SchedulerStatusCancelled SchedulerStatus = "cancelled"
)
func (s SchedulerStatus) IsTerminal() bool { // succeeded | failed | cancelled → true }
```

state also has a **single reusable terminal-transition routine**, `Run.finalize(outcome, now)` (`run.go:584`), which does two coupled things:

```go
func (r *Run) finalize(outcome SchedulerStatus, now time.Time) []DomainEvent {
	r.status = outcome
	r.completedAt = &now              // (1) sets completed_at
	r.changes.statusDirty = true
	r.changes.completedDirty = true   // → SaveRun routes to FinalizeRunTx (sets completed_at in SQL)
	evt := RunFinalized{ID: r.scheduleID, Name: r.scheduleName, Outcome: outcome}
	r.events = append(r.events, evt)  // (2) emits RunFinalized → run.finalized:v1
	return []DomainEvent{evt}
}
```

Every SUCCEEDED/FAILED path funnels through `finalize()` (`finalizeIfComplete`, `AcceptDispatch` auto-rollup, `MarkDispatchFailed`). The orchestrator then projects `run.finalized:v1` onto its Neo4j `:Run` node via the already-wired `RunFinalizedHandler` → `RunAggregateRepository.FinalizeRun` (whose own doc comment states: *"The orchestrator merely projects the outcome state has already decided."*).

**`Run.Cancel()` (`run.go:359`) is a parallel terminal transition that never calls `finalize()`:**

```go
func (r *Run) Cancel(ctx, tasks, by, reason, now) ([]DomainEvent, error) {
	if r.IsTerminal() { return nil, ErrAlreadyTerminal }
	r.status = SchedulerStatusCancelled  // inline — not via finalize()
	r.cancelledAt = &now                 // sets cancelled_at, NOT completed_at
	r.changes.cancelDirty = true         // → SaveRun routes to CancelTx (no completed_at)
	r.changes.statusDirty = true
	tasks.BulkCancel(ctx, r.scheduleID, by)
	evt := RunCancelled{...}             // emits RunCancelled → schedule.cancelled:v1 ONLY
	return []DomainEvent{evt}, nil
}
```

Consequences, both observed in §2:

- `SaveRun` dispatches on the dirty flag: `cancelDirty` → `CancelTx` (`UPDATE … SET status, cancelled_at, cancelled_by, cancellation_reason`), which **never sets `completed_at`**. `completedDirty` → `FinalizeRunTx` (sets `completed_at`) is the *other* branch and is not taken. So state's own `scheduler_tracker.completed_at` is NULL for cancelled runs.
- `Cancel()` emits only `RunCancelled` → `schedule.cancelled:v1`. It does **not** emit `RunFinalized`, so **`run.finalized:v1` is never published for a cancellation** — the orchestrator's projection channel never learns the run is terminal.

The orchestrator side then plays out exactly as the symptom requires: `schedule.cancelled:v1` is consumed only by `ScheduleCancelledHandler`, which writes the `cancelled_schedules` *guard* table and never touches Neo4j; and that guard makes `handle_node_completed.go:81` *suppress* any late `node.updated:v1`. So the `:Run.completed_at` stays NULL forever, and since `ListActiveRuns` defines "active" as `completed_at IS NULL`, the zombie is surfaced as in-flight and `/api/schedules` pins its gen 0/NULL onto a healthy schedule.

**In one sentence:** the cancel transition is terminal in the domain model but skips the `finalize()` side-effects (set `completed_at`, emit `run.finalized:v1`), so neither store records the run as *completed* and the orchestrator's topology projection never converges. This is precisely the case the drift design dismissed in its §4.6 as "currently impossible" — `HasActiveSchedule` blocks *concurrent* triggers, not *terminal-but-unfinalized* runs.

## 4. Goal & non-goals

**Goal:** a cancelled run becomes *finalized* everywhere a succeeded/failed run is — `completed_at` set in both stores, and `run.finalized:v1` published — so the orchestrator's existing projection marks the `:Run` terminal and the dashboard stops attaching dead runs' topology generations to schedules.

**Non-goals:**
- No redefinition of cancellation semantics. `cancelled_at`/`cancelled_by`/`cancellation_reason` and the `schedule.cancelled:v1` guard event stay — they drive the orchestrator/executor/k8s work-suppression guards and must keep firing. We are *adding* the missing finalization side-effects, not replacing the guard path.
- No change to the drift read contract (`0`/NULL still means "unknown"; `getDriftState` unchanged).
- No change to the SUCCEEDED/FAILED paths — they are healthy.

## 5. Options considered

### Option 1 — Route `Run.Cancel()` through the finalization side-effects (recommended)

Make cancellation a finalizing terminal transition in `state`'s domain, the same way every other terminal transition is. `Run.Cancel()` should, in addition to recording cancellation metadata and `RunCancelled`:

- set `completed_at` (alongside `cancelled_at`), so `SaveRun` persists it (mark `completedDirty` so the `FinalizeRunTx` write also runs, or have `CancelTx` set `completed_at` too — see §6.1); and
- emit `RunFinalized{Outcome: SchedulerStatusCancelled}` *in addition to* `RunCancelled`.

The cleanest expression is to reuse `finalize()` for the status/`completed_at`/`RunFinalized` part and keep the cancellation-specific metadata + `RunCancelled` + `BulkCancel` around it, so cancel emits **both** events: `RunCancelled` (→ `schedule.cancelled:v1`, guards) and `RunFinalized` (→ `run.finalized:v1`, projection).

The orchestrator needs **no change**: `RunFinalizedHandler` → `FinalizeRun(runID, "cancelled")` already accepts any status string (no whitelist), and `FinalizeRun` is idempotent (`completed_at = COALESCE(completed_at, datetime())`).

- **Pros:** fixes the cause at the single domain seam where it lives; makes "terminal" and "finalized" coincide for *all* outcomes; repairs state's own `completed_at` gap; reuses the entire existing projection pipeline; aligns with the architecture's stated ownership ("orchestrator projects what state decided"). One conceptual change, mostly in one aggregate.
- **Cons:** widens the `run.finalized:v1` status contract to include `cancelled` (see §6.2 — blast radius is tiny: orchestrator is the sole consumer and does not whitelist). Two events now emitted on cancel; must confirm no consumer double-counts (the guard path keys on `schedule.cancelled:v1`, the projection on `run.finalized:v1` — disjoint).

### Option 2 — Project cancellation in the orchestrator's guard handler

Extend `ScheduleCancelledHandler` to also call `RunAggregateRepository.FinalizeRun(runID, "cancelled")`.

- **Rejected as primary.** It patches the *projection* (orchestrator) while leaving state's own `completed_at` NULL and `run.finalized:v1` unemitted — i.e. it treats the symptom and leaves the domain model internally inconsistent (a terminal run that state never marks completed). It also spreads run-terminality logic across two services. Keep it in mind only if a state release is somehow off the table.

### Option 3 — Read-side filter only

Exclude cancelled runs in `ListActiveRuns`. Rejected: cross-store Neo4j→Postgres join, leaks the guard concept into the read model, and `terminal_status IS NOT NULL` does not even discriminate (zombies have it NULL). Useful only as the read-hardening layer in §6.3.

**Recommendation: Option 1**, plus the layers in §6.

## 6. Details & defense in depth

### 6.1 `completed_at` vs `cancelled_at` in state

Set **both** on cancel. Semantics become uniform and self-consistent: `completed_at` = "the run reached a terminal state, at this time" (true for succeeded/failed/cancelled); `cancelled_at` = "the terminal state was cancellation, at this time". This makes state's `completed_at` the single reliable "run is over" predicate, matching how the orchestrator's `:Run.completed_at` and "active" semantics already work. Implementation choice: either mark `completedDirty` in `Cancel()` so `SaveRun` runs `FinalizeRunTx` after `CancelTx`, or fold `completed_at = NOW()` into `CancelTx`'s `UPDATE`. **Fold it into `CancelTx`** — one SQL statement, no two-write ordering question — which is verified safe (§9 Q1: no state reader keys liveness or display on `completed_at`; all such logic keys on `status`).

### 6.2 `run.finalized:v1` contract

`pkg/events/run_lifecycle.go` documents `Status` as `succeeded | failed`; `pkg/streams/streams.gen.go` likewise says "success or failure". Widen both comments to include `cancelled`. The only consumer is the orchestrator (`orchestrator-run-finalized` group); its parser and `RunFinalizedHandler` accept any non-empty status with no validation, and `FinalizeRun` writes it verbatim. No code change needed in the consumer; this is a contract-comment widening plus a test asserting `cancelled` flows through.

### 6.3 Read hardening (defensive)

Change `ListActiveRuns` ordering to `ORDER BY r.schedule_name, r.created_at DESC` and have `ListActiveRunDrifts` keep only the most-recent in-flight run per schedule. Today the ordering is `ORDER BY r.schedule_name` only, so *which* zombie surfaces is nondeterministic and `/api/schedules` collapses by name last-write-wins. Even after Option 1, this bounds the blast radius of any future drift to "show the newest run" rather than an arbitrary one.

**Layer placement (strict):** the Cypher `ORDER BY` change is an adapter detail (`orchestrator/adapters/neo4j`), behind the `RunReader` port already owned by `orchestrator/service/queries`. The "one drift row per schedule, newest wins" policy is application vocabulary and is **owned by** `RunQueryService.ListActiveRunDrifts` (the query/application layer), reached through `RunReader` — consistent with that service already owning the "warn on >1 active run" rule. Doing the dedup in Cypher is a permissible adapter-side optimization of that application-owned contract, but the contract's owner remains the query service; the adapter must not become the place that *decides* the policy.

### 6.4 Reconciliation sweep (implemented backstop)

**Implemented:** `orchestrator/internal/reconciler` ticks every `ORCHESTRATOR_RECONCILER_INTERVAL_SECONDS` (default 60), lists active `:Run`s (`ListActiveRuns`), reads each run's status from state via the orchestrator-owned `ports.RunStatusReader` (adapter `adapters/grpc.RunStatusReader` over the existing state gRPC client, mapping `GetScheduler` → succeeded/failed/cancelled), and calls `FinalizeRun` for any that are terminal. No cross-service DB read.


A periodic orchestrator sweep (alongside the existing `cancelled_schedules` sweeper / dispatch watchdog in `main.go`) that finalizes any `:Run` with `completed_at IS NULL` whose run state owns reports as terminal. Source of truth for "is this run over" is state; Neo4j must converge to it.

**DDD constraint (strict):** the orchestrator must **not** read state's `scheduler_tracker` table — that table is owned by the `state` service and lives in a different database. Cross-service truth must flow through a state gRPC query, consumed behind an **orchestrator-owned port** (`orchestrator/service/ports`, e.g. `RunStatusReader`), implemented by an `orchestrator/adapters/grpc` client with a `var _ RunStatusReader = (*impl)(nil)` assertion. The watchdog already establishes this exact shape (it holds a state gRPC client behind an interface to call `CancelSchedule`); the sweep would reuse that pattern. If state exposes no suitable terminal-status query today, adding one is a state-side change and a cost to weigh against the value of the sweep.

Because Option 1 fixes emission at the source, this sweep is a **pure backstop**, not the primary mechanism: `run.finalized:v1` consumer-group redelivery already covers transient projection misses, and the one-off backfill (§6.6) clears the existing six zombies. Recommendation: ship Option 1 + §6.6 first; add the sweep only if operational experience shows residual drift, and only via the gRPC port above — never a direct DB read.

### 6.5 Cancel-before-snapshot race — real, handled by snapshot guard + reconciliation

A run exists in **state** the moment it is triggered, but its Neo4j `:Run` is created **asynchronously** by the orchestrator's snapshot (on `scheduler.started:v1`). A cancel issued immediately after trigger emits `run.finalized:v1{cancelled}`, which can be consumed *before* the snapshot has created the `:Run`. `RunAggregateRepository.FinalizeRun` does `MATCH (run:Run {run_id})`, matches nothing, returns nil (ACK); the later snapshot then MERGEs a non-terminal `:Run` with nothing to re-finalize it — a stuck active run.

Broadening `FinalizeRun` to "retry on `MATCH=0`" is **not** a safe fix: a `:Run` can be legitimately absent (retention-deleted by `DeleteExpiredRuns`, never-snapshotted, stale redelivery), so an unbounded retry would poison-loop. The race is closed by two bounded mechanisms instead:

- **Snapshot stamps terminal on create.** `snapshotsvc.Service` checks the `cancelled_schedules` guard (`CancelledSchedulesRepository.Exists`) before writing; when set, `snapshot.Params.Cancelled` flows to the writer, whose `MERGE … ON CREATE` stamps `completed_at` + `terminal_status='cancelled'`. Deterministic, no retry, covers every snapshot entry point (cron/trigger/rerun/rebase/single-node). `FinalizeRun` deliberately stays a no-op on a missing `:Run`.
- **Reconciliation sweep (§6.4).** Catches the residual sub-race (snapshot commits before the guard insert) and any other missed projection.

### 6.6 Backfill of the six existing zombies

The §6.4 sweep finalizes them automatically once deployed. If we want them cleared sooner, a one-off Cypher (run knowingly against the cluster, **not** a migration) stamps them:

```cypher
MATCH (r:Run) WHERE r.completed_at IS NULL AND r.terminal_status IS NULL
  AND r.run_id IN [$cancelled_run_ids]
SET r.completed_at = datetime(), r.terminal_status = 'cancelled'
```

### 6.7 Two events from one `Cancel()` — why it is safe (not duplication)

Option 1 makes one `Run.Cancel()` mutation emit two domain events on two streams: `RunCancelled` → `schedule.cancelled:v1` (guard, consumed by orchestrator/executor/k8s) and `RunFinalized{Outcome: cancelled}` → `run.finalized:v1` (projection, consumed by orchestrator only). This is two *distinct facts*, not a duplicated event, and it is safe by construction on three independent grounds — all verified against the code:

- **Atomic write.** The gRPC cancel path is `Begin → SaveRun → Outbox.Append(events) → Commit`. `OutboxPublisher.Append` (`state/adapters/postgres/outbox_publisher.go:36`) writes one `state_outbox` row per event using a repository bound to the *same* UoW transaction (`p.tx`). So both rows and the `scheduler_tracker` mutation commit as one unit — all-or-nothing. No partial write; the two rows can never diverge from the status change.
- **No duplicate emission.** Re-cancel cannot produce a second pair: `Run.Cancel()` returns `ErrAlreadyTerminal` on an already-terminal run, and `LoadRunForUpdate` (`SELECT … FOR UPDATE`) serializes concurrent cancels. Redelivery on the consumer side is absorbed by the `message_processing` dedup keyed on `(stream, outbox_entry_id)`.
- **Publish/consume order is irrelevant.** State's outbox processor runs in **default (non-FIFO) mode** (`state/main.go:100` — `ProcessorConfig{Tick, BatchSize}`, no `WithPerAggregateOrdering()`); `GetPendingBatch` is `ORDER BY created_at ASC`, i.e. best-effort creation order. But the two events land on different streams with independent consumer groups and separate async handlers, so effect order is arbitrary regardless of publish order. That is fine because the two effects are **independent, commutative, and idempotent**: the guard handler does `INSERT … ON CONFLICT DO NOTHING`; the projection does `FinalizeRun` with `completed_at = COALESCE(completed_at, datetime())`. Neither reads the other's state; both interleavings reach the identical final state. The transient window where only one has been consumed is harmless — a late `node.updated:v1` in that window can only mark a node terminal, never un-set `completed_at` (and `COALESCE` protects it).

**FIFO is not required and should not be enabled for this.** `WithPerAggregateOrdering()` (per-aggregate FIFO, used by executor for "RUNNING before node_deployed", `29b76a8`) exists for the case where one consumer must observe an aggregate's events *in order*. Cancel's two events go to different consumers and commute, so FIFO would only add head-of-line blocking for zero correctness gain.

**One deliberate (non-bug) decision:** in a genuine cancel-vs-succeed race, `FinalizeRun` does `SET terminal_status = $status`, so the projection's `terminal_status` reflects whatever state decided. Because state serializes the outcome to a *single* terminal status (FOR UPDATE + `ErrAlreadyTerminal`), only one of `RunFinalized{succeeded}` or `RunFinalized{cancelled}` is ever emitted for a run; Neo4j converges to that one authoritative decision. No split-brain.

### 6.8 DDD & layering compliance (strict)

This change must satisfy the repository's port-ownership rules (CLAUDE.md "Port ownership"): ports are owned by the innermost layer whose vocabulary they speak; adapters only *implement* ports (`var _ Port = (*impl)(nil)`); the dependency arrow always runs adapter → port; and `service/handlers` imports no `adapters/*`. Per-change audit:

**state — primary fix (Option 1):**

- `Run.Cancel()` is **pure domain** (`state/domain/aggregate/run/run.go`). It must stay infrastructure-free: it mutates aggregate state, sets the dirty flags on its own changeset, and returns `[]DomainEvent` (`RunCancelled`, `RunFinalized`). It must **not** know about Redis, stream names, or SQL. No port is involved at this layer.
- Mapping a domain event → a Redis stream is the **adapter's** job. `OutboxPublisher` (`state/adapters/postgres`) translates `RunFinalized` to a `state_outbox` row and **must reference the stream via `streams.RunFinalizedV1`** (never the literal `"run.finalized:v1"`), per the stream-name rule. No domain code touches `pkg/streams`.
- The `OutboxPublisher` (technical port) already lives in `state/service/ports`; the `Run`/scheduler repository (domain repository port) in `state/domain/repository`; `UnitOfWork` in `state/service/uow`; concrete impls (incl. `*UnitOfWork`) in `state/adapters/postgres`. **No new port is introduced.**
- `CancelTx` / `FinalizeRunTx` are methods on the adapter-internal scheduler-tracker repository, consumed only by the run-repository adapter (adapter→adapter wiring). They are correctly adapter-internal and stay in `state/adapters/postgres`. Folding `completed_at = NOW()` into `CancelTx` (§6.1) is an adapter-local SQL change behind the existing repository port — invisible to domain and application layers. The `CancelScheduler` gRPC handler keeps reaching persistence and outbox only through the `UnitOfWork` port (`u.Run()`, `u.Outbox()`), never an adapter import.

**orchestrator — no change required for the primary fix:**

- Option 1 needs **zero orchestrator code**: the existing `RunFinalizedHandler` projects `run.finalized:v1` through the `RunFinalizer` interface (its consumer-owned port) → `RunAggregateRepository.FinalizeRun` (adapter). The dependency direction is already correct. (Note: `RunFinalizer` is presently declared in `service/handlers` next to its consumer — the Go "accept-interfaces" idiom. It is application-owned and imports no adapter, so it satisfies the rule; a stricter reading would relocate it to `service/ports`, but this change introduces no reason to move it, so leave it.)
- This is why Option 2 was rejected on DDD grounds too: bolting `FinalizeRun` onto `ScheduleCancelledHandler` would scatter run-terminality projection across two services and leave state's own model internally inconsistent.

**orchestrator — defensive layers:**

- Read hardening (§6.3): Cypher in `adapters/neo4j` behind `RunReader`; the dedup policy owned by `RunQueryService` (application).
- Reconciliation sweep (§6.4, optional): state truth via an **orchestrator-owned** `RunStatusReader` port in `service/ports`, implemented in `adapters/grpc` (`var _ RunStatusReader = (*impl)(nil)`). **No cross-service DB read.**

**Cross-cutting checks for the PR:**

- Any new handler directory is added to `handlerDirs` in `pkg/streams/handler_imports_test.go`; the AST guard `TestServiceHandlersDoNotImportAdapters` must stay green (no `service/handlers` → `adapters/*` import is introduced — none is, here).
- No new versioned stream/group literal anywhere; `run.finalized:v1` stays referenced via `streams.RunFinalizedV1`. If `contract.yaml` comments are widened (§6.2), regenerate `streams.gen.go` and commit (CI's `go generate && git diff --exit-code`).
- Each new adapter implementation carries its `var _ Port = (*impl)(nil)` assertion.

## 7. Testing strategy

Per the repo rule "whenever you find edge cases on the logic and you solve the problem, build a proper test to avoid this issue resurfacing":

- **state domain** (`state/domain/aggregate/run/run_test.go`): `Cancel()` on a running run sets `completed_at` and `cancelled_at`, transitions to `cancelled`, and returns **both** `RunCancelled` and `RunFinalized{Outcome: cancelled}`; `Cancel()` on an already-terminal run returns `ErrAlreadyTerminal` and emits nothing. This is the regression lock for the cause.
- **state outbox** (`outbox_publisher_test.go`): a `RunFinalized{Outcome: cancelled}` maps to a `run.finalized:v1` entry with `status="cancelled"`; appending the `[RunCancelled, RunFinalized]` pair writes exactly two rows (`schedule.cancelled:v1` + `run.finalized:v1`) and, if the transaction rolls back, neither row persists (atomicity lock for §6.7).
- **state persistence** (`run_repository`/`scheduler_repository` test): saving a cancelled run sets `scheduler_tracker.completed_at` (not just `cancelled_at`).
- **orchestrator adapter** (`orchestrator_query_repository_test.go`): seed three `:Run`s for one schedule — succeeded (completed), cancelled (completed via projection), and genuinely in-flight; assert `ListActiveRuns` returns only the in-flight one. Plus the read-hardening case: two in-flight runs, assert the newest surfaces.
- **orchestrator** projection of `run.finalized:v1` with `status="cancelled"` is already covered by `RunFinalizedHandler` tests; add a `cancelled` value case.
- **reconciliation sweep** test: Neo4j run `completed_at IS NULL` whose state row is `cancelled` → sweep finalizes it.
- **e2e:** the suite already cancels `e2e-schedule` mid-flight at teardown (this is *how* the zombies were produced). Add an assertion that after a cancellation the schedule reports no active run / no drift strip, so the cluster cannot re-accumulate zombies.

## 8. Documentation impact

- `docs/arch/services/state.md` — document that cancellation is a finalizing terminal transition: it sets `completed_at`, emits `run.finalized:v1` (status `cancelled`) for projection **and** `schedule.cancelled:v1` for the work-suppression guards.
- `docs/arch/services/orchestrator.md` — note that `:Run` terminality is projected from `run.finalized:v1` for all outcomes (succeeded/failed/cancelled), plus the node-completion aggregate fast path; and that a reconciliation sweep converges the Neo4j projection to state's authoritative status.
- `pkg/events/run_lifecycle.go` + `pkg/streams/contract.yaml` comments — widen `run.finalized:v1` status to `succeeded | failed | cancelled` (regenerate `streams.gen.go`).
- `docs/superpowers/specs/2026-05-06-topology-drift-detection-design.md` §4.6 — note the invariant does not hold for cancelled runs, and "active" must mean "not terminal", not merely `completed_at IS NULL` on a projection fed only by succeeded/failed finalization.

## 9. Open questions

1. **`completed_at` on cancel — RESOLVED (fold into `CancelTx`).** Verified that no reader in state treats `completed_at IS NULL AND cancelled_at IS NOT NULL` as a meaningful state: every liveness/active check keys on `status` (`HasActiveSchedule` → `status IN ('pending','running')`; the `is_running` projection; `CancelTx`'s guard `status NOT IN ('succeeded','failed','cancelled')`; the domain's `run.go:229`), never on `completed_at`. The dashboard's per-schedule read (`GetLastRunPerSchedule`) selects `status`/`created_at`/`is_running` and does not read `completed_at` at all. The only `completed_at IS NULL` predicates in the repo are on the unrelated `schedule_catalog.removed_at`. The single observable change is that state's `GetScheduler` gRPC response (`scheduler_handler.go:321`) now carries `completed_at` for a cancelled run alongside `cancelled_at` — additive and consistent, since cancelled is terminal. So fold `completed_at = NOW()` into `CancelTx`'s `UPDATE` (one statement, no two-write ordering question).
2. **Two events on cancel — RESOLVED (see §6.7).** Both events are written into one UoW transaction (atomic, no partial write / no duplicate), and their consumer effects are independent, commutative, and idempotent, so arbitrary publish/consume order is safe. Per-aggregate FIFO is *not* needed (and should not be enabled — it would add head-of-line blocking for no gain). No consumer reacts to both: the guard keys on `schedule.cancelled:v1`, the projection on `run.finalized:v1`.
3. **Sweep cadence & ownership.** Reuse the existing `cancelled_schedules` sweeper tick in orchestrator `main.go`, or a separate reconciliation goroutine? Lean: fold into the existing tick.
