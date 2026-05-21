# Design: Attempt-monotonic task-status transitions

**Issue:** #73 — `state: make task-status transitions attempt-monotonic (ignore stale RUNNING after terminal)`
**Follow-up to:** PR #70 / issue #68 (deploy dispatcher; RUNNING / `node_deployed` split)
**Status:** Proposed

---

## 1. Problem

`task.status.updated:v1` has two producers:

- **executor-controller** emits `RUNNING` (queued announcement, fired when it deploys the job).
- **k8s-controller** emits the terminal `SUCCEEDED` / `FAILED` (observed from the live pod).

They share one stream but originate from different services watching different things, so their **processing order at `state` is not guaranteed**. PR #70's per-aggregate FIFO outbox publishing shrinks the window but cannot close it — cross-stream / cross-consumer ordering is not a property a producer can enforce.

`Run.RecordTaskStatus` (`state/domain/aggregate/run/run.go:447-456`) treats *any* non-terminal `RUNNING` that arrives after a terminal status as a k8s retry and un-fills the slot (`terminalTaskCount--`). That is correct for a **genuine retry**, but a **stale** `RUNNING` from the *original* attempt arriving late (after its own terminal) is indistinguishable by status alone — and it decrements the run's terminal counter, which can stall run finalization (the counter never re-reaches `totalTaskCount`, or double-counts on the next terminal).

```
executor: RUNNING(attempt k) ─┐
                              ├─ same stream, order not guaranteed at `state`
k8s:      SUCCEEDED(attempt k)┘

If SUCCEEDED is processed first, the late RUNNING(k) un-fills a slot that
should stay filled. terminal_task_count is now wrong for the rest of the run.
```

## 2. The discriminator

`retry_count` is already on the event and stored in `task_tracker`. Use it as the **attempt number**:

- A **genuine retry**'s `RUNNING` belongs to a *later* attempt → carries a **strictly higher** `retry_count` than the recorded terminal → **honor it** (un-fill the slot).
- A **stale** `RUNNING` belongs to the *same* attempt as the terminal already recorded → carries `retry_count <= ` the terminal → **ignore it** (no status change, no un-fill, no overwrite of stored `retry_count`).

This is order-independent: the decision depends only on the attempt numbers, not on which message landed first.

### 2.1 Truth table (target behaviour)

Given a recorded terminal at attempt `t` and an incoming `RUNNING` at attempt `r`:

| Scenario | terminal `t` | incoming RUNNING `r` | `r > t`? | Action |
|---|---|---|---|---|
| Stale RUNNING after SUCCEEDED (same attempt) | `SUCCEEDED(k)` | `r=k` | no | **ignore** |
| Stale RUNNING after FAILED (same attempt) | `FAILED(k)` | `r=k` | no | **ignore** |
| Genuine retry after retryable FAILED | `FAILED(k)` | `r=k+1` | yes | **honor (un-fill)** |
| Normal forward transition (prev non-terminal) | `RUNNING/PENDING` | any | n/a | update as today |

The rule reduces to: **honor the un-fill iff `incoming.retry_count > stored_terminal.retry_count`; otherwise ignore.**

## 3. Root cause that must be fixed first: producer `retry_count` is not attempt-consistent

The discriminator above is correct **only if** a `RUNNING` and the terminal of the *same attempt* carry the *same* `retry_count`, and a *later* attempt carries a strictly higher one. Acceptance criterion #5 calls this out as "verify producer consistency." Verification shows the producers are **not** consistent today:

| Producer site | File:line | Stamps `retry_count` = | Correct? |
|---|---|---|---|
| executor RUNNING | `executor-controller/adapters/publisher/outbox_publisher.go:79-84` | `d.TaskRetryCount` (the attempt that is starting) | ✅ |
| k8s SUCCEEDED | `k8s-controller/service/handlers/check_status_handler.go:116` | **hardcoded `0`** | ❌ |
| k8s FAILED (permanent) | `check_status_handler.go:197` | `retryCount` (the attempt that ran) | ✅ |
| k8s FAILED (retryable) | `check_status_handler.go:250` | **`retryCount+1`** (the *next* attempt) | ❌ |

Concrete failure if we ship only the aggregate change:

- **SUCCEEDED bug.** Attempt `k` runs as `RUNNING(k)`, succeeds, and `state` stores `SUCCEEDED(0)`. A late stale `RUNNING(k)` with `k>0` would satisfy `r > t` (`k > 0`) and be **wrongly honored** — the exact corruption we are trying to prevent.
- **Retryable-FAILED bug.** Attempt `k` runs as `RUNNING(k)`, fails retryably, and `state` stores `FAILED(k+1)`. The genuine retry then deploys as `RUNNING(k+1)`. Now `r = t = k+1`, so `r > t` is **false** and the genuine retry would be **wrongly ignored**, stalling the run.

### 3.1 Producer reconciliation (prerequisite to the aggregate change)

Establish the invariant: **the terminal status of attempt `k` carries `retry_count = k`, identical to that attempt's `RUNNING`; a retry is a new attempt `k+1` whose `RUNNING` carries `k+1`.**

1. **`handleSucceeded`** (`check_status_handler.go:116`): stamp `cmd.RetryCount` instead of `0`.
2. **`handleFailedWithRetry`** (`check_status_handler.go:250`): stamp the `task_status_updated` FAILED row with `retryCount` (the attempt that just ran), **not** `newRetryCount`. Keep the `task_retry` / `retry.task:v1` row at `newRetryCount = retryCount + 1` — that is the attempt number of the *next* run, which becomes the genuine retry's `RUNNING` value.

After this, every (RUNNING, terminal) pair of the same attempt shares one `retry_count`, and each retry is strictly `+1`.

### 3.2 Ripple: `HasRetryableFailed`

`HasRetryableFailedTaskTx` (`state/adapters/postgres/task_repository.go:457-473`) gates run finalization on `status='failed' AND retry_count < max_retries`. Changing what the retryable-FAILED row stores (from `k+1` to `k`) changes this predicate and **must be checked**:

- `handleFailedWithRetry` is entered only when `retryCount < maxRetries` (`check_status_handler.go:95`). So the FAILED row it writes now has `retry_count = k < maxRetries` ⇒ `HasRetryableFailed` is **true** through the whole retryable window — which is the intended meaning ("a retry is pending, don't finalize yet").
- Under the *current* `k+1` storage, the last retryable failure (`k+1 == maxRetries`) makes the predicate **false** even though a retry is in flight — a latent premature-finalization hazard. The reconciliation removes it.
- `handleFailedPermanent` already stores `retryCount` (`= maxRetries` at exhaustion) ⇒ predicate false ⇒ finalize proceeds with FAILED outcome. Unchanged and still correct.

This ripple is a net correctness improvement, but it changes finalize timing, so it gets explicit tests (§6).

## 4. Placement

The transition decision lives in the **`Run` aggregate** (`RecordTaskStatus`), per the issue's stated preference and because `state` owns task status. The repository's only new responsibility is to **supply the prior status *and* its stored attempt**; it makes no decision.

## 5. Changes in `state`

### 5.1 Port: `TaskCollection`

`GetStatus` currently returns `(status, exists, err)`. Two options:

- **(Recommended) Add a sibling** `GetStatusAndAttempt(ctx, taskID) (status TaskStatus, retryCount int32, exists bool, err error)`. Leaves the existing `GetStatus` caller `ResetTaskToPending` (`run.go:605`) untouched.
- *(Alternative)* Widen `GetStatus` to also return `retryCount`. Fewer methods, but churns the `ResetTaskToPending` call site and the test doubles for no behavioural reason.

Recommend the sibling. Adapter wiring:

- `TaskCollectionAdapter.GetStatusAndAttempt` (`state/adapters/postgres/task_collection_adapter.go`) delegating to a new `GetStatusAndAttemptTx`.
- `GetStatusAndAttemptTx` (`state/adapters/postgres/task_repository.go`): `SELECT COALESCE(status,''), COALESCE(retry_count,0) FROM task_tracker WHERE task_id=$1`, same `sql.ErrNoRows → ("", 0, false)` fallback as `GetStatusTx`.

### 5.2 `Run.RecordTaskStatus` restructure

The stale check must happen **before** `UpdateStatusIfChanged`, because that statement flips status to `RUNNING` and overwrites the stored `retry_count` with the (stale, lower) incoming value. The fix returns early without touching the row.

Sketch (replacing `run.go:413` read and the `447-456` un-fill block):

```go
prev, prevAttempt, exists, err := tasks.GetStatusAndAttempt(ctx, taskID)
// ... existing read-failure / not-projected handling, now carrying prevAttempt ...

prevWasTerminal := prev != "" && prev.IsTerminal()

// Attempt-monotonic guard: a non-terminal status arriving for an attempt that
// is not strictly newer than the recorded terminal is a stale duplicate from
// the original attempt (cross-producer reordering). Ignore it entirely — no
// status regression, no un-fill, no overwrite of the stored attempt.
if !newStatus.IsTerminal() && prevWasTerminal && retryCount <= prevAttempt {
    return nil, nil
}

affected, err := tasks.UpdateStatusIfChanged(ctx, taskID, newStatus, retryCount)
// ... unchanged ...

if !newStatus.IsTerminal() {
    if prevWasTerminal {
        // Genuine retry (retryCount > prevAttempt): un-fill the slot.
        r.terminalTaskCount--
        if r.terminalTaskCount < 0 {
            r.terminalTaskCount = 0
        }
        r.changes.terminalTaskCountDirty = true
    }
    return nil, nil
}
// terminal path unchanged.
```

Notes:

- The early return preserves the terminal row's `status` *and* `retry_count`, so subsequent reads still see the true terminal attempt.
- Terminal transitions are unaffected; the guard only fires for non-terminal-after-terminal.
- The `ErrTaskRowNotProjected` transient path and the lenient read-failure fallback (`run.go:414-433`) are preserved; `prevAttempt` simply defaults to `0` on a degraded read, which keeps the conservative behaviour (a `RUNNING(0)` after a degraded read just won't be treated as stale).

## 6. Tests

### 6.1 Aggregate unit tests — `state/domain/aggregate/run`
- **Stale RUNNING after SUCCEEDED ignored:** `SUCCEEDED(k)` recorded, then `RUNNING(k)` ⇒ no un-fill, `terminal_task_count` unchanged, status stays SUCCEEDED.
- **Stale RUNNING after FAILED ignored:** `FAILED(k)` recorded, then `RUNNING(k)` ⇒ no un-fill.
- **Genuine retry honored:** `FAILED(k)` recorded, then `RUNNING(k+1)` ⇒ un-fill (`terminal_task_count--`), status RUNNING.
- **Normal forward transition unaffected:** `PENDING/RUNNING → SUCCEEDED/FAILED` increments as today.
- **Boundary:** `RUNNING(k)` after `FAILED(k)` is a no-op even when it is the message that would otherwise complete the run (assert no spurious `RunFinalized`).

The fake `TaskCollection` gains `GetStatusAndAttempt` returning a configurable `(status, attempt)`.

### 6.2 Producer consistency tests
- **k8s `handleSucceeded`** stamps `task_status_updated.retry_count == cmd.RetryCount` (regression guard for the hardcoded-`0` bug).
- **k8s `handleFailedWithRetry`** stamps the FAILED row with `retryCount` and the `retry.task` row with `retryCount+1` (asserts the two rows now diverge intentionally).
- Optional integration assertion: for one attempt, executor's `RUNNING` and k8s's terminal carry equal `retry_count`.

### 6.3 Finalize-timing regression
- A run with a retryable FAILED (`retry_count=k < max_retries`) is **not** finalized while the retry is pending (`HasRetryableFailed` true), and **is** finalized once the retry terminates. Guards the §3.2 ripple.

## 7. Acceptance criteria mapping

| Criterion | Covered by |
|---|---|
| `RUNNING` with `retry_count <= ` terminal attempt is a no-op | §5.2 guard + §6.1 first two cases |
| `RUNNING` with `retry_count > ` terminal attempt honored as retry | §5.2 + §6.1 genuine-retry case |
| Decision in the `Run` aggregate; repo only supplies status + attempt | §4, §5.1 (`GetStatusAndAttempt`), §5.2 |
| Unit tests for stale / genuine / normal | §6.1 |
| Verify producer `retry_count` consistency | §3 (found inconsistent) + §3.1 reconciliation + §6.2 |

## 8. Rollout / compatibility

- **No schema migration.** `retry_count` already exists on `task_tracker` and on the event; only the values producers stamp and the aggregate's read change.
- **In-flight runs across deploy.** A run mid-flight when the producers change could have one terminal row written under the old convention (e.g. a retryable `FAILED(k+1)`); a genuine retry's `RUNNING(k+1)` would then read `r == t` and be ignored once. This is a single-window, single-task edge during the rollout, self-corrects on the task's next terminal, and is bounded by the existing PR #70 FIFO mitigation. Deploy `state` and the two producer services together to minimize it; no special migration step.
- **Ordering of the two changes.** The producer reconciliation (§3.1) and the aggregate guard (§5.2) are correct independently but only *jointly* close the race. Ship them in one change set / one PR.

## 9. Files touched

- `state/domain/aggregate/run/task_collection.go` — add `GetStatusAndAttempt` to the port.
- `state/domain/aggregate/run/run.go` — `RecordTaskStatus` read + guard restructure.
- `state/adapters/postgres/task_collection_adapter.go` — implement `GetStatusAndAttempt`.
- `state/adapters/postgres/task_repository.go` — add `GetStatusAndAttemptTx`.
- `k8s-controller/service/handlers/check_status_handler.go` — `handleSucceeded` (`:116`) and `handleFailedWithRetry` (`:250`) `retry_count` stamping.
- Tests in `state/domain/aggregate/run/` and `k8s-controller/service/handlers/`.
- `docs/arch/*` — reconcile the task-status / retry semantics (state-machine + sequence flows) once implemented, per the repo's architecture-documentation working agreement.
