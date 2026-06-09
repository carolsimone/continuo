# State Machine: Task & Scheduler Transition Flow and Service Ownership

Source of truth: `state/domain/aggregate/run/` (`run.go` aggregate methods; `status.go` scheduler transition table)

---

## TaskTracker

### States

```
pending → running → succeeded
                 ↘ failed → running   (direct re-run via executor-controller)
       ↘ skipped                       (cascade-skip when an upstream node fails)
```

| State | Description |
|---|---|
| `pending` | Task created or reset, waiting to be picked up by the executor |
| `running` | Task is actively being executed |
| `succeeded` | Task completed successfully (terminal) |
| `failed` | Task execution failed; eligible for retry (quasi-terminal) |
| `cancelled` | Task was cancelled (terminal; handled outside the transition table) |
| `skipped` | Task was cascade-skipped because an upstream node failed (terminal; set by `orchestrator` via `task.status.updated:v1`, never executed) |

### Transitions

| From | To | Produced by | Trigger |
|---|---|---|---|
| `pending` | `running` | `executor-controller` | Task picked up from the dependency-resolved queue |
| `failed` | `running` | `executor-controller` | Direct re-run of a failed task |
| `running` | `succeeded` | `k8s-controller` | Kubernetes job completes successfully |
| `running` | `failed` | `k8s-controller` | Kubernetes job errors or times out |
| `pending` | `skipped` | `orchestrator` | Upstream node failed; the run aggregate cascade-skips the still-pending downstream and emits `task.status.updated:v1` (`"skipped"`) |

Each transition is produced by exactly one service — the producer of the
corresponding `task.status.updated:v1` event (or, for `skipped`, the
orchestrator cascade). There is no caller-authorization table on task
transitions: `state` applies whatever status the producer reports, ordered by
attempt number, via `Run.RecordTaskStatus`.

### Application

Task status is applied by `Run.RecordTaskStatus` in
`state/domain/aggregate/run/run.go`, which consumes `task.status.updated:v1`
and orders updates by attempt (`retry_count`) so the projection is independent
of delivery order — see **Attempt-monotonic status updates** below. There is no
per-transition `(from, to, owner)` table; status is a straight, attempt-ordered
projection of the events the executor and k8s controllers emit.

### Notes

- `cancelled` has no entries in the transition table. Cancellation is handled via a dedicated `CancelTask` path.
- `skipped` is set only by the `orchestrator` cascade (`task.status.updated:v1`), never by the executor pipeline. The gRPC `TaskStatus` enum includes `TASK_STATUS_SKIPPED`, and the read handlers map the domain `skipped` status to it, so task-read APIs (e.g. the run NODES panel) report skipped nodes as `SKIPPED` rather than `UNSPECIFIED`.

### Attempt-monotonic status updates

`task.status.updated:v1` has two producers — **executor-controller** emits `running`, **k8s-controller** emits the terminal `succeeded` / `failed`. The two messages for one task ride the same stream but originate from different services, so `state` can process them out of order: a `running` from the original attempt may arrive *after* that attempt's terminal status.

`retry_count` is the **attempt number** and disambiguates this. Producers stamp it so that a `running` and the terminal of the *same* attempt carry the *same* `retry_count`, and a retry is a strictly newer attempt:

- executor-controller stamps the attempt it is starting on `running`.
- k8s-controller stamps the attempt that ran on the terminal `succeeded` / `failed`.
- on a retryable failure, k8s-controller records `failed` at the attempt that ran and dispatches the retry at `attempt + 1`; the retry then runs as `running` with that higher number.

The `Run` aggregate (`RecordTaskStatus`) orders **every** update by attempt, so the projection is independent of processing order:

- an update for an **older** attempt (lower `retry_count`) is superseded and ignored — whether it is a stale `running` or a stale terminal;
- for the **same** attempt, the first terminal fills the slot (`terminal_task_count++`); a `running` re-delivered after that attempt's terminal is ignored (no un-fill, no status regression);
- for a **newer** attempt, a `running` after a terminal is a genuine retry and un-fills the slot (`terminal_task_count--`), while a terminal after an older terminal advances the stored attempt **without** double-counting and re-checks finalization (a retryable failure followed by a permanent one must finalize).

The decision lives entirely in the aggregate; the repository supplies the prior status and stored attempt (under a `FOR UPDATE` lock so concurrent deliveries for one task serialize) and persists what the aggregate decides. This closes the cross-producer race regardless of delivery order. The structural follow-up — consolidating to a single producer so the reordering cannot arise — is tracked separately.

---

## SchedulerTracker

`SchedulerTracker` has **two independent state fields** that serve different purposes:

| Field | Purpose | Managed by |
|---|---|---|
| `status` | Scheduler lifecycle (`pending / running / succeeded / failed / cancelled`) | `UpdateScheduler` gRPC + `CancelScheduler` |
| `initialization_status` | DAG-projection progress (`pending / in_progress / completed / failed`) | `state` (internal, on `run.entries.dispatched:v1`) |

These are separate concerns. `initialization_status` tracks whether the `orchestrator` has projected the run's DAG and dispatched its task entries — it is **not** part of the lifecycle state machine. However, it has one side effect on `status`: when `initialization_status` reaches `"completed"`, the `state` service internally triggers `status: pending → running`. This is the only coupling between the two fields.

---

### `status` — Lifecycle State Machine

```
[creation] → pending → running → succeeded
                    ↘          ↘ failed
              cancelled ←───────┘
              (from pending or running, via CancelScheduler only)
```

| State | Description |
|---|---|
| `pending` | Scheduler created; graph initialization not yet complete |
| `running` | Graph initialized; tasks are actively being executed |
| `succeeded` | All tasks completed successfully (terminal) |
| `failed` | One or more tasks failed permanently (terminal) |
| `cancelled` | Run was cancelled externally (terminal) |

#### Allowed Transitions

| From | To | Owner | Trigger |
|---|---|---|---|
| `pending` | `running` | `state` service (internal) | Side effect of `initialization_status` reaching `"completed"` when the dispatched projection is accepted — see below |
| `running` | `succeeded` | `state` service (internal) | `TaskStatusUpdatedHandler` finalises `scheduler_tracker` when `terminal_task_count` reaches `total_task_count` with no `failed` task |
| `running` | `failed` | `state` service (internal) | `TaskStatusUpdatedHandler` finalises `scheduler_tracker` when `terminal_task_count` reaches `total_task_count` with at least one `failed` task |
| `pending` or `running` | `cancelled` | `CancelScheduler` endpoint | `repo.Cancel()` — separate path, not through `Transition()` |

#### Service Ownership

**`state` service**
- `pending → running`: never called directly by any external service. Fires inside the `Run` aggregate's `AcceptDispatch` (`state/domain/aggregate/run/run.go`) when `state` consumes `run.entries.dispatched:v1`: the same method sets `initialization_status = "completed"` and, if not all dispatched tasks are already terminal, transitions `status: pending → running`.

**`state` service (internal, via `TaskStatusUpdatedHandler`)**
- `running → succeeded`: when `terminal_task_count == total_task_count` and no task is in `failed`.
- `running → failed`: when `terminal_task_count == total_task_count` and at least one task is in `failed`.

In both cases the same SQL transaction writes a `run.finalized:v1` outbox row. The orchestrator consumes the stream and projects `terminal_status` / `completed_at` onto Neo4j `:Run`; this is the authoritative path for runs that never produce `node.updated:v1` traffic (e.g. full-inherited rebases). For runs that do produce node completions, the orchestrator's `Run` aggregate also stamps the same Neo4j fields when `terminal_count == total_nodes` during `HandleNodeCompleted`; the consumer's update is idempotent in that case.

#### Enforcement

`SchedulerTracker.Transition(to SchedulerStatus) error` in `state/domain/aggregate/run/run.go`:

- `ErrInvalidTransition` — the `(from, to)` pair is not allowed.
- Status is **only mutated on success**.
- No caller-ID check (unlike `TaskTracker`): each transition direction has exactly one owner by design.

`UpdateScheduler` gRPC handler returns `codes.FailedPrecondition` on `ErrInvalidTransition`.

---

### `initialization_status` — Graph Loading Progress

Tracks whether the run's DAG has been projected and its task entries dispatched. Managed internally by `state` when it consumes `run.entries.dispatched:v1` from the `orchestrator`. Not part of the lifecycle state machine.

```
pending → in_progress → completed
                     ↘ failed
```

| Value | Description |
|---|---|
| `pending` | Scheduler created; initialization not yet started |
| `in_progress` | Run created; awaiting the orchestrator's dispatched projection |
| `completed` | Projection accepted and child tasks created; triggers `status: pending → running` as a side effect |
| `failed` | Projection / dispatch failed |

**Owner:** `state` (internal) only.
- Created with `"in_progress"` when the run is minted.
- Sets `"completed"` inside `Run.AcceptDispatch` after the dispatched task entries are bulk-created and the local transaction is committed.

**Side effect on `status`:** Within the same `AcceptDispatch` transaction, when `initialization_status` reaches `"completed"` and `status` is still `pending`, `state` transitions `SchedulerTracker` to `running` (or straight to a terminal outcome if every dispatched task is already terminal). This is the only place `pending → running` is ever triggered.

---

### Notes

- `CreateScheduler` always creates with `status = pending` and `initialization_status = pending` (both omitted from the request; state service defaults).
- `cancelled` is not in the `Transition()` table. Cancellation goes through `repo.Cancel()` which enforces its own terminal-state guard (`ErrNotCancellable`).
