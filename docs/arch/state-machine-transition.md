# State Machine: Task & Scheduler Transition Flow and Service Ownership

Source of truth: `state/domain/model/model.go`

---

## TaskTracker

### States

```
pending → running → succeeded
       ↗         ↘ failed → pending  (retry via startup-controller)
  failed                  → running  (direct re-run via executor-controller)
```

| State | Description |
|---|---|
| `pending` | Task created or reset, waiting to be picked up by the executor |
| `running` | Task is actively being executed |
| `succeeded` | Task completed successfully (terminal) |
| `failed` | Task execution failed; eligible for retry (quasi-terminal) |
| `cancelled` | Task was cancelled (terminal; handled outside the transition table) |

### Allowed Transitions

| From | To | Owner | Trigger |
|---|---|---|---|
| `failed` | `pending` | `startup-controller` | Rerun command — resets a failed task before re-queuing |
| `pending` | `running` | `executor-controller` | Task picked up from the dependency-resolved queue |
| `failed` | `running` | `executor-controller` | Direct re-run of a failed task (skips reset-to-pending) |
| `running` | `succeeded` | `k8s-controller` | Kubernetes job completes successfully |
| `running` | `failed` | `k8s-controller` | Kubernetes job errors or times out |

### Service Ownership

Each transition is exclusively owned by one service. An attempt by the wrong caller returns `ErrUnauthorizedTransition`.

#### `startup-controller`
- `failed → pending`: resets a failed task to pending on a rerun, before re-queuing it for execution.

#### `executor-controller`
- `pending → running`: picks up a pending task and begins execution.
- `failed → running`: re-executes a failed task directly (bypasses reset-to-pending for immediate retry).

#### `k8s-controller`
- `running → succeeded`: marks the task complete after the Kubernetes job finishes successfully.
- `running → failed`: marks the task failed after the Kubernetes job errors or times out.

### Enforcement

`TaskTracker.Transition(caller CallerID, to TaskStatus) error` in `state/domain/model/model.go`:

- `ErrInvalidTransition` — the `(from, to)` pair is not in the allowed set.
- `ErrUnauthorizedTransition` — the pair exists but this caller does not own it.
- Status is **only mutated on success**.

### Notes

- `cancelled` has no entries in the transition table. Cancellation is handled via a dedicated `CancelTask` path.

### Attempt-monotonic status updates

`task.status.updated:v1` has two producers — **executor-controller** emits `running`, **k8s-controller** emits the terminal `succeeded` / `failed`. The two messages for one task ride the same stream but originate from different services, so `state` can process them out of order: a `running` from the original attempt may arrive *after* that attempt's terminal status.

`retry_count` is the **attempt number** and disambiguates this. Producers stamp it so that a `running` and the terminal of the *same* attempt carry the *same* `retry_count`, and a retry is a strictly newer attempt:

- executor-controller stamps the attempt it is starting on `running`.
- k8s-controller stamps the attempt that ran on the terminal `succeeded` / `failed`.
- on a retryable failure, k8s-controller records `failed` at the attempt that ran and dispatches the retry at `attempt + 1`; the retry then runs as `running` with that higher number.

The `Run` aggregate (`RecordTaskStatus`) uses this to keep `terminal_task_count` correct:

- a `running` whose `retry_count` is **strictly greater** than the recorded terminal is a genuine retry → it un-fills the slot (`terminal_task_count--`).
- a `running` whose `retry_count` is **≤** the recorded terminal is a stale duplicate of the already-terminated attempt → it is ignored (no status regression, no un-fill, the stored attempt is preserved).

This closes the race regardless of delivery order; the decision lives in the aggregate, with the repository only supplying the prior status and stored attempt.

---

## SchedulerTracker

`SchedulerTracker` has **two independent state fields** that serve different purposes:

| Field | Purpose | Managed by |
|---|---|---|
| `status` | Scheduler lifecycle (`pending / running / succeeded / failed / cancelled`) | `UpdateScheduler` gRPC + `CancelScheduler` |
| `initialization_status` | Graph loading progress (`pending / in_progress / completed / failed`) | `UpdateSchedulerInitStatus` gRPC |

These are separate concerns. `initialization_status` tracks whether `startup-controller` has finished loading the DAG into the graph — it is **not** part of the lifecycle state machine. However, it has one side effect on `status`: when `initialization_status` reaches `"completed"`, the `state` service internally triggers `status: pending → running`. This is the only coupling between the two fields.

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
| `pending` | `running` | `state` service (internal) | Side effect of `initialization_status` reaching `"completed"` — see below |
| `running` | `succeeded` | `state` service (internal) | `TaskStatusUpdatedHandler` finalises `scheduler_tracker` when `terminal_task_count` reaches `total_task_count` with no `failed` task |
| `running` | `failed` | `state` service (internal) | `TaskStatusUpdatedHandler` finalises `scheduler_tracker` when `terminal_task_count` reaches `total_task_count` with at least one `failed` task |
| `pending` or `running` | `cancelled` | `CancelScheduler` endpoint | `repo.Cancel()` — separate path, not through `Transition()` |

#### Service Ownership

**`state` service**
- `pending → running`: never called directly by any external service. Fires automatically inside `UpdateSchedulerInitStatus` when `initialization_status` is set to `"completed"` and `status` is still `pending`.

**`state` service (internal, via `TaskStatusUpdatedHandler`)**
- `running → succeeded`: when `terminal_task_count == total_task_count` and no task is in `failed`.
- `running → failed`: when `terminal_task_count == total_task_count` and at least one task is in `failed`.

In both cases the same SQL transaction writes a `run.finalized:v1` outbox row. The orchestrator consumes the stream and projects `terminal_status` / `completed_at` onto Neo4j `:Run`; this is the authoritative path for runs that never produce `node.updated:v1` traffic (e.g. full-inherited rebases). For runs that do produce node completions, the orchestrator's `Run` aggregate also stamps the same Neo4j fields when `terminal_count == total_nodes` during `HandleNodeCompleted`; the consumer's update is idempotent in that case.

#### Enforcement

`SchedulerTracker.Transition(to SchedulerStatus) error` in `state/domain/model/model.go`:

- `ErrInvalidTransition` — the `(from, to)` pair is not allowed.
- Status is **only mutated on success**.
- No caller-ID check (unlike `TaskTracker`): each transition direction has exactly one owner by design.

`UpdateScheduler` gRPC handler returns `codes.FailedPrecondition` on `ErrInvalidTransition`.

---

### `initialization_status` — Graph Loading Progress

Tracks whether `startup-controller` has finished loading the DAG. Managed exclusively via the `UpdateSchedulerInitStatus` gRPC call. Not part of the lifecycle state machine.

```
pending → in_progress → completed
                     ↘ failed
```

| Value | Description |
|---|---|
| `pending` | Scheduler created; initialization not yet started |
| `in_progress` | `startup-controller` is loading the graph |
| `completed` | Graph loaded; triggers `status: pending → running` as a side effect |
| `failed` | Graph loading failed |

**Owner:** `startup-controller` only.
- Sets `"in_progress"` at the start of graph initialization.
- Sets `"completed"` after all nodes and tasks are loaded and the local transaction is committed.

**Side effect on `status`:** When `UpdateSchedulerInitStatus("completed")` is called, the `state` service checks `if status == pending` and calls `SchedulerTracker.Transition(running)` internally. This is the only place `pending → running` is ever triggered.

---

### Notes

- `CreateScheduler` always creates with `status = pending` and `initialization_status = pending` (both omitted from the request; state service defaults).
- `cancelled` is not in the `Transition()` table. Cancellation goes through `repo.Cancel()` which enforces its own terminal-state guard (`ErrNotCancellable`).
- `startup-controller` never writes `status` directly — only `initialization_status`.
