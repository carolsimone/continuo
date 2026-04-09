# startup-controller

## Purpose

`startup-controller` converts schedule-start and rerun commands into executable node dispatches.

It is responsible for:
- initializing a run after `scheduler.started:v1` (snapshot graph, pre-register tasks, dispatch roots/seeds)
- orchestrating rerun scope after `command.rerun:v1` (reset target + failed downstream, dispatch target)

This service does **not** own scheduler or task state — it only owns durable dispatch intents (`startup_outbox`).

## Owned Storage (Postgres: `continuo_startup`)

| Table | Purpose |
|---|---|
| `startup_outbox` | Dispatch intents; one row per node to be queued for execution; processed by outbox processor → `query.model:v1` |

## Inbound Interfaces

### Redis consumers

| Stream | Handler |
|---|---|
| `scheduler.started:v1` | `InitializeSchedulerHandler` — initializes a new run |
| `command.rerun:v1` | `RerunHandler` — resets scope and re-dispatches target node |

### HTTP (port 8083)

- `GET /health` — liveness probe only

## Outbound Interfaces

### Redis producer

| Stream | Description |
|---|---|
| `query.model:v1` | One message per node to execute; consumed by `executor-controller` |

`query.model:v1` payload fields:
- `outbox_entry_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema`, `table_name`, `task_id`, `job_name`, `node_type`

### gRPC to `state`

| Method | Used by |
|---|---|
| `UpdateSchedulerInitStatus` | Both handlers: gates idempotency and signals dependency-controller |
| `GetTask` (by schedule + node) | Both handlers: check task existence / fetch task_id |
| `CreateTask` | Init handler: pre-register all nodes with `max_retries=3` |
| `UpdateTask` (status → PENDING) | Init handler: ensure seed/root tasks are PENDING before dispatch |
| `ResetTask` | Rerun handler: reset target + FAILED downstream tasks |
| `ResetInProgressInitializations` | On service startup: recover `in_progress` init statuses left by a crash |
| `GetScheduler` | (used in internal checks) |
| `GetSchedulerInitStatus` | (available via adapter; used in startup recovery) |

### gRPC to `graph`

| Method | Used by |
|---|---|
| `SnapshotGraph` | Init handler: create `Run` node and `EXECUTES` edges (all PENDING) |
| `GetScheduleInitNodes` | Both handlers: get `allNodes`, `rootNodes`, `seedNodes` in one call |
| `GetTransitiveDownstream` | Rerun handler: get all non-SUCCEEDED downstream nodes |
| `UpdateNodeStatus` | Rerun handler: reset target + FAILED downstream nodes to PENDING in graph |

## Initialization Flow (`scheduler.started:v1`)

```
1. UpdateSchedulerInitStatus → "in_progress"
   → if already "completed": idempotency skip (message replay safety)

2. SnapshotGraph(run_id, schedule_name)
   → creates Run node + EXECUTES edges for all nodes (initial status: PENDING)

3. GetScheduleInitNodes(schedule_name, run_id)
   → returns: allNodes, rootNodes, seedNodes

4. Pre-register all tasks:
   For each node in allNodes:
     GetTask(schedule_id, service, schema, table)
     → if not found: CreateTask(new UUID, schedule_id, ..., max_retries=3)
     → if found: leave untouched (idempotent on retry)

5. Dispatch selection:
   - if seedNodes present → dispatch seeds only
     (model root nodes will be dispatched by dependency-controller after seeds complete)
   - if no seeds → dispatch root nodes directly

6. For each node to dispatch:
   - GetTask → ensure PENDING (UpdateTaskStatus if not)
   - ComputeJobName
   - Write outbox entry to startup_outbox (inside Postgres tx)

7. UpdateSchedulerInitStatus → "completed" (gRPC to state)

8. Commit Postgres tx (outbox entries land atomically)
```

> **Init status ordering**: `"completed"` is set via gRPC **before** the Postgres commit. This is intentional — it gates `dependency-controller`'s finalization check. If the commit fails, the state service records `"completed"` but the outbox entries are lost; a retry of the message will hit the idempotency skip at step 1 and will not re-dispatch.

## Rerun Flow (`command.rerun:v1`)

```
1. UpdateSchedulerInitStatus → "in_progress"
   → if already "completed": idempotency skip

2. GetTransitiveDownstream(schedule_name, schema, table_name)
   → returns all non-SUCCEEDED downstream nodes (may include PENDING and FAILED)

3. Reset target node in graph:
   UpdateNodeStatus(target, → PENDING, run_id)

4. Fetch target task from state:
   GetTask(schedule_id, service, schema, table)
   → if status == FAILED: ResetTask(task_id)
     (HTTP rerun handler already reset it atomically, but handles replay on crash)

5. For each FAILED downstream node:
   - UpdateNodeStatus(node, → PENDING, run_id)
   - GetTask + ResetTask

6. GetScheduleInitNodes(schedule_name, run_id)
   → to look up target node's NodeType and effective service_name from graph
   (service_name may differ if node was re-pointed to a different service during the fix)

7. Begin Postgres tx
   Write single outbox entry for target node only
   Commit

8. UpdateSchedulerInitStatus → "completed" (gRPC to state)
   AFTER outbox commit — allows dependency-controller to resume finalization
```

> **Rerun init status ordering**: `"completed"` is set **after** the Postgres commit, unlike the init flow. This is critical: `dependency-controller` skips schedule finalization while `init_status != "completed"`, preventing it from writing a terminal status while nodes are still being reset.

> **PENDING nodes not reset**: `GetTransitiveDownstream` may return PENDING nodes (never run). Only FAILED nodes are reset; PENDING ones are left as-is to avoid unnecessary churn.

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer (`scheduler.started:v1`) | Reads and dispatches to `InitializeSchedulerHandler` |
| Redis consumer (`command.rerun:v1`) | Reads and dispatches to `RerunHandler` |
| Outbox processor | Polls `startup_outbox`; publishes pending entries to `query.model:v1`; marks processed |
| Startup recovery | `ResetInProgressInitializations` called at boot; resets stale `in_progress` to `pending` |

## Reliability Patterns

- **Idempotency gate**: both handlers call `UpdateSchedulerInitStatus("in_progress")` first; if already `"completed"`, they skip without re-dispatching (replay safety)
- **Transactional outbox**: outbox entries are committed in Postgres before the outbox processor publishes to Redis
- **Task pre-registration idempotency**: `GetTask` before `CreateTask`; existing tasks are left untouched
- **Crash recovery on startup**: `ResetInProgressInitializations` reverts any run left in `"in_progress"` init state to `"pending"`, allowing the message consumer to replay and re-initialize
