# startup-controller

## Purpose

`startup-controller` converts schedule-start and rerun commands into executable node dispatches.

It is responsible for:
- initializing a run after `scheduler.started:v1` (request run snapshot via `initialize.run:v1`, pre-register tasks, dispatch roots/seeds)
- orchestrating rerun dispatch after `rerun.ready:v1` (reset target/downstream tasks, dispatch target)

This service does **not** own scheduler or task state -- it only owns durable dispatch intents (`startup_outbox`).

## Owned Storage (Postgres: `continuo_startup`)

| Table | Purpose |
|---|---|
| `startup_outbox` | Dispatch intents; one row per node to be queued for execution; processed by outbox processor -> `query.model:v1` |

## Inbound Interfaces

### Redis consumers

| Stream | Handler |
|---|---|
| `scheduler.started:v1` | `InitializeSchedulerHandler` -- initiates a new run by publishing `initialize.run:v1` |
| `run.initialized:v1` | `RunInitializedHandler` -- receives run snapshot from orchestrator, pre-registers tasks, dispatches roots/seeds |
| `rerun.ready:v1` | `RerunReadyHandler` -- receives rerun scope from orchestrator, resets tasks, dispatches target |

### HTTP (port 8083)

- `GET /health` -- liveness probe only

## Outbound Interfaces

### Redis producer

| Stream | Description |
|---|---|
| `initialize.run:v1` | Request to orchestrator for run snapshot creation (with optional rerun target) |
| `query.model:v1` | One message per node to execute; consumed by `executor-controller` |

`query.model:v1` payload fields:
- `outbox_entry_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema`, `table_name`, `task_id`, `job_name`, `node_type`

### gRPC to `state`

| Method | Used by |
|---|---|
| `UpdateSchedulerInitStatus` | Both handlers: gates idempotency and signals orchestrator |
| `GetTask` (by schedule + node) | Both handlers: check task existence / fetch task_id |
| `CreateTask` | Init handler: pre-register all nodes with `max_retries=3` |
| `UpdateTask` (status -> PENDING) | Init handler: ensure seed/root tasks are PENDING before dispatch |
| `ResetTask` | Rerun handler: reset target + FAILED downstream tasks |
| `ResetInProgressInitializations` | On service startup: recover `in_progress` init statuses left by a crash |
| `GetScheduler` | (used in internal checks) |
| `GetSchedulerInitStatus` | (available via adapter; used in startup recovery) |

## Initialization Flow (`scheduler.started:v1`)

```
1. UpdateSchedulerInitStatus -> "in_progress"
   -> if already "completed": idempotency skip (message replay safety)

2. Publish initialize.run:v1 (schedule_name, run_id)
   -> orchestrator consumes, creates Run node + EXECUTES edges

3. Consume run.initialized:v1 from orchestrator
   -> returns: allNodes, rootNodes, seedNodes

4. Pre-register all tasks:
   For each node in allNodes:
     GetTask(schedule_id, service, schema, table)
     -> if not found: CreateTask(new UUID, schedule_id, ..., max_retries=3)
     -> if found: leave untouched (idempotent on retry)

5. Dispatch selection:
   - if seedNodes present -> dispatch seeds only
     (model root nodes will be dispatched by orchestrator after seeds complete)
   - if no seeds -> dispatch root nodes directly

6. For each node to dispatch:
   - GetTask -> ensure PENDING (UpdateTaskStatus if not)
   - ComputeJobName
   - Write outbox entry to startup_outbox (inside Postgres tx)

7. UpdateSchedulerInitStatus -> "completed" (gRPC to state)

8. Commit Postgres tx (outbox entries land atomically)
```

> **Init status ordering**: `"completed"` is set via gRPC **before** the Postgres commit. This is intentional -- it gates `orchestrator`'s finalization check. If the commit fails, the state service records `"completed"` but the outbox entries are lost; a retry of the message will hit the idempotency skip at step 1 and will not re-dispatch.

## Rerun Flow (`rerun.ready:v1`)

```
1. Consume rerun.ready:v1 from orchestrator
   -> contains target node info + downstream FAILED nodes (already reset to PENDING in graph)
   -> each NodePayload carries:
      - service_name: current graph value — used for K8s job dispatch
      - original_service_name (optional): original value from the rerun command — used for
        task lookup in state (in case the service was renamed/fixed since the original run)
      - schedule_name: schedule name for the run

2. Fetch target task from state:
   GetTask(schedule_id, node.LookupServiceName(), schema, table)
   -> LookupServiceName() returns original_service_name if set, else service_name
   -> if status == FAILED: ResetTask(task_id)
   -> K8s job dispatch uses node.ServiceName (current graph value)

3. For each FAILED downstream node:
   - GetTask(using LookupServiceName()) + ResetTask

4. Begin Postgres tx
   Write single outbox entry for target node only
   Commit

5. UpdateSchedulerInitStatus -> "completed" (gRPC to state)
   AFTER outbox commit -- allows orchestrator to resume finalization
```

> **Rerun init status ordering**: `"completed"` is set **after** the Postgres commit, unlike the init flow. This is critical: `orchestrator` skips schedule finalization while `init_status != "completed"`, preventing it from writing a terminal status while nodes are still being reset.

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer (`scheduler.started:v1`) | Reads and dispatches to `InitializeSchedulerHandler` |
| Redis consumer (`run.initialized:v1`) | Reads and dispatches to `RunInitializedHandler` |
| Redis consumer (`rerun.ready:v1`) | Reads and dispatches to `RerunReadyHandler` |
| Outbox processor | Polls `startup_outbox`; publishes pending entries to `query.model:v1`; marks processed |
| Startup recovery | `ResetInProgressInitializations` called at boot; resets stale `in_progress` to `pending` |

## Reliability Patterns

- **Idempotency gate**: handlers call `UpdateSchedulerInitStatus("in_progress")` first; if already `"completed"`, they skip without re-dispatching (replay safety)
- **Transactional outbox**: outbox entries are committed in Postgres before the outbox processor publishes to Redis
- **Task pre-registration idempotency**: `GetTask` before `CreateTask`; existing tasks are left untouched
- **Crash recovery on startup**: `ResetInProgressInitializations` reverts any run left in `"in_progress"` init state to `"pending"`, allowing the message consumer to replay and re-initialize
