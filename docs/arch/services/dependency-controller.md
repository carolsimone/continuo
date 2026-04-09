# dependency-controller

## Purpose

`dependency-controller` is the runtime orchestration bridge. It consumes terminal node updates, unlocks newly-ready downstream nodes, and finalizes schedule runs when all nodes drain.

It connects:
- completion signals from `k8s-controller` (via Redis)
- dependency readiness queries to `graph`
- task lookups and scheduler finalization to `state`
- downstream dispatch to `executor-controller` (via Redis)

## Owned Storage (Postgres: `continuo_dependency`)

| Table | Purpose |
|---|---|
| `message_processing` | Inbound dedup: one row per `update.table:v1` message ID; tracks state (`processing` → `completed` → `acked`) |
| `outbox` | Outbound dispatch intents: one row per downstream node ready for execution |
| `published_messages` | Outbound idempotency: records `(outbox_entry_id, redis_message_id)` after successful publish |

## Inbound Interfaces

### Redis consumer

| Stream | Description |
|---|---|
| `update.table:v1` | Terminal node status updates; consumed per message; fields: `task_id`, `schedule_id`, `schedule_name`, `service_name`, `schema`, `table_name`, `status` |

### HTTP (port 8086)

- `GET /health` — liveness probe only

## Outbound Interfaces

### Redis producer

| Stream | Trigger |
|---|---|
| `query.model:v1` | One message per newly-ready downstream node after a SUCCEEDED node is processed |

`query.model:v1` payload fields:
- `outbox_entry_id`
- `schedule_id`
- `schedule_name`
- `service_name`
- `schema`
- `table_name`
- `task_id`
- `job_name`
- `node_type`

### gRPC to `state`

| Method | When called |
|---|---|
| `GetTask` (by schedule + node) | For each ready downstream node, to retrieve pre-registered task ID |
| `GetSchedulerInitStatus` | Before finalization, to guard against premature terminal write during rerun setup |
| `UpdateScheduler` | To write SUCCEEDED or FAILED on the scheduler run when the graph drains |

### gRPC to `graph`

| Method | When called |
|---|---|
| `UpdateNodeStatus` | On every `update.table:v1` message; sets `EXECUTES.status` |
| `GetReadyDownstream` | After a SUCCEEDED node; returns downstream nodes whose all upstreams are now SUCCEEDED |
| `CheckScheduleCompletion` | After any terminal node (SUCCEEDED or FAILED); checks whether all run nodes are terminal |
| `FinalizeRun` | On schedule completion; writes `Run.completed_at` and `Run.terminal_status` in Neo4j |

## Processing Logic

### On `update.table:v1` — per message

```
1. Dedup check: insert into message_processing (INSERT IF NOT EXISTS)
   → if already completed/acked: skip and return

2. Open Postgres transaction
   a. UpdateNodeStatus in graph (outside tx — idempotent gRPC call)
   b. If status == SUCCEEDED:
      - GetReadyDownstream from graph
      - For each ready node:
          - GetTask from state (task must already exist; pre-registered by startup-controller)
          - ComputeJobName
          - Write outbox entry into `outbox` table
      - Update message_processing state → completed
   c. Commit transaction

3. After commit, if status is terminal (SUCCEEDED or FAILED):
   a. GetSchedulerInitStatus from state
      → if not "completed": skip finalization (rerun guard)
   b. CheckScheduleCompletion from graph
      → if not complete: return
   c. UpdateScheduler in state → SUCCEEDED or FAILED
   d. FinalizeRun in graph → stamp Run node with terminal status and timestamp
```

> **Rerun guard**: finalization is skipped if `initialization_status != "completed"`. This prevents `dependency-controller` from overwriting `RUNNING` with `FAILED` while `startup-controller` is still resetting nodes during a rerun. The guard is lifted once startup-controller sets `init_status = "completed"`.

> **Task pre-registration assumption**: tasks for all downstream nodes must already exist in `state` (created by `startup-controller` during initialization). If `GetTask` returns not-found, the message is treated as an error.

> **FAILED nodes**: status is still written to graph and completion is checked, but no downstream nodes are unlocked (only SUCCEEDED triggers `GetReadyDownstream`).

## Background Loops

| Loop | Description |
|---|---|
| Redis consumer | Reads `update.table:v1`; dispatches to `ProcessStatusHandler` |
| Outbox processor | Polls `outbox` for pending entries; publishes to `query.model:v1`; records in `published_messages` |

## Reliability Patterns

- **Inbound dedup**: `message_processing` keyed by Redis message ID; `INSERT IF NOT EXISTS` prevents double-processing
- **Outbound idempotency**: `published_messages` tracks published outbox entries; republishing is safe
- **Graph update is outside the Postgres tx**: `UpdateNodeStatus` is called before the transaction and is idempotent; if the tx fails the message will be redelivered and the gRPC call will be retried
- **Finalization is post-commit and best-effort**: if `checkAndFinalizeSchedule` fails, the error is logged but the message is still ACK'd (main work is already committed); a future reconciliation recovers stale records
