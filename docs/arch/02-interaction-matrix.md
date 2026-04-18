# Interaction Matrix

## Dependency Matrix

Legend:

- `R` = read
- `W` = write
- `RW` = both
- `-` = no direct interaction found

| Service | Own Postgres | Own Neo4j | Redis | state gRPC | orchestrator gRPC | K8s API | S3 |
|---|---|---|---|---|---|---|---|
| `state` | `RW` | `-` | `RW` | server | `-` | `-` | `-` |
| `orchestrator` | `RW` | `RW` | `RW` | `-` | server | `-` | `-` |
| `executor-controller` | `RW` | `-` | `RW` | `-` | `-` | `W` | `-` |
| `k8s-controller` | `RW` | `-` | `RW` | `-` | `-` | `R` | `W` |
| `manifest-controller` | `-` | `-` | `RW` | `-` | `-` | `-` | `R` |
| `ui-service` | `-` | `-` | `W` | `RW` | `R` | `-` | `-` |

> `startup-controller` has been removed. Its responsibilities were absorbed into `orchestrator`.

## Redis Stream Matrix

| Stream | Producer(s) | Consumer(s) | Purpose |
|---|---|---|---|
| `update.graph:v1` | `ui-service` | `manifest-controller` | Trigger manifest reload from `local` or `s3` source |
| `manifest.loaded:v1` | `manifest-controller` | `orchestrator` | Topology payload for graph ingestion |
| `schedules.loaded:v1` | `orchestrator` | `state` | Reconcile `schedule_catalog` |
| `scheduler.started:v1` | `state` | `orchestrator` | Start schedule initialization; orchestrator creates run snapshot and emits `run.entries.dispatched:v1` |
| `run.entries.dispatched:v1` | `orchestrator` | `state` | All task entries with pre-assigned UUIDs; state creates task rows, sets `total_task_count`, marks run as initialized |
| `initialize.run:v1` | *(rerun command path)* | `orchestrator` | Request rerun scope resolution |
| `run.rerun.dispatched:v1` | `orchestrator` | `state` | Rerun scope resolved; state resets target task(s) to PENDING |
| `query.model:v1` | `orchestrator` | `executor-controller` | Dispatch executable nodes |
| `node.deployed:v1` | `executor-controller` | `k8s-controller` | Begin runtime monitoring |
| `check.k8s:v1` | `k8s-controller` | `k8s-controller` | Delayed re-check queue |
| `retry.task:v1` | `k8s-controller` | `executor-controller` | Re-dispatch retry deployment |
| `task.failed:v1` | `k8s-controller` | not consumed | Terminal failure event (external observability) |
| `task.status.updated:v1` | `executor-controller` (RUNNING), `k8s-controller` (SUCCEEDED/FAILED) | `state` | Task status update; drives finalization state machine in state |
| `task.execution.recorded:v1` | `k8s-controller` | `state` | Persist task execution record with timing and S3 log key |
| `node.updated:v1` | `k8s-controller` | `orchestrator` | Node terminal status projection; orchestrator unlocks downstream nodes |
| `run.finalized:v1` | `state` | *(future consumers)* | Run completed; emitted when all tasks reach terminal state |

## Outbound gRPC Calls by Service

### Calls to `state`

Internal pipeline writes to `state` are now event-driven (via Redis). The only remaining gRPC callers are UI-facing services.

| Caller | Methods used |
|---|---|
| `ui-service` | `ListAllSchedules`, `ListTasks`, `GetScheduler`, `ListTaskExecutions`, `TriggerRerun`, `TriggerSchedule` |

### Calls to `orchestrator`

| Caller | Methods used |
|---|---|
| `ui-service` | `GetScheduleGraph`, `ListRuns`, `GetRunGraph` |

## S3 Matrix

| Service | Operation type | Concrete calls |
|---|---|---|
| `manifest-controller` | read | `list_objects_v2`, `download_file` |
| `k8s-controller` | write | `PutObject` |

## Local Durable State by Service

| Service | Tables / durable structures |
|---|---|
| `state` | `scheduler_tracker`, `task_tracker`, `task_execution`, `schedule_catalog`, `state_outbox`, `processed_events` |
| `orchestrator` | Neo4j `Table`, `Run`, `DEPENDS_ON`, `EXECUTES` (with `task_id`); Postgres `message_processing`, `outbox`, `published_messages` |
| `executor-controller` | `deployment_outbox`, `processed_events` |
| `k8s-controller` | `k8s_status_outbox`, `processed_events` |
| `manifest-controller` | none |
| `ui-service` | none |
