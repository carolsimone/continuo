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
| `orchestrator` | `RW` | `RW` | `RW` | `RW` | server | `-` | `-` |
| `startup-controller` | `RW` | `-` | `RW` | `RW` | `-` | `-` | `-` |
| `executor-controller` | `RW` | `-` | `RW` | `W` | `-` | `W` | `-` |
| `k8s-controller` | `RW` | `-` | `RW` | `RW` | `-` | `R` | `W` |
| `manifest-controller` | `-` | `-` | `RW` | `-` | `-` | `-` | `R` |
| `ui-service` | `-` | `-` | `W` | `RW` | `R` | `-` | `-` |

## Redis Stream Matrix

| Stream | Producer(s) | Consumer(s) | Purpose |
|---|---|---|---|
| `update.graph:v1` | `ui-service` | `manifest-controller` | Trigger manifest reload from `local` or `s3` source |
| `manifest.loaded:v1` | `manifest-controller` | `orchestrator` | Topology payload for graph ingestion |
| `schedules.loaded:v1` | `orchestrator` | `state` | Reconcile `schedule_catalog` |
| `scheduler.started:v1` | `state` | `startup-controller` | Start schedule initialization |
| `initialize.run:v1` | `startup-controller` | `orchestrator` | Request run snapshot creation |
| `run.initialized:v1` | `orchestrator` | `startup-controller` | Run snapshot ready with root/seed node lists |
| `rerun.ready:v1` | `orchestrator` | `startup-controller` | Rerun scope resolved; payload carries `service_name` (current graph, for K8s dispatch) and `original_service_name` (from rerun command, for task lookup in state) |
| `query.model:v1` | `orchestrator` | `executor-controller` | Dispatch executable nodes |
| `node.deployed:v1` | `executor-controller` | `k8s-controller` | Begin runtime monitoring |
| `check.k8s:v1` | `k8s-controller` | `k8s-controller` | Delayed re-check queue |
| `retry.task:v1` | `k8s-controller` | `executor-controller` | Re-dispatch retry deployment |
| `task.failed:v1` | `k8s-controller` | not consumed | Terminal failure event |
| `node.updated:v1` | `k8s-controller` | `orchestrator` | Node terminal status projection |

## Outbound gRPC Calls by Service

### Calls to `state`

| Caller | Methods used |
|---|---|
| `startup-controller` | `GetTaskByScheduleAndNode`, `CreateTask`, `UpdateTask`, `UpdateSchedulerInitStatus`, `GetScheduler`, `ResetInProgressInitializations`, `ResetTask`, `GetSchedulerInitStatus` |
| `orchestrator` | `GetTaskByScheduleAndNode`, `GetSchedulerInitStatus`, `UpdateScheduler` |
| `executor-controller` | `UpdateTask` |
| `k8s-controller` | `GetTask`, `UpdateTask`, `CreateTaskExecution` |
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
| `orchestrator` | Neo4j `Table`, `Run`, `DEPENDS_ON`, `EXECUTES`; Postgres `message_processing`, `outbox`, `published_messages` |
| `startup-controller` | `startup_outbox` |
| `executor-controller` | `deployment_outbox`, `processed_events` |
| `k8s-controller` | `k8s_status_outbox`, `processed_events` |
| `manifest-controller` | none |
| `ui-service` | none |
