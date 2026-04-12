# Interaction Matrix

## Dependency Matrix

Legend:

- `R` = read
- `W` = write
- `RW` = both
- `-` = no direct interaction found

| Service | Own Postgres | Own Neo4j | Redis | state gRPC | graph gRPC | K8s API | S3 |
|---|---|---|---|---|---|---|---|
| `state` | `RW` | `-` | `RW` | server | `-` | `-` | `-` |
| `graph` | `-` | `RW` | `-` | `-` | server | `-` | `-` |
| `startup-controller` | `RW` | `-` | `RW` | `RW` | `RW` | `-` | `-` |
| `dependency-controller` | `RW` | `-` | `RW` | `RW` | `RW` | `-` | `-` |
| `executor-controller` | `RW` | `-` | `RW` | `W` | `-` | `W` | `-` |
| `k8s-controller` | `RW` | `-` | `RW` | `RW` | `-` | `R` | `W` |
| `manifest-controller` | `-` | `-` | `RW` | `-` | `W` | `-` | `R` |
| `ui-service` | `-` | `-` | `-` | `RW` | `R` | `-` | `-` |

## Redis Stream Matrix

| Stream | Producer(s) | Consumer(s) | Purpose |
|---|---|---|---|
| `update.graph:v1` | not found in repo | `manifest-controller` | Trigger manifest reload from `local` or `s3` source |
| `schedules.loaded:v1` | `manifest-controller` | `state` | Reconcile `schedule_catalog` |
| `scheduler.started:v1` | `state` | `startup-controller` | Start schedule initialization |
| `command.rerun:v1` | `state` | `startup-controller` | Start rerun/reset flow |
| `query.model:v1` | `startup-controller`, `dependency-controller` | `executor-controller` | Dispatch executable nodes |
| `executor.deployed:v1` | `executor-controller` | `k8s-controller` | Begin runtime monitoring |
| `k8s.check:v1` | `k8s-controller` | `k8s-controller` | Delayed re-check queue |
| `task.retry:v1` | `k8s-controller` | `executor-controller` | Re-dispatch retry deployment |
| `task.failed:v1` | `k8s-controller` | not found in repo | Terminal failure event |
| `update.table:v1` | `k8s-controller` | `dependency-controller` | Node terminal status projection |

## Outbound gRPC Calls by Service

### Calls to `state`

| Caller | Methods used |
|---|---|
| `startup-controller` | `GetTaskByScheduleAndNode`, `CreateTask`, `UpdateTask`, `UpdateSchedulerInitStatus`, `GetScheduler`, `ResetInProgressInitializations`, `ResetTask`, `GetSchedulerInitStatus` |
| `dependency-controller` | `GetTaskByScheduleAndNode`, `GetSchedulerInitStatus`, `UpdateScheduler` |
| `executor-controller` | `UpdateTask` |
| `k8s-controller` | `GetTask`, `UpdateTask`, `CreateTaskExecution` |
| `ui-service` | `ListAllSchedules`, `ListTasks`, `GetScheduler`, `ListTaskExecutions`, `TriggerRerun` |

### Calls to `graph`

| Caller | Methods used |
|---|---|
| `startup-controller` | `SnapshotGraph`, `GetScheduleInitNodes`, `GetTransitiveDownstream`, `UpdateNodeStatus` |
| `dependency-controller` | `UpdateNodeStatus`, `GetReadyDownstream`, `CheckScheduleCompletion`, `FinalizeRun` |
| `manifest-controller` | `CreateNode` |
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
| `graph` | Neo4j `Table`, `Run`, `DEPENDS_ON`, `EXECUTES` |
| `startup-controller` | `startup_outbox` |
| `dependency-controller` | `message_processing`, `outbox`, `published_messages` |
| `executor-controller` | `deployment_outbox`, `processed_events` |
| `k8s-controller` | `k8s_status_outbox`, `processed_events` |
| `manifest-controller` | none |
| `ui-service` | none |
