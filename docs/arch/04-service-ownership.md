# Service Ownership Quick Reference

This sheet is the fastest way to answer three questions for each service:

- What durable state does it own?
- Which gRPC server surface does it own?
- Which Redis streams does it consume and produce?

Use this before diving into the full service dossiers.

## `state`

| Category | Owned / used surface |
|---|---|
| Durable state | `scheduler_tracker`, `task_tracker`, `task_execution`, `schedule_catalog`, `state_outbox`, `processed_events` |
| gRPC server methods owned | `CreateScheduler`, `GetScheduler`, `UpdateScheduler`, `CancelScheduler`, `UpdateSchedulerInitStatus`, `ResetInProgressInitializations`, `ActivateSchedule`, `ListAllSchedules`, `TriggerSchedule`, `CancelSchedule`, `CreateTask`, `GetTask`, `GetTaskByScheduleAndNode`, `UpdateTask`, `DeleteTask`, `ListTasks`, `ResetTask`, `GetSchedulerInitStatus`, `CreateTaskExecution`, `GetTaskExecution`, `ListTaskExecutions` |
| Redis consumes | `schedules.loaded:v1` |
| Redis produces | `scheduler.started:v1`, `command.rerun:v1` |
| Outbound gRPC calls | none |

## `graph`

| Category | Owned / used surface |
|---|---|
| Durable state | Neo4j `Table` nodes, `Run` nodes, `DEPENDS_ON` edges, `EXECUTES` edges |
| gRPC server methods owned | `CreateNode`, `UpdateNodeTimestamp`, `GetStaleRootNodes`, `GetDownstreamDependencies`, `CheckUpstreamFreshness`, `GetScheduleGraph`, `GetScheduleInitNodes`, `UpdateNodeStatus`, `GetReadyDownstream`, `CheckScheduleCompletion`, `SnapshotGraph`, `FinalizeRun`, `ListRuns`, `GetRunGraph`, `GetTransitiveDownstream` |
| Redis consumes | none |
| Redis produces | none |
| Outbound gRPC calls | none |

## `startup-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | `startup_outbox` |
| gRPC server methods owned | none |
| Redis consumes | `scheduler.started:v1`, `command.rerun:v1` |
| Redis produces | `query.model:v1` |
| Outbound gRPC calls | `state`: `GetTaskByScheduleAndNode`, `CreateTask`, `UpdateTask`, `UpdateSchedulerInitStatus`, `GetScheduler`, `ResetInProgressInitializations`, `ResetTask`, `GetSchedulerInitStatus`; `graph`: `SnapshotGraph`, `GetScheduleInitNodes`, `GetTransitiveDownstream`, `UpdateNodeStatus` |

## `dependency-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | `message_processing`, `outbox`, `published_messages` |
| gRPC server methods owned | none |
| Redis consumes | `update.table:v1` |
| Redis produces | `query.model:v1` |
| Outbound gRPC calls | `state`: `GetTaskByScheduleAndNode`, `GetSchedulerInitStatus`, `UpdateScheduler`; `graph`: `UpdateNodeStatus`, `GetReadyDownstream`, `CheckScheduleCompletion`, `FinalizeRun` |

## `executor-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | `deployment_outbox`, `processed_events` |
| gRPC server methods owned | none |
| Redis consumes | `query.model:v1`, `task.retry:v1` |
| Redis produces | `executor.deployed:v1` |
| Outbound gRPC calls | `state`: `UpdateTask` |

## `k8s-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | `k8s_status_outbox`, `processed_events` |
| gRPC server methods owned | none |
| Redis consumes | `executor.deployed:v1`, `k8s.check:v1` |
| Redis produces | `k8s.check:v1`, `task.retry:v1`, `task.failed:v1`, `update.table:v1` |
| Outbound gRPC calls | `state`: `GetTask`, `UpdateTask`, `CreateTaskExecution` |

## `manifest-controller`

| Category | Owned / used surface |
|---|---|
| Durable state | none |
| gRPC server methods owned | none |
| Redis consumes | `update.graph:v1` |
| Redis produces | `schedules.loaded:v1` |
| Outbound gRPC calls | `graph`: `CreateNode` |

## `ui-service`

| Category | Owned / used surface |
|---|---|
| Durable state | none |
| gRPC server methods owned | none |
| Redis consumes | none |
| Redis produces | none |
| Outbound gRPC calls | `state`: `ListAllSchedules`, `ListTasks`, `GetScheduler`, `ListTaskExecutions`; `graph`: `GetScheduleGraph`, `ListRuns`, `GetRunGraph` |
