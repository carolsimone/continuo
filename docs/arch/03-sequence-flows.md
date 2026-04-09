# Sequence Flows

## 1. Schedule Startup

```mermaid
sequenceDiagram
  participant Cron as state cron
  participant ST as state
  participant R as Redis
  participant SC as startup-controller
  participant GR as graph
  participant EC as executor-controller
  participant KC as k8s-controller
  participant DC as dependency-controller

  Cron->>ST: activate schedule
  ST->>ST: create scheduler_tracker + state_outbox
  ST->>R: publish scheduler.started:v1
  R->>SC: consume scheduler.started:v1
  SC->>ST: UpdateSchedulerInitStatus(in_progress)
  SC->>GR: SnapshotGraph(run_id, schedule_name)
  SC->>GR: GetScheduleInitNodes(schedule_name, run_id)
  SC->>ST: pre-register tasks for all nodes
  SC->>SC: write startup_outbox for roots/seeds
  SC->>ST: UpdateSchedulerInitStatus(completed)
  SC->>R: publish query.model:v1
  R->>EC: consume query.model:v1
  EC->>EC: write deployment_outbox
  EC->>KC: create K8s job and mark task RUNNING
  EC->>R: publish executor.deployed:v1
  R->>KC: consume executor.deployed:v1
  KC->>KC: start runtime monitoring loop
```

## 2. Steady-State Success Path

```mermaid
sequenceDiagram
  participant R as Redis
  participant KC as k8s-controller
  participant ST as state
  participant DC as dependency-controller
  participant GR as graph
  participant EC as executor-controller

  R->>KC: executor.deployed:v1
  KC->>KC: GetJobStatus
  KC->>ST: GetTask(task_id)
  KC->>KC: write k8s_status_outbox(task_succeeded + node_status_updated)
  KC->>ST: UpdateTask(status=succeeded)
  KC->>ST: CreateTaskExecution(...)
  KC->>R: publish update.table:v1

  R->>DC: consume update.table:v1
  DC->>GR: UpdateNodeStatus(SUCCEEDED)
  DC->>GR: GetReadyDownstream(...)
  DC->>ST: ensure downstream tasks exist
  DC->>DC: write outbox
  DC->>R: publish query.model:v1

  R->>EC: consume query.model:v1
  EC->>EC: write deployment_outbox
  EC->>ST: UpdateTask(status=RUNNING)
  EC->>R: publish executor.deployed:v1
```

## 3. Retry and Terminal Failure Path

```mermaid
sequenceDiagram
  participant R as Redis
  participant KC as k8s-controller
  participant ST as state
  participant S3 as S3
  participant EC as executor-controller
  participant DC as dependency-controller
  participant GR as graph

  R->>KC: executor.deployed:v1 or k8s.check:v1
  KC->>KC: GetJobStatus -> FAILED
  KC->>ST: GetTask(task_id)
  alt retries remain
    KC->>KC: write k8s_status_outbox(task_retry)
    KC->>ST: UpdateTask(status=failed, retry_count+1)
    KC->>ST: CreateTaskExecution(...)
    KC->>S3: upload pod logs
    KC->>R: publish task.retry:v1
    R->>EC: consume task.retry:v1
    EC->>ST: UpdateTask(status=RUNNING)
    EC->>R: publish executor.deployed:v1
  else retries exhausted
    KC->>KC: write k8s_status_outbox(task_failed + node_status_updated)
    KC->>ST: UpdateTask(status=failed)
    KC->>ST: CreateTaskExecution(...)
    KC->>S3: upload pod logs
    KC->>R: publish task.failed:v1
    KC->>R: publish update.table:v1
    R->>DC: consume update.table:v1
    DC->>GR: UpdateNodeStatus(FAILED)
    DC->>GR: CheckScheduleCompletion(...)
    DC->>ST: UpdateScheduler(FAILED) when drained
    DC->>GR: FinalizeRun(FAILED)
  end
```

## 4. Rerun Flow

```mermaid
sequenceDiagram
  participant U as user/API client
  participant ST as state
  participant R as Redis
  participant SC as startup-controller
  participant GR as graph
  participant EC as executor-controller

  U->>ST: POST /schedules/{id}/rerun
  ST->>ST: reset scheduler + target task + write state_outbox
  ST->>R: publish command.rerun:v1
  R->>SC: consume command.rerun:v1
  SC->>ST: UpdateSchedulerInitStatus(in_progress)
  SC->>GR: GetTransitiveDownstream(target)
  SC->>GR: UpdateNodeStatus(target/downstream FAILED nodes -> PENDING)
  SC->>ST: ResetTask(target/downstream FAILED tasks)
  SC->>GR: GetScheduleInitNodes(... ) for node_type/service lookup
  SC->>SC: write startup_outbox for target only
  SC->>R: publish query.model:v1
  SC->>ST: UpdateSchedulerInitStatus(completed)
  R->>EC: consume query.model:v1
```

## Why These Diagrams Are Not Enough On Their Own

These diagrams show timing and ordering well, but they do not fully show:

- who owns durable state
- which service is the source of truth for a given field
- which Redis streams are durable integration boundaries vs local loops
- which side effects are retried through outbox tables
- which flows are optional or currently unconsumed

Use these diagrams together with the service dossiers.
