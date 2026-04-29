# Sequence Flows

## 1. Schedule Startup

```mermaid
sequenceDiagram
  participant Cron as state cron
  participant ST as state
  participant R as Redis
  participant SC as startup-controller
  participant OR as orchestrator
  participant EC as executor-controller
  participant KC as k8s-controller

  Cron->>ST: activate schedule
  ST->>ST: create scheduler_tracker + state_outbox
  ST->>R: publish scheduler.started:v1
  R->>SC: consume scheduler.started:v1
  SC->>ST: UpdateSchedulerInitStatus(in_progress)
  SC->>R: publish initialize.run:v1 (schedule_name, run_id)
  R->>OR: consume initialize.run:v1
  OR->>OR: SnapshotGraph (Run node + EXECUTES edges)
  OR->>R: publish run.initialized:v1 (root/seed nodes, all nodes)
  R->>SC: consume run.initialized:v1
  SC->>ST: pre-register tasks for all nodes
  SC->>SC: write startup_outbox for roots/seeds
  SC->>ST: UpdateSchedulerInitStatus(completed)
  SC->>R: publish query.model:v1
  R->>EC: consume query.model:v1
  EC->>EC: write deployment_outbox
  EC->>KC: create K8s job and mark task RUNNING
  EC->>R: publish node.deployed:v1
  R->>KC: consume node.deployed:v1
  KC->>KC: start runtime monitoring loop
```

## 2. Steady-State Success Path

```mermaid
sequenceDiagram
  participant R as Redis
  participant KC as k8s-controller
  participant ST as state
  participant OR as orchestrator
  participant EC as executor-controller

  R->>KC: node.deployed:v1
  KC->>KC: GetJobStatus
  KC->>ST: GetTask(task_id)
  KC->>KC: write k8s_status_outbox(task_succeeded + node_status_updated)
  KC->>ST: UpdateTask(status=succeeded)
  KC->>ST: CreateTaskExecution(...)
  KC->>R: publish node.updated:v1

  R->>OR: consume node.updated:v1
  OR->>OR: UpdateNodeStatus(SUCCEEDED) in Neo4j
  OR->>OR: GetReadyDownstream(...)
  OR->>ST: ensure downstream tasks exist (GetTaskByScheduleAndNode)
  OR->>OR: write outbox
  OR->>R: publish query.model:v1

  R->>EC: consume query.model:v1
  EC->>EC: write deployment_outbox
  EC->>ST: UpdateTask(status=RUNNING)
  EC->>R: publish node.deployed:v1
```

## 3. Retry and Terminal Failure Path

```mermaid
sequenceDiagram
  participant R as Redis
  participant KC as k8s-controller
  participant ST as state
  participant S3 as S3
  participant EC as executor-controller
  participant OR as orchestrator

  R->>KC: node.deployed:v1 or check.k8s:v1
  KC->>KC: GetJobStatus -> FAILED
  KC->>ST: GetTask(task_id)
  alt retries remain
    KC->>KC: write k8s_status_outbox(task_retry)
    KC->>ST: UpdateTask(status=failed, retry_count+1)
    KC->>ST: CreateTaskExecution(...)
    KC->>S3: upload pod logs
    KC->>R: publish retry.task:v1
    R->>EC: consume retry.task:v1
    EC->>ST: UpdateTask(status=RUNNING)
    EC->>R: publish node.deployed:v1
  else retries exhausted
    KC->>KC: write k8s_status_outbox(task_failed + node_status_updated)
    KC->>ST: UpdateTask(status=failed)
    KC->>ST: CreateTaskExecution(...)
    KC->>S3: upload pod logs
    KC->>R: publish task.failed:v1
    KC->>R: publish node.updated:v1
    R->>OR: consume node.updated:v1
    OR->>OR: UpdateNodeStatus(FAILED) in Neo4j
    OR->>OR: CheckScheduleCompletion(...)
    OR->>ST: UpdateScheduler(FAILED) when drained
    OR->>OR: FinalizeRun(FAILED) in Neo4j
  end
```

## 4. Rerun Flow

```mermaid
sequenceDiagram
  participant U as user/API client
  participant UI as ui-service (BFF)
  participant ST as state (gRPC)
  participant R as Redis
  participant SC as startup-controller
  participant OR as orchestrator
  participant EC as executor-controller

  U->>UI: POST /api/schedulers/{id}/rerun
  UI->>ST: TriggerRerun(schedule_id, schema, table_name, service_name)
  ST->>ST: reset scheduler + target task + write state_outbox (atomic tx)
  ST-->>UI: TriggerRerunResponse{}
  UI-->>U: 200 OK
  ST->>R: publish scheduler.started:v1 (via OutboxProcessor)
  R->>SC: consume scheduler.started:v1
  SC->>ST: UpdateSchedulerInitStatus(in_progress)
  SC->>R: publish initialize.run:v1 (with rerun target fields)
  R->>OR: consume initialize.run:v1
  OR->>OR: GetTransitiveDownstream(target)
  OR->>OR: UpdateNodeStatus(target/downstream FAILED nodes -> PENDING)
  OR->>OR: GetNodeType(schema_name, table_name) from Neo4j
  OR->>OR: GetNodeServiceName(schema_name, table_name) from Neo4j
  OR->>R: publish rerun.ready:v1 (target_nodes: service_name=current graph value)
  R->>SC: consume rerun.ready:v1
  SC->>ST: ResetTask(target/downstream FAILED tasks, lookup by service_name, schema_name, table_name)
  SC->>SC: write startup_outbox for target only (dispatch uses service_name)
  SC->>R: publish query.model:v1
  SC->>ST: UpdateSchedulerInitStatus(completed)
  R->>EC: consume query.model:v1
```

## 5. Service Process Startup (Pre-Flight)

Before any service participates in the flows above, it runs an environment validation step:

```mermaid
sequenceDiagram
  participant OS as OS / container env
  participant M as main()
  participant V as pkg/config.Validator
  participant CFG as config.Load()
  participant SVC as service components

  M->>V: create Validator{}
  M->>CFG: Load(v) — calls LoadPostgres/LoadRedis/LoadS3 with v
  CFG->>V: v.Require(key) for each Tier-1 env var
  M->>V: v.Missing()
  alt any missing
    M->>OS: log.Error("missing required env vars", keys...) + os.Exit(1)
  else all present
    M->>SVC: initialize connections, start goroutines
  end
```

All Tier-1 required keys are accumulated before any `os.Exit(1)` so the operator sees the full list of missing vars in a single log line. The service will not attempt any DB or Redis connection until this check passes.

## 6. Schedule Cancellation

```mermaid
sequenceDiagram
  participant U as user
  participant UI as ui-service
  participant ST as state
  participant R as Redis
  participant OR as orchestrator
  participant EC as executor-controller
  participant KC as k8s-controller

  U->>UI: POST /api/schedules/:name/cancel
  UI->>ST: CancelSchedule(schedule_name, cancelled_by?, cancellation_reason?) gRPC
  ST->>ST: UPDATE scheduler_tracker → cancelled
  ST->>ST: UPDATE task_tracker (pending/running → cancelled)
  ST->>ST: INSERT state_outbox for schedule.cancelled:v1
  ST-->>UI: CancelScheduleResponse { schedule_id }
  UI-->>U: 200 OK { schedule_id }

  ST->>R: publish schedule.cancelled:v1 (via OutboxProcessor)

  par each consumer receives independently
    R->>OR: consume schedule.cancelled:v1
    OR->>OR: INSERT cancelled_schedules(schedule_id)
  and
    R->>EC: consume schedule.cancelled:v1
    EC->>EC: INSERT cancelled_schedules(schedule_id)
  and
    R->>KC: consume schedule.cancelled:v1
    KC->>KC: INSERT cancelled_schedules(schedule_id)
  end

  note over R,KC: in-flight messages are now absorbed by local guards

  R->>EC: query.model:v1 (in-flight for cancelled schedule)
  EC->>EC: SELECT EXISTS cancelled_schedules → true → drop, ack

  R->>KC: check.k8s:v1 (in-flight for cancelled schedule)
  KC->>KC: SELECT EXISTS cancelled_schedules → true → stop polling, ack

  R->>OR: node.updated:v1 (job finished before guard reached KC)
  OR->>OR: SELECT EXISTS cancelled_schedules → true → update Neo4j only, no cascade
```

Running K8s pods are left to complete naturally; their results are suppressed at the outbox layer (graceful cancellation). Rows in `cancelled_schedules` are swept after a configurable TTL (default 24h) by a background goroutine in each service.

## 7. Topology Versioning — Lazy Generation Switch

Shows what happens when `manifest.loaded:v1` arrives while a run is in-flight.

```mermaid
sequenceDiagram
  participant MC as manifest-controller
  participant R as Redis
  participant OR as orchestrator
  participant ORPG as orchestrator Postgres
  participant NEO as Neo4j

  note over MC,NEO: Run S1 is already in-flight — SnapshotGraph already committed

  MC->>R: manifest.loaded:v1 (image_tag=T2, manifest_version=V2)
  R->>OR: consume manifest.loaded:v1
  OR->>ORPG: UPDATE topology_state SET topology_generation = G1+1
  OR->>NEO: MERGE :TopologyRoot SET topology_generation=G1+1, service_metadata=...
  OR->>NEO: MERGE Table nodes SET image_tag=T2, topology_generation=G1+1

  note over NEO: Run S1 node still has topology_generation=G1 — unchanged
  note over NEO: S1's EXECUTES edges still carry image_tag=T1 — unchanged

  OR->>R: publish schedules.loaded:v1 (catalog update)

  note over OR,NEO: Next run (S2) triggers SnapshotGraph
  OR->>NEO: MATCH :TopologyRoot → copy topology_generation=G1+1, service_metadata to Run S2
  OR->>NEO: EXECUTES edges for S2 get image_tag=T2 from Table nodes
```

Key invariant: `SnapshotGraph` reads `:TopologyRoot` at call time. Any in-flight run's `Run` node and its `EXECUTES` edges are immutable after creation.

## Why These Diagrams Are Not Enough On Their Own

These diagrams show timing and ordering well, but they do not fully show:

- who owns durable state
- which service is the source of truth for a given field
- which Redis streams are durable integration boundaries vs local loops
- which side effects are retried through outbox tables
- which flows are optional or currently unconsumed

Use these diagrams together with the service dossiers.
