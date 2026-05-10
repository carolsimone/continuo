# Sequence Flows

## 1. Schedule Startup

```mermaid
sequenceDiagram
  participant Cron as state cron
  participant ST as state
  participant R as Redis
  participant OR as orchestrator
  participant EC as executor-controller
  participant KC as k8s-controller

  Cron->>ST: CronScheduler.activateSchedule(name)
  Note over ST: ScheduleActivationService.ActivateSchedule (1 tx)<br/>scheduler_tracker INSERT — status=PENDING, init_status=in_progress, kind='cron'<br/>state_outbox INSERT for scheduler.started v1
  ST->>R: publish scheduler.started v1 (state OutboxProcessor)
  Note right of R: payload — runner_id, schedule_name, service_metadata, kind='cron', source_run_id=''

  R->>OR: consume scheduler.started v1
  Note over OR: HandleSchedulerStartedHandler.Handle (1 tx)<br/>Snapshot(LatestFullDAG) in Neo4j — Run + EXECUTES with pre-assigned task UUIDs<br/>GetScheduleInitNodes — AllNodes, RootNodes, SeedNodes<br/>orchestrator outbox — 1x run.entries.dispatched v1<br/>orchestrator outbox — Nx query.model v1 (seeds, fallback to roots)
  OR->>R: publish run.entries.dispatched v1
  OR->>R: publish query.model v1 (per seed/root)

  par state registers run skeleton
    R->>ST: consume run.entries.dispatched v1
    Note over ST: RunEntriesDispatchedHandler.Handle (1 tx)<br/>row-lock scheduler_tracker (skip if cancelled)<br/>BulkCreate task_tracker rows — status=PENDING<br/>SetTotalTaskCount, init_status=completed, status=RUNNING
  and executor launches seed/root jobs
    R->>EC: consume query.model v1
    Note over EC: write deployment_outbox<br/>OutboxProcessor — CreateQueryJob (idempotent on JobName)
    EC->>R: publish task.status.updated v1 (RUNNING)
    EC->>R: publish node.deployed v1
    R->>ST: consume task.status.updated v1 (RUNNING)
    Note over ST: task_tracker.status = RUNNING
    R->>KC: consume node.deployed v1
    Note over KC: start poll loop — CheckJobStatus + check.k8s v1 backoff
  end
```

> Stages 2A (state) and 2B (executor) race — the seed K8s Job can start before the matching `task_tracker` row exists. `TaskStatusUpdatedHandler` tolerates this by NACKing until `RunEntriesDispatchedHandler` has caught up.

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

> **Permanent dispatch errors fast-path.** Any error wrapping
> `pkg/events.ErrPermanent` (e.g. `image_tag missing` from
> `executor-controller/adapters/k8s/client.go:177`) takes the terminal-failure
> branch on attempt 1 instead of consuming the retry budget. The executor's
> outbox processor calls `MarkTaskTerminallyFailed`, which publishes
> `task.status.updated:v1` (`Status="FAILED"`) and `node.updated:v1`
> (`status="FAILED"`), then marks the outbox entry failed. From the
> schedule's perspective the outcome is identical to retries-exhausted —
> only the latency differs (~1 tick vs. minutes).

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

## 4. Rerun Flow (new run on source's schedule)

```mermaid
sequenceDiagram
  participant U as user/API client
  participant UI as ui-service (BFF)
  participant ST as state (gRPC)
  participant R as Redis
  participant OR as orchestrator
  participant EC as executor-controller

  U->>UI: POST /api/schedulers/{id}/rerun
  UI->>ST: TriggerRerun(source_run_id, schema, table_name, service_name)
  Note over ST: RerunHandler.TriggerRerun (sync, 1 tx)<br/>validations — source run exists, target task FAILED, source terminal<br/>scheduler_tracker INSERT — new row, kind='rerun', source_run_id=src, schedule_name=src.schedule_name, status=PENDING<br/>state_outbox INSERT for trigger.rerun v1<br/>source row left untouched — stays at terminal FAILED forever
  ST-->>UI: TriggerRerunResponse { run_id, schedule_name }
  UI-->>U: 200 OK
  ST->>R: publish trigger.rerun v1 (via state OutboxProcessor)
  Note right of R: payload — schedule_id (NEW run), schedule_name, scope='node', schema_name, table_name, service_name, source_run_id

  R->>OR: consume trigger.rerun v1
  Note over OR: HandleRerunHandler.Handle (1 tx)<br/>Snapshot(SourcePinnedDAG) in Neo4j —<br/>  read source :Run's :EXECUTES set<br/>  MERGE new :Run inheriting topology_generation + service_metadata from source :Run<br/>  project target + non-SUCCEEDED descendants → rebased PENDING (source's pinned image_tag/manifest_version)<br/>  project everything else → SUCCEEDED inherit + inherited_from_task_id (root-resolved)<br/>orchestrator outbox — 1x run.entries.dispatched v1 (full projection, per-task Status + InheritedFromTaskID)<br/>orchestrator outbox — Nx query.model v1 (rebased rows only)
  OR->>R: publish run.entries.dispatched v1
  OR->>R: publish query.model v1 (rebased only)

  par state registers run skeleton
    R->>ST: consume run.entries.dispatched v1
    Note over ST: RunEntriesDispatchedHandler.Handle (1 tx)<br/>BulkCreate task_tracker — honour per-task Status + inherited_from_task_id<br/>SetTotalTaskCount, init_status=completed<br/>auto-rollup if every task already terminal (else status=RUNNING)
  and executor relaunches rebased K8s Jobs
    R->>EC: consume query.model v1
    Note over EC: identical to Flow 1 from here<br/>deployment_outbox to CreateQueryJob to task.status.updated v1 RUNNING + node.deployed v1
  end
```

> Differences vs. Flow 1: synchronous gRPC entry; the new `:Run` inherits `topology_generation` + `service_metadata` from the **source** `:Run` (not `:TopologyRoot`), so the rerun stays bound to the source's snapshot metadata. The source run is never mutated; the schedule's run history grows by one entry per rerun trigger. Rerun shares the same `run.entries.dispatched:v1` pipeline as every other run kind.

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

  note over MC,NEO: Run S1 is already in-flight — Snapshot already committed

  MC->>R: manifest.loaded:v1 (image_tag=T2, manifest_version=V2)
  R->>OR: consume manifest.loaded:v1
  OR->>ORPG: UPDATE topology_state SET topology_generation = G1+1
  OR->>NEO: MERGE :TopologyRoot SET topology_generation=G1+1, service_metadata=...
  OR->>NEO: MERGE Table nodes SET image_tag=T2, topology_generation=G1+1

  note over NEO: Run S1 node still has topology_generation=G1 — unchanged
  note over NEO: S1's EXECUTES edges still carry image_tag=T1 — unchanged

  OR->>R: publish schedules.loaded:v1 (catalog update)

  note over OR,NEO: Next run (S2) triggers Snapshot(LatestFullDAG)
  OR->>NEO: MATCH :TopologyRoot → copy topology_generation=G1+1, service_metadata to Run S2
  OR->>NEO: EXECUTES edges for S2 get image_tag=T2 from Table nodes
```

Key invariant: `Snapshot(LatestFullDAG)` reads `:TopologyRoot` at call time. Derived runs (rerun / rebase / stale-mode single-node) instead inherit `topology_generation` + `service_metadata` from their source `:Run`. Any in-flight run's `Run` node and its `EXECUTES` edges are immutable after creation.

## 8. Dispatch Watchdog Termination

Periodic loop in `orchestrator` terminates schedules that sit in
`is_running=true` but have no task in `RUNNING` and whose most recent task
was created longer than `ORCHESTRATOR_WATCHDOG_NO_PROGRESS_MINUTES` ago.

```mermaid
sequenceDiagram
  participant W as orchestrator watchdog
  participant ST as state (gRPC)
  participant R as Redis
  participant OR as orchestrator
  participant EC as executor-controller
  participant KC as k8s-controller

  loop every ORCHESTRATOR_WATCHDOG_INTERVAL_SECONDS
    W->>ST: ListAllSchedules()
    Note over W,ST: filter is_running=true
    W->>ST: ListTasks(schedule_id=last_run_id) for each
    W->>W: IsScheduleStuck(tasks, now, NO_PROGRESS_MINUTES)
    alt stuck
      W->>ST: CancelSchedule(schedule_name, cancelled_by="watchdog", reason="watchdog: ...")
      ST->>ST: write outbox row (schedule.cancelled:v1)
      ST->>R: publish schedule.cancelled:v1
      Note over R,KC: cancelled_schedules guards (§6) absorb in-flight messages
    end
  end
```

A schedule is "stuck" iff (a) the most recent task's `created_at` is older
than `NO_PROGRESS_MINUTES` and (b) no task is currently in `RUNNING`. The
hasRunning guard distinguishes "stuck dispatching" from "legitimately
running a long task" — a 4-hour dbt model with zero transitions during
execution is not stuck.

The watchdog reuses `state.CancelSchedule` (the same path user-driven
cancellations use), so no new publisher or terminal state is introduced.
Defaults: 60s polling, 30 min no-progress threshold. Toggle via
`ORCHESTRATOR_WATCHDOG_ENABLED` (default `true`).

## 9. Single-Node Run

An ad-hoc, one-task run for a single dbt node. No schedule catalog entry is created; the synthesised run is excluded from `ListAllSchedules` by construction.

```mermaid
sequenceDiagram
  participant U as user/API client
  participant UI as ui-service (BFF)
  participant ST as state (gRPC)
  participant R as Redis
  participant OR as orchestrator
  participant EC as executor-controller

  U->>UI: POST /api/nodes/{node}/run (out of scope for this flow)
  UI->>ST: TriggerSingleNodeRun(service_name, schema_name, table_name, metadata_source, source_run_id?)
  Note over ST: SingleNodeRunHandler.Handle (sync, 1 tx)<br/>validate fields (INVALID_ARGUMENT on bad input)<br/>scheduler_tracker INSERT — kind='single_node_run', schedule_name='single-node-run-<8hex>'<br/>state_outbox INSERT for trigger.single_node_run:v1<br/>synthesised schedule_name NOT inserted into schedule_catalog
  ST-->>UI: TriggerSingleNodeRunResponse { run_id, schedule_name }
  UI-->>U: 200 OK

  ST->>R: publish trigger.single_node_run:v1 (via state OutboxProcessor)
  Note right of R: payload — schedule_id, schedule_name, service_name,<br/>schema_name, table_name, metadata_source, source_run_id?

  R->>OR: consume trigger.single_node_run:v1
  Note over OR: HandleSingleNodeRunHandler.Handle (1 tx)<br/>dedup on message_processing<br/>Neo4j: Snapshot(SingleNode{Target, MetadataSource, SourceRunID?})<br/>  latest mode → selector reads :TopologyRoot + :Table for metadata; new :Run inherits from :TopologyRoot<br/>  stale mode  → selector reads source :Run's EXECUTES edge; new :Run inherits topology_generation + service_metadata from source :Run
  alt ErrTargetNotFound (node absent in Neo4j)
    OR->>R: publish run.entries.dispatch_failed:v1 (synthesised run will be marked terminal-failed)
    R->>ST: consume run.entries.dispatch_failed:v1
    Note over ST: RunEntriesDispatchFailedHandler.Handle (1 tx)<br/>row-lock scheduler_tracker, FinalizeRunTx → status='failed'<br/>state_outbox INSERT for run.finalized:v1
  else node found
    OR->>R: publish run.entries.dispatched:v1 (1 task entry, MaxRetries=DefaultTaskMaxRetries)
    OR->>R: publish query.model:v1 (single dispatch)
  end

  par state registers run skeleton
    R->>ST: consume run.entries.dispatched:v1
    Note over ST: RunEntriesDispatchedHandler.Handle (1 tx)<br/>BulkCreate task_tracker row (1 row)<br/>SetTotalTaskCount=1, init_status=completed, status=RUNNING
  and executor launches the job
    R->>EC: consume query.model:v1
    Note over EC: identical to Flow 1 from here<br/>deployment_outbox → CreateQueryJob → task.status.updated:v1 RUNNING + node.deployed:v1
  end
```

> Differences vs. Flow 1 (schedule startup): synchronous gRPC entry; synthesised `schedule_name` is not in `schedule_catalog`; `Snapshot(SingleNode)` creates exactly one `EXECUTES` edge (no full topology snapshot); only a single `query.model:v1` is produced; on `ErrTargetNotFound` the synthesised run is immediately failed.

## 10. Rebase from Failed/Cancelled Run

A new run on the source's schedule that re-executes the failed/cancelled tasks + descendants + new arrivals against the latest topology, while inheriting everything that succeeded. The source run is never mutated; the schedule's run history grows by one row per rebase trigger.

```mermaid
sequenceDiagram
  participant U as user/API client
  participant UI as ui-service (BFF)
  participant ST as state (gRPC)
  participant R as Redis
  participant OR as orchestrator
  participant NEO as Neo4j
  participant EC as executor-controller

  U->>UI: POST /api/schedulers/{src_run_id}/rebase
  UI->>ST: TriggerRebase(source_run_id)
  Note over ST: RebaseHandler.TriggerRebase (sync, 1 tx)<br/>eligibility — source exists, terminal FAILED|CANCELLED, ≥1 non-SUCCEEDED task<br/>scheduler_tracker INSERT — new row, kind='rebase', source_run_id=src, schedule_name=src.schedule_name, status=PENDING<br/>state_outbox INSERT for trigger.rebase v1<br/>source row left untouched
  ST-->>UI: TriggerRebaseResponse { run_id, schedule_name }
  UI-->>U: 200 OK
  ST->>R: publish trigger.rebase v1 (via state OutboxProcessor)
  Note right of R: payload — schedule_id (NEW run), schedule_name, source_run_id

  R->>OR: consume trigger.rebase v1
  Note over OR: HandleRebaseHandler.Handle (1 tx)<br/>dedup on message_processing<br/>Snapshot(RebasePartition)
  OR->>NEO: read source :Run's :EXECUTES set + latest :Tables for schedule
  Note over OR: rebase_set = (source.status ≠ SUCCEEDED) ∪ descendants(latest) ∪ new_arrivals<br/>inherit_set = SUCCEEDED in source ∩ exists_in_latest \ rebase_set<br/>drop_set = exists_in_source \ exists_in_latest (silently dropped)<br/>inherited rows: root-resolve inherited_from_task_id (chain depth ≤ 1 forever)
  OR->>NEO: MERGE new :Run (inherit topology_generation + service_metadata from source :Run)<br/>MERGE :EXECUTES per projection<br/>  rebased = PENDING + latest image_tag/manifest_version<br/>  inherited = SUCCEEDED + source's pinned pair + inherited_from_task_id

  OR->>R: publish run.entries.dispatched v1 (full projection — per-task Status + InheritedFromTaskID)
  OR->>R: publish query.model v1 (rebased rows only)

  par state registers run skeleton
    R->>ST: consume run.entries.dispatched v1
    Note over ST: RunEntriesDispatchedHandler.Handle (1 tx)<br/>BulkCreate task_tracker — inherited rows land at SUCCEEDED with inherited_from_task_id<br/>rebased rows land at PENDING<br/>SetTotalTaskCount, init_status=completed<br/>auto-rollup if every task already terminal (defensive — no-op rebase)<br/>else status=RUNNING
  and executor launches rebased K8s Jobs
    R->>EC: consume query.model v1
    Note over EC: identical to Flow 1 from here<br/>deployment_outbox → CreateQueryJob → task.status.updated v1 RUNNING + node.deployed v1
  end
```

> Differences vs. Flow 4 (rerun): rebase reads against the **latest** topology, so new arrivals and topology drift land in the projection; rerun reads strictly against the source's pinned `:EXECUTES` set. Both share the same materialiser, the same `:Run` MERGE, and the same `run.entries.dispatched:v1` pipeline.

## Why These Diagrams Are Not Enough On Their Own

These diagrams show timing and ordering well, but they do not fully show:

- who owns durable state
- which service is the source of truth for a given field
- which Redis streams are durable integration boundaries vs local loops
- which side effects are retried through outbox tables
- which flows are optional or currently unconsumed

Use these diagrams together with the service dossiers.
