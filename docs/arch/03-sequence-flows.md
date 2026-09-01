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
  ST->>R: publish scheduler.started v1 (via pkg/outbox.Processor)
  Note right of R: payload — runner_id, schedule_name, service_metadata, kind='cron', source_run_id='', initiated_by

  R->>OR: consume scheduler.started v1
  Note over OR: HandleSchedulerStartedHandler.Handle (1 tx)<br/>Snapshot(LatestFullDAG) in Neo4j — Run + EXECUTES with pre-assigned task UUIDs<br/>frontier (ReadyToDispatch: seeds-first-else-roots) computed by the selector — no second Neo4j read
  alt Snapshot returns ErrEmptyProjection (operation=run, zero :Table nodes) or ErrNoTests (operation=test, every node gated)
    OR->>R: publish run.entries.dispatch_failed:v1 (reason=empty_projection or reason=no_tests)
    R->>ST: consume run.entries.dispatch_failed:v1
    Note over ST: RunEntriesDispatchFailedHandler.Handle (1 tx)<br/>row-lock scheduler_tracker, MarkDispatchTerminal → status='failed' (reason=empty_projection) or status='skipped' (reason=no_tests)<br/>state_outbox INSERT for run.finalized:v1
  else snapshot succeeds
    OR->>R: publish run.entries.dispatched v1
    OR->>R: publish query.model v1 (per seed/root)

    par state registers run skeleton
      R->>ST: consume run.entries.dispatched v1
      Note over ST: RunEntriesDispatchedHandler.Handle (1 tx)<br/>row-lock scheduler_tracker (skip if cancelled)<br/>BulkCreate task_tracker rows — status=PENDING<br/>SetTotalTaskCount, init_status=completed, status=RUNNING
    and executor launches seed/root jobs
      R->>EC: consume query.model v1
      Note over EC: write executor_deployments (pending)<br/>deployer.Dispatcher — CreateQueryJob (idempotent on JobName)<br/>write executor_outbox row (node_deployed)
      EC->>R: publish node.deployed v1
      R->>KC: consume node.deployed v1
      Note over KC: start poll loop — CheckJobStatus + check.k8s v1 backoff<br/>first time the Job is observed running: announce RUNNING once per attempt
      KC->>R: publish task.status.updated v1 (RUNNING)
      R->>ST: consume task.status.updated v1 (RUNNING)
      Note over ST: task_tracker.status = RUNNING
    end
  end
```

> On the success path (right side of the alt), stages 2A (state) and 2B (executor) race — the seed K8s Job can start before the matching `task_tracker` row exists. `TaskStatusUpdatedHandler` tolerates this by NACKing until `RunEntriesDispatchedHandler` has caught up. On the failure path (left side), if a schedule's topology has zero active `:Table` nodes — typically a configuration error in the dbt manifest — the cron run fails fast via `run.entries.dispatch_failed:v1` with `reason=empty_projection`; the state consumer is the same `RunEntriesDispatchFailedHandler` used by single-node, rerun, and rebase paths.

> `scheduler.started:v1` also carries `operation` (`""` \| `"run"` \| `"test"` \| `"build"`). A manual `TriggerSchedule` call (not the cron path shown above) can set `operation=test` or `operation=build`; `HandleSchedulerStartedHandler` always runs `Snapshot(LatestFullDAG)`, passing `operation` through to the selector, which branches on it:
>
> - `operation=test`: the selector drops any node whose `test_count` is a known zero or unset and marks every kept node `ReadyToDispatch` — a flat, edgeless fan-out where every `query.model:v1` dispatches `dbt test --select <node>` immediately, with no seeds-first-else-roots ordering and no `NodeUnblocked` follow-up. If every node is gated this way (no schedule node has a known-positive `test_count`), the selector returns `ErrNoTests` instead of an empty projection: the run fails fast via `run.entries.dispatch_failed:v1` (`reason=no_tests`), and state finalizes it `skipped` rather than `failed` — a schedule with no confirmable tests is a benign outcome, distinct from the zero-`:Table` `empty_projection` case above.
> - `operation=build` (and the default `""`/`"run"`): the selector uses the normal dependency-ordered frontier — the same seeds-first-else-roots computation as a plain run, with `NodeUnblocked` dispatching each node's dependents as it completes. No node is dropped for `test_count == 0`: `dbt build --select <node>` both materializes and tests every node in the DAG. A failed node's descendants are cascade-skipped by the same blocked/unblocked bookkeeping a plain run uses, rather than fanned out independently like `test`.
>
> A run's `operation` is stamped onto its `:Run` node and inherited by any derived run (rerun/rebase) and by every `NodeUnblocked`/derived-run dispatch, so re-running or rebasing a build stays a build.

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
  KC->>KC: write k8s_outbox(task_succeeded + node_status_updated)
  KC->>ST: UpdateTask(status=succeeded)
  KC->>ST: CreateTaskExecution(...)
  KC->>R: publish node.updated:v1

  R->>OR: consume node.updated:v1
  Note over OR: HandleNodeCompletedHandler.Handle (1 tx)<br/>dedup on message_processing, cancelled-schedule guard<br/>runs.Rehydrate(runID, ScopeNodeCompletion{Key, Status=SUCCEEDED})<br/>agg.CompleteNode(key, SUCCEEDED) → [NodeUnblocked …]<br/>runs.Save (retry on ErrVersionConflict, on ErrNodeAlreadyTerminal re-derives effects via agg.EffectsForTerminal)
  OR->>OR: write outbox entry per NodeUnblocked
  OR->>R: publish query.model:v1

  R->>EC: consume query.model:v1
  EC->>EC: write executor_outbox (node_deployed)
  EC->>R: publish node.deployed:v1
  Note over KC: node.deployed:v1 → poll loop<br/>k8s announces RUNNING once when the Job is first observed running
```

## 3. Retry and Terminal Failure Path

> **Permanent dispatch errors fast-path.** Any error wrapping
> `pkg/events.ErrPermanent` (e.g. `image_tag missing` from
> `executor-controller/adapters/k8s/client.go`) takes the terminal-failure
> branch on attempt 1 instead of consuming the retry budget. `deployer.Dispatcher`
> calls `writeFailed`, which writes `task.status.updated:v1` (`Status="FAILED"`)
> and `node.updated:v1` (`status="FAILED"`) as ordinary `executor_outbox` rows,
> then marks the `executor_deployments` row `failed`. From the schedule's
> perspective the outcome is identical to retries-exhausted —
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
    KC->>KC: write k8s_outbox(task_retry at retry_count+1)
    KC->>ST: UpdateTask(status=failed, retry_count of the attempt that ran)
    KC->>ST: CreateTaskExecution(...)
    KC->>S3: upload pod logs
    KC->>R: publish retry.task:v1
    R->>EC: consume retry.task:v1
    Note over EC: write executor_deployments (pending)<br/>deployer.Dispatcher — CreateQueryJob<br/>write executor_outbox row (node_deployed)
    EC->>R: publish node.deployed:v1
    Note over KC: node.deployed:v1 → poll loop<br/>k8s announces RUNNING once at the retry's attempt number
  else retries exhausted
    KC->>KC: write k8s_outbox(task_failed + node_status_updated)
    KC->>ST: UpdateTask(status=failed)
    KC->>ST: CreateTaskExecution(...)
    KC->>S3: upload pod logs
    KC->>R: publish task.failed:v1
    KC->>R: publish node.updated:v1
    R->>OR: consume node.updated:v1
    Note over OR: HandleNodeCompletedHandler.Handle (1 tx)<br/>runs.Rehydrate(runID, ScopeNodeCompletion{Key, Status=FAILED})<br/>agg.CompleteNode(key, FAILED) → [NodeCascadeSkipped …, RunFinalized?]<br/>runs.Save writes per-node status, terminal_count, failed_count, version<br/>and on RunFinalized also :Run.terminal_status + completed_at (first-writer-wins)
    OR->>R: publish task.status.updated:v1 (cascade_task_skipped) per skipped node
    Note over ST: TaskStatusUpdatedHandler increments terminal_task_count<br/>when terminal_task_count == total_task_count it finalizes scheduler_tracker<br/>and emits run.finalized:v1 via state_outbox
  end
```

## 4. Rerun Flow (new run on source's schedule)

```mermaid
sequenceDiagram
  participant U as User
  participant UI as ui
  participant ST as state
  participant R as Redis
  participant OR as orchestrator
  participant N4 as Neo4j

  U->>UI: POST /api/schedulers/{id}/rerun (empty body)
  UI->>ST: TriggerRerun(source_run_id)
  Note over ST: RerunHandler.TriggerRerun (sync, 1 tx) — delegates to<br/>synthesiseDerivedRun(kind='rerun', stream='trigger.rerun:v1'):<br/>validations — source exists / source FAILED|CANCELLED / source has ≥1 non-SUCCEEDED task / no active run on schedule_name<br/>scheduler_tracker INSERT — new row, kind='rerun', source_run_id=src, schedule_name=src.schedule_name, status=PENDING<br/>state_outbox INSERT for trigger.rerun:v1 with payload {schedule_id, schedule_name, kind, source_run_id, initiated_by}<br/>source row left untouched — stays at terminal status forever
  ST-->>UI: TriggerRerunResponse { run_id, schedule_name }

  ST->>R: publish trigger.rerun:v1 (via pkg/outbox.Processor)

  R->>OR: consume trigger.rerun:v1
  Note over OR: DerivedRunHandler.Handle (rerun config, 1 tx)<br/>Snapshot(SourcePinnedDAG{}) in Neo4j —<br/>  read source :Run's :EXECUTES set<br/>  seed rebase set with non-SUCCEEDED source tasks<br/>  grow rebase set by DescendantsInSourceRun<br/>  MERGE new :Run inheriting topology_generation + service_metadata from source :Run<br/>  project rebased rows → PENDING with source's pinned (image_tag, manifest_version)<br/>  project everything else → InitialStatus = source's stored status, with inherited_from_task_id (root-resolved)<br/>DispatchDerivedRun helper writes 1× run.entries.dispatched:v1 (full projection, per-task Status + InheritedFromTaskID) + N× query.model:v1 (rebased rows only)
```

**Differences vs. Flow 1 (cron/trigger):** the new `:Run` inherits `topology_generation` + `service_metadata` from the **source** `:Run` (not `:TopologyRoot`), so the rerun stays bound to the source's snapshot metadata. The source run is never mutated; the schedule's run history grows by one entry per rerun trigger.

**Differences vs. Flow 5 (rebase):** rerun reads against the source's pinned `:EXECUTES` set (no drift, no new arrivals), whereas rebase reads against latest topology and adds new arrivals. The eligibility checks, payload shape, helper code paths, and downstream pipeline (`run.entries.dispatched:v1` + `query.model:v1`) are otherwise identical.

**Test-run source unsupported, build source inherited:** before calling `Snapshot`, `DerivedRunHandler` resolves the source run's operation via `SnapshotService.SourceOperation` (backed by `TopologyReader.SourceRunOperation`) and threads it into `Params.Operation`. `SourcePinnedDAG.SelectTasks` checks that value; if it is `"test"`, the selector returns `ErrRerunOfTestUnsupported` instead of a projection, and `DerivedRunHandler` emits `run.entries.dispatch_failed:v1` (`reason=rerun_of_test_unsupported`) — a rerun's projection carries no per-task operation, so it cannot safely reissue `dbt test` against the failed nodes; the caller must trigger a fresh `node test` instead. If the source operation is `"build"` (or the default `""`), the selector proceeds normally: `Params.Operation` flows through as the derived run's own operation, carried on the frontier `query.model:v1` dispatch, so a rerun of a build re-runs `dbt build`. `RebasePartition` (Flow 5) applies the identical resolution and the same test-source guard. This does not affect single-node `snapshot_of_run` reruns, which legitimately allow a test-operation source.

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
  participant UI as ui
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

  ST->>R: publish schedule.cancelled:v1 (via pkg/outbox.Processor)

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

Shows what happens when `release.promoted:v1` arrives while a run is in-flight.

```mermaid
sequenceDiagram
  participant RC as release-controller
  participant R as Redis
  participant OR as orchestrator
  participant ORPG as orchestrator Postgres
  participant NEO as Neo4j

  note over RC,NEO: Run S1 is already in-flight — Snapshot already committed

  RC->>R: release.promoted:v1 (image_tag=T2, release_id=V2)
  R->>OR: consume release.promoted:v1
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

Periodic loop in `orchestrator` terminates active runs whose dispatch has
silently stalled: no task in `RUNNING` and the most recent task created longer
than `ORCHESTRATOR_WATCHDOG_NO_PROGRESS_MINUTES` ago. Stuck detection is a
single indexed query inside state — the watchdog issues O(1) RPCs per tick and
considers every task of each run (no 50-task paging blind spot).

```mermaid
sequenceDiagram
  participant W as orchestrator watchdog
  participant ST as state (gRPC)
  participant R as Redis
  participant KC as k8s-controller

  loop every ORCHESTRATOR_WATCHDOG_INTERVAL_SECONDS (under a per-tick deadline)
    W->>W: cutoff = now - NO_PROGRESS_MINUTES
    W->>ST: ListStuckCandidates(cutoff)
    Note over W,ST: server-side: active runs with >=1 task,<br/>no RUNNING task, max(task.created_at) < cutoff
    loop each candidate
      W->>ST: CancelSchedule(schedule_name, cancelled_by="watchdog", reason="watchdog: ...")
      ST->>ST: write outbox row (schedule.cancelled:v1)
      ST->>R: publish schedule.cancelled:v1
      Note over R,KC: cancelled_schedules guards (§6) absorb in-flight messages
    end
  end
```

A run is "stuck" iff it has at least one task, the most recent task's
`created_at` is older than `NO_PROGRESS_MINUTES`, and no task is currently in
`RUNNING`. The RUNNING guard distinguishes "stuck dispatching" from
"legitimately running a long task" — a 4-hour dbt model with zero transitions
during execution is not stuck, and because the check is a server-side
aggregation it sees a RUNNING task even in a run with hundreds of tasks. A run
with zero tasks has not started and is never a candidate.

The watchdog speaks domain-typed ports (`ports.StuckScheduleReader`,
`ports.ScheduleCanceller`) implemented by `adapters/grpc.StuckScheduleAdapter`;
no gRPC/proto wire types reach the application layer. It reuses
`state.CancelSchedule` (the same path user-driven cancellations use), so no new
publisher or terminal state is introduced. Defaults: 60s polling, 30 min
no-progress threshold. Toggle via `ORCHESTRATOR_WATCHDOG_ENABLED` (default
`true`).

## 9. Single-Node Run

An ad-hoc, one-task run for a single dbt node. No schedule catalog entry is created; the synthesised run is excluded from `ListAllSchedules` by construction.

```mermaid
sequenceDiagram
  participant U as user/API client
  participant UI as ui (BFF)
  participant ST as state (gRPC)
  participant R as Redis
  participant OR as orchestrator
  participant EC as executor-controller

  U->>UI: POST /api/nodes/{node}/run (out of scope for this flow)
  UI->>ST: TriggerSingleNodeRun(service_name, schema_name, table_name, metadata_source, source_run_id?, operation?)
  Note over ST: SingleNodeRunHandler.Handle (sync, 1 tx)<br/>validate fields (INVALID_ARGUMENT on bad input)<br/>scheduler_tracker INSERT — kind='single_node_run', schedule_name='single-node-run-<8hex>'<br/>state_outbox INSERT for trigger.single_node_run:v1<br/>synthesised schedule_name NOT inserted into schedule_catalog
  ST-->>UI: TriggerSingleNodeRunResponse { run_id, schedule_name }
  UI-->>U: 200 OK

  ST->>R: publish trigger.single_node_run:v1 (via pkg/outbox.Processor)
  Note right of R: payload — schedule_id, schedule_name, service_name,<br/>schema_name, table_name, metadata_source, source_run_id?, operation?, initiated_by

  R->>OR: consume trigger.single_node_run:v1
  Note over OR: HandleSingleNodeRunHandler.Handle (1 tx)<br/>dedup on message_processing<br/>Neo4j: Snapshot(SingleNode{Target, MetadataSource, SourceRunID?}, Operation)<br/>  latest mode → selector reads :TopologyRoot + :Table for metadata, new :Run inherits from :TopologyRoot<br/>  stale mode  → selector reads source :Run's EXECUTES edge, new :Run inherits topology_generation + service_metadata from source :Run
  alt ErrTargetNotFound (node absent in Neo4j) or ErrNoTests (operation=test and the node's test_count is a known zero or unset)
    OR->>R: publish run.entries.dispatch_failed:v1 (reason=target_not_found or reason=no_tests — synthesised run will be marked terminal-failed or terminal-skipped respectively)
    R->>ST: consume run.entries.dispatch_failed:v1
    Note over ST: RunEntriesDispatchFailedHandler.Handle (1 tx)<br/>row-lock scheduler_tracker, MarkDispatchTerminal → status='skipped' (reason=no_tests) or status='failed' (reason=target_not_found)<br/>state_outbox INSERT for run.finalized:v1
  else node found (and, for operation=test, test_count > 0)
    OR->>R: publish run.entries.dispatched:v1 (1 task entry, MaxRetries=DefaultTaskMaxRetries)
    OR->>R: publish query.model:v1 (single dispatch, carrying operation)
  end

  par state registers run skeleton
    R->>ST: consume run.entries.dispatched:v1
    Note over ST: RunEntriesDispatchedHandler.Handle (1 tx)<br/>BulkCreate task_tracker row (1 row)<br/>SetTotalTaskCount=1, init_status=completed, status=RUNNING
  and executor launches the job
    R->>EC: consume query.model:v1
    Note over EC: identical to Flow 1 from here<br/>executor_deployments (pending) → deployer.Dispatcher → CreateQueryJob → executor_outbox row → node.deployed:v1 (k8s announces RUNNING on first observed run)
  end
```

> Differences vs. Flow 1 (schedule startup): synchronous gRPC entry; synthesised `schedule_name` is not in `schedule_catalog`; `Snapshot(SingleNode)` creates exactly one `EXECUTES` edge (no full topology snapshot); only a single `query.model:v1` is produced; on `ErrTargetNotFound` the synthesised run is immediately failed; on `ErrNoTests` it is immediately skipped.
>
> `operation=test` runs `dbt test --select <node>` for the target instead of its default verb. A single-node test against a target with no known-positive `test_count` (a known zero or unset count) is its own dispatch-failed reason, `no_tests` — the same reason a whole-DAG test run (Flow 1) surfaces when every node is gated — and both finalize the run `skipped` rather than `failed`, distinct from the `empty_projection` reason (zero active `:Table` nodes on an `operation=run` schedule).

### ListNodeRuns read path

Before triggering a single-node run the UI fetches the node's run history: `GET /api/nodes/:service/:schema/:table/runs` → `state.ListNodeRuns(service_name, schema_name, table_name, limit=50)`. This is a pure read with no side effects. `state` executes one SQL query (`NodeRunRepository.List`) that joins `task_tracker × scheduler_tracker × task_execution` via a `target_tasks → latest_exec` CTE chain and returns the most recent rows ordered by `scheduler_tracker.created_at DESC`. The result drives the node detail page and populates the source-run picker used by the `metadata_source=snapshot_of_run` branch of Flow 9.

### GetNode read path

The node detail page also fetches per-node topology metadata: `GET /api/nodes/:service/:schema/:table/meta` → `orchestrator.GetNode(service_name, schema_name, table_name)`, an uncached single-row Neo4j `:Table` lookup returning `{node_type, test_count, test_count_known}`. The UI uses `test_count_known && test_count == 0` to disable the page's `test` operation option before it ever reaches `TriggerSingleNodeRun`, so an untested node's `no_tests` dispatch-failed path (Flow 9) is a defensive backstop rather than the primary UX signal.

## 10. Rebase from Failed/Cancelled Run

A new run on the source's schedule that re-executes the failed/cancelled tasks + descendants + new arrivals against the latest topology, while inheriting everything that succeeded. The source run is never mutated; the schedule's run history grows by one row per rebase trigger.

```mermaid
sequenceDiagram
  participant U as user/API client
  participant UI as ui (BFF)
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
  ST->>R: publish trigger.rebase v1 (via pkg/outbox.Processor)
  Note right of R: payload — schedule_id (NEW run), schedule_name, source_run_id, initiated_by

  R->>OR: consume trigger.rebase v1
  Note over OR: DerivedRunHandler.Handle (rebase config, 1 tx)<br/>dedup on message_processing<br/>Snapshot(RebasePartition)
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
    Note over EC: identical to Flow 1 from here<br/>executor_deployments (pending) → deployer.Dispatcher → CreateQueryJob → executor_outbox row → node.deployed v1 (k8s announces RUNNING on first observed run)
  end
```

> Differences vs. Flow 4 (rerun): rebase reads against the **latest** topology, so new arrivals and topology drift land in the projection; rerun reads strictly against the source's pinned `:EXECUTES` set. Both share the same materialiser, the same `:Run` MERGE, and the same `run.entries.dispatched:v1` pipeline.

## 11. dbt Blue/Green Release (Validate-Then-Promote)

A candidate dbt release targets a single dbt service: that team's CI posts a one-service candidate. `release-controller` runs the release through three legs in order — **compile** (dbt compile to produce the changed service's manifest), **seed_build** (build any new/changed dbt seeds into the candidate schema), and **validation** (self-contained empty-build of every changed model and its full transitive closure) — and swaps production only if all three pass. A failure on any leg emits a uniform `release.rejected:v1` event with a `stage` field and a `per_node` array; `remediation` consumes every rejection and builds a `FailureEvidence` appropriate to the stage. `release-controller` owns the lifecycle, holds `current_prod` (the live topology snapshot) and the `service_prod` pointer table (one row per service: its live manifest key + kind + image tag + release id). Releases run a FIFO queue: one release is active at a time, each terminal outcome advances the next, and each promotion refreshes the changed service's `service_prod` pointer.

A candidate for a **python** service follows the same queue and Phases 2–6 below, but skips Phase 1b entirely: its CI compiles and uploads `contract.yaml` (not `manifest.json`) to the canonical key before `POST /releases`, so on activation `release-controller` transitions the release straight from Received to Parsing and emits `release.requested:v1` directly — there is no `compile.requested:v1`/`compile.completed:v1` round trip and no `compile` leg or `compile`-stage rejection for it. Phase 4 also differs per node kind: a changed-closure python node validates via `build_from_columns` (a JSON spec of declared reads + output columns) rather than `build_from_sql`. See `docs/arch/services/release-controller.md` (Processing Logic → `POST /releases`) and `docs/arch/services/executor-controller.md` (`CreateValidationJob`) for the full per-kind behavior.

```mermaid
sequenceDiagram
  participant CI as CI deploy
  participant S3 as S3
  participant RC as release-controller
  participant R as Redis
  participant MC as topology-controller
  participant EC as executor-controller
  participant KC as k8s-controller
  participant OR as orchestrator
  participant ST as state
  participant RM as remediation

  Note over CI,RC: Phase 1 — submit (one service per release)
  CI->>RC: POST /releases {service, release_id, image_tag, bootstrap?, repo, commit_sha}
  Note over RC: create Received release for this service (idempotent on release_id)<br/>FIFO queue advance → Compiling<br/>record changed_service, image_tag, repo, commit_sha<br/>release_controller_outbox INSERT for compile.requested:v1
  RC->>R: publish compile.requested:v1 {release_id, service, image_tag, bucket, candidate_schema}

  Note over EC,RC: Phase 1b — compile (changed service's manifest is compiled first)
  R->>EC: consume compile.requested:v1
  Note over EC: CreateCompileJob: initContainer "compile" runs the resolved compile command,<br/>then two more team-image initContainers "parse-prod"/"parse-candidate" export + rehearse<br/>the service's partial-parse cache (the rehearsal gate — fails parse_rehearsal_failed<br/>if partial parsing is disabled or the project re-parses under run-pod conditions),<br/>then main container "upload" (s3-sidecar) publishes manifest.json + both parse-cache artifacts to S3<br/>emits compile.node.completed:v1 via k8s-controller → aggregate compile.completed:v1
  EC->>R: publish compile.completed:v1 {release_id, status, per_node[{node_id, status, dbt_log_uri, failed_container?}]}
  R->>RC: consume compile.completed:v1
  alt compile failed
    Note over RC: RecordStageResults("compile") + Reject(reason) where reason is compile_failed,<br/>parse_rehearsal_failed (failed_container=parse-prod/parse-candidate), or<br/>artifact_upload_failed (failed_container=upload), derived from failed_container<br/>emit release.rejected:v1 {release_id, stage="compile", reason, failing_nodes,<br/>per_node[{node_id,status,dbt_log_uri,run_results_uri}], repo, commit_sha,<br/>code_bundle_uri=""} (empty: no parse has happened yet)
    RC->>R: publish release.rejected:v1
    R->>RM: consume release.rejected:v1
    alt reason is parse_rehearsal_failed or artifact_upload_failed
      Note over RM: continuo-internal failure, not a model defect — no FailureEvidence built,<br/>no classification_decision row, no remediation.requested:v2 (excluded before classification)
    else reason is compile_failed
      Note over RM: stage="compile" → SourceCompile — ExtractDbtFilePath(log) → file_path<br/>classify + emit ONE remediation.requested:v2 whose nodes[] carries file_path (no candidate SQL)
    end
  else compile ok
    Note over RC: TransitionFromCompiling → Parsing, re-assemble manifest set
    RC->>R: publish release.requested:v1 {release_id, manifest_keys}
  end

  Note over MC,RC: Phase 2 — candidate parse
  R->>MC: consume release.requested:v1
  MC->>S3: download each manifest named in manifest_keys (explicit key list)
  Note over MC: each manifest_keys entry's kind (dbt manifest / python contract) selects the parser<br/>both produce the same ManifestNode shape, so every later step is kind-blind<br/>parse dbt model/seed/snapshot nodes, or a python contract's declared nodes<br/>content_hash = sha256(source_hash|shared_code_hash|config_hash) fold<br/>(source_hash: dbt checksum w/ sha256 fallback, or the python script's own hash<br/>shared_code_hash: transitive macro checksums for dbt, "" for python — config_hash: resolved config minus meta/docs/description/grants/tags for dbt, the canonical contract entry for python)<br/>resolve upstreams via sqlglot (qualified refs only — dbt compiled SQL or python declared reads)<br/>dbt: rewrite compiled SQL to _candidate_{release} schema refs via sqlglot, upload → S3 candidate-sql/{release_id}/candidate_{unique_id}.sql<br/>python: rewrite each declared read to _candidate_{release} schema refs via sqlglot, upload {reads, output_columns, config} → S3 candidate-sql/{release_id}/candidate_{unique_id}.json<br/>(upload failure is fatal for either kind, error_class=CandidateArtifactUploadFailed — dbt seeds → empty candidate_artifact_uri)<br/>build one code-bundle document (contract_version 1) → S3 code-bundles/{release_id}/bundle.json<br/>(upload failure is fatal, error_class=CodeBundleUploadFailed — runs only after every node's candidate artifact has uploaded)
  alt manifest malformed / unqualified table ref / candidate-SQL or code-bundle upload failure
    MC->>R: publish manifest.loaded.candidate:v1 {status=failed, error_class}
    R->>RC: consume manifest.loaded.candidate:v1 (failed)
    Note over RC: Reject(parse_failed) → release.rejected:v1 {stage not present for parse failures}, advance queue
  else parsed and uploaded ok
    MC->>R: publish manifest.loaded.candidate:v1 {status=ok, topology[] (per node: candidate_artifact_uri), code_bundle_uri}
    R->>RC: consume manifest.loaded.candidate:v1 (ok)
    Note over RC: join per-service image_tags into candidate topology<br/>record code_bundle_uri on the release row
  end

  Note over RC: Phase 3 — change detection + gate
  alt DuplicateClaims(topology) non-empty
    Note over RC: two independent checks run unconditionally and merge into one result —<br/>a RELATION collision: two or more nodes write the same physical relation, grouped by<br/>resolved_relation_id (a dbt node's alias, when it has one, else its declared name — falls<br/>back to unique_id when a node carries no resolved_relation_id), so two differently-named<br/>nodes that alias to the same table still collide — the second write would silently overwrite<br/>the first the moment either lands in current_prod<br/>an IDENTITY collision: two or more nodes share a unique_id without all resolving to the<br/>same relation — every downstream lookup keyed on unique_id (the code bundle, the candidate<br/>artifact store, NodeRegistry, the orchestrator's :Table MERGE) sees only one of them,<br/>silently erasing the other from the graph, the code bundle, and the artifact store, whether<br/>or not their relations also collide<br/>checked above every path that can promote — bootstrap below, nothing-to-validate below,<br/>the seed-build leg's own promotion, and the post-validation promotion<br/>Reject(reason=duplicate_table, stage-less, error_class=DuplicatedTable)<br/>per_node carries a rename target/competitor pair only for a two-claimant RELATION collision<br/>an identity collision, or a three-or-more-way relation collision, is named in<br/>error_detail/failing_nodes but gets no per_node entry<br/>also stamps code_bundle_uri (parse already succeeded by this point)
    RC->>R: publish release.rejected:v1
  else shadow:true AND nothing to validate (in-set empty)
    Note over RC: a shadow release measures a proposed fix by validating it, so validating<br/>nothing measures nothing — it must not reach the terminal "validated" a<br/>reviewer reads as proof<br/>Reject(reason=nothing_to_validate, stage-less, no per_node)<br/>the remediation classifier derives no evidence from it (no per_node), and the<br/>shadow-verify reconciler reads the rejected status as a failed fix attempt
    RC->>R: publish release.rejected:v1 {release_id, reason, error_detail, shadow:true}
  else bootstrap:true OR nothing to validate (in-set empty, non-shadow)
    Note over RC: promote directly (skip validation)<br/>current_prod ← candidate topology<br/>upsert changed service's service_prod pointer, transition Promoted
    RC->>R: publish release.promoted:v1 {..., code_bundle_uri, bootstrap}
  else has changed nodes
    Note over RC: changed = content_hash diff vs current_prod snapshot<br/>(empty snapshot / new node ⇒ treated as changed)<br/>inSet = DescendantsClosure(changed) ∪ FullAncestorsClosure(inSet)<br/>(changed + downstream + full transitive upstream closure, cross-service)
    alt an in-set node has a direct upstream absent from the candidate topology
      Note over RC: Reject(unbuildable_cross_service_upstream)
      RC->>R: publish release.rejected:v1
    else inSet has new/changed seeds
      Note over RC: transition SeedBuilding
      RC->>R: publish seed.build.requested:v1 {release_id, per changed-seed node}
      R->>EC: consume seed.build.requested:v1
      Note over EC: CreateSeedBuildJob per changed seed (resolved seed_build command, DBT_TARGET_SCHEMA=candidate)<br/>aggregate → seed.build.completed:v1
      EC->>R: publish seed.build.completed:v1 {release_id, status, per_node[{node_id,status,dbt_log_uri}]}
      R->>RC: consume seed.build.completed:v1
      alt seed_build failed
        Note over RC: RecordStageResults("seed_build") + Reject(seed_build_failed)<br/>emit release.rejected:v1 {release_id, stage="seed_build", reason, failing_nodes,<br/>per_node[{node_id,status,dbt_log_uri,run_results_uri}], repo, commit_sha,<br/>code_bundle_uri, candidate_schema}
        RC->>R: publish release.rejected:v1
        R->>RM: consume release.rejected:v1
        Note over RM: stage="seed_build" → SourceSeed — ExtractDbtFilePath(log) → file_path<br/>classify EVERY failed seed + emit ONE remediation.requested:v2 carrying them all (no candidate SQL)
      else seed_build ok
        Note over RC: TransitionFromSeedBuilding → Validating
        RC->>R: publish validation.requested:v1<br/>{candidate_schema=_candidate_{id}, per node:<br/>node_type, image_tag, candidate_artifact_uri, upstream_node_ids}
      end
    else buildable, no changed seeds
      Note over RC: transition Validating
      RC->>R: publish validation.requested:v1<br/>{candidate_schema=_candidate_{id}, per node:<br/>node_type, image_tag, candidate_artifact_uri, upstream_node_ids}
    end
  end

  Note over EC,KC: Phase 4 — self-contained validation (one empty-build Job per node, gated in dependency order)
  R->>EC: consume validation.requested:v1
  Note over EC: create _candidate_{id} schema once (advisory lock, before fan-out)<br/>per node → executor_deployments (mode=validation)<br/>roots → pending, nodes with upstreams → blocked<br/>(inbound dedup is per-release)
  loop dispatch pending rows, unblocking downstream as upstreams settle ok
    Note over EC: build_from_sql (changed dbt node): single validation container fetches CANDIDATE_SQL_URI from S3 itself → CREATE TABLE {candidate}.{table} AS (SQL) WITH NO DATA<br/>build_from_columns (changed python node): fetches CANDIDATE_SPEC_URI (declared reads + output columns) from S3 → creates the empty typed table from the spec<br/>clone_from_prod: single validation container, no S3 → clone prod table shape empty<br/>(seeds and unchanged upstreams of either kind use clone_from_prod)
    EC->>R: publish node.deployed:v1 (synthetic ids — routes by mode=validation label)
    R->>KC: consume node.deployed:v1 / check.k8s:v1
    Note over KC: poll Job, re-arm check.k8s:v1 until terminal
    KC->>S3: upload runner/dbt pod log
    KC->>R: publish validation.node.completed:v1 {release_id, node_id, outcome, dbt_log_uri}
    R->>EC: consume validation.node.completed:v1
    Note over EC: RecordOutcome, then gating — ok unblocks ready downstream,<br/>non-ok skips all reachable downstream
    EC->>R: publish validation.result:v1 kind=node (per node, as it settles)<br/>{kind:"node", release_id, stage="validation", node_id, status, dbt_log_uri?, run_results_uri?}
    R->>RC: consume kind=node → upsert per_node_results (read model)
  end
  Note over EC: per-release advisory lock + emission sentinel (exactly-once):<br/>when no node remains pending/blocked/deployed → build decision
  EC->>R: publish validation.result:v1 kind=complete (emitted last — decision reads aggregate_status, order-independent)<br/>{kind:"complete", release_id, aggregate_status, candidate_schema} (decision only — no per-node array)

  Note over RC: Phase 5 — promote or reject
  R->>RC: consume kind=complete
  Note over RC: load release FOR UPDATE — read stored per_node_results (stage="validation")<br/>in-order delivery ⇒ store already complete — a permanently-dropped node falls back to aggregate_status
  alt every stored node ok and aggregate_status=ok
    Note over RC: current_prod ← candidate topology<br/>upsert changed service's service_prod pointer, transition Promoted
    RC->>R: publish release.promoted:v1 {..., code_bundle_uri, bootstrap}
  else any stored node failed / aggregate_status not ok
    Note over RC: Reject(validation_failed) using stored per-node results<br/>emit release.rejected:v1 {release_id, stage="validation", reason, failing_nodes,<br/>per_node[{node_id,status,dbt_log_uri,run_results_uri,candidate_artifact_uri,<br/>node_type,file_path,service}], repo, commit_sha,<br/>code_bundle_uri}
    RC->>R: publish release.rejected:v1
    R->>RM: consume release.rejected:v1
    Note over RM: stage="validation" → SourceValidation — node_type/file_path/service/changed_ancestors carried directly from the payload<br/>classify EVERY failed node + emit ONE remediation.requested:v2 carrying them all (forwarded verbatim — the agent falls back to orchestrator GetNodeLocation only when file_path is absent)
  end
  Note over RC: advance FIFO queue

  Note over OR,ST: Phase 6 — production swap
  R->>OR: consume release.promoted:v1
  Note over OR: PromoteRelease (Neo4j, 1 tx) — retire-then-orphan-cleanup:<br/>idempotent if :Meta current_release already = release_id<br/>retire :Table not in release (keep :Run-[:EXECUTES] history)<br/>MERGE release nodes (node_type, image_tag, schedule_name), active=true<br/>rebuild :DEPENDS_ON, DETACH DELETE orphaned retired nodes<br/>MERGE :Meta current_release = release_id
  OR->>OR: IncrementGeneration (topology_state, separate tx)<br/>SetServiceMetadata on :TopologyRoot
  R->>OR: consume release.promoted:v1 (orchestrator-release-promoted-versions group)
  Note over OR: GET code_bundle_uri → code-bundles/{release_id}/bundle.json<br/>(absent → retry via PEL — undecodable → ErrPermanent, ACK-drop)<br/>per bundle node, compare content_hash vs the node's :CURRENT :NodeVersion<br/>(graph-authoritative — the event's changed flags only set healed provenance)<br/>differs → MERGE :NodeVersion (+ :USES_CODE to the exact :CodeUnitVersions),<br/>move :CURRENT under a promoted_at guard<br/>same statement: forward-link [:RESOLVED_BY] every still-open :Rejection of the<br/>node to the version that just became :CURRENT<br/>node with no :Table yet → retry until the topology swap above lands
  OR->>R: publish schedules.loaded:v1 {schedule_names, service_metadata, topology_generation}
  R->>ST: consume schedules.loaded:v1
  Note over ST: ScheduleCatalogHandler — Reconcile schedule_catalog (empty-list guard)
  R->>EC: consume validation.result:v1 kind=complete (executor-validation-result-teardown group)
  Note over EC: teardown — schedule a one-shot engine-image drop_schema Job<br/>(executor holds no warehouse connection) to drop _candidate_{release}

  Note over ST,EC: Phase 7 — build the release's changed seeds into prod
  Note over OR: in the SAME tx as the swap above: filter topology to<br/>Changed && node_type=dbt-seed — emit nothing when none changed
  OR->>R: publish release.seeds.pending:v1 {release_id, nodes[] with pinned node_type + image_tag}
  R->>ST: consume release.seeds.pending:v1
  Note over ST: PromotedSeedsHandler — create a RUNNING run, kind=promote_seed,<br/>id derived from release_id (deterministic, so a redelivered<br/>event maps to the same run rather than rebuilding seeds)
  ST->>R: publish trigger.promoted_seeds:v1 {run_id, schedule_name, release_id, nodes[]}
  R->>OR: consume trigger.promoted_seeds:v1
  Note over OR: Snapshot(NodeSet) over the named seeds
  OR->>R: publish run.entries.dispatched:v1 {all_tasks, total_task_count}
  OR->>R: publish query.model:v1 (one per seed)
  R->>ST: consume run.entries.dispatched:v1 — task rows, init_status=completed
  Note over EC: dispatches each seed Job as an ordinary production task,<br/>so a failure retries on the task's budget, records an execution,<br/>and finalises the run
```

**Self-contained validation (zero model edits).** The validation build set is the changed nodes, their downstream descendants, and their *full transitive upstream closure across service boundaries*. `topology-controller` builds each node's candidate artifact and uploads it to S3 at `candidate-sql/<release_id>/candidate_<unique_id>.<sql|json>` — a dbt node's compiled SQL rewritten to the candidate schema (via sqlglot) as `.sql`, or a python node's declared reads rewritten the same way plus its output columns and physical-layout config as `.json` — and emits a per-node `candidate_artifact_uri`, an `s3://` reference to that object. `executor-controller` builds every upstream as an empty table in `_candidate_<release>` in dependency order, then the changed node against them. Every validation Job is a single-container pod running the external `continuo-python-runtime-<engine>` image (PostgreSQL and Trino today), released from the `continuo-python-runtime` repository and pinned by the chart's `validation.imageTag` independently of `global.imageTag`/`appVersion`; the engine adapter, the runtime harness, and the validation-result contract are installed into the image from PyPI at pinned versions — no dbt, no sidecar. The image is dual-role — its default command runs the python-node harness — so the executor sets the command explicitly to `["continuo-runtime","validation-op"]` on every validation and schema-op pod. (The runtime, contract, and both engine adapters publish to PyPI from that same repository on the same `v*` tag that publishes the images; the image installs those published, versioned packages, and an outside adapter author consumes the same published `continuo-engine-contract`.) For `build_from_sql` nodes (models and snapshots that carry candidate SQL), the container fetches its compiled SQL directly from S3 at `CANDIDATE_SQL_URI` and runs `CREATE TABLE <candidate>.<table> AS (<sql>) WITH NO DATA`; its warehouse connection comes from the Secret attached via `envFrom` (named by `VALIDATION_WAREHOUSE_SECRET`), plus S3 credentials. For `build_from_columns` nodes (changed python models), the container fetches the JSON validation spec at `CANDIDATE_SPEC_URI` instead and creates the empty table directly from its declared output columns — same warehouse connection and S3 credentials, no compiled SQL involved. `clone_from_prod` nodes — including unchanged upstreams and seeds, of either kind — run as a single container with no S3 credentials; the runner clones the prod table's shape empty. The `s3-sidecar` is used only by the compile leg (manifest upload); `dbt-base` and the team image are run only for compile, seed-build, and scheduled runs. Because the SQL's refs already point at the candidate schema, a model whose source still reads `FROM analytics.table_a` validates against the candidate copy — teams never template their schema names. The candidate schema itself is created and dropped by the engine adapter, not the executor: the executor schedules a one-shot engine-image Job (harness `ensure_schema` before the fan-out, `drop_schema` after the aggregate result is consumed) and never connects to the warehouse. Because node validation is materialized by the engine directly and dbt runs only for seeds, a nodes-only release still gets its schema created by the explicit ensure Job. Nothing in production is touched during validation.

**Gating and exactly-once aggregation.** Each node's `executor_deployments` row starts `blocked` if it has in-set upstreams; a node is dispatched only once all its upstreams have settled `ok` (their empty tables now exist). A non-`ok` terminal skips all reachable downstream nodes. When no node remains non-terminal, a per-release advisory lock plus an insert-once emission sentinel guarantee a single `validation.result:v1` `kind=complete` is produced even under redelivery or crash-retry. `aggregate_status` is `ok` iff every per-node status is `ok`.

**Reject reasons.** A release ends in `Rejected` for one of eight reasons, each emitted as `release.rejected:v1`. The event is uniform across all legs: it always carries `release_id`, `stage`, `reason`, `repo`, `commit_sha`, `failing_nodes`, and `per_node[]` (each entry: `node_id`, `status`, `dbt_log_uri`, optional `run_results_uri`). A top-level `code_bundle_uri` (the release's code-bundle S3 URI) is threaded onto six of the eight: set (non-empty) for `duplicate_table`, `seed_build_failed`, and `validation_failed`, all of which follow a completed parse; present but empty for `compile_failed`, `parse_rehearsal_failed`, and `artifact_upload_failed`, which precede the parse that produces the bundle. The remaining two reasons, `parse_failed` and `unbuildable_cross_service_upstream`, carry no `code_bundle_uri` key at all — their payload is the narrower `{release_id, reason, error_class, error_detail}`, without `stage`, `repo`, `commit_sha`, `failing_nodes`, or `per_node[]` either. The eight reasons are: `compile_failed` (dbt compile job failed for a reason other than the two below; `stage="compile"`), `parse_rehearsal_failed` (the compile Job's `parse-prod` or `parse-candidate` initContainer failed — partial parsing is disabled for the project, or the project re-parses under run-pod conditions; `stage="compile"`), `artifact_upload_failed` (the compile Job's `upload` main container failed to publish an artifact to S3; `stage="compile"`), `parse_failed` (a malformed dbt manifest or python contract, an unrecognized manifest kind, an unqualified table reference, a node's runtime having no candidate-artifact builder, a candidate-artifact S3 upload failure, or a code-bundle S3 upload failure; no explicit stage), `duplicate_table` (the assembled candidate topology has a relation collision — two or more nodes write the same `<schema>.<table>` — or an identity collision — two or more nodes share a `unique_id` without all resolving to the same relation; no explicit stage; `error_class: DuplicatedTable`; `per_node[]` carries the rename target's `service`/`file_path`/`node_type`, the contested `relation_id`, and the competing `other_service`/`other_file_path` — but only for a relation collision with exactly two claimants; an identity collision, or a relation collision with three or more claimants, produces no `per_node` entry and therefore no heal trigger, though it still names every claimant in `failing_nodes`/`error_detail`), `unbuildable_cross_service_upstream` (an in-set node depends on an upstream absent from the candidate topology; no explicit stage), `seed_build_failed` (a candidate seed-build job failed; `stage="seed_build"`), and `validation_failed` (one or more validation jobs failed; `stage="validation"`). For `validation_failed`, per-node entries additionally carry `candidate_artifact_uri` plus the candidate topology's `node_type`, `file_path`, and `service` for that node — the kind is what routes the failure to the right fixer in agent-remediation (a python node's contract fix, described in Flow 11a, rather than a dbt source fix), and the location is the path *this* candidate declares, which no promoted-topology lookup can supply for a release that was rejected. Every rejection payload also carries a top-level `shadow` boolean, true only for a fix-verification release (Flow 11a); the remediation classifier records such a rejection and emits nothing for it. `parse_rehearsal_failed` and `artifact_upload_failed` are release-controller's `compileRejection` mapping of the compile Job's `failed_container` attribution (see Flow 11 above) onto a reject reason distinct from `compile_failed`, so a rehearsal-gate miss or an internal upload failure is never presented as a dbt SQL error. Remediation consumes every leg's rejection and discriminates by `stage` (falling back to `reason` when `stage` is absent) to build `FailureEvidence` with the appropriate source (`SourceCompile`, `SourceSeed`, `SourceValidation`, or `SourceDuplicateTable`) — except `parse_rehearsal_failed` and `artifact_upload_failed`, which it excludes entirely (no evidence built, no classification, no trigger) before source resolution, since neither is a model defect a heal proposal could fix. For compile, the evidence-producing reasons extract `file_path` from the dbt log so the agent can read the real source file directly; `seed_build`, `validation`, and `duplicate_table` need no extraction — release-controller already threads `file_path`/`service` from the candidate topology onto each `per_node[]` entry, plus `node_type` for validation and duplicate_table, and `other_service`/`other_file_path` for duplicate_table's competing claimant.

**Failure-precedent case base.** `remediation.requested:v2` (emitted by `remediation` once per rejected release, carrying every healable classified failure), `remediation.pr_opened:v1` (emitted by `agent-remediation` when an operator records a fix PR), and `remediation.pr_closed:v1` (emitted by `agent-remediation`'s PR-outcome reconciler on a terminal GitHub PR state) each carry a consumer group on `orchestrator`, alongside their other consumers — `agent-remediation`'s trigger consumer and `orchestrator`'s case-base rejections consumer both read `remediation.requested:v2`; `orchestrator`'s case-base proposals consumer reads `remediation.pr_opened:v1`; `orchestrator`'s case-base provenance consumer (group `orchestrator-remediation-pr-closed-provenance`) reads `remediation.pr_closed:v1`. The rejections consumer loops the message's `nodes[]` and records each classified failure as its own `:Rejection`, linked to a global `:ErrorSignature` hub keyed on the classifier's `error_signature` and anchored `[:FAILED {release_id}]` from the node's `:Table` when one exists; the proposals consumer records each opened PR as a `:Proposal`, `[:PROPOSED]` from its rejection (MERGing a stub rejection first if the PR arrives before it), with the PR's own facts (`pr_url`, `pr_number`, `pr_state`, `opened_by`, `opened_at`) recorded once on a `:PullRequest` node keyed by (`proposal_id`, `service`) and linked `[:HAS_PR]` from the `:Proposal` — a proposal spanning several services gets one `:PullRequest` per service. `[:RESOLVED_BY]` is a shared relationship type with two target-label families, written independently: `(:Rejection)-[:RESOLVED_BY]->(:NodeVersion)` — linking a rejection to the own-timeline code version that fixed it — converges from both directions: the versions consumer (Phase 6 above) forward-links every still-open rejection of a node the moment a new version becomes its `:CURRENT`, stamping the edge with that promotion's own timestamp; the rejections consumer back-links a newly-recorded rejection to an already-promoted version when the fix landed first — ordinarily the oldest version promoted after the rejection, or, for a revert (a promotion that reuses an existing, older version node), the node's `:CURRENT` version instead. Both writers guard on "no existing `[:RESOLVED_BY]->(:NodeVersion)` edge", scoped to that one target-label family, so whichever direction arrives first wins; a race between them still leaving two `:NodeVersion` edges is resolved deterministically on read (oldest-by-`promoted_at` wins). Independently, `(:Rejection)-[:RESOLVED_BY {amended, service}]->(:Proposal)` is the merged-PR provenance edge: on a merged outcome, the provenance consumer draws this edge for every node the PR resolved (MERGEing a stub `:Rejection` when needed), stamping `service` so a precedent read of a proposal spanning services surfaces only the resolving service's edits, and, per file the PR edited, `(:Proposal)-[:EDITED {path, amended, diff, service}]->(:Table)` — skipped when the edit's target `:Table` is absent; a rejected outcome updates only the `:PullRequest`'s terminal `pr_state`/`closed_at`, drawing neither edge. Because the guard is scoped per target-label family, a rejection can carry both a `:NodeVersion` link and a `:Proposal` link without either suppressing the other. `GetPrecedents` (gRPC) and `continuo precedents` (CLI) read this case base by signature or `(category, reason)` — both filtered on each `:Rejection`'s own properties — resolved-first then newest, and a rejection resolved only via the `:Proposal` edge surfaces the merged PR's edited-node provenance alongside the classified failure.

**Bootstrap and empty-diff short-circuits.** A `bootstrap:true` release skips validation entirely: it records the candidate topology, seeds `current_prod`, and promotes — the initial cutover (or a trusted re-baseline) against an empty or mismatched snapshot. A non-bootstrap release against an empty snapshot instead treats every candidate node as changed and validates the whole topology. A release whose diff is empty (e.g. an image-tag-only bump) trivially passes the gate and promotes directly. All three promotion paths point `current_prod` at the candidate topology and upsert the changed service's `service_prod` pointer, so the next release's change-detection diff is correct and any other service's next release assembles against the refreshed pointer.

**Why the seeds request rides the swap transaction.** `release.seeds.pending:v1` is written by `ReleasePromotedHandler` inside the transaction that swaps the topology, and it carries each seed's `node_type` and `image_tag` from the promoted release rather than leaving them to be re-read later. Both details are load-bearing. The snapshot writer `MATCH`es `:Table` nodes, so a run projected before the swap committed would attach no `:EXECUTES` edges at all; and a promotion can be overtaken by a later one between trigger and projection, so re-reading metadata would build this release's seeds with a different release's image. Sequencing the request behind the swap and pinning its metadata removes both.

**Promoted seeds run like anything else.** A release's changed seeds are materialised into the production schema by an ordinary run, minted by `state` from `release.promoted:v1` and projected by orchestrator through `Snapshot(NodeSet)`. Going through a real run is what makes a production seed build observable and recoverable: `state` has a run for the terminal events to land against, the executor emits the announcement that starts k8s polling, and the Job is observed. The seeds appear as a run named `promote-seed-<id>`, each seed is a task with the standard retry budget, and a failed seed build is retried, recorded, and visible as a failed run an operator can rerun. A promotion that changed no seeds creates nothing.

**Promotion is a lazy generation switch.** `PromoteRelease` swaps the live `:Table` topology using retire-then-orphan-cleanup — retired nodes still referenced by a `:Run-[:EXECUTES]` edge are kept, preserving run history; only truly orphaned retired nodes are deleted. The handler never reads or writes `:Run` nodes or `EXECUTES` edges, so in-flight runs are unaffected: an existing run keeps its `topology_generation` and its edges keep their pinned `image_tag`. The next scheduled run's `Snapshot(LatestFullDAG)` reads `:TopologyRoot` at call time and picks up the new generation and image tags. See **Flow 7 (Topology Versioning — Lazy Generation Switch)** for the consumption-side detail. `node_type` is threaded through the promotion MERGE so promoted seeds are typed correctly and seed-backed schedules dispatch their `dbt seed` jobs without stalling.

### 11a. Batched Remediation and Shadow Verification

One rejected release is one unit of remediation. However many nodes it failed on, the classifier emits **one** trigger, the agent produces **one** attempt, and a human reviews **one** pull request. And no fix is offered for review until a real release has run it: every attempt ends by submitting a **shadow release** per edited service — a real release that runs parse → candidate-schema → validation and then stops at the terminal `validated` status instead of promoting.

Two facts make the batching more than bookkeeping. `release.rejected:v1` stamps each failing node's `changed_ancestors` — the ancestors this release changed that the failure descends from, each with the file path and service this candidate declares — so the agent can see that several failures share one cause, and can edit that ancestor where THIS release holds it rather than where the promoted graph still places it. And the classifier's `error_signature` ignores the database's echoed `LINE n: <statement>`, so two siblings broken by the same upstream change sign identically instead of being keyed on the relation each of them happened to be building. Together they let one edit, to a node that may never have failed, resolve several failures.

```mermaid
sequenceDiagram
  participant RC as release-controller
  participant RM as remediation (classifier)
  participant OR as orchestrator (case base)
  participant RA as agent-remediation
  participant GH as GitHub (read-only)
  participant LLM as LLM provider
  participant S3 as S3

  Note over RC: a release fails validation on several nodes (Flow 11)
  RC->>RM: release.rejected:v1 { stage=validation, shadow=false,<br/>per_node[node_id, node_type, service, file_path,<br/>changed_ancestors{node_id,file_path,service}] }
  Note over RM: classify EVERY failing node, record one classification_decision each,<br/>then emit ONE trigger for the whole healable set
  RM->>RA: remediation.requested:v2 { release_id, remediation_round, repo,<br/>commit_sha, code_bundle_uri, nodes[...] }
  RM->>OR: (same message, case-base group) one :Rejection per node in the batch

  Note over RA: attempt n — group the failing set (domain/typology)
  Note over RA: same signature + shared changed ancestor → ONE cluster targeting<br/>that ancestor; everything else → one independent cluster each
  RA->>RA: proposal(status=generating) over the whole failing set

  loop one per cluster
    alt shared upstream
      RA->>S3: GetObject code bundle (the changed ancestor's source)
      RA->>LLM: propose_fix (ancestor source, own-change diff,<br/>every member's error excerpt, precedent)
      Note over RA: a declined answer is a SKIP, not a failure —<br/>each member is re-queued as an independent cluster
    else independent
      RA->>GH: read the failing node's source (or the repo tarball, python lane)
      RA->>LLM: propose_fix / propose_python_fix / propose_csv_fix
    end
    Note over RA: the Fixer returns EDITS ONLY, each naming its target_node_id
  end

  Note over RA: bucket the edits by service (longest project-root prefix wins)
  loop one shadow release per edited service
    alt python service
      RA->>S3: PutObject <service>/<shadow_id>/contract.yaml (packaged contract)
    else dbt service
      RA->>S3: PutObject <service>/<shadow_id>/source-overlay.tar.gz<br/>(proposed files, keyed relative to the project root)
    end
    RA->>RC: GET /releases/{original_id} (image tag for the service)
    RA->>RC: POST /releases { shadow:true, kind, release_id=<shadow_id>,<br/>source_overlay_uri (dbt only) }
  end
  Note over RA: ONE proposal(status=verifying) — resolved_node_ids, node_outcomes,<br/>verifications[], file_edits[], and the raw trigger for a possible retry

  Note over RC: each shadow release runs the full pipeline (Flow 11).<br/>A dbt one's compile Job lays the overlay over the team project first.

  loop shadow-verify reconciler, every SHADOW_VERIFY_POLL_INTERVAL
    RA->>RC: GET /releases/{shadow_id} — for EVERY verification on the row
  end

  alt every shadow release validated
    Note over RA: CAS verifying → proposed + enqueue remediation.proposed:v1<br/>in one transaction; each waiting node's outcome becomes proposed
    RA->>RA: remediation.proposed:v1 → the human review surface (Flow 12)
  else any shadow release rejected
    RC->>RM: release.rejected:v1 { shadow=true }
    Note over RM: decision recorded as drop / shadow_verification — NOTHING emitted
    Note over RA: CAS verifying → failed, verify_error = the rejections' per-node errors<br/>commit, THEN start attempt n+1 from the stored trigger,<br/>replayed in full (the failed attempt's edits are all discarded)
    RA->>RA: attempt n+1 (re-grouped over the same failing set)
  else a release has been RUNNING longer than SHADOW_VERIFY_TIMEOUT
    Note over RA: verifying → failed, verify_error = "shadow verification timed out"<br/>(the budget is per release, from its own activation — queueing spends nothing)
  end
```

**How a dbt fix is verified.** A python service's fix packages a contract yaml, which *is* the release artifact, so the shadow release simply reads it. A dbt service has no such artifact: the fix changes files inside a project that lives in a team image the release runs. So the agent packs the proposed files into a deterministic `source-overlay.tar.gz`, keyed by each file's path within that project, and release-controller threads its URI onto `compile.requested:v1`. Executor-controller then adds an `overlay` init container (the s3-sidecar's `overlay_fetcher.py`) ahead of every team-image container of the compile Job and prefixes each of their commands with a staging prologue that copies the team project into a writable `/work` emptyDir, lays the proposed files over the copy, and runs dbt from there — so the manifest, both parse artifacts, and the candidate SQL the release validates all describe the proposed source. The proposed files are never written into the image's own project directory, which means a team image needs no Dockerfile change to be verifiable: root-owned project files (what a plain `COPY` produces) work unchanged.

**One rejection, one pull request per owning service.** The attempt's row carries `resolved_node_ids` (every failing node it addresses), `node_outcomes` (how it ended for each of them — a cluster whose fixer skipped leaves its members skipped while the rest verify), `verifications` (one shadow release per edited service), and `file_edits` with each edit's `target_node_id`. The release page joins each proposal to every node in `resolved_node_ids` and reads that node's own outcome. The attempt's edits are bucketed by owning service (the same bucketing that drives one shadow release per service above); `pr_services` names the sorted service groups, `[""]` for a proposal whose edits all land in one service (or a legacy row from before the split), and the Create PR route (Flow 12) opens one pull request per group — titled `fix N nodes` and suffixed with the service name for a split proposal, on a branch agent-remediation names for the attempt and that service (`remediation/<release_id>/attempt<n>/<service>`, or `remediation/<release_id>/attempt<n>` for the legacy, unsplit group) — rather than one pull request for the whole attempt. Each per-service shadow assembles the OTHER edited services' nodes unchanged (their fixes ship on separate PRs), so release-controller re-validates only the nodes a shadow's fix changed relative to BOTH `current_prod` and the rejected release's candidate topology — the intersection of the two `content_hash` diffs. A sibling's still-unfixed failure, byte-identical to that candidate, differs from `current_prod` but not from the candidate, so it is excluded; a node another release promoted since the rejection matches `current_prod` but differs from the candidate's stale copy, so it is excluded too; only the shadow's own fix delta differs from both and is validated (see `services/release-controller.md`). Without it, each shadow would re-validate the other services' unfixed failures and no multi-service proposal could pass.

**The same flow for a python-csv node.** A python-csv node carries `node_type=python-csv` instead, has no script at all, and rejects for a different reason — its declared `output_columns` name a column the CSV file's own header does not carry. `fixer.For(validation, python-csv)` resolves to its own `csvValidationFixer` rather than the python contract fixer, and the LLM call becomes `propose_csv_fix` with a prompt telling the model the CSV file is the source of truth (fix the contract to match it, not the other way round) and that there is no script whose reads must be preserved. The repository search, apply-then-package ordering, and the reconciler loop below it are not merely the same shape — `csvValidationFixer` and `pythonValidationFixer` call the same two functions, `locateContractForFix` and `proposeContractFixViaShadow`. Only the evidence shown to the model and the post-apply guard differ, and the guard differs only for the failing node's own `csv` read: that key's uri may be corrected without refusing the fix, but every other read on every node the answer touches — including a python-model sibling's, in a directory the fix packages whole — is still held to the same "no read may be dropped" rule the python-model lane applies everywhere. See **CSV validation fixer** in `docs/arch/services/agent-remediation.md` for the full delta.

**Why the loop cannot eat itself.** A rejected shadow release emits `release.rejected:v1` like any other, and that event reaches the same classifier. Without a marker it would be classified healable and turned into a fresh trigger, remediating a failed fix attempt as though it were a new failure someone had shipped. The agent is already handling that verdict through its own reconciler, so the trigger would drive a second, independent retry chain — one keyed on the shadow release's id rather than the original's, which is the id the attempt history, the release page, and the operator's review are all anchored on. The `shadow` flag on the payload is what stops it: the classifier still writes its `classification_decision` row — no drop in that service is invisible — with the category and signature the rules produced, then overrides the routing to `drop` / `shadow_verification` and emits nothing. Polling the releases it submitted is the agent's only path back from a verdict.

**Why nothing shadow reaches production.** The gate is one branch at the top of `promoteToProduction`, the single function every promoting path funnels through: a shadow release transitions to `validated` and saves there, so the `current_prod` read, the `service_prod` pointer upsert, the changed-node derivation, and the `release.promoted:v1` emit are all skipped rather than made conditional further downstream. No `release.promoted:v1` means no Neo4j topology swap and no `:NodeVersion` written for the shadow's id, so an unapproved fix can never enter production history. Shipping the fix stays a separate, human act: the verified proposal goes to the same operator-gated Create PR surface as every other proposal (Flow 12).

**Why attempt n+1 is better informed.** A failed attempt's row keeps both the diffs it applied (as S3 artifacts) and the errors its shadow releases reported. The python lanes' prompts render every earlier attempt with both, so the model is told what has already been tried and rejected rather than re-proposing it. The retry covers the same failing set as the attempt it follows, even when the rejection names only some of its nodes: the attempt is one proposal over one set of edits, so the edits for the nodes that did pass were never offered to anyone and die with the failed attempt. Retrying only the still-failing nodes would end in a pull request carrying their fixes alone, silently dropping ones an earlier attempt had already got right. Which nodes failed is not lost — it is the attempt's recorded `verify_error`, which is exactly what the next attempt is shown. The retry is driven from the trigger stored on that row, given a dedup identity derived from the proposal id — the original message id was already claimed by the first attempt's own transaction, so replaying it verbatim would be reported as "already processed" and quietly do nothing.

**The attempt budget is per release, per round.** `AGENT_REMEDIATION_MAX_ATTEMPTS` (default 3) counts terminal attempts for `(release_id, remediation_round)` — three whole-release tries, not three tries per broken node. Exhausting it records one `escalated` attempt covering the whole failing set and calls no model.

### 11b. Remediation Retry Round (Try Again on a Rejected Release)

A rejected release is a dead end for remediation once every failed node's latest attempt has settled without an open PR — nothing generating, nothing verifying, nothing proposed and still awaiting review — and only a human can move it forward. The release page surfaces a **Try again** button exactly in that state (it stays hidden while any attempt is in flight, since a retry then would only race it). Clicking it does not require a new commit: it asks `release-controller` to run the release's own stored rejection back through the classifier once more, as remediation round N+1.

```mermaid
sequenceDiagram
  participant OP as operator (browser)
  participant UI as ui
  participant RC as release-controller
  participant RA as agent-remediation (gRPC 50054)
  participant RM as remediation (classifier)

  OP->>UI: POST /api/releases/:id/retry-remediation ("Try again")
  UI->>RC: POST /releases/{id}/retry-remediation
  RC->>RC: load release FOR UPDATE

  alt release not found
    RC-->>UI: 404 { error: not_found }
  else status != rejected
    RC-->>UI: 409 { error: not_rejected }
  else reject_reason not healable<br/>(only compile_failed, seed_build_failed, validation_failed, duplicate_table qualify)
    RC-->>UI: 409 { error: not_healable }
  else no stored rejection_payload (release predates the column)
    RC-->>UI: 409 { error: not_retryable }
  else remediation_round >= MaxRemediationRounds (3)
    RC-->>UI: 409 { error: rounds_exhausted }
  else
    RC->>RA: ListProposals({release_id})
    alt gRPC call itself fails
      RA-->>RC: error
      RC-->>UI: 502 { error: proposal_reader_unavailable }
    else any node's latest attempt is generating/verifying/proposed,<br/>or its PR is opening/open/merged
      RA-->>RC: proposals
      RC-->>UI: 409 { error: proposal_open, proposal_id, pr_url }
    else every node's latest attempt is terminal and unclaimed
      RA-->>RC: proposals
      Note over RC: round = remediation_round + 1 (StartRemediationRound records<br/>a "remediation_retry" transition, saves)
      Note over RC: decode stored rejection_payload, set top-level remediation_round=round,<br/>re-encode — the exact bytes release-controller emitted at rejection,<br/>replayed one round later
      RC->>RM: publish remediation.retry_requested:v1 { ...stored release.rejected:v1 payload,<br/>remediation_round=round } (event_id = remediation-retry:<release_id>:<round>)
      RC-->>UI: 202 { release_id, remediation_round=round }
      UI-->>OP: status pill "rejected · round N"; proposal polling restarts

      Note over RM: classifyRejectionMessages — the same handler that reads<br/>release.rejected:v1 (Flow 11) — decodes the identical payload shape<br/>and classifies every per_node entry again
      RM->>RM: classification_decision upsert keyed (source, release_id,<br/>remediation_round, node_id) — a fresh slot, independent of round 1's rows
      RM->>RA: remediation.requested:v2 { ...as Flow 11, remediation_round=round }<br/>(event_id suffixed "|round|N" so it never collides with round 1's trigger)
      Note over RA: attempt cap resets for this round's key (release_id,<br/>remediation_round, source, node_id, error_signature); the attempt NUMBER<br/>itself keeps the release's one running sequence — see Flow 11a and<br/>docs/arch/services/agent-remediation.md
    end
  end
```

**Why the round bump is not a queue transition.** Every other release state change in this doc is driven by an inbound stream message through the FIFO queue advance. A retry is triggered by a human clicking a button, so `RetryRemediation` runs synchronously in the HTTP handler against a `FOR UPDATE`-locked release row and never touches the release's own `status` or the FIFO queue — `rejected` stays `rejected`; only `remediation_round` and the replayed rejection change. Everything after the retry stream — classification, proposal, a python node's shadow verification, human PR review — is Flow 11 / 11a / 12 run again, with round N+1 threaded through the trigger.

**Why the check reads agent-remediation over gRPC, not remediation's own table.** `classification_decision`'s round-scoped natural key only tells release-controller whether a node was *classified* again, not whether a fix from that classification is still being worked or already out for review — that state lives entirely in agent-remediation's `proposal` table. `RetryRemediation` reads it directly via `ListProposals` rather than inferring it from an event it would otherwise have to wait for, so the refusal is synchronous: the operator sees "a fix is already proposed" immediately, not after a round trip through two more services.

**What ends the loop.** A round exhausts remediation the same way round 1 does: every failed node reaches a terminal, unclaimed outcome (`skipped`, `failed`, or `escalated`, or a `proposed` fix nobody opened a PR for). At that point the release is a dead end again and, below the cap, the button reappears for round N+2. At `MaxRemediationRounds = 3` the button is replaced by a fixed message telling the operator to push a new commit instead — the round counter belongs to this one release's rejection lineage and there is no route back to round 1 short of a fresh `POST /releases`.

## 12. Human-Gated Create PR (Remediation Surface)

An operator reviews a fix proposal in the Remediation tab and clicks **Create PR**. ui reads the proposal's `pr_services` — the sorted owning-service groups its pull requests split into (`[""]` for a legacy, unsplit proposal) — and repeats the claim / GitHub-create / record sequence once per group, independently: a group that already has a PR is skipped rather than retried, and a group that fails does not lose a group that already succeeded in the same request. agent-remediation enforces single-winner idempotency per (proposal, service). The PR's eventual close is then mirrored back onto its (proposal, service) row by a background reconciler, independent of ui.

```mermaid
sequenceDiagram
  participant OP as operator (browser)
  participant UI as ui
  participant RA as agent-remediation (gRPC 50054)
  participant S3 as S3
  participant GH as GitHub App API

  OP->>UI: POST /api/remediation/proposals/:id/pull-request (operator role required)
  UI->>RA: GetProposal(id)
  alt not found
    RA-->>UI: NOT_FOUND
    UI-->>OP: 404
  else found
    RA-->>UI: { pr_services: [...] } — [""] for a legacy, unsplit proposal
    Note over UI: services := sorted(pr_services)

    loop each service in services, independently
      UI->>RA: BeginPullRequest(id, service)
      Note over RA: CAS, scoped to this (proposal, service) row:<br/>pr_state '' or 'failed' → 'opening' (guarded on source_resolved=true AND status='proposed')<br/>Stamps pr_claimed_at — RETURNING reads it back into the response<br/>Returns repo, commit_sha, branch (agent-remediation-computed, service-scoped), claimed_at, edits (this service's files only)<br/>Returns FAILED_PRECONDITION + this service's existing pr_url if already opening/open<br/>Returns FAILED_PRECONDITION if source_resolved=false or status is not 'proposed'

      alt already opening or open for this service
        RA-->>UI: FAILED_PRECONDITION { pr_url }
        Note over UI: skip — this service already has a PR; report its URL, do not touch GitHub
      else source_resolved=false, or status is not 'proposed'
        RA-->>UI: FAILED_PRECONDITION
        Note over UI: record a per-service error, continue to the next service
      else claim granted
        RA-->>UI: { repo, commit_sha, branch, claimed_at, edits, resolved_node_ids, ... }

        alt edits list empty and no single-file fallback
          Note over UI: nothing to commit for this service — refuse rather than open an empty PR
          UI->>RA: FailPullRequest(id, claimed_at, service)
          Note over UI: record a per-service error, continue to the next service
        else edits usable (a legacy edits-empty row falls back to its single-file fields)
          alt S3 fetch fails for any edit
            UI->>S3: GetObject(edit.content_uri) (any edit)
            S3-->>UI: error
            UI->>RA: FailPullRequest(id, claimed_at, service)
            Note over UI: record a per-service error, continue to the next service
          else all edits fetched
            loop each edit for this service
              UI->>S3: GetObject(edit.content_uri)
              S3-->>UI: corrected file content
            end

            UI->>GH: mint installation token (App JWT → /installations/:id/access_tokens)
            UI->>GH: POST /repos/{repo}/git/refs (branch off commit_sha; 422 "already exists" treated as idempotent)
            UI->>GH: GET /repos/{repo}/git/commits/{commit_sha} → base tree SHA
            loop each edit
              UI->>GH: POST /repos/{repo}/git/blobs (base64 content)
            end
            UI->>GH: POST /repos/{repo}/git/trees (base_tree=base tree SHA, one entry per edit, mode 100644)
            UI->>GH: POST /repos/{repo}/git/commits (tree=new tree SHA, parents=[commit_sha])
            Note over GH: every edited file for this service lands in this one commit
            UI->>GH: PATCH /repos/{repo}/git/refs/heads/{branch} (sha=new commit SHA, force=true)
            UI->>GH: POST /repos/{repo}/pulls (base=main, head=branch)
            Note over GH: 422 "PR already exists for head" → GET existing PR

            alt GitHub step errors
              UI->>RA: FailPullRequest(id, claimed_at, service)
              Note over RA: CAS: pr_state 'opening' → 'failed' (retryable), scoped to this (proposal, service) row<br/>released=false (not an error) if the claim already moved on —<br/>e.g. the opening sweep released it first on a slow S3/GitHub step
              Note over UI: record a per-service error, continue to the next service
            else GitHub step succeeds
              GH-->>UI: { pr_url, pr_number }
              UI->>RA: RecordPullRequest(id, pr_url, pr_number, opened_by=session.user_id, service)
              Note over RA: CAS: pr_state 'opening' → 'open' for this (proposal, service) row (WHERE pr_state='opening')<br/>pr_opened_at = now(), pr_opened_by = user_id<br/>INSERT remediation_agent_outbox (remediation.pr_opened:v1, pointer-only)<br/>a CAS miss (e.g. the opening sweep already recorded this same claim) is a silent no-op, not an error
              RA-->>UI: ok
              Note over UI: append { service, pr_url, pr_number } to the response's pull_requests
            end
          end
        end
      end
    end

    Note over UI: every service attempted — pull_requests[] holds every success (opened or already-open),<br/>errors[] holds every per-service failure
    alt no service succeeded
      UI-->>OP: 502 { pull_requests: [], errors: [...] }
    else some but not all succeeded
      UI-->>OP: 207 { pull_requests: [...], errors: [...] }
    else every service succeeded
      UI-->>OP: 200 { pull_requests: [...], errors: [] }
    end
  end
```

> ui checks for a configured GitHub App (`GITHUB_APP_ID`/`GITHUB_APP_PRIVATE_KEY`/`GITHUB_APP_INSTALLATION_ID`) before even calling `GetProposal`, returning 503 if it is absent — unchanged from the single-PR route this replaced. The deterministic branch name and the GitHub-level "PR already exists for head" guard together make each service's slice of the flow safe to retry independently: a double-click or browser reload issues a second `BeginPullRequest` for that service, which — if the first already reached `opening`/`open` — short-circuits with the existing `pr_url` before touching GitHub again, without disturbing any other service's claim. `FailPullRequest` resets that one (proposal, service) row's `pr_state` to `failed` so a subsequent click by the same or a different operator can retry just that service cleanly; it is a compare-and-set on the `claimed_at` its own `BeginPullRequest` call returned, not an unconditional write, so a claim this same request already lost — released by the opening sweep after the grace period elapsed while the S3/GitHub round trip was still in flight, then re-claimed by someone else — is never reset out from under its new owner. `RecordPullRequest` carries the same guard on the way into `'open'`: its CAS only fires while that row is still `'opening'`, so if the opening sweep (Flow 12b) recovers and records the same PR first — recomputing the same deterministic branch and finding it on GitHub — this call's own attempt is a harmless no-op rather than a second write or a second outbox entry; ui still reports that service's PR link either way, since the PR itself was already created by the time this call runs. The `remediation.pr_opened:v1` outbox event feeds orchestrator's case-base proposals consumer (see Flow 11's "Failure-precedent case base" above), which records the opened PR as a `:Proposal` linked from its `:Rejection`.

### 12a. PR-Outcome Reconciler (Close-Loop Tail)

Independent of the operator-driven flow above, a background loop in agent-remediation polls GitHub for every proposal whose `pr_state='open'` and mirrors the terminal outcome back onto the row.

```mermaid
sequenceDiagram
  participant RA as agent-remediation (reconciler)
  participant GH as GitHub Pulls API
  participant PG as agent-remediation Postgres

  loop every REMEDIATION_PR_POLL_INTERVAL (default 60s)
    RA->>PG: ListOpenPullRequests(limit=50) -- pr_state='open', oldest-opened first
    loop each open-PR row
      RA->>GH: GET /repos/{repo}/pulls/{number}
      alt PR still open
        GH-->>RA: state=open
        Note over RA: leave row untouched — retried next pass
      else PR closed
        GH-->>RA: state=closed, merged=<bool>, closed_at
        Note over RA: outcome = merged if merged=true else rejected
        RA->>PG: RecordOutcome(id, outcome, closed_at) -- 1 tx:<br/>CAS pr_state 'open' -> outcome<br/>INSERT remediation_agent_outbox (remediation.pr_closed:v1, pointer-only)
        alt CAS hit (row was still 'open')
          Note over PG: pr_state=outcome, pr_closed_at=closed_at — outbox row committed
        else CAS miss (row already left 'open')
          Note over PG: no-op -- nothing written, no event emitted
        end
      end
    end
  end
```

> Per-row errors — a failed GitHub read or a failed `RecordOutcome` — are logged and skipped so one bad row never blocks the rest of the batch; that row is retried on the next pass. A PR reopened on GitHub after reaching `merged`/`rejected` is not tracked: the reconciler only lists `pr_state='open'` rows, so a terminal row is never re-examined. The outbox publisher drains the `remediation.pr_closed:v1` row on its own loop, same as every other outbox entry; orchestrator's case-base provenance consumer (group `orchestrator-remediation-pr-closed-provenance`) reads it to stamp the `:PullRequest`'s terminal state and, on a merged outcome, draw the `[:RESOLVED_BY]`/`[:EDITED]` provenance edges.

### 12b. Opening Sweep (Stranded-Claim Recovery)

`RecordPullRequest` in Flow 12 is explicitly best-effort: by the time it runs, the PR already exists on GitHub, so ui logs loudly and returns the PR link to the operator on failure rather than failing the request. That leaves the claimed (proposal, service) row stuck at `pr_state='opening'` in the `proposal_pull_request` child table, with no `pr_url`, which the UI reads as "no PR yet" — inviting a duplicate. Because the PR lifecycle is per (proposal, service), a proposal split across several owning services can have several independently-stuck claims, one per service, each recovered or released on its own. The same background reconciler that drives Flow 12a also sweeps these stuck claims, on the same tick.

```mermaid
sequenceDiagram
  participant RA as agent-remediation (reconciler)
  participant GH as GitHub Pulls API
  participant PG as agent-remediation Postgres

  loop every REMEDIATION_PR_POLL_INTERVAL (default 60s)
    RA->>PG: ListStuckOpening(limit=50, cursor) -- reads proposal_pull_request child rows with pr_state='opening',<br/>(created_at, id, service) order (service breaks ties between one proposal's several stuck children), resumes after cursor
    PG-->>RA: rows, next_cursor
    Note over RA: cursor := next_cursor (nil wraps to the start) -- advanced before any row below is handled
    loop each stuck-opening (proposal, service) child row
      Note over RA: recompute branch_name = BuildBranch(release_id, attempt, service) --<br/>remediation/<release_id>/attempt<n>/<service>, or remediation/<release_id>/attempt<n> for the legacy ("") group
      RA->>GH: GET /repos/{repo}/pulls?head={owner}:{branch_name}&state=all&per_page=1
      alt PR found
        GH-->>RA: [{ number, html_url, created_at }]
        RA->>PG: Record(id, service, pr_url, pr_number, opened_by="") -- 1 tx:<br/>CAS on the (id, service) child row: pr_state 'opening' -> 'open' (WHERE proposal_id=id AND service=service AND pr_state='opening'), pr_opened_at=created_at, pr_claimed_at=NULL<br/>INSERT remediation_agent_outbox (remediation.pr_opened:v1)
        alt CAS hit (row was still 'opening')
          Note over PG: gap closed -- UI now shows the recovered pr_url for this service
        else CAS miss (ui's own RecordPullRequest recorded this same claim first)
          Note over PG: no-op -- nothing written, no event emitted
        end
      else PR not found
        GH-->>RA: []
        alt pr_claimed_at is NULL
          Note over RA: unmeasurable claim age (the proposal_stamp_pr_claimed_at trigger did not run -- schema corruption or a manual edit) — leave untouched
        else now - pr_claimed_at > REMEDIATION_PR_OPENING_GRACE_PERIOD
          RA->>PG: FailStuckOpeningPR(id, service, observed_pr_claimed_at) -- CAS on the (id, service) child row:<br/>pr_state 'opening' -> 'failed', pr_claimed_at=NULL<br/>WHERE proposal_id=id AND service=service AND pr_state='opening' AND pr_claimed_at=observed_pr_claimed_at
          alt CAS hit
            PG-->>RA: true
            Note over PG: retryable: BeginPR's claim guard accepts '' or 'failed'
          else CAS miss (released and re-claimed since this row was listed)
            PG-->>RA: false
            Note over RA: leave the fresh claim untouched -- never overwrite a claim this pass did not observe
          end
        else within the grace period
          Note over RA: leave row untouched — retried next pass
        end
      end
    end
  end
```

> `BeginPullRequest` stamps `pr_claimed_at` with the wall-clock moment it claims the row (via the service's `Clock` port, not SQL `now()`) and reads the persisted value back into its response so the caller can present it to a later `FailPullRequest` call; a `BEFORE UPDATE` trigger (`proposal_stamp_pr_claimed_at`) stamps it with `clock_timestamp()` instead whenever a row's `pr_state` is becoming `'opening'` and the column is still `NULL` — closing the gap for a claim taken by a binary that predates the column and cannot be taught to set it itself, so every claim carries a value regardless of writer version. The same trigger clears `pr_claimed_at` back to `NULL` on **every** transition out of `'opening'`, unconditionally, regardless of what value (if any) the transitioning statement itself wrote to the column — this is the database-boundary guarantee, not an application-layer convention each writer must remember: a binary that predates the column issues its `RecordPullRequest`/`FailPullRequest`-equivalent write without mentioning `pr_claimed_at` at all, and without this trigger clause the column would keep the exiting claim's stale value in place for the next claim to inherit. Combined with the fill-when-NULL clause, a row entering `'opening'` is therefore always guaranteed to find `pr_claimed_at NULL` beforehand, so a later re-claim of the same proposal (`opening → failed → opening`) always ages from its own claim time, never an earlier one — regardless of which binary version performed either transition. The sweep compares `now - pr_claimed_at` directly against `REMEDIATION_PR_OPENING_GRACE_PERIOD`, read from a stored timestamp rather than tracked in memory: a claim taken moments before a pass runs is never raced out from under an operator, because its age is always far short of the grace period regardless of the reconciler's poll interval or how many passes have run. The release itself is a compare-and-set on the exact `pr_claimed_at` the caller observed or was itself given (`FailStuckOpeningPR`), not a blind `pr_state='opening'` write, and this applies to both callers of that CAS: the reconciler's opening sweep releasing a claim it read earlier in the same pass, and `FailPullRequest` releasing the exact claim its own `BeginPullRequest` call acquired. A claim released and re-claimed by a second reconciler instance, an operator's retry, or the opening sweep itself (if the S3/GitHub round trip in Flow 12 outlives the grace period) between the caller's own claim/observation and this point leaves the CAS a no-op (`released=false`, not an error), so the fresh claim is never clobbered — this is the fresh-claim invariant every write against an `'opening'` row must preserve, and a claim's age is never computed from an earlier claim's timestamp. `RecordPR` — the write both this sweep's `Record` step and ui's own `RecordPullRequest` call issue to resolve a claim into `'open'` — preserves the same invariant with a `WHERE pr_state='opening'` guard rather than a blind write: whichever of the two callers reaches the row first wins, and the other's call is a no-op, never a second write racing to overwrite the first with the same pr_url and pr_number. A row with `pr_claimed_at IS NULL` — unmeasurable regardless of cause — is left untouched rather than swept, since an unmeasurable claim can never safely be judged stale. A per-row GitHub error leaves the row untouched regardless of its age — an inconclusive read is not a confirmed miss — and does not block the rest of the batch. Because the cursor advances to the next page before any row in the current page is handled, a page of persistently unresolvable rows (a standing GitHub error, or a claim that never ages out) never keeps the rows behind it out of every pass: a page shorter than the limit wraps the cursor back to the start, so a full rotation through every stuck row repeats indefinitely instead of a single stuck prefix monopolizing every pass.

## Why These Diagrams Are Not Enough On Their Own

These diagrams show timing and ordering well, but they do not fully show:

- who owns durable state
- which service is the source of truth for a given field
- which Redis streams are durable integration boundaries vs local loops
- which side effects are retried through outbox tables
- which flows are optional or currently unconsumed

Use these diagrams together with the service dossiers.
