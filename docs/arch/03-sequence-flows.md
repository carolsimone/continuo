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
  Note right of R: payload — runner_id, schedule_name, service_metadata, kind='cron', source_run_id=''

  R->>OR: consume scheduler.started v1
  Note over OR: HandleSchedulerStartedHandler.Handle (1 tx)<br/>Snapshot(LatestFullDAG) in Neo4j — Run + EXECUTES with pre-assigned task UUIDs<br/>frontier (ReadyToDispatch: seeds-first-else-roots) computed by the selector — no second Neo4j read
  alt Snapshot returns ErrEmptyProjection (zero :Table nodes for this schedule)
    OR->>R: publish run.entries.dispatch_failed:v1 (reason=empty_projection)
    R->>ST: consume run.entries.dispatch_failed:v1
    Note over ST: RunEntriesDispatchFailedHandler.Handle (1 tx)<br/>row-lock scheduler_tracker, FinalizeRunTx → status='failed'<br/>state_outbox INSERT for run.finalized:v1
  else snapshot succeeds
    OR->>R: publish run.entries.dispatched v1
    OR->>R: publish query.model v1 (per seed/root)

    par state registers run skeleton
      R->>ST: consume run.entries.dispatched v1
      Note over ST: RunEntriesDispatchedHandler.Handle (1 tx)<br/>row-lock scheduler_tracker (skip if cancelled)<br/>BulkCreate task_tracker rows — status=PENDING<br/>SetTotalTaskCount, init_status=completed, status=RUNNING
    and executor launches seed/root jobs
      R->>EC: consume query.model v1
      Note over EC: write executor_deployments (pending)<br/>deployer.Dispatcher — CreateQueryJob (idempotent on JobName)<br/>write executor_outbox rows (task_status_updated RUNNING + node_deployed)
      EC->>R: publish task.status.updated v1 (RUNNING)
      EC->>R: publish node.deployed v1
      R->>ST: consume task.status.updated v1 (RUNNING)
      Note over ST: task_tracker.status = RUNNING
      R->>KC: consume node.deployed v1
      Note over KC: start poll loop — CheckJobStatus + check.k8s v1 backoff
    end
  end
```

> On the success path (right side of the alt), stages 2A (state) and 2B (executor) race — the seed K8s Job can start before the matching `task_tracker` row exists. `TaskStatusUpdatedHandler` tolerates this by NACKing until `RunEntriesDispatchedHandler` has caught up. On the failure path (left side), if a schedule's topology has zero active `:Table` nodes — typically a configuration error in the dbt manifest — the cron run fails fast via `run.entries.dispatch_failed:v1` with `reason=empty_projection`; the state consumer is the same `RunEntriesDispatchFailedHandler` used by single-node, rerun, and rebase paths.

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
  EC->>EC: write executor_outbox
  EC->>ST: UpdateTask(status=RUNNING)
  EC->>R: publish node.deployed:v1
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
    Note over EC: write executor_deployments (pending)<br/>deployer.Dispatcher — CreateQueryJob<br/>write executor_outbox rows (task_status_updated RUNNING + node_deployed)
    EC->>R: publish task.status.updated v1 (RUNNING)
    EC->>R: publish node.deployed:v1
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
  participant UI as ui-service
  participant ST as state
  participant R as Redis
  participant OR as orchestrator
  participant N4 as Neo4j

  U->>UI: POST /api/schedulers/{id}/rerun (empty body)
  UI->>ST: TriggerRerun(source_run_id)
  Note over ST: RerunHandler.TriggerRerun (sync, 1 tx) — delegates to<br/>synthesiseDerivedRun(kind='rerun', stream='trigger.rerun:v1'):<br/>validations — source exists / source FAILED|CANCELLED / source has ≥1 non-SUCCEEDED task / no active run on schedule_name<br/>scheduler_tracker INSERT — new row, kind='rerun', source_run_id=src, schedule_name=src.schedule_name, status=PENDING<br/>state_outbox INSERT for trigger.rerun:v1 with payload {schedule_id, schedule_name, kind, source_run_id}<br/>source row left untouched — stays at terminal status forever
  ST-->>UI: TriggerRerunResponse { run_id, schedule_name }

  ST->>R: publish trigger.rerun:v1 (via pkg/outbox.Processor)

  R->>OR: consume trigger.rerun:v1
  Note over OR: DerivedRunHandler.Handle (rerun config, 1 tx)<br/>Snapshot(SourcePinnedDAG{}) in Neo4j —<br/>  read source :Run's :EXECUTES set<br/>  seed rebase set with non-SUCCEEDED source tasks<br/>  grow rebase set by DescendantsInSourceRun<br/>  MERGE new :Run inheriting topology_generation + service_metadata from source :Run<br/>  project rebased rows → PENDING with source's pinned (image_tag, manifest_version)<br/>  project everything else → InitialStatus = source's stored status, with inherited_from_task_id (root-resolved)<br/>DispatchDerivedRun helper writes 1× run.entries.dispatched:v1 (full projection, per-task Status + InheritedFromTaskID) + N× query.model:v1 (rebased rows only)
```

**Differences vs. Flow 1 (cron/trigger):** the new `:Run` inherits `topology_generation` + `service_metadata` from the **source** `:Run` (not `:TopologyRoot`), so the rerun stays bound to the source's snapshot metadata. The source run is never mutated; the schedule's run history grows by one entry per rerun trigger.

**Differences vs. Flow 5 (rebase):** rerun reads against the source's pinned `:EXECUTES` set (no drift, no new arrivals), whereas rebase reads against latest topology and adds new arrivals. The eligibility checks, payload shape, helper code paths, and downstream pipeline (`run.entries.dispatched:v1` + `query.model:v1`) are otherwise identical.

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

  ST->>R: publish trigger.single_node_run:v1 (via pkg/outbox.Processor)
  Note right of R: payload — schedule_id, schedule_name, service_name,<br/>schema_name, table_name, metadata_source, source_run_id?

  R->>OR: consume trigger.single_node_run:v1
  Note over OR: HandleSingleNodeRunHandler.Handle (1 tx)<br/>dedup on message_processing<br/>Neo4j: Snapshot(SingleNode{Target, MetadataSource, SourceRunID?})<br/>  latest mode → selector reads :TopologyRoot + :Table for metadata, new :Run inherits from :TopologyRoot<br/>  stale mode  → selector reads source :Run's EXECUTES edge, new :Run inherits topology_generation + service_metadata from source :Run
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
    Note over EC: identical to Flow 1 from here<br/>executor_deployments (pending) → deployer.Dispatcher → CreateQueryJob → executor_outbox rows → task.status.updated:v1 RUNNING + node.deployed:v1
  end
```

> Differences vs. Flow 1 (schedule startup): synchronous gRPC entry; synthesised `schedule_name` is not in `schedule_catalog`; `Snapshot(SingleNode)` creates exactly one `EXECUTES` edge (no full topology snapshot); only a single `query.model:v1` is produced; on `ErrTargetNotFound` the synthesised run is immediately failed.

### ListNodeRuns read path

Before triggering a single-node run the UI fetches the node's run history: `GET /api/nodes/:service/:schema/:table/runs` → `state.ListNodeRuns(service_name, schema_name, table_name, limit=50)`. This is a pure read with no side effects. `state` executes one SQL query (`NodeRunRepository.List`) that joins `task_tracker × scheduler_tracker × task_execution` via a `target_tasks → latest_exec` CTE chain and returns the most recent rows ordered by `scheduler_tracker.created_at DESC`. The result drives the node detail page and populates the source-run picker used by the `metadata_source=snapshot_of_run` branch of Flow 9.

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
  ST->>R: publish trigger.rebase v1 (via pkg/outbox.Processor)
  Note right of R: payload — schedule_id (NEW run), schedule_name, source_run_id

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
    Note over EC: identical to Flow 1 from here<br/>executor_deployments (pending) → deployer.Dispatcher → CreateQueryJob → executor_outbox rows → task.status.updated v1 RUNNING + node.deployed v1
  end
```

> Differences vs. Flow 4 (rerun): rebase reads against the **latest** topology, so new arrivals and topology drift land in the projection; rerun reads strictly against the source's pinned `:EXECUTES` set. Both share the same materialiser, the same `:Run` MERGE, and the same `run.entries.dispatched:v1` pipeline.

## 11. dbt Blue/Green Release (Validate-Then-Promote)

A candidate dbt release targets a single dbt service: that team's CI uploads only its own compiled manifest to the canonical S3 key `s3://<bucket>/<service>/<release_id>/manifest.json` and posts a one-service candidate. When the release becomes active, `release-controller` assembles the full manifest set by combining the changed service's new key with every other service's live `service_prod` pointer, parses the whole set, validates every changed node self-contained in an isolated schema, and swaps production (Neo4j topology, image tags, schedule catalog) only if validation passes. `release-controller` owns the lifecycle, holds `current_prod` (the live topology snapshot) and the `service_prod` pointer table (one row per service: its live manifest key + image tag + release id). Releases run a FIFO queue: one release parses/validates at a time, each terminal outcome advances the next, and each promotion refreshes the changed service's `service_prod` pointer.

```mermaid
sequenceDiagram
  participant CI as CI deploy
  participant S3 as S3
  participant RC as release-controller
  participant R as Redis
  participant MC as manifest-controller
  participant EC as executor-controller
  participant KC as k8s-controller
  participant OR as orchestrator
  participant ST as state

  Note over CI,RC: Phase 1 — submit (one service per release)
  CI->>S3: upload {service}/{release_id}/manifest.json (canonical key)
  CI->>RC: POST /releases {service, release_id, image_tag, bootstrap?}
  Note over RC: create Received release for this service (idempotent on release_id)<br/>FIFO queue advance → Parsing<br/>assemble manifest_keys from service_prod (changed service + every other<br/>service's live pointer); persist assembled image tags<br/>release_controller_outbox INSERT for release.requested:v1
  RC->>R: publish release.requested:v1 {release_id, manifest_keys:[{service, s3_uri}]}

  Note over MC,RC: Phase 2 — candidate parse
  R->>MC: consume release.requested:v1
  MC->>S3: download each manifest named in manifest_keys (explicit key list)
  Note over MC: parse model/seed/snapshot nodes<br/>content_hash = dbt checksum (sha256 fallback, never empty)<br/>resolve upstreams via sqlglot (qualified refs only)<br/>rewrite SQL to _candidate_{release} schema refs via sqlglot<br/>upload rewritten SQL → S3 candidate-sql/{release_id}/{unique_id}.sql<br/>(upload failure is fatal; seeds → empty candidate_sql_uri)
  alt manifest malformed / unqualified table ref / S3 upload failure
    MC->>R: publish manifest.loaded.candidate:v1 {status=failed, error_class}
    R->>RC: consume manifest.loaded.candidate:v1 (failed)
    Note over RC: Reject(parse_failed) → release.rejected:v1, advance queue
  else parsed and uploaded ok
    MC->>R: publish manifest.loaded.candidate:v1 {status=ok, topology[] (per node: candidate_sql_uri)}
    R->>RC: consume manifest.loaded.candidate:v1 (ok)
    Note over RC: join per-service image_tags into candidate topology
  end

  Note over RC: Phase 3 — change detection + gate
  alt bootstrap:true OR nothing to validate (in-set empty)
    Note over RC: promote directly (skip validation)<br/>current_prod ← candidate topology<br/>upsert changed service's service_prod pointer, transition Promoted
    RC->>R: publish release.promoted:v1
  else has changed nodes
    Note over RC: changed = content_hash diff vs current_prod snapshot<br/>(empty snapshot / new node ⇒ treated as changed)<br/>inSet = DescendantsClosure(changed) ∪ FullAncestorsClosure(inSet)<br/>(changed + downstream + full transitive upstream closure, cross-service)
    alt an in-set node has a direct upstream absent from the candidate topology
      Note over RC: Reject(unbuildable_cross_service_upstream)
      RC->>R: publish release.rejected:v1
    else buildable
      Note over RC: transition Validating
      RC->>R: publish validation.requested:v1<br/>{candidate_schema=_candidate_{id}, per node:<br/>node_type, image_tag, candidate_sql_uri, upstream_node_ids}
    end
  end

  Note over EC,KC: Phase 4 — self-contained validation (one empty-build Job per node, gated in dependency order)
  R->>EC: consume validation.requested:v1
  Note over EC: per node → executor_deployments (mode=validation)<br/>roots → pending, nodes with upstreams → blocked<br/>(inbound dedup is per-release)
  loop dispatch pending rows, unblocking downstream as upstreams settle ok
    Note over EC: model/snapshot → validation_runner.py (CANDIDATE_SQL_URI env):<br/>fetch SQL from S3 via boto3 → CREATE TABLE {candidate}.{table} AS (fetched SQL) WITH NO DATA<br/>seed (empty CANDIDATE_SQL_URI) → dbt seed --select {table} --empty
    EC->>R: publish node.deployed:v1 (synthetic ids — routes by mode=validation label)
    R->>KC: consume node.deployed:v1 / check.k8s:v1
    Note over KC: poll Job, re-arm check.k8s:v1 until terminal
    KC->>S3: upload runner/dbt pod log
    KC->>R: publish validation.node.completed:v1 {release_id, node_id, outcome, dbt_log_uri}
    R->>EC: consume validation.node.completed:v1
    Note over EC: RecordOutcome, then gating — ok unblocks ready downstream,<br/>non-ok skips all reachable downstream
  end
  Note over EC: per-release advisory lock + emission sentinel (exactly-once):<br/>when no node remains pending/blocked/deployed → build aggregate
  EC->>R: publish validation.completed:v1<br/>{per_node_results[{node_id, status, dbt_log_uri}], aggregate_status, candidate_schema}

  Note over RC: Phase 5 — promote or reject
  R->>RC: consume validation.completed:v1
  alt aggregate_status=ok
    Note over RC: current_prod ← candidate topology<br/>upsert changed service's service_prod pointer, transition Promoted
    RC->>R: publish release.promoted:v1
  else any node failed / missing
    Note over RC: Reject(validation_failed) → release.rejected:v1
  end
  Note over RC: advance FIFO queue

  Note over OR,ST: Phase 6 — production swap
  R->>OR: consume release.promoted:v1
  Note over OR: PromoteRelease (Neo4j, 1 tx) — retire-then-orphan-cleanup:<br/>idempotent if :Meta current_release already = release_id<br/>retire :Table not in release (keep :Run-[:EXECUTES] history)<br/>MERGE release nodes (node_type, image_tag, schedule_name), active=true<br/>rebuild :DEPENDS_ON, DETACH DELETE orphaned retired nodes<br/>MERGE :Meta current_release = release_id
  OR->>OR: IncrementGeneration (topology_state, separate tx)<br/>SetServiceMetadata on :TopologyRoot
  OR->>R: publish schedules.loaded:v1 {schedule_names, service_metadata, topology_generation}
  R->>ST: consume schedules.loaded:v1
  Note over ST: ScheduleCatalogHandler — Reconcile schedule_catalog (empty-list guard)
  R->>EC: consume validation.completed:v1 (executor-validation-completed group)
  Note over EC: drop the _candidate_{release} schema (teardown)
```

**Self-contained validation (zero model edits).** The validation build set is the changed nodes, their downstream descendants, and their *full transitive upstream closure across service boundaries*. `manifest-controller` rewrites each node's compiled SQL to the candidate schema (via sqlglot), uploads it to S3 at `candidate-sql/<release_id>/<unique_id>.sql`, and emits a per-node `candidate_sql_uri` — an `s3://` reference to that object. `executor-controller` builds every upstream as an empty table (`CREATE TABLE … WITH NO DATA`, seeds via `dbt seed --empty`) in `_candidate_<release>` in dependency order, then the changed node against them. The validation runner (`validation_runner.py`) fetches the SQL object from S3 via `CANDIDATE_SQL_URI` before materializing the table. Because the fetched SQL's refs already point at the candidate schema, a model whose source still reads `FROM analytics.table_a` validates against the candidate copy — teams never template their schema names. Nothing in production is touched during validation, and the candidate schema is dropped after the aggregate result is consumed.

**Gating and exactly-once aggregation.** Each node's `executor_deployments` row starts `blocked` if it has in-set upstreams; a node is dispatched only once all its upstreams have settled `ok` (their empty tables now exist). A non-`ok` terminal skips all reachable downstream nodes. When no node remains non-terminal, a per-release advisory lock plus an insert-once emission sentinel guarantee a single `validation.completed:v1` is produced even under redelivery or crash-retry. `aggregate_status` is `ok` iff every per-node status is `ok`.

**Reject reasons.** A release ends in `Rejected` for one of three reasons, each also emitted as `release.rejected:v1` for observers: `parse_failed` (manifest malformed, an unqualified table reference, or candidate SQL S3 upload failure), `unbuildable_cross_service_upstream` (an in-set node depends on an upstream absent from the candidate topology — i.e. not in the release at all), or `validation_failed` (one or more validation Jobs failed). For `validation_failed`, the `release.rejected:v1` event includes top-level `repo` and `commit_sha` (provenance) plus, per failing node, `candidate_sql_uri` — allowing a downstream remediation agent to fetch the SQL for each failed node without re-parsing the manifest.

**Bootstrap and empty-diff short-circuits.** A `bootstrap:true` release skips validation entirely: it records the candidate topology, seeds `current_prod`, and promotes — the initial cutover (or a trusted re-baseline) against an empty or mismatched snapshot. A non-bootstrap release against an empty snapshot instead treats every candidate node as changed and validates the whole topology. A release whose diff is empty (e.g. an image-tag-only bump) trivially passes the gate and promotes directly. All three promotion paths point `current_prod` at the candidate topology and upsert the changed service's `service_prod` pointer, so the next release's change-detection diff is correct and any other service's next release assembles against the refreshed pointer.

**Promotion is a lazy generation switch.** `PromoteRelease` swaps the live `:Table` topology using retire-then-orphan-cleanup — retired nodes still referenced by a `:Run-[:EXECUTES]` edge are kept, preserving run history; only truly orphaned retired nodes are deleted. The handler never reads or writes `:Run` nodes or `EXECUTES` edges, so in-flight runs are unaffected: an existing run keeps its `topology_generation` and its edges keep their pinned `image_tag`. The next scheduled run's `Snapshot(LatestFullDAG)` reads `:TopologyRoot` at call time and picks up the new generation and image tags. See **Flow 7 (Topology Versioning — Lazy Generation Switch)** for the consumption-side detail. `node_type` is threaded through the promotion MERGE so promoted seeds are typed correctly and seed-backed schedules dispatch their `dbt seed` jobs without stalling.

## Why These Diagrams Are Not Enough On Their Own

These diagrams show timing and ordering well, but they do not fully show:

- who owns durable state
- which service is the source of truth for a given field
- which Redis streams are durable integration boundaries vs local loops
- which side effects are retried through outbox tables
- which flows are optional or currently unconsumed

Use these diagrams together with the service dossiers.
