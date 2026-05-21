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
| `orchestrator` | `RW` (`topology_state` also read on query path) | `RW` | `RW` | `R` (watchdog) | server | `-` | `-` |
| `executor-controller` | `RW` | `-` | `RW` | `-` | `-` | `W` | `-` |
| `k8s-controller` | `RW` | `-` | `RW` | `-` | `-` | `R` | `W` |
| `manifest-controller` | `-` | `-` | `RW` | `-` | `-` | `-` | `R` |
| `ui-service` | `-` | `-` | `W` | `RW` | `R` | `-` | `-` |
| `continuo CLI` | `-` | `-` | `-` | `R` | `R` | `-` | `-` |

> `startup-controller` has been removed. Its responsibilities were absorbed into `orchestrator`.

> `continuo CLI` is an external consumer (not a Docker Compose service). It is invoked by humans or LLM agents and makes direct gRPC calls to `state` (port 50051) and `orchestrator` (port 50052). It produces no Redis events and holds no durable state.

## Redis Stream Matrix

| Stream | Producer(s) | Consumer(s) | Purpose |
|---|---|---|---|
| `update.graph:v1` | `ui-service` (manual UI button) **AND `deploy.yml` `publish-dbt-manifests` job (auto, end of every deploy)** | `manifest-controller` | Trigger manifest reload from `local` or `s3` source. **ACK on permanent**: validation failure (empty `image_tag`) → ERROR log `event=manifest_publish_rejected` + skip publish + ACK (S3 hasn't changed, redelivery cannot help). |
| `manifest.loaded:v1` | `manifest-controller` | `orchestrator` | Topology payload for graph ingestion; each node carries image_tag per-service. **ACK on permanent**: handler returns `events.ErrPermanent`-wrapped error → consumer logs ERROR, writes forensics row to `rejected_topology_messages`, ACKs (no XCLAIM loop). |
| `schedules.loaded:v1` | `orchestrator` | `state` | Reconcile `schedule_catalog` |
| `scheduler.started:v1` | `state` | `orchestrator` | Start schedule initialization; orchestrator creates run snapshot and emits `run.entries.dispatched:v1` |
| `run.entries.dispatched:v1` | `orchestrator` | `state` | All task entries with pre-assigned UUIDs and per-task manifest_version + image_tag (each carries the canonical k8s retry budget `pkg/events.DefaultTaskMaxRetries = 2`); state creates task rows, sets total_task_count, marks run as initialized |
| `run.entries.dispatch_failed:v1` | `orchestrator` | `state` | Symmetric counterpart of `run.entries.dispatched:v1`. Emitted when orchestrator cannot produce dispatch work for a run (e.g. single-node-run target not found). State row-locks `scheduler_tracker`, finalizes it as `failed`, and writes `run.finalized:v1`. Idempotent on already-terminal rows. |
| `trigger.rerun:v1` | `state` (outbox processor on `TriggerRerun` gRPC call) | `orchestrator` | Request rerun; orchestrator's `HandleRerun` runs `Snapshot(SourcePinnedDAG{})` against the source's pinned `:EXECUTES` set and emits `run.entries.dispatched:v1` for the new run |
| `trigger.rebase:v1` | `state` (outbox processor on `TriggerRebase` gRPC call) | `orchestrator` | Request rebase from a terminal source run; orchestrator's `HandleRebase` runs `Snapshot(RebasePartition)` and emits `run.entries.dispatched:v1` (rebased rows = latest metadata; inherited rows = source's pinned metadata) |
| `trigger.single_node_run:v1` | `state` (outbox processor on `TriggerSingleNodeRun` gRPC call) | `orchestrator` | Request a one-task run; orchestrator's `HandleSingleNodeRun` runs `Snapshot(SingleNode)` and emits `run.entries.dispatched:v1` + one `query.model:v1`. Latest mode reads metadata from `:TopologyRoot`; `snapshot_of_run` mode reads it from the source `:Run`'s `:EXECUTES` edge |
| `query.model:v1` | `orchestrator` | `executor-controller` | Dispatch executable nodes; carries image_tag and manifest_version as stream fields |
| `node.deployed:v1` | `executor-controller` | `k8s-controller` | Begin runtime monitoring |
| `check.k8s:v1` | `k8s-controller` | `k8s-controller` | Delayed re-check queue |
| `retry.task:v1` | `k8s-controller` | `executor-controller` | Re-dispatch retry deployment |
| `task.failed:v1` | `k8s-controller` | not consumed | Terminal failure event (external observability) |
| `task.status.updated:v1` | `executor-controller` (RUNNING **and FAILED on permanent dispatch error or retry-exhaustion**), `k8s-controller` (SUCCEEDED/FAILED), `orchestrator` (SKIPPED on cascade-skip) | `state` | Task status update; drives finalization state machine in state. All producers serialize via the shared `pkg/events.TaskStatusUpdated.ToMap`. |
| `task.execution.recorded:v1` | `k8s-controller` | `state` | Persist task execution record with timing and S3 log key |
| `node.updated:v1` | `k8s-controller` (**also `executor-controller` on permanent dispatch error or retry-exhaustion**) | `orchestrator` | Node terminal status projection; orchestrator unlocks downstream nodes |
| `run.finalized:v1` | `state` | `orchestrator` | Run completed; emitted when all tasks reach terminal state. Orchestrator projects the outcome onto Neo4j `:Run.terminal_status` / `:Run.completed_at`. |
| `schedule.cancelled:v1` | `state` (triggered by `ui-service.CancelSchedule` **OR by `orchestrator` dispatch watchdog**) | `orchestrator`, `executor-controller`, `k8s-controller` | Signal active-run cancellation; consumers halt in-flight work for the cancelled schedule. Watchdog uses `cancelled_by="watchdog"` and `cancellation_reason="watchdog: ..."`. |

## Outbound gRPC Calls by Service

### Calls to `state`

Internal pipeline writes to `state` are event-driven (via Redis). The only remaining gRPC callers are UI-facing services.

| Caller | Methods used |
|---|---|
| `ui-service` | `ListAllSchedules`, `ListTasks`, `GetScheduler`, `ListTaskExecutions`, `ListNodeRuns`, `TriggerRerun`, `TriggerRebase`, `TriggerSingleNodeRun`, `TriggerSchedule`, `CancelSchedule` |
| `continuo CLI` | `ListAllSchedules`, `TriggerSchedule` |
| `orchestrator` (watchdog) | `ListAllSchedules`, `ListTasks`, `CancelSchedule` |

### Calls to `orchestrator`

| Caller | Methods used |
|---|---|
| `ui-service` | `GetScheduleGraph`, `ListRuns`, `GetRunGraph`, `ListActiveRunDrifts` |
| `continuo CLI` | `GetScheduleGraph` |

## S3 Matrix

| Service | Operation type | Concrete calls |
|---|---|---|
| `manifest-controller` | read | `list_objects_v2`, `download_file` (manifest.json + service_metadata.json sidecar) |
| `k8s-controller` | write | `PutObject` |

## Local Durable State by Service

| Service | Tables / durable structures |
|---|---|
| `state` | `scheduler_tracker`, `schedule_catalog` (+ `service_metadata` JSONB), `task_tracker` (+ `manifest_version` column), `task_execution`, `state_outbox`, `message_processing` |
| `orchestrator` | Neo4j `Table` (+ `image_tag`, `topology_generation`), `Run` (+ `topology_generation`, `service_metadata`), `DEPENDS_ON`, `EXECUTES` (+ `image_tag`); Neo4j `:TopologyRoot {id:'singleton'}`; Postgres `topology_state`, `message_processing`, `orchestrator_outbox`, `rejected_topology_messages` (forensic sink for ingest validation rejections) |
| `executor-controller` | `executor_deployments`, `executor_outbox`, `message_processing`, `cancelled_schedules` |
| `k8s-controller` | `k8s_outbox`, `message_processing` |
| `manifest-controller` | none |
| `ui-service` | none |
