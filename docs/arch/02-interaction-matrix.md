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
| `orchestrator` | `RW` | `RW` | `RW` | `R` (watchdog) | server | `-` | `-` |
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
| `run.entries.dispatched:v1` | `orchestrator` | `state` | All task entries with pre-assigned UUIDs and per-task manifest_version + image_tag; state creates task rows, sets total_task_count, marks run as initialized |
| `initialize.run:v1` | *(rerun command path)* | `orchestrator` | Request rerun scope resolution |
| `run.rerun.dispatched:v1` | `orchestrator` | `state` | Rerun scope resolved; state resets target task(s) to PENDING |
| `query.model:v1` | `orchestrator` | `executor-controller` | Dispatch executable nodes; carries image_tag and manifest_version as stream fields |
| `node.deployed:v1` | `executor-controller` | `k8s-controller` | Begin runtime monitoring |
| `check.k8s:v1` | `k8s-controller` | `k8s-controller` | Delayed re-check queue |
| `retry.task:v1` | `k8s-controller` | `executor-controller` | Re-dispatch retry deployment |
| `task.failed:v1` | `k8s-controller` | not consumed | Terminal failure event (external observability) |
| `task.status.updated:v1` | `executor-controller` (RUNNING **and FAILED on permanent dispatch error or retry-exhaustion**), `k8s-controller` (SUCCEEDED/FAILED) | `state` | Task status update; drives finalization state machine in state |
| `task.execution.recorded:v1` | `k8s-controller` | `state` | Persist task execution record with timing and S3 log key |
| `node.updated:v1` | `k8s-controller` (**also `executor-controller` on permanent dispatch error or retry-exhaustion**) | `orchestrator` | Node terminal status projection; orchestrator unlocks downstream nodes |
| `run.finalized:v1` | `state` | *(future consumers)* | Run completed; emitted when all tasks reach terminal state |
| `schedule.cancelled:v1` | `state` (triggered by `ui-service.CancelSchedule` **OR by `orchestrator` dispatch watchdog**) | `orchestrator`, `executor-controller`, `k8s-controller` | Signal active-run cancellation; consumers halt in-flight work for the cancelled schedule. Watchdog uses `cancelled_by="watchdog"` and `cancellation_reason="watchdog: ..."`. |

## Outbound gRPC Calls by Service

### Calls to `state`

Internal pipeline writes to `state` are now event-driven (via Redis). The only remaining gRPC callers are UI-facing services.

| Caller | Methods used |
|---|---|
| `ui-service` | `ListAllSchedules`, `ListTasks`, `GetScheduler`, `ListTaskExecutions`, `TriggerRerun`, `TriggerSchedule`, `CancelSchedule` |
| `continuo CLI` | `ListAllSchedules`, `TriggerSchedule` |
| `orchestrator` (watchdog) | `ListAllSchedules`, `ListTasks`, `CancelSchedule` |

### Calls to `orchestrator`

| Caller | Methods used |
|---|---|
| `ui-service` | `GetScheduleGraph`, `ListRuns`, `GetRunGraph` |
| `continuo CLI` | `GetScheduleGraph` |

## S3 Matrix

| Service | Operation type | Concrete calls |
|---|---|---|
| `manifest-controller` | read | `list_objects_v2`, `download_file` (manifest.json + service_metadata.json sidecar) |
| `k8s-controller` | write | `PutObject` |

## Local Durable State by Service

| Service | Tables / durable structures |
|---|---|
| `state` | `scheduler_tracker`, `schedule_catalog` (+ `service_metadata` JSONB), `task_tracker` (+ `manifest_version` column), `task_execution`, `state_outbox`, `processed_events` |
| `orchestrator` | Neo4j `Table` (+ `image_tag`, `topology_generation`), `Run` (+ `topology_generation`, `service_metadata`), `DEPENDS_ON`, `EXECUTES` (+ `image_tag`); Neo4j `:TopologyRoot {id:'singleton'}`; Postgres `topology_state`, `message_processing`, `outbox`, `published_messages`, `rejected_topology_messages` (forensic sink for V7 ingest validation rejections) |
| `executor-controller` | `deployment_outbox` (+ `image_tag` column), `processed_events` |
| `k8s-controller` | `k8s_status_outbox`, `processed_events` |
| `manifest-controller` | none |
| `ui-service` | none |
