# Interaction Matrix

## Dependency Matrix

Legend:

- `R` = read
- `W` = write
- `RW` = both
- `-` = no direct interaction found

| Service | Own Postgres | Own Neo4j | Redis | state gRPC | orchestrator gRPC | release-controller HTTP | K8s API | S3 | dbt Postgres | LLM provider HTTPS | agent-runner gRPC |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `state` | `RW` | `-` | `RW` | server | `-` | `-` | `-` | `-` | `-` | `-` | `-` |
| `orchestrator` | `RW` (`topology_state` also read on query path) | `RW` | `RW` | `R` (watchdog) | server | `-` | `-` | `-` | `-` | `-` | `-` |
| `executor-controller` | `RW` | `-` | `RW` | `-` | `-` | `-` | `W` | `-` | `W` (candidate schema teardown) | `-` | `-` |
| `k8s-controller` | `RW` | `-` | `RW` | `-` | `-` | `-` | `R` | `W` | `-` | `-` | `-` |
| `manifest-controller` | `-` | `-` | `RW` | `-` | `-` | `-` | `-` | `RW` | `-` | `-` | `-` |
| `release-controller` | `RW` | `-` | `RW` | `-` | `-` | server | `-` | `W` (prune-time delete of `candidate-sql/<release_id>/`) | `-` | `-` | `-` |
| `ui-service` | `-` | `-` | `RW` | `RW` | `R` | `R` | `-` | `R` | `-` | `-` | `RW` (bidi stream) |
| `agent-runner` | `RW` (`continuo_agent`) | `-` | `RW` (optional, rate limiter) | `-` | `-` | `-` | `-` | `W` (optional archive) | `-` | `W` (tool-use loop) | server |
| `remediation` | `RW` (`continuo_remediation`) | `-` | `RW` | `-` | `-` | `-` | `-` | `R` (dbt log fetch from `logs/` prefix) | `-` | `-` | `-` |
| `remediation-agent` | `RW` (`continuo_remediation_agent`) | `-` | `RW` | `-` | `R` (`GetNodeAncestry`, best-effort) | `-` | `-` | `RW` (read candidate SQL + dbt log; write `proposed-fix/` artifacts) | `-` | `W` (Anthropic or OpenAI-compatible HTTPS) | `-` |
| `continuo CLI` | `-` | `-` | `-` | `R` | `R` | `-` | `-` | `-` | `-` | `-` | `-` |

> `ui-service` uses Redis only for server-side login sessions (`AUTH_MODE=oidc`): plain `uisession:<id>` keys with TTLs, created at the OIDC (OpenID Connect) callback, read and refreshed on every authenticated request, deleted on logout. They are ordinary keys, not Redis Streams — `ui-service` produces and consumes no stream events, and `pkg/streams/contract.yaml` is unaffected. In `AUTH_MODE=dev` it constructs no Redis client.

> `continuo CLI` is an external consumer (not a Docker Compose service). It is invoked by humans or LLM agents and makes direct gRPC calls to `state` (port 50051) and `orchestrator` (port 50052). It produces no Redis events and holds no durable state.

> `agent-runner` reaches `state` and `orchestrator` indirectly: it spawns the bundled `continuo` CLI binary via direct argv exec (no shell) and that subprocess makes the gRPC calls. `agent-runner` itself holds no gRPC stubs for those services and never imports their internals. Its S3 writes are optional chat-archive uploads to `chat-archive/<user>/<thread>.json`. Chat conversations involve no Redis Streams — the `AgentChat.Chat` RPC is a synchronous bidirectional gRPC stream between `ui-service` and `agent-runner`. When `REDIS_ADDR` is set, `agent-runner` holds one Redis connection used solely for the shared per-user rate limiter (plain sorted-set keys, not a stream).

## Redis Stream Matrix

| Stream | Producer(s) | Consumer(s) | Purpose |
|---|---|---|---|
| `schedules.loaded:v1` | `orchestrator` | `state` | Reconcile `schedule_catalog` |
| `scheduler.started:v1` | `state` | `orchestrator` | Start schedule initialization; orchestrator creates run snapshot and emits `run.entries.dispatched:v1` |
| `run.entries.dispatched:v1` | `orchestrator` | `state` | All task entries with pre-assigned UUIDs and per-task manifest_version + image_tag (each carries the canonical k8s retry budget `pkg/events.DefaultTaskMaxRetries = 2`); state creates task rows, sets total_task_count, marks run as initialized |
| `run.entries.dispatch_failed:v1` | `orchestrator` | `state` | Symmetric counterpart of `run.entries.dispatched:v1`. Emitted when orchestrator cannot produce dispatch work for a run. Carries a `reason`: `target_not_found` (single-node-run target missing), `empty_projection` (topology has zero active nodes), or `invalid_node_type` (a seed/root or unblocked node has an unparseable node_type — a permanent defect, so the run fails fast instead of stalling until the watchdog cancels it). State row-locks `scheduler_tracker`, finalizes it as `failed`, and writes `run.finalized:v1`. Idempotent on already-terminal rows. |
| `trigger.rerun:v1` | `state` (outbox processor on `TriggerRerun` gRPC call) | `orchestrator` | Request rerun; orchestrator's `HandleRerun` runs `Snapshot(SourcePinnedDAG{})` against the source's pinned `:EXECUTES` set and emits `run.entries.dispatched:v1` for the new run |
| `trigger.rebase:v1` | `state` (outbox processor on `TriggerRebase` gRPC call) | `orchestrator` | Request rebase from a terminal source run; orchestrator's `HandleRebase` runs `Snapshot(RebasePartition)` and emits `run.entries.dispatched:v1` (rebased rows = latest metadata; inherited rows = source's pinned metadata) |
| `trigger.single_node_run:v1` | `state` (outbox processor on `TriggerSingleNodeRun` gRPC call) | `orchestrator` | Request a one-task run; orchestrator's `HandleSingleNodeRun` runs `Snapshot(SingleNode)` and emits `run.entries.dispatched:v1` + one `query.model:v1`. Latest mode reads metadata from `:TopologyRoot`; `snapshot_of_run` mode reads it from the source `:Run`'s `:EXECUTES` edge |
| `query.model:v1` | `orchestrator` | `executor-controller` | Dispatch executable nodes; carries image_tag and manifest_version as stream fields |
| `node.deployed:v1` | `executor-controller` | `k8s-controller` | Begin runtime monitoring |
| `check.k8s:v1` | `k8s-controller` | `k8s-controller` | Delayed re-check queue |
| `retry.task:v1` | `k8s-controller` | `executor-controller` | Re-dispatch retry deployment |
| `task.failed:v1` | `k8s-controller` | not consumed | Terminal failure event (external observability) |
| `task.status.updated:v1` | `k8s-controller` (RUNNING + SUCCEEDED/FAILED — the pod lifecycle), `executor-controller` (FAILED only, on the never-deployed path: permanent dispatch error or retry-exhaustion before a pod exists), `orchestrator` (SKIPPED on cascade-skip) | `state` | Task status update; drives finalization state machine in state. Each producer owns a non-overlapping slice; all serialize via the shared `pkg/events.TaskStatusUpdated.ToMap`. |
| `task.execution.recorded:v1` | `k8s-controller` | `state` | Persist task execution record with timing and S3 log key |
| `node.updated:v1` | `k8s-controller` (**also `executor-controller` on permanent dispatch error or retry-exhaustion**) | `orchestrator` | Node terminal status projection; orchestrator unlocks downstream nodes |
| `run.finalized:v1` | `state` | `orchestrator` | Run completed; emitted when all tasks reach terminal state. Orchestrator projects the outcome onto Neo4j `:Run.terminal_status` / `:Run.completed_at`. |
| `schedule.cancelled:v1` | `state` (triggered by `ui-service.CancelSchedule` **OR by `orchestrator` dispatch watchdog**) | `orchestrator`, `executor-controller`, `k8s-controller` | Signal active-run cancellation; consumers halt in-flight work for the cancelled schedule. Watchdog uses `cancelled_by="watchdog"` and `cancellation_reason="watchdog: ..."`. |
| `release.requested:v1` | `release-controller` | `manifest-controller` | Trigger manifest load for a candidate release. |
| `manifest.loaded.candidate:v1` | `manifest-controller` | `release-controller` | Resolved candidate topology (or parse failure) for a release; release-controller derives the validation set and advances the state machine. |
| `validation.requested:v1` | `release-controller` | `executor-controller` | Candidate-release validation request; each node entry carries `upstream_node_ids` (in-set gating edges, intra- and cross-service) and `candidate_sql_uri` (the `s3://` pointer to the node's rewritten SQL, empty for seeds); executor-controller creates one `executor_deployments` row per node (`blocked` or `pending`). |
| `validation.node.completed:v1` | `k8s-controller` | `executor-controller` | Per-node validation Job terminal status; executor-controller records the outcome, unblocks or skips in-set downstreams, and runs the per-release aggregate-emit gate. |
| `validation.completed:v1` | `executor-controller` | `release-controller` (result), `executor-controller` (group `executor-validation-completed`, candidate schema teardown) | Per-release validation aggregate; consumed by release-controller to advance the release state machine and by executor-controller to drop `_candidate_<release>` from the dbt warehouse. |
| `release.promoted:v1` | `release-controller` | `orchestrator` | A release is promoted to production; orchestrator swaps schedules, topology, and image tags. |
| `release.rejected:v1` | `release-controller` | `remediation` (group `remediation-release-rejected`) | A release failed parsing, validation, or pre-validation checks (e.g. `unbuildable_cross_service_upstream`); consumed by the remediation classifier to triage each failing node. |
| `remediation.requested:v1` | `remediation` | `remediation-agent` (group `remediation-agent-remediation-requested`) | Per-node remediation trigger for each healable validation failure; pointer-only payload (no error text). |
| `remediation.proposed:v1` | `remediation-agent` | (approval surface) | Per-node fix proposal; pointer-only payload (S3 URIs for proposed SQL and unified diff, short rationale, confidence). |

## Outbound gRPC Calls by Service

### Calls to `state`

Internal pipeline writes to `state` are event-driven (via Redis). The only remaining gRPC callers are UI-facing services.

| Caller | Methods used |
|---|---|
| `ui-service` | `ListAllSchedules`, `ListTasks`, `GetScheduler`, `ListTaskExecutions`, `ListNodeRuns`, `ListNodes`, `TriggerRerun`, `TriggerRebase`, `TriggerSingleNodeRun`, `TriggerSchedule`, `CancelSchedule` |
| `continuo CLI` | `ListAllSchedules`, `ListTasks`, `TriggerSchedule` |
| `orchestrator` (watchdog) | `ListStuckCandidates`, `CancelSchedule` |
| `orchestrator` (reconciler) | `GetScheduler` |

### Calls to `orchestrator`

| Caller | Methods used |
|---|---|
| `ui-service` | `GetScheduleGraph`, `ListRuns`, `GetRunGraph` |
| `continuo CLI` | `GetScheduleGraph` |

### Calls to `agent-runner`

| Caller | Methods used |
|---|---|
| `ui-service` | `AgentChat.Chat` (bidirectional streaming) |

> `agent-runner` does not make direct outbound gRPC calls to `state` or `orchestrator`. Instead it spawns the bundled `continuo` CLI subprocess (via direct argv exec), which makes those gRPC calls on its behalf using the public service addresses. This keeps agent-runner decoupled from service internals.

## HTTP Calls to `release-controller`

| Caller | Routes used |
|---|---|
| `ui-service` | `GET /releases`, `GET /releases/{id}`, `GET /current-prod` |

## S3 Matrix

| Service | Operation type | Concrete calls |
|---|---|---|
| `manifest-controller` | read + write | `download_file` (the `manifest.json` files named in the `release.requested:v1` `manifest_keys` list; no S3 listing); `PutObject` (candidate SQL per non-seed node to `candidate-sql/<release_id>/<unique_id>.sql`; upload failure aborts the load) |
| `k8s-controller` | write | `PutObject` (pod logs to `logs/` prefix) |
| `release-controller` | delete | `DeleteObjects` — prune-time delete of `candidate-sql/<release_id>/` prefix for each expired release (soft-fail; lifecycle rule is the backstop) |
| `ui-service` | read | `GetObject` — task-execution pod logs (proxied via `GET /api/task-executions/:id/logs`) and dbt validation logs (proxied via `GET /api/releases/log`) |
| `agent-runner` | write (optional) | `PutObject` — conversation archive to `chat-archive/<user>/<thread>.json` before a thread is deleted by the retention job; enabled when `RETENTION_ARCHIVE_S3=true` |
| `remediation` | read | `GetObject` — dbt execution log per failing node (`logs/` prefix, URI supplied by `release.rejected:v1`); fetch failure on a missing key is treated as an empty log and classified `unknown:log_unavailable` |
| `remediation-agent` | read + write | `GetObject` — candidate SQL (from `candidate_sql_uri` in the trigger) and dbt log (from `dbt_log_uri`); `PutObject` — proposed SQL to `proposed-fix/<release_id>/<node_id>/attempt-<attempt>.sql` and unified diff to `proposed-fix/<release_id>/<node_id>/attempt-<attempt>.diff` |

## Local Durable State by Service

| Service | Tables / durable structures |
|---|---|
| `state` | `scheduler_tracker`, `schedule_catalog` (+ `service_metadata` JSONB), `task_tracker` (+ `manifest_version` column), `task_execution`, `state_outbox`, `message_processing` |
| `orchestrator` | Neo4j `Table` (+ `image_tag`, `topology_generation`), `Run` (+ `topology_generation`, `service_metadata`), `DEPENDS_ON`, `EXECUTES` (+ `image_tag`); Neo4j `:TopologyRoot {id:'singleton'}`; Postgres `topology_state`, `message_processing`, `orchestrator_outbox` |
| `executor-controller` | `executor_deployments`, `executor_outbox`, `message_processing`, `cancelled_schedules`, `validation_aggregates` |
| `k8s-controller` | `k8s_outbox`, `message_processing` |
| `release-controller` | `releases` (+ `changed_service`, assembled `image_tags`), `current_prod` (live `topology_snapshot`), `service_prod` (per-service live `manifest_s3_key` + `image_tag` + `release_id`), `release_controller_outbox`, `message_processing` |
| `manifest-controller` | none |
| `ui-service` | Redis `uisession:<id>` plain keys (server-side login sessions, `AUTH_MODE=oidc`; TTL-bound, not streams) |
| `agent-runner` | Postgres `continuo_agent`: `threads` (conversation metadata per user), `messages` (full turn history), `pending_actions` (tool calls awaiting human confirmation) |
| `remediation` | Postgres `continuo_remediation`: `classification_decision` (audit of every triage outcome, emit and drop alike; natural key `(source, release_id, node_id)` enforces inbound idempotency), `remediation_outbox`, `message_processing` (FK target of outbox, not used for inbound consumer dedup) |
| `remediation-agent` | Postgres `continuo_remediation_agent`: `proposal` (one row per attempt; unique on `(release_id, node_id, attempt)`; status: `proposed`, `skipped`, `failed`, `escalated`), `remediation_agent_outbox`, `message_processing` |
