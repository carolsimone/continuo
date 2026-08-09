# Interaction Matrix

## Dependency Matrix

Legend:

- `R` = read
- `W` = write
- `RW` = both
- `-` = no direct interaction found

| Service | Own Postgres | Own Neo4j | Redis | state gRPC | orchestrator gRPC | release-controller HTTP | K8s API | S3 | dbt Postgres | LLM provider HTTPS | GitHub HTTPS | agent-runner gRPC | remediation-agent gRPC |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `state` | `RW` | `-` | `RW` | server | `-` | `-` | `-` | `-` | `-` | `-` | `-` | `-` | `-` |
| `orchestrator` | `RW` (`topology_state` also read on query path) | `RW` | `RW` | `R` (watchdog) | server | `-` | `-` | `-` | `-` | `-` | `-` | `-` | `-` |
| `executor-controller` | `RW` | `-` | `RW` | `-` | `-` | `-` | `W` | `-` | `-` (no direct connection; candidate-schema create/drop run as engine-image K8s Jobs) | `-` | `-` | `-` | `-` |
| `k8s-controller` | `RW` | `-` | `RW` | `-` | `-` | `-` | `R` | `W` | `-` | `-` | `-` | `-` | `-` |
| `manifest-controller` | `-` | `-` | `RW` | `-` | `-` | `-` | `-` | `RW` | `-` | `-` | `-` | `-` | `-` |
| `release-controller` | `RW` | `-` | `RW` | `-` | `-` | server | `-` | `W` (prune-time delete of `candidate-sql/<release_id>/` and `code-bundles/<release_id>/`) | `-` | `-` | `-` | `-` | `-` |
| `ui-service` | `-` | `-` | `RW` | `RW` | `R` | `R` | `-` | `R` | `-` | `-` | `W` (GitHub App; create branch + commit + PR on `continuo-dbt-demo`) | `RW` (bidi stream) | `RW` (List/Get/Begin/Record/Fail PR) |
| `agent-runner` | `RW` (`continuo_agent`) | `-` | `RW` (optional, rate limiter) | `-` | `-` | `-` | `-` | `W` (optional archive) | `-` | `W` (tool-use loop) | `-` | server | `-` |
| `remediation` | `RW` (`continuo_remediation`) | `-` | `RW` | `-` | `-` | `-` | `-` | `R` (dbt log fetch from `logs/` prefix) | `-` | `-` | `-` | `-` | `-` |
| `remediation-agent` | `RW` (`continuo_remediation_agent`) | `-` | `RW` | `-` | `R` (`GetNodeAncestry`, best-effort; called once per validation proposal, and only as a fallback for seed_build) | `-` | `-` | `RW` (read candidate SQL + dbt log; write `proposed-fix/` artifacts) | `-` | `W` (Anthropic or OpenAI-compatible HTTPS) | `R` (Contents API, file + directory reads at `commit_sha`; commit API, best-effort upstream-ancestor diffs for validation; Pulls API, PR-status polling by the reconciler; all read-only) | `-` | server (port 50054) |
| `continuo CLI` | `-` | `-` | `-` | `R` | `R` | `-` | `-` | `-` | `-` | `-` | `-` | `-` | `-` |

> `ui-service` uses Redis only for server-side login sessions (`AUTH_MODE=oidc`): plain `uisession:<id>` keys with TTLs, created at the OIDC (OpenID Connect) callback, read and refreshed on every authenticated request, deleted on logout. They are ordinary keys, not Redis Streams — `ui-service` produces and consumes no stream events, and `pkg/streams/contract.yaml` is unaffected. In `AUTH_MODE=dev` it constructs no Redis client.

> `continuo CLI` is an external consumer (not a Docker Compose service). It is invoked by humans or LLM agents and makes direct gRPC calls to `state` (port 50051) and `orchestrator` (port 50052). It produces no Redis events and holds no durable state.

> `agent-runner` reaches `state` and `orchestrator` indirectly: it spawns the bundled `continuo` CLI binary via direct argv exec (no shell) and that subprocess makes the gRPC calls. `agent-runner` itself holds no gRPC stubs for those services and never imports their internals. Its S3 writes are optional chat-archive uploads to `chat-archive/<user>/<thread>.json`. Chat conversations involve no Redis Streams — the `AgentChat.Chat` RPC is a synchronous bidirectional gRPC stream between `ui-service` and `agent-runner`. When `REDIS_ADDR` is set, `agent-runner` holds one Redis connection used solely for the shared per-user rate limiter (plain sorted-set keys, not a stream).

> `remediation-agent` GitHub access is read-only: it calls the Contents API to fetch one file's raw text (`GET /repos/{repo}/contents/{path}?ref={commit_sha}`, `Accept: application/vnd.github.raw+json`) and, for compile proposals only, to list a directory's entries (`GET /repos/{repo}/contents/{dir}?ref={commit_sha}`, `Accept: application/vnd.github+json`) to find a failing model's co-located `schema.yml` files. For validation proposals only, it also calls the commit API (`GET /repos/{repo}/commits/{sha}`, `Accept: application/vnd.github+json`) to fetch, best-effort, the diff of the recent change to each of up to 5 most-recently-changed upstream ancestors, embedded into the Step-1 diagnosis prompt; a per-ancestor read error is logged and skipped rather than retried. It also calls the Pulls API to poll PR status (`GET /repos/{repo}/pulls/{number}`, `Accept: application/vnd.github+json`), used by the PR-outcome reconciler to detect a merged or closed-unmerged PR, and to look a PR up by head branch (`GET /repos/{repo}/pulls?head={owner}:{branch}&state=all&per_page=1`), used by the same reconciler's opening sweep to recover a `pr_state='opening'` claim whose PR was created on GitHub but never recorded. It never writes to, commits to, or opens pull requests against any repository — every call, across all APIs, is a GET. All calls are authenticated with a fine-grained PAT (`GITHUB_TOKEN`, `Contents: Read` and `Pull requests: Read` on the dbt repo) when the token is set; without a token the request is unauthenticated (subject to rate limits). For compile and seed_build proposals, the offending-file read is load-bearing: a 404 skips the proposal, any other error is transient and the trigger is redelivered. For validation, the real-source read is a Step-2 best-effort dependency: any failure there degrades to the candidate proposal (`source_resolved=false`) without affecting trigger reliability. For the Pulls API reads, any non-2xx response leaves the proposal's `pr_state` untouched and is retried on the reconciler's next pass.

> `ui-service` GitHub access is write-only (via GitHub App): it mints short-lived installation tokens and calls the Repos API to create a branch, write a file, and open a pull request on `continuo-dbt-demo` — the one repo the App is installed on. It never calls merge or delete APIs and never targets `main` directly. The App credential (`GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY`, `GITHUB_APP_INSTALLATION_ID`) is held in `continuo-app-credentials`. This path is exercised only on the operator-gated `POST /api/remediation/proposals/:id/pull-request` route.

## Redis Stream Matrix

| Stream | Producer(s) | Consumer(s) | Purpose |
|---|---|---|---|
| `schedules.loaded:v1` | `orchestrator` | `state` | Reconcile `schedule_catalog` |
| `scheduler.started:v1` | `state` | `orchestrator` | Start schedule initialization; orchestrator creates run snapshot and emits `run.entries.dispatched:v1` |
| `run.entries.dispatched:v1` | `orchestrator` | `state` | All task entries with pre-assigned UUIDs and per-task manifest_version + image_tag (each carries the canonical k8s retry budget `pkg/events.DefaultTaskMaxRetries = 2`); state creates task rows, sets total_task_count, marks run as initialized |
| `run.entries.dispatch_failed:v1` | `orchestrator` | `state` | Symmetric counterpart of `run.entries.dispatched:v1`. Emitted when orchestrator cannot produce dispatch work for a run. Carries a `reason`: `target_not_found` (single-node-run target missing), `empty_projection` (a whole-DAG `operation=run` schedule whose topology has zero active nodes), `invalid_node_type` (a seed/root or unblocked node has an unparseable node_type — a permanent defect, so the run fails fast instead of stalling until the watchdog cancels it), `no_tests` (no known-positive `test_count` to run: either a single-node `operation=test` target with a known-zero or unset count, or a whole-DAG `operation=test` schedule where every node is gated the same way), or `rerun_of_test_unsupported` (rerun/rebase source `:Run.operation == "test"` — a rerun/rebase cannot safely reissue `dbt test`, since the derived selectors carry no per-task operation; the caller must trigger a fresh `node test`/`schedule test` instead). State row-locks `scheduler_tracker` and finalizes it via `MarkDispatchTerminal`, writing `run.finalized:v1`: the benign `no_tests` reason finalizes `skipped`, every other reason finalizes `failed`. Idempotent on already-terminal rows. |
| `trigger.rerun:v1` | `state` (outbox processor on `TriggerRerun` gRPC call) | `orchestrator` | Request rerun; orchestrator's `HandleRerun` resolves the source run's operation, runs `Snapshot(SourcePinnedDAG{})` against the source's pinned `:EXECUTES` set, and emits `run.entries.dispatched:v1` for the new run — unless the source run's operation was `test`, in which case `SourcePinnedDAG.SelectTasks` rejects it (`reason=rerun_of_test_unsupported`); a source operation of `build` (or the default `""`) is inherited by the new run |
| `trigger.rebase:v1` | `state` (outbox processor on `TriggerRebase` gRPC call) | `orchestrator` | Request rebase from a terminal source run; orchestrator's `HandleRebase` resolves the source run's operation, runs `Snapshot(RebasePartition)`, and emits `run.entries.dispatched:v1` (rebased rows = latest metadata; inherited rows = source's pinned metadata) — unless the source run's operation was `test`, in which case `RebasePartition.SelectTasks` rejects it (`reason=rerun_of_test_unsupported`); a source operation of `build` (or the default `""`) is inherited by the new run |
| `trigger.single_node_run:v1` | `state` (outbox processor on `TriggerSingleNodeRun` gRPC call) | `orchestrator` | Request a one-task run; orchestrator's `HandleSingleNodeRun` runs `Snapshot(SingleNode)` and emits `run.entries.dispatched:v1` + one `query.model:v1`. Latest mode reads metadata from `:TopologyRoot`; `snapshot_of_run` mode reads it from the source `:Run`'s `:EXECUTES` edge |
| `query.model:v1` | `orchestrator` | `executor-controller` | Dispatch executable nodes; carries image_tag, manifest_version, and operation (`""`/`test`/`build`) as stream fields, on every dispatch path (initial frontier, downstream unblock, derived-run frontier) |
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
| `validation.requested:v1` | `release-controller` | `executor-controller` | Candidate-release validation request; each node entry carries `upstream_node_ids` (in-set gating edges, intra- and cross-service) and `candidate_artifact_uri` (the `s3://` pointer to the object the node's validation Job fetches: rewritten SQL for a dbt node, a validation spec for a python node, empty for a dbt seed); executor-controller creates one `executor_deployments` row per node (`blocked` or `pending`). |
| `validation.node.completed:v1` | `k8s-controller` | `executor-controller` | Per-node validation Job terminal status; executor-controller records the outcome, unblocks or skips in-set downstreams, and runs the per-release aggregate-emit gate. |
| `validation.result:v1` | `executor-controller` | `release-controller` (project + decide), `executor-controller` (group `executor-validation-result-teardown`, candidate schema teardown) | Unified validation-leg stream. `kind=node` messages carry a per-node outcome (emitted as each `mode=validation` node settles) which release-controller projects into the release's `per_node_results` read model. The trailing `kind=complete` message carries the per-release decision (`release_id`, `aggregate_status`, `candidate_schema` — no per-node content); release-controller advances the release state machine reading per-node results from its read model, and executor-controller drops `_candidate_<release>` by scheduling a one-shot engine-image `drop_schema` Job (it holds no warehouse connection). Each `kind=node` row gets its own generated `aggregate_id` (not the terminal's), so per-node rows publish to the outbox in parallel instead of draining through one shared FIFO lane; the decision reads `aggregate_status` and does not depend on delivery order. |
| `release.promoted:v1` | `release-controller` | `orchestrator` | A release is promoted to production; orchestrator swaps schedules, topology, and image tags. |
| `release.rejected:v1` | `release-controller` | `remediation` (group `remediation-release-rejected`) | A release failed parsing, validation, or pre-validation checks (e.g. `unbuildable_cross_service_upstream`); consumed by the remediation classifier to triage each failing node. |
| `remediation.requested:v1` | `remediation` | `remediation-agent` (group `remediation-agent-remediation-requested`) | Per-node remediation trigger for each healable validation failure; pointer-only payload (no error text). |
| `remediation.proposed:v1` | `remediation-agent` | (approval surface) | Per-node fix proposal; pointer-only payload (S3 URIs for proposed SQL and unified diff, short rationale, confidence). |
| `remediation.pr_opened:v1` | `remediation-agent` | (no consumer; audit seam) | Emitted when an operator records a pull request via `RecordPullRequest`. Pointer-only payload: `proposal_id`, `release_id`, `node_id`, `pr_url`, `pr_number`, `opened_by`, `opened_at`. |
| `remediation.pr_closed:v1` | `remediation-agent` | (no consumer; audit seam) | Emitted when the PR-outcome reconciler observes a terminal GitHub PR state and `RecordOutcome` performs the CAS `pr_state: open → merged | rejected`. Pointer-only payload: `proposal_id`, `release_id`, `node_id`, `pr_url`, `pr_number`, `outcome` (`merged` or `rejected`), `closed_at`. |
| `outbox.dead_letter:v1` | `state`, `orchestrator`, `executor-controller`, `k8s-controller`, `release-controller`, `remediation`, `remediation-agent` (every service's `pkg/outbox.Processor`) | (no consumer; operational DLQ) | Terminal outbox publish failure — a permanent payload error, or a transient error that exhausted its retry budget — written by the outbox processor in the same transaction that marks the original row `failed`. Operational signal distinct from domain `<event>.failed:v1` compensation events; see `docs/arch/05-error-classification.md` §Outbox processor resilience. |

## Outbound gRPC Calls by Service

### Calls to `state`

Internal pipeline writes to `state` are event-driven (via Redis). The only remaining gRPC callers are UI-facing services.

| Caller | Methods used |
|---|---|
| `ui-service` | `ListAllSchedules`, `ListTasks`, `GetScheduler`, `ListTaskExecutions`, `ListNodeRuns`, `ListNodes`, `TriggerRerun`, `TriggerRebase`, `TriggerSingleNodeRun`, `TriggerSchedule`, `CancelSchedule` |
| `continuo CLI` | `ListAllSchedules`, `ListTasks`, `TriggerSchedule` |
| `orchestrator` (watchdog) | `ListStuckCandidates`, `CancelSchedule` |
| `orchestrator` (reconciler) | `GetScheduler` |

> `ListNodeRuns` and `ListNodes` are both scoped by a `run`\|`test`\|`build` operation parameter (default `run`): a node's model-run history and test-run history are disjoint slices, and `ListNodes` omits a node from the catalog entirely for an operation it has never run under, rather than returning it with empty stats.

### Calls to `orchestrator`

| Caller | Methods used |
|---|---|
| `ui-service` | `GetScheduleGraph`, `ListRuns`, `GetRunGraph`, `GetNode` |
| `continuo CLI` | `GetScheduleGraph` |

### Calls to `agent-runner`

| Caller | Methods used |
|---|---|
| `ui-service` | `AgentChat.Chat` (bidirectional streaming) |

> `agent-runner` does not make direct outbound gRPC calls to `state` or `orchestrator`. Instead it spawns the bundled `continuo` CLI subprocess (via direct argv exec), which makes those gRPC calls on its behalf using the public service addresses. This keeps agent-runner decoupled from service internals.

### Calls to `remediation-agent`

| Caller | Methods used |
|---|---|
| `ui-service` | `ListProposals`, `GetProposal`, `BeginPullRequest`, `RecordPullRequest`, `FailPullRequest` |

## HTTP Calls to `release-controller`

| Caller | Routes used |
|---|---|
| `ui-service` | `GET /releases`, `GET /releases/{id}`, `GET /current-prod` |

## S3 Matrix

| Service | Operation type | Concrete calls |
|---|---|---|
| `manifest-controller` | read + write | `download_file` (the `manifest.json` or `contract.yaml` artifact named in each `release.requested:v1` `manifest_keys` entry — `kind` selects which; no S3 listing); `PutObject` (each node's candidate artifact — rewritten SQL for a dbt model/snapshot, a validation spec for a python node, skipped entirely for a dbt seed — to `candidate-sql/<release_id>/candidate_<unique_id>.<sql\|json>`; upload failure aborts the load); `PutObject` (one code-bundle contract document per release to `code-bundles/<release_id>/bundle.json`; upload failure aborts the load) |
| `k8s-controller` | write | `PutObject` (pod logs to `logs/` prefix) |
| `executor-controller` | read + write | Write, via the compile Job's `upload` container (`s3-sidecar`): `PutObject` the canonical manifest to `<service>/<release_id>/manifest.json`, plus — only when the compile leg's parse-export/rehearsal gate ran — the two exported partial-parse artifacts: the prod-context artifact to `<service>/parse-cache/<image_tag>/partial_parse.msgpack` (keyed by image tag, not release) and the candidate-context artifact to `<service>/<release_id>/partial_parse.candidate.msgpack` (a sibling of that release's manifest). Read, via the `hydrate-parse-cache` initContainer on production-run and seed-build Jobs (gated on `S3_BUCKET` being set): `GetObject` the context-appropriate partial-parse artifact into an `emptyDir` mounted over the team container's dbt target dir; a missing bucket/key, an unfetchable object, or a write failure all degrade (exit 0, team container still starts) rather than failing the Job. |
| `release-controller` | delete | `DeleteObjects` — prune-time delete of `candidate-sql/<release_id>/` and `code-bundles/<release_id>/` prefixes for each expired release, both soft-fail and both backstopped by a 30-day S3 lifecycle rule on the respective prefix |
| `ui-service` | read | `GetObject` — task-execution pod logs (proxied via `GET /api/task-executions/:id/logs`); dbt validation logs (proxied via `GET /api/releases/log`); corrected real-source SQL (`proposed_sql_uri` → `proposed-fix/<release>/<node>/attempt-<n>.source.sql`, fetched during Create PR to supply the file content to GitHub) |
| `agent-runner` | write (optional) | `PutObject` — conversation archive to `chat-archive/<user>/<thread>.json` before a thread is deleted by the retention job; enabled when `RETENTION_ARCHIVE_S3=true` |
| `remediation` | read | `GetObject` — dbt execution log per failing node (`logs/` prefix, URI supplied by `release.rejected:v1`); fetch failure on a missing key is treated as an empty log and classified `unknown:log_unavailable` |
| `remediation-agent` | read + write | `GetObject` — candidate SQL (validation only, from `candidate_artifact_uri` in the trigger) and dbt log (from `dbt_log_uri`, all classes); `PutObject` — for validation: candidate artifacts `proposed-fix/<release_id>/<node_id>/attempt-<attempt>.sql`/`.diff` (always written on a non-empty Step-1 result) plus real-source artifacts `attempt-<attempt>.source.sql`/`.source.diff` (written when Step 2 succeeds); for compile and seed_build: only the source artifacts `attempt-<attempt>.source.sql`/`.source.diff`, written on a proposed outcome |

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
| `remediation-agent` | Postgres `continuo_remediation_agent`: `proposal` (one row per attempt; unique on `(release_id, node_id, attempt)`; status: `proposed`, `skipped`, `failed`, `escalated`; source-location columns: `repo`, `commit_sha`, `file_path`; PR-tracking columns: `pr_url`, `pr_number`, `pr_state`, `pr_opened_at`, `pr_opened_by`), `remediation_agent_outbox`, `message_processing` |
