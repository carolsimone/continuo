# Changelog

All notable changes to the `continuo` Helm chart (`deploy/continuo/`) are
documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Chart `version` follows semver against the values contract — see the "Helm
chart versioning" section of the repository's `AGENTS.md` for the exact rule
and the process for updating this file.

This changelog starts at the point it was introduced. `v0.1.0-rc.1` and
`v0.1.0` predate it — see `git log v0.1.0 -- deploy/continuo` for what
shipped in those.

## [Unreleased]

## [0.15.0] - 2026-10-26

agent-chat and agent-remediation reach the outside world only through egress-proxy, on by default, and with `networkPolicy.enabled` a NetworkPolicy drops every connection that skips it. An install whose agents reach a host that is not on the proxy's allowlist, or a Postgres or Redis at a host name that resolves to a public address, needs `egress.allowedDomains`, `egress.privateDomains` or `egress.directDestinations` (or `egress.enabled=false`) before it upgrades; apart from the render failures listed under Breaking, an unmodified values file keeps working. **MINOR.**

### Breaking

- agent-chat and agent-remediation reach the outside world only through egress-proxy, on by default (`egress.enabled`). The chart sets `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY` on both agents and, with `networkPolicy.enabled`, adds egress NetworkPolicies. The agents may resolve names, reach every pod in the release namespace, an external Postgres or Redis on its configured port (on a private network, or as a pod of any namespace), the pods of another namespace on the port of each in-cluster address they are given there (`global.*GrpcAddr` or a URL in their `env`), and `egress.directDestinations`; egress-proxy reaches public addresses on its allowlisted ports, and private ones only on the ports of entries that allow them. `NO_PROXY` lists the release's own Services in every form and on every port, and every other in-cluster address the agents are given as `host:port`; an address in another namespace written as `<name>.<namespace>`, without `.svc`, goes through the proxy, which refuses it unless the allowlist names it. An install whose every destination is an IP address has an empty allowlist: no egress-proxy object and no proxy variable render, and the agents' NetworkPolicy still applies. Every IP address the agents are given (the host of a URL or `host:port` in their `env`, of a `global.*GrpcAddr`, of `llm.baseUrl`, `GITHUB_BASE_URL` or `s3.endpointUrl`) is reached directly on its port, because egress-proxy admits host names only, and so is an external Postgres or Redis whose `externalDatabase.host` or `externalRedis.host` is an IP address outside the private ranges. Affected: an install whose agents reach a host that is not on the allowlist (`kubectl get configmap <release>-egress-proxy`), a Postgres or Redis at a host name that resolves to a public address (`helm upgrade` names the configured host), or a service in another namespace reached through a `<name>.<namespace>` address (write `<name>.<namespace>.svc`). Before upgrading, add such a host to `egress.allowedDomains` (or `egress.privateDomains`, when it resolves to a private address) and such an address to `egress.directDestinations`, or set `egress.enabled=false` to keep open egress; `helm upgrade --reuse-values` from a chart without an `egress` block gets the lockdown too. With `networkPolicy.enabled=false` nothing stops a connection that skips the proxy, and `helm upgrade` prints a warning. egress-proxy logs each refusal (`kubectl -n <namespace> logs -l app.kubernetes.io/name=egress-proxy --tail=-1 | grep "egress denied"`). The render fails, naming the key, on: a `services` entry for either agent whose `env` or `secretEnv` sets `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` (in either case) or `S3_ENDPOINT_URL` while `egress.enabled` is true; a `GITHUB_BASE_URL` in the `secretEnv` of either agent (the allowlist reads agent-remediation's `env`); an `LLM_PROVIDER` in the `secretEnv` of either agent, or in its `env` with a value other than `__LLM_PROVIDER__` (the allowlist follows `llm.provider`); an `llm.baseUrl`, `GITHUB_BASE_URL` or `s3.endpointUrl` whose host is no DNS host name egress-proxy accepts (an underscore, as in `http://vllm_x:8000`) or whose port is outside 1-65535, where a DNS-valid host name or the IP address fixes it; an `egress.directDestinations` entry that is malformed or covers loopback, link-local, the unspecified network, a cloud metadata address, IPv4-mapped or NAT64 addresses of those, or every address (prefix length 0), so a whole `100.64.0.0/10` or `fc00::/7` fails and its narrower ranges are listed instead; an IP address the agents are given or an external datastore's IP address that lies in link-local or a cloud metadata range, is no valid address or has a port outside 1-65535; and, with `networkPolicy.enabled` and an external Postgres or Redis, an `externalDatabase.port` or `externalRedis.port` that is not a port from 1 to 65535. MINOR.

### Added

- `egress.directDestinations` (default `[]`) lists the addresses the agents reach without the proxy, as `{cidr, ports}` entries: a Postgres or Redis on a public address, for one. Each entry is checked as written (a CIDR with a prefix length of at least 1, ports as whole numbers from 1 to 65535; write them without a leading zero, because YAML reads an unquoted `0443` as the octal number 291 and the chart cannot detect it) and reaches `NO_PROXY` and the agents' NetworkPolicy. Every IP address the agents are given (the host of `llm.baseUrl`, `GITHUB_BASE_URL`, `s3.endpointUrl`, or of a URL or `host:port` in their `env` or a `global.*GrpcAddr`) and an external Postgres or Redis at a public IP address are added to it for you, on their ports. MINOR.

## [0.14.0] - 2026-10-24

A cancel is asynchronous: state moves a run to `cancelling` and its tasks in flight to `stopping`, execution-controller deletes each attempt's Job and reports it `stopped`, and the run then ends `cancelled`; the `schedule.cancelled:v1` stream is replaced by `run.cancelled:v1`. Runs in flight when this release deploys are not carried over: cancel them once it has deployed. The chart also deploys egress-proxy, a forward proxy for the two LLM agents' outbound traffic; the agents do not use it in this release. **MINOR.**

### Breaking

- A cancel is asynchronous. state moves the run to `cancelling` and its tasks in flight to `stopping`; execution-controller deletes each attempt's Job and reports the attempt `stopped` once its pods are gone; the run then ends `cancelled`. While a run is `cancelling` it holds its schedule: no cron fire and no manual trigger creates a second run, and a rerun or rebase of it is refused until it is `cancelled`. A cancel of a run with nothing in flight still ends it `cancelled` at once, and a second cancel of a `cancelling` run succeeds and changes nothing. A client that treats the answer of a cancel, or the `cancelled` status, as "the run's work has stopped" must wait for `cancelled`: `continuo schedule cancel` now reports `cancelling`, and the ui shows a `cancelling` run and its `stopping` tasks. A warehouse statement a pod sent may outlive the pod, so `cancelled` means no pod of the run can run again, not that the warehouse has stopped. MINOR.
- The `schedule.cancelled:v1` stream is replaced by `run.cancelled:v1` (consumer group `executor-run-cancelled`), which names the attempts state still waits on and is sent again for them 5 minutes after the cancel, then at gaps of 10, 20, 40 and 60 minutes, and every 60 minutes after that. dead-letter-controller deletes the retired stream, and its open dead letters are listed as not redrivable. execution-controller keeps a fence per cancelled run (`cancelled_runs`, 30 days): a dispatch of a fenced run creates no Job and is answered `stopped`, and the fence deletes the Jobs of the run it holds no record of. MINOR.
- Runs in flight when this release deploys are not carried over: cancel every pending or running run once it has deployed. state cancels by itself, when it starts, every active run it did not plan. The attempt status `cancelled` is `stopped`, and the migrations rename it in place: state V50 (`cancelling` runs, `stopping` tasks, `stopped` attempts, `task_attempts.stop_requested_at`) and execution V13 (`cancelled_schedules` becomes `cancelled_runs`; the `stopped` outcome). MINOR.

### Added

- egress-proxy, a new service: a forward proxy that admits a connection only to a host and port on its allowlist and resolves the name itself, refusing loopback, unspecified, link-local and cloud-metadata addresses and, unless the entry allows them, private ones. The chart deploys it (Deployment and Service `egress-proxy`, port 3128, two replicas) and renders the allowlist into the ConfigMap `<release>-egress-proxy` from `llm.baseUrl` (else the provider's public API), the `GITHUB_BASE_URL` of agent-remediation's `services` entry with `codeload.github.com` for archive downloads, `s3.endpointUrl` when `minio.enabled` is false, `egress.allowedDomains` and `egress.privateDomains`. A URL whose host is an IP address, is no valid host name (an underscore, no scheme) or has a port outside 1-65535 is left out of the allowlist, and when nothing is left (every URL is left out and no domain is listed) no egress-proxy object renders, because the proxy refuses to start on an empty list. `egress.enabled` (default true), `egress.proxy.replicas` (2), `egress.proxy.maxTunnels` (1024) and `egress.proxy.resources` are optional; a release whose values hold no `egress` block (`helm upgrade --reuse-values` from an older chart) runs the proxy with these defaults. Each `egress.allowedDomains` and `egress.privateDomains` entry is checked as written, and rendering fails naming the key and the entry on one that is not a host name with an optional port; a port is written without a leading zero. `metrics.port` may not be 3128 or 8093, and a `services` entry may not be named `egress-proxy`, while the proxy renders. The agents do not use the proxy yet.
- `continuo_runs_cancelling` and `continuo_runs_cancelling_overdue` (state, gauges): runs in `cancelling`, and those that have waited more than 15 minutes for their attempts to stop (state also logs the attempts it waits for). `continuo_run_cancel_seconds` (state, histogram): seconds from a run's cancel to its end as `cancelled`.
- `continuo_attempts_stopped_total` (execution-controller): attempts reported `stopped` because their run was cancelled. `continuo_fenced_jobs_deleted_total{reason}` (execution-controller): Jobs deleted because no row deploys them, with reason `fenced_run` (their run is fenced) or `ended_undeployed` (their row ended before it was deployed, for example after a failed create call).
- gRPC `SCHEDULER_STATUS_CANCELLING` and `TASK_STATUS_STOPPING`, and the attempt status `stopped`; the run page and `continuo schedule status` show them. No values change.

### Removed

- execution-controller's `CANCELLED_SCHEDULES_TTL_HOURS` and `CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES`: a run's fence is kept 30 days and swept hourly. The chart never set them; a `services[].env` override that still does is ignored.

## [0.13.0] - 2026-10-23

state decides every attempt of every task and execution-controller runs exactly one attempt per dispatch: a failed attempt is retried by its failure class, each task's Job is stopped at its timeout, and a run left with nothing in flight fails as `stalled`. `runs.taskMaxRetries`, `runs.taskTimeoutHours` and `execution.startTimeoutMinutes` are new with defaults, and an existing values file keeps working. **MINOR.**

### Added
- Domain teams declare a node's task timeout: `meta.timeout_minutes` on a dbt node (inherited from `dbt_project.yml`), `timeout_minutes` on a Python contract entry (only when both the CLI that builds the contract and the image base the service's nodes run in are continuo-python-runtime 0.10.0 or later; with an older image base every run of every node of that service fails when it loads its contracts). The value must be an integer from 1 to 10,080 minutes, or the release is rejected as `invalid_artifact` naming the node. It is outside the content hash, so a release that changes only a timeout promotes without validation. state stores it per release (`topology_nodes.timeout_seconds`, migration V46); the node page and `continuo node list` show the timeout a node's tasks get and whether it is declared or the install default (`timeout_seconds`, `timeout_declared`). Each task's Job is stopped at its timeout (see `runs.taskTimeoutHours`). MINOR.
- `runs.taskMaxRetries` (default 2, 0–10; `TASK_MAX_RETRIES` on the shared ConfigMap): how many retries a task gets after its first attempt, stamped on each task when its run is created. state retries a failed attempt at once while its task has retries left, unless the attempt failed by timing out or because its dispatch cannot run (failure classes `timeout` and `invalid`). A budget of 0 fails a task on its first failure. A value outside the range fails the render, and state refuses to start on one. MINOR.
- `runs.taskTimeoutHours` (default 24, 1–168; `TASK_TIMEOUT_SECONDS` on the shared ConfigMap): how long a task's container may run when its node declares no `timeout_minutes`, and how long a validation, seed-build or compile Job may run. Every such Job carries `activeDeadlineSeconds` = the start timeout + its timeout; Kubernetes stops it there and the attempt fails `timeout deadline_exceeded`, which is not retried. A value outside the range fails the render, and state and execution-controller refuse to start on one. MINOR.
- `continuo_runs_stalled_total` (state): runs state failed as `stalled`, which only a defect in state causes.
- `execution.startTimeoutMinutes` (default 15, 1–120; `JOB_START_TIMEOUT_SECONDS` on the shared ConfigMap): how long a Job's pod may take, after the Job is created, to start any container (init or main) — waiting for a node, pulling images, mounting Secrets. Past it execution-controller deletes the Job and fails the attempt as `start_timeout`; work a container does once it runs does not count. A value outside the range fails the render, and execution-controller refuses to start on one. MINOR.

### Changed
- execution-controller observes every Job, task and release leg alike, through a watch on its Jobs and pods instead of polling each one: a task is reported running when its main container starts and finished within about a second of its pod. A pod waiting on an image it cannot pull, a missing Secret or ConfigMap, or a container the runtime cannot create has its Job deleted, and its attempt is reported failed once the pod is gone, so a retry never runs beside it. A pod that has not stopped 15 minutes after its deletion raises `continuo_attempts_awaiting_pod_stop`. Each attempt the watch sees fail records why it failed (`deployments.facts`), and a task's retry Job is named `<prefix>-<first 8 characters of the run id>-r<attempt>` without ever cutting the run-id part; a retry whose node prefix must be cut ends it with a short hash of the full prefix, so two nodes of one run whose attempt-0 Job names differ keep different retry Job names. execution-controller refuses to start until the execution database migration has applied.
- state records every attempt of every task in `task_attempts` (migration V44): which Job ran it, when it was dispatched, queued, started and finished, its status, exit code, failure class and reason, log and run results. The `task_execution` table is dropped (migration V45), and the execution history of earlier runs is not carried over. state refuses to start until its migration has applied.
- The run page shows each task's attempts, for the latest run and for historical runs alike.
- `continuo node history` and state's gRPC rename `retry_count` to `attempt` (same field numbers) and carry `failure_class` and `failure_reason`. gRPC `ListTaskAttempts` replaces `ListTaskExecutions` and `GetTaskExecution`.
- state owns every attempt of every task: it numbers the attempt, names its Job, sends it on `task.dispatch:v1` with the attempt's task timeout and, when an attempt fails, decides in the same transaction whether to dispatch the next one by the failure's class. execution-controller runs exactly one attempt per dispatch, however often it is delivered, labels its Job with `attempt` and gives its pods `TASK_ATTEMPT`, and reports it on `task.status.updated:v2`: `queued` once accepted, `running` once its main container starts, and the final report, with its class and reason, once no pod of it can run again. It records each attempt's number and the last report it sent (`deployments.attempt`, `deployments.last_report`, execution migration V12). It never retries a task. A Job name another task's or attempt's Job holds fails the attempt `invalid job_name_collision`; a Job Kubernetes refuses on every try fails it `infra create_failed`, but only once a live read of the API finds no Job.
- Tasks have two more statuses, `dispatched` (sent, not yet accepted) and `queued` (accepted, waiting for a slot; state migration V47); a failure with retries left never shows the task `failed`. The ui shows both (queued titled "waiting for a slot"), `continuo schedule status` names them, and gRPC `TaskStatus` gains `TASK_STATUS_DISPATCHED` and `TASK_STATUS_QUEUED`.
- state asks execution-controller about an attempt it has heard nothing of, by sending its dispatch again with its known status: while unacknowledged, after 5 minutes, then at gaps of 10, 20, 40 and 60 minutes and every 60 after that; once acknowledged, after 30 minutes of silence and then every 30. A run with no attempt in flight and no progress for 5 minutes fails as `stalled`. A run that fails without a failed task records why (`stalled`, `empty_selection`, `invalid_node_type`; `scheduler_tracker.failure_reason`, state migration V47): the ui shows it beside the run's status, and gRPC `Scheduler.failure_reason` and `RunSummary.failure_reason` carry it. State migrations V48 and V49 restate the column comments of `task_attempts.next_check_at` and `scheduler_tracker.last_heartbeat_at` and change no schema.
- Runs in flight when this release deploys are not carried over: cancel every pending or running run once it has deployed. Their tasks were dispatched on `query.model:v1`, which nothing reads any more. MINOR.
- Node duration statistics count succeeded attempts only. No values change. MINOR.
- The default `validation.imageTag` moves from `v0.7.0` to `v0.10.0` (continuo-python-runtime 0.10.0), the first runtime that accepts `timeout_minutes` on a Python node. An install that leaves `validation.imageTag` unset pulls the new image on `helm upgrade`; an install that pins its own `imageTag` is unaffected, and no values key changes. A Python domain repository declares `timeout_minutes` only when both the CLI that builds its contract and the image base its nodes run in (`FROM …continuo-python-runtime-<engine>:v0.10.0` or later) are continuo-python-runtime 0.10.0 or later; with an older image base every run of every node of that service fails when it loads its contracts. MINOR.

### Removed
- orchestrator's dispatch watchdog, which cancelled a run that made no progress for 30 minutes, and its requirement for `STATE_GRPC_ADDR`: state fails a stalled run itself, and no healthy run waiting for a slot is cancelled any more. orchestrator calls no state RPC, so the NetworkPolicy on state's gRPC port (50051) admits agent-chat and ui only, and gRPC `ListStuckCandidates` is gone. `global.stateGrpcAddr` and `STATE_GRPC_ADDR` stay on the shared ConfigMap, which ui still reads. No values change. MINOR.
- The `query.model:v1`, `task.status.updated:v1` and `task.execution.recorded:v1` streams are retired: dead-letter-controller deletes them, and their open dead letters are listed as not redrivable. execution-controller no longer warns that it ignores `DEFAULT_TASK_MAX_RETRIES`.
- execution-controller's default `env` drops `K8S_CHECK_DELAY_SECONDS` and `K8S_FIRST_CHECK_DELAY_SECONDS`, which nothing reads any more; a values file that still sets them keeps working. The `check.k8s:v1` stream is retired: dead-letter-controller deletes it, and its open dead letters are listed as not redrivable.

### Fixed
- dead-letter-controller lists a dead letter as not redrivable, and says why in `not_redrivable_reason` (shown by `continuo dlq list` and `dlq show`), when its stream is retired or the streams contract does not name its stream or consumer group, and refuses to redrive it (gRPC `FAILED_PRECONDITION`, `continuo dlq redrive` exit 4). A redrive into a retired stream reported success and lost the message. No values change. PATCH.

### Security
- Service and CLI images are built with Go 1.27.3, and every Go module requires Go 1.26.10: both fix GO-2026-6809 in `crypto/x509` certificate verification (compromised intermediate certificates are distrusted). No values change. PATCH.

## [0.12.0] - 2026-10-16

`llm.baseUrl` is new, with an empty default that keeps each provider's public address; an unmodified values file upgrades unchanged, including with `--reuse-values`, unless it sets `LLM_BASE_URL` through `services[].env` or `services[].secretEnv` (see Breaking). **MINOR.**

### Breaking

- A `services` entry for agent-chat or agent-remediation whose `env` or `secretEnv` sets `LLM_BASE_URL` fails the render ("services[name=...].env sets LLM_BASE_URL" or "services[name=...].secretEnv sets LLM_BASE_URL", each followed by ", which the chart sets itself: set llm.baseUrl instead"). Before upgrading, move the value to `llm.baseUrl`, as the server root without `/v1`; this is the setting an install running `llm.provider: openai-compatible` used. MINOR.

### Added

- `llm.baseUrl` (default empty) points the agents at an LLM server: the root of an OpenAI-compatible server (`llm.provider: openai-compatible`, e.g. Mistral, OpenRouter, Groq, or a vLLM/Ollama you host), or a gateway in front of Anthropic or OpenAI. The chart sets it on agent-chat and agent-remediation as `LLM_BASE_URL`; a `services` entry whose `env` or `secretEnv` sets `LLM_BASE_URL` fails the render. Give the root without `/v1`: the render fails on a URL ending in `/v1`, on anything but an http(s) URL with a host, on a `user:password@`, `?query` or `#fragment` (put the key in `llm.apiKey`), and on `openai-compatible` without it. A release whose values hold no `llm.baseUrl` (`helm upgrade --reuse-values` from an older chart) renders with the default.

### Changed

- agent-chat reads `LLM_BASE_URL` for every provider, not only `openai-compatible` (agent-remediation already did). Both strip a trailing slash from it and refuse to start on one ending in `/v1`, or holding a `user:password@`, `?query` or `#fragment`. agent-chat's LLM client now honours `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY`. `llm.provider` accepts only `anthropic`, `openai` and `openai-compatible`; the schema rejects any other value.
- Every run records the release it runs on and that release's promotion seq: the live release for scheduled, triggered, rebased and single-node runs, the source run's for reruns and single-node runs pinned to a past run. Every task records its node's key in that release.
- The state service serves run graphs, run lists and active-run drift (`GetRunGraph`, `ListRuns`, `ListActiveRunDrifts`), read from each run's own release, and `ListNodeRuns` returns each run's content hash. The ui's run graph, drift badge and run list and the CLI's `node history` read them from the state service.
- Releases a run names are kept by topology retention, finished runs included.
- The state service plans every run itself, in the transaction that creates it, and starts its ready tasks. Recording a task's status starts the tasks a success makes ready and skips the pending tasks below a task that failed on its last attempt, so a run with a failure finishes `failed` as soon as nothing else can run instead of waiting for the dispatch watchdog. The orchestrator no longer takes part in running runs.
- A trigger that cannot make a valid run is refused and creates no run, instead of recording a `failed` or `skipped` run: a single-node run of a node the live release does not hold (gRPC `NOT_FOUND`, HTTP 404); a single-node `test` of a node without data tests, a trigger of a schedule with no node in the live release, and a rerun or rebase of a `test` run or of a run that names no release (`FAILED_PRECONDITION`, 409); a full refresh of a node type that cannot be rebuilt from scratch (`INVALID_ARGUMENT`, 400). A cron fire on a schedule with no node in the live release still records a `failed` run, and a whole-schedule `test` of a schedule without data tests still records a `skipped` one.
- **Rerun failed nodes → Latest snapshot** (a rebase) restarts the failed run from its failure on the live release: it re-runs the nodes that failed or were skipped, the live schedule's nodes the failed run never ran, every node below those and, with "Include changed upstream nodes" (checked by default in the ui), every node upstream of what it re-runs whose `content_hash`, node type, secret or upstream nodes changed since the release it was last built on, so a fix to the node that caused the failure is picked up, also when the failed run is itself a rebase that inherited that node from an earlier run. A new image tag alone is not a change. Before it starts, the dialog previews the selection: how many nodes run and why, and each changed upstream node with how many nodes it feeds and how long its last successful run took; unchecking the option re-runs only the failed part. The state service gains `PreviewRebase`, and `TriggerRebase` takes `include_changed_upstream` and `expected_release_id`: a rebase previewed on a release that is no longer live is refused (`FAILED_PRECONDITION`, "the live release changed; preview again"; HTTP 409 `release_moved`) and the dialog previews again. A caller that sends neither field gets the rebase without changed upstream nodes and no release check. No values change.
- The state Deployment rolls with strategy `Recreate`: its previous pod stops before the new one starts, because both would otherwise consume the task status stream and an old pod would record a success without dispatching the tasks it unblocks. state is unavailable for the seconds between the two; tasks keep running and their statuses wait on the stream. No values change.
- Upgrade: on its first start on this version, the state service cancels every pending or running run it did not plan (cancelled by `system`). Such runs were created by continuo 0.11.x or earlier and cannot be rerun: trigger their schedules again. To keep them, upgrade with no run active: turn maintenance on, wait until no run is pending or running, upgrade, then turn maintenance off. `helm upgrade` prints the same note. No values change.

### Removed

- The orchestrator's `ListRuns`, `GetRunGraph`, `ListActiveRunDrifts` and `GetNodeRunHistory` RPCs.
- The Redis streams `scheduler.started:v1`, `run.entries.dispatched:v1`, `run.entries.dispatch_failed:v1`, `trigger.rerun:v1`, `trigger.rebase:v1`, `trigger.single_node_run:v1`, `trigger.promoted_seeds:v1`, `node.updated:v1` and `run.finalized:v1`: nothing writes or reads them, and dead-letter-controller deletes their keys.
- The orchestrator's `cancelled_schedules` table (orchestrator migration V16) and its `ORCHESTRATOR_RECONCILER_INTERVAL_SECONDS`, `CANCELLED_SCHEDULES_TTL_HOURS` and `CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES` settings. The chart never set them; the orchestrator ignores them if a `services[].env` override still does.

## [0.11.0] - 2026-10-13

### Breaking
- release-controller's database drops the columns that held topologies inline in Postgres — `release_pipeline_runs.candidate_topology`, `release_pipeline_runs.parsing_at_upgrade` and `current_prod.topology_snapshot` — and release-controller drops the startup steps that moved their contents into object storage. An install must have run release-controller of continuo 0.10.0 at least once before it upgrades to this release. The release database migration refuses to run otherwise: while a run still holds an inline candidate topology, a run left parsing by that earlier upgrade is still parsing, or `current_prod` names a release with no topology object, it fails with "upgrade to continuo 0.10.0 and let release-controller start once before upgrading to this release" and drops nothing. From an older release, upgrade to 0.10.0 first, wait until release-controller is Ready, then upgrade to this release. Upgrade with no release in flight (maintenance on, the active release finished): a release-controller 0.10.0 pod still serving during the upgrade fails any promotion it handles once the column is dropped. release-controller refuses to start until that migration has applied, so with the bundled Postgres, where pods start before the `db-init-migrate` Job ends, it restarts until the Job completes; if the migration refused, release-controller keeps exiting with a message naming the Job and 0.10.0, and the previous release's data is untouched. No values change. MINOR.

## [0.10.0] - 2026-10-13

A new service, dead-letter-controller, stores every dead letter in its own database, `continuo_dead_letter`, and serves them to `continuo dlq list`, `show` and `redrive`. Stream consumers dead-letter the messages they cannot process and pause while a dependency refuses connections, every Go service bounds its handlers' run time and its Postgres pool, outbox relays publish as soon as the writing transaction commits (Postgres `LISTEN`/`NOTIFY`), keep the rows of one aggregate in creation order in every service and give every row one retry budget, `database.pool` sets the pool limits install-wide, orchestrator, release-controller and remediation honour `externalDatabase.sslMode`, every Go service serves Prometheus metrics at `/metrics` on `metrics.port`, and `helm upgrade` rolls services whose mounted ConfigMap changed. dead-letter-controller now trims every Redis stream every 5 minutes, storing the entries a lagging consumer group still needs as dead letters before it trims past the retention, and the `stream-reaper` CronJob and image are removed; `streamReaper.enabled` and `streamReaper.retention` keep their meaning and `streamReaper.schedule` is ignored. `maintenance.enabled` (new, default false) puts continuo in maintenance mode: no new work starts and work already running finishes. New values with safe defaults; an unmodified values file upgrades unchanged, including with `--reuse-values`. A values file that overrides the whole `services:` list does not get dead-letter-controller: add its entry from the chart's `values.yaml`, and `CONTINUO_DEAD_LETTER_ADDR` to agent-chat's `env`, or `continuo dlq` cannot reach it. Every Go service and topology-controller now log at `global.logLevel`, which must be `DEBUG`, `INFO`, `WARN`, `WARNING` or `ERROR` in any case: any other level fails the install and upgrade, and a service refuses to start on it, like `streamReaper.retention` above; `message_processing` dedup rows are pruned after 30 days in every service that writes them, and execution-controller and agent-remediation prune their processed outbox rows; the shared ConfigMap drops `STATE_SERVICE_GRPC_ADDR` and `DEAD_LETTER_CONTROLLER_GRPC_ADDR`, which no service read, and execution-controller's default `env` drops `IMAGE_TAG`. execution-controller admits Jobs atomically under `execution.maxConcurrentJobs` (new, default 50), counted across replicas. topology-controller writes each release's resolved topology once to object storage (`tenants/default/topologies/<release_id>/topology.json.gz`, in the existing bucket) and announces it on `manifest.loaded.candidate:v2`; release-controller reads it from there instead of storing it in Postgres, and an object-storage outage while a release is resolved now pauses that release until storage is back instead of rejecting it. release-controller numbers every promotion (`promotion_seq`) in the transaction that moves `current_prod` and announces it by reference on `release.promoted:v2`; orchestrator applies a promotion only when its number is higher than the live one, so a late or redriven older promotion no longer reverts the live topology or the schedule catalog. state keeps its own copy of every promoted topology and loads a dbt seed whenever the live release's content for it differs from the version last loaded, and `release.seeds.pending:v1` is retired. **MINOR.**

### Added
- `maintenance.enabled` (default false) puts continuo in maintenance mode: no new work starts and work already running finishes. While enabled, ui, the public release API (`POST /releases`) and the CLI (`continuo schedule trigger`/`build`/`test`, `continuo node trigger`/`build`/`test`/`full-refresh`, `continuo dlq redrive`) refuse new work with HTTP 503 / gRPC `UNAVAILABLE` and the message "continuo is in maintenance mode", and exit with code 7 (the `maintenance` CLI code). state skips cron fires (they are not caught up after maintenance is off). release-controller activates no queued release. agent-remediation records each fix attempt that comes due as skipped, and **Retry remediation** (in the UI) starts it once maintenance is off. Runs, releases and fix verifications already in progress complete. Changing it restarts every continuo service (the shared ConfigMap changes); turn it on with `helm upgrade <release> <chart> --reuse-values --set maintenance.enabled=true` and off again with `--set maintenance.enabled=false`. Five services (`state`, `release-controller`, `agent-remediation`, `dead-letter-controller`, `ui`) refuse to start unless `MAINTENANCE_ENABLED` is exactly `true` or `false`. The chart always sets it; a deployment that does not use the chart must set it.
- dead-letter-controller, a new service, stores every dead letter in its own database, `continuo_dead_letter`, created and migrated by the install like the others. It stores the messages stream consumers give up on (`consumer.dead_letter:v1`) and the outbox rows relays give up on (`outbox.dead_letter:v1`). It serves `DeadLetterService` on gRPC port 50055 (`global.deadLetterControllerGrpcAddr`, default `dead-letter-controller:50055`), which `continuo dlq list`, `continuo dlq show` and `continuo dlq redrive` use; agent-chat reaches it through `CONTINUO_DEAD_LETTER_ADDR`, which the chart sets from `global.deadLetterControllerGrpcAddr`. A redrive republishes the stored message to its original stream, only to the consumer group that failed it, within 30 days of the original message; older dead letters are deleted. Its metrics add `continuo_dead_letters_total`, `continuo_dead_letters_redriven_total`, `continuo_dead_letters_expired_total`, `continuo_dead_letter_backlog` and `continuo_dead_letter_oldest_open_age_seconds`.
- Outbox dead letters carry the fields the failed row would have published (`original_fields`), its creation time and its outbox table, so they can be redriven.
- `database.pool.maxOpenConns` and `database.pool.maxIdleConns` (optional; default 0 keeps each service's own limit) bound every Go service's Postgres pool, published as `DB_MAX_OPEN_CONNS` / `DB_MAX_IDLE_CONNS` on the shared ConfigMap. Each service now has an explicit limit (open/idle): state 20/10, orchestrator 35/5, execution-controller 30/5, release-controller 15/5, remediation 10/5, agent-remediation 10/5, agent-chat 20/5, dead-letter-controller 12/5. Set the two together: the render fails when `maxIdleConns` exceeds `maxOpenConns` or is set without `maxOpenConns`. A service refuses to start on a limit that is not a whole number of at least 1, on an idle limit above its open limit, or on an idle limit above its own default open limit when no open limit is set.
- Every Go service serves Prometheus metrics at `/metrics` on a listener of its own, on `metrics.port` (default 9464, `METRICS_PORT` on the shared ConfigMap). They cover consumer handler durations by result, dead letters, infrastructure pauses, consumer lag and pending entries per group, outbox rows published and failed, outbox backlog, the age of the oldest open row, dead-lettered rows, Postgres pool statistics, and Go runtime and process metrics. Lag and backlog are read when scraped. `metrics.podAnnotations` (default true) adds `prometheus.io/*` pod annotations. `metrics.scrapeNamespaceSelector` (default empty) adds a NetworkPolicy that admits the selected namespaces to the metrics port only. A service refuses to start when it cannot bind the port, and rendering fails when `metrics.port` equals a service's HTTP or gRPC port.
- `execution.maxConcurrentJobs` (default 50, `MAX_CONCURRENT_JOBS` on the shared ConfigMap) caps the Jobs execution-controller holds at once: every task, whole-DAG test, validation, seed-build and compile Job takes a slot from admission until its outcome is recorded; per-release schema-operation Jobs take none. The cap holds across replicas and during a rolling update. Rendering fails on a value below 1, and execution-controller refuses to start on one.

### Changed
- Promotions travel by reference on `release.promoted:v2` (topology URI, SHA-256 and `promotion_seq`); `release.promoted:v1` is retired and dead-letter-controller deletes its key. orchestrator and state ignore a promotion older than the live one. The ui's drift badge counts promotions. On its first start after the upgrade release-controller writes the topology object of the current prod release and re-announces it; orchestrator's `topology_state` table is dropped. The release-controller image ships `announce-topology`, an operator command that publishes a topology file (or re-announces the current prod release) under a fresh `promotion_seq` without changing `current_prod`; the benchmark harness and the e2e suite use it.
- orchestrator's `OrchestratorQuery` gRPC fields that carry a promotion seq are named for it: `GetScheduleGraphResponse.promotion_seq`, `GetRunGraphResponse.run_promotion_seq` and `latest_promotion_seq`, `ListActiveRunDriftsResponse.latest_promotion_seq` and `ActiveRunDrift.run_promotion_seq`. Their field numbers are unchanged, so the wire format is the same and clients built from either name read the same value. The ui's `/api/schedules/:name/graph` and `/api/runs/:run_id/graph` responses use the same names, the drift badge reads "source N releases behind latest" ("source 1 release behind latest" for one) and the latest-topology chip reads "promotion N". A browser tab left open across the upgrade shows no drift badge or promotion chip until it is reloaded. No values change.
- With an external bucket (`s3.*`), orchestrator and state also need read access to the `tenants/` prefix.
- state reads each promoted release's topology artifact from object storage. The chart now gives state the S3 credentials and admits it to the bundled MinIO; state refuses to start without `S3_ENDPOINT_URL`, `S3_BUCKET` and `AWS_DEFAULT_REGION`, which the chart's shared ConfigMap sets. No values key changes.
- state stores every promoted release's topology (its nodes and the edges between them) in its own database, once per release, and keeps its own live release, moved only by a higher `promotion_seq`; a new migration (state V40) adds `topology_releases`, `topology_nodes`, `topology_edges`, `live_release`, `node_materializations` and `scheduler_tracker.release_id`. state now decides seed loads itself: a dbt seed of the live release is loaded when its `content_hash` differs from the version state last recorded as loaded into the warehouse, in one `promote-seed-<id>` run per check, right after each promotion and every 60 seconds. A seed already at the live content is not reloaded (a new image alone reloads nothing), at most one load of a seed runs at a time, and a load that failed is not retried until the seed's content changes or an operator reruns the run. While maintenance is on, no seed load starts. The version a seed holds is recorded when a promote-seed run, or a rerun of one, succeeds. On the first promotion state applies after the upgrade, the seeds that promotion did not change are recorded as already loaded; state's new consumer group then reads the promotions the stream still holds, oldest first, so a seed changed among them may be loaded once more than needed. state deletes a stored topology once it is older than `RETENTION_DAYS`, unless it is live or a pending or running run builds it. orchestrator dispatches a seed run only once its own topology has reached the promotion the run was planned against, waiting up to 30 seconds and then retrying. `release.seeds.pending:v1` is retired; dead-letter-controller deletes its key.
- The resolved candidate topology of each release is an immutable gzipped JSON object under `tenants/default/topologies/<release_id>/` in the configured bucket, written by topology-controller and announced with its SHA-256 on `manifest.loaded.candidate:v2` (`manifest.loaded.candidate:v1` is retired; dead-letter-controller deletes its key). release-controller verifies the checksum on every read; a missing or corrupt object rejects the release with `internal_error`. An object-storage connection failure or 5xx no longer rejects a release: topology-controller and release-controller pause and retry until storage answers. If you set `s3.*` for an external bucket, topology-controller needs write access and release-controller read, write and delete access to the `tenants/` prefix; a bucket lifecycle rule must not expire objects under `tenants/`.
- Upgrade: drain releases first. Turn maintenance on (`--set maintenance.enabled=true`), wait until no release is being compiled, parsed or validated, upgrade, then turn maintenance off. On its first start release-controller writes the topology object of every stored release that does not have one yet; a release that was being parsed during the upgrade is rejected with the new reason `upgrade_interrupted` and must be submitted again.
- execution-controller counts the Jobs it holds from its own records instead of listing every Job on each tick, and a Job counts from admission, including while its pod is still being scheduled, so the same cap admits fewer Jobs at once when pods start slowly. Queued work starts as soon as a slot frees or new work arrives (Postgres `LISTEN`/`NOTIFY`), with the 5 s poll as a fallback; the listener is one more Postgres connection per execution-controller pod. A new migration (execution V9) adds the admission record and the deployment states. Jobs running when you upgrade finish normally but are not counted against the cap until they end.
- execution-controller's default `env` drops `MAX_CONCURRENT_JOBS`; the value comes from `execution.maxConcurrentJobs`. A values file that sets `MAX_CONCURRENT_JOBS` in execution-controller's `env` keeps working, and that entry wins over the ConfigMap.
- Outbox relays publish as soon as the writing transaction commits: each outbox table carries an insert trigger that notifies its relay. The relays poll less often, every 5 s instead of every 500 ms (state) or every second (the others), as a fallback for retries coming due and for notifications lost while a relay reconnects. A new migration in each of the six services with an outbox replaces the relay's claim indexes and adds the trigger.
- Every outbox publishes the rows of one aggregate in creation order (only execution-controller's did), whole batches at a time. state and orchestrator send a whole batch in one Redis pipeline, so there a row sent in the same pipeline as an older row of its aggregate that failed can reach its stream before that row's retry.
- Every outbox row gets the same publish budget: 13 attempts over about 24 minutes, then `outbox.dead_letter:v1`. state's rows had 3 or 5.
- Each outbox relay (state, orchestrator, execution-controller, release-controller, remediation, agent-remediation, dead-letter-controller) holds one extra Postgres connection per pod for `LISTEN`, outside the pool limits above, so a service pod with a relay uses its open limit plus one. The notification needs a session-mode connection: through a transaction-mode pooler such as PgBouncer in transaction mode `LISTEN` does not work and relays publish on the 5 s poll instead.
- Every Go service refuses to start when `POSTGRES_PORT` is set to anything but a port number from 1 to 65535, instead of connecting to 5432.
- Every Go service closes a Postgres connection 30 minutes after it was opened and an idle connection after 5 minutes. State recycled connections after 2 minutes.
- Stream consumers no longer drop messages they cannot process. A message whose handler reports a permanent error, or still fails on its fifth delivery, is first written to the new `consumer.dead_letter:v1` stream (with its original stream, group, id, fields, error and delivery count) and acknowledged only once that write succeeded. This covers every Go service and topology-controller; release-controller, remediation and agent-remediation dead-letter malformed payloads they used to discard.
- A dependency that refuses or drops connections (connection refused or reset, DNS failure, host unreachable, a Postgres connection error, an S3 5xx, Neo4j unavailable) pauses the consumer without counting toward a message's delivery limit: the consumer retries the same message after 1 s, doubling to 60 s, and resumes when the dependency answers. An outage that makes a handler hang until its deadline counts as a failed delivery.
- Every Go service's message handlers have a time limit; topology-controller has none. The limit is 30 s by default (state's consumers other than `release.promoted:v2`, and orchestrator's other consumers); orchestrator's whole-DAG and whole-release consumers and state's `release.promoted:v2` consumer allow 5 minutes; execution-controller allows 60 s, and 6 minutes for its schema-operation consumers; release-controller and remediation allow 60 s; agent-remediation allows 5 minutes. A message that runs out its limit on every delivery is dead-lettered after its fifth. A consumer keeps the messages it is working on from being taken by another replica; a pending message nobody is working on (one a crashed, stopped or stuck consumer left, or one waiting for a retry after a failure) is taken by a replica's sweep once it has been idle for the handler's limit plus one minute (1.5 minutes for the 30 s consumers).
- dead-letter-controller trims every stream every 5 minutes, up to the oldest entry any of the stream's consumer groups (as declared by the release) still needs. Entries older than `streamReaper.retention` (default 72h) that a lagging group still needs are first stored as dead letters of kind `trimmed`, listed by `continuo dlq list quarantine` and redrivable for 30 days, and only then trimmed. A group whose service has not started yet keeps every entry newer than the retention. `streamReaper.enabled` and `streamReaper.retention` keep their meaning and reach dead-letter-controller as `STREAM_TRIM_ENABLED` and `STREAM_RETENTION`; dead-letter-controller refuses to start with a retention outside (0, 720h). `streamReaper.schedule` is ignored. New metrics: `continuo_stream_trimmed_entries_total`, `continuo_stream_quarantined_total`, `continuo_stream_length`, `continuo_redis_used_memory_bytes`, `continuo_redis_maxmemory_bytes`, `continuo_stream_trim_last_success_timestamp_seconds`, and consumer lag and pending entries for every consumer group.
- Publishers no longer cap streams at about 10,000 entries; trimming alone bounds them.
- Streams the release no longer uses (14 names, from `compile.node.completed:v1` to `validation.node.completed:v1`) are deleted from Redis.
- Stream consumers acknowledge without handling an entry that dead-letter-controller redrove for another consumer group, and an entry that was trimmed from the stream while it was pending.
- `message_processing` dedup rows are pruned in state, orchestrator, execution-controller and agent-remediation once they are older than 30 days (longer when the service's `RETENTION_DAYS` is longer), whatever their state; state's, execution-controller's and agent-remediation's rows were never pruned. execution-controller and agent-remediation also prune processed outbox rows after `RETENTION_DAYS` (default 7), like state and orchestrator. A new execution migration makes `deployments.message_processing_id` `ON DELETE SET NULL` and indexes it, so pruning clears that provenance instead of being refused. release-controller and remediation write no dedup rows.
- Every Go service and topology-controller log at `global.logLevel` (`LOG_LEVEL` on the shared ConfigMap); they logged at INFO whatever it said. The accepted levels are `DEBUG`, `INFO`, `WARN`, `WARNING` and `ERROR`, in any case. Any other value now fails `helm install` and `helm upgrade` with a message naming the accepted levels, and a service started outside the chart with another value refuses to start. ui's server log has no level and is unaffected.
- remediation reads at most 64 MiB of a failing node's log, and agent-remediation at most 64 MiB of an evidence object (candidate SQL, a dbt or runner log, a run result, a proposal's content or diff). A larger object is treated as missing: remediation classifies the failure `unknown:log_unavailable`, and agent-remediation works without that evidence. Both read the whole object before.
- Values-contract semver impact: MINOR; existing supported `global.logLevel` overrides continue to work unchanged. Chart versions are stamped by the release workflow.

### Removed
- The `stream-reaper` CronJob and its image. An install that mirrors images no longer needs `stream-reaper`.
- `DB_POOL_SIZE` and `DB_MAX_OVERFLOW` from state's default `env`. State's own defaults equal the values they set (20 open, 10 idle). An overridden `services` list that still sets them keeps working: state ignores them and logs a warning at startup naming `DB_MAX_OPEN_CONNS` / `DB_MAX_IDLE_CONNS`, the keys it reads; use those instead.
- `STATE_SERVICE_GRPC_ADDR` and `DEAD_LETTER_CONTROLLER_GRPC_ADDR` from the shared ConfigMap. No service read them: `global.stateGrpcAddr` reaches orchestrator and ui as `STATE_GRPC_ADDR`, and `global.deadLetterControllerGrpcAddr` reaches agent-chat as `CONTINUO_DEAD_LETTER_ADDR`; both values keys are unchanged.
- `IMAGE_TAG` from execution-controller's default `env`. execution-controller does not read it: each team dbt Job runs the image tag its dispatch carries. A values file that overrides the whole `services:` list and still sets it keeps working; the variable is ignored.

### Fixed
- orchestrator, release-controller and remediation honour `externalDatabase.sslMode` (`DB_SSLMODE`); they always connected with `sslmode=disable`.
- `helm upgrade` rolls state, execution-controller and agent-remediation when the chart-managed ConfigMap each one mounts changes: the schedules (`files/schedules.yaml`), the dbt commands (`files/dbt-commands.yaml`) and the service repos (`serviceRepos`, or `files/service_repos.yaml` when that is empty). Each service reads that file only at startup, and its pod template carried no digest of it, so an upgrade that changed only the file updated the ConfigMap and left the service on the previous crons, dbt commands or repo map with no error. Their pod templates now carry `checksum/schedules`, `checksum/dbt-commands` and `checksum/service-repos`, so each rolls once on the upgrade to this release. A volume that names an operator-owned ConfigMap (`volumes[].configMap.name` set) carries no digest; after editing such a ConfigMap, run `kubectl rollout restart` on the service. The ui needs neither: it re-reads `cancel-config.json` on every request.
- agent-chat, agent-remediation, execution-controller and remediation use the AWS SDK's default credential chain (an IAM role, workload identity, environment variables or shared config) when the S3 access keys are empty (`s3.accessKeyId` and `s3.secretKey` unset, no `s3.existingSecret`), as orchestrator and release-controller do. They signed every object-storage request with the empty keys, so on an install that authenticates through a role their reads and writes failed: execution-controller dropped every task log and run result, remediation could not read failure logs, agent-remediation could not read evidence or code bundles or store proposal artifacts, and agent-chat's retention archive failed. Every service that uses object storage now refuses to start when the default chain cannot be loaded, for example when `AWS_PROFILE` names a profile the shared config does not define.

## [0.9.1] - 2026-10-02

Template and service startup fixes; no values change. An unmodified values file upgrades unchanged. **PATCH.**

### Fixed
- With the bundled Redis (`redis.enabled=true`), every service that connects to Redis — state, orchestrator, agent-chat, execution-controller, release-controller, remediation, agent-remediation, ui and topology-controller — waits for it in a `wait-for-redis` init container. Only ui and topology-controller waited before, so on a fresh install the Go services could start before Redis was resolvable and crash-loop. BYO installs render no Redis gate.
- The seven Go services wait for Redis at startup in code too (capped backoff, 5 minutes in total), so BYO installs and docker-compose ride out a Redis that is still starting instead of exiting on the first refused connection; a wrong Redis password still fails at once. Each serves its liveness endpoint while it waits and holds readiness at 503 until startup completes.
- `wait-for-migrations` checks for the service's database through the `postgres` maintenance database and for `flyway_schema_history` with `to_regclass`, so waiting on a database or table that does not exist yet no longer logs a `FATAL`/`ERROR` on Postgres every few seconds.
- The MinIO bucket-init Job prints one `waiting for minio (N)...` line per attempt instead of an `mc` error, gives up after 10 minutes, and then prints `mc`'s last error.

## [0.9.0] - 2026-10-02

CD pipelines release through continuo's public URL with their GitHub Actions OIDC token, and release-controller is reached only inside the cluster. New values with safe defaults; an unmodified values file upgrades unchanged, including with `--reuse-values`. **MINOR.**

### Added
- `ciAuth` values (optional, as is each key): CD pipelines release through `POST <publicUrl>/api/v1/releases` with their GitHub Actions OIDC token. `ciAuth.bindings` maps each service to the GitHub `repositoryId` (plus optional `ref`, `refProtected`, `environment`, `workflowRef`, `allowBootstrap`) allowed to release it; a token can release, and read releases of, only its bound services. Empty bindings (the default) grant no CI access: the ui does not trust the CI issuer at all. An install upgraded with `helm upgrade --reuse-values` (no `ciAuth` in its stored values) renders the defaults — the GitHub Actions issuer, the origin of `auth.publicUrl` as audience, no bindings. The ui's CI-auth file, its mount and `CI_AUTH_CONFIG_PATH` are injected by the Deployment template, so an overridden `services` list keeps them.
- The ui serves `GET /api/v1/releases/{id}` and `GET /api/v1/current-prod` for pipelines polling a release, limited to 300 reads a minute per principal.
- Bundled Dex enables the password grant and issues ID tokens valid for 1 hour, so an operator can obtain an API bearer token with one `curl`.

### Changed
- release-controller's internal `POST /releases` answers 409 when the release id already exists with a different service, image tag, kind, bootstrap flag, repo or commit sha, instead of silently keeping the first submission; resubmitting identical content still answers 202. Two simultaneous first submissions of the same id can no longer both be accepted. Only invalid input answers 400; a storage failure answers 500, which the public API reports as 503 `upstream_unavailable`.
- The bundled Dex config change (password grant, 1-hour ID tokens) restarts the Dex pod on upgrade.

### Fixed
- release-controller and remediation receive their HTTP port under the variable they read (`RELEASE_CONTROLLER_HTTP_PORT`, `REMEDIATION_HTTP_PORT`); agent-remediation no longer receives an unused `HTTP_PORT`.

### Security
- Dependency updates: `undici` 7.30.0 and `ip-address` 10.7.2 in the ui, `urllib3` 2.8.0 in topology-controller and the dbt images.

## [0.8.1] - 2026-10-01

### Fixed
- `orchestrator` waits for Neo4j at startup (capped backoff, 5 minutes in total) instead of exiting on the first connection error and relying on a pod restart. On a fresh bundled install it no longer shows one or two restarts while Neo4j's Service name starts resolving. Its health endpoint now answers while it waits, with `/ready` returning 503 until startup completes.
- The state service lists node services in byte order, independent of the database's locale, so the order no longer varies between installs.

### Changed
- The state database migration drops the unused `task_tracker.manifest_version` column that 0.8.0 kept for rollout safety. **Upgrade through 0.8.0 first.** An install going straight from a release before 0.8.0 to this one runs the drop while the old state pods still read and write the column, and they fail until replaced. Installs already on 0.8.0 are unaffected.

## [0.8.0] - 2026-10-01

### Added
- `dbt-commands.yaml` accepts two optional keys per block, both backing the new single-node full-refresh operation: `full_refresh` (rebuilds one dbt model from scratch) and `seed_full_refresh` (rebuilds one dbt seed from scratch). A block without one of them keeps loading; full refresh of that node kind is then unavailable for the services it covers. Production, scheduled and promoted-release seed loads are unaffected — they remain a normal, non-destructive `dbt seed`.
- `python-api` node kind: a python script node with no declared reads. Its contract may name one Secret with `secret_ref` (must start with `continuo-api-`); execution-controller attaches it to the node's pod with `envFrom`, before the warehouse Secret. Create the Secret in the namespace execution-controller runs Jobs in (`K8S_NAMESPACE`, the release namespace by default) with kubectl, External Secrets Operator or Vault Secrets Operator (see docs/run-projects-in-continuo.md).

### Changed
- `manifest_version` is gone from node run history. `continuo node history` (and the chat tool built on it) no longer returns it, and the node page's run history drops its Manifest column; `image_tag` identifies the code a run executed. The field was always empty for release-sourced runs. The state database keeps the now-unused `task_tracker.manifest_version` column (empty, defaulted) so the previous release's pods keep working while `helm upgrade` migrates; a later release drops it.
- Default `validation.imageTag` is `v0.7.0` (continuo-python-runtime 0.7.0).
- The python script node kind is `python-node`. Contracts written by continuo-python-runtime < 0.6.0 (kind `python-model`) keep working; stored node types are rewritten on upgrade by the release DB migration and orchestrator's startup data migration. Domain repos that upgrade to continuo-python-runtime 0.6.0 see every python node re-validated once on their next release (the node kind is part of its content hash).

### Breaking
- The chart refuses a release name / `fullnameOverride` / Secret name inside the reserved `continuo-api-` prefix, which is kept for the operator-created Secrets a `python-api` contract may name. Affected: an install whose release name or `fullnameOverride` is `continuo-api` or starts with `continuo-api-`, or whose `*.existingSecret`, `validation.warehouseSecret` or `ingress.tls.secretName` value starts with `continuo-api-`; `helm upgrade` fails to render for it. Before upgrading, rename any such Secret and point its values key at the new name; for a release name or `fullnameOverride` inside the prefix, move to a release name or `fullnameOverride` outside it (either renames every chart object, including the bundled datastores' volumes, so migrate their data first). Installs outside the prefix are unaffected. MAJOR.

## [0.7.1] - 2026-09-24

### Fixed
- Bundled MinIO quickstart images are now served from the chart's own
  `ghcr.io/carolsimone/continuo-minio` and `continuo-mc` repositories, mirrored
  from an anonymously-pullable MinIO build. Upstream MinIO gated its `quay.io`
  images behind authentication, which broke `helm install` for the bundled
  object store (`ImagePullBackOff`). The MinIO StatefulSet now runs the `minio`
  binary directly (`command: ["minio"]`) so it behaves like the upstream image
  regardless of the packaging's own entrypoint — same `server /data`, uid 1000,
  read-only root filesystem, S3 API, and `continuo` bucket. No registry
  credentials are required to install. Existing overrides are unaffected,
  including a custom `minio.image` (any MinIO image ships the `minio` binary);
  `minio.enabled=false` installs never used these images. PATCH.

## [0.7.0] - 2026-09-24

Operators can map their own service names for agent-remediation without forking
the chart, and the shipped map is labelled demo-only. New value with a safe
default; an unmodified existing values file upgrades unchanged. MINOR.

### Added
- `serviceRepos` (default `{}`): a `service_name → project-root` map for
  agent-remediation source resolution. When non-empty it **replaces** the shipped
  demo map (`files/service_repos.yaml`) entirely, so a production install maps
  only the services it runs and a demo entry can never mis-own a path;
  empty/unset keeps the demo map. Explicit empty-string values (a dbt project at
  the repository root) are preserved.

### Changed
- `files/service_repos.yaml` now also maps the `continuo-core-finance-demo`
  services (`continuo-core`, `continuo-finance`), and is labelled EXAMPLE DATA —
  NOT FOR PRODUCTION.

## [0.6.2] - 2026-09-17

Fast tasks are now observed `running`, and the Run tab graph reads status
at a glance. Adds one execution-controller env value with a safe default;
an unmodified existing values file upgrades unchanged. MINOR.

### Added
- `executionController.env.K8S_FIRST_CHECK_DELAY_SECONDS` (default `"1"`):
  how long after a Kubernetes Job is created its first status check runs.
  The first check is what marks a task `running`, so with only the 10s
  `K8S_CHECK_DELAY_SECONDS` re-check cadence a Job finishing in a few seconds
  went straight from `pending` to `succeeded` and never showed as running in
  the UI.

### Changed
- UI: dependency graphs on the Run tab (List order, Graph swimlane) and the
  Topology tab flow in execution order — seeds in the leftmost column,
  arrows pointing from a table to the tables that read it, and a selected
  node's highlighted upstreams are the tables it depends on. The UI server
  reverses the orchestrator's DEPENDS_ON edges once at the API boundary.
- UI: the Run tab graph colours each node by its status (tinted fill and
  border, dashed while pending, struck through when skipped) instead of by
  service, adds a status legend, and sizes the lane-label column to the
  longest service name so labels no longer sit under the first node column.

## [0.6.1] - 2026-09-16

Pre-go-live polish of the operator dashboard. Ships an updated `ui` image
that removes cosmetic rough edges from the Runs and Nodes tabs and the
schedule detail page; no values-contract change, so an unmodified existing
values file upgrades unchanged. PATCH.

### Fixed
- UI: a run predating topology tracking no longer renders a grey "topology
  version unknown" strip on the Runs dashboard cards or the schedule/node
  detail headers. That strip read like an error on the home page. Only a run
  pinned to an older topology than the latest still shows a strip — the amber
  "source N gen behind latest" warning, which remains a real drift signal.
- UI: the schedule detail page no longer stretches its Run list and Past runs
  across the whole monitor. The run-mode tabs and content sit in a centered,
  width-capped column, and a past-run row shows its status pill next to the
  timestamp instead of at the far edge, so neither surface reads as a sea of
  empty space on a wide screen.

### Removed
- UI: the `Flaky` column on the node catalog and the "% flaky" stat on the
  node detail header, dropped as a redundant metric. The state gRPC contract
  is unchanged (`NodeSummary.flaky_rate_pct` still returned); the UI simply
  stops surfacing it.

## [0.6.0] - 2026-09-15

### Breaking
- `executor-controller` and `k8s-controller` are replaced by one service,
  `execution-controller` (port 8084), with one database `continuo_execution`
  and one outbox. The migrate job creates and migrates `continuo_execution`
  and no longer touches `continuo_executor` or `continuo_k8s`; those are left
  in place for the operator to drop. Nothing in flight is carried over, so
  upgrade only when nothing is in flight: stop the old services and let every
  run, release and queued retry finish first. A values
  file that overrides either old `services[]` entry must replace both with one
  `execution-controller` entry. RBAC is the union of the two old Roles. The
  `k8s-controller` allow on the state gRPC NetworkPolicy is gone (never used).
  MAJOR.

### Removed
- `RESOLVER_CHECK_INTERVAL_SECONDS`, `RESOLVER_STUCK_THRESHOLD_SECONDS`,
  `RESOLVER_BATCH_SIZE`, `RESOLVER_MAX_ATTEMPTS`: the stuck-entry resolver is
  gone. Its condition (`pending` with `retry_count >= max_retries`) cannot
  occur because the outbox processor marks such rows failed and dead-letters
  them.

### Fixed
- Pull the bundled MinIO server and client images from Quay
  (`quay.io/minio/minio`, `quay.io/minio/mc`) instead of Docker Hub, which no
  longer serves the `minio/minio` / `minio/mc` community images. Same image
  releases; only the registry changes, so an existing values file that does
  not pin `minio.image.repository` / `minio.mcImage.repository` keeps working.

## [0.5.0] - 2026-09-09

### Added
- `global.agentRemediationGrpcAddr` (default `agent-remediation:50054`) —
  release-controller reads remediation attempts before starting a retry
  round. New key with a safe default; an unmodified existing values file
  keeps working unmodified. MINOR.
- Validation bind-checks every dbt test of a changed model (`check_binds`): a
  test whose compiled SQL names a column or relation the release removed
  rejects the release. Tests are validation-only nodes; they are not
  promoted, scheduled, or drawn.

### Changed
- `validation.imageTag` default is `v0.5.0` (continuo-python-runtime with the
  `check_binds` op). An existing install's first release per service after
  upgrading bind-checks all of that service's tests once.
- The chart refuses a `validation.imageTag` below `v0.5.0`: dbt-test bind
  checks (`check_binds`) require it, and a v0.4.x runner would report success
  without running the `check_binds` nodes topology-controller emits, promoting
  a changed test's bind unvalidated. Only explicit v0.4.x pins are affected —
  a default install (default is `v0.5.0`) renders unchanged; re-pin to
  `v0.5.0`+ or drop the override to track the chart default. PATCH.
- The shipped `files/service_repos.yaml` (the `-service-repos` ConfigMap
  agent-remediation reads) now maps the `service-py` python service to
  `services/service-py`. Without it a python node's proposed contract fix
  belongs to no configured service and the attempt is recorded failed before
  verification. Installs that name their own ConfigMap in
  `services.agent-remediation.volumes` are unaffected. PATCH.
- During a rolling upgrade, the old `remediation` binary logs `ON CONFLICT`
  constraint errors for a few seconds after migration `V4` applies the wider
  `classification_decision` unique key; affected messages stay pending on
  their stream and are processed once the new pod is up. No values or
  template change.
- Fix-verification runs are no longer releases: they leave `GET /releases` and the
  Releases tab, get their own `GET /verification-runs/{id}` and UI page
  `/verifications/:id`, and end in `passed`/`failed`. Existing rows are
  converted in place by the release-controller migration. No values change.

### Breaking
- `agentRemediation.env.SHADOW_VERIFY_TIMEOUT` and
  `agentRemediation.env.SHADOW_VERIFY_POLL_INTERVAL` are renamed
  `VERIFICATION_TIMEOUT` and `VERIFICATION_POLL_INTERVAL` (same defaults,
  `20m` and `15s`). An override of the old keys is ignored after upgrade. MAJOR.

### Fixed
- `templates/networkpolicy.yaml`: added the two remediation reachability edges
  the default-deny policy was missing — `agent-remediation → release-controller`
  on 8088 (the verification lane: submitting/polling verification runs and
  reading the failing release's image tags) and `release-controller →
  agent-remediation` on 50054 (`ListProposals` before a retry round). Without
  them, on a cluster whose CNI enforces NetworkPolicy every proposed fix stalled
  before verification and every "try again" failed. No values key change; PATCH.

## [0.4.1] - 2026-08-25

### Changed
- Comment-only: `files/dbt-commands.yaml` now names the demo repository by its
  new name, `continuo-demo` (renamed from `continuo-dbt-demo`). No rendered
  template, values key, or behavior changes; PATCH when released.
- `validation.imageTag` default bumped `v0.4.0` → `v0.4.1`. The
  `continuo-python-runtime-<engine>` image now installs its runtime and engine
  adapter from PyPI (versioned) instead of building from source, and the
  adapter packages were renamed to `continuo-<engine>-adapter`. The image name,
  entrypoint, `run`/`validation-op` commands, uid `65532`, and adapter
  discovery are unchanged, so it is a drop-in; an unmodified existing values
  file that keeps `imageTag: "v0.4.0"` still installs and runs. PATCH when
  released.

## [0.4.0] - 2026-08-23

### Added
- `networkPolicy.enabled` now also renders an `allow-acme-solver` policy when
  `ingress.enabled=true`: cert-manager's HTTP-01 solver pods (label
  `acme.cert-manager.io/http01-solver`, port 8089) answer ACME challenges
  through the ingress controller, and the chart's default-deny would otherwise
  block the challenge so certificate issuance times out. Selects only pods
  carrying cert-manager's solver label, so it is inert for installs that
  terminate TLS elsewhere or bring their own certificate Secret. No values
  schema change; an unmodified existing values file renders the same
  manifests as before unless both toggles are already on.

### Changed
- Bumped the default `validation.imageTag` (and its `_helpers.tpl` fallback)
  from `v0.3.0` to `v0.4.0`. The runner at that release adds python-csv node
  support. No values key changes shape, so an unmodified existing values file
  keeps working and simply picks up the new default on upgrade. **Installs
  pinning `validation.imageTag`** must re-pin to `v0.4.0` (or drop the
  override to track the chart's default) before upgrading, the same way as
  any other `continuo-python-runtime-<engine>` tag bump — an unmirrored or
  stale pin otherwise renders healthy at install and only fails later, at the
  next release promotion, as a validation-pod `ErrImagePull`. A pin to an
  older tag is now caught at render time rather than left as a warning here:
  see the `validation.imageTag` capability gate under Breaking below.

### Breaking
- Renamed service `ui-service` to `ui`: the `services[]` entry, Deployment,
  Service, NetworkPolicy, and ingress backend now use the name `ui`.
  Image is now `continuo-ui`. The default `ingress.tls.secretName` also
  changed, from `ui-service-tls` to `ui-tls`: any install with
  `ingress.tls.enabled=true` that relies on this default (rather than
  setting `ingress.tls.secretName` explicitly) must rename/recreate the
  Secret before upgrading, or TLS termination breaks.
- Renamed service `agent-runner` to `agent-chat`; its database
  `continuo_agent` is now `continuo_agent_chat` (renamed in place on
  upgrade — see NOTES). Image is now `continuo-agent-chat`. The
  `global.agentRunnerGrpcAddr` values key is renamed to
  `global.agentChatGrpcAddr`; any install overriding it must rename the
  key before upgrading.
- Renamed service `remediation-agent` to `agent-remediation`; database
  `continuo_remediation_agent` is now `continuo_agent_remediation` (rename in
  place on upgrade — see NOTES); consumer group
  `remediation-agent-remediation-requested` is now
  `agent-remediation-remediation-requested`. Image is now
  `continuo-agent-remediation`.
- Renamed service `manifest-controller` to `topology-controller`; consumer
  group `manifest-controller-release-requested` is now
  `topology-controller-release-requested` (drained group deleted on upgrade —
  see NOTES). Image is now `continuo-topology-controller`. Stream names are
  unchanged — `manifest.loaded.candidate:v1` still names the dbt-manifest
  artifact this service loads, not the service itself.
- `validation.imageTag`, when set explicitly, is now checked against the tag
  ranges known to predate python-csv validation support: `_helpers.tpl`'s
  `continuo.validation.image` fails the render (rather than only warning here)
  when the tag matches `v0.1.x`-`v0.3.x`, or when it isn't shaped like a
  released tag (`"vX.Y.Z"` or `"vX.Y.Z@sha256:<digest>"`) at all — an
  unparseable tag can't be checked against that known-bad range, so it fails
  closed the same way. **BREAKING only for an install that explicitly pins
  `validation.imageTag` to one of those tags** (or to something unparseable):
  it previously rendered and installed, silently running a validation runner
  that ignores a python-csv node's `csv_source` contract field and reports
  success without checking the file's header, letting a mismatched-header csv
  promote unvalidated; it now fails `helm template`/`helm install`/`helm
  upgrade` outright, naming the tag and the required floor. No values key is
  added, renamed or removed, and an install that has never overridden
  `validation.imageTag` (or that already pins `v0.4.0` or later) is
  unaffected. Re-pin to `v0.4.0` or later, or drop the override, before
  upgrading.

## [0.3.0] - 2026-08-21

### Added
- `global.terminationGracePeriodSeconds` (default `30`) — sets every service
  Deployment's `terminationGracePeriodSeconds` explicitly instead of relying
  on Kubernetes' implicit 30s default. Each service bounds its own graceful
  shutdown sequence via the `SHUTDOWN_GRACE` env var (default 15s, 10s for
  `agent-runner`); the pod-level value must stay comfortably above whatever
  `SHUTDOWN_GRACE` is configured to, or Kubernetes can SIGKILL the container
  mid-teardown. The default of `30` matches today's implicit behavior, so an
  unmodified existing values file renders the same Deployment as before.
- `remediation-agent.env.RELEASE_CONTROLLER_URL` (default
  `http://release-controller:8088`), `remediation-agent.env.SHADOW_VERIFY_TIMEOUT`
  (default `"20m"`) and `remediation-agent.env.SHADOW_VERIFY_POLL_INTERVAL`
  (default `"15s"`) — release-controller's HTTP address, how long a python-node
  fix proposal waits for its shadow verification release to reach a verdict
  before the attempt is recorded as failed, and how often the remediation-agent
  reads those releases. The timeout is spent only while the shadow release is
  actually running: time it spends queued behind another release does not count
  against it. All three sit in the same free-form `env` map as the
  existing `REMEDIATION_PR_POLL_INTERVAL`/`REMEDIATION_PR_OPENING_GRACE_PERIOD`
  keys, so no schema change is required; an unmodified existing values file
  already gets these defaults via the chart's own `env` defaults.

### Breaking
- The validation image is renamed to `continuo-python-runtime-<engine>` (it ships
  from the merged `continuo-python-runtime` repository, which now carries both the
  python-node runtime and the validation runner); the default
  `validation.imageTag` becomes `v0.3.0`.

  **BREAKING for any install that pins `validation.imageTag`.** No values key is
  added, renamed or removed, but the key's meaning moves to a different image
  repository, and `validation.imageTag` is composed into the ref verbatim. An
  existing values file that keeps the previous default `imageTag: "v0.4.0"` now
  renders `continuo-python-runtime-<engine>:v0.4.0` — a manifest that does not
  exist, so every validation Job fails with `ErrImagePull`. A digest pin is worse:
  `"v0.4.0@sha256:<digest>"` names a digest from the retired
  `continuo-validation-<engine>` repository, which is meaningless against the new
  one. Only an install that never set `imageTag` upgrades cleanly, because it
  picks up the new `v0.3.0` default.

  **Upgrade guidance.** Before upgrading, either remove your
  `validation.imageTag` override entirely (recommended — you then track the
  chart's default), or set it to a tag that exists in the new repository:

  ```yaml
  validation:
    imageTag: "v0.3.0"                      # or "v0.3.0@sha256:<digest>" to pin immutably
  ```

  Re-resolve any digest against `ghcr.io/carolsimone/continuo-python-runtime-<engine>`;
  digests are repository-scoped and do not carry over. Operators who mirror
  images into a private registry must mirror the new name. The old
  `continuo-validation-<engine>` images are no longer referenced by the chart.
  The engine remains part of the image name rather than the tag, so the
  `"vX.Y.Z@sha256:<digest>"` override form still composes a valid immutable ref.
- `remediation-agent` now refuses to start when one of its optional duration
  settings — `LLM_CACHE_TTL`, `REMEDIATION_PR_POLL_INTERVAL`,
  `REMEDIATION_PR_OPENING_GRACE_PERIOD`, `SHADOW_VERIFY_TIMEOUT`,
  `SHADOW_VERIFY_POLL_INTERVAL` — is set to something that is not a Go duration
  (e.g. `"20 minutes"`), naming the offending key in the boot log. Previously
  such a value was silently replaced by the default, so an install looked
  configured while every process ran the built-in value. Leaving a key unset is
  unchanged and still runs its documented default, so an unmodified values file
  is unaffected.
- The `orchestrator` service now requires object storage to be reachable at
  start-up: it reads each release's code-bundle document to record node
  code-version history in the graph. Endpoint, bucket and region already reach
  every pod through the shared ConfigMap.
- S3 credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) are now injected
  by the deployment template for every storage-reading service, instead of being
  listed in each service's `secretEnv`. `services` is a list and Helm replaces
  lists wholesale, so an operator upgrading with their own older copy of
  `values.yaml` would otherwise carry an entry lacking the new refs and start
  orchestrator with no credentials. An unmodified existing values file now keeps
  working, and an entry that still lists the refs is de-duplicated rather than
  emitted twice. Installs authenticating by IAM role or workload identity leave
  the secret empty and are unaffected.
- `orchestrator` is now allowed through the bundled-MinIO NetworkPolicy. Without
  it, a default install with `networkPolicy.enabled=true` blocked every
  code-bundle read.
- `files/service_repos.yaml` header comments describe the map accurately: it
  covers every service that ships data jobs, not dbt services only, and each
  team's repository is named by the remediation trigger rather than assumed to
  be one shared checkout. Comments only — the file's keys, the ConfigMap it
  populates, and the values contract are unchanged, so an existing
  `values.yaml` or override needs no edit.
- `state`, `orchestrator`, and `agent-runner` now set `livenessPath: /livez`
  instead of falling back to the always-200 `/health`. For `state` and
  `orchestrator` this means a dead or wedged Redis stream consumer restarts
  the pod, since `/livez` reports workers and heartbeats only, deliberately
  excluding dependency probes, so a transient Redis/Postgres outage no longer
  restarts a pod whose consumers are already retrying. `agent-runner` runs no
  stream consumers, so this change gives it no new restart behavior — it now
  answers `/livez` so every Go service exposes the same liveness probe
  contract. Readiness (`readinessPath: /ready`) is unchanged for all three,
  and is not uniform across the chart: `state`, `orchestrator`,
  `executor-controller`, `k8s-controller`, and `agent-runner` use `/ready`,
  while `release-controller`, `remediation`, and `remediation-agent` use
  `/healthz`. `services` is a list and Helm replaces lists wholesale, so an
  operator who overrides it in their own values file keeps their old entries
  as-is on upgrade; they must add `livenessPath: /livez` to their own service
  entries themselves, or those pods keep probing the always-200 `/health`.
  Implies a MINOR version bump.
- `global.terminationGracePeriodSeconds`'s schema `minimum` is now `20`,
  raised from `1`. A value at or below the largest `SHUTDOWN_GRACE` default
  in the code (15s) guarantees the kubelet SIGKILLs the container
  mid-teardown before its own graceful-shutdown sequence can finish; 20s
  leaves 5s of genuine headroom above that default. The chart's own default
  of `30` is unaffected. An existing values file that already sets this key
  to `20` or above is unaffected; one that set it below `20` now fails
  `helm lint`/install and must raise the value. Implies a MINOR version
  bump (a previously-valid low value is no longer accepted, but no existing
  *default* installation is affected).
- `pkg/lifecycle`'s in-flight drain step is capped at half of
  `SHUTDOWN_GRACE`, not the full value — fixed in the previous release
  alongside `global.terminationGracePeriodSeconds`'s introduction, but not
  called out here at the time. `state`, `orchestrator`,
  `executor-controller`, and `k8s-controller` therefore drain in-flight work
  for up to 7.5s (half of their 15s default), not 15s. An operator who had
  raised `SHUTDOWN_GRACE` specifically to widen the in-flight drain window
  now gets half of what they set for that half, though the *total* shutdown
  sequence (drain plus infra teardown) stays bounded by the same
  `SHUTDOWN_GRACE` value as before. All tracked goroutines in every service
  unwind well within 7.5s on cancellation, so this is not expected to change
  observed shutdown behavior for a default install.


## [0.2.0] - 2026-08-10

### Added
- `remediation-agent.env.REMEDIATION_PR_OPENING_GRACE_PERIOD` (default `"10m"`) — how long the remediation-agent reconciler's opening sweep waits, measured against the claim's own `pr_claimed_at`, before releasing a stranded `pr_state='opening'` proposal back to `'failed'` for retry. Sits alongside the existing `REMEDIATION_PR_POLL_INTERVAL` in the same free-form `env` map, so no schema change is required; an unmodified existing values file already gets this default via the chart's own `env` defaults.
- `validation.imageTag` — pins the external `continuo-validation-<engine>` image version independently of the chart `appVersion` (default `v0.4.0`). The key is optional (not in `validation`'s `required` list), so a release that predates it — e.g. one upgraded with `helm upgrade --reuse-values` — still validates and falls back to the chart's current default in `templates/_helpers.tpl`'s `continuo.validation.image`. An explicit `imageTag: null` override collapses to that same "key absent, use the default" path (Helm drops a null-valued override key before merging). A present-but-empty string (`""`) is different — it survives the merge as a deliberate misconfiguration — and is rejected: at `helm lint`/`install` time by the schema's `minLength: 1`, and at `helm upgrade --reuse-values` time (where the schema cannot see the merged value) by a `fail` in the same helper. Accepts an optional `@sha256:<digest>` suffix (`"vX.Y.Z@sha256:<digest>"`) for an immutable pin — a plain tag is mutable, since `continuo-validation`'s publish workflow re-pushes `:vX.Y.Z` on every tag; see `values.yaml`/README.
- `helm upgrade` NOTES.txt now reminds operators that the validation image is externally released and pinned by `validation.imageTag` independently of `appVersion`, and that private-registry mirrors need it mirrored explicitly — an unmirrored bump otherwise renders healthy at install and only fails later, at the next release promotion, as a validation-pod `ErrImagePull`.

### Changed
- The validation image is now the externally released `continuo-validation-<engine>` (from github.com/carolsimone/continuo-validation), replacing the chart-appVersion-tagged `continuo-validation-runner-<engine>`; `global.imageTag` no longer applies to it.
- Bumped the default `validation.imageTag` (and its `_helpers.tpl` fallback) from `v0.2.0` to `v0.4.0`. The runner at that release adds the `build_from_columns` op and the `CANDIDATE_SPEC_URI` env contract, which python-node validation Jobs require; an install still on `v0.2.0` would dispatch a Job the runner rejects as an unknown op.

### Fixed
- Service pods now carry a `checksum/config` annotation over the shared
  ConfigMap, so `helm upgrade` restarts the pods whose configuration actually
  changed. Services read that ConfigMap through `envFrom`, and Kubernetes never
  refreshes environment variables in a running pod, so changing `REDIS_HOST`,
  `S3_BUCKET`, `LOG_LEVEL` or `validation.engine` previously rendered an
  identical Deployment and every pod kept serving the old value indefinitely.
  The engine case corrupted output rather than merely going stale:
  `executor-controller` rolls on an engine change because its validation image
  reference changes, so `manifest-controller` would have kept uploading the
  previous engine's SQL to the new engine's validator. **Upgrade note:** an
  upgrade that changes any shared ConfigMap value now performs a rolling
  restart of the affected services; an upgrade that changes none does not, as
  the digest covers only values derived deterministically from `values.yaml`
  (generated credentials live in Secrets).
- `manifest-controller` now reads and re-renders SQL in the dialect of the
  engine the install actually targets, instead of always assuming Postgres. The
  shared ConfigMap gained a `WAREHOUSE_ENGINE` key derived from the existing
  `validation.engine` value — no new values key, so unmodified overrides keep
  working. On a `validation.engine: trino` install, the candidate SQL uploaded
  for blue/green validation was previously re-rendered through sqlglot's
  `postgres` dialect, emitting constructs Trino rejects (a cast came out as
  `CAST(x AS TEXT)` rather than `CAST(x AS VARCHAR)`), so validation could fail
  on SQL the warehouse would otherwise have accepted. An engine with no dialect
  mapping now fails the service at startup rather than silently emitting another
  engine's SQL.
- Redis-backed services (`manifest-controller`, `ui-service`) no longer start
  before the bundled Redis is reachable. They previously raced the Redis
  StatefulSet, crashed on connect, and entered CrashLoopBackOff — whose
  exponential backoff (10s, 20s, 40s, 80s...) kept them down well after Redis
  became ready, so the restart loop cost more time than the wait it replaced. A
  `wait-for-redis` initContainer now gates them, mirroring the existing
  `wait-for-migrations` gate for Postgres-backed services. Bundled installs only;
  BYO installs point at Redis that is already running and render no initContainer.
- The quickstart's `/etc/hosts` step is documented with the two things operators
  actually get stuck on: it must be run from a real terminal, because sudo reads
  the password from the controlling terminal and exits with `a terminal is
  required to read the password` in an IDE pane or agent shell; and the password
  it asks for is the local account password, not the Dex demo login printed
  directly below it. The chart README now also documents a browser
  `--host-resolver-rules` flag that resolves the issuer hostname without root,
  for operators who cannot use sudo at all.

## [0.1.1] - 2026-07-29

### Fixed
- `values.schema.json` was rejecting the existing `services[].image` override
  and any resource key beyond `cpu`/`memory` (e.g. `ephemeral-storage`,
  `nvidia.com/gpu`) — both are valid, already-supported overrides that would
  have blocked upgrades for anyone using them.
- `templates/NOTES.txt` advertised an HTTPS URL regardless of
  `ingress.tls.enabled`, a fixed "password" login regardless of
  `dex.demoUser.passwordHash` overrides, and omitted the Dex port-forward/
  `/etc/hosts` steps the quickstart login actually needs when Ingress is off.
- `dbt/README.md` described the dispatched dbt Jobs as running
  `base/entrypoint.sh` against a throwaway DuckDB file; in reality
  `executor-controller` sets `Command` explicitly from
  `deploy/continuo/files/dbt-commands.yaml` (bypassing the image entrypoint)
  and every service's `profiles.yml` targets real Postgres.

### Added
- `values.schema.json` validates `values.yaml` structure and types on `helm
  lint`/`install`/`upgrade`, catching typos and wrong-shaped overrides before
  they reach template rendering or the `required` guards in `_helpers.tpl`.
- `templates/NOTES.txt` prints post-install/upgrade access instructions, the
  quickstart Dex login, and an upgrade-notes pointer to this changelog.
- This changelog, and a CI gate requiring it to be updated whenever
  `values.yaml` or `templates/` change.

## [0.1.0] - 2026-07-17

First published chart release (`oci://ghcr.io/carolsimone/charts/continuo`).
Predates this changelog — see repository git history for what shipped.
