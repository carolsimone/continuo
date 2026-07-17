# executor-controller

## Purpose

`executor-controller` turns dispatch messages into Kubernetes Jobs.

It is responsible for:
- consuming executable node intents from Redis
- deduplicating repeated dispatches via `pkg/messageprocessing` keyed on `(message_id, stream_name)`
- durably recording deployment intent in its own outbox
- creating K8s Jobs via the Kubernetes API
- publishing `task.status.updated:v1` (FAILED) on the never-deployed path so `state` learns of dispatch-time terminal failures (k8s-controller owns the RUNNING/terminal pod lifecycle)
- publishing `node.deployed:v1` so `k8s-controller` can begin monitoring

## Owned Storage (Postgres: `continuo_executor`)

| Table | Purpose |
|---|---|
| `executor_deployments` | K8s-deploy command queue. Handlers write a row here inside their Unit-of-Work transaction (pure Postgres write, no Kubernetes I/O). `deployer.Dispatcher` drains due rows, calls `CreateQueryJob`, and on success writes canonical announcement rows to `executor_outbox`. A `mode` column distinguishes `production` rows (the default query.model path) from `validation` rows (candidate-release validation checks, which carry `release_id`/`node_id`, `upstream_node_ids` (in-set gating edges, covering both intra- and cross-service upstreams), and a per-node terminal `outcome`). Validation rows start `pending` (no in-set upstreams) or `blocked` (has in-set upstreams that are not yet `ok`). |
| `executor_outbox` | Canonical transactional outbox — one row per pending Redis announcement (`task_status_updated` FAILED on the never-deployed path, plus `node_deployed`/`node_updated`); `pkg/outbox.Processor` polls and performs the Redis XADD per row. The deploy-success path no longer announces RUNNING — k8s-controller owns the running/terminal pod lifecycle. |
| `message_processing` | Inbound dedup: keyed on `(message_id, stream_name)`; prevents double-processing of duplicate Redis messages |
| `cancelled_schedules` | Records schedule cancellations; consulted by deploy handlers before writing to `executor_deployments` |
| `executor_worker_pools` | Registers the pools of reusable worker pods, one row per (service, image, runtime manifest, credential) combination, keyed by `pool_key`. Holds the pool's `credential_sha256` — the digest the internal worker API authenticates against; the raw credential is never stored — plus `desired_replicas`, `last_activity_at`, and `initialization_error` (non-empty means the pool's workers could not hydrate their artifact, so the pool is handed no work). Rows are written by the pool reconciler, one per identity the waiting worker-routed work names; with the deployed default of `EXECUTION_MODE=jobs` and no overrides nothing is routed to a worker, so the table stays empty and the worker API authenticates nobody. |
| `validation_aggregates` | Per-release-per-mode sentinel (PK: `(release_id, mode)`; mode CHECK allows `validation`, `seed_build`, `compile`). `ClaimEmission(release_id, mode)` does an `INSERT … ON CONFLICT DO NOTHING` so exactly one caller wins the right to emit the aggregate announcement for that leg. `LockRelease(release_id, mode)` takes a transaction-scoped advisory lock (`pg_advisory_xact_lock(hashtext(release_id || ':' || mode))`) that serializes the whole count→claim→emit gate per `(release_id, mode)` pair, so the three legs of one release lock independently and concurrent last-node terminals cannot both no-op (lost emission) under READ COMMITTED. |

`executor_deployments` schema: `id`, `message_processing_id` (nullable FK), `task_id`, `schedule_id`, `job_params` (JSONB), `status` (`pending` / `blocked` / `dispatching` / `deployed` / `leased` / `running` / `retry_pending` / `succeeded` / `failed` / `skipped` / `cancelled`), `retry_count`, `max_retries`, `next_attempt_at`, `created_at`, `deployed_at`, `error_message`, plus the validation-mode columns `mode` (`production` / `validation` / `seed_build` / `compile`), `release_id`, `node_id`, `outcome` (`ok` / `failed` / `skipped`), `dbt_log_uri`, `run_results_uri` (S3 key of the structured validation result; NULL when the pod emitted no block), `outcome_at`. Production rows leave the validation columns NULL. Validation, seed-build, and compile rows have no real task/schedule identity, so the `NOT NULL` `task_id`/`schedule_id` columns are filled with deterministic UUIDv5 values derived from an immutable namespace over `(release_id, node_id)`; a partial unique index on `(release_id, node_id) WHERE mode IN ('validation','seed_build','compile')` enforces one row per (release, node, mode) combination, and a partial unique index on `(candidate_release, candidate_release_node_mode)` covers the same predicate for release-level uniqueness. Migration V16 relaxed the status and outcome CHECK constraints to accommodate the new values. `blocked` is non-terminal in the aggregate gate (the release cannot complete while any node is `blocked`). `skipped` is a terminal non-`ok` outcome (a node that could not run because an upstream failed; its presence fails the release).

`executor_outbox` rows conform to the canonical schema: `id`, `message_processing_id` (nullable), `aggregate_type`, `aggregate_id`, `event_type`, `payload` (JSONB), `stream_name`, `status`, `retry_count`, `max_retries`, `created_at`, `processed_at`, `error_message`.

## Inbound Interfaces

### Redis consumers

| Stream | Consumer group | Description |
|---|---|---|
| `query.model:v1` | `executor-query-model` | Primary dispatch: new node ready for execution |
| `retry.task:v1` | `executor-retry` | Retry dispatch: re-attempt a failed node |
| `schedule.cancelled:v1` | `executor-schedule-cancelled` | Schedule cancellation: suppress future deployments for the schedule and cancel the ones already in flight, releasing their execution slots and stopping the worker pod of every one a worker holds under a lease |
| `executor.job.terminal:v1` | `executor-job-terminal` | A dispatched Kubernetes Job has settled; releases the execution slot it held |
| `validation.requested:v1` | `executor-validation-requested` | Candidate-release validation request: enqueue one `mode=validation` deployment per node |
| `validation.node.completed:v1` | `executor-validation-node-completed` | Per-node validation Job terminal status from k8s-controller; records the node outcome, unblocks or skips in-set downstreams, and runs the per-release aggregate-emit gate |
| `validation.result:v1` | `executor-validation-result-teardown` | The `kind=complete` message that executor-controller itself emits on this stream; a dedicated consumer in the same process reacts to it to drop the `_candidate_<release>` schema from the dbt warehouse via `CandidateSchemaCleaner` |
| `seed.build.requested:v1` | `executor-seed-build-requested` | Candidate seed-build request: enqueue one `mode=seed_build` deployment per new/changed dbt-seed node |
| `seed.build.node.completed:v1` | `executor-seed-build-node-completed` | Per-seed build terminal status from k8s-controller; records outcome and runs the per-release seed-build aggregate-emit gate |
| `compile.requested:v1` | `executor-compile-requested` | Compile request: enqueue exactly one `mode=compile` deployment for the changed service |
| `compile.node.completed:v1` | `executor-compile-node-completed` | Compile Job terminal status from k8s-controller; records outcome and emits `compile.completed:v1` via the aggregate gate |

`query.model:v1` and `retry.task:v1` carry the same fields:
- `task_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema_name`, `table_name`, `job_name`
- `node_type`
- `operation` — the dbt verb to run: `""` (default, `run`), `test`, or `build`; selects the argv `CommandResolver.NodeCommand` builds for the node, independently of `node_type`.

### Inbound message processing

executor-controller consumes the streams above via `pkg/redis.StreamConsumer`. For each stream the wire path is:

`pkg/redis.StreamConsumer` → `adapters/redis/<stream>_binding.go` → `service/handlers/<stream>_handler.go`

The binding parses the XMessage into a typed `domain/events.<Event>`, runs `pkg/messageprocessing.Dedup` against the per-service `message_processing` table (keyed on `(message_id, stream_name)`), and invokes the handler inside a single Unit-of-Work transaction. `schedule.cancelled:v1` skips dedup because it is naturally idempotent: `cancelled_schedules.Insert` is `INSERT ... ON CONFLICT DO NOTHING`, and a redelivery finds the schedule's deployments already terminal, so the schedule-scoped `FOR UPDATE` lookup returns none to cancel.

The cancelled-schedule guard runs inside `QueryModelHandler` and `RetryTaskHandler` via `uow.CancelledSchedulesRepo().Exists`; a cancelled match commits the dedup row (so the message is ACKed and never reprocessed) and returns without writing to `executor_deployments`.

`validation.requested:v1` carries every node for a release in one message (flat JSON `payload` field). Each node entry includes `upstream_node_ids` — the in-set upstreams (intra- and cross-service) that must complete before this node can be dispatched — and `candidate_sql_uri`, the `s3://` pointer to the node's rewritten SQL (empty for seeds). The handler writes one `executor_deployments` row per node: `blocked` if `upstream_node_ids` is non-empty, `pending` otherwise, and records `candidate_sql_uri` in the row's `job_params` for use when the K8s validation Job is created. Its binding deduplicates per-release on a deterministic release-derived `outbox_entry_id` rather than the inbound `msg.ID`, so a redelivery with a fresh Redis ID still collides on the same key.

Before enqueuing any node, the binding creates the release's `_candidate_<release>` schema in the dbt warehouse exactly once via `CandidateSchemaCreator.EnsureCandidateSchema`. Creation runs inside a warehouse transaction that first takes a transaction-scoped advisory lock (`pg_advisory_xact_lock`) keyed on the schema name, so concurrent callers for the same schema serialize and only one issues the `CREATE SCHEMA IF NOT EXISTS`; a unique-violation is additionally tolerated as success. Pre-creating the schema once, ahead of the fan-out, means every validation Job finds the schema already present on start, so parallel root Jobs never race `CREATE SCHEMA` on `pg_namespace`. Schema creation runs against the warehouse outside the executor's Unit-of-Work transaction; a failure returns before any deployment row is enqueued and the message is retried. The pre-create runs only for a fresh release: a deduped redelivery skips it.

`validation.node.completed:v1` carries one node's terminal result as a flat JSON `payload` field (`release_id`, `node_id`, `outcome` ∈ {`ok`,`failed`}, optional `dbt_log_uri`, optional `run_results_uri`) with `outbox_entry_id` as a flat sibling. Its binding uses STANDARD `(message_id, stream_name)` dedup with an `outbox_entry_id` fallback, because it carries a normal upstream outbox row id from k8s-controller. The `ValidationNodeCompletedHandler` looks up the `(release_id, node_id)` validation deployment, attaches the outcome via `RecordOutcome`, saves, then propagates topological state: on `ok`, any `blocked` downstream whose every in-set upstream is now `ok` transitions to `pending` (unblocked for dispatch); on `failed`, every transitively `blocked` downstream is marked `skipped` (terminal, non-`ok`). The handler then runs the aggregate-emit gate. An unknown `(release_id, node_id)` (no matching row) is logged and ACKed; a redelivery whose deployment already carries an outcome is a no-op ACK (no double-record, no duplicate aggregate).

The `kind=complete` message on `validation.result:v1` is both produced and consumed by executor-controller (via the `executor-validation-result-teardown` consumer group). The teardown consumer calls `CandidateSchemaCleaner.DropCandidateSchema(candidate_schema)`, which issues a `DROP SCHEMA … CASCADE` against the dbt warehouse (`DBT_POSTGRES_DB`). This removes the shared `_candidate_<release>` schema regardless of whether validation passed or failed. `kind=node` messages on the same stream carry per-node projections only and are ignored by the teardown consumer.

### HTTP (port 8084)

- `GET /health` — liveness probe only

## Outbound Interfaces

### Redis producers (via outbox)

`deployer.Dispatcher` writes canonical announcement rows to `executor_outbox` as part of each deploy cycle (never at inbound-message time). `pkg/outbox.Processor` then drains those rows via the executor `OutboxPublisher`, which is a uniform marshal-and-XADD — one Redis XADD per row, no K8s I/O and no executor-specific fanout logic in the publisher.

On a successful deploy, the dispatcher writes one outbox row:
1. `node_deployed` → XADD `node.deployed:v1`

The deploy path no longer announces RUNNING — k8s-controller announces `task.status.updated:v1` (RUNNING) the first time it observes the Job running, so the running/terminal pod lifecycle has a single producer.

On terminal failure (permanent error or retry-budget exhaustion), the dispatcher writes two outbox rows:
1. `task_status_updated` (FAILED) → XADD `task.status.updated:v1`
2. `node_updated` (FAILED) → XADD `node.updated:v1`

| Stream | Description |
|---|---|
| `task.status.updated:v1` | Published with `status=FAILED` on the never-deployed terminal dispatch failure only (permanent error or retry-budget exhaustion before a pod exists). k8s-controller owns RUNNING and the pod terminal (SUCCEEDED/FAILED). |
| `node.deployed:v1` | Published after K8s job creation succeeds (both production and validation Jobs); triggers `k8s-controller` monitoring. For validation Jobs the `task_id`/`schedule_id` are the deterministic synthetic UUIDs derived from `(release_id, node_id)`; they are inert carriers because k8s-controller routes the validation Job's status by its `mode=validation` label, not by these IDs |
| `node.updated:v1` | Published on terminal dispatch failure only; consumed by `orchestrator` to advance the schedule |
| `validation.result:v1` (`kind=complete`) | Per-release validation decision; emitted exactly once when every `mode=validation` node for a release is terminal. Payload is the decision only: `kind` (`"complete"`), `release_id`, `candidate_schema` (the `_candidate_<release>` schema name, for teardown), and `aggregate_status` (`ok` iff every node is `ok`, else `failed`). It carries no per-node content and derives `aggregate_status` from the stored per-node outcomes directly, not from stream delivery order. It keeps the release's deterministic `aggregate_id` (`uuid.NewSHA1(namespace, "release:"+releaseID)`). |
| `validation.result:v1` (`kind=node`) | Per-node validation projection; emitted in `SettleNodeTerminal` (validation leg) as each node settles, so `release-controller` can render per-node validation results live and build its per-node read model. One emit is produced for the settled node itself, plus one for every downstream node the settle's failure propagation skips (a failed node's transitive blocked descendants never run through their own settle, so the settle emits their projection on their behalf, with `status` `"skipped"` and no URIs). Each is an idempotent last-write projection keyed by `node_id`, and each gets its own generated `aggregate_id` (not the terminal's deterministic one) so per-node rows publish to the outbox in parallel rather than draining one at a time behind the terminal row's `PerAggregateFIFO` lane. In the normal case the per-node rows and the terminal are all still pending together and flush in one `created_at`-ordered outbox batch anyway; release-controller's decision reads only `aggregate_status`, which does not depend on delivery order. Payload: `kind` (`"node"`), `release_id`, `stage` (`"validation"`), `node_id`, `status` (`ok`/`failed`/`skipped`), optional `dbt_log_uri`, optional `run_results_uri` |
| `seed.build.completed:v1` | Per-release seed-build aggregate; emitted exactly once when every `mode=seed_build` node for a release is terminal. Payload: `release_id`, `status` (`ok`/`failed`), `per_node`, `candidate_schema` |
| `compile.completed:v1` | Compile aggregate; emitted exactly once when the compile node settles. Payload: `release_id`, `status` (`ok`/`failed`), `per_node`, `candidate_schema` |

`task.status.updated:v1` payload fields:
- `task_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema_name`, `table_name`
- `status` — `FAILED` (the only status executor emits on this stream; the never-deployed terminal)

`node.deployed:v1` is emitted as a typed JSON `payload` field (`pkg/events.NodeDeployed`), with `outbox_entry_id` as a flat sibling field for consumer-side dedup. Payload fields:
- `task_id`, `schedule_id`, `schedule_name`
- `service_name`, `schema_name`, `table_name`, `job_name`
- `node_type`, `image_tag`, `operation` (omitted when empty)
- `task_retry_count`, `max_retries`

`operation` is the dbt verb the Job runs (e.g. `test`, `build`); it is empty for a normal `dbt run` (whose wire format is unchanged) and rides `node.deployed:v1` so `k8s-controller` carries it through its durable check/retry chain rather than re-deriving it from Job metadata.

### Execution mode

How a production record reaches dbt: `jobs` gives every task its own Kubernetes Job, `workers` has tasks claimed from a pool of reusable pods. Both paths draw execution slots from the one `MAX_CONCURRENT_EXECUTIONS` budget.

| Variable | Meaning |
|---|---|
| `EXECUTION_MODE` | The path a service takes unless it is pinned. `jobs` or `workers`; absent means `jobs`. An unrecognised value is a fatal boot error |
| `EXECUTION_MODE_OVERRIDES_JSON` | A JSON object pinning individual services, e.g. `{"finance":"workers"}`. This is the rollout lever: with the default mode `jobs`, naming a service here runs that service — and only it — on workers. An unparseable object, or a mode that is neither `jobs` nor `workers`, is a fatal boot error rather than a silently dropped pin |
| `MAX_CONCURRENT_EXECUTIONS` | The shared execution budget. Required and positive, with no in-code default, so an executor can never size its own concurrency from a literal that does not match its cluster. `MAX_CONCURRENT_JOBS` is read as a transition alias for a deployment still carrying the older spelling |
| `WORKER_IDLE_TIMEOUT` | How long a pool with nothing to do keeps its pods before it is sized to zero |
| `WORKER_LEASE_TTL` | How long a claim holds a task before the reaper may reassign it |
| `WORKER_HEARTBEAT_INTERVAL` | How often a worker is expected to extend its lease. Three of them must fit inside `WORKER_LEASE_TTL` — a worker has to be able to lose two heartbeats without losing its task — and a value that does not is a fatal boot error |
| `WORKER_CLAIM_WAIT` | The longest a claim request blocks waiting for work before answering empty |
| `WORKER_CONTROL_PLANE_URL` | The base URL worker pods call to claim tasks and report outcomes, baked into every pod a pool creates. It has no default and is required whenever a pool can be created — the mode is `workers`, or a service is pinned to them — so a URL that would strand every worker fails the boot rather than the pod. A deployment routing every service to Jobs creates no pool and needs none |

The four `WORKER_*` durations are Go duration strings (`60s`); a bare number is not parsed and leaves the in-code default in place. The deployed configuration is `EXECUTION_MODE=jobs` with empty overrides, so no task is routed to a worker until a service is explicitly named.

### Command resolution (`dbt-commands.yaml`)

Container commands for production runs, seed-build, and compile Jobs are resolved through the `ports.CommandResolver` port rather than a hardcoded dbt invocation. `adapters/commandcfg.Resolver` implements the port by loading an optional `dbt-commands.yaml` file at the path given by the `DBT_COMMANDS_CONFIG_PATH` environment variable. The file covers seven operations — `run`, `seed`, `snapshot`, `seed_build`, `test`, `build`, `compile`. When a file is present, the `default` block is required and must define all seven, and every `services.<name>` override must define all seven too; a service never falls through to `default` or a built-in for a missing key. An incomplete or missing block is a fatal boot error. With no file, the built-in complete plain-dbt default is used for every service:

| Operation | Built-in command |
|---|---|
| `run` | `dbt run --select <node>` |
| `seed` | `dbt seed --select <node>` |
| `snapshot` | `dbt snapshot --select <node>` |
| `seed_build` | `dbt seed --select <node>` (schema routing relies on the `DBT_TARGET_SCHEMA` env var contract) |
| `test` | `dbt test --select <node>` |
| `build` | `dbt build --select <node>` — materializes and tests the node in one invocation |
| `compile` | `dbt compile --profiles-dir /project`, writing its manifest to `/project/target/manifest.json` |

`run`, `seed`, and `snapshot` are selected by the node's `node_type` when the query's `operation` is empty (the default). `test` and `build` are selected by `operation` directly — `CommandResolver.NodeCommand(serviceName, operation, nodeType, node)` resolves the `test`/`build` template regardless of `node_type` once `operation` requests it.

Templates support two placeholders: `{{ node }}` (the dbt `--select` target; required in `run`, `seed`, `snapshot`, and `seed_build`) and `{{ target_schema }}` (the release's candidate schema; allowed only in `seed_build`). A `compile` override pairs its argv with an explicit `manifest_path` — the absolute, placeholder-free filesystem path where that team's tool writes `manifest.json`.

Two optional keys describe what a team's tool does beyond producing `manifest.json`:

| Key | Meaning |
|---|---|
| `compile.partial_parse_path` | Absolute, placeholder-free path where the tool leaves dbt's partial-parse msgpack. Omitted when the tool leaves it where dbt writes it; `CommandResolver.CompileCommand` then derives `partial_parse.msgpack` in the same directory as `manifest_path`. |
| `worker.wrapper_cache` | `required` when the team's wrapper reliably writes a reusable partial parse at that path, `opaque` when its cache behaviour is unknown. Valid on a `services.<name>` block only, and resolved from that block alone: it is a claim about one team's wrapper, and the `default` block describes plain dbt, so a `worker` block on `default` is a fatal boot error rather than an inherited value. An absent service block, an absent `worker` block, and a `worker` block without `wrapper_cache` all resolve to `opaque` — continuo only assumes a reusable parse cache when the owning team declares one. Native dbt ignores the key. |

Alongside the argv templates, the port exposes the resolved compile leg (`CompileCommand` — argv plus both output paths), the wrapper policy (`WrapperCachePolicy`), and `RuntimeContext`: canonical JSON of a service's whole resolved command surface — the seven raw, unsubstituted templates, both compile paths, and the wrapper policy. It is marshaled from a struct rather than a map, so identical configuration always serializes byte-identically and can be hashed into a stable digest.

`DBT_COMMANDS_CONFIG_PATH` unset, or set to a path with no file present, yields the built-in commands for every operation. A file that exists but fails to parse or fails validation (unknown operation key, empty argv, an unrecognised `{{ ... }}` placeholder, a missing required placeholder, a relative/placeholder-bearing `compile.manifest_path` or `compile.partial_parse_path`, a `worker.wrapper_cache` that is neither `required` nor `opaque`, a `worker` block on the `default` block, or an incomplete `default`/`services.<name>` block) is a fatal boot error, so a dialect typo or a partially-specified team override surfaces at startup rather than mid-release — load-time validation covers all seven keys: per-template checks (non-empty argv, allowed/required placeholders, absolute literal `compile.manifest_path`) plus a completeness check that every configured block defines all seven keys. The optional `compile.partial_parse_path` and `worker` keys are exempt from the completeness check but validated whenever present; `worker` is additionally rejected on the `default` block. Resolution through `CommandResolver` applies only to `CreateQueryJob` (production runs, including `test`), `CreateSeedBuildJob`, and `CreateCompileJob`; `CreateValidationJob` always runs the continuo-owned `validation_runner.py` regardless of any configured dialect. The file is read once at process start, so changing the mounted file — e.g. a Helm ConfigMap update — takes effect only after the executor-controller pod restarts.

**Example: finance service.** The deployed `dbt-commands.yaml` routes the `finance` service through its own CLI wrapper, `wise-dbt`, which is baked into the finance dbt image at `/usr/local/bin/wise-dbt`. The service declares all seven keys: `run: ["wise-dbt", "run-model", "{{ node }}"]`, `seed: ["wise-dbt", "load-seed", "{{ node }}"]`, `snapshot: ["wise-dbt", "capture-snapshot", "{{ node }}"]`, `test: ["wise-dbt", "test-model", "{{ node }}"]`, `build: ["wise-dbt", "build-model", "{{ node }}"]`, `seed_build: ["wise-dbt", "load-seed", "{{ node }}"]`, and `compile: {command: ["wise-dbt", "compile-project"], manifest_path: "/project/target/manifest.json"}`. It also declares `worker: {wrapper_cache: required}`: `wise-dbt` leaves dbt's partial parse where dbt writes it, beside `manifest.json`, so it declares no `partial_parse_path` and the path derives. When executor-controller builds a Job for a finance model run (node type `dbt-model`, table name `orders`), the resolver returns `["wise-dbt", "run-model", "orders"]`. The resolver applies this substitution at Job-build time; domain events carry only intent (node type and table name), so the command dialect choice is decoupled from the event stream.

### Kubernetes API

- `CreateQueryJob` — creates a K8s batch Job in the configured namespace with label `app=dbt-job`; container name is `dbt-job`; treated as idempotent (already-exists is not an error on retry). The container command is `CommandResolver.NodeCommand(serviceName, operation, nodeType, tableName)` — for the default `operation=run` the resolved `run`, `seed`, or `snapshot` argv for the node's type; for `operation=test` the resolved `test` argv (`dbt test --select <node>` unless overridden); for `operation=build` the resolved `build` argv (`dbt build --select <node>` unless overridden) — both `test` and `build` apply regardless of the node's type. The container `imagePullPolicy` is `IfNotPresent`: production deploy/query images are referenced by content-addressed tags that are unique per build, so a cached image of a given tag is always the correct one. Every Job pod executor-controller creates — this one included — carries pod-level `seccompProfile: RuntimeDefault`; the `dbt-job` container drops all Linux capabilities and forbids privilege escalation, while keeping the team image's own user.
- `CreateSeedBuildJob` — creates a `mode=seed_build` K8s batch Job (idempotent by job name). Uses the team service image (`imagePullPolicy: IfNotPresent`) and runs the container command from `CommandResolver.SeedBuildCommand(serviceName, tableName, candidateSchema)` — the resolved `seed_build` argv (always defined, whether from the built-in default or a configured block). `DBT_TARGET_SCHEMA=<CandidateSchema>` is set on the Job unconditionally, so the `generate_schema_name` macro in dbt-base routes the seed output to the candidate schema even when the resolved command carries no `{{ target_schema }}` placeholder. The pod carries `seccompProfile: RuntimeDefault`; the `dbt-job` container drops all capabilities and forbids privilege escalation, keeping the team image's own user.
- `CreateCompileJob` — creates a `mode=compile` K8s batch Job (idempotent by job name). The Job is a two-container pod with a shared `emptyDir` volume (`shared`, mounted at `/shared`) and pod-level `seccompProfile: RuntimeDefault`. The initContainer (`compile`) uses the team service image (`<service>:<image_tag>`, `imagePullPolicy: IfNotPresent`), drops all capabilities and forbids privilege escalation while keeping the team image's own user, and runs a single `sh -c` line built from `CommandResolver.CompileCommand(serviceName)` (quoted and joined by `shellJoin`/`shellQuote`, which pass shell-safe tokens through unquoted so the built-in compile form stays byte-identical to `dbt compile --profiles-dir /project`), in three parts: the resolved compile argv; `&& cp <manifest_path> /shared/manifest.json && chmod 644 /shared/manifest.json`, where `<manifest_path>` is the absolute path the resolver returns alongside the compile argv — the built-in `/project/target/manifest.json` unless overridden; and `&& if [ -x /continuo/bin/continuo-export-runtime-manifest ]; then ... else echo ...; fi`, which exports the release's runtime manifest artifacts into `/shared` when the team image ships the exporter. The exporter is invoked with `--manifest`, `--partial-parse` (the resolver's `PartialParsePath`, deriving beside the manifest unless the service declares one), `--output-dir /shared`, `--service-name`, `--release-id`, `--image-tag`, `--artifact-uri` (the `partial_parse.msgpack` key sibling to `MANIFEST_S3_URI`), and `--controller-context "$CONTINUO_RUNTIME_CONTEXT_JSON"`, then chmods the two artifacts it wrote 644. Everything the init container leaves in `/shared` is chmod'd 644 so it stays readable regardless of the team image's uid/umask — the upload container runs as a fixed, different uid. A team image built before the exporter existed takes the `else` branch, logs `runtime exporter absent; manifest-only compatibility release`, and still produces a working manifest-only release. `DBT_POSTGRES_*` env vars are set on the init container, plus `CONTINUO_RUNTIME_CONTEXT_JSON` — the canonical parse context from `runtimecontext.Build(CommandResolver.RuntimeContext(serviceName), os.Getenv)`, passed by env rather than on the command line so the compile line stays stable regardless of its contents. The main container (`upload`) uses the continuo-owned `s3-sidecar` image (a minimal python+boto3 image with no dbt; hosts `compile_uploader.py`), configured via the `S3_SIDECAR_IMAGE` env var — the Helm chart sets this to `<global.dockerHubUser>/continuo-s3-sidecar:<global.imageTag>` (`global.dockerHubUser` holds the `ghcr.io/<owner>` registry prefix used for every continuo-owned image); when unset it falls back to the locally-built `s3-sidecar:latest`, `DOCKERHUB_USERNAME`-prefixed when that env is set (local/e2e clusters that side-load images). The container runs `python /compile_uploader.py` with `COMPILE_MANIFEST_PATH=/shared/manifest.json`, `MANIFEST_S3_URI`, `COMPILE_PARTIAL_PARSE_PATH=/shared/partial_parse.msgpack`, `COMPILE_RUNTIME_DESCRIPTOR_PATH=/shared/runtime-manifest.json`, and S3 credential env vars; drops all capabilities and forbids privilege escalation; and, because it runs a continuo-owned image, additionally runs as non-root uid 65532. Its pull policy is governed by `VALIDATION_IMAGE_PULL_POLICY`. The uploader always uploads `manifest.json`; the two runtime artifacts are an all-or-nothing set, uploaded to sibling keys under the release prefix only when both exist and the descriptor's `sha256` matches the msgpack bytes. Neither present is the manifest-only compatibility path; exactly one present, or a digest mismatch, uploads nothing and fails the Job. A missing `image_tag` is a permanent error.
- `CreateValidationJob` — creates a `mode=validation` K8s batch Job in the configured namespace (idempotent by job name, `BackoffLimit` 0, `RestartPolicy` Never). The pod carries `seccompProfile: RuntimeDefault`. The `imagePullPolicy` defaults to `Always`: `VALIDATION_IMAGE` and `S3_SIDECAR_IMAGE` are both pinned to the release's image tag (same `global.dockerHubUser`/`global.imageTag`-derived `ghcr.io/<owner>/continuo-*` reference every other continuo-owned image uses), so `Always` is a cheap registry digest check rather than a full re-pull. The policy is overridable via `VALIDATION_IMAGE_PULL_POLICY` (`IfNotPresent`/`Never`/`Always`) for clusters that side-load images and have no registry to pull from — the kind-based e2e suite and local dev set `IfNotPresent`, where `Always` would fail with `ErrImagePull`. Labels: `app=dbt-job` (so existing watchers stay correct) plus `mode=validation` and `release-id`/`node-id` (sanitised to valid label values) for selection/observability; the `mode` label is what k8s-controller routes on. The raw, unmodified release/node identity is stamped separately as annotations `continuo.dev/release-id` and `continuo.dev/node-id` (allowing arbitrary values); k8s-controller reads these — not the sanitised labels — into the `validation.node.completed:v1` payload so the outcome lookup keyed on the unmodified `executor_deployments` row matches even when a label would be altered. `CreateValidationJob` builds a single-container Job that runs the continuo-owned `validation-runner` image (`python:3.12-slim` + psycopg2 + boto3 — no dbt); the container drops all capabilities, forbids privilege escalation, and — because it is a continuo-owned image — runs as non-root uid 65532. Every node type runs `python /validation_runner.py`, which dispatches on `VALIDATION_OP`: **`build_from_sql`** (default) — the container fetches the node's compiled SQL from `CANDIDATE_SQL_URI` in S3 itself (boto3) and runs `CREATE TABLE <candidate>.<table> AS (<sql>) WITH NO DATA`; it receives the warehouse connection env, S3 credentials (`S3_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`), and `CANDIDATE_SQL_URI`. **`clone_from_prod`** — the container clones an existing prod table's shape empty (`CREATE TABLE … AS SELECT * … WHERE 1=0`); no S3 credentials and no `CANDIDATE_SQL_URI`. Both ops are single-container: there is no init container, no shared `emptyDir`, and no dbt in the validation path — `dbt-base` and the team image are run only for compile, seed-build, and scheduled runs; the `s3-sidecar` is used only by the compile leg (manifest upload). The validation container holds S3-read and warehouse-write credentials together; this is acceptable because it is a continuo-owned, trusted image that already required warehouse credentials, and reading its own compiled-SQL input is not a privilege escalation. The runner prints the same sentinel-framed structured validation-result block (`status`/`message`/`failures`/`unique_id`) as its last stdout, which k8s-controller uploads to S3 as `run_results_uri`. The candidate schema is created once, race-safely, by executor-controller before the fan-out (see the `validation.requested:v1` handling above), so by the time any Job runs the schema already exists; `validation_runner.py` still calls `ensure_schema` under a session-level advisory lock and tolerates duplicate-schema errors as a second line of defense. Topological gating ensures every in-set upstream (intra- and cross-service) has been built as an empty table in the shared `_candidate_<release>` schema before any dependent node runs; because the SQL carried by `CANDIDATE_SQL_URI` already has every known-node schema ref rewritten to the candidate schema, no model edits and no dbt recompile are needed. Env vars set on every validation container: `DBT_TARGET_SCHEMA` (= candidate schema), `TABLE_NAME`, `RELEASE_ID`, `NODE_ID`, `SERVICE_NAME`, `SCHEMA`, `JOB_NAME`, `VALIDATION_OP`, `PROD_SCHEMA`, plus `DBT_POSTGRES_*` connection forwarding. S3 credentials and `CANDIDATE_SQL_URI` are additionally set on `build_from_sql` pods only. A missing `image_tag` is a permanent error.

- Worker pools (`k8s.WorkerPools`) — a pool is a Deployment and a Secret sharing one name, `dbt-worker-<first 16 chars of pool key>`, both labelled `app=continuo-dbt-worker` plus `pool-key` and `service_name`; the full pool key and the artifact identity travel as annotations, because they are longer than a label value may hold. `Ensure` writes the Secret first and the Deployment second, so a pod never starts before the credential it authenticates with exists, and is idempotent: it creates what is absent and updates what differs. `Status` reads the Deployment's replica counts and the Secret's presence independently — a Secret deleted from under a live pool is the case that must be repaired. `DeletePod` removes one pod by name and UID. A pool has no Service: workers are pull-only and are never called, so nothing needs to reach them.

### RBAC

The executor's ServiceAccount is granted, in its own namespace: `batch/jobs` (`create`, `get`, `list`, `watch`) for the Jobs path; `apps/deployments` (`get`, `create`, `update`) and `secrets` (`get`, `create`, `update`) for the pools it reconciles; and `pods` (`get`, `list`, `watch`, `delete`) — the reads the Jobs path makes, plus the delete that stops a worker's pod when its task is taken back. Deleting a Deployment or a Secret is not granted, because the reconciler never does it: a pool with nothing to do is sized to zero replicas rather than removed. Worker pods use no Kubernetes RBAC at all — they reach the control plane over HTTP and the object store through signed URLs, and their ServiceAccount needs nothing.

### K8s client configuration

`NewK8sClient` uses `KUBECONFIG` when set (local / docker-compose). If `KUBECONFIG` is not set it falls back to `rest.InClusterConfig()` for pod deployments with a ServiceAccount.

## Processing Logic

### On `query.model:v1` or `retry.task:v1`

```
1. Dedup check via pkg/messageprocessing against message_processing (message_id, stream_name)
   → if already present: skip (ACK without processing)
2. Check cancelled_schedules for the schedule_id
   → if cancelled: commit dedup row and return (no deployment row written)
3. Write deploy intent to executor_deployments (status=pending)
4. Commit (dedup row + deployment row in one transaction)
```

### Deploy dispatcher (every 5 seconds)

`deployer.Dispatcher` polls `executor_deployments` for due rows, capped by the execution slots it reserves:

```
0. Recover one stranded reservation — GetStaleDispatchingForUpdate: a row that has
   held 'dispatching' longer than the recovery window (2 min) repeats its idempotent
   Job create and finishes its transition, keeping its slot reserved throughout

For each due row, up to batchSize (defaults to MAX_CONCURRENT_EXECUTIONS):

Transaction A — reserve:
1. LockCapacity — pg_advisory_xact_lock serializing all capacity accounting
2. ActiveSlotCount — rows WHERE slot_reserved_at IS NOT NULL AND slot_released_at IS NULL,
   counting Jobs-mode and worker-mode work alike
   if active >= MAX_CONCURRENT_EXECUTIONS: stop (pending rows stay pending until next tick)
3. GetDueJobs(1) — a row WHERE status='pending' AND next_attempt_at <= NOW()
   AND execution_mode='jobs'
4. ReserveForDispatch — takes the slot, status 'dispatching'; commit

Kubernetes: create the idempotent Job outside any transaction

Transaction B — record the outcome:
  a. job_params that will not unmarshal, or a row with invalid fields
     → writeFailed → write FAILED outbox rows, RegisterFailure (releases the slot)
  b. Job create result:
     → success:
       - write node_deployed outbox row, naming this row and its mode, so
         k8s-controller status-checks the Job (it never polls) and its terminal
         status can release the slot; every mode emits it, promote_seed included
       - MarkDeployed — the slot stays held until the Job reports terminal
     → transient error AND retry budget remains:
       - RegisterFailure reschedules with exponential backoff (base 5s, cap 2m)
         via next_attempt_at and releases the slot
     → permanent error (errors.Is ErrPermanent) OR retry budget exhausted:
       - writeFailed: write task_status_updated (FAILED) + node_updated (FAILED)
         outbox rows; RegisterFailure marks it failed and releases the slot
```

A reserved slot is released exactly one of two ways: by the aggregate transition
that settles the row (`RegisterFailure`, `RejectBeforeExecution`,
`FailValidation`, `FailSeedBuild`, `FailCompile`, `Complete`,
`MarkRetryPending`, `MarkFailed`, `Cancel`), or — for a Job that actually
launched — by the `executor.job.terminal:v1` event k8s-controller emits when the
Job settles. A Job's row has no aggregate transition of its own at that point,
which is why that stream exists.

A cancelled schedule is the one case where a launched Job's terminal never
arrives: k8s-controller absorbs the status check for it. `ScheduleCancelledHandler`
closes that gap by cancelling the schedule's non-terminal rows itself, so the
release happens through `Cancel` instead. It reads them `FOR UPDATE`, which also
fences the dispatcher: a settle transaction re-reads its row under the same lock
and abandons a dispatch whose row was cancelled while its Job was being created,
rather than writing `deployed` back over the cancellation and re-taking a slot
that no terminal would ever return.

The dispatcher only dispatches `pending` validation rows; `blocked` rows (with unresolved in-set upstreams) remain in place until the `ValidationNodeCompletedHandler` transitions them to `pending`. On a successful K8s validation Job creation, the dispatcher `MarkDeployed`s the row and writes a single `node_deployed` → `node.deployed:v1` outbox row so k8s-controller status-checks the Job — it never polls, so without this trigger the release would hang in `validating`. This is the same single-row deploy announcement the production path now writes; validation rows have no real task/schedule, and k8s-controller suppresses the RUNNING announcement for `mode=validation` Jobs, so no `task_status_updated` row is ever surfaced for them. The per-node terminal outcome (`ok`/`failed`) arrives later via `validation.node.completed:v1`. A validation row that fails AT dispatch (not deployable, or a permanent pre-deploy error) is made terminal via `FailValidation`, records `outcome=failed`, emits no `node.deployed` trigger, skips transitively `blocked` downstreams (marking them `skipped`), and runs the aggregate gate.

### Internal worker API

`executor-controller` serves one HTTP server on `HTTP_PORT` (8084): the `/health`
and `/ready` probes Kubernetes reads, and an internal API worker pods call. The
transport is pull-only — the executor never calls a worker. A worker asks for
work, reports on it, and asks for the URLs it uploads through; nothing is pushed
to a pod.

**Current reach.** A caller authenticates against a row in
`executor_worker_pools`, and the pool reconciler writes those rows for the work
routed to workers. Which work that is comes from `EXECUTION_MODE` and
`EXECUTION_MODE_OVERRIDES_JSON`: with the deployed default (`jobs`, no
overrides), no task is routed to a worker, no pool is registered, the table stays
empty, and every request to the API is answered `401`. Naming a service in the
overrides is what makes the path below live, and only for that service.

| Endpoint | Purpose |
|---|---|
| `GET /internal/v1/worker/runtime` | Signed reads of the pool's runtime descriptor and artifact |
| `POST /internal/v1/workers/claim` | Long-poll for one task in the caller's pool; `204` when none is due. `wait_seconds` of zero or less asks not to wait and is answered by one immediate look for work; anything longer is capped at `WORKER_CLAIM_WAIT` |
| `POST /internal/v1/workers/initialization` | Record or clear the pool's initialization error |
| `POST /internal/v1/leases/{id}/start` | The lease holder's dbt process has started |
| `POST /internal/v1/leases/{id}/heartbeat` | Extend the lease |
| `POST /internal/v1/leases/{id}/result-urls` | Signed uploads for the task's log and run results |
| `POST /internal/v1/leases/{id}/complete` | The lease holder's terminal report |

**Two credentials, two scopes.** A request carries `X-Continuo-Pool-Key` naming a
pool and `Authorization: Bearer <credential>` proving it; `executor_worker_pools`
stores only the credential's SHA-256 digest, compared in constant time, so a read
of the row authenticates nothing. Lease-scoped endpoints additionally carry the
raw lease token in `X-Continuo-Lease-Token`. Neither secret is logged, echoed in
an error body, or persisted.

**Fencing, then ownership.** Every lease-scoped call is fenced on the lease token
first and checked against the authenticated pool second. A caller that does not
hold the lease is told only that its lease is stale — including one naming a
deployment that does not exist at all — so it cannot use the distinction to
discover which tasks exist; only a caller holding the genuine token can learn
that the task belongs to another pool. The pool a claim is served from is read
from the credential, never from the request body.

**Every rejection is settled, not transient.** A worker that gets one of these
stops; none is a signal to retry, and answering any of them `5xx` would leave a
superseded worker retrying against the fence forever.

| Condition | Status | Code |
|---|---|---|
| No valid pool credential | `401` | `unauthenticated` |
| Task belongs to another pool | `403` | `pool_mismatch` |
| Lease no longer current (`ErrStaleLease`), or naming a deployment that does not exist | `409` | `stale_lease` |
| Pool has not hydrated its artifact | `409` | `pool_not_ready` |
| Task was cancelled | `410` | `cancelled` |
| Malformed body or identifier | `400` | `invalid_request` |

`410 cancelled` answers a heartbeat and a terminal report alike. Cancelling a task
releases its slot and deletes the pod running it, which Kubernetes serves with a
termination grace period; a worker that outlives that period still holds the lease
— cancelling keeps it — so whichever call it makes next is answered `410` and it
abandons the task. Both calls are checked after the lease fence, so a caller that
does not hold the lease is answered `409 stale_lease` and learns nothing about the
task.

**Signed URLs are capabilities.** A worker holds no object-store credential. It
reads its artifact and writes its results through URLs the executor signs, each
scoped to one object, one operation, and a 15-minute expiry. Result locations are
derived from the task's own fenced row —
`s3://<bucket>/dbt-runs/<schedule_id>/<task_id>/<lease_id>/{dbt.log,run_results.json}`
— never from the request: a worker that could name its own keys could mint a
capability to write into another schedule's prefix. A terminal report is accepted
only if the artifact URIs it carries are the ones that lease was issued, or none
at all (a worker that failed before uploading still has to report).

**Initialization.** A worker that cannot hydrate its artifact reports the failure
and stays unready rather than crash-looping; the pool records the error and is
handed no work until a later worker reports a clean hydration, which clears it.
Hydration duration is logged as an observation of one pod's startup and is not
stored — it is not part of the pool's state.

### Per-release aggregate gate

The aggregate gate is mode-parametrized. Each mode has its own helper invoked from the same two call sites (dispatcher and node-completed handler), both running inside their own transaction:

- `EmitValidationAggregateIfComplete` handles `mode=validation` and emits the `kind=complete` message on `validation.result:v1`.
- `EmitSeedBuildAggregateIfComplete` handles `mode=seed_build` and emits `seed.build.completed:v1`.
- The compile equivalent handles `mode=compile` and emits `compile.completed:v1`.

Each helper first takes a per-`(release_id, mode)` transaction advisory lock (`LockRelease(release_id, mode)` → `pg_advisory_xact_lock(hashtext(release_id || ':' || mode))`) so the whole count→claim→emit sequence is serialized for that leg; the three legs of one release lock independently. The second caller for any leg blocks until the first commits and then evaluates against its committed state. This closes the lost-emission window where the dispatcher's `FailValidation`-at-dispatch path and the node-completed handler (or two replicas) bring the last two nodes terminal in overlapping transactions and, under READ COMMITTED, each reads the other as still pending and both no-op — hanging the release.

Under the lock the gate is a no-op while `PendingValidationCount(release_id, mode) > 0`. `PendingValidationCount` counts rows for the given `(release_id, mode)` pair that are not yet terminal: `pending`, `blocked`, and `deployed` are all non-terminal. Once every node for the leg is terminal (`outcome` is `ok`, `failed`, or `skipped`) the gate claims the `validation_aggregates` sentinel via `ClaimEmission(release_id, mode)` (`INSERT … ON CONFLICT DO NOTHING`); the single winner reads the per-node results to compute the aggregate status, builds the decision payload, and writes one outbox row whose `aggregate_id` is a deterministic UUIDv5 over an immutable namespace and `release:<release_id>:<mode>`, so any re-emission deduplicates downstream. For the validation leg the payload is the decision only (no per-node array); each node's content was already streamed as it settled via the `kind=node` messages on the same `validation.result:v1` stream. The `kind=complete` payload also includes `candidate_schema` so the teardown consumer knows which schema to drop. The advisory lock guarantees exactly-once (never zero); the sentinel guarantees exactly-once (never double). Losers return without emitting.

### Outbox processor (every 5 seconds, batch of 100)

`pkg/outbox.Processor` polls `executor_outbox` and calls `OutboxPublisher.Publish` per entry. The publisher marshals the payload and performs a single Redis XADD. There is no `TerminalFailureHook` — terminal outcomes are written as ordinary outbox rows by the dispatcher.

```
For each pending executor_outbox entry:
  1. Marshal payload (already a typed event struct)
  2. XADD to the row's stream_name
  3. MarkProcessed

On XADD failure:
  - retry_count < max_retries: retry on next poll
  - retry_count >= max_retries: MarkFailed (the deployment row is already failed/deployed)
```

## Background Loops

| Loop | Description |
|---|---|
| Redis consumers | Reads `query.model:v1`, `retry.task:v1`, `schedule.cancelled:v1`, `executor.job.terminal:v1`, `validation.requested:v1`, `validation.node.completed:v1`, `validation.result:v1` (`kind=complete`, for candidate schema teardown), `seed.build.requested:v1`, `seed.build.node.completed:v1`, `compile.requested:v1`, and `compile.node.completed:v1` via `pkg/redis.StreamConsumer`; crash-recovery for pending messages on startup |
| Deploy dispatcher (`deployer.Dispatcher`) | Polls `executor_deployments` every 5 seconds; creates K8s Jobs and writes outbox announcement rows, capped by `MAX_CONCURRENT_EXECUTIONS` |
| Outbox processor (`pkg/outbox.Processor`) | Polls `executor_outbox` every 5 seconds; processes up to 100 entries per batch via `OutboxPublisher` (uniform marshal-and-XADD) |
| Worker lease reaper (`reaper.Reaper`) | Polls `executor_deployments` every 10 seconds for `leased`/`running` rows whose `lease_expires_at` has passed; deletes the pod that held each expired lease, drops the lease, and either parks the task for another attempt or fails it permanently |
| Worker pool reconciler (`pool.Reconciler`) | Every 5 seconds: registers a pool for each identity the waiting worker-routed work names, shares `MAX_CONCURRENT_EXECUTIONS` between the eligible pools, and asks the runtime for each pool's Deployment, Secret, and replica count. A failed pass is logged and the next one repairs it. With every service on the Jobs path nothing is routed to a worker, so no identity is ever named and the loop finds nothing to do |

## Reliability Patterns

- **Inbound dedup**: `message_processing` keyed on `(message_id, stream_name)` prevents double-processing of duplicate Redis messages; managed by `pkg/messageprocessing.Dedup`
- **Decoupled command queue**: inbound handlers write only a `pending` row to `executor_deployments` (a pure Postgres write, no Kubernetes I/O); the K8s deploy happens asynchronously in the dispatcher, keeping the Unit-of-Work transaction free of external side effects
- **Explicit transaction boundary**: each inbound message runs dedup + deployment row insert inside a single Unit-of-Work transaction; the dedup row and deployment intent are committed atomically
- **Concurrency cap**: capacity is durable state, not an observation of Kubernetes. A deployment reserves an execution slot (`slot_reserved_at`) before its Job is created and holds it until the work settles; `deployer.Dispatcher` reserves under a `pg_advisory_xact_lock` so the count it reads cannot be stale by the time it takes the slot. Jobs-mode and worker-mode work draw from the one `MAX_CONCURRENT_EXECUTIONS` budget, so a slot held by a worker lease throttles the Jobs path exactly as a Job's own does; rows beyond the cap stay `pending` until the next tick
- **K8s idempotency**: `CreateQueryJob` treats already-exists as success; a dispatcher restart or crash after K8s success but before commit will re-attempt safely
- **Dispatch crash recovery**: a crash between reserving a slot and recording the Job leaves the row in `dispatching` still holding its slot — deliberately, because its Job may be running. Each tick re-drives one such row past a 2-minute recovery window, repeating the idempotent create and finishing the transition; the slot is never released to be re-reserved while the Job it accounts for may still be live
- **Dispatcher backoff**: transient K8s failures reschedule the row via `next_attempt_at` with exponential backoff (base 5s, cap 2 min); the row stays `pending` and is retried on the next tick when due
- **Worker crash recovery**: a lease is a worker's promise to keep heartbeating. When the deadline passes, the reaper deletes the pod by name *and* UID (so a pod the pool has already replaced under the same name is untouched), drops the lease, and applies the transition — `retry_pending` when the task has attempts left, `failed` when its last attempt was the one that went silent. The transition releases the execution slot, so a crashed pod cannot cost the executor a slot permanently. The pod deletion is requested inside the transaction and its failure rolls the recovery back: the lease it could not fence stays authoritative, no other worker can claim the task, and the next tick tries again
- **Worker fencing**: the paths that take a task away from a live worker stop its pod as well as fence its reports. A reaped lease is dropped, so every later report from that worker is answered `409 stale_lease`; a cancelled task keeps its lease, so a worker outliving the pod deletion's grace period is answered `410 cancelled` on its next heartbeat, which is its notice to abandon the task. Both are terminal for the worker, which never retries against either
- **Terminal failure propagation**: on `ErrPermanent` or retry-budget exhaustion, the dispatcher writes `task_status_updated` FAILED + `node_updated` FAILED as ordinary `executor_outbox` rows before marking the deployment `failed` — ensuring orchestrator and state always learn of the terminal outcome
- **Uniform outbox publisher**: the executor `OutboxPublisher` is a plain marshal-and-XADD; it carries no K8s deploy logic and has no `TerminalFailureHook`; all failure signalling is handled upstream by the dispatcher
- **No state gRPC dependency**: executor-controller does not call state gRPC; task status updates flow via `task.status.updated:v1`
- **Task max retries**: `task_max_retries` written into `executor_deployments.job_params` defaults to 2 (3 total execution attempts: initial + 2 retries); propagated to `k8s-controller` via `node.deployed:v1` to govern task-level retry logic
