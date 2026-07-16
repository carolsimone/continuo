# Parse-free per-node dbt execution with reusable workers

**Date:** 2026-07-16

**Status:** Approved design

**Scope:** Production per-node dbt execution. Release compile and validation
remain Kubernetes Jobs.

## Problem

Continuo merges multiple team-owned dbt projects into one Neo4j DAG, then
dispatches each ready node independently. Today each `query.model:v1` event
becomes a Kubernetes Job that runs a command such as:

```text
dbt run --select my_model
```

That command starts a pod and Python process and asks dbt to build a project
manifest before it can resolve the selector. The manifest build is
all-or-nothing even though Continuo needs only one node. For small models, pod
startup and parsing dominate the useful work.

The release pipeline has already compiled the exact team image before
promotion. The compile output contains both `manifest.json` and dbt's internal
`partial_parse.msgpack`, but Continuo currently uploads only `manifest.json`.

Executing `compiled_code` directly is not correct. It contains the model query,
not the runtime materialization wrapper. dbt must still run the materialization
macro so incremental models perform their normal target-existence checks,
`merge`, `insert overwrite`, schema reconciliation, hooks, and adapter
dispatch.

## Goals

- Do not build a dbt project manifest for every production node.
- Do not create a Kubernetes pod for every production node.
- Keep dbt as the source of truth for materializations, including incremental
  models.
- Preserve every command defined by `dbt-commands.yaml`, including custom team
  wrappers such as `wise-dbt`.
- Execute parallel DAG branches in separate worker pods.
- Keep concurrency configurable; `50` is a deploy-time default, not an
  algorithmic constant.
- Preserve image and manifest pinning so a rerun uses the release artifacts
  captured by its source run.
- Require little team effort and leave existing batch-image behavior unchanged.

## Non-goals

- Precomputing or hand-writing CTAS, merge, or schema-change SQL.
- Removing dbt or materialization macro evaluation from runtime.
- Running multiple dbt invocations concurrently inside one process.
- Replacing the Neo4j DAG scheduler or its dependency-unlock logic.
- Moving compile, seed-build, or candidate validation to reusable workers in
  the first implementation.
- Requiring a team image to expose an HTTP server.

## Chosen architecture

Continuo will use pull-based, reusable Kubernetes worker Deployments.

The `executor-controller` remains the scheduler for physical execution. A
worker pool is identified by:

```text
service_name + image_tag + runtime_manifest_sha256
```

Each worker pod:

1. starts the Continuo worker command supplied by the shared dbt base image;
2. downloads and verifies the promoted runtime manifest;
3. hydrates that manifest once in its Python process;
4. polls the executor's internal lease API;
5. executes one leased task at a time;
6. reports the result and returns to polling.

The pod does not terminate when a node completes. It terminates only because of
idle scale-down, rollout, cancellation, failure, or Kubernetes eviction.
Parallelism comes from multiple pods, never concurrent `dbtRunner` calls in one
pod.

The worker has no inbound application endpoint. It is an outbound client of
`executor-controller`. Teams therefore do not add a server, controller, or POST
handler to their projects.

The control plane remains Go. The shared dbt base image contains a small Python
worker runtime because dbt's `dbtRunner` and Manifest APIs are Python APIs. This
runtime is not a new independently deployed microservice and adds no dependency
beyond Python/dbt already present in the image.

## Release artifact contract

### Compile output

The existing team-image compile Job remains authoritative. After the configured
compile command succeeds, its shared hand-off volume contains:

- `manifest.json` — used by `manifest-controller` to build the candidate DAG;
- `partial_parse.msgpack` — used by production workers as the hydrated dbt
  Manifest;
- `runtime-manifest.json` — a small Continuo descriptor for the msgpack file.

The S3 layout remains release- and service-scoped:

```text
s3://<bucket>/<service>/<release_id>/manifest.json
s3://<bucket>/<service>/<release_id>/partial_parse.msgpack
s3://<bucket>/<service>/<release_id>/runtime-manifest.json
```

The descriptor contains:

```json
{
  "format": "dbt-partial-parse-msgpack-v1",
  "service_name": "finance",
  "release_id": "release-id",
  "image_tag": "image-tag",
  "artifact_uri": "s3://bucket/finance/release-id/partial_parse.msgpack",
  "sha256": "hex-digest",
  "dbt_core_version": "exact-version",
  "adapter_type": "postgres",
  "parse_context_sha256": "hex-digest"
}
```

`parse_context_sha256` covers parse-affecting runtime inputs that are not fixed
by the image: dbt vars, target name/type/database/schema, and the values of
parse-time environment variables recorded by dbt. Plaintext secrets are not
stored. The worker recomputes the digest before accepting work. A mismatch is a
closed failure, not permission to parse the project again.

The artifact is tied to the exact team image and exact dbt Core version.
`partial_parse.msgpack` is an internal dbt format, so cross-version loading is
explicitly unsupported.

### Custom compile paths

`dbt-commands.yaml` remains authoritative for the compile command and
`manifest_path`. Its `compile` block gains an optional
`partial_parse_path`. When omitted, Continuo derives
`<dirname(manifest_path)>/partial_parse.msgpack`.

Standard dbt projects and wrappers that retain dbt's normal target directory
need no configuration change. A wrapper that moves the msgpack elsewhere adds
one path; its command remains unchanged.

### Upload and propagation

The compile uploader hashes and uploads both artifacts and their descriptor.
`manifest-controller` continues to parse only `manifest.json`; it reads the
small descriptor and includes one runtime-manifest reference per service in its
candidate result.

On promotion, `release-controller` adds the service artifact map to the existing
`release.promoted:v1` payload. The orchestrator stamps the matching runtime
reference onto each promoted topology node.

`release-controller` stores the runtime reference with each service's
`current_prod` record, alongside its image and manifest URI. Incremental
releases therefore carry forward the correct artifact for unchanged services
instead of accidentally binding them to the changed service's artifact.

Binary manifest content never travels through Redis or Neo4j. Only its URI,
digest, version, and context digest do.

Runtime artifacts must be retained for at least as long as the release/run
history that can be rerun.

## Runtime pinning and event data

The following fields are added to the promoted topology, run snapshot,
`query.model:v1` task payload, and `retry.task:v1` retry payload:

- `dbt_unique_id`;
- `runtime_manifest_uri`;
- `runtime_manifest_sha256`;
- `runtime_manifest_dbt_version`;
- `runtime_manifest_parse_context_sha256`.

The orchestrator stores the current values on topology nodes. When it creates a
run snapshot, it copies them to the run's `:EXECUTES` edge alongside
`image_tag` and `manifest_version`. All later dispatches use the pinned edge
values, never the latest topology values.

Therefore:

- promotion changes only the current topology pointers;
- promotion does not start worker pods;
- a run in progress is unaffected by a later promotion;
- a rerun of a post-migration run can recreate the old pool from its old image
  and manifest reference.

The new event fields are additive. Old messages and historical runs without a
runtime reference remain valid but must use the legacy Job path.

## Command compatibility and execution paths

`dbt-commands.yaml` remains the sole source of command syntax. The executor
resolves the same argv it resolves today and persists that argv on the first
lease attempt so retries cannot change command midway through a task.

Each lease contains both the exact dbt `unique_id` and the resolved argv. The
worker verifies that the unique ID exists in its loaded Manifest and identifies
one node before execution. Schema/table fields remain available for existing
environment-variable and wrapper contracts.

### Native dbt path

When the resolved executable is dbt itself, the worker removes only the
executable token and passes the remaining argv unchanged to:

```python
dbtRunner(manifest=loaded_manifest).invoke(argv_without_dbt)
```

Selector resolution uses the supplied Manifest. dbt does not call its project
parser. It still initializes runtime configuration and executes the normal dbt
task, adapter calls, hooks, materialization macros, and warehouse introspection.
This is what preserves incremental behavior without reconstructing SQL.

The loaded Manifest is retained for the pod lifetime and Continuo never alters
it. Invocations are sequential, and task-specific dbt/runtime state is
discarded after each invocation. Task-specific environment variables are
installed for the invocation and then restored so values cannot leak to the
next task. If a dbt command leaves the shared runtime in an unsafe state, the
worker exits and lets the Deployment replace it.

The direct path removes:

- per-node Kubernetes Job/pod startup;
- per-node Python/dbt process startup;
- per-node project parsing.

Adapter connection reuse is left to dbt and is not a correctness or performance
assumption.

### Custom-wrapper path

If the resolved executable is not dbt, the worker launches the exact configured
argv as a child process:

- same image and working directory;
- same task and warehouse environment supplied by current Jobs;
- no shell rewriting or argument translation by Continuo;
- stdout/stderr captured per task;
- process tree terminated on cancellation.

For dbt Core-backed wrappers, the worker additionally injects:

```text
DBT_PARTIAL_PARSE_FILE_PATH=<task-local-copy-of-promoted-msgpack>
DBT_LOG_FORMAT=json
```

The task-local copy prevents a child dbt process from mutating the pool's
canonical artifact. dbt may perform its normal cheap cache validation, but it
must not silently fall back to a full parse. A structured dbt cache-rejection
event makes the worker terminate the child and report
`runtime_manifest_rejected`.

A wrapper that delegates to dbt Core therefore keeps its exact interface while
using the prebuilt manifest cache. It still pays child-process startup. A
wrapper implemented by another engine runs unchanged; it has no dbt parse to
skip.

An opaque wrapper that suppresses dbt events cannot be claimed as parse-free
until its integration test proves cache use. It remains functionally supported
in worker compatibility mode, or can remain on Jobs during rollout. Continuo
does not pretend an arbitrary external process shares the in-memory Python
Manifest.

## Executor lease model

Production `query.model:v1` and `retry.task:v1` handlers continue to create an
`executor_deployments` command record. Worker-mode records progress through:

```text
pending -> leased -> running
                    |-> succeeded
                    |-> retry_pending -> pending
                    |-> failed
                    |-> cancelled
```

A lease stores:

- deployment/task/schedule identity;
- pool identity;
- owner worker and pod identity;
- unguessable lease token;
- attempt number;
- lease expiry and last heartbeat;
- resolved argv and execution path;
- start/finish timestamps and terminal result.

The repository selects ready rows with `FOR UPDATE SKIP LOCKED`. A serialized
Postgres capacity reservation makes the global concurrency check and lease
creation atomic. Concurrent worker claims cannot lease the same task or exceed
the configured limit.

The executor exposes a versioned internal JSON/HTTP API containing:

- claim or long-poll for the authenticated pool;
- acknowledge-start;
- heartbeat;
- complete;
- request short-lived artifact/log URLs.

No worker receives Redis, Postgres, or S3 credentials. The executor creates a
pool-scoped credential when reconciling the Deployment and stores only the
verification form outside Kubernetes. The credential can claim and report only
for its pool. It is not inherited by the wrapper child environment.

Lease mutations use the lease token as a compare-and-set fence. Duplicate
start, heartbeat, and completion requests are idempotent. A stale worker whose
lease has expired cannot update the task.

## Completion and existing lifecycle events

Worker completion does not introduce a second business lifecycle. In one
executor database transaction, acknowledge-start records `running` and writes
`task.status.updated:v1` with `RUNNING` exactly once for that attempt.

Completion records the attempt outcome and uses the same fan-out rules as
`k8s-controller`:

- success and permanent failure write `task.status.updated:v1`,
  `task.execution.recorded:v1`, and `node.updated:v1`;
- retryable failure writes `task.status.updated:v1`,
  `task.execution.recorded:v1`, and `retry.task:v1`, without prematurely
  unlocking downstream nodes.

The retry event moves the worker deployment through `retry_pending` and back to
`pending` after the existing backoff. All transitions and outbox rows are
idempotent by task attempt.

The state service remains authoritative for task/run transitions. The
orchestrator continues to unlock downstream DAG nodes from `node.updated:v1`.

`k8s-controller` continues to own Job observation for compile, validation,
seed-build, promote-seed, and services still in legacy Job mode. It does not
monitor work performed inside persistent worker pods.

dbt structured events, stdout/stderr, and `run_results.json` are isolated by
task. Workers obtain short-lived presigned URLs from the executor and upload
without an S3 SDK or credentials. Upload retry is separate from dbt execution:
a transient log-upload failure must not rerun a successful warehouse mutation.

## Concurrency and pool scaling

`MAX_CONCURRENT_EXECUTIONS` is the authoritative configurable execution budget.
Docker Compose and Helm set its deployed default to `50`. Service configuration
requires a positive value, and scaling calculations never use a literal `50`.

An execution slot is consumed by:

- a leased/running worker task; or
- a reserved/running legacy, compile, validation, seed-build, or promote-seed
  Job.

Idle worker pods consume no execution slot. This keeps mixed-mode canaries from
exceeding the same warehouse-facing budget. `MAX_CONCURRENT_JOBS` is accepted
as a transition alias, then removed after deployment configuration has moved to
the new name.

Every Job dispatch first reserves a slot in Postgres and stamps its
`executor_deployment_id` in the Job annotations. `k8s-controller` emits a
capacity-only `executor.job.terminal:v1` notification containing that ID when
any observed Job terminates, including fire-and-forget promote-seed Jobs. The
executor consumes that notification idempotently and releases the reservation.
Existing business-lifecycle events remain unchanged; this stream exists only
to keep the shared execution budget accurate.

The pool reconciler runs inside `executor-controller`:

1. group ready production records by pool key;
2. allocate available global slots oldest-ready-first across pools;
3. create or update the matching Kubernetes Deployment;
4. set each desired replica count to active leases plus that pool's allocated
   pending slots;
5. never reduce a pool while it is busy;
6. scale a pool to zero only when it has no pending or active lease for the
   configured idle timeout.

The executor does not reduce a pool while it contains an active lease, avoiding
Kubernetes choosing a busy pod for scale-down. A later task can scale a
zero-replica old pool back up from its pinned metadata.

Configuration:

- `EXECUTION_MODE=jobs|workers` — global default, initially `jobs`;
- `EXECUTION_MODE_OVERRIDES_JSON` — validated service-to-mode canary overrides;
- `MAX_CONCURRENT_EXECUTIONS` — global execution limit;
- `WORKER_IDLE_TIMEOUT_SECONDS` — zero-scale delay;
- `WORKER_LEASE_TTL_SECONDS` and `WORKER_HEARTBEAT_INTERVAL_SECONDS` — lease
  timing, validated so several heartbeats fit inside one TTL.

## Worker readiness and artifact validation

On pod startup, the worker authenticates to the executor, obtains a fresh
presigned GET URL, and downloads the msgpack into pod-local storage. This
avoids embedding an expiring S3 URL in a zero-replica Deployment.

Before becoming ready, it verifies:

- SHA-256;
- descriptor format;
- service and image binding;
- exact installed dbt Core version;
- adapter type;
- parse-context digest;
- successful Manifest hydration;
- presence of nodes for the pool's service/package.

Readiness is an exec/file probe; no inbound worker server is required.
Each claimed `dbt_unique_id` is validated separately before invocation.

Any mismatch keeps the pod unready and reports a permanent pool initialization
error. Continuo never responds by reading project files and building a new
Manifest. The operator can switch that service to `jobs` and rerun while the
artifact/configuration problem is corrected.

## Failure, retry, and cancellation semantics

- **dbt failure:** report the same failure data and use the existing
  task-retry/backoff policy.
- **worker or pod crash:** the lease expires; executor fences the token,
  terminates the old pod if it still exists, then requeues using the existing
  retry budget.
- **heartbeat loss:** handled like a crash. Requeue happens only after fencing
  and pod termination has been requested.
- **artifact corruption, version mismatch, or parse-context mismatch:** fail
  closed as a permanent runtime-manifest error.
- **wrapper cache rejection:** stop the child and fail explicitly; never
  continue silently with a full parse.
- **cancellation before claim:** mark the pending deployment cancelled.
- **cancellation while active:** fence the lease and delete its worker pod. The
  Deployment creates a replacement idle pod if the pool still has work.
- **late/duplicate completion:** ignored through token and terminal-state
  idempotency.

Warehouse execution remains at-least-once, as it is with retried Jobs today. A
pod can lose connectivity after the warehouse accepted work but before
completion was recorded. Fencing prevents two workers from reporting the same
attempt; it cannot make arbitrary warehouse statements exactly-once.

An unhandled in-process dbt error that may have contaminated global state makes
the worker exit after reporting/fencing its lease. The replacement pod hydrates
a clean runtime.

## Team-image contract

The next version of `dbt-base` installs the worker command at a fixed path. Its
existing `ENTRYPOINT` and normal batch behavior remain unchanged. Kubernetes
selects worker mode by overriding the pod command, exactly as it already
overrides the command for per-node Jobs.

For a standard project, the team effort is:

1. inherit/rebuild from the new base image;
2. publish the normal release image.

No project source, server, wrapper, or Docker entrypoint change is required.
Only wrappers that move `partial_parse.msgpack` away from the manifest directory
add `partial_parse_path`.

The worker uses Python/dbt dependencies already installed in the image and no
new heavy runtime package. In `jobs` mode the worker code is dormant, so current
container startup and execution behavior do not change.

## Rollout and backward compatibility

1. Ship artifact production and optional event fields while every service
   remains in `jobs` mode.
2. Ship the worker runtime, lease API, and pool reconciler behind
   `EXECUTION_MODE`.
3. Qualify a standard dbt service in worker mode.
4. Qualify a dbt Core-backed custom wrapper, including proof that it consumes
   the supplied cache.
5. Expand the per-service allowlist, then set the global production mode to
   `workers`.

Rollback is configuration-only: set the affected service back to `jobs`. There
is no automatic fallback from a failed worker attempt to a full parse, because
that would hide the invariant and can duplicate warehouse work.

Releases compiled before the runtime-artifact contract and runs snapshotted
without the new fields use Jobs. New releases may be promoted without a runtime
artifact only while their service remains configured for Jobs. Enabling worker
mode for a service with an incomplete artifact produces an explicit permanent
dispatch error; the executor cannot validate release-specific metadata at
process startup.

## Observability and performance gates

Metrics:

- pending records and oldest-ready age by pool;
- desired/ready worker replicas;
- active execution slots and configured limit;
- claim latency, lease wait, heartbeat expiry, and retries;
- artifact download and Manifest hydration duration;
- native versus wrapper execution count;
- partial-cache accepted/rejected count;
- ready-to-dbt-start latency;
- dbt execution and result-upload duration;
- scale-up and scale-to-zero count.

Structured logs include pool key, task ID, attempt, lease ID, execution path,
and pinned artifact digest without exposing credentials or presigned URLs.

The implementation is accepted only when:

- a warm native task creates no Kubernetes Job or pod;
- the native-path Manifest is hydrated once per worker process;
- a test that makes dbt project parsing fatal still completes native tasks;
- qualified dbt Core wrappers demonstrate cache acceptance and fail on cache
  rejection;
- parallel ready DAG branches run in separate pods;
- active executions never exceed `MAX_CONCURRENT_EXECUTIONS` under concurrent
  claims and mixed Job/worker mode;
- idle pools scale to zero and later restart;
- table, view, seed, snapshot, incremental first-run, incremental existing-run,
  test, and build semantics match the Job path;
- cancellation, lease expiry, retry, stale completion, promotion during a run,
  and pinned old-release rerun behave correctly;
- existing `jobs` mode resolves identical argv and passes its existing e2e
  suite;
- in the Kind benchmark, warm native lease-acceptance-to-`dbtRunner.invoke`
  latency is at most one second at p95 and at least 80% below the current
  Job-path p95;
- benchmark output also records cold worker and qualified-wrapper
  ready-to-dbt-start latency before broad rollout.

The base-image change must not start an extra process in batch mode or introduce
a new third-party runtime dependency. This is the concrete guard against
degrading team-owned batch containers.

## Testing strategy

### Unit tests

- runtime descriptor/path derivation, hashing, and mismatch classification;
- command classification and exact argv preservation;
- unique-ID lookup and zero/multiple-match rejection;
- lease aggregate transitions, token fencing, idempotency, and backoff;
- atomic capacity reservation and configurable limit;
- pool-key generation and desired-replica calculation;
- task environment install/cleanup;
- native runner invocation with the parser replaced by a failing stub;
- wrapper subprocess environment, log capture, cache rejection, and
  cancellation.

### Integration tests

- compile Job uploads all three artifacts for default and custom compile paths;
- manifest-controller and release-controller propagate runtime references;
- orchestrator topology and snapshots pin the new fields;
- many concurrent Postgres claims neither duplicate a task nor exceed the cap;
- fake-Kubernetes reconciliation creates separate pools and scales to zero;
- a real dbt fixture covers view/table/incremental/seed/snapshot/test/build;
- the deployed `wise-dbt` fixture executes unchanged and proves cache use;
- worker completion produces the same three lifecycle streams as a Job.
- the capacity-only Job terminal event releases a reservation exactly once.

### End-to-end tests

- a parallel DAG uses multiple reusable pods;
- sequential nodes reuse a pod and hydrate one Manifest;
- no production Job exists for worker-mode tasks;
- pod crash, heartbeat expiry, cancellation, retry, and stale completion;
- release promotion during a run and rerun from the pinned old release;
- idle scale-to-zero followed by successful cold restart;
- legacy Job mode and pre-migration run fallback.

Tests run in the repository's Docker/Kind environment, followed by the complete
`tests/e2e/README.md` procedure before the implementation branch is finished.

## Architecture documentation changes

Implementation must reconcile:

- `docs/arch/01-topology.md` — runtime-manifest and dbt unique-ID properties;
- `docs/arch/02-interaction-matrix.md` — worker-to-executor API and completion
  ownership;
- `docs/arch/03-sequence-flows.md` — release artifact, worker dispatch,
  cancellation, retry, and scale-to-zero sequences;
- `docs/arch/04-service-ownership.md` — executor lease/pool ownership and the
  reduced production role of k8s-controller;
- `docs/arch/streams.md` — additive fields and executor as a producer of the
  existing lifecycle streams;
- relevant `docs/arch/services/*` pages for executor-controller,
  release-controller, manifest-controller, orchestrator, and k8s-controller.

No implementation phase is complete until these files match the delivered
behavior.

## Delivery slices

The implementation plan should keep deployable checkpoints:

1. runtime artifact upload, descriptor, propagation, and pinning;
2. executor lease domain, database migration, capacity reservation, and
   internal API;
3. base-image worker runtime and native dbt execution;
4. custom-wrapper cache path and result artifacts;
5. Kubernetes pool reconciler, cancellation, and scale-to-zero;
6. canary configuration, observability, benchmarks, e2e, and final architecture
   reconciliation.

Every slice keeps `jobs` mode operational. Worker mode is enabled only after the
artifact, runtime, and controller parts for that service are present.
Each slice updates the architecture pages affected by the behavior it
introduces; documentation is not deferred to the final slice.

## Alternatives retained

1. **Per-node Jobs with `partial_parse.msgpack`.** Smallest change and useful as
   the compatibility fallback, but every node still pays Kubernetes and process
   startup.
2. **Push-based reusable workers.** The executor sends tasks to worker
   endpoints. This adds an inbound server to team pods plus service discovery,
   routing, and backpressure, with no execution advantage over pull leases.
3. **Precomputed fully wrapped SQL.** Removes dbt from runtime but requires
   harvesting both incremental branches, schema guards, invalidation, and
   fallback. It has the highest correctness and maintenance risk.
4. **dbt Fusion.** Faster parsing can complement either Jobs or workers, but it
   does not eliminate per-node pod startup or make execution parse-free.
5. **Cooperative wrapper shim/local protocol.** A future wrapper SDK could send
   its translated dbt argv to the already-running worker process and share the
   in-memory Manifest. This improves wrapper startup further but requires a
   small team wrapper change and is unnecessary for the first version.
6. **Permanent legacy Jobs for incompatible images.** A supported operational
   escape hatch when an image cannot inherit the worker-enabled base or a
   wrapper cannot prove cache use; it is not the preferred steady state.
