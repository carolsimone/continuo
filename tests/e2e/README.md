# E2E System Test

Comprehensive end-to-end test validating the complete Continuo orchestration pipeline.

## Test Overview

The suite covers both the run-orchestration pipeline and the dbt blue/green release pipeline. The happy-path test exercises a 6-node `ftable_*` DAG through every service:
- `state` - Plans and creates runs, dispatches ready tasks, owns the run/task lifecycle
- `orchestrator` - Applies each promoted release to the Neo4j topology and serves topology reads and the node catalog (nodes, code versions, precedents)
- `execution-controller` - Deploys k8s jobs and monitors their status
- `topology-controller` - Parses dbt manifests (candidate + legacy paths)
- `release-controller` - Owns the blue/green candidate-release lifecycle and `current_prod`
- `ui` - HTTP API verified to return correct scheduler/task/release data

### DAG Layout

```
ftable_a (service-1)  ftable_b (service-1)
              \           /
           ftable_c (service-3)
          /                    \
ftable_d (service-2)   ftable_e (service-2, FAILS)
          \                    /
           ftable_f (service-3)  <- never deployed in failure path
```

## From Blank State (first run / full reset)

Use this when starting from a completely pruned Docker environment.

```bash
# 1. Full reset — wipes all containers, images, volumes, and the kind cluster
make nuke

# 2. Build the Go base image
DOCKER_BUILDKIT=1 docker build -t continuo-base:latest -f Dockerfile.base .

# 3. Bootstrap everything in the background:
#    - Creates the kind cluster
#    - Builds all docker-compose service images (including dbt-base)
#    - Builds and loads dbt service images into kind
#    - Compiles dbt manifests (requires postgres to be up briefly)
#    - Writes kubeconfigs into service directories
#    - Starts docker-compose in detached mode
bash scripts/setup.sh

# 4. Start Go service processes inside containers and wait for health
bash tests/e2e/start-services.sh

# 5. Start the ui container
docker compose up -d ui

# 6. Deploy pre-built controller images into the kind cluster
bash tests/e2e/deploy-k8s-controllers.sh

# 7. Run the tests
docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator \
  go test -v -count=1 -timeout 125m /app/tests/e2e/...

# 8. Clean up k8s resources
bash tests/e2e/cleanup-k8s-controllers.sh
```

> `setup.sh` takes ~10 minutes on first run.

## Subsequent Runs

When the kind cluster and all Docker images already exist:

```bash
# Tear down stale containers — including any left over from the main project directory
# (the main project uses bare container names like /state, /ui that conflict with this worktree)
docker rm -f state orchestrator execution-controller ui 2>/dev/null || true
docker compose down

# Start (or restart) all docker-compose services
docker compose up -d

# Wait for neo4j to be healthy (can take up to 90s)
docker compose ps   # neo4j should show "(healthy)"

# Start Go service processes and wait for health
bash tests/e2e/start-services.sh

# Start ui
docker compose up -d ui

# If any service images changed since last run, rebuild and reload into kind:
bash tests/e2e/provision-k8s-test-env.sh
# Otherwise, if images are unchanged, just (re)deploy:
# bash tests/e2e/deploy-k8s-controllers.sh

# Run the tests
docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator \
  go test -v -count=1 -timeout 125m /app/tests/e2e/...

# Clean up k8s resources
bash tests/e2e/cleanup-k8s-controllers.sh
```

Or use the convenience target (assumes kind cluster and images already exist):

```bash
make e2e-full
```

`make e2e-full` runs `docker compose up -d`, waits for health, starts services,
runs `provision-k8s-test-env.sh`, then executes the tests.

## Auth e2e suite (OIDC against Dex)

The auth suite exercises the real OIDC login flow (ui in `oidc` mode
against a Dex identity provider). It is skipped unless `UI_AUTH_HTTP_BASE` is
set.

```bash
# Requires the standard stack to be up (state, orchestrator, redis, …).
docker compose build ui-auth
docker compose --profile auth-e2e up -d dex ui-auth

docker exec -e UI_AUTH_HTTP_BASE=http://ui-auth:8090 orchestrator \
  go test -v -run TestAuthOIDC /app/tests/e2e/...

docker compose --profile auth-e2e down dex ui-auth
```

Static users (password `password`): `operator@example.com` (operator role via
email override), `viewer@example.com` (viewer), `norole@example.com` (no role —
denied). Dex static users carry no groups claim; the groups-mapping path is
covered by ui unit tests.

## Script Reference

| Script | What it does |
|--------|-------------|
| `scripts/setup.sh` | Full bootstrap: kind cluster + image builds + dbt manifests + kubeconfig + compose up |
| `tests/e2e/start-services.sh` | Starts Go service processes inside containers (`go run`), waits for HTTP health |
| `tests/e2e/provision-k8s-test-env.sh` | Rebuilds controller + dbt images, loads them into kind, regenerates kubeconfig, deploys k8s manifests |
| `tests/e2e/deploy-k8s-controllers.sh` | Deploys pre-built images already in kind (no rebuild); used by CI after `setup.sh` |
| `tests/e2e/cleanup-k8s-controllers.sh` | Removes the controller Deployment from the kind cluster |

### How CI runs it

CI mirrors the blank-state flow above:

1. `bash scripts/setup.sh` (with `CI=true` set automatically by GitHub Actions)
2. Wait for containers to be running
3. Build + start `state` and `orchestrator` binaries (gRPC health check)
4. Run per-service unit tests
5. Build + start `topology-controller`
6. `docker compose up -d ui`
7. `bash tests/e2e/deploy-k8s-controllers.sh` (images pre-loaded by setup.sh)
8. `docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator go test -v -timeout 125m /app/tests/e2e/...`
9. `bash tests/e2e/cleanup-k8s-controllers.sh`

CI uses `deploy-k8s-controllers.sh` (not `provision-k8s-test-env.sh`) because
`setup.sh` already built and loaded all images into kind.

## UI Dashboard

After a successful test run the dashboard at **http://localhost:8090** shows live data:

```bash
open http://localhost:8090
```

You should see:
- 1 scheduler card with a **succeeded** badge
- A task table with **6 tasks** (the `ftable_*` DAG), each with **succeeded** status
- ISO-8601 timestamps showing when each task ran

## Architecture

The E2E test uses a hybrid setup:

**In docker-compose:**
- PostgreSQL, Redis, Neo4j (data stores), MinIO (S3 — dbt manifests)
- state, orchestrator, topology-controller, release-controller (services)
- egress-proxy (forward proxy for the outbound HTTP of agent-chat and agent-remediation; they reach their LLM and GitHub only through it, and reach the stack's own services directly via `NO_PROXY`). `start-services.sh` starts it before agent-remediation: health `:8093/ready`, proxy port `:3128`. Its allowlist is `tests/e2e/config/egress-allowlist.yaml`, which admits `stub-llm`, `stub-github` and the public LLM and GitHub APIs

**In kind cluster (Kubernetes):**
- execution-controller (Deployment)
- dbt service jobs (run as k8s Jobs, one per table)

The controller in kind connects to docker-compose services via docker bridge network (172.17.0.1).

## Test Structure

| File | Purpose |
|------|---------|
| `system_test.go` | `TestE2E_HappyPath_FullDAGExecution` — 6-node `ftable_*` DAG through every service |
| `trigger_test.go` | `TestTriggerSchedule_SeedRunAndRerun` — trigger seed schedule via the ui's `POST /api/schedules/seed/trigger` (`StartRun`), wait for completion, re-trigger |
| `failure_test.go` | `TestE2E_FailurePath_RerunRebasesBothFailureSubtrees` — two parallel failing subtrees, then a rerun rebases both |
| `rebase_test.go` | `TestRebaseFromFailedRun`, `TestRebaseAllInheritedFinalizes` |
| `rebase_upstream_fix_test.go` | `TestRebase_ReRunsAChangedUpstreamNode` — a release changes `ftable_c`, upstream of the failing `ftable_e`, and only `ftable_a`'s image tag: `PreviewRebase` names `ftable_c` (three nodes below it, its last successful duration) with and without the changed upstream nodes; a rebase previewed on the source's release is refused (`FailedPrecondition`, "the live release changed; preview again") and creates no run; a rebase without them re-executes `ftable_e`..`ftable_h`, and one with them also `ftable_c` and `ftable_d`, which succeed; `ftable_a` stays inherited |
| `single_node_run_test.go` | `TestSingleNodeRunLatest`, `TestSingleNodeRunStale`, `TestSingleNodeRunTargetNotFound` — a target the live release does not hold is refused with `NotFound` and creates no run |
| `cancel_test.go` | `TestCancelMidwayAndRetrigger` — Phase 1: trigger the happy-path DAG and let level 0 (the seeds) succeed. Phase 2: cancel the run mid-way; the run moves to `cancelling`, execution-controller fences it (`cancelled_runs.run_id`), deletes its Jobs and reports each attempt `stopped` once its pods are gone, and the run ends `cancelled` (the test waits up to 3 minutes). The test asserts the run is `cancelled`, its in-flight tasks are `cancelled`, the fence row exists, no Job labelled `run-id=<run>` is live (a Job is live while it has no deletion timestamp and is not Complete or Failed) and no pod of the run is unfinished (phase other than `Succeeded` or `Failed`), and state no longer lists the run as active. Phase 3: re-trigger only after the run is `cancelled`, with no cleanup; the new run gets a new `run_id`. Phase 4: the new run completes `succeeded` |
| `empty_dag_test.go` | `TestEmptyDAG_ManualStartIsRefused` — `StartRun` of a schedule with no node in the live release, whatever its operation, creates no run: with no cron either it is `NotFound`; once the test inserts a paused `schedules` row of the name by SQL (`UpsertSchedule` refuses the first cron of a name with no model), it is refused with `FailedPrecondition` (an empty selection). `TestEmptyDAG_ACronFireRecordsAFailedRun` — a real fire of state's cron loop: the test inserts a `schedules` row by SQL whose `next_fire_at` is five seconds ahead on the database's clock and whose cron fires once a year (noon on yesterday's date, `Europe/Paris`), so only that slot can fire. Within 90 s (the loop's 60 s tick, should the INSERT's notification be lost, with headroom) `ListRuns` lists one run of the name, of kind `cron`, `failed` with `failure_reason` `empty_selection` and `scheduled_for` the slot, and the row's `next_fire_at` has moved to the cron's next slot with its `version` unchanged. Both tests delete their row and runs at the end |
| `retry_test.go` | `TestE2E_Retry_AFailOnceModelSucceedsOnItsSecondAttempt` — `ftable_retry` (schedule `e2e-retry`) fails its first attempt and succeeds its second: its model reads the `TASK_ATTEMPT` env var and fails the build on attempt 0. The test asserts the attempt rows are exactly `0:failed:task:exit_code` and `1:succeeded`, the task and the run succeed, attempt 1's Job is named after attempt 0's with `-r1`, and the task's Kubernetes Jobs carry the `attempt` labels 0 and 1 |
| `start_failure_test.go` | `TestE2E_StartFailure_AMissingImageIsRetriedWithoutOverlap` — a single-node run of a node whose image tag does not exist: every attempt (`max_retries` + 1) fails `start:image_pull`, the task and the run fail, and no pod of an earlier attempt is still pending or running once a later attempt's Job or pod is seen. `TestE2E_StartFailure_APodThatNeverStartsTimesOutWithoutARetry` — the test applies a ResourceQuota `e2e-no-pods` (`pods: "0"`) to the Jobs' `default` namespace, so no pod can be created: the attempt fails `timeout:start_timeout` about one start timeout (`JOB_START_TIMEOUT_SECONDS`, read from the live execution-controller Deployment) after its Job was created, no pod of the task ever exists, and the task is not retried. The quota stands only for the test: `t.Cleanup` deletes it, and `deploy-k8s-controllers.sh`, `provision-k8s-test-env.sh` and `cleanup-k8s-controllers.sh` delete it too, so a test binary killed while it stands cannot leave the reused kind cluster unable to create pods. While it stands, no pod can be created in `default`, so it must not run in parallel with another test |
| `node_timeout_test.go` | `TestE2E_NodeTimeout_ATaskPastItsDeadlineFailsWithoutARetry` — `ftable_deadline` (schedule `e2e-deadline`) declares `timeout_minutes: 1` and sleeps for ten minutes; the announced topology gives it 60 s. The test asserts its Job carries `activeDeadlineSeconds` = the start timeout + 60, the attempt fails `timeout:deadline_exceeded` about at that deadline and is the only attempt, `ftable_deadline_down` is skipped and never deployed, and the run fails. The blue/green timeout-only test in this file is described under [Blue/Green Release Tests](#bluegreen-release-tests) |
| `node_lease_test.go` | `TestE2E_NodeLease_ASingleNodeRunWaitsForTheScheduledWriter` — starts a run of `e2e-schedule` (`StartRun`; the run's kind is `trigger`) and, once the schedule run's `table_a` is `running` (it has held the node's write lease since its dispatch), a single-node run of `table_a`. Read right after the trigger, `ListTasks` and `GetRunGraph` show the single-node task `pending` with `wait_reason` `node_lease`, `waiting_on_run_id` the schedule run, and a `waiting_since`. Once the schedule run's `table_a` succeeds, the lease waker dispatches the single-node task, whose wait fields are then empty, and the single-node run succeeds. The test asserts the two Jobs did not overlap: the single-node run's pod started (`status.startTime`) no earlier than the schedule run's pod finished (its latest container `finishedAt`; both are truncated to the second, so an equal second is not an overlap). It then waits for the schedule run to succeed and asserts `node_leases` holds no lease of `table_a`. The single-node run is created while the schedule run's `table_a` container runs, which takes several seconds against a few milliseconds for the trigger and the two reads; a failure whose message says the schedule run's `table_a` had already ended missed that window, not the lease |
| `run_service_test.go` | `TestE2E_RunService_ARetriedStartRunReturnsTheSameRun` — `StartRun` of `e2e-schedule` twice with one `request_id` returns the same `run_id`, leaves one run of the schedule and one `command_requests` row; the same `request_id` with `operation` `build` is `ALREADY_EXISTS`; a fresh `request_id` while the run is active is `FAILED_PRECONDITION` and stores no record |
| `schedules_test.go` | `TestE2E_Schedules_UpsertPreviewAndDeleteOnAPausedSchedule` — the schedule RPCs on `e2e-schedule`, whose cron the test creates paused so nothing fires: `UpsertSchedule("0 3 * * *", Europe/Paris, paused)` saves version 1 as the caller's identity with no next fire; `ListSchedules` lists it paused with no next fire and the live release's models of it, and its `schedules` row has no `next_fire_at`; `PreviewSchedule` returns three fires, each 03:00 in Paris on consecutive days; `DeleteSchedule` removes the row, and the schedule is still listed, with no cron, since the live release has its models; no run of `e2e-schedule` is created. `t.Cleanup` deletes the row whatever the outcome |
| `dag_latest_snapshot_test.go` | `TestE2E_DAGLatestSnapshot` |
| `topology_versioning_test.go` | `TestTopologyVersioning_MidRunIsolation` — lazy generation switch (in-flight runs immutable) |
| `release_promote_test.go` | dbt blue/green release tests — see [Blue/Green Release Tests](#bluegreen-release-tests) |
| `release_promote_python_test.go` | `TestE2E_ReleasePromote_PythonContractSkipsCompileAndPromotes`, `TestE2E_PythonNodeRun_MaterializesAndReportsFailures` — a python-contract release skips the compile leg, validates via `build_from_columns`, promotes; then each node runs in its own image. The contract's mixed DAG includes a `python-csv` node (`py_csv`, no script) whose source csv is seeded into MinIO S3 before the release posts: the validation runner range-fetches its header and checks it against `output_columns`, and the run path fetches the full object and lands its rows in the warehouse. A `python-api` node (`py_api`) names Secret `continuo-api-probe`: the run test creates that Secret in the Jobs' `default` namespace and checks the script received its key as an env var, then deletes it and checks the run fails instead of hanging |
| `python_timeout_test.go` | `TestE2E_PythonNodeTimeout_ReachesStateThroughTheRuntimeCLI` — a python contract (`fixtures/py-timeout`) declares `timeout_minutes: 90` on one of two nodes. The test streams the fixture into the fixture image and runs the image's own `continuo-runtime merge` (`docker run`, no bind mount) to build the wire contract, so the key is written by the runtime release the chart defaults to, not by the test. The contract is uploaded and a python release posted; once it promotes, state's live release carries `timeout_seconds = 5400` on that node and NULL on every other node. The image must be built on a runtime that accepts `timeout_minutes` (0.10.0 or later); `scripts/setup.sh` rebuilds it |
| `remediation_python_test.go` | `TestE2E_PythonValidationFailure_VerifiedFix`, `TestE2E_PythonValidationFailure_VerificationErrorFeedsNextAttempt`, `TestE2E_PythonParseFailure_VerifiedFix` — a rejected python node is repaired in the contract yaml, verified by a real verification run that stops at `passed`, and proposed for review; the second test proves a failed verification run's error feeds the next attempt and reaches no classifier at all; the third drives the same lane from a parse-stage rejection (a read whose relation is not schema-qualified), where the rejection carries the parser's text inline and no log. Uses the fixture repository under `fixtures/py-remediation-repo`, which `stub-github` serves as a tarball |
| `dead_letter_test.go` | `TestDeadLetter_MalformedMessagesAreDeadLetteredNotDropped`, `TestDeadLetter_RedriveReachesOnlyItsGroup`, `TestDeadLetter_CLIListsAndRedrives`, `TestDeadLetter_BacklogMetricIsExposed` — a message no consumer can parse is stored by dead-letter-controller and served through `ListDeadLetters`; a redrive of it on `release.promoted:v2` is handled only by the group that failed it (the other groups acknowledge it without a handler) and a repeated redrive publishes nothing; the real `continuo dlq list`/`redrive` binary (built from the `./cli` mount with `GOWORK=off`) drives the same flow; the backlog gauge appears once a dead letter is open. dead-letter-controller is started by `start-services.sh` (health `:8096/ready`, gRPC `:50055`) |
| `egress_test.go` | `TestEgress_ProxyRefusesHostNotOnAllowlist`, `TestEgress_ProxyAdmitsListedHost`, `TestEgress_ChatTrafficGoesThroughProxy` — a `CONNECT` to a host off the allowlist is answered 403 and one to `stub-llm:9100` is answered 200; one chat turn raises the proxy's `continuo_egress_requests_total{decision="allowed"}` count, which shows agent-chat reaches its LLM through egress-proxy. The remediation tests' LLM and GitHub calls go through the proxy as well |
| `seed_topology_test.go` | `seedTopology` helper — announces the e2e DAG through release-controller's `announce-topology` (artifact in MinIO, next promotion seq, `release.promoted:v2`), which the orchestrator applies to Neo4j and state stores as its live release on the promotion path. It returns once both have applied it (`waitForAnnouncedTopology` in `announce_topology_test.go`): the orchestrator's `:Meta` and state's `live_release` name the announced release, and the release's `topology_nodes` in state carry every schedule the DAG declares, which is what `StartRun` needs to know a schedule |
| `topology_by_reference_test.go` | `TestTopologyByReference_LatePromotionMovesNothing` — announces A, with a model of schedule `e2e-stale-probe`, then B without it, and replays A's `release.promoted:v2` entry under a fresh event id: both the orchestrator's and state's groups acknowledge it without a dead letter, the orchestrator's live pointer and state's live release stay at B, and for 10 s no node of state's live release carries `e2e-stale-probe` and `ListSchedules` does not list it. `TestTopologyByReference_CorruptArtifactIsDeadLettered` — a promotion whose artifact does not match its checksum is dead-lettered as permanent by the orchestrator's and state's groups, and the orchestrator's live pointer does not move |
| `seed_reconcile_test.go` | `TestSeedReconcile_LoadsAChangedSeedOnce` — announces the e2e DAG with `seed_table_1` carrying a new `content_hash`: state plans one `promote_seed` run and dispatches the seed in the promotion's transaction, the seed loads with service-1's image and state records the hash in `node_materializations`; announcing the same content again under a new release loads nothing. Restores the default DAG (no seed hashes) at the end |
| `ui_http_test.go` | HTTP assertions against the ui (`verifyUIService`) |
| `public_api_test.go`, `ci_token.go` | `TestPublicAPI_Authorization` — the public `/api/v1` release API's refusals (unbound repository, bootstrap without `allowBootstrap`, body/token repository mismatch, garbage token) and a CI token reading `current-prod`. `ci_token.go` mints GitHub Actions-shaped tokens from `stub-github`, which the ui trusts as its CI issuer through `tests/e2e/ci-auth.json`; every release the suite submits goes through `/api/v1/releases` with such a token |
| `auth_oidc_test.go` | `TestAuthOIDC` — real OIDC login flow against Dex (auth-e2e profile), plus Dex password-grant bearers against `/api/v1`; skipped unless `UI_AUTH_HTTP_BASE` is set |
| `verify.go` | DAG-level assertions (execution-controller's k8s Jobs, dispatch of ready tasks, failure helpers) |
| `clients.go` | gRPC, Redis, Postgres, Neo4j, S3, and release-controller client setup |
| `helpers.go` | `pollUntil`, k8s Job and pod helpers (a pod's phase, start time and container termination times), `containsAll` |
| `cleanup.go` | Removes all test data from every data store |

## Blue/Green Release Tests

`release_promote_test.go` drives the dbt blue/green release pipeline end-to-end through the ui's public API — `POST /api/v1/releases`, authenticated with a GitHub Actions-shaped OIDC token minted by `stub-github` (`ci_token.go`) — the same request a CD pipeline issues. Validation runs **real `continuo-python-runtime-<engine>` K8s Jobs in kind** (no dbt in the validation path — see the execution-controller doc's `CreateValidationJob`), exercising the full event chain:

```
POST /api/v1/releases → release.requested:v1 → topology-controller candidate parse
→ manifest.loaded.candidate:v1 → release-controller derives the changed-node set
→ validation.requested:v1 → execution-controller runs per-node validation jobs,
settling each terminal outcome in-process (the Job watch's observer → service/outcomes.Recorder,
same transaction as the observation) → validation.result:v1 (kind=complete)
→ release-controller promotes → release.promoted:v2
→ orchestrator loads the topology artifact and swaps the Neo4j topology
```

| Test | What it verifies |
|------|------------------|
| `TestE2E_ReleasePromote_ValidatesAndSwapsTopology` | Full cutover. `current_prod` is pre-seeded with every candidate node **except** `rel_probe`, so the derived changed set is exactly `{rel_probe}` — a self-contained leaf that validates without touching production. Asserts promotion, the Neo4j topology swap, and the per-node dbt log URI surfaced through the UI BFF. |
| `TestE2E_ReleasePromote_GatedIntraServiceUpstream` | A changed node with a changed **intra-service** upstream (`rel_probe_down` → `rel_probe_up`): the upstream builds into the candidate schema first (topological gating), the dependent validates against it, then the candidate schema is torn down. |
| `TestE2E_ReleasePromote_GatedCrossServiceUpstream` | A clean **cross-service** chain (`xprobe_down`@service-2 → `xprobe_up`@service-3) validates self-contained — the changed node and its cross-service upstream are both built empty in the candidate schema. |
| `TestE2E_ReleasePromote_BootstrapSkipsValidation` | `bootstrap:true` promotes directly, skipping validation, and seeds `current_prod`. |
| `TestE2E_ReleasePromote_ATimeoutOnlyChangePromotesWithoutValidation` (`node_timeout_test.go`) | A release that changes only a node's `meta.timeout_minutes` promotes without validation: no node is in the changed set, no `validation.requested:v1` is published and no validation Job runs, and state's live release stores the timeout (`topology_nodes.timeout_seconds`) on the declaring node only. `requireNoOtherNodeTimeout` checks the other nodes and tolerates exactly one timeout besides it: the 60 s that service-1's `e2e_schema.ftable_deadline` model declares for itself (the Python timeout test uses the same check). |

These tests read the dbt manifests that `setup.sh` uploads to MinIO S3 and use the content-addressed image tags loaded into kind, so they need no setup beyond the standard blank-state harness. Compile and seed-build run real dbt, and each validation job runs a real `continuo-python-runtime-<engine>` Job — allow up to ~10 minutes per test, and expect a cold kind run to be slower than a warm one.

Run only the blue/green tests:

```bash
docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator \
  go test -v -count=1 -timeout 125m -run 'TestE2E_ReleasePromote' /app/tests/e2e/...
```

## Failure Path Test

`TestE2E_FailurePath_RerunRebasesBothFailureSubtrees` uses the 6-node `ftable_*` DAG. It verifies:

- `ftable_e` fails every attempt its budget allows (`max_retries` + 1, so 3 with the default `runs.taskMaxRetries` of 2), each recorded in `task_attempts` with failure class `task`, and reaches `failed` status
- Downstream node (`ftable_f`) is never deployed and is recorded `skipped`
- The scheduler is finalised as `FAILED`

The failure model `ftable_e` runs in the `service-2` Docker image but JOINs `public.wrong_name`, which does not exist. This causes the dbt run to fail at execution time on every attempt.

## Test Flow

### Happy Path
1. **Setup** - Initialize clients, verify services healthy
2. **Cleanup** - Remove any leftover test data
3. **Seed** - Create 6-node `ftable_*` DAG in Neo4j via graph service
4. **Trigger** - `RunService.StartRun` gRPC call → state creates the run (kind `trigger`)
5. **Verify Level 0** - Check root jobs (`ftable_a`, `ftable_b`) deploy and complete
6. **Verify Level 1** - Check `ftable_c` unlocks and executes
7. **Verify Level 2** - Check `ftable_d` and `ftable_e` execute
8. **Verify Level 3** - Check `ftable_f` executes
9. **Verify UI** - `GET /api/runs/:id` and `/api/runs/:id/tasks` return `status: "succeeded"` and ISO-8601 timestamps (skipped if `UI_HTTP_BASE` unset)
10. **Cleanup** - Remove all test data

### Trigger Run-and-Rerun (seed schedule)
1. **Setup** - Initialize clients, verify services healthy, cleanup seed schedule data
2. **Topology** - Announce the e2e DAG (`seedTopology`), which returns once the orchestrator and state's live release hold it, with the seed schedule's models
3. **Run 1** - `POST /api/schedules/seed/trigger` → wait for 3 seed tasks to succeed → scheduler SUCCEEDED
4. **Run 2** - `POST /api/schedules/seed/trigger` again → wait for second run to complete → scheduler SUCCEEDED
5. **Cleanup** - Remove all seed schedule data

### Failure Path
1. **Setup** - Initialize clients, verify services healthy
2. **Seed** - Create the 6-node `ftable_*` DAG in Neo4j
3. **Trigger** - `RunService.StartRun` gRPC call
4. **Verify Level 0** - Root nodes (`ftable_a`, `ftable_b`) complete successfully
5. **Verify Level 1** - `ftable_c` executes; `ftable_d` and `ftable_e` are deployed
6. **Wait for ftable_e failure** - Poll until `tasks` holds `attempt = max_retries` and `status = 'failed'` and `task_attempts` holds one failed row of class `task` for each attempt from 0 to `max_retries`; `ftable_d` succeeds
7. **Verify the cascade** - Confirm `ftable_f` is never deployed and is recorded `skipped`
8. **Verify scheduler FAILED** - Poll `runs` until `status = 'failed'`
9. **Cleanup** - Remove all test data

## Expected Duration

- Happy path: ~1 minute
- Failure path: ~1 minute (3 attempts, each retry dispatched at once)
- Whole suite: ~39 minutes warm, measured without the retry, start-failure, start-timeout and deadline
  scenarios (`TestE2E_Retry_*`, `TestE2E_StartFailure_*`, `TestE2E_NodeTimeout_*`); the CI e2e job
  measures the whole suite on every run. A cold stack is slower.
- Timeout: 125 minutes for the whole `tests/e2e/...` package (`go test -timeout`).
  Derived, not measured: the two longest per-test context ceilings (35m each)
  wedging in one run, plus 55m for the rest of the suite. Every per-test
  context has to be able to fire before the package timeout, or a wedge
  prints a goroutine dump instead of a named assertion. `.github/workflows/ci.yml`
  uses the same value and allows 140 `timeout-minutes` around it.

## Troubleshooting

**"No space left on device" / Redis MISCONF error:**

This means Colima's virtual disk is full. Check usage with `docker system df`. To resolve:
```bash
# Free up unused images and build cache
docker system prune -f

# If still not enough, increase Colima disk (destructive — wipes all Docker data)
colima stop
colima delete
colima start --disk 100  # 100GB
# Then rebuild: make nuke && DOCKER_BUILDKIT=1 docker build -t continuo-base:latest -f Dockerfile.base . && bash scripts/setup.sh
```

**Test fails with connection errors:**
- Verify docker-compose services are running: `docker compose ps`
- Check service health: `curl http://localhost:8082/health`

**An agent call fails with a 403 or a connection error to `egress-proxy:3128`:**
- egress-proxy refused the host. `docker logs egress-proxy | grep 'egress denied'` names the refused host and port; add them to `tests/e2e/config/egress-allowlist.yaml` and `docker compose restart egress-proxy` (the allowlist is read when the process starts), then start the process again with `docker exec -d egress-proxy bash -c "cd /app/egress-proxy && go run ."`.
- A service the agents should reach directly (a compose service name) belongs in that agent's `NO_PROXY` in `docker-compose.yml`.

**Neo4j not healthy / services fail to start:**
- Neo4j takes up to 90 seconds to become healthy. Wait for `docker compose ps` to show `neo4j` as `(healthy)` before running `bash tests/e2e/start-services.sh`.

**Test fails with kubectl errors:**
- Verify kind cluster is running: `kind get clusters`
- Check kubectl can reach cluster: `kubectl cluster-info`

**Test times out waiting for jobs:**
- Check k8s jobs: `kubectl get jobs -l schedule=e2e-schedule`
- Check job logs: `kubectl logs -l schedule=e2e-schedule`

**No pod is created in `default` (Jobs sit without pods):**
- A killed test binary can leave the start-timeout test's ResourceQuota behind: `kubectl get resourcequota -n default`. Remove it with `kubectl delete resourcequota e2e-no-pods -n default`; `bash tests/e2e/provision-k8s-test-env.sh` does the same.

**Test fails at k8s controller health check:**
- Check pod status: `kubectl get pods -n default | grep controller`
- View pod logs: `kubectl logs -l app=execution-controller -n default`
- Re-run provisioning: `bash tests/e2e/provision-k8s-test-env.sh`

**Test fails with "table does not exist":**
- Verify flyway migrations completed: `docker compose logs flyway-orchestrator`
- Check the execution outbox table exists: `docker compose exec postgres psql -U continuo_svc -d continuo_execution -c '\dt execution_outbox'`

**Services not starting:**
```bash
docker compose ps
docker logs orchestrator --tail 50
docker compose restart orchestrator state
bash tests/e2e/start-services.sh
```

**continuo-base image missing (after docker system prune):**
```bash
DOCKER_BUILDKIT=1 docker build -t continuo-base:latest -f Dockerfile.base .
```

**Kubeconfig not accessible:**
```bash
bash scripts/setup.sh
docker exec execution-controller ls -la /root/.kube/
```

**Controller not starting:**
```bash
kubectl get pods -n default | grep controller
kubectl logs -l app=execution-controller -n default
docker exec continuo-control-plane crictl images | grep continuo
```

**Network connectivity issues:**
```bash
# Test from controller pod to postgres
kubectl exec deployment/execution-controller -- nc -zv 172.17.0.1 5432

# Test from controller pod to state service
kubectl exec deployment/execution-controller -- nc -zv 172.17.0.1 50051
```

**Rebuild all k8s images and redeploy:**
```bash
bash tests/e2e/cleanup-k8s-controllers.sh
bash tests/e2e/provision-k8s-test-env.sh
```

**Neo4j fails to start ("Neo4j is already running"):**

This happens when Colima is stopped while neo4j is running, leaving a stale `store_lock`. Fix:
```bash
docker run --rm -v continuo_neo4j_data:/data alpine rm -f /data/databases/store_lock
docker-compose rm -f neo4j
docker-compose up -d --force-recreate neo4j
```

`E2E_WAREHOUSE_DB` (default `continuo_dbt`) selects the warehouse database the harness inspects for dbt-materialized tables.

**Maintenance mode (`TestMaintenanceMode`):** triggers a single-node run, then restarts the `state` and `release-controller` containers with `MAINTENANCE_ENABLED=true` (relaunching `go run .` with the flag overridden; each service's output goes to `/tmp/<container>.log` inside its container). With maintenance on it asserts a new node run (via `ui`) and a new public release are refused with HTTP 503 and the `maintenance` code, and that the run already in flight still reaches `succeeded`. It then restarts both services with the flag off and asserts a new run is accepted and succeeds. The restarts take a minute or more each, and a `t.Cleanup` turns maintenance off again even on failure. If the test is interrupted before that cleanup runs, restore the stack with `docker restart state release-controller && bash tests/e2e/start-services.sh`.
