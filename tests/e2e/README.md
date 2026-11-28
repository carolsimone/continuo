# E2E System Test

Comprehensive end-to-end test validating continuo's run orchestration and release checks.

## Test Overview

The suite covers both run orchestration and the dbt blue/green release check. The happy-path test exercises a 6-node `ftable_*` DAG through every component:
- `run-controller` - Plans and creates runs, dispatches ready tasks, owns the run/task lifecycle
- `catalog` - Applies each promoted release to the Neo4j topology (a node a release drops stays, retired, with its history), serves the topology, code-version and failure-precedent reads (nodes, code versions, precedents), and backs up each tenant's graph to MinIO (`catalog-backup take`)
- `execution-controller` - Deploys k8s jobs and monitors their status
- `topology-controller` - Parses dbt manifests (candidate + legacy paths)
- `release-controller` - Owns the blue/green candidate-release lifecycle, the live release and each project's production pointer
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
#    - Builds every docker-compose image (including dbt-base)
#    - Builds and loads dbt project images into kind
#    - Compiles dbt manifests (requires postgres to be up briefly)
#    - Writes kubeconfigs into component directories
#    - Starts docker-compose in detached mode
bash scripts/setup.sh

# 4. Start Go component processes inside containers and wait for health
bash tests/e2e/start-components.sh

# 5. Start the ui container
docker compose up -d ui

# 6. Deploy the pre-built execution-controller image into the kind cluster
bash tests/e2e/deploy-execution-controller.sh

# 7. Run the tests
docker exec -e UI_HTTP_BASE=http://ui:8090 catalog \
  go test -v -count=1 -timeout 172m /app/tests/e2e/...

# 8. Clean up k8s resources
bash tests/e2e/cleanup-execution-controller.sh
```

> `setup.sh` takes ~10 minutes on first run.

## Subsequent Runs

When the kind cluster and all Docker images already exist:

> The compose execution-controller sets `JOB_S3_SECRET_NAME=continuo-job-s3`, so its Job pods fail with class `start` on a kind cluster that lacks that Secret. `setup.sh`, `provision-k8s-test-env.sh` and `deploy-execution-controller.sh` apply it; on an existing cluster without it, run `kubectl apply -f tests/e2e/k8s/job-s3-secret.yaml` once.

```bash
# Tear down stale containers — including any left over from the main project directory
# (the main project uses bare container names like /run-controller, /ui that conflict with this worktree)
docker rm -f run-controller catalog execution-controller ui 2>/dev/null || true
docker compose down

# Start (or restart) all docker-compose services. The one-shot streams-init
# service creates every Redis stream and consumer group under the continuo:
# prefix and exits; the components wait up to five minutes for their groups.
docker compose up -d
# The compose Redis has no persistence: after it restarts or loses its data,
# run `docker compose up streams-init` to create the streams and groups again.

# Wait for neo4j to be healthy (can take up to 90s)
docker compose ps   # neo4j should show "(healthy)"

# Start Go component processes and wait for health
bash tests/e2e/start-components.sh

# Start ui
docker compose up -d ui

# If any image loaded into kind changed since last run, rebuild and reload into kind:
bash tests/e2e/provision-k8s-test-env.sh
# Otherwise, if images are unchanged, just (re)deploy:
# bash tests/e2e/deploy-execution-controller.sh

# Run the tests
docker exec -e UI_HTTP_BASE=http://ui:8090 catalog \
  go test -v -count=1 -timeout 172m /app/tests/e2e/...

# Clean up k8s resources
bash tests/e2e/cleanup-execution-controller.sh
```

Or use the convenience target (assumes kind cluster and images already exist):

```bash
make e2e-full
```

`make e2e-full` runs `docker compose up -d`, waits for health, starts components,
runs `provision-k8s-test-env.sh`, then executes the tests.

## Auth e2e suite (OIDC against Dex)

The auth suite exercises the real OIDC login flow (ui in `oidc` mode
against a Dex identity provider). It is skipped unless `UI_AUTH_HTTP_BASE` is
set.

```bash
# Requires the standard stack to be up (run-controller, catalog, redis, …).
docker compose build ui-auth
docker compose --profile auth-e2e up -d dex ui-auth

docker exec -e UI_AUTH_HTTP_BASE=http://ui-auth:8090 catalog \
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
| `scripts/setup.sh` | Full bootstrap: kind cluster + image builds + dbt manifests + dbt seeds + the outside table `e2e_raw.ingest_probe` + kubeconfig + compose up |
| `tests/e2e/start-components.sh` | Starts Go component processes inside containers (`go run`), waits for HTTP health |
| `tests/e2e/provision-k8s-test-env.sh` | Rebuilds controller + dbt images, loads them into kind, regenerates kubeconfig, deploys k8s manifests |
| `tests/e2e/deploy-execution-controller.sh` | Deploys the pre-built execution-controller image already in kind (no rebuild); used by CI after `setup.sh` |
| `tests/e2e/cleanup-execution-controller.sh` | Removes the execution-controller Deployment from the kind cluster |

### How CI runs it

CI mirrors the blank-state flow above:

1. `bash scripts/setup.sh` (with `CI=true` set automatically by GitHub Actions)
2. Wait for containers to be running
3. Build + start `run-controller` and `catalog` binaries (gRPC health check)
4. Run per-component unit tests
5. Build + start `topology-controller`
6. `docker compose up -d ui`
7. `bash tests/e2e/deploy-execution-controller.sh` (images pre-loaded by setup.sh)
8. `docker exec -e UI_HTTP_BASE=http://ui:8090 catalog go test -v -timeout 172m /app/tests/e2e/...`
9. `bash tests/e2e/cleanup-execution-controller.sh`

CI uses `deploy-execution-controller.sh` (not `provision-k8s-test-env.sh`) because
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
- run-controller, catalog, topology-controller, release-controller (components)
- egress-proxy (forward proxy for the outbound HTTP of agent-chat and agent-remediation; they reach their LLM and GitHub only through it, and reach the stack's own components directly via `NO_PROXY`). `start-components.sh` starts it before agent-remediation: health `:8093/ready`, proxy port `:3128`. Its allowlist is `tests/e2e/config/egress-allowlist.yaml`, which admits `stub-llm`, `stub-github` and the public LLM and GitHub APIs

**In kind cluster (Kubernetes):**
- execution-controller (Deployment)
- dbt project jobs (run as k8s Jobs, one per table)
- Secret `continuo-job-s3` (`tests/e2e/k8s/job-s3-secret.yaml`, the MinIO keys), which `scripts/setup.sh`, `deploy-execution-controller.sh` and `provision-k8s-test-env.sh` apply: execution-controller's Job pods read their S3 keys from it by reference (`JOB_S3_SECRET_NAME`)

The controller in kind connects to docker-compose services via docker bridge network (172.17.0.1).

## Test Structure

| File | Purpose |
|------|---------|
| `system_test.go` | `TestE2E_HappyPath_FullDAGExecution` — 6-node `ftable_*` DAG through every component |
| `manual_run_test.go` | `TestManualRun_SeedRunAndRerun` — start a manual run of the seed schedule via the ui's `POST /api/schedules/seed/manual-run` (`StartRun`), wait for completion, start it again |
| `failure_test.go` | `TestE2E_FailurePath_RerunRebasesBothFailureSubtrees` — two parallel failing subtrees, then a rerun rebases both |
| `rebase_test.go` | `TestRebaseFromFailedRun`, `TestRebaseAllInheritedFinalizes` |
| `rebase_upstream_fix_test.go` | `TestRebase_ReRunsAChangedUpstreamNode` — a release changes `ftable_c`, upstream of the failing `ftable_e`, and only `ftable_a`'s image tag: `PreviewRebase` names `ftable_c` (three nodes below it, its last successful duration) with and without the changed upstream nodes; a rebase previewed on the source's release is refused (`FailedPrecondition`, "the live release changed; preview again") and creates no run; a rebase without them re-executes `ftable_e`..`ftable_h`, and one with them also `ftable_c` and `ftable_d`, which succeed; `ftable_a` stays inherited |
| `single_node_run_test.go` | `TestSingleNodeRunLatest`, `TestSingleNodeRunStale`, `TestSingleNodeRunTargetNotFound` — a target the live release does not hold is refused with `NotFound` and creates no run |
| `cancel_test.go` | `TestCancelMidwayAndStartAgain` — Phase 1: start a manual run of the happy-path DAG and let level 0 (the seeds) succeed. Phase 2: cancel the run mid-way; the run moves to `cancelling`, execution-controller fences it (`cancelled_runs.run_id`), deletes its Jobs and reports each attempt `stopped` once its pods are gone, and the run ends `cancelled` (the test waits up to 3 minutes). The test asserts the run is `cancelled`, its in-flight tasks are `cancelled`, the fence row exists, no Job labelled `run-id=<run>` is live (a Job is live while it has no deletion timestamp and is not Complete or Failed) and no pod of the run is unfinished (phase other than `Succeeded` or `Failed`), and run-controller no longer lists the run as active. Phase 3: start a manual run again only after the run is `cancelled`, with no cleanup; the new run gets a new `run_id`. Phase 4: the new run completes `succeeded` |
| `empty_dag_test.go` | `TestEmptyDAG_ManualStartIsRefused` — `StartRun` of a schedule with no node in the live release, whatever its operation, creates no run: with no cron either it is `NotFound`; once the test inserts a paused `schedules` row of the name by SQL (`UpsertSchedule` refuses the first cron of a name with no model), it is refused with `FailedPrecondition` (an empty selection). `TestEmptyDAG_ACronFireRecordsAFailedRun` — a real fire of run-controller's cron loop: the test inserts a `schedules` row by SQL whose `next_fire_at` is five seconds ahead on the database's clock and whose cron fires once a year (noon on yesterday's date, `Europe/Paris`), so only that slot can fire. Within 90 s (the loop's 60 s tick, should the INSERT's notification be lost, with headroom) `ListRuns` lists one run of the name, of kind `cron`, `failed` with `failure_reason` `empty_selection` and `scheduled_for` the slot, and the row's `next_fire_at` has moved to the cron's next slot with its `version` unchanged. Both tests delete their row and runs at the end |
| `retry_test.go` | `TestE2E_Retry_AFailOnceModelSucceedsOnItsSecondAttempt` — `ftable_retry` (schedule `e2e-retry`) fails its first attempt and succeeds its second: its model reads the `TASK_ATTEMPT` env var and fails the build on attempt 0. The test asserts the attempt rows are exactly `0:failed:task:exit_code` and `1:succeeded`, the task and the run succeed, attempt 1's Job is named after attempt 0's with `-r1`, and the task's Kubernetes Jobs carry the `attempt` labels 0 and 1 |
| `start_failure_test.go` | `TestE2E_StartFailure_AMissingImageIsRetriedWithoutOverlap` — a single-node run of a node whose image tag does not exist: every attempt (`max_retries` + 1) fails `start:image_pull`, the task and the run fail, and no pod of an earlier attempt is still pending or running once a later attempt's Job or pod is seen. `TestE2E_StartFailure_APodThatNeverStartsTimesOutWithoutARetry` — the test applies a ResourceQuota `e2e-no-pods` (`pods: "0"`) to the Jobs' `default` namespace, so no pod can be created: the attempt fails `timeout:start_timeout` about one start timeout (`JOB_START_TIMEOUT_SECONDS`, read from the live execution-controller Deployment) after its Job was created, no pod of the task ever exists, and the task is not retried. The quota stands only for the test: `t.Cleanup` deletes it, and `deploy-execution-controller.sh`, `provision-k8s-test-env.sh` and `cleanup-execution-controller.sh` delete it too, so a test binary killed while it stands cannot leave the reused kind cluster unable to create pods. While it stands, no pod can be created in `default`, so it must not run in parallel with another test |
| `node_timeout_test.go` | `TestE2E_NodeTimeout_ATaskPastItsDeadlineFailsWithoutARetry` — `ftable_deadline` (schedule `e2e-deadline`) declares `timeout_minutes: 1` and sleeps for ten minutes; the announced topology gives it 60 s. The test asserts its Job carries `activeDeadlineSeconds` = the start timeout + 60, the attempt fails `timeout:deadline_exceeded` about at that deadline and is the only attempt, `ftable_deadline_down` is skipped and never deployed, and the run fails. The blue/green timeout-only test in this file is described under [Blue/Green Release Tests](#bluegreen-release-tests) |
| `node_lease_test.go` | `TestE2E_NodeLease_ASingleNodeRunWaitsForTheScheduledWriter` — starts a run of `e2e-schedule` (`StartRun`; the run's kind is `manual`) and, once the schedule run's `table_a` is `running` (it has held the node's write lease since its dispatch), a single-node run of `table_a`. Read right after the start, `ListTasks` and `GetRunGraph` show the single-node task `pending` with `wait_reason` `node_lease`, `waiting_on_run_id` the schedule run, and a `waiting_since`. Once the schedule run's `table_a` succeeds, the lease waker dispatches the single-node task, whose wait fields are then empty, and the single-node run succeeds. The test asserts the two Jobs did not overlap: the single-node run's pod started (`status.startTime`) no earlier than the schedule run's pod finished (its latest container `finishedAt`; both are truncated to the second, so an equal second is not an overlap). It then waits for the schedule run to succeed and asserts `node_leases` holds no lease of `table_a`. The single-node run is created while the schedule run's `table_a` container runs, which takes several seconds against a few milliseconds for the start and the two reads; a failure whose message says the schedule run's `table_a` had already ended missed that window, not the lease |
| `run_service_test.go` | `TestE2E_RunService_ARetriedStartRunReturnsTheSameRun` — `StartRun` of `e2e-schedule` twice with one `request_id` returns the same `run_id`, leaves one run of the schedule and one `command_requests` row; the same `request_id` with `operation` `build` is `ALREADY_EXISTS`; a fresh `request_id` while the run is active is `FAILED_PRECONDITION` and stores no record |
| `schedules_test.go` | `TestE2E_Schedules_UpsertPreviewAndDeleteOnAPausedSchedule` — the schedule RPCs on `e2e-schedule`, whose cron the test creates paused so nothing fires: `UpsertSchedule("0 3 * * *", Europe/Paris, paused)` saves version 1 as the caller's identity with no next fire; `ListSchedules` lists it paused with no next fire and the live release's models of it, and its `schedules` row has no `next_fire_at`; `PreviewSchedule` returns three fires, each 03:00 in Paris on consecutive days; `DeleteSchedule` removes the row, and the schedule is still listed, with no cron, since the live release has its models; no run of `e2e-schedule` is created. `t.Cleanup` deletes the row whatever the outcome |
| `dag_latest_snapshot_test.go` | `TestE2E_DAGLatestSnapshot` |
| `topology_versioning_test.go` | `TestTopologyVersioning_MidRunIsolation` — lazy generation switch (in-flight runs immutable) |
| `catalog_history_test.go` | `TestDroppedNodeKeepsItsHistory` — a release of service-1 promotes with no changed node (production holds the baseline's own hashes), so it needs no validation, and its promotion carries the release's code bundle: catalog makes `e2e_schema.rel_probe` active and records its code version (the test waits until the node's `code_version_promotion_seq` reaches the promotion's seq, which works warm and cold). As a control, `GetNode` finds the node live and `GetNodeVersions` marks exactly one version current. The test then announces the e2e DAG without `rel_probe`: `GetNode` with `include_inactive` reports it `inactive`, `GetNode` without it is `NOT_FOUND`, `GetNodeVersions` returns exactly the versions it returned before the drop, none current, and with `current_only` an empty list. A deferred restore announces the default e2e DAG again (`seedTopology`) and waits for the promote-seed runs the first release planned, so the next test finds the DAG live with `rel_probe` active; the live release stays on the test's first release, which every test that reads it seeds first |
| `unknown_external_node_test.go` | `TestE2E_UnknownExternalNode_ShowsInTheGraphAndNeverRuns` — service-1's `ingest_probe_reader` model (schedule `ingest-probe`) reads `e2e_raw.ingest_probe`, a table no model builds, which `setup.sh` creates in the warehouse. A bootstrap release of service-1 promotes, and the test asserts the table is an `unknown-external` node everywhere: in run-controller's copy of the release (`topology_nodes` with no project and no schedule, and the edge into the reader), in catalog's graph (an active `:Node` of type `unknown-external` that the reader `:DEPENDS_ON`) and in the ui's schedule graph (`GET /api/schedules/ingest-probe/graph` lists node `.e2e_raw.ingest_probe`, typed `unknown-external`, and the edge from it to `service-1.e2e_schema.ingest_probe_reader`). `StartRun(ingest-probe)` then plans exactly the reader, which succeeds, and no Job is created for the table; a `StartSingleNodeRun` of the table is `INVALID_ARGUMENT`, with no project and with the reader's project (whose refusal names `unknown-external`), and stores no command record; no run of the tenant holds a task for the table. A deferred restore announces the default e2e DAG again and waits for the promote-seed runs the release planned |
| `dormant_node_test.go` | `TestE2E_DormantNode_IsListedRunsByHandAndNeverRunsOnASchedule` — service-1's `dormant_probe` model carries the project's owner and no schedule tag. A bootstrap release of service-1 promotes: run-controller's copy of the release holds the model as a `dbt-model` with an empty `schedule_name`, dormant by `model.IsDormant`, and catalog's graph as an active `:Node` with an empty `schedule_name`. Before any run of it, `ListNodes` with `dormant_only` lists it dormant with no task, no last status and no last run, leaves out `rel_probe`, a model of a schedule that `ListNodes` lists as not dormant, and lists only dormant nodes; `GET /api/nodes?dormant=1` lists it the same way; `ListNodes` lists no node of `e2e_raw.ingest_probe`, the release's `unknown-external` node; `ListScheduleTopologies` counts the release's dormant nodes as `model.IsDormant` counts them and has no tile with an empty name. `StartRun` with no schedule name is `INVALID_ARGUMENT` and stores no command record, and `ListSchedules` lists no empty name. `StartRun` of every schedule of the release creates a run with no task for the node: each run is cancelled as soon as it is created, since its tasks are written with it, and ends `cancelled`. Before the first of these `StartRun` calls the test waits up to 5 minutes for the promote-seed runs of every release to end, requires that no promote-seed run of any release is still active, and applies the `e2e-no-pods` ResourceQuota (the start-timeout test's), so none of these runs gets a pod or builds a model; the test removes the quota once they have ended, and `t.Cleanup` removes it however the test ends. No run of the tenant then holds a task for the node. A `StartSingleNodeRun` of it creates a `single_node_run` of exactly its task, which succeeds, and `ListNodes` then lists it, still dormant, with one task, last status `succeeded` and a last run. A single-node run of the node that an earlier attempt left is removed first; the schedules' runs and the single-node run are deleted at the end, and a deferred restore announces the default e2e DAG again and waits for the promote-seed runs the release planned |
| `rebaseline_test.go` | `TestE2E_RebaselineRelease_AdoptsSeedsAndValidatesNothing` — with production on the baseline, a release of service-1 posted with `"bootstrap": true, "rebaseline": true` promotes and no validation Job is created for it. run-controller plans no promote-seed run for it and records each service-1 seed in `node_materializations` with `source` `adopted`, the release's id, and the `content_hash` and non-empty `seed_columns_hash` the release stores for it; every `:NodeVersion` catalog records for the release is `healed` (catalog writes a version once per unique id and hash, so a stack where an earlier release wrote these hashes has none to check, and the count is logged); `GET /api/v1/releases/<id>` answers `"rebaseline": true`. A deferred restore announces the default e2e DAG again. `TestE2E_RebaselineWithoutBootstrap_Is400` — `"rebaseline": true` without `"bootstrap": true` is a 400 `bad_request` that names the rule, and release-controller records no release |
| `alias_version_test.go` | `TestE2E_AliasedAndVersionedModels_ValidateWithTheirDependants` — service-1's models of schedule `e2e-alias-version`: `alias_src` (alias `alias_src_tbl`), `alias_dep` (reads it through `ref()`), version 2 of the versioned model `ver_src` (unique id `e2e_schema.ver_src.v2`, built as `ver_src_v2`) and `ver_dep` (reads that version through `ref()`). With production holding every baseline node but those four, a release of service-1 validates exactly the four: its `validation.requested:v1` carries `relation_name` `alias_src_tbl` and `ver_src_v2` on the two upstream models, none on the dependants, and names each upstream among its dependant's `upstream_node_ids`. The release promotes, which needs each dependant to read its upstream's copy in the candidate schema, since no run builds the four in production |
| `catalog_backup_test.go` | `TestCatalogBackupTakeWritesAnObjectAndMetric` — runs `go run ./cmd/catalog-backup take --tenant default` in `/app/catalog` with the catalog container's environment (run again, up to three times, when it exits 3 because the scheduled backup holds the lease). stdout is exactly one JSON object (`object_key`, `bytes`, `nodes`, `relationships`, `duration_seconds`); the key is new under `tenants/default/backups/catalog/`, ends in `.jsonl.gz` and its MinIO object's size equals `bytes`; the object gunzips to a header of `format_version` 1 and tenant `default` whose node and relationship totals equal the printed counts and the number of record lines. `catalog-backup list --tenant default` lists the key, newest first. catalog's `/metrics` reports `continuo_catalog_backup_last_success_timestamp_seconds` at or after the second the test started (polled for up to 90 s, past the gauge's 60 s cache). `t.Cleanup` deletes the object it wrote |
| `release_promote_test.go` | dbt blue/green release tests — see [Blue/Green Release Tests](#bluegreen-release-tests) |
| `release_promote_python_test.go` | `TestE2E_ReleasePromote_PythonContractSkipsCompileAndPromotes`, `TestE2E_PythonNodeRun_MaterializesAndReportsFailures` — a Python release skips the compile leg, validates via `build_from_columns`, promotes; then each node runs in its own image. The test builds the release's `odcs-bundle.json` from the `.odcs.yaml` files of `fixtures/py-probe/contracts` and uploads it before the release posts. It computes each node's `source_hash` as `continuo-runtime pack` does, so the fixture image's harness reproduces the release's `content_hash`. The bundle's package list is the one the fixture image's `continuo-runtime packages` prints. The contracts' mixed DAG includes a `python-csv` node (`py_csv`, no script) whose source csv is seeded into MinIO S3 before the release posts: the validation runner range-fetches its header and checks it against the contract's columns, and the run path fetches the full object and lands its rows in the warehouse. A `python-api` node (`py_api`) names Secret `continuo-api-probe`: the run test creates that Secret in the Jobs' `default` namespace and checks the script received its key as an env var, then deletes it and checks the run fails instead of hanging |
| `contracts_test.go` | `TestE2E_PythonRelease_IsPromotedAndPublished`, `TestE2E_PythonRelease_AnInvalidContractIsRejected` — the `py-probe` project is released as kind `python` and promoted. Once catalog's live folder index names the release, `continuo node data-contract e2e_schema.py_probe` prints version 1 with the contract's `team.name`, the object `tenants/default/contracts/live/e2e_schema.py_probe.odcs.yaml` exists in MinIO with `version: "1"`, and `_index.json` lists it with that version and `contract_hash`. On a stack where an earlier run promoted the node, the expected version is the one after the highest catalog holds, or that one when the contract is unchanged. A second release that changes only `team.name` validates nothing (no `validation.requested:v1`, no `validation_node_ids`) and makes version 2, with the same `content_hash`. A bundle whose contract has the type `NUMERIC(12)` is rejected with `reject_reason` `invalid_contract`, the failed node named in `failing_nodes`, in `per_node_results` with its contract file and in the public `reject_detail`, and `continuo_contracts` `0.5.0` (the version topology-controller pins) on the public release status. `TestE2E_DbtContracts_AnInvalidContractIsRejected` pins service-2's production manifest at a copy of its baseline in which the model `e2e_schema.unfixable_infra` (a leaf that reads no table and no model reads) has `config.meta.criticality` `high`, the way `remediation_parse_test.go` pins a doctored manifest, and releases service-1: the release is rejected `invalid_contract` with that model as the only failing node, named in `reject_detail`, and its `parse` row in `per_node_results` is `failed`, carries the model's `original_file_path` and a detail that names `criticality`. `TestE2E_DbtContracts_ASeedWithAnUndeclaredHeaderColumnIsRejected` pins service-1's production manifest at a copy of its baseline in which the seed `e2e_schema.seed_table_2` no longer declares its `id` column (the compile extras beside it still record `id` in the CSV header), and releases service-2: the release is rejected `invalid_contract` with that seed as the only failing node, named in `reject_detail`, and its `parse` row in `per_node_results` is `failed`, carries the seed's `original_file_path` and a detail with `id is in the CSV header but not declared` |
| `ontology_test.go` | `TestE2E_Ontology` — the install's ontology switch and the ontology project, with the four contracts of `fixtures/py-probe` bound to the things of `fixtures/ontology/ontology.yaml`. With the switch off (the compose default), a release of kind `ontology` for `ontology-e2e` is refused with 400. The test then restarts topology-controller, release-controller and catalog with `ONTOLOGY_ENABLED=true` (catalog: `ONTOLOGY_BASE_URL=https://ontology.example/things`): the first two restart their container, and catalog, whose container runs the test, restarts its process only. The py-probe project is released and promoted, and its contract bundle says the switch is on and no ontology is released, as a warning. The ontology project then releases `ontology.yaml`: it promotes with nothing validated, its bundle names the project, release and SHA-256 of the file and the project it declares, each py-probe contract has a new version whose `ontology_version` is `<release_id>@sha256:<hex>`, and `continuo node data-contract e2e_schema.py_probe` and the live folder's file carry `domain: e2e` and `https://ontology.example/things/ProbeRow`. A release of `ontology_breaking.yaml` (the same file with `ProbeRow` renamed) is rejected `invalid_contract` with `e2e_schema.py_probe` as the only failing node, no remediation trigger, no new contract version, an unchanged live file and the first ontology still in its production pointer. A release of `service-1` (dbt) as kind `ontology` answers 409. The test turns the switch off again and deletes the production pointers of `ontology-e2e` and `svc-py-e2e` when it ends. If it is interrupted before that cleanup runs, restore the stack with `docker restart topology-controller release-controller`, then relaunch the three processes without the override (each takes the container's own environment, in which the switch is off and the base URL is empty): `docker exec -d topology-controller bash -c "cd /app && PYTHONPATH=/app/proto uv run python main.py > /tmp/mc.log 2>&1"`, `docker exec -d release-controller bash -c "cd /app/release-controller && go run . > /tmp/release-controller.log 2>&1"`, and, for catalog, whose container runs the suite and is not restarted, `docker exec catalog pkill -x catalog` and then `docker exec -d catalog bash -c "cd /app/catalog && go run . > /tmp/catalog.log 2>&1"` |
| `python_timeout_test.go` | `TestE2E_PythonNodeTimeout_ReachesRunControllerThroughTheRuntimeCLI` — one of the two python contracts of `fixtures/py-timeout` declares `timeoutMinutes: 90`. The test streams the fixture into the fixture image and runs the image's own `continuo-runtime pack` (`docker run`, no bind mount). Its `--image-packages` is the list the image's `continuo-runtime packages` prints. So the runtime release the chart defaults to writes the bundle, not the test. The bundle is uploaded and a Python release posted; once it promotes, run-controller's live release carries `timeout_seconds = 5400` on that node and NULL on every other node. The image must be built on a runtime that reads `.odcs.yaml` files (0.13.0 or later); `scripts/setup.sh` rebuilds it |
| `image_contract_test.go` | `TestE2E_PythonImageContractMismatch_FailsInvalid` — the release of python project `svc-py-mismatch-e2e` carries a bundle packed from `fixtures/py-probe/mismatch/py_mismatch.released.odcs.yaml` by the mismatch image's own `continuo-runtime pack`, and names the image `continuo-e2e-py-probe:mismatch` (the Dockerfile's `mismatch` target), which bakes `py_mismatch.baked.odcs.yaml` instead: the same node `e2e_schema.py_mismatch` with one read changed in meaning. The test first checks the `content_hash` the image's `continuo-runtime hash` computes from its baked contract differs from the one it computes from the released contract. The release is a bootstrap one, so it promotes without validation; `StartRun(e2e-py-mismatch)` then runs the node, the runtime harness finds the image cannot reproduce the `CONTENT_HASH` execution-controller passes, and the task fails `invalid` `image_contract_mismatch` after a single attempt. A deferred cleanup deletes the project's production pointer |
| `remediation_python_test.go` | `TestE2E_PythonValidationFailure_VerifiedFix`, `TestE2E_PythonValidationFailure_ReleaseCheckErrorFeedsNextTry`, `TestE2E_PythonParseFailure_VerifiedFix` — a rejected python node is repaired in its `.odcs.yaml` contract, verified by a real release check that stops at `passed`, and proposed for review; the second test proves a failed release check's error feeds the next try and reaches no classifier at all; the third drives the same lane from a parse-stage rejection (a read whose relation is not schema-qualified), where the rejection carries the parser's text inline and no log. Uses the fixture repository under `fixtures/py-remediation-repo`, which `stub-github` serves as a tarball; the test builds each release's bundle from the project's directory there rather than with `continuo-runtime pack`, since the parse scenario's read is one the runtime refuses |
| `dead_letter_test.go` | `TestDeadLetter_MalformedMessagesAreDeadLetteredNotDropped`, `TestDeadLetter_RedriveReachesOnlyItsGroup`, `TestDeadLetter_CLIListsAndRedrives`, `TestDeadLetter_BacklogMetricIsExposed` — a message no consumer can parse is stored by dead-letter-controller and served through `ListDeadLetters`; a redrive of it on `release.promoted:v1` is handled only by the group that failed it (the other groups acknowledge it without a handler) and a repeated redrive publishes nothing; the real `continuo dlq list`/`redrive` binary (built from the `./cli` mount with `GOWORK=off`) drives the same flow; the backlog gauge appears once a dead letter is open. dead-letter-controller is started by `start-components.sh` (health `:8096/ready`, gRPC `:50055`) |
| `egress_test.go` | `TestEgress_ProxyRefusesHostNotOnAllowlist`, `TestEgress_ProxyAdmitsListedHost`, `TestEgress_ChatTrafficGoesThroughProxy` — a `CONNECT` to a host off the allowlist is answered 403 and one to `stub-llm:9100` is answered 200; one chat turn raises the proxy's `continuo_egress_requests_total{decision="allowed"}` count, which shows agent-chat reaches its LLM through egress-proxy. The remediation tests' LLM and GitHub calls go through the proxy as well |
| `seed_topology_test.go` | `seedTopology` helper — announces the e2e DAG through release-controller's `announce-topology` (artifact in MinIO, next promotion seq, `release.promoted:v1`), which catalog applies to Neo4j and run-controller stores as its live release on the promotion path. It returns once both have applied it (`waitForAnnouncedTopology` in `announce_topology_test.go`): catalog's `:Meta` and run-controller's `live_release` name the announced release, and the release's `topology_nodes` in run-controller carry every schedule the DAG declares, which is what `StartRun` needs to know a schedule |
| `topology_by_reference_test.go` | `TestTopologyByReference_LatePromotionMovesNothing` — announces A, with a model of schedule `e2e-stale-probe`, then B without it, and replays A's `release.promoted:v1` entry under a fresh event id: both catalog's and run-controller's groups acknowledge it without a dead letter, catalog's live pointer and run-controller's live release stay at B, and for 10 s no node of run-controller's live release carries `e2e-stale-probe` and `ListSchedules` does not list it. `TestTopologyByReference_CorruptArtifactIsDeadLettered` — a promotion whose artifact does not match its checksum is dead-lettered as permanent by catalog's and run-controller's groups, and catalog's live pointer does not move |
| `seed_reconcile_test.go` | `TestSeedReconcile_LoadsAChangedSeedOnce` — announces the e2e DAG with `seed_table_1` carrying a new `content_hash`: run-controller plans one `promote_seed` run and dispatches the seed in the promotion's transaction, the seed loads with service-1's image and run-controller records the hash in `node_materializations`; announcing the same content again under a new release loads nothing. Restores the default DAG (no seed hashes) at the end |
| `seed_reload_test.go` | `TestSeedReload_RebuildsTheViewAndStopsOnAViewContinuoDoesNotOwn` — service-1's `seed_table_1_view` (a dbt view over `seed_table_1`, schedule `seed-view`) is rebuilt by a seed reload, on Postgres. The live release holds `seed_table_1` with the declared columns `id` and `legacy` and no view, and run-controller's `node_materializations` records the seed loaded at that version; a release of service-1 is posted and release-controller's notice (`GET /releases/{id}` and `continuo release notices`) says the load is a `full_refresh` that drops the table, removes `legacy`, rebuilds the view and `reloads`. Its promote-seed run full-refreshes the seed (`plan_reason` `columns changed`) and runs the view (`rebuild: dropped by the load of e2e_schema.seed_table_1`), as `continuo run show` prints, and the view exists. The test then creates `e2e_bi.seed_reader_base`, a view over the seed, and `e2e_bi.seed_reader`, a materialized view over that view, and moves the recorded version back: the seed reconcile's next promote-seed run stops the seed before dbt (`invalid`/`unowned_dependents`, its `dependents_check` listing both objects not allowed at depths 1 and 2 with completeness `exact`), skips the view and drops nothing. `continuo node full-refresh … --acknowledge` naming both objects reloads the seed and rebuilds the view, and the cascade drops the two objects. `t.Cleanup` drops `e2e_bi` and the view and restores the default DAG whatever the outcome |
| `ui_http_test.go` | HTTP assertions against the ui (`verifyUIHTTPAPI`) |
| `public_api_test.go`, `ci_token.go` | `TestPublicAPI_Authorization` — the public `/api/v1` release API's refusals (unbound repository, bootstrap without `allowBootstrap`, body/token repository mismatch, garbage token) and a CI token reading `live-release`. `ci_token.go` mints GitHub Actions-shaped tokens from `stub-github`, which the ui trusts as its CI issuer through `tests/e2e/ci-auth.json`; every release the suite submits goes through `/api/v1/releases` with such a token |
| `auth_oidc_test.go` | `TestAuthOIDC` — real OIDC login flow against Dex (auth-e2e profile), plus Dex password-grant bearers against `/api/v1`; skipped unless `UI_AUTH_HTTP_BASE` is set |
| `verify.go` | DAG-level assertions (execution-controller's k8s Jobs, dispatch of ready tasks, failure helpers) |
| `clients.go` | gRPC, Redis, Postgres, Neo4j, S3, and release-controller client setup |
| `helpers.go` | `pollUntil`, k8s Job and pod helpers (a pod's phase, start time and container termination times), `containsAll` |
| `cleanup.go` | Removes all test data from every data store |

## Blue/Green Release Tests

`release_promote_test.go` drives the dbt blue/green release check end-to-end through the ui's public API — `POST /api/v1/releases`, authenticated with a GitHub Actions-shaped OIDC token minted by `stub-github` (`ci_token.go`) — the same request a CD pipeline issues. Validation runs **real `continuo-python-runtime-<engine>` K8s Jobs in kind** (no dbt in the validation path — see the execution-controller doc's `CreateValidationJob`), exercising the full event chain:

```
POST /api/v1/releases → release.requested:v1 → topology-controller candidate parse
→ release.parsed:v1 → release-controller derives the changed-node set
→ validation.requested:v1 → execution-controller runs per-node validation jobs,
settling each terminal outcome in-process (the Job watch's observer → service/outcomes.Recorder,
same transaction as the observation) → validation.result:v1 (kind=complete)
→ release-controller promotes → release.promoted:v1
→ catalog loads the topology artifact and swaps the Neo4j topology
```

| Test | What it verifies |
|------|------------------|
| `TestE2E_ReleasePromote_ValidatesAndSwapsTopology` | Full cutover. The live release is pre-seeded with every candidate node **except** `rel_probe`, so the derived changed set is exactly `{rel_probe}` — a self-contained leaf that validates without touching production. Asserts promotion, the Neo4j topology swap, the release's contract bundle holding a `dbt` contract for `rel_probe` (its project and `content_hash`, `team.name` the model's owner, and a `schedule` property equal to the topology node's `schedule`), `continuo node data-contract e2e_schema.rel_probe` printing that contract (same `contract_hash`) with its `team.name`, and the per-node dbt log URI surfaced through the UI BFF. |
| `TestE2E_ReleasePromote_GatedIntraProjectUpstream` | A changed node with a changed **intra-project** upstream (`rel_probe_down` → `rel_probe_up`): the upstream builds into the candidate schema first (topological gating), the dependent validates against it, then the candidate schema is torn down. |
| `TestE2E_ReleasePromote_GatedCrossProjectUpstream` | A clean **cross-project** chain (`xprobe_down`@service-2 → `xprobe_up`@service-3) validates self-contained — the changed node and its cross-project upstream are both built empty in the candidate schema. |
| `TestE2E_ReleasePromote_BootstrapSkipsValidation` | `bootstrap:true` promotes directly, skipping validation, and seeds the live release. |
| `TestE2E_ReleasePromote_ATimeoutOnlyChangePromotesWithoutValidation` (`node_timeout_test.go`) | A release that changes only a node's `meta.timeout_minutes` promotes without validation: no node is in the changed set, no `validation.requested:v1` is published and no validation Job runs, and run-controller's live release stores the timeout (`topology_nodes.timeout_seconds`) on the declaring node only. `requireNoOtherNodeTimeout` checks the other nodes and tolerates exactly one timeout besides it: the 60 s that service-1's `e2e_schema.ftable_deadline` model declares for itself (the Python timeout test uses the same check). |

These tests read the dbt manifests that `setup.sh` uploads to MinIO S3, with the `compile-extras.json` it uploads beside each, and use the content-addressed image tags loaded into kind, so they need no setup beyond the standard blank-state harness. They seed the live release with the `content_hash` topology-controller's own parser gives each baseline node (`contentHashesOf` in `content_hash.go` runs `parse_manifest` in the `topology-controller` container over the manifest and its compile extras), so a node a release leaves unchanged reads as unchanged; a test that writes a manifest of its own copies the baseline's compile extras beside it (`copyBaselineExtras`). Compile and seed-build run real dbt, and each validation job runs a real `continuo-python-runtime-<engine>` Job — allow up to ~10 minutes per test, and expect a cold kind run to be slower than a warm one.

Run only the blue/green tests:

```bash
docker exec -e UI_HTTP_BASE=http://ui:8090 catalog \
  go test -v -count=1 -timeout 172m -run 'TestE2E_ReleasePromote' /app/tests/e2e/...
```

## Failure Path Test

`TestE2E_FailurePath_RerunRebasesBothFailureSubtrees` uses the 6-node `ftable_*` DAG. It verifies:

- `ftable_e` fails every attempt its budget allows (`max_retries` + 1, so 3 with the default `runs.taskMaxRetries` of 2), each recorded in `task_attempts` with failure class `task`, and reaches `failed` status
- Downstream node (`ftable_f`) is never deployed and is recorded `skipped`
- The scheduler is finalised as `FAILED`

The failure model `ftable_e` runs in the `service-2` Docker image but JOINs `public.wrong_name`, which does not exist. This causes the dbt run to fail at execution time on every attempt.

## Test Flow

### Happy Path
1. **Setup** - Initialize clients, verify components healthy
2. **Cleanup** - Remove any leftover test data
3. **Seed** - Create 6-node `ftable_*` DAG in Neo4j via catalog
4. **Start** - `RunService.StartRun` gRPC call → run-controller creates the run (kind `manual`)
5. **Verify Level 0** - Check root jobs (`ftable_a`, `ftable_b`) deploy and complete
6. **Verify Level 1** - Check `ftable_c` unlocks and executes
7. **Verify Level 2** - Check `ftable_d` and `ftable_e` execute
8. **Verify Level 3** - Check `ftable_f` executes
9. **Verify UI** - `GET /api/runs/:id` and `/api/runs/:id/tasks` return `status: "succeeded"` and ISO-8601 timestamps (skipped if `UI_HTTP_BASE` unset)
10. **Cleanup** - Remove all test data

### Manual run and rerun (seed schedule)
1. **Setup** - Initialize clients, verify components healthy, cleanup seed schedule data
2. **Topology** - Announce the e2e DAG (`seedTopology`), which returns once catalog and run-controller's live release hold it, with the seed schedule's models
3. **Run 1** - `POST /api/schedules/seed/manual-run` → wait for 3 seed tasks to succeed → scheduler SUCCEEDED
4. **Run 2** - `POST /api/schedules/seed/manual-run` again → wait for second run to complete → scheduler SUCCEEDED
5. **Cleanup** - Remove all seed schedule data

### Failure Path
1. **Setup** - Initialize clients, verify components healthy
2. **Seed** - Create the 6-node `ftable_*` DAG in Neo4j
3. **Start** - `RunService.StartRun` gRPC call
4. **Verify Level 0** - Root nodes (`ftable_a`, `ftable_b`) complete successfully
5. **Verify Level 1** - `ftable_c` executes; `ftable_d` and `ftable_e` are deployed
6. **Wait for ftable_e failure** - Poll until `tasks` holds `attempt = max_retries` and `status = 'failed'` and `task_attempts` holds one failed row of class `task` for each attempt from 0 to `max_retries`; `ftable_d` succeeds
7. **Verify the cascade** - Confirm `ftable_f` is never deployed and is recorded `skipped`
8. **Verify scheduler FAILED** - Poll `runs` until `status = 'failed'`
9. **Cleanup** - Remove all test data

## Expected Duration

- Happy path: ~1 minute
- Failure path: ~1 minute (3 attempts, each retry dispatched at once)
- Whole suite: about 70 minutes in the CI e2e job for every test but the seed reload test, which
  adds an estimated 20 to 30 minutes (a release, a 60 s seed reconcile pass and three seed loads).
  The job starts the stack cold and measures the whole suite on every run.
- Timeout: 172 minutes for the whole `tests/e2e/...` package (`go test -timeout`).
  Derived, not measured: the two longest per-test context ceilings (47m for
  the seed reload test, 40m for the remediation parse test) wedging in one
  run, plus 85m for the rest of the suite (a healthy cold run of it takes about
  70m). Every per-test context has to be
  able to fire before the package timeout, or a wedge prints a goroutine dump
  instead of a named assertion. `.github/workflows/ci.yml` uses the same value
  and allows 187 `timeout-minutes` around it.

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
- Check component health: `curl http://localhost:8082/health`

**An agent call fails with a 403 or a connection error to `egress-proxy:3128`:**
- egress-proxy refused the host. `docker logs egress-proxy | grep 'egress denied'` names the refused host and port; add them to `tests/e2e/config/egress-allowlist.yaml` and `docker compose restart egress-proxy` (the allowlist is read when the process starts), then start the process again with `docker exec -d egress-proxy bash -c "cd /app/egress-proxy && go run ."`.
- A component the agents should reach directly (a compose service name) belongs in that agent's `NO_PROXY` in `docker-compose.yml`.

**Neo4j not healthy / components fail to start:**
- Neo4j takes up to 90 seconds to become healthy. Wait for `docker compose ps` to show `neo4j` as `(healthy)` before running `bash tests/e2e/start-components.sh`.

**Test fails with kubectl errors:**
- Verify kind cluster is running: `kind get clusters`
- Check kubectl can reach cluster: `kubectl cluster-info`

**Test times out waiting for jobs:**
- Check k8s jobs: `kubectl get jobs -l schedule=e2e-schedule`
- Check job logs: `kubectl logs -l schedule=e2e-schedule`

**No pod is created in `default` (Jobs sit without pods):**
- A killed test binary can leave the ResourceQuota of the start-timeout or the dormant-node test behind: `kubectl get resourcequota -n default`. Remove it with `kubectl delete resourcequota e2e-no-pods -n default`; `bash tests/e2e/provision-k8s-test-env.sh` does the same.

**Test fails at the execution-controller health check:**
- Check pod status: `kubectl get pods -n default | grep controller`
- View pod logs: `kubectl logs -l app=execution-controller -n default`
- Re-run provisioning: `bash tests/e2e/provision-k8s-test-env.sh`

**Test fails with "table does not exist":**
- Verify the migrations job completed (Flyway, then each login's grants and check): `docker compose logs migrations`
- Check the execution outbox table exists: `docker compose exec postgres psql -U continuo_svc -d continuo_execution -c '\dt execution_outbox'`

**Components not starting:**
```bash
docker compose ps
docker logs catalog --tail 50
docker compose restart catalog run-controller
bash tests/e2e/start-components.sh
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

# Test from controller pod to run-controller
kubectl exec deployment/execution-controller -- nc -zv 172.17.0.1 50051
```

**Rebuild all k8s images and redeploy:**
```bash
bash tests/e2e/cleanup-execution-controller.sh
bash tests/e2e/provision-k8s-test-env.sh
```

**Neo4j fails to start ("Neo4j is already running"):**

This happens when Colima is stopped while neo4j is running, leaving a stale `store_lock`. Fix:
```bash
docker run --rm -v continuo_neo4j_data:/data alpine rm -f /data/databases/store_lock
docker-compose rm -f neo4j
docker-compose up -d --force-recreate neo4j
```

The harness runs inside the `catalog` container, whose `REDIS_*` and `POSTGRES_*` variables are catalog's own login, so it reads its own: `E2E_REDIS_PASSWORD` (default `continuo`, Redis's default user) and `E2E_POSTGRES_PASSWORD` (default `continuo`, the migration login `continuo_svc`). `E2E_WAREHOUSE_DB` (default `continuo_dbt`) selects the warehouse database the harness inspects for dbt-materialized tables; it signs in there as `E2E_WAREHOUSE_USER` (default `continuo_warehouse`) with `E2E_WAREHOUSE_PASSWORD` (default `local-continuo_warehouse`).

**Maintenance mode (`TestMaintenanceMode`):** starts a single-node run, then restarts the `run-controller` and `release-controller` containers with `MAINTENANCE_ENABLED=true` (relaunching `go run .` with the flag overridden; each component's output goes to `/tmp/<container>.log` inside its container). With maintenance on it asserts a new node run (via `ui`) and a new public release are refused with HTTP 503 and the `maintenance` code, and that the run already in flight still reaches `succeeded`. It then restarts both components with the flag off and asserts a new run is accepted and succeeds. The restarts take a minute or more each, and a `t.Cleanup` turns maintenance off again even on failure. If the test is interrupted before that cleanup runs, restore the stack with `docker restart run-controller release-controller && bash tests/e2e/start-components.sh`.
