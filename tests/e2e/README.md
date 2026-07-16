# E2E System Test

Comprehensive end-to-end test validating the complete Continuo orchestration pipeline.

## Test Overview

The suite covers both the run-orchestration pipeline and the dbt blue/green release pipeline. The happy-path test exercises a 6-node `ftable_*` DAG through every service:
- `state` - Creates scheduler, owns the run/task lifecycle
- `orchestrator` - Snapshots topology, identifies root nodes, unlocks downstream dependencies, publishes to query.model:v1 (owns the responsibilities of the former `graph` and `dependency-controller` services)
- `executor-controller` - Deploys k8s jobs
- `k8s-controller` - Monitors job status
- `manifest-controller` - Parses dbt manifests (candidate + legacy paths)
- `release-controller` - Owns the blue/green candidate-release lifecycle and `current_prod`
- `ui-service` - HTTP API verified to return correct scheduler/task/release data

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

# 5. Start the ui-service container
docker compose up -d ui

# 6. Deploy pre-built controller images into the kind cluster
bash tests/e2e/deploy-k8s-controllers.sh

# 7. Run the tests
docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator \
  go test -v -count=1 -timeout 25m /app/tests/e2e/...

# 8. Clean up k8s resources
bash tests/e2e/cleanup-k8s-controllers.sh
```

> `setup.sh` takes ~10 minutes on first run.

## Subsequent Runs

When the kind cluster and all Docker images already exist:

```bash
# Tear down stale containers — including any left over from the main project directory
# (the main project uses bare container names like /state, /ui that conflict with this worktree)
docker rm -f state orchestrator executor-controller k8s-controller ui 2>/dev/null || true
docker compose down

# Start (or restart) all docker-compose services
docker compose up -d

# Wait for neo4j to be healthy (can take up to 90s)
docker compose ps   # neo4j should show "(healthy)"

# Start Go service processes and wait for health
bash tests/e2e/start-services.sh

# Start ui-service
docker compose up -d ui

# If any service images changed since last run, rebuild and reload into kind:
bash tests/e2e/provision-k8s-test-env.sh
# Otherwise, if images are unchanged, just (re)deploy:
# bash tests/e2e/deploy-k8s-controllers.sh

# Run the tests
docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator \
  go test -v -count=1 -timeout 25m /app/tests/e2e/...

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

The auth suite exercises the real OIDC login flow (ui-service in `oidc` mode
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
covered by ui-service unit tests.

## Script Reference

| Script | What it does |
|--------|-------------|
| `scripts/setup.sh` | Full bootstrap: kind cluster + image builds + dbt manifests + kubeconfig + compose up |
| `tests/e2e/start-services.sh` | Starts Go service processes inside containers (`go run`), waits for HTTP health |
| `tests/e2e/provision-k8s-test-env.sh` | Rebuilds controller + dbt images, loads them into kind, regenerates kubeconfig, deploys k8s manifests |
| `tests/e2e/deploy-k8s-controllers.sh` | Deploys pre-built images already in kind (no rebuild); used by CI after `setup.sh` |
| `tests/e2e/cleanup-k8s-controllers.sh` | Removes controller Deployments from the kind cluster |

### How CI runs it

CI mirrors the blank-state flow above:

1. `bash scripts/setup.sh` (with `CI=true` set automatically by GitHub Actions)
2. Wait for containers to be running
3. Build + start `state` and `orchestrator` binaries (gRPC health check)
4. Run per-service unit tests
5. Build + start `manifest-controller`
6. `docker compose up -d ui`
7. `bash tests/e2e/deploy-k8s-controllers.sh` (images pre-loaded by setup.sh)
8. `docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator go test -v -timeout 25m /app/tests/e2e/...`
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
- PostgreSQL, Redis, Neo4j (data stores), LocalStack (S3 — dbt manifests)
- state, orchestrator, manifest-controller, release-controller (services)

**In kind cluster (Kubernetes):**
- executor-controller (Deployment)
- k8s-controller (Deployment)
- dbt service jobs (run as k8s Jobs, one per table)

Controllers in kind connect to docker-compose services via docker bridge network (172.17.0.1).

## Test Structure

| File | Purpose |
|------|---------|
| `system_test.go` | `TestE2E_HappyPath_FullDAGExecution` — 6-node `ftable_*` DAG through every service |
| `trigger_test.go` | `TestTriggerSchedule_SeedRunAndRerun` — trigger seed schedule via TriggerSchedule RPC, wait for completion, re-trigger |
| `failure_test.go` | `TestE2E_FailurePath_RerunRebasesBothFailureSubtrees` — two parallel failing subtrees, then a rerun rebases both |
| `rebase_test.go` | `TestRebaseFromFailedRun`, `TestRebaseAllInheritedFinalizes` |
| `single_node_run_test.go` | `TestSingleNodeRunLatest`, `TestSingleNodeRunStale`, `TestSingleNodeRunTargetNotFound` |
| `cancel_test.go` | `TestCancelMidwayAndRetrigger` — cancel an in-flight schedule, then re-trigger |
| `empty_dag_test.go` | `TestEmptyCronDAG_FinalisesAsFailed` — empty projection fails the run fast |
| `dag_latest_snapshot_test.go` | `TestE2E_DAGLatestSnapshot` |
| `topology_versioning_test.go` | `TestTopologyVersioning_MidRunIsolation` — lazy generation switch (in-flight runs immutable) |
| `watchdog_termination_test.go` | `TestWatchdog_TerminatesStuckSchedule` |
| `release_promote_test.go` | dbt blue/green release tests — see [Blue/Green Release Tests](#bluegreen-release-tests) |
| `seed_topology_test.go` | `seedTopology` helper — publishes `release.promoted:v1` to establish the e2e DAG in Neo4j (the kept production path) |
| `ui_http_test.go` | HTTP assertions against the ui-service (`verifyUIService`) |
| `auth_oidc_test.go` | `TestAuthOIDC` — real OIDC login flow against Dex (auth-e2e profile); skipped unless `UI_AUTH_HTTP_BASE` is set |
| `verify.go` | DAG-level assertions (executor jobs, k8s jobs, dependency unlocking, failure helpers) |
| `clients.go` | gRPC, Redis, Postgres, Neo4j, S3, and release-controller client setup |
| `helpers.go` | `pollUntil`, k8s job helpers, `containsAll` |
| `cleanup.go` | Removes all test data from every data store |

## Blue/Green Release Tests

`release_promote_test.go` drives the dbt blue/green release pipeline end-to-end via the production entry point — `POST /releases`, the exact request CI's `deploy.yml` issues. Validation runs **real `dbt --empty` jobs as K8s Jobs in kind**, exercising the full event chain:

```
POST /releases → release.requested:v1 → manifest-controller candidate parse
→ manifest.loaded.candidate:v1 → release-controller derives the changed-node set
→ validation.requested:v1 → executor/k8s run per-node validation jobs
→ validation.node.completed:v1 → validation.result:v1 (kind=complete)
→ release-controller promotes → release.promoted:v1
→ orchestrator swaps the Neo4j topology
```

| Test | What it verifies |
|------|------------------|
| `TestE2E_ReleasePromote_ValidatesAndSwapsTopology` | Full cutover. `current_prod` is pre-seeded with every candidate node **except** `rel_probe`, so the derived changed set is exactly `{rel_probe}` — a self-contained leaf that validates without touching production. Asserts promotion, the Neo4j topology swap, and the per-node dbt log URI surfaced through the UI BFF. |
| `TestE2E_ReleasePromote_GatedIntraServiceUpstream` | A changed node with a changed **intra-service** upstream (`rel_probe_down` → `rel_probe_up`): the upstream builds into the candidate schema first (topological gating), the dependent validates against it, then the candidate schema is torn down. |
| `TestE2E_ReleasePromote_GatedCrossServiceUpstream` | A clean **cross-service** chain (`xprobe_down`@service-2 → `xprobe_up`@service-3) validates self-contained — the changed node and its cross-service upstream are both built empty in the candidate schema. |
| `TestE2E_ReleasePromote_BootstrapSkipsValidation` | `bootstrap:true` promotes directly, skipping validation, and seeds `current_prod`. |

These tests read the dbt manifests that `setup.sh` uploads to LocalStack S3 and use the content-addressed image tags loaded into kind, so they need no setup beyond the standard blank-state harness. Each validation job is real dbt — allow up to ~10 minutes per test, and expect a cold kind run to be slower than a warm one.

Run only the blue/green tests:

```bash
docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator \
  go test -v -count=1 -timeout 25m -run 'TestE2E_ReleasePromote' /app/tests/e2e/...
```

## Failure Path Test

`TestE2E_FailurePath_NodeFailureDrainsSchedule` uses the 6-node `ftable_*` DAG. It verifies:

- `ftable_e` exhausts 2 retries (3 total attempts) and reaches `failed` status
- Downstream node (`ftable_f`) is never deployed
- The scheduler is finalised as `FAILED`

The failure model `ftable_e` runs in the `service-2` Docker image but JOINs `public.wrong_name`, which does not exist. This causes the dbt run to fail at execution time on every attempt.

## Test Flow

### Happy Path
1. **Setup** - Initialize clients, verify services healthy
2. **Cleanup** - Remove any leftover test data
3. **Seed** - Create 6-node `ftable_*` DAG in Neo4j via graph service
4. **Trigger** - `ActivateSchedule` gRPC call → state creates scheduler record
5. **Verify Level 0** - Check root jobs (`ftable_a`, `ftable_b`) deploy and complete
6. **Verify Level 1** - Check `ftable_c` unlocks and executes
7. **Verify Level 2** - Check `ftable_d` and `ftable_e` execute
8. **Verify Level 3** - Check `ftable_f` executes
9. **Verify UI** - `GET /api/schedulers` and `/api/schedulers/:id/tasks` return `status: "succeeded"` and ISO-8601 timestamps (skipped if `UI_HTTP_BASE` unset)
10. **Cleanup** - Remove all test data

### TriggerSchedule Run-and-Rerun (seed schedule)
1. **Setup** - Initialize clients, verify services healthy, cleanup seed schedule data
2. **Graph Load** - Trigger manifest-controller, verify seed nodes and catalog
3. **Run 1** - `TriggerSchedule("seed")` → wait for 3 seed tasks to succeed → scheduler SUCCEEDED
4. **Run 2** - `TriggerSchedule("seed")` again → wait for second run to complete → scheduler SUCCEEDED
5. **Cleanup** - Remove all seed schedule data

### Failure Path
1. **Setup** - Initialize clients, verify services healthy
2. **Seed** - Create the 6-node `ftable_*` DAG in Neo4j
3. **Trigger** - `ActivateSchedule` gRPC call
4. **Verify Level 0** - Root nodes (`ftable_a`, `ftable_b`) complete successfully
5. **Verify Level 1** - `ftable_c` executes; `ftable_d` and `ftable_e` are deployed
6. **Wait for ftable_e failure** - Poll until `retry_count = 2` and `status = 'failed'` in `task_tracker`; `ftable_d` succeeds
7. **Verify no downstream jobs** - Confirm `ftable_f` is never deployed
8. **Verify scheduler FAILED** - Poll `scheduler_tracker` until `status = 'failed'`
9. **Cleanup** - Remove all test data

## Expected Duration

- Happy path: ~1 minute
- Failure path: ~1 minute (3 retries + retry delay)
- Timeout: 25 minutes per test

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

**Neo4j not healthy / services fail to start:**
- Neo4j takes up to 90 seconds to become healthy. Wait for `docker compose ps` to show `neo4j` as `(healthy)` before running `bash tests/e2e/start-services.sh`.

**Test fails with kubectl errors:**
- Verify kind cluster is running: `kind get clusters`
- Check kubectl can reach cluster: `kubectl cluster-info`

**Test times out waiting for jobs:**
- Check k8s jobs: `kubectl get jobs -l schedule=e2e-schedule`
- Check job logs: `kubectl logs -l schedule=e2e-schedule`

**Test fails at k8s controller health check:**
- Check pod status: `kubectl get pods -n default | grep controller`
- View pod logs: `kubectl logs -l app=executor-controller -n default`
- Re-run provisioning: `bash tests/e2e/provision-k8s-test-env.sh`

**Test fails with "table does not exist":**
- Verify flyway migrations completed: `docker compose logs flyway-orchestrator`
- Check outbox table exists: `docker exec continuo-postgres-1 psql -U runner -d continuo_orchestrator -c "\dt"`

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
docker exec executor-controller ls -la /root/.kube/
```

**Controllers not starting:**
```bash
kubectl get pods -n default | grep controller
kubectl logs -l app=executor-controller -n default
kubectl logs -l app=k8s-controller -n default
docker exec continuo-control-plane crictl images | grep continuo
```

**Network connectivity issues:**
```bash
# Test from controller pod to postgres
kubectl exec deployment/executor-controller -- nc -zv 172.17.0.1 5432

# Test from controller pod to state service
kubectl exec deployment/executor-controller -- nc -zv 172.17.0.1 50051
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
