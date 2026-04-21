# E2E System Test

Comprehensive end-to-end test validating the complete Continuo orchestration pipeline.

## Test Overview

Tests a 10-node diamond DAG executing through all services:
- `state` - Creates scheduler
- `orchestrator` - Identifies root nodes, publishes to query.model:v1
- `executor-controller` - Deploys k8s jobs
- `k8s-controller` - Monitors job status
- `dependency-controller` - Unlocks downstream dependencies
- `ui-service` - HTTP API verified to return correct scheduler/task data

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
bash e2e/start-services.sh

# 5. Start the ui-service container
docker compose up -d ui

# 6. Deploy pre-built controller images into the kind cluster
bash e2e/deploy-k8s-controllers.sh

# 7. Run the tests
docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator \
  go test -v -count=1 -timeout 25m /app/e2e/...

# 8. Clean up k8s resources
bash e2e/cleanup-k8s-controllers.sh
```

> `setup.sh` takes ~10 minutes on first run.

## Subsequent Runs

When the kind cluster and all Docker images already exist:

```bash
# Start (or restart) all docker-compose services
docker compose up -d

# Wait for neo4j to be healthy (can take up to 90s)
docker compose ps   # neo4j should show "(healthy)"

# Start Go service processes and wait for health
bash e2e/start-services.sh

# Start ui-service
docker compose up -d ui

# If any service images changed since last run, rebuild and reload into kind:
bash e2e/provision-k8s-test-env.sh
# Otherwise, if images are unchanged, just (re)deploy:
# bash e2e/deploy-k8s-controllers.sh

# Run the tests
docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator \
  go test -v -count=1 -timeout 25m /app/e2e/...

# Clean up k8s resources
bash e2e/cleanup-k8s-controllers.sh
```

Or use the convenience target (assumes kind cluster and images already exist):

```bash
make e2e-full
```

`make e2e-full` runs `docker compose up -d`, waits for health, starts services,
runs `provision-k8s-test-env.sh`, then executes the tests.

## Script Reference

| Script | What it does |
|--------|-------------|
| `scripts/setup.sh` | Full bootstrap: kind cluster + image builds + dbt manifests + kubeconfig + compose up |
| `e2e/start-services.sh` | Starts Go service processes inside containers (`go run`), waits for HTTP health |
| `e2e/provision-k8s-test-env.sh` | Rebuilds controller + dbt images, loads them into kind, regenerates kubeconfig, deploys k8s manifests |
| `e2e/deploy-k8s-controllers.sh` | Deploys pre-built images already in kind (no rebuild); used by CI after `setup.sh` |
| `e2e/cleanup-k8s-controllers.sh` | Removes controller Deployments from the kind cluster |

### How CI runs it

CI mirrors the blank-state flow above:

1. `bash scripts/setup.sh` (with `CI=true` set automatically by GitHub Actions)
2. Wait for containers to be running
3. Build + start `state` and `orchestrator` binaries (gRPC health check)
4. Run per-service unit tests
5. Build + start `manifest-controller`
6. `docker compose up -d ui`
7. `bash e2e/deploy-k8s-controllers.sh` (images pre-loaded by setup.sh)
8. `docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator go test -v -timeout 25m /app/e2e/...`
9. `bash e2e/cleanup-k8s-controllers.sh`

CI uses `deploy-k8s-controllers.sh` (not `provision-k8s-test-env.sh`) because
`setup.sh` already built and loaded all images into kind.

## UI Dashboard

After a successful test run the dashboard at **http://localhost:8090** shows live data:

```bash
open http://localhost:8090
```

You should see:
- 1 scheduler card with a **succeeded** badge
- A task table with **10 tasks** (the diamond DAG), each with **succeeded** status
- ISO-8601 timestamps showing when each task ran

## Architecture

The E2E test uses a hybrid setup:

**In docker-compose:**
- PostgreSQL, Redis, Neo4j (data stores)
- state, orchestrator, manifest-controller (services)

**In kind cluster (Kubernetes):**
- executor-controller (Deployment)
- k8s-controller (Deployment)
- dbt service jobs (run as k8s Jobs, one per table)

Controllers in kind connect to docker-compose services via docker bridge network (172.17.0.1).

## Test Structure

| File | Purpose |
|------|---------|
| `system_test.go` | `TestE2E_HappyPath_FullDAGExecution` |
| `trigger_test.go` | `TestTriggerSchedule_SeedRunAndRerun` — trigger seed schedule via TriggerSchedule RPC, wait for completion, re-trigger |
| `failure_test.go` | `TestE2E_FailurePath_NodeFailureDrainsSchedule` |
| `ui_http_test.go` | HTTP assertions against the ui-service (`verifyUIService`) |
| `verify.go` | DAG-level assertions (executor jobs, k8s jobs, dependency unlocking, failure helpers) |
| `clients.go` | gRPC, Redis, Postgres, Neo4j client setup |
| `fixtures.go` | 10-node diamond DAG definition, failure DAG fixture |
| `seed.go` | Seeds DAG nodes into Neo4j via the graph service |
| `helpers.go` | `pollUntil`, k8s job helpers, `containsAll` |
| `cleanup.go` | Removes all test data from every data store |

## Failure Path Test

`TestE2E_FailurePath_NodeFailureDrainsSchedule` uses the same 10-node diamond DAG but with `table_e` pointing to `service-3-broken`. It verifies:

- `table_e` exhausts 2 retries (3 total attempts) and reaches `failed` status
- Downstream nodes (`table_g`, `table_h`, `table_i`, `table_j`) are never deployed
- The scheduler is finalised as `FAILED`

`service-3-broken`'s `table_e` model raises a dbt compiler error unconditionally:
```sql
{{ exceptions.raise_compiler_error("intentional failure: service-3-broken table_e") }}
```
This makes `dbt run --select table_e` exit non-zero on every attempt.

## Test Flow

### Happy Path
1. **Setup** - Initialize clients, verify services healthy
2. **Cleanup** - Remove any leftover test data
3. **Seed** - Create 10-node DAG in Neo4j via graph service
4. **Trigger** - `ActivateSchedule` gRPC call → state creates scheduler record
5. **Verify Level 0** - Check 3 root jobs deploy and complete
6. **Verify Level 1** - Check dependency-controller unlocks and 3 jobs execute
7. **Verify Level 2** - Check 2 converging jobs execute
8. **Verify Level 3** - Check 2 final jobs execute
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
2. **Seed** - Create the same 10-node DAG with `table_e` using `service-3-broken`
3. **Trigger** - `ActivateSchedule` gRPC call
4. **Verify Level 0** - Root nodes complete successfully
5. **Verify Level 1 deployed** - All 3 level-1 jobs are deployed; `table_d` and `table_f` succeed
6. **Wait for table_e failure** - Poll until `retry_count = 3` and `status = 'failed'` in `task_tracker`
7. **Verify no downstream jobs** - Confirm `table_g`, `table_h`, `table_i`, `table_j` are never deployed
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
- Neo4j takes up to 90 seconds to become healthy. Wait for `docker compose ps` to show `neo4j` as `(healthy)` before running `bash e2e/start-services.sh`.

**Test fails with kubectl errors:**
- Verify kind cluster is running: `kind get clusters`
- Check kubectl can reach cluster: `kubectl cluster-info`

**Test times out waiting for jobs:**
- Check k8s jobs: `kubectl get jobs -l schedule=e2e-schedule`
- Check job logs: `kubectl logs -l schedule=e2e-schedule`

**Test fails at k8s controller health check:**
- Check pod status: `kubectl get pods -n default | grep controller`
- View pod logs: `kubectl logs -l app=executor-controller -n default`
- Re-run provisioning: `bash e2e/provision-k8s-test-env.sh`

**Test fails with "table does not exist":**
- Verify flyway migrations completed: `docker compose logs flyway-orchestrator`
- Check outbox table exists: `docker exec continuo-postgres-1 psql -U runner -d continuo_orchestrator -c "\dt"`

**Services not starting:**
```bash
docker compose ps
docker logs orchestrator --tail 50
docker compose restart orchestrator state
bash e2e/start-services.sh
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
bash e2e/cleanup-k8s-controllers.sh
bash e2e/provision-k8s-test-env.sh
```

**Neo4j fails to start ("Neo4j is already running"):**

This happens when Colima is stopped while neo4j is running, leaving a stale `store_lock`. Fix:
```bash
docker run --rm -v continuo_neo4j_data:/data alpine rm -f /data/databases/store_lock
docker-compose rm -f neo4j
docker-compose up -d --force-recreate neo4j
```
