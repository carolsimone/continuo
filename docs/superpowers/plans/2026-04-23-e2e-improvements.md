# E2E Test Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename `e2e/` to `tests/e2e/`, replace `service-3-broken` with inline-SQL failure models, simplify the failure test to 6 nodes, and remove `rerun_test.go`.

**Architecture:** Direct Neo4j seeding is kept for the failure path (bypasses manifest-controller). Failure behaviour is now expressed in SQL (`LEFT JOIN public.wrong_name`) inside the real service-2 image rather than a separate broken Docker image. Cross-service SQL references follow the existing `e2e_schema.table_name` convention — `{{ ref() }}` is used only for intra-service deps (seeds inside the same project), matching every existing model in the codebase.

**Tech Stack:** Go 1.25, dbt (postgres adapter), bash, kind/kubectl, GitHub Actions CI.

---

## File Map

**Create:**
- `tests/e2e/system_fixtures.go` — `getDiamondDAG()` and `getDAGLevels()` (moved from `failure_fixtures.go`, used by happy-path test)
- `dbt/services/service-1/models/ftable_a.sql`
- `dbt/services/service-1/models/ftable_b.sql`
- `dbt/services/service-3/models/ftable_c.sql`
- `dbt/services/service-3/models/ftable_f.sql`
- `dbt/services/service-2/models/ftable_d.sql`
- `dbt/services/service-2/models/ftable_e.sql`

**Move:**
- `e2e/` → `tests/e2e/` (entire directory, via `git mv`)

**Modify:**
- `tests/e2e/go.mod` — relative replace paths `../` → `../../`
- `tests/e2e/failure_fixtures.go` — replace 13-node diamond DAG with 6-node failure DAG; remove `getDiamondDAG`, `getDAGLevels`, `failureTableServiceMap`, `getFailureServiceNameForTable`
- `tests/e2e/failure_test.go` — simplify body to 6-step failure assertion
- `tests/e2e/verify.go` — rename `verifyTableEExhaustedRetries` → `verifyNodeExhaustedRetries(tableName string)`
- `tests/e2e/README.md` — update all paths and remove rerun/service-3-broken references
- `dbt/services/service-1/dbt_project.yml` — remove global tag, add per-model tags
- `dbt/services/service-2/dbt_project.yml` — same
- `dbt/services/service-3/dbt_project.yml` — same
- `scripts/setup.sh` — remove `service-3-broken` build and kind-load lines
- `tests/e2e/provision-k8s-test-env.sh` — same
- `.github/workflows/ci.yml` — update `e2e/` path references to `tests/e2e/`

**Delete:**
- `dbt/services/service-3-broken/` (entire directory)
- `dbt/services-fixed/` (entire directory)
- `tests/e2e/rerun_test.go`

---

## Task 1: Rename `e2e/` → `tests/e2e/`

**Files:**
- Move: `e2e/` → `tests/e2e/`
- Modify: `tests/e2e/go.mod`

- [ ] **Step 1: Move the directory**

```bash
git mv e2e tests/e2e
```

- [ ] **Step 2: Update `go.mod` relative replace paths**

In `tests/e2e/go.mod`, the `replace` block references `../orchestrator` and `../state`. After the move the repo root is two levels up, so update:

```
replace (
	github.com/carolsimone/continuo/orchestrator => ../../orchestrator
	github.com/carolsimone/continuo/state => ../../state
)
```

- [ ] **Step 3: Verify compilation from inside the orchestrator container**

```bash
docker exec orchestrator go build ./tests/e2e/...
```

Expected: exits 0, no output.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e go.mod 2>/dev/null; git add -A tests/
git commit -m "refactor(e2e): rename e2e/ to tests/e2e/"
```

---

## Task 2: Update path references in scripts and CI

**Files:**
- Modify: `scripts/setup.sh`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Update CI workflow**

In `.github/workflows/ci.yml`, three lines reference the old path:

| Old | New |
|-----|-----|
| `bash e2e/deploy-k8s-controllers.sh` | `bash tests/e2e/deploy-k8s-controllers.sh` |
| `go test -v -timeout 25m /app/e2e/...` | `go test -v -timeout 25m /app/tests/e2e/...` |
| `bash e2e/cleanup-k8s-controllers.sh` | `bash tests/e2e/cleanup-k8s-controllers.sh` |

The `start-services.sh` reference in CI (if any) uses an absolute path computed at runtime — no change needed there.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: update e2e path references to tests/e2e/"
```

---

## Task 3: Remove `service-3-broken` and `services-fixed`

**Files:**
- Delete: `dbt/services/service-3-broken/`
- Delete: `dbt/services-fixed/`
- Modify: `scripts/setup.sh`
- Modify: `tests/e2e/provision-k8s-test-env.sh`

- [ ] **Step 1: Delete the directories**

```bash
git rm -r dbt/services/service-3-broken dbt/services-fixed
```

- [ ] **Step 2: Remove `service-3-broken` from `scripts/setup.sh`**

Remove these two lines (lines 72 and 87):

```bash
# Remove:
DOCKER_BUILDKIT=1 docker build -t service-3-broken:latest dbt/services/service-3-broken/
# and:
kind load docker-image service-3-broken:latest --name ${CLUSTER_NAME} &
```

The `wait` after the parallel loads stays — it still waits on service-1, service-2, service-3, and the two controller images.

- [ ] **Step 3: Remove `service-3-broken` from `tests/e2e/provision-k8s-test-env.sh`**

Remove these two lines (lines 63 and 80):

```bash
# Remove:
DOCKER_BUILDKIT=1 docker build -t service-3-broken:latest dbt/services/service-3-broken/ || { log_error "failed to build service-3-broken"; exit 1; }
# and:
kind load docker-image service-3-broken:latest --name continuo || { log_error "Failed to load service-3-broken into kind"; exit 1; }
```

- [ ] **Step 4: Commit**

```bash
git add scripts/setup.sh tests/e2e/provision-k8s-test-env.sh
git commit -m "chore(dbt): remove service-3-broken and services-fixed"
```

---

## Task 4: Add 6 failure dbt model SQL files

**Files:**
- Create: `dbt/services/service-1/models/ftable_a.sql`
- Create: `dbt/services/service-1/models/ftable_b.sql`
- Create: `dbt/services/service-3/models/ftable_c.sql`
- Create: `dbt/services/service-3/models/ftable_f.sql`
- Create: `dbt/services/service-2/models/ftable_d.sql`
- Create: `dbt/services/service-2/models/ftable_e.sql`

Cross-service references follow the existing convention: all models in this codebase reference tables from other services using the direct `e2e_schema.table_name` SQL pattern (see `service-3/models/table_d.sql`, `service-2/models/table_g.sql`). `{{ ref() }}` is used only for intra-service deps (e.g., service-1 models referencing seeds in the same project).

- [ ] **Step 1: Create `dbt/services/service-1/models/ftable_a.sql`**

```sql
{{ config(materialized='table') }}
SELECT 1 AS id
```

- [ ] **Step 2: Create `dbt/services/service-1/models/ftable_b.sql`**

```sql
{{ config(materialized='table') }}
SELECT 1 AS id
```

- [ ] **Step 3: Create `dbt/services/service-3/models/ftable_c.sql`**

Depends on `ftable_a` and `ftable_b` (both in service-1 → cross-service → direct SQL):

```sql
{{ config(materialized='table') }}
SELECT a.id
FROM e2e_schema.ftable_a a
LEFT JOIN e2e_schema.ftable_b b ON a.id = b.id
```

- [ ] **Step 4: Create `dbt/services/service-2/models/ftable_d.sql`**

Depends on `ftable_c` (in service-3 → cross-service → direct SQL):

```sql
{{ config(materialized='table') }}
SELECT id FROM e2e_schema.ftable_c
```

- [ ] **Step 5: Create `dbt/services/service-2/models/ftable_e.sql`**

Depends on `ftable_c` (cross-service) and deliberately joins a non-existent table to force failure:

```sql
{{ config(materialized='table') }}
SELECT c.id
FROM e2e_schema.ftable_c c
LEFT JOIN public.wrong_name w ON c.id = w.id
```

- [ ] **Step 6: Create `dbt/services/service-3/models/ftable_f.sql`**

Depends on `ftable_d` and `ftable_e` (both in service-2 → cross-service → direct SQL). This node must never run when `ftable_e` fails.

```sql
{{ config(materialized='table') }}
SELECT d.id
FROM e2e_schema.ftable_d d
LEFT JOIN e2e_schema.ftable_e e ON d.id = e.id
```

- [ ] **Step 7: Commit**

```bash
git add dbt/services/service-1/models/ftable_a.sql \
        dbt/services/service-1/models/ftable_b.sql \
        dbt/services/service-3/models/ftable_c.sql \
        dbt/services/service-3/models/ftable_f.sql \
        dbt/services/service-2/models/ftable_d.sql \
        dbt/services/service-2/models/ftable_e.sql
git commit -m "feat(dbt): add failure-tagged ftable_* models across services"
```

---

## Task 5: Update `dbt_project.yml` for all three services

The current configs apply `+tags: ["e2e-schedule"]` globally to all models. Adding `ftable_*` models to the same service would inherit that tag. To ensure each model carries exactly one schedule tag, remove the global tag and set it per model instead.

**Files:**
- Modify: `dbt/services/service-1/dbt_project.yml`
- Modify: `dbt/services/service-2/dbt_project.yml`
- Modify: `dbt/services/service-3/dbt_project.yml`

- [ ] **Step 1: Replace `dbt/services/service-1/dbt_project.yml`**

```yaml
name: service_1
version: '1.0.0'
config-version: 2
profile: default

model-paths: ["models"]
seed-paths: ["seeds"]

seeds:
  service_1:
    +full_refresh: false
    +meta:
      owner: "test"
      criticality: "CORE"

models:
  service_1:
    +materialized: table
    +meta:
      owner: "test"
      criticality: "CORE"
    table_a:
      +tags: ["e2e-schedule"]
    table_b:
      +tags: ["e2e-schedule"]
    table_c:
      +tags: ["e2e-schedule"]
    ftable_a:
      +tags: ["e2e-schedule-failure"]
    ftable_b:
      +tags: ["e2e-schedule-failure"]
```

- [ ] **Step 2: Replace `dbt/services/service-2/dbt_project.yml`**

```yaml
name: service_2
version: '1.0.0'
config-version: 2
profile: default

model-paths: ["models"]

models:
  service_2:
    +materialized: table
    +meta:
      owner: "test"
      criticality: "CORE"
    table_g:
      +tags: ["e2e-schedule"]
    table_h:
      +tags: ["e2e-schedule"]
    ftable_d:
      +tags: ["e2e-schedule-failure"]
    ftable_e:
      +tags: ["e2e-schedule-failure"]
```

- [ ] **Step 3: Replace `dbt/services/service-3/dbt_project.yml`**

```yaml
name: service_3
version: '1.0.0'
config-version: 2
profile: default

model-paths: ["models"]

models:
  service_3:
    +materialized: table
    +meta:
      owner: "test"
      criticality: "CORE"
    table_d:
      +tags: ["e2e-schedule"]
    table_e:
      +tags: ["e2e-schedule"]
    table_f:
      +tags: ["e2e-schedule"]
    table_i:
      +tags: ["e2e-schedule"]
    table_j:
      +tags: ["e2e-schedule"]
    ftable_c:
      +tags: ["e2e-schedule-failure"]
    ftable_f:
      +tags: ["e2e-schedule-failure"]
```

- [ ] **Step 4: Commit**

```bash
git add dbt/services/service-1/dbt_project.yml \
        dbt/services/service-2/dbt_project.yml \
        dbt/services/service-3/dbt_project.yml
git commit -m "feat(dbt): add e2e-schedule-failure tag to ftable_* models"
```

---

## Task 6: Create `system_fixtures.go`; strip diamond-DAG helpers from `failure_fixtures.go`

`getDiamondDAG()` and `getDAGLevels()` are used only by the happy-path test (`verifyFullDAGExecution` in `verify.go`). They do not belong in `failure_fixtures.go`. Move them to a new file and clean up the leftovers.

**Files:**
- Create: `tests/e2e/system_fixtures.go`
- Modify: `tests/e2e/failure_fixtures.go`

- [ ] **Step 1: Create `tests/e2e/system_fixtures.go`**

```go
package e2e

// getDiamondDAG returns the 13-node diamond DAG (3 seeds + 10 models) used by the happy-path test.
func getDiamondDAG() []dagNode {
	return []dagNode{
		{Name: "seed_table_1", Dependencies: nil, ServiceName: "service-1", NodeType: "dbt-seed"},
		{Name: "seed_table_2", Dependencies: nil, ServiceName: "service-1", NodeType: "dbt-seed"},
		{Name: "seed_table_3", Dependencies: nil, ServiceName: "service-1", NodeType: "dbt-seed"},
		{Name: "table_a", Dependencies: []string{"seed_table_1"}, ServiceName: "service-1"},
		{Name: "table_b", Dependencies: []string{"seed_table_2"}, ServiceName: "service-1"},
		{Name: "table_c", Dependencies: []string{"seed_table_3"}, ServiceName: "service-1"},
		{Name: "table_d", Dependencies: []string{"table_a", "table_b"}, ServiceName: "service-3"},
		{Name: "table_e", Dependencies: []string{"table_b", "table_c"}, ServiceName: "service-3"},
		{Name: "table_f", Dependencies: []string{"table_a", "table_c"}, ServiceName: "service-3"},
		{Name: "table_g", Dependencies: []string{"table_d", "table_e"}, ServiceName: "service-2"},
		{Name: "table_h", Dependencies: []string{"table_e", "table_f"}, ServiceName: "service-2"},
		{Name: "table_i", Dependencies: []string{"table_g", "table_h"}, ServiceName: "service-3"},
		{Name: "table_j", Dependencies: []string{"table_g", "table_h"}, ServiceName: "service-3"},
	}
}

// getDAGLevels returns happy-path DAG nodes grouped by execution level.
func getDAGLevels() [][]string {
	return [][]string{
		{"seed_table_1", "seed_table_2", "seed_table_3"},
		{"table_a", "table_b", "table_c"},
		{"table_d", "table_e", "table_f"},
		{"table_g", "table_h"},
		{"table_i", "table_j"},
	}
}
```

- [ ] **Step 2: Remove `getDiamondDAG`, `getDAGLevels`, `failureTableServiceMap`, and `getFailureServiceNameForTable` from `tests/e2e/failure_fixtures.go`**

Delete the following blocks from `failure_fixtures.go`:

- The `getDiamondDAG()` function (lines 21–37)
- The `getDAGLevels()` function (lines 39–48)
- The `failureTableServiceMap` var (lines 149–155)
- The `getFailureServiceNameForTable()` function (lines 157–163)

Also update `getFailureDAG()` (currently on line 51) — the new version in the next task replaces it entirely.

- [ ] **Step 3: Verify compilation**

```bash
docker exec orchestrator go build ./tests/e2e/...
```

Expected: exits 0.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/system_fixtures.go tests/e2e/failure_fixtures.go
git commit -m "refactor(e2e): move diamond DAG helpers to system_fixtures.go"
```

---

## Task 7: Replace `getFailureDAG()` with 6-node version in `failure_fixtures.go`

**Files:**
- Modify: `tests/e2e/failure_fixtures.go`

- [ ] **Step 1: Replace `getFailureDAG()` with the 6-node version**

Remove the old `getFailureDAG()` function entirely and replace it with:

```go
// getFailureDAG returns the 6-node DAG used by the failure-path test.
// ftable_e runs against service-2 but JOINs public.wrong_name, causing it to
// fail after exhausting retries. ftable_f is downstream and must never run.
func getFailureDAG() []dagNode {
	return []dagNode{
		{Name: "ftable_a", Dependencies: nil, ServiceName: "service-1"},
		{Name: "ftable_b", Dependencies: nil, ServiceName: "service-1"},
		{Name: "ftable_c", Dependencies: []string{"ftable_a", "ftable_b"}, ServiceName: "service-3"},
		{Name: "ftable_d", Dependencies: []string{"ftable_c"}, ServiceName: "service-2"},
		{Name: "ftable_e", Dependencies: []string{"ftable_c"}, ServiceName: "service-2"},
		{Name: "ftable_f", Dependencies: []string{"ftable_d", "ftable_e"}, ServiceName: "service-3"},
	}
}
```

Also update the constants block at the bottom of the file. The `failureTestScheduleName` stays `"e2e-schedule-failure"`. Remove `failureTestOwner` if it is no longer referenced after the fixture cleanup:

```go
const (
	failureTestScheduleName = "e2e-schedule-failure"
	failureTestSchemaName   = "e2e_schema"
	failureTestOwner        = "test"
)
```

(`failureTestOwner` is still used by `seedNodes` — keep it.)

- [ ] **Step 2: Verify compilation**

```bash
docker exec orchestrator go build ./tests/e2e/...
```

Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/failure_fixtures.go
git commit -m "refactor(e2e): replace 13-node failure DAG with 6-node ftable_* DAG"
```

---

## Task 8: Generalize `verifyTableEExhaustedRetries` in `verify.go`

**Files:**
- Modify: `tests/e2e/verify.go`

- [ ] **Step 1: Rename and parameterise the function**

In `tests/e2e/verify.go`, replace the `verifyTableEExhaustedRetries` function (lines 218–241) with:

```go
// verifyNodeExhaustedRetries polls state DB until the given node has retry_count = 2
// and status = 'failed', confirming all 2 retries were exhausted (3 total attempts).
func verifyNodeExhaustedRetries(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
	tableName string,
) {
	t.Helper()
	pollUntil(t, ctx, 10*time.Minute, 5*time.Second, func() (bool, error) {
		var retryCount int
		var status string
		err := clients.stateDB.QueryRow(`
			SELECT retry_count, status
			FROM task_tracker
			WHERE schedule_id = $1 AND table_name = $2
		`, schedulerID, tableName).Scan(&retryCount, &status)
		if err != nil {
			return false, err
		}
		return retryCount == 2 && status == "failed", nil
	}, fmt.Sprintf("Timeout waiting for %s to exhaust 2 retries and reach failed status", tableName))

	t.Logf("✅ %s exhausted 2 retries and is permanently failed (3 total attempts)", tableName)
}
```

- [ ] **Step 2: Verify compilation**

```bash
docker exec orchestrator go build ./tests/e2e/...
```

Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/verify.go
git commit -m "refactor(e2e): generalise verifyTableEExhaustedRetries to accept tableName"
```

---

## Task 9: Simplify `failure_test.go`

**Files:**
- Modify: `tests/e2e/failure_test.go`

- [ ] **Step 1: Rewrite `tests/e2e/failure_test.go`**

Replace the entire file:

```go
package e2e

import (
	"context"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestE2E_FailurePath_NodeFailureDrainsSchedule verifies that when ftable_e
// permanently fails (exhausts 2 retries / 3 total attempts), its downstream
// node ftable_f is never deployed and the scheduler is finalised as FAILED.
//
// DAG (all nodes seeded directly into Neo4j — bypasses manifest-controller):
//
//	ftable_a (service-1)  ftable_b (service-1)
//	              \           /
//	           ftable_c (service-3)
//	          /                    \
//	ftable_d (service-2)   ftable_e (service-2, FAILS — JOINs public.wrong_name)
//	          \                    /
//	           ftable_f (service-3)  ← never deployed
func TestE2E_FailurePath_NodeFailureDrainsSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	defer func() {
		cleanupTestData(t, ctx, clients, failureTestScheduleName)
	}()

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, failureTestScheduleName)

	t.Log("Seeding 6-node failure DAG...")
	seedFailureDAG(t, ctx, clients)

	t.Log("Activating schedule...")
	schedulerIDStr := createAndActivateFailureScheduler(t, ctx, clients)
	schedulerID, err := uuid.Parse(schedulerIDStr)
	require.NoError(t, err, "Invalid schedule_id returned from ActivateSchedule")

	t.Log("Waiting for ftable_e to exhaust retries...")
	verifyNodeExhaustedRetries(t, ctx, clients, schedulerID, "ftable_e")

	t.Log("Verifying ftable_f was never deployed...")
	verifyNoJobsDeployed(t, ctx, []string{"ftable_f"})

	t.Log("Verifying scheduler reaches FAILED state...")
	verifySchedulerFailed(t, ctx, clients, schedulerID)

	t.Log("✅ Failure path test completed successfully")
}

// createAndActivateFailureScheduler activates the failure schedule via state gRPC.
func createAndActivateFailureScheduler(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
) string {
	resp, err := clients.stateClient.ActivateSchedule(ctx, &statev1.ActivateScheduleRequest{
		ScheduleName: failureTestScheduleName,
	})
	require.NoError(t, err, "Failed to activate failure schedule via state service")
	t.Logf("Activated failure schedule: schedule_id=%s", resp.ScheduleId)
	return resp.ScheduleId
}
```

- [ ] **Step 2: Verify compilation**

```bash
docker exec orchestrator go build ./tests/e2e/...
```

Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/failure_test.go
git commit -m "refactor(e2e): simplify failure test to 6-node ftable_* DAG"
```

---

## Task 10: Delete `rerun_test.go` and update README

**Files:**
- Delete: `tests/e2e/rerun_test.go`
- Modify: `tests/e2e/README.md`

- [ ] **Step 1: Delete the rerun test file**

```bash
git rm tests/e2e/rerun_test.go
```

- [ ] **Step 2: Verify compilation**

```bash
docker exec orchestrator go build ./tests/e2e/...
```

Expected: exits 0.

- [ ] **Step 3: Update `tests/e2e/README.md`**

Make the following changes:
- Replace every occurrence of `/app/e2e/...` with `/app/tests/e2e/...`
- Replace every occurrence of `bash e2e/` with `bash tests/e2e/`
- Remove the `TestRerunFailedNode` row from the Test Structure table
- In the Failure Path Test section, update the description: replace the `service-3-broken` paragraph with: "The failure model `ftable_e` runs in the `service-2` Docker image but JOINs `public.wrong_name`, which does not exist. This causes the dbt run to fail at execution time on every attempt."
- Update the DAG diagram to show the 6-node `ftable_*` layout instead of the 13-node diamond

- [ ] **Step 4: Final compilation check**

```bash
docker exec orchestrator go build ./tests/e2e/...
```

Expected: exits 0.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/rerun_test.go tests/e2e/README.md
git commit -m "chore(e2e): delete rerun_test.go and update README"
```
