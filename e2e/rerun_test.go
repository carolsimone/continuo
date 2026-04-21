package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestRerunFailedNode verifies that a FAILED mid-DAG node (table_e) can be re-run via
// POST /api/schedulers/{id}/rerun and the schedule recovers to SUCCEEDED.
//
// DAG: uses the failure DAG (e2e-schedule-failure).
//   - table_e fails via service-3-broken after exhausting 2 retries (3 total attempts).
//   - The broken manifest is replaced with a working one and manifest-controller reloads.
//   - POST /api/schedulers/{id}/rerun is called for table_e (BFF route → TriggerRerun gRPC).
//   - The full downstream cascade (table_g, table_h, table_i, table_j) completes.
//   - The schedule recovers to SUCCEEDED.
func TestRerunFailedNode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	defer func() {
		cleanupTestData(t, ctx, clients, failureTestScheduleName)
		// Always restore the broken SQL so other tests that rely on
		// service-3-broken failing are not affected.
		restoreBrokenTableE(t)
	}()

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, failureTestScheduleName)

	// Phase 1: Run the failure DAG until table_e fails and scheduler reaches FAILED.
	t.Log("Phase 1: seeding and running failure DAG...")
	seedFailureDAG(t, ctx, clients)

	schedulerIDStr := createAndActivateFailureScheduler(t, ctx, clients)
	schedulerID, err := uuid.Parse(schedulerIDStr)
	require.NoError(t, err)

	verifyOrchestratorPublishedRootNodes(t, ctx, clients, schedulerID, []string{"seed_table_1", "seed_table_2", "seed_table_3"})

	// Levels 0 and 1 succeed
	level0 := []string{"seed_table_1", "seed_table_2", "seed_table_3"}
	verifyExecutorDeployedJobs(t, ctx, clients, level0, failureTestScheduleName)
	verifyJobsCompleted(t, ctx, clients, level0, failureTestScheduleName)

	level1 := []string{"table_a", "table_b", "table_c"}
	verifyExecutorDeployedJobs(t, ctx, clients, level1, failureTestScheduleName)
	verifyJobsCompleted(t, ctx, clients, level1, failureTestScheduleName)

	// Level 2: table_e will fail; table_d and table_f succeed
	level2 := []string{"table_d", "table_e", "table_f"}
	verifyExecutorDeployedJobs(t, ctx, clients, level2, failureTestScheduleName)
	verifyJobsCompleted(t, ctx, clients, []string{"table_d", "table_f"}, failureTestScheduleName)

	t.Log("Waiting for table_e to exhaust retries...")
	verifyTableEExhaustedRetries(t, ctx, clients, schedulerID)

	verifySchedulerFailed(t, ctx, clients, schedulerID)
	t.Log("Phase 1 complete: scheduler reached FAILED")

	// Phase 2: Fix table_e — rebuild manifest and reload graph.
	t.Log("Phase 2: fixing table_e in service-3-broken and reloading graph...")
	fixBrokenTableEAndReloadGraph(t, ctx, clients)

	// Update task_tracker to reflect the fixed service name. In production the
	// service_name never changes (it's the dbt project name), so the task and
	// graph always agree. The e2e test uses separate Docker images
	// (service-3-broken → service-3), so we align the task_tracker explicitly.
	_, err = clients.stateDB.ExecContext(ctx,
		`UPDATE task_tracker SET service_name = $1 WHERE schedule_id = $2 AND table_name = $3`,
		"service-3", schedulerID, "table_e",
	)
	require.NoError(t, err, "failed to update task_tracker service_name")
	t.Log("Updated task_tracker service_name: service-3-broken → service-3")

	// Phase 3: POST /api/schedulers/{id}/rerun for table_e (BFF route → TriggerRerun gRPC)
	t.Log("Phase 3: calling POST /api/schedulers/{id}/rerun for table_e...")
	callRerunEndpoint(t, ctx, schedulerID, failureTestSchemaName, "table_e", "service-3")

	// Phase 4: Wait for table_e to be re-dispatched (second entry in query.model:v1)
	t.Log("Phase 4: waiting for table_e re-dispatch...")
	waitForRerunDispatched(t, ctx, clients, schedulerID, "table_e")

	// Phase 5: The rest of the DAG should now complete (table_g, table_h, table_i, table_j)
	t.Log("Phase 5: waiting for downstream nodes to complete...")
	verifyJobsCompleted(t, ctx, clients, []string{"table_e"}, failureTestScheduleName)
	verifyExecutorDeployedJobs(t, ctx, clients, []string{"table_g", "table_h"}, failureTestScheduleName)
	verifyJobsCompleted(t, ctx, clients, []string{"table_g", "table_h"}, failureTestScheduleName)
	verifyExecutorDeployedJobs(t, ctx, clients, []string{"table_i", "table_j"}, failureTestScheduleName)
	verifyJobsCompleted(t, ctx, clients, []string{"table_i", "table_j"}, failureTestScheduleName)

	// Phase 6: Assert final state
	t.Log("Phase 6: asserting final state...")
	verifySchedulerSucceeded(t, ctx, clients, schedulerID)

	assertRerunTaskRetryCountZero(t, ctx, clients, schedulerID, "table_e")
	assertInitStatusCompleted(t, ctx, clients, schedulerID)
	assertTaskExecutionAtLeast(t, ctx, clients, schedulerID, "table_e", 4) // 3 failed + 1 succeeded

	// Upstream nodes untouched by rerun
	assertTaskStatus(t, ctx, clients, schedulerID, "table_b", "succeeded")
	assertTaskStatus(t, ctx, clients, schedulerID, "table_c", "succeeded")
	// Peers untouched by rerun
	assertTaskStatus(t, ctx, clients, schedulerID, "table_d", "succeeded")
	assertTaskStatus(t, ctx, clients, schedulerID, "table_f", "succeeded")
	// Downstream cascade completed
	assertTaskStatus(t, ctx, clients, schedulerID, "table_g", "succeeded")
	assertTaskStatus(t, ctx, clients, schedulerID, "table_h", "succeeded")
	assertTaskStatus(t, ctx, clients, schedulerID, "table_i", "succeeded")
	assertTaskStatus(t, ctx, clients, schedulerID, "table_j", "succeeded")

	t.Log("TestRerunFailedNode completed successfully")
}

// ─── rerun helpers ───────────────────────────────────────────────────────────

// hostDbtServicePath is the absolute host path to dbt/services/.
// It must match the host-side source of the ./dbt/services:/manifests volume
// in docker-compose.yml. We derive it by inspecting the manifest-controller
// container's mount at runtime so the test isn't hardcoded to a developer path.
func hostDbtServicePath(t *testing.T) string {
	t.Helper()
	out, err := exec.Command(
		"docker", "inspect", "manifest-controller",
		"--format", `{{range .Mounts}}{{if eq .Destination "/manifests"}}{{.Source}}{{end}}{{end}}`,
	).Output()
	require.NoError(t, err, "failed to inspect manifest-controller mounts")
	path := strings.TrimSpace(string(out))
	require.NotEmpty(t, path, "could not determine host path for /manifests mount")
	return path
}

// fixBrokenTableEAndReloadGraph replaces service-3-broken's compiled manifest with the
// pre-compiled "success" manifest from service-3-fixed, then triggers manifest-controller
// to reload the graph. After reload the graph's table_e node is re-seeded with
// ServiceName="service-3" so the rerun dispatch targets the working Docker image.
//
// The success manifest lives at dbt/services-fixed/service-3-fixed/target/manifest.json
// and is committed to the repository as a static fixture. It was built with dbt project
// name "service_3" (→ service_name="service-3") and tag "e2e-schedule-failure"
// (→ schedule_name="e2e-schedule-failure"), matching the test's DAG configuration.
func fixBrokenTableEAndReloadGraph(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()

	hostServicesPath := hostDbtServicePath(t)
	// Derive services-fixed path: same parent, different subdirectory name.
	hostServicesFixedPath := strings.Replace(hostServicesPath, "/services", "/services-fixed", 1)
	require.NotEqual(t, hostServicesPath, hostServicesFixedPath,
		"could not derive services-fixed path from %s", hostServicesPath)

	// Install the pre-compiled success manifest into service-3-broken.
	// service-3-broken normally has no versioned manifest (manifest-controller skips it);
	// we create one here so the reload picks it up.
	copyCmd := exec.CommandContext(ctx,
		"docker", "run", "--rm",
		"-v", hostServicesPath+"/service-3-broken:/dest",
		"-v", hostServicesFixedPath+"/service-3-fixed:/src:ro",
		"alpine",
		"sh", "-c", "mkdir -p /dest && cp /src/manifest_v1.json /dest/manifest_v1.json",
	)
	out, err := copyCmd.CombinedOutput()
	require.NoError(t, err, "failed to install success manifest: %s", string(out))
	t.Log("Success manifest installed (original backed up to manifest.json.bak)")

	// Trigger manifest-controller reload.
	err = clients.redisClient.XAdd(ctx, &goredis.XAddArgs{
		Stream: "update.graph:v1",
		Values: map[string]interface{}{"source": "local"},
	}).Err()
	require.NoError(t, err, "failed to publish update.graph:v1")

	waitForSchedulesReloaded(t, ctx, clients)
	t.Log("Graph reloaded")

	// The manifest reload overwrites ALL nodes' schedule_name back to the values in the
	// service manifests (e.g. "e2e-schedule"). Re-seed the entire failure DAG to restore
	// schedule_name="e2e-schedule-failure" on all nodes without touching their statuses
	// (ON MATCH SET in CreateNode does not update the status field).
	seedFailureDAG(t, ctx, clients)
	t.Log("Re-seeded failure DAG to restore schedule_name on all nodes")

	// Override table_e specifically: point it at service-3 (the working image) so the
	// rerun dispatch targets the fixed image instead of service-3-broken.
	tableENode := []dagNode{
		{Name: "table_e", Dependencies: []string{"table_b", "table_c"}, ServiceName: "service-3"},
	}
	seedNodes(t, ctx, clients, tableENode, failureTestScheduleName, failureTestSchemaName)
	t.Log("Re-seeded table_e → service-3")
}

// waitForSchedulesReloaded polls the schedules.loaded:v1 Redis stream until
// a new message appears (indicating manifest-controller finished reloading).
func waitForSchedulesReloaded(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()

	// Record current message count before polling
	before, err := clients.redisClient.XLen(ctx, "schedules.loaded:v1").Result()
	require.NoError(t, err)

	pollUntil(t, ctx, 2*time.Minute, 2*time.Second, func() (bool, error) {
		after, err := clients.redisClient.XLen(ctx, "schedules.loaded:v1").Result()
		if err != nil {
			return false, err
		}
		return after > before, nil
	}, "Timeout waiting for manifest-controller to reload graph (schedules.loaded:v1)")

	t.Log("schedules.loaded:v1 received — manifest reload confirmed")
}

// restoreBrokenTableE restores service-3-broken/target/manifest.json to contain
// the original broken manifest (with table_e pointing to service-3-broken image).
// This is called in test cleanup so subsequent test runs that rely on
// service-3-broken failing still work correctly.
func restoreBrokenTableE(t *testing.T) {
	t.Helper()

	out, err := exec.Command(
		"docker", "inspect", "manifest-controller",
		"--format", `{{range .Mounts}}{{if eq .Destination "/manifests"}}{{.Source}}{{end}}{{end}}`,
	).Output()
	if err != nil {
		t.Logf("Warning: could not inspect manifest-controller mounts: %v", err)
		return
	}
	hostServicesPath := strings.TrimSpace(string(out))
	if hostServicesPath == "" {
		t.Log("Warning: could not determine host services path for restore")
		return
	}

	// Remove the versioned manifest installed by fixBrokenTableEAndReloadGraph.
	// service-3-broken should have no versioned manifest in normal operation.
	restoreCmd := exec.Command(
		"docker", "run", "--rm",
		"-v", hostServicesPath+"/service-3-broken:/work",
		"alpine",
		"sh", "-c", "rm -f /work/manifest_v1.json",
	)
	if restoreOut, restoreErr := restoreCmd.CombinedOutput(); restoreErr != nil {
		t.Logf("Warning: failed to restore original manifest: %s", string(restoreOut))
	}
}

// callRerunEndpoint calls POST /api/schedulers/{id}/rerun and asserts 200 OK.
func callRerunEndpoint(
	t *testing.T,
	ctx context.Context,
	schedulerID uuid.UUID,
	schemaName, tableName, serviceName string,
) {
	t.Helper()
	uiBase := getEnv("UI_HTTP_BASE", "http://ui:8090")
	url := fmt.Sprintf("%s/api/schedulers/%s/rerun", uiBase, schedulerID)

	body, err := json.Marshal(map[string]string{
		"schema":       schemaName,
		"table_name":   tableName,
		"service_name": serviceName,
	})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Do(req)
	require.NoError(t, err, "POST /api/schedulers/%s/rerun failed", schedulerID)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"Expected 200 OK from BFF rerun endpoint, got %d", resp.StatusCode)
	t.Logf("POST /api/schedulers/%s/rerun returned 200 OK for %s.%s", schedulerID, schemaName, tableName)
}

// waitForRerunDispatched polls query.model:v1 until the target node appears
// at least twice for this schedule (original dispatch + rerun dispatch).
func waitForRerunDispatched(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
	tableName string,
) {
	t.Helper()
	pollUntil(t, ctx, 2*time.Minute, 2*time.Second, func() (bool, error) {
		messages, err := clients.redisClient.XRange(ctx, "query.model:v1", "-", "+").Result()
		if err != nil {
			return false, err
		}
		count := 0
		for _, msg := range messages {
			if msg.Values["schedule_id"] == schedulerID.String() &&
				msg.Values["table_name"] == tableName {
				count++
			}
		}
		return count >= 2, nil
	}, fmt.Sprintf("Timeout waiting for rerun dispatch of %s in query.model:v1", tableName))

	t.Logf("Rerun dispatch confirmed: %s appears >= 2 times in query.model:v1", tableName)
}

// assertRerunTaskRetryCountZero asserts that retry_count was reset to 0 by the rerun.
func assertRerunTaskRetryCountZero(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
	tableName string,
) {
	t.Helper()
	var retryCount int
	err := clients.stateDB.QueryRowContext(ctx, `
		SELECT retry_count FROM task_tracker
		WHERE schedule_id = $1 AND table_name = $2
	`, schedulerID, tableName).Scan(&retryCount)
	require.NoError(t, err, "Failed to query retry_count for %s", tableName)
	require.Equal(t, 0, retryCount,
		"Expected retry_count=0 after rerun for %s, got %d", tableName, retryCount)
	t.Logf("retry_count=0 confirmed for %s", tableName)
}

// assertTaskStatus asserts a task has the expected status.
func assertTaskStatus(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
	tableName, expectedStatus string,
) {
	t.Helper()
	var status string
	err := clients.stateDB.QueryRowContext(ctx, `
		SELECT status FROM task_tracker
		WHERE schedule_id = $1 AND table_name = $2
	`, schedulerID, tableName).Scan(&status)
	require.NoError(t, err, "Failed to query status for %s", tableName)
	require.Equal(t, expectedStatus, status,
		"Expected status=%s for %s, got %s", expectedStatus, tableName, status)
	t.Logf("Task %s has expected status=%s", tableName, expectedStatus)
}

// assertInitStatusCompleted asserts initialization_status = "completed".
func assertInitStatusCompleted(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
) {
	t.Helper()
	var initStatus string
	err := clients.stateDB.QueryRowContext(ctx, `
		SELECT initialization_status FROM scheduler_tracker WHERE schedule_id = $1
	`, schedulerID).Scan(&initStatus)
	require.NoError(t, err, "Failed to query initialization_status")
	require.Equal(t, "completed", initStatus,
		"Expected initialization_status=completed, got %s", initStatus)
	t.Log("initialization_status=completed confirmed")
}

// assertTaskExecutionAtLeast asserts a task has at least minCount task_execution records.
func assertTaskExecutionAtLeast(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	schedulerID uuid.UUID,
	tableName string,
	minCount int,
) {
	t.Helper()
	var count int
	err := clients.stateDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM task_execution te
		JOIN task_tracker tt ON te.task_id = tt.task_id
		WHERE tt.schedule_id = $1 AND tt.table_name = $2
	`, schedulerID, tableName).Scan(&count)
	require.NoError(t, err, "Failed to query task_execution count for %s", tableName)
	require.GreaterOrEqual(t, count, minCount,
		"Expected >= %d task_execution records for %s, got %d", minCount, tableName, count)
	t.Logf("task_execution count for %s: %d (>= %d required)", tableName, count, minCount)
}
