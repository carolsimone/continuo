package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUIWholeScheduleTestOperation drives a whole-schedule `test` run through the
// ui HTTP API — POST /api/schedules/:name/manual-run with a JSON
// {"operation":"test"} body — the exact wire path the UI's operation selector
// uses. It proves the operation survives the HTTP → gRPC StartRun hop
// by asserting the resulting run's stored operation, covering the
// ui HTTP layer on top of the gRPC-level whole-schedule test coverage.
func TestUIWholeScheduleTestOperation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	const seedSchedule = "seed"

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, seedSchedule)
	defer cleanupTestData(t, ctx, clients, seedSchedule)
	seedTopology(t, ctx, clients)

	t.Logf("POST /api/schedules/%s/manual-run {operation:test} via ui HTTP", seedSchedule)
	scheduleID := startManualRunOperationHTTP(t, clients.uiBase, seedSchedule, "test")

	id, err := uuid.Parse(scheduleID)
	require.NoError(t, err, "run_id must be a valid UUID")

	t.Log("Waiting for the UI-started whole-schedule test run to reach 'succeeded'...")
	verifySchedulerSucceeded(t, ctx, clients, id)

	assert.Equal(t, "test", queryRunOperation(t, clients, id),
		"the run's operation must be 'test' for a UI-started whole-schedule test run")

	t.Log("TestUIWholeScheduleTestOperation passed")
}

// TestUISingleNodeBuildOperation drives a single-node `build` run through the
// ui HTTP API — POST /api/nodes/:project/:schema/:table/run with a
// JSON {"operation":"build"} body — the exact wire path NodeDetailPage's
// operation selector uses. It runs against the seed_table_1 node and asserts
// the dispatched Job's command carries `dbt build`, proving the operation
// threaded through the HTTP → StartSingleNodeRun → Job-assembly path.
func TestUISingleNodeBuildOperation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	const (
		targetProject = "service-1"
		targetSchema  = "e2e_schema"
		targetTable   = "seed_table_1"
	)

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, "single-node-build-op-ui")
	seedTopology(t, ctx, clients)

	t.Logf("POST /api/nodes/%s/%s/%s/run {operation:build} via ui HTTP", targetProject, targetSchema, targetTable)
	runIDStr, scheduleName := startNodeRunOperationHTTP(t, clients.uiBase, targetProject, targetSchema, targetTable, "build")

	runID, err := uuid.Parse(runIDStr)
	require.NoError(t, err, "run_id must be a valid UUID")
	defer cleanupSingleNodeRun(t, ctx, clients, runID, scheduleName)

	t.Log("Waiting for the UI-started single-node build run to reach 'succeeded'...")
	verifySchedulerSucceeded(t, ctx, clients, runID)

	assert.Equal(t, "build", queryRunOperation(t, clients, runID),
		"the run's operation must be 'build' for a UI-started single-node build run")

	// table_name alone is a sufficient filter here: this test runs a single
	// serial single-node run against a stack cleaned by cleanupTestData, so no
	// other job for this table can be in flight and Items[0] is unambiguous —
	// mirrors the same single-run assumption in build_operation_test.go.
	jobs, err := getK8sJobs(ctx, fmt.Sprintf("table_name=%s", targetTable))
	require.NoError(t, err, "failed to query K8s jobs for %s", targetTable)
	require.NotEmpty(t, jobs.Items, "%s must have been dispatched", targetTable)
	cmd := strings.Join(jobs.Items[0].Spec.Template.Spec.Containers[0].Command, " ")
	assert.Contains(t, cmd, "build",
		"UI-started single-node run must dispatch `dbt build` when operation=build")
	assert.NotContains(t, cmd, "dbt run",
		"UI-started build operation must not fall back to `dbt run`")

	t.Log("TestUISingleNodeBuildOperation passed")
}

// startManualRunOperationHTTP starts a manual run of a schedule with an explicit run
// operation via the ui HTTP endpoint (POST /api/schedules/:name/manual-run
// {"request_id": id, "operation": op}) and returns the run_id. Mirrors
// startManualRunHTTP in manual_run_test.go but also sends the operation,
// exercising the UI operation-selector's wire path end to end.
func startManualRunOperationHTTP(t *testing.T, base, scheduleName, operation string) string {
	t.Helper()

	body, err := json.Marshal(map[string]string{"request_id": uuid.NewString(), "operation": operation})
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("%s/api/schedules/%s/manual-run", base, scheduleName),
		"application/json", bytes.NewReader(body),
	)
	require.NoError(t, err, "POST /api/schedules/%s/manual-run: request failed", scheduleName)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"POST /api/schedules/%s/manual-run: expected 200, got %d: %s", scheduleName, resp.StatusCode, string(raw))

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &result))

	runID, ok := result["run_id"].(string)
	require.True(t, ok && runID != "", "run_id missing from manual-run response")

	return runID
}

// startNodeRunOperationHTTP starts a single-node run with an explicit run
// operation via the ui HTTP endpoint (POST
// /api/nodes/:project/:schema/:table/run {"request_id": id, "operation": op})
// and returns (run_id, schedule_name).
func startNodeRunOperationHTTP(t *testing.T, base, project, schema, table, operation string) (string, string) {
	t.Helper()

	body, err := json.Marshal(map[string]string{"request_id": uuid.NewString(), "operation": operation})
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("%s/api/nodes/%s/%s/%s/run", base, project, schema, table),
		"application/json", bytes.NewReader(body),
	)
	require.NoError(t, err, "POST /api/nodes/%s/%s/%s/run: request failed", project, schema, table)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"POST /api/nodes/%s/%s/%s/run: expected 200, got %d: %s", project, schema, table, resp.StatusCode, string(raw))

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &result))

	runID, ok := result["run_id"].(string)
	require.True(t, ok && runID != "", "run_id missing from node-run response")
	scheduleName, ok := result["schedule_name"].(string)
	require.True(t, ok && scheduleName != "", "schedule_name missing from node-run response")

	return runID, scheduleName
}
