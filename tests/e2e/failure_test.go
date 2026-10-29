package e2e

import (
	"context"
	"testing"
	"time"

	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_FailurePath_RerunRebasesBothFailureSubtrees verifies the full
// rerun alignment: two independent failure subtrees in one run, a single
// TriggerRerun call (no target fields) rebases BOTH subtrees in one new
// run, and the projection on the new run partitions correctly between
// rebased and inherited rows.
//
// DAG topology loaded via topology-controller from dbt/services/service-{1,2,3}/:
//
//	ftable_a (service-1) ─┬─ ftable_g (service-3, FAILS) ─ ftable_h (service-2) ← cascade-skipped
//	                      │
//	                      └─┐
//	ftable_b (service-1) ──┴─ ftable_c (service-3) ─┬─ ftable_d (service-2) ─┐
//	                                                │                         ├─ ftable_f (service-3) ← cascade-skipped
//	                                                └─ ftable_e (service-2, FAILS) ─┘
func TestE2E_FailurePath_RerunRebasesBothFailureSubtrees(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	defer func() {
		cleanupTestData(t, ctx, clients, failureTestScheduleName)
	}()

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	cleanupTestData(t, ctx, clients, failureTestScheduleName)

	t.Log("Seeding topology via announce-topology...")
	seedTopology(t, ctx, clients)

	t.Log("Starting a run of the schedule...")
	schedulerIDStr := startFailureScheduleRun(t, ctx, clients)
	schedulerID, err := uuid.Parse(schedulerIDStr)
	require.NoError(t, err, "Invalid run_id returned from StartRun")

	t.Log("Waiting for both ftable_e and ftable_g to exhaust retries...")
	verifyNodeExhaustedRetries(t, ctx, clients, schedulerID, "ftable_e")
	verifyNodeExhaustedRetries(t, ctx, clients, schedulerID, "ftable_g")

	t.Log("Verifying ftable_f and ftable_h were never deployed (cascade-skipped)...")
	verifyNoJobsDeployed(t, ctx, []string{"ftable_f", "ftable_h"})
	verifyTasksSkipped(t, ctx, clients, schedulerID, []string{"ftable_f", "ftable_h"})

	t.Log("Verifying scheduler reaches FAILED state...")
	verifySchedulerFailed(t, ctx, clients, schedulerID)

	t.Log("Triggering rerun via gRPC TriggerRerun with only source_run_id...")
	rerunResp, err := clients.runClient.TriggerRerun(ctx, &runv1.TriggerRerunRequest{RequestId: uuid.NewString(),
		SourceRunId: schedulerIDStr,
	})
	require.NoError(t, err, "TriggerRerun must succeed against a FAILED run")
	require.NotEmpty(t, rerunResp.RunId, "TriggerRerun must return the new run's id")
	require.NotEqual(t, schedulerIDStr, rerunResp.RunId, "rerun must mint a NEW run id")

	newRunID, err := uuid.Parse(rerunResp.RunId)
	require.NoError(t, err)

	sourceKind := queryRunKind(t, clients.stateDB, schedulerID)
	assert.Equal(t, "trigger", sourceKind, "source row must NOT be mutated")

	newKind := queryRunKind(t, clients.stateDB, newRunID)
	assert.Equal(t, "rerun", newKind, "new run must have kind='rerun'")

	t.Log("Waiting for rerun tasks rows to materialise...")
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		var count int
		qErr := clients.stateDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM tasks WHERE run_id = $1`, newRunID,
		).Scan(&count)
		if qErr != nil {
			return false, qErr
		}
		return count >= 8, nil
	}, "tasks rows for rerun run did not materialise (expected 8)")

	rows, err := clients.stateDB.QueryContext(ctx, `
		SELECT table_name, status, inherited_from_task_id IS NULL AS is_real
		FROM tasks
		WHERE run_id = $1
		ORDER BY table_name`, newRunID)
	require.NoError(t, err)
	defer rows.Close()

	type taskRow struct {
		tableName string
		status    string
		isReal    bool
	}
	var seen []taskRow
	for rows.Next() {
		var r taskRow
		require.NoError(t, rows.Scan(&r.tableName, &r.status, &r.isReal))
		seen = append(seen, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, seen, 8, "expected exactly 8 ftable_* rows on the rerun run")

	for _, r := range seen {
		switch r.tableName {
		case "ftable_a", "ftable_b", "ftable_c", "ftable_d":
			assert.False(t, r.isReal, "%s should be inherited (inherited_from_task_id non-NULL)", r.tableName)
			assert.Equal(t, "succeeded", r.status, "%s should be inherited as succeeded", r.tableName)
		case "ftable_e", "ftable_f", "ftable_g", "ftable_h":
			assert.True(t, r.isReal, "%s should be rebased (inherited_from_task_id NULL)", r.tableName)
		default:
			t.Errorf("unexpected table_name in rerun tasks: %s", r.tableName)
		}
	}

	t.Log("Rerun alignment verified: single trigger rebased both subtrees")
}

// startFailureScheduleRun starts a run of the failure schedule through
// RunService.StartRun.
func startFailureScheduleRun(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
) string {
	resp, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{
		RequestId:    uuid.NewString(),
		ScheduleName: failureTestScheduleName,
	})
	require.NoError(t, err, "Failed to start a run of the failure schedule via state service")
	t.Logf("Started a run of the failure schedule: run_id=%s", resp.GetRunId())
	return resp.GetRunId()
}
