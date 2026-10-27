package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEmptyCronDAG_FinalisesAsFailed guards the empty-selection path: a cron
// activation of a schedule with no node in the live release records the run
// 'failed' with no task, in the transaction that creates it, and a manual
// trigger of the same schedule is refused with FailedPrecondition and creates
// no run, whatever its operation.
//
// Fixture: a unique schedule name is inserted directly into schedule_catalog;
// the live release holds no node of it. This is safe to do without touching
// topology-controller or the dbt DAG because state selects a schedule's nodes
// from the live release by schedule_name.
func TestEmptyCronDAG_FinalisesAsFailed(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)

	// Mint a unique schedule name so the test is fully isolated from other runs
	// and from the shared e2e schedules.
	scheduleName := fmt.Sprintf("empty-dag-%s", uuid.NewString()[:8])
	t.Logf("empty-DAG test schedule name: %s", scheduleName)

	now := time.Now()

	// Seed schedule_catalog so ActivateSchedule passes the ExistsActive check.
	// The live release holds no node of this schedule, so its selection is
	// empty.
	_, err := clients.stateDB.ExecContext(ctx, `
		INSERT INTO schedule_catalog (schedule_name, first_seen_at, last_seen_at, service_metadata)
		VALUES ($1, $2, $2, '{}')
		ON CONFLICT (schedule_name) DO NOTHING`,
		scheduleName, now)
	require.NoError(t, err, "INSERT schedule_catalog failed")

	t.Cleanup(func() {
		bg := context.Background()

		// Resolve schedule_id for outbox cleanup (best effort).
		var scheduleID string
		_ = clients.stateDB.GetContext(bg, &scheduleID,
			`SELECT run_id FROM runs WHERE schedule_name = $1`, scheduleName)

		if scheduleID != "" {
			if _, err := clients.stateDB.ExecContext(bg,
				`DELETE FROM state_outbox WHERE aggregate_id = $1`, scheduleID); err != nil {
				t.Logf("Warning: failed to delete state_outbox rows for run %s: %v", scheduleID, err)
			}
		}

		_, _ = clients.stateDB.ExecContext(bg,
			`DELETE FROM tasks WHERE run_id IN (
				SELECT run_id FROM runs WHERE schedule_name = $1)`, scheduleName)
		_, _ = clients.stateDB.ExecContext(bg,
			`DELETE FROM runs WHERE schedule_name = $1`, scheduleName)
		_, _ = clients.stateDB.ExecContext(bg,
			`DELETE FROM schedule_catalog WHERE schedule_name = $1`, scheduleName)
	})

	// Activate the schedule via gRPC — state creates the run and, finding no
	// node of the schedule in the live release, records it failed.
	resp, err := clients.stateClient.ActivateSchedule(ctx, &statev1.ActivateScheduleRequest{
		ScheduleName: scheduleName,
	})
	require.NoError(t, err, "ActivateSchedule(%q) must succeed", scheduleName)
	require.NotEmpty(t, resp.ScheduleId, "ActivateSchedule must return a non-empty schedule_id")

	scheduleID, err := uuid.Parse(resp.ScheduleId)
	require.NoError(t, err, "schedule_id must be a valid UUID")
	t.Logf("ActivateSchedule created run: schedule_id=%s", scheduleID)

	// The run is recorded 'failed' in the transaction that creates it; the poll
	// only absorbs read latency.
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		var dbStatus string
		qErr := clients.stateDB.GetContext(ctx, &dbStatus,
			`SELECT status FROM runs WHERE run_id = $1`, scheduleID)
		if qErr != nil {
			return false, nil // row not yet visible — retry
		}
		return dbStatus == "failed", nil
	}, fmt.Sprintf("Timeout: runs for empty-DAG run %s did not reach status='failed'", scheduleID))

	t.Logf("runs reached status='failed' for empty-DAG run %s", scheduleID)

	var taskCount int
	require.NoError(t, clients.stateDB.GetContext(ctx, &taskCount,
		`SELECT count(*) FROM tasks WHERE run_id = $1`, scheduleID))
	assert.Zero(t, taskCount, "an empty selection stores no task")

	// A manual trigger of the same schedule is refused and creates no run,
	// whatever its operation: a test run of a schedule with no node is an empty
	// selection, not a skipped run.
	for _, operation := range []string{"", "test"} {
		_, err := clients.stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
			ScheduleName: scheduleName,
			Operation:    operation,
		})
		require.Error(t, err, "TriggerSchedule(%q, operation=%q) must be refused", scheduleName, operation)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "operation=%q: %v", operation, err)
	}
	var runs int
	require.NoError(t, clients.stateDB.GetContext(ctx, &runs,
		`SELECT count(*) FROM runs WHERE schedule_name = $1`, scheduleName))
	assert.Equal(t, 1, runs, "a refused trigger creates no run")
}
