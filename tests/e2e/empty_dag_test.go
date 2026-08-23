package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestEmptyCronDAG_FinalisesAsFailed guards the empty-projection path introduced
// in the ref-emit-dispatch branch:
//
//	ActivateSchedule("empty-dag-*") → scheduler.started:v1
//	→ orchestrator HandleSchedulerStarted: Snapshot(LatestFullDAG) returns
//	  ErrEmptyProjection (no :Table nodes for this schedule in Neo4j)
//	→ run.entries.dispatch_failed:v1 with reason="empty_projection"
//	→ state RunEntriesDispatchFailedHandler: scheduler_tracker.status = 'failed'
//
// Fixture: a unique schedule name is inserted directly into schedule_catalog with
// no corresponding :Table nodes in Neo4j.  This is safe to do without touching
// topology-controller or the dbt DAG because LatestFullDAG queries Neo4j by
// schedule_name and returns an empty map when no nodes match.
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
	// No :Table nodes are seeded in Neo4j for this schedule, so
	// LoadLatestSourceDAG returns an empty map and Snapshot returns
	// ErrEmptyProjection.
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
			`SELECT schedule_id FROM scheduler_tracker WHERE schedule_name = $1`, scheduleName)

		if scheduleID != "" {
			_, _ = clients.stateDB.ExecContext(bg,
				`DELETE FROM state_outbox WHERE aggregate_id = $1`, scheduleID)
			_, _ = clients.orchestratorDB.ExecContext(bg,
				`DELETE FROM outbox WHERE aggregate_id = $1`, scheduleID)
		}

		_, _ = clients.stateDB.ExecContext(bg,
			`DELETE FROM task_tracker WHERE schedule_id IN (
				SELECT schedule_id FROM scheduler_tracker WHERE schedule_name = $1)`, scheduleName)
		_, _ = clients.stateDB.ExecContext(bg,
			`DELETE FROM scheduler_tracker WHERE schedule_name = $1`, scheduleName)
		_, _ = clients.stateDB.ExecContext(bg,
			`DELETE FROM schedule_catalog WHERE schedule_name = $1`, scheduleName)

		// Remove Redis messages for this schedule from the dispatch-failed stream.
		// Other tests are isolated by schedule_name, so trimming the whole stream
		// is safe only if no concurrent e2e test writes there; delete matching
		// entries by reading + selectively acking is complex, so we leave the
		// stream in place (it has no side-effects on subsequent test runs).
	})

	// Activate the schedule via gRPC — state creates scheduler_tracker and
	// publishes scheduler.started:v1 via its outbox processor.
	resp, err := clients.stateClient.ActivateSchedule(ctx, &statev1.ActivateScheduleRequest{
		ScheduleName: scheduleName,
	})
	require.NoError(t, err, "ActivateSchedule(%q) must succeed", scheduleName)
	require.NotEmpty(t, resp.ScheduleId, "ActivateSchedule must return a non-empty schedule_id")

	scheduleID, err := uuid.Parse(resp.ScheduleId)
	require.NoError(t, err, "schedule_id must be a valid UUID")
	t.Logf("ActivateSchedule created run: schedule_id=%s", scheduleID)

	// Wait for scheduler_tracker to reach 'failed'.
	// Path: orchestrator processes scheduler.started:v1, finds no :Table nodes,
	// emits run.entries.dispatch_failed:v1, state sets status='failed'.
	// Nominal latency is a few seconds; 30 s is a comfortable bound.
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		var dbStatus string
		qErr := clients.stateDB.GetContext(ctx, &dbStatus,
			`SELECT status FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
		if qErr != nil {
			return false, nil // row not yet visible — retry
		}
		return dbStatus == "failed", nil
	}, fmt.Sprintf("Timeout: scheduler_tracker for empty-DAG run %s did not reach status='failed'", scheduleID))

	t.Logf("scheduler_tracker reached status='failed' for empty-DAG run %s", scheduleID)
}
