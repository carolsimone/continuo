package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEmptyDAG_ManualStartIsRefused guards the empty-selection path of a
// manual start: StartRun of a schedule with no node in the live release is
// refused with FailedPrecondition and creates no run, whatever its operation.
//
// Fixture: a unique schedule name is inserted directly into schedule_catalog;
// the live release holds no node of it. This is safe to do without touching
// topology-controller or the dbt DAG because state selects a schedule's nodes
// from the live release by schedule_name.
func TestEmptyDAG_ManualStartIsRefused(t *testing.T) {
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

	// Seed schedule_catalog so StartRun passes the catalog check.
	// The live release holds no node of this schedule, so its selection is
	// empty.
	_, err := clients.stateDB.ExecContext(ctx, `
		INSERT INTO schedule_catalog (tenant_id, schedule_name, first_seen_at, last_seen_at, service_metadata)
		VALUES ('default', $1, $2, $2, '{}')
		ON CONFLICT (tenant_id, schedule_name) DO NOTHING`,
		scheduleName, now)
	require.NoError(t, err, "INSERT schedule_catalog failed")

	t.Cleanup(func() {
		bg := context.Background()

		// Resolve the run id for outbox cleanup (best effort).
		var runID string
		_ = clients.stateDB.GetContext(bg, &runID,
			`SELECT run_id FROM runs WHERE schedule_name = $1`, scheduleName)

		if runID != "" {
			if _, err := clients.stateDB.ExecContext(bg,
				`DELETE FROM state_outbox WHERE aggregate_id = $1`, runID); err != nil {
				t.Logf("Warning: failed to delete state_outbox rows for run %s: %v", runID, err)
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

	// A manual start of the schedule is refused and creates no run, whatever
	// its operation: a test run of a schedule with no node is an empty
	// selection, not a skipped run.
	for _, operation := range []string{"", "test"} {
		_, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: uuid.NewString(),
			ScheduleName: scheduleName,
			Operation:    operation,
		})
		require.Error(t, err, "StartRun(%q, operation=%q) must be refused", scheduleName, operation)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "operation=%q: %v", operation, err)
	}
	var runs int
	require.NoError(t, clients.stateDB.GetContext(ctx, &runs,
		`SELECT count(*) FROM runs WHERE schedule_name = $1`, scheduleName))
	assert.Zero(t, runs, "a refused start creates no run")
}
