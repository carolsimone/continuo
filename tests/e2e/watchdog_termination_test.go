package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWatchdog_TerminatesStuckSchedule verifies that orchestrator's dispatch
// watchdog cancels an active run that has made no lifecycle progress within
// ORCHESTRATOR_WATCHDOG_NO_PROGRESS_MINUTES, by the run's id through state's
// CancelScheduler, and leaves alone a run whose progress clock is recent.
//
// The test seeds two runs directly in scheduler_tracker / task_tracker, both
// created 31 minutes ago so they are older than the 30-minute threshold:
//
//   - stalled: pending, zero tasks, no last_heartbeat_at — its dispatch never
//     arrived. Its progress time is created_at, so it is a candidate.
//   - progressing: running, one pending task created 31 minutes ago, and
//     last_heartbeat_at = now(). Task creation is old but the progress clock is
//     fresh, so it is not a candidate.
//
// progressing is inserted first, so the watchdog tick that lists stalled has
// already evaluated progressing. Once stalled is cancelled, progressing must
// still be active. One tick (INTERVAL_SECONDS=60) plus the cancel round-trip
// bounds the wait.
func TestWatchdog_TerminatesStuckSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)

	suffix := uuid.NewString()[:8]
	stalledName := fmt.Sprintf("watchdog-stalled-%s", suffix)
	progressingName := fmt.Sprintf("watchdog-progressing-%s", suffix)
	stalledID := uuid.New()
	progressingID := uuid.New()
	createdAt := time.Now().Add(-31 * time.Minute)
	t.Logf("seeding runs: stalled=%s (%s) progressing=%s (%s)",
		stalledName, stalledID, progressingName, progressingID)

	t.Cleanup(func() {
		// task_tracker rows cascade from scheduler_tracker; both deletes are
		// scoped to the two runs this test created.
		for _, id := range []uuid.UUID{stalledID, progressingID} {
			_, _ = clients.stateDB.ExecContext(context.Background(),
				`DELETE FROM task_tracker WHERE schedule_id = $1`, id)
			_, _ = clients.stateDB.ExecContext(context.Background(),
				`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, id)
		}
	})

	// Status values are lowercase per the scheduler_tracker CHECK constraint.
	_, err := clients.stateDB.ExecContext(ctx, `
		INSERT INTO scheduler_tracker (
		  schedule_id, schedule_name, status, created_at, last_heartbeat_at,
		  initialization_status, service_metadata, total_task_count, terminal_task_count
		) VALUES ($1, $2, 'running', $3, now(), 'completed', '{}', 1, 0)`,
		progressingID, progressingName, createdAt)
	require.NoError(t, err, "INSERT scheduler_tracker (progressing) failed")
	_, err = clients.stateDB.ExecContext(ctx, `
		INSERT INTO task_tracker (
		  task_id, schedule_id, created_at,
		  service_name, schema_name, table_name, job_name,
		  status, retry_count, max_retries
		) VALUES ($1, $2, $3, 'svc-watchdog', 'raw', 'users',
		          'job-svc-watchdog-raw-users', 'pending', 0, 3)`,
		uuid.New(), progressingID, createdAt)
	require.NoError(t, err, "INSERT task_tracker (progressing) failed")

	_, err = clients.stateDB.ExecContext(ctx, `
		INSERT INTO scheduler_tracker (
		  schedule_id, schedule_name, status, created_at,
		  initialization_status, service_metadata, terminal_task_count
		) VALUES ($1, $2, 'pending', $3, 'in_progress', '{}', 0)`,
		stalledID, stalledName, createdAt)
	require.NoError(t, err, "INSERT scheduler_tracker (stalled) failed")

	t.Log("Runs seeded — waiting for the watchdog to cancel the stalled one...")

	// Worst case: the seed just missed a tick, the next comes 60s later, then
	// CancelScheduler commits the cancel in one transaction.
	pollUntil(t, ctx, 150*time.Second, 5*time.Second, func() (bool, error) {
		st := readScheduleState(t, ctx, clients, stalledName)
		if st.Status == "cancelled" && strings.HasPrefix(st.CancellationReason, "watchdog:") {
			t.Logf("watchdog cancelled the stalled run: cancelled_by=%s reason=%q",
				st.CancelledBy, st.CancellationReason)
			return true, nil
		}
		return false, nil
	}, "Timeout waiting for the watchdog to cancel the stalled run")

	st := readScheduleState(t, ctx, clients, stalledName)
	assert.Equal(t, "cancelled", st.Status, "stalled run: expected status=cancelled")
	assert.Equal(t, "watchdog", st.CancelledBy, "stalled run: expected cancelled_by=watchdog")
	assert.True(t, strings.HasPrefix(st.CancellationReason, "watchdog:"),
		"stalled run: expected cancellation_reason to start with 'watchdog:', got %q", st.CancellationReason)

	kept := readScheduleState(t, ctx, clients, progressingName)
	assert.Equal(t, "running", kept.Status,
		"a run with a fresh progress clock must stay active even though its task was created 31 minutes ago")
	assert.Empty(t, kept.CancelledBy, "progressing run must not be cancelled")
}
