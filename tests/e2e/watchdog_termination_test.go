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

// TestWatchdog_TerminatesStuckSchedule verifies the orchestrator's dispatch
// watchdog (PR #33 B2) detects a schedule that has no task in 'running' and no
// task progress within ORCHESTRATOR_WATCHDOG_NO_PROGRESS_MINUTES, then cancels
// it via the state.CancelSchedule pathway — leaving cancellation_reason
// starting with "watchdog:".
//
// We bypass the dispatch path entirely (B1 would otherwise terminal-fail an
// empty-image_tag dispatch before B2 has a chance) by inserting
// scheduler_tracker + task_tracker rows directly via SQL with stale timestamps.
//
// Local docker-compose's watchdog overrides:
//   ORCHESTRATOR_WATCHDOG_INTERVAL_SECONDS=5
//   ORCHESTRATOR_WATCHDOG_NO_PROGRESS_MINUTES=2
// So the watchdog should detect within ~5s of the stale window opening, and
// the test finishes in ~30-90s (since the rows are seeded already-stale).
func TestWatchdog_TerminatesStuckSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)

	scheduleName := fmt.Sprintf("watchdog-test-%s", uuid.NewString()[:8])
	scheduleID := uuid.New()
	taskID := uuid.New()
	t.Logf("seeding stuck schedule: name=%s schedule_id=%s task_id=%s",
		scheduleName, scheduleID, taskID)

	// Created 3 minutes ago so the schedule is past the 2-minute NO_PROGRESS
	// threshold the moment we insert it.
	createdAt := time.Now().Add(-3 * time.Minute)

	// Insert into schedule_catalog so state.ListAllSchedules surfaces the
	// schedule (the watchdog reads its candidate set from there). Without
	// this row the schedule is invisible to the watchdog and never cancelled.
	_, err := clients.stateDB.ExecContext(ctx, `
		INSERT INTO schedule_catalog (schedule_name, first_seen_at, last_seen_at, service_metadata)
		VALUES ($1, $2, $2, '{}')
		ON CONFLICT (schedule_name) DO NOTHING`,
		scheduleName, createdAt)
	require.NoError(t, err, "INSERT schedule_catalog failed")

	// Insert the scheduler_tracker row.
	// Status must be lowercase per the table's CHECK constraint (see schema).
	_, err = clients.stateDB.ExecContext(ctx, `
		INSERT INTO scheduler_tracker (
		  schedule_id, schedule_name, status, created_at,
		  initialization_status, service_metadata, total_task_count, terminal_task_count
		) VALUES ($1, $2, 'running', $3, 'completed', '{}', 1, 0)`,
		scheduleID, scheduleName, createdAt)
	require.NoError(t, err, "INSERT scheduler_tracker failed")

	// Insert one task in 'pending' state, also stale, so IsScheduleStuck's
	// check (no task in 'running' + most-recent created_at older than
	// NO_PROGRESS) is satisfied.
	_, err = clients.stateDB.ExecContext(ctx, `
		INSERT INTO task_tracker (
		  task_id, schedule_id, created_at,
		  service_name, schema_name, table_name, job_name,
		  status, retry_count, max_retries
		) VALUES ($1, $2, $3, 'svc-watchdog', 'raw', 'users',
		          'job-svc-watchdog-raw-users', 'pending', 0, 3)`,
		taskID, scheduleID, createdAt)
	require.NoError(t, err, "INSERT task_tracker failed")

	t.Cleanup(func() {
		// Best-effort cleanup: delete by schedule_id so we don't leak rows
		// across test runs. task_tracker has ON DELETE CASCADE so deleting
		// the scheduler row also removes the tasks, but explicit is clearer.
		_, _ = clients.stateDB.ExecContext(context.Background(),
			`DELETE FROM task_tracker WHERE schedule_id = $1`, scheduleID)
		_, _ = clients.stateDB.ExecContext(context.Background(),
			`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
		_, _ = clients.stateDB.ExecContext(context.Background(),
			`DELETE FROM schedule_catalog WHERE schedule_name = $1`, scheduleName)
	})

	t.Log("Stuck schedule seeded — waiting for watchdog to cancel it...")

	// Poll up to 90s (one watchdog tick = 5s + cancel path round-trip via
	// state.CancelSchedule outbox + cancel publisher).
	pollUntil(t, ctx, 90*time.Second, 5*time.Second, func() (bool, error) {
		st := readScheduleState(t, ctx, clients, scheduleName)
		if st.Status == "cancelled" && strings.HasPrefix(st.CancellationReason, "watchdog:") {
			t.Logf("✅ watchdog cancelled: status=%s cancelled_by=%s reason=%q",
				st.Status, st.CancelledBy, st.CancellationReason)
			return true, nil
		}
		return false, nil
	}, "Timeout waiting for watchdog to cancel the stuck schedule")

	// Final structural assertion in case the polled condition raced.
	st := readScheduleState(t, ctx, clients, scheduleName)
	assert.Equal(t, "cancelled", st.Status, "expected status=cancelled")
	assert.Equal(t, "watchdog", st.CancelledBy, "expected cancelled_by=watchdog")
	assert.True(t, strings.HasPrefix(st.CancellationReason, "watchdog:"),
		"expected cancellation_reason to start with 'watchdog:', got %q", st.CancellationReason)
}
