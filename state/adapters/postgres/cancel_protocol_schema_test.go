package postgres_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scheduler_tracker accepts exactly the run statuses of the run aggregate.
func TestSchedulerTrackerSchema_StatusIsOneOfTheRunStatuses(t *testing.T) {
	db := newTestDB(t)
	insert := func(status string) error {
		id := uuid.New()
		t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, id) })
		_, err := db.Exec(`
			INSERT INTO scheduler_tracker (schedule_id, schedule_name, status)
			VALUES ($1, $2, $3)`, id, "schema-"+id.String(), status)
		return err
	}
	for _, s := range run.SchedulerStatuses() {
		assert.NoError(t, insert(string(s)), "status %s", s)
	}
	err := insert("waiting")
	require.Error(t, err)
	assert.Equal(t, "23514", pqCode(t, err))
}

// A cancelling run still holds its schedule's single active slot: a second
// run of the schedule cannot start until it has ended cancelled.
func TestSchedulerTrackerSchema_OneActiveRunPerScheduleCoversCancelling(t *testing.T) {
	db := newTestDB(t)
	name := "schema-" + uuid.NewString()
	first, second := uuid.New(), uuid.New()
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id IN ($1, $2)`, first, second)
	})
	insert := func(id uuid.UUID, status string) error {
		_, err := db.Exec(`
			INSERT INTO scheduler_tracker (schedule_id, schedule_name, status)
			VALUES ($1, $2, $3)`, id, name, status)
		return err
	}
	require.NoError(t, insert(first, "cancelling"))

	err := insert(second, "pending")
	require.Error(t, err)
	assert.Equal(t, "23505", pqCode(t, err))

	_, err = db.Exec(`UPDATE scheduler_tracker SET status = 'cancelled' WHERE schedule_id = $1`, first)
	require.NoError(t, err)
	assert.NoError(t, insert(second, "pending"), "the slot frees once the run has ended")
}

// An attempt is in one of the six attempt statuses; a task-level status such
// as pending is not one of them, and neither is cancelled: an attempt of a
// cancelled run ends stopped.
func TestTaskAttemptsSchema_StatusIsOneOfTheAttemptStatuses(t *testing.T) {
	db := newTestDB(t)
	taskID := newSchemaTask(t, db)

	statuses := []string{"dispatched", "queued", "running", "succeeded", "failed", "stopped"}
	for i, status := range statuses {
		require.NoError(t, insertAttempt(db, taskID, i, status), status)
	}

	for i, status := range []string{"pending", "cancelled"} {
		err := insertAttempt(db, taskID, len(statuses)+i, status)
		require.Error(t, err, status)
		assert.Equal(t, "23514", pqCode(t, err), status)
	}
}

// An attempt records when a stop was requested of it; none until then.
func TestTaskAttemptsSchema_StopRequestedAtIsNullUntilAStopIsRequested(t *testing.T) {
	db := newTestDB(t)
	taskID := newSchemaTask(t, db)
	require.NoError(t, insertAttempt(db, taskID, 0, "running"))

	var at *time.Time
	require.NoError(t, db.Get(&at, `SELECT stop_requested_at FROM task_attempts WHERE task_id = $1`, taskID))
	assert.Nil(t, at)
}
