package postgres_test

import (
	"testing"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tasks accepts exactly the task statuses of the run aggregate, the
// two an attempt waits in before it runs (dispatched, queued) among them.
func TestTaskTrackerSchema_StatusIsOneOfTheTaskStatuses(t *testing.T) {
	db := newTestDB(t)
	taskID := newSchemaTask(t, db)
	for _, s := range run.TaskStatuses() {
		_, err := db.Exec(`UPDATE tasks SET status = $2 WHERE task_id = $1`, taskID, string(s))
		assert.NoError(t, err, "status %s", s)
	}
	_, err := db.Exec(`UPDATE tasks SET status = 'waiting' WHERE task_id = $1`, taskID)
	require.Error(t, err)
	assert.Equal(t, "23514", pqCode(t, err))
}

// A run records why it failed when no task of it failed, with one of the
// run aggregate's reasons; a run that did not fail that way records none.
func TestSchedulerTrackerSchema_FailureReasonIsOneOfTheRunFailureReasons(t *testing.T) {
	db := newTestDB(t)
	insert := func(reason any) error {
		id := uuid.New()
		t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, id) })
		_, err := db.Exec(`
			INSERT INTO runs (run_id, schedule_name, status, failure_reason)
			VALUES ($1, $2, 'failed', $3)`, id, "schema-"+id.String(), reason)
		return err
	}
	for _, r := range run.RunFailureReasons() {
		assert.NoError(t, insert(string(r)), "failure reason %s", r)
	}
	assert.NoError(t, insert(nil))
	err := insert("gremlins")
	require.Error(t, err)
	assert.Equal(t, "23514", pqCode(t, err))
}
