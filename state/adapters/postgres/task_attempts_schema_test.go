package postgres_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSchemaTask inserts one run and one task of it, and deletes the run (and
// through it the task and its attempts) when the test ends.
func newSchemaTask(t *testing.T, db *sqlx.DB) uuid.UUID {
	t.Helper()
	scheduleID, taskID := uuid.New(), uuid.New()
	_, err := db.Exec(`
		INSERT INTO runs (run_id, schedule_name, status)
		VALUES ($1, $2, 'running')`, scheduleID, "schema-"+scheduleID.String())
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, scheduleID)
	})
	_, err = db.Exec(`
		INSERT INTO tasks (task_id, run_id, service_name, schema_name, table_name, job_name, status, max_retries)
		VALUES ($1, $2, 'svc', 'sch', 'tbl', 'job-0', 'pending', 2)`, taskID, scheduleID)
	require.NoError(t, err)
	return taskID
}

func insertAttempt(db *sqlx.DB, taskID uuid.UUID, attempt int, status string) error {
	_, err := db.Exec(`
		INSERT INTO task_attempts (task_id, attempt, job_name, status)
		VALUES ($1, $2, $3, $4)`, taskID, attempt, "job-name", status)
	return err
}

// Deleting a task deletes its attempts.
func TestTaskAttemptsSchema_DeletingATaskDeletesItsAttempts(t *testing.T) {
	db := newTestDB(t)
	taskID := newSchemaTask(t, db)
	require.NoError(t, insertAttempt(db, taskID, 0, "failed"))
	require.NoError(t, insertAttempt(db, taskID, 1, "running"))

	_, err := db.Exec(`DELETE FROM tasks WHERE task_id = $1`, taskID)
	require.NoError(t, err)

	var n int
	require.NoError(t, db.Get(&n, `SELECT count(*) FROM task_attempts WHERE task_id = $1`, taskID))
	assert.Zero(t, n)
}

// An attempt belongs to a stored task.
func TestTaskAttemptsSchema_AnAttemptNeedsItsTask(t *testing.T) {
	db := newTestDB(t)

	err := insertAttempt(db, uuid.New(), 0, "running")

	require.Error(t, err)
	assert.Equal(t, "23503", pqCode(t, err))
}

// Attempt numbers start at 0.
func TestTaskAttemptsSchema_AttemptIsNeverNegative(t *testing.T) {
	db := newTestDB(t)
	taskID := newSchemaTask(t, db)

	err := insertAttempt(db, taskID, -1, "running")

	require.Error(t, err)
	assert.Equal(t, "23514", pqCode(t, err))
}

// An attempt inserted with only its identity is stamped dispatched by the
// database clock, has sent no check and is not yet due; the due index covers
// the three open statuses.
func TestTaskAttemptsSchema_DefaultsAndTheDueIndex(t *testing.T) {
	db := newTestDB(t)
	taskID := newSchemaTask(t, db)
	require.NoError(t, insertAttempt(db, taskID, 0, "dispatched"))

	var row struct {
		DispatchedAt *time.Time `db:"dispatched_at"`
		ChecksSent   int        `db:"checks_sent"`
		NextCheckAt  *time.Time `db:"next_check_at"`
		DBNow        time.Time  `db:"db_now"`
	}
	require.NoError(t, db.Get(&row, `
		SELECT dispatched_at, checks_sent, next_check_at, now() AS db_now
		FROM task_attempts WHERE task_id = $1 AND attempt = 0`, taskID))
	require.NotNil(t, row.DispatchedAt)
	assert.WithinDuration(t, row.DBNow, *row.DispatchedAt, 5*time.Second)
	assert.Zero(t, row.ChecksSent)
	assert.Nil(t, row.NextCheckAt)

	var def string
	require.NoError(t, db.Get(&def, `SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_task_attempts_due'`))
	assert.Contains(t, def, "(next_check_at)")
	for _, status := range []string{"'dispatched'", "'queued'", "'running'"} {
		assert.Contains(t, def, status)
	}
}

// tasks names its current attempt `attempt`, starting at 0; the old
// column name is gone.
func TestTaskTrackerSchema_AttemptReplacesRetryCount(t *testing.T) {
	db := newTestDB(t)
	taskID := newSchemaTask(t, db)

	var attempt int
	require.NoError(t, db.Get(&attempt, `SELECT attempt FROM tasks WHERE task_id = $1`, taskID))
	assert.Zero(t, attempt)

	var old int
	require.NoError(t, db.Get(&old, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name = 'tasks' AND column_name = 'retry_count'`))
	assert.Zero(t, old)
}
