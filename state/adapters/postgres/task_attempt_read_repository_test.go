//go:build integration

package postgres_test

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/projection"
)

// attemptKey names an attempt "<task_id>/<attempt>".
func attemptKey(taskID uuid.UUID, attempt int) string {
	return taskID.String() + "/" + strconv.Itoa(attempt)
}

func attemptKeys(rows []projection.TaskAttempt) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, attemptKey(r.TaskID, r.Attempt))
	}
	return out
}

// attemptRun stores a run of two tasks, the first with attempts 0 and 1 and
// the second with attempt 0, plus another run with one attempt. It returns
// the run, its two tasks and the keys of its attempts in (task_id, attempt)
// order.
func attemptRun(t *testing.T, db *sqlx.DB) (uuid.UUID, []uuid.UUID, []string) {
	t.Helper()
	runID, tasks := newRunWithTasks(t, db, 2)
	_, other := newRunWithTasks(t, db, 1)
	insertAttemptRow(t, db, storedAttempt{TaskID: tasks[0], Attempt: 0, Status: "failed"})
	insertAttemptRow(t, db, storedAttempt{TaskID: tasks[0], Attempt: 1, Status: "running"})
	insertAttemptRow(t, db, storedAttempt{TaskID: tasks[1], Attempt: 0, Status: "succeeded"})
	insertAttemptRow(t, db, storedAttempt{TaskID: other[0], Attempt: 0, Status: "succeeded"})

	want := []string{attemptKey(tasks[0], 0), attemptKey(tasks[0], 1), attemptKey(tasks[1], 0)}
	slices.SortFunc(want, strings.Compare)
	return runID, tasks, want
}

func TestTaskAttemptReadRepository_ListsARunsAttemptsByTaskThenNumber(t *testing.T) {
	db := newTestDB(t)
	runID, tasks, want := attemptRun(t, db)
	at := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	_, err := db.Exec(`
		UPDATE task_attempts SET failure_class = 'job', failure_reason = 'oom_killed', exit_code = 137,
		       queued_at = $2, started_at = $2, finished_at = $2, execution_seconds = 4.5,
		       error_message = 'killed', log_s3_key = 'logs/a.log', run_results_uri = 'rr/a.json'
		WHERE task_id = $1 AND attempt = 0`, tasks[0], at)
	require.NoError(t, err)

	rows, total, err := postgres.NewTaskAttemptReadRepository(db).ListRunAttempts(context.Background(), runID, nil, 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, want, attemptKeys(rows))

	i := slices.Index(attemptKeys(rows), attemptKey(tasks[0], 0))
	got := rows[i]
	assert.Equal(t, "failed", got.Status)
	assert.Equal(t, "job-"+tasks[0].String()[:8]+"-0", got.JobName)
	assert.Equal(t, "job", *got.FailureClass)
	assert.Equal(t, "oom_killed", *got.FailureReason)
	assert.Equal(t, 137, *got.ExitCode)
	assert.False(t, got.DispatchedAt.IsZero())
	assert.True(t, at.Equal(*got.QueuedAt))
	assert.True(t, at.Equal(*got.StartedAt))
	assert.True(t, at.Equal(*got.FinishedAt))
	assert.InDelta(t, 4.5, *got.ExecutionSeconds, 1e-9)
	assert.Equal(t, "killed", *got.ErrorMessage)
	assert.Equal(t, "logs/a.log", *got.LogS3Key)
	assert.Equal(t, "rr/a.json", *got.RunResultsURI)

	bare := rows[slices.Index(attemptKeys(rows), attemptKey(tasks[1], 0))]
	assert.Nil(t, bare.FailureClass)
	assert.Nil(t, bare.ExitCode)
	assert.Nil(t, bare.QueuedAt)
	assert.Nil(t, bare.ExecutionSeconds)
	assert.Nil(t, bare.RunResultsURI)
}

func TestTaskAttemptReadRepository_FiltersToOneTask(t *testing.T) {
	db := newTestDB(t)
	runID, tasks, _ := attemptRun(t, db)

	rows, total, err := postgres.NewTaskAttemptReadRepository(db).ListRunAttempts(context.Background(), runID, &tasks[0], 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, []string{attemptKey(tasks[0], 0), attemptKey(tasks[0], 1)}, attemptKeys(rows))
}

func TestTaskAttemptReadRepository_PagesWithAStableOrder(t *testing.T) {
	db := newTestDB(t)
	runID, _, want := attemptRun(t, db)
	repo := postgres.NewTaskAttemptReadRepository(db)

	first, total, err := repo.ListRunAttempts(context.Background(), runID, nil, 2, 0)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	second, total, err := repo.ListRunAttempts(context.Background(), runID, nil, 2, 2)
	require.NoError(t, err)
	assert.Equal(t, 3, total)

	assert.Equal(t, want, append(attemptKeys(first), attemptKeys(second)...))
}
