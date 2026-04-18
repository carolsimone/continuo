package redis_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	redisadapter "github.com/carolsimone/continuo/state/adapters/redis"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTaskExecutionRecordedTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// seedSchedulerAndTaskForExecution creates a scheduler_tracker and task_tracker row
// required as foreign-key parents for task_execution records.
func seedSchedulerAndTaskForExecution(t *testing.T, db *sqlx.DB, scheduleID, taskID uuid.UUID) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO scheduler_tracker (
			schedule_id, schedule_name, status, created_at,
			initialization_status, manifest_versions, total_task_count, terminal_task_count
		) VALUES ($1, $2, $3, $4, $5, $6, 1, 0)
	`,
		scheduleID,
		"test-schedule-"+scheduleID.String(),
		string(model.SchedulerStatusRunning),
		time.Now(),
		"completed",
		json.RawMessage(`{}`),
	)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO task_tracker (
			task_id, schedule_id, created_at, service_name, schema_name,
			table_name, job_name, status, retry_count, max_retries
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`,
		taskID, scheduleID, time.Now(),
		"svc-test", "public", "table_"+taskID.String()[:8],
		"job_"+taskID.String()[:8],
		string(model.TaskStatusRunning), 0, 3,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Exec(`DELETE FROM task_execution WHERE task_id = $1`, taskID)
		db.Exec(`DELETE FROM task_tracker WHERE schedule_id = $1`, scheduleID)
		db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
		db.Exec(`DELETE FROM processed_events`)
	})
}

func buildTaskExecutionRecordedPayload(t *testing.T, executionID, taskID uuid.UUID, jobName string, execSeconds float64, startedAt, completedAt time.Time) string {
	t.Helper()
	evt := events.TaskExecutionRecorded{
		ExecutionID:      executionID.String(),
		TaskID:           taskID.String(),
		JobName:          jobName,
		StartedAt:        startedAt.Format(time.RFC3339),
		CompletedAt:      completedAt.Format(time.RFC3339),
		ExecutionSeconds: execSeconds,
	}
	b, err := json.Marshal(evt)
	require.NoError(t, err)
	return string(b)
}

func TestTaskExecutionRecordedHandler_InsertsRecord(t *testing.T) {
	db := setupTaskExecutionRecordedTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	taskID := uuid.New()
	executionID := uuid.New()

	seedSchedulerAndTaskForExecution(t, db, scheduleID, taskID)

	execRepo := postgres.NewTaskExecutionRepository(db, logger)
	handler := redisadapter.NewTaskExecutionRecordedHandler(db, execRepo, logger)

	now := time.Now().UTC().Truncate(time.Second)
	startedAt := now.Add(-10 * time.Second)
	completedAt := now
	execSeconds := 10.0

	payload := buildTaskExecutionRecordedPayload(t, executionID, taskID, "test-job", execSeconds, startedAt, completedAt)
	messageID := executionID.String() + "-test-insert"

	shouldACK, err := handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Retrieve via repo and assert fields
	got, err := execRepo.GetByID(context.Background(), executionID)
	require.NoError(t, err)
	assert.Equal(t, executionID, got.ID)
	assert.Equal(t, taskID, got.TaskID)
	require.NotNil(t, got.ExecutionTimeSeconds)
	assert.InDelta(t, execSeconds, *got.ExecutionTimeSeconds, 0.001)
	require.NotNil(t, got.K8sJobName)
	assert.Equal(t, "test-job", *got.K8sJobName)
}

func TestTaskExecutionRecordedHandler_Idempotent(t *testing.T) {
	db := setupTaskExecutionRecordedTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	taskID := uuid.New()
	executionID := uuid.New()

	seedSchedulerAndTaskForExecution(t, db, scheduleID, taskID)

	execRepo := postgres.NewTaskExecutionRepository(db, logger)
	handler := redisadapter.NewTaskExecutionRecordedHandler(db, execRepo, logger)

	now := time.Now().UTC().Truncate(time.Second)
	payload := buildTaskExecutionRecordedPayload(t, executionID, taskID, "test-job", 5.0, now.Add(-5*time.Second), now)
	messageID := executionID.String() + "-test-idem"

	// First call
	shouldACK, err := handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Second call — same message ID, should be a no-op
	shouldACK, err = handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Assert only 1 execution record exists
	var count int
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM task_execution WHERE id = $1`, executionID,
	).Scan(&count))
	assert.Equal(t, 1, count, "duplicate message should not create extra task_execution rows")
}
