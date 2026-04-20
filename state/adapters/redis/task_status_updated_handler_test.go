package redis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	redisadapter "github.com/carolsimone/continuo/state/adapters/redis"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTaskStatusTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// seedSchedulerForTaskStatus seeds a scheduler_tracker row ready to receive task status events.
func seedSchedulerForTaskStatus(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID, totalCount, terminalCount int32) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO scheduler_tracker (
			schedule_id, schedule_name, status, created_at,
			initialization_status, manifest_versions, total_task_count, terminal_task_count
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`,
		scheduleID,
		"test-schedule-"+scheduleID.String(),
		string(model.SchedulerStatusRunning),
		time.Now(),
		"completed",
		json.RawMessage(`{}`),
		totalCount,
		terminalCount,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Exec(`DELETE FROM state_outbox WHERE aggregate_id = $1`, scheduleID)
		db.Exec(`DELETE FROM task_tracker WHERE schedule_id = $1`, scheduleID)
		db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
		db.Exec(`DELETE FROM processed_events`)
	})
}

// seedTaskForStatus inserts a task_tracker row.
func seedTaskForStatus(t *testing.T, db *sqlx.DB, taskID, scheduleID uuid.UUID, status model.TaskStatus) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO task_tracker (
			task_id, schedule_id, created_at, service_name, schema_name,
			table_name, job_name, status, retry_count, max_retries
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`,
		taskID, scheduleID, time.Now(),
		"svc-test", "public", "table_"+taskID.String()[:8],
		"job_"+taskID.String()[:8],
		string(status), 0, 3,
	)
	require.NoError(t, err)
}

// buildTaskStatusPayload builds a flat-field map for a task.status.updated:v1 event (Redis format).
func buildTaskStatusPayload(t *testing.T, taskID, scheduleID uuid.UUID, status string, retryCount int32) map[string]interface{} {
	t.Helper()
	return map[string]interface{}{
		"task_id":     taskID.String(),
		"schedule_id": scheduleID.String(),
		"status":      status,
		"retry_count": fmt.Sprintf("%d", retryCount),
	}
}

// getOutboxCountForSchedule returns the number of state_outbox rows for the given aggregate_id and event_type.
func getOutboxCountForSchedule(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID, eventType string) int {
	t.Helper()
	var count int
	err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM state_outbox WHERE aggregate_id = $1 AND event_type = $2`,
		scheduleID, eventType,
	).Scan(&count)
	require.NoError(t, err)
	return count
}

func newTaskStatusHandler(db *sqlx.DB, logger *slog.Logger) *redisadapter.TaskStatusUpdatedHandler {
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	return redisadapter.NewTaskStatusUpdatedHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)
}

// TestTaskStatusUpdatedHandler_TerminalCountAndFinalization verifies that processing two
// SUCCEEDED events increments terminal_task_count and finalizes the run as succeeded.
func TestTaskStatusUpdatedHandler_TerminalCountAndFinalization(t *testing.T) {
	db := setupTaskStatusTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	tA := uuid.New()
	tB := uuid.New()

	seedSchedulerForTaskStatus(t, db, scheduleID, 2, 0)
	seedTaskForStatus(t, db, tA, scheduleID, model.TaskStatusRunning)
	seedTaskForStatus(t, db, tB, scheduleID, model.TaskStatusRunning)

	handler := newTaskStatusHandler(db, logger)

	// Process tA SUCCEEDED
	payloadA := buildTaskStatusPayload(t, tA, scheduleID, "SUCCEEDED", 0)
	shouldACK, err := handler.Handle(context.Background(), "msg-tA-1", payloadA)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Assert terminal=1, still RUNNING
	assert.Equal(t, int32(1), getTerminalTaskCount(t, db, scheduleID))
	assert.Equal(t, model.SchedulerStatusRunning, getSchedulerStatus(t, db, scheduleID))
	assert.Equal(t, 0, getOutboxCountForSchedule(t, db, scheduleID, "run.finalized:v1"))

	// Process tB SUCCEEDED
	payloadB := buildTaskStatusPayload(t, tB, scheduleID, "SUCCEEDED", 0)
	shouldACK, err = handler.Handle(context.Background(), "msg-tB-1", payloadB)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Assert terminal=2, status=succeeded, outbox entry written
	assert.Equal(t, int32(2), getTerminalTaskCount(t, db, scheduleID))
	assert.Equal(t, model.SchedulerStatusSucceeded, getSchedulerStatus(t, db, scheduleID))
	assert.Equal(t, 1, getOutboxCountForSchedule(t, db, scheduleID, "run.finalized:v1"))
}

// TestTaskStatusUpdatedHandler_AnyFailedMarksRunFailed verifies that a single FAILED task
// causes the run to finalize as failed.
func TestTaskStatusUpdatedHandler_AnyFailedMarksRunFailed(t *testing.T) {
	db := setupTaskStatusTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	tA := uuid.New()
	tB := uuid.New()

	seedSchedulerForTaskStatus(t, db, scheduleID, 2, 0)
	seedTaskForStatus(t, db, tA, scheduleID, model.TaskStatusRunning)
	seedTaskForStatus(t, db, tB, scheduleID, model.TaskStatusRunning)

	handler := newTaskStatusHandler(db, logger)

	// Process tA SUCCEEDED
	shouldACK, err := handler.Handle(context.Background(), "msg-fail-tA",
		buildTaskStatusPayload(t, tA, scheduleID, "SUCCEEDED", 0))
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Process tB FAILED with retry_count=3 == max_retries=3 (exhausted — no retries left)
	shouldACK, err = handler.Handle(context.Background(), "msg-fail-tB",
		buildTaskStatusPayload(t, tB, scheduleID, "FAILED", 3))
	require.NoError(t, err)
	assert.True(t, shouldACK)

	assert.Equal(t, int32(2), getTerminalTaskCount(t, db, scheduleID))
	assert.Equal(t, model.SchedulerStatusFailed, getSchedulerStatus(t, db, scheduleID))
	assert.Equal(t, 1, getOutboxCountForSchedule(t, db, scheduleID, "run.finalized:v1"))
}

// TestTaskStatusUpdatedHandler_RetriesWhenTaskRowMissing verifies that handling an event
// for a non-existent task_id returns an error (triggering Redis redelivery).
func TestTaskStatusUpdatedHandler_RetriesWhenTaskRowMissing(t *testing.T) {
	db := setupTaskStatusTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	unknownTaskID := uuid.New()

	seedSchedulerForTaskStatus(t, db, scheduleID, 2, 0)
	// no task rows seeded

	handler := newTaskStatusHandler(db, logger)

	payload := buildTaskStatusPayload(t, unknownTaskID, scheduleID, "SUCCEEDED", 0)
	shouldACK, err := handler.Handle(context.Background(), "msg-missing-task", payload)
	assert.Error(t, err, "expected error for missing task row")
	assert.False(t, shouldACK, "should not ACK when task row is missing")
}

// TestTaskStatusUpdatedHandler_Idempotent verifies that processing the same message twice
// does not double-increment the terminal count.
func TestTaskStatusUpdatedHandler_Idempotent(t *testing.T) {
	db := setupTaskStatusTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	tA := uuid.New()

	seedSchedulerForTaskStatus(t, db, scheduleID, 2, 0)
	seedTaskForStatus(t, db, tA, scheduleID, model.TaskStatusRunning)

	handler := newTaskStatusHandler(db, logger)

	payload := buildTaskStatusPayload(t, tA, scheduleID, "SUCCEEDED", 0)

	shouldACK, err := handler.Handle(context.Background(), "msg-idem-tA", payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Second call with same message ID — must be a no-op
	shouldACK, err = handler.Handle(context.Background(), "msg-idem-tA", payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// terminal_task_count must NOT be double-incremented
	assert.Equal(t, int32(1), getTerminalTaskCount(t, db, scheduleID))
	// run still running (total=2, terminal=1)
	assert.Equal(t, model.SchedulerStatusRunning, getSchedulerStatus(t, db, scheduleID))
}

// TestTaskStatusUpdatedHandler_RetryableFailureDoesNotFinalize verifies that a FAILED event
// for a task that still has retries remaining does NOT finalize the run.
func TestTaskStatusUpdatedHandler_RetryableFailureDoesNotFinalize(t *testing.T) {
	db := setupTaskStatusTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	tA := uuid.New()

	// Single-task run; seedTaskForStatus uses max_retries=3, so retry_count=0 is retryable.
	seedSchedulerForTaskStatus(t, db, scheduleID, 1, 0)
	seedTaskForStatus(t, db, tA, scheduleID, model.TaskStatusRunning)

	handler := newTaskStatusHandler(db, logger)

	// FAILED with retry_count=0 (still has retries left)
	shouldACK, err := handler.Handle(context.Background(), "msg-retry-fail",
		buildTaskStatusPayload(t, tA, scheduleID, "FAILED", 0))
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// terminal_task_count is incremented (task is terminal)
	assert.Equal(t, int32(1), getTerminalTaskCount(t, db, scheduleID))
	// Run must NOT be finalized — stays running
	assert.Equal(t, model.SchedulerStatusRunning, getSchedulerStatus(t, db, scheduleID))
	assert.Equal(t, 0, getOutboxCountForSchedule(t, db, scheduleID, "run.finalized:v1"))
}

// TestTaskStatusUpdatedHandler_RetryRevivesRun verifies the full retry cycle:
// FAILED (retryable) → terminal++, no finalize → RUNNING (retry) → terminal-- → SUCCEEDED → finalize succeeded.
func TestTaskStatusUpdatedHandler_RetryRevivesRun(t *testing.T) {
	db := setupTaskStatusTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	tA := uuid.New()

	seedSchedulerForTaskStatus(t, db, scheduleID, 1, 0)
	seedTaskForStatus(t, db, tA, scheduleID, model.TaskStatusRunning)

	handler := newTaskStatusHandler(db, logger)

	// 1. FAILED (retry_count=0, max_retries=3) — retryable, must NOT finalize
	shouldACK, err := handler.Handle(context.Background(), "msg-retry-1-fail",
		buildTaskStatusPayload(t, tA, scheduleID, "FAILED", 0))
	require.NoError(t, err)
	assert.True(t, shouldACK)
	assert.Equal(t, 0, getOutboxCountForSchedule(t, db, scheduleID, "run.finalized:v1"))
	assert.Equal(t, int32(1), getTerminalTaskCount(t, db, scheduleID))
	assert.Equal(t, model.SchedulerStatusRunning, getSchedulerStatus(t, db, scheduleID))

	// 2. RUNNING (k8s retry) — must decrement terminal_count
	_, err = handler.Handle(context.Background(), "msg-retry-1-running",
		buildTaskStatusPayload(t, tA, scheduleID, "RUNNING", 1))
	require.NoError(t, err)
	assert.Equal(t, int32(0), getTerminalTaskCount(t, db, scheduleID))
	assert.Equal(t, model.SchedulerStatusRunning, getSchedulerStatus(t, db, scheduleID))

	// 3. SUCCEEDED on retry — should finalize as succeeded
	shouldACK, err = handler.Handle(context.Background(), "msg-retry-1-succeeded",
		buildTaskStatusPayload(t, tA, scheduleID, "SUCCEEDED", 1))
	require.NoError(t, err)
	assert.True(t, shouldACK)
	assert.Equal(t, int32(1), getTerminalTaskCount(t, db, scheduleID))
	assert.Equal(t, model.SchedulerStatusSucceeded, getSchedulerStatus(t, db, scheduleID))
	assert.Equal(t, 1, getOutboxCountForSchedule(t, db, scheduleID, "run.finalized:v1"))
}

// TestTaskStatusUpdatedHandler_ExhaustedRetriesFinalizeFailed verifies that a task
// with retry_count == max_retries (permanently failed) still finalizes the run as failed.
func TestTaskStatusUpdatedHandler_ExhaustedRetriesFinalizeFailed(t *testing.T) {
	db := setupTaskStatusTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	tA := uuid.New()

	seedSchedulerForTaskStatus(t, db, scheduleID, 1, 0)
	seedTaskForStatus(t, db, tA, scheduleID, model.TaskStatusRunning) // max_retries=3

	handler := newTaskStatusHandler(db, logger)

	// FAILED with retry_count=3 == max_retries=3 → permanently failed, no retries left
	shouldACK, err := handler.Handle(context.Background(), "msg-exhausted",
		buildTaskStatusPayload(t, tA, scheduleID, "FAILED", 3))
	require.NoError(t, err)
	assert.True(t, shouldACK)

	assert.Equal(t, int32(1), getTerminalTaskCount(t, db, scheduleID))
	assert.Equal(t, model.SchedulerStatusFailed, getSchedulerStatus(t, db, scheduleID))
	assert.Equal(t, 1, getOutboxCountForSchedule(t, db, scheduleID, "run.finalized:v1"))
}
