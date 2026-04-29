package handlers_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	statehandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRerunTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// seedSchedulerForRerun inserts a scheduler_tracker row with given status, total, and terminal counts.
func seedSchedulerForRerun(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID, status model.SchedulerStatus, totalCount, terminalCount int32) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO scheduler_tracker (
			schedule_id, schedule_name, status, created_at,
			initialization_status, service_metadata, total_task_count, terminal_task_count
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`,
		scheduleID,
		"test-schedule-"+scheduleID.String(),
		string(status),
		time.Now(),
		"completed",
		json.RawMessage(`{}`),
		totalCount,
		terminalCount,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Exec(`DELETE FROM task_tracker WHERE schedule_id = $1`, scheduleID)
		db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
		db.Exec(`DELETE FROM processed_events`)
	})
}

// seedTaskForRerun inserts a task_tracker row with the given status.
func seedTaskForRerun(t *testing.T, db *sqlx.DB, taskID, scheduleID uuid.UUID, status model.TaskStatus) {
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

// getTaskStatus reads back the status for a task from the DB.
func getTaskStatus(t *testing.T, db *sqlx.DB, taskID uuid.UUID) model.TaskStatus {
	t.Helper()
	var statusStr string
	err := db.QueryRowContext(context.Background(),
		`SELECT status FROM task_tracker WHERE task_id = $1`, taskID,
	).Scan(&statusStr)
	require.NoError(t, err)
	return model.TaskStatus(statusStr)
}

// getTerminalTaskCount reads back terminal_task_count for a scheduler row.
func getTerminalTaskCount(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID) int32 {
	t.Helper()
	var count int32
	err := db.QueryRowContext(context.Background(),
		`SELECT terminal_task_count FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID,
	).Scan(&count)
	require.NoError(t, err)
	return count
}

// getSchedulerStatus reads back the status for a scheduler row.
func getSchedulerStatus(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID) model.SchedulerStatus {
	t.Helper()
	var statusStr string
	err := db.QueryRowContext(context.Background(),
		`SELECT status FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID,
	).Scan(&statusStr)
	require.NoError(t, err)
	return model.SchedulerStatus(statusStr)
}

func buildRerunPayload(t *testing.T, scheduleID uuid.UUID, tasksToReset []string, entryTaskID string) string {
	t.Helper()
	evt := events.RunRerunDispatched{
		ScheduleID:   scheduleID.String(),
		ScheduleName: "test-schedule",
		TasksToReset: tasksToReset,
		EntryTaskID:  entryTaskID,
	}
	b, err := json.Marshal(evt)
	require.NoError(t, err)
	return string(b)
}

func TestRunRerunDispatchedHandler_ResetsTasksAndRevivesRun(t *testing.T) {
	db := setupRerunTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	tA := uuid.New()
	tB := uuid.New()
	tC := uuid.New()

	// Seed: scheduler with status=FAILED, init_status=completed, total=3, terminal=3
	seedSchedulerForRerun(t, db, scheduleID, model.SchedulerStatusFailed, 3, 3)
	// Seed tasks
	seedTaskForRerun(t, db, tA, scheduleID, model.TaskStatusSucceeded)
	seedTaskForRerun(t, db, tB, scheduleID, model.TaskStatusSucceeded)
	seedTaskForRerun(t, db, tC, scheduleID, model.TaskStatusFailed)

	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)

	handler := statehandlers.NewRunRerunDispatchedHandler(db, schedulerRepo, taskRepo, logger)

	payload := buildRerunPayload(t, scheduleID, []string{tC.String(), tB.String()}, tC.String())
	messageID := scheduleID.String() + "-test-rerun"

	shouldACK, err := handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// tA stays SUCCEEDED
	assert.Equal(t, model.TaskStatusSucceeded, getTaskStatus(t, db, tA))
	// tB and tC reset to PENDING
	assert.Equal(t, model.TaskStatusPending, getTaskStatus(t, db, tB))
	assert.Equal(t, model.TaskStatusPending, getTaskStatus(t, db, tC))
	// terminal_task_count decremented from 3 -> 1 (2 tasks reset)
	assert.Equal(t, int32(1), getTerminalTaskCount(t, db, scheduleID))
	// scheduler status = RUNNING
	assert.Equal(t, model.SchedulerStatusRunning, getSchedulerStatus(t, db, scheduleID))
}

func TestRunRerunDispatchedHandler_Idempotent(t *testing.T) {
	db := setupRerunTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	tA := uuid.New()

	// Seed: scheduler with 1 FAILED task, terminal=total=1
	seedSchedulerForRerun(t, db, scheduleID, model.SchedulerStatusFailed, 1, 1)
	seedTaskForRerun(t, db, tA, scheduleID, model.TaskStatusFailed)

	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)

	handler := statehandlers.NewRunRerunDispatchedHandler(db, schedulerRepo, taskRepo, logger)

	payload := buildRerunPayload(t, scheduleID, []string{tA.String()}, tA.String())
	messageID := scheduleID.String() + "-test-rerun-idem"

	// First call
	shouldACK, err := handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Second call — same message ID, should be a no-op
	shouldACK, err = handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// terminal_task_count should NOT be double-decremented — stays at 0 (1 - 1 = 0)
	assert.Equal(t, int32(0), getTerminalTaskCount(t, db, scheduleID))

	// tA is PENDING (from first call)
	assert.Equal(t, model.TaskStatusPending, getTaskStatus(t, db, tA))

	// scheduler is RUNNING
	assert.Equal(t, model.SchedulerStatusRunning, getSchedulerStatus(t, db, scheduleID))

	// processed_events dedup: exactly one entry
	dedupID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("run.rerun.dispatched:"+messageID))
	var count int
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, dedupID,
	).Scan(&count))
	assert.Equal(t, 1, count, "processed_events should have exactly one entry for duplicate message")
}
