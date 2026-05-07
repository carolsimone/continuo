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

func setupRunEntriesDispatchedTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func seedSchedulerTracker(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID, initStatus string) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO scheduler_tracker (
			schedule_id, schedule_name, status, created_at,
			initialization_status, service_metadata, total_task_count, terminal_task_count
		) VALUES ($1, $2, $3, $4, $5, $6, NULL, 0)
	`,
		scheduleID,
		"test-schedule-"+scheduleID.String(),
		string(model.SchedulerStatusPending),
		time.Now(),
		initStatus,
		json.RawMessage(`{}`),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Exec(`DELETE FROM task_tracker WHERE schedule_id = $1`, scheduleID)
		db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
	})
}

func buildDispatchedPayload(t *testing.T, scheduleID uuid.UUID, tasks []events.DispatchedTask) string {
	t.Helper()
	evt := events.RunEntriesDispatched{
		ScheduleID:     scheduleID.String(),
		ScheduleName:   "test-schedule",
		AllTasks:       tasks,
		TotalTaskCount: int32(len(tasks)),
	}
	b, err := json.Marshal(evt)
	require.NoError(t, err)
	return string(b)
}

func TestRunEntriesDispatchedHandler_BulkCreatesTasksAndTransitionsInitStatus(t *testing.T) {
	db := setupRunEntriesDispatchedTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	seedSchedulerTracker(t, db, scheduleID, "in_progress")

	// clean up processed_events for this test (dedup IDs are deterministic UUIDs derived from messageID)
	t.Cleanup(func() {
		db.Exec(`DELETE FROM processed_events`)
	})

	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)

	handler := statehandlers.NewRunEntriesDispatchedHandler(db, schedulerRepo, taskRepo, logger)

	tasks := []events.DispatchedTask{
		{TaskID: uuid.New().String(), ServiceName: "svc-a", SchemaName: "public", TableName: "orders", NodeType: "model", MaxRetries: 3},
		{TaskID: uuid.New().String(), ServiceName: "svc-b", SchemaName: "raw", TableName: "events", NodeType: "model", MaxRetries: 2},
	}
	payload := buildDispatchedPayload(t, scheduleID, tasks)
	messageID := scheduleID.String() + "-test-bulk"

	shouldACK, err := handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Assert task rows created
	created, err := taskRepo.ListAllByScheduleID(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.Len(t, created, 2)

	// Assert scheduler_tracker updated
	tracker, err := schedulerRepo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.Equal(t, "completed", tracker.InitializationStatus)
	assert.Equal(t, model.SchedulerStatusRunning, tracker.Status)
	assert.True(t, tracker.TotalTaskCount.Valid)
	assert.Equal(t, int32(2), tracker.TotalTaskCount.Int32)
}

func TestRunEntriesDispatchedHandler_Idempotent(t *testing.T) {
	db := setupRunEntriesDispatchedTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	seedSchedulerTracker(t, db, scheduleID, "in_progress")

	t.Cleanup(func() { db.Exec(`DELETE FROM processed_events`) })

	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)

	handler := statehandlers.NewRunEntriesDispatchedHandler(db, schedulerRepo, taskRepo, logger)

	taskID := uuid.New().String()
	tasks := []events.DispatchedTask{
		{TaskID: taskID, ServiceName: "svc-a", SchemaName: "public", TableName: "orders", NodeType: "model", MaxRetries: 3},
	}
	payload := buildDispatchedPayload(t, scheduleID, tasks)
	messageID := scheduleID.String() + "-test-idem"

	// First call
	shouldACK, err := handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Second call — same message ID, should be a no-op
	// Reset status so UpdateStatusTx won't fail if scheduler is already RUNNING
	// (the handler is idempotent because it exits after dedup check)
	shouldACK, err = handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Assert still only 1 task row
	created, listErr := taskRepo.ListAllByScheduleID(context.Background(), scheduleID)
	require.NoError(t, listErr)
	assert.Len(t, created, 1, "duplicate message should not create extra task rows")

	// processed_events deduplication: exactly one entry for this message (keyed on derived UUID)
	dedupID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("run.entries.dispatched:"+messageID))
	var count int
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, dedupID,
	).Scan(&count))
	assert.Equal(t, 1, count, "processed_events should have exactly one entry")

	// The TotalTaskCount.Valid check
	tracker, err := schedulerRepo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.True(t, tracker.TotalTaskCount.Valid)
	assert.Equal(t, int32(1), tracker.TotalTaskCount.Int32)
}

func TestRunEntriesDispatchedHandler_PersistsManifestVersionPerTask(t *testing.T) {
	db := setupRunEntriesDispatchedTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	seedSchedulerTracker(t, db, scheduleID, "in_progress")
	t.Cleanup(func() { db.Exec(`DELETE FROM processed_events`) })

	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	handler := statehandlers.NewRunEntriesDispatchedHandler(db, schedulerRepo, taskRepo, logger)

	taskID := uuid.New()
	tasks := []events.DispatchedTask{
		{
			TaskID:          taskID.String(),
			ServiceName:     "svc-a",
			SchemaName:      "public",
			TableName:       "users",
			NodeType:        "dbt-model",
			MaxRetries:      3,
			ManifestVersion: "v7",
			ImageTag:        "abc123-1714300000",
		},
	}
	payload := buildDispatchedPayload(t, scheduleID, tasks)

	shouldACK, err := handler.Handle(context.Background(), scheduleID.String()+"-manifest-test", payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	created, err := taskRepo.ListAllByScheduleID(context.Background(), scheduleID)
	require.NoError(t, err)
	require.Len(t, created, 1)
	assert.Equal(t, "v7", created[0].ManifestVersion, "manifest_version must be persisted from the event payload")
	assert.Equal(t, "abc123-1714300000", created[0].ImageTag, "image_tag must be persisted from the event payload")
}

func TestRunEntriesDispatchedHandler_NoopWhenSchedulerCancelled(t *testing.T) {
	db := setupRunEntriesDispatchedTestDB(t)
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	ctx := context.Background()

	// Create a scheduler, then move its status to 'cancelled' directly.
	schedID := uuid.New()
	seedSchedulerTracker(t, db, schedID, "completed")
	_, err := db.ExecContext(ctx, `UPDATE scheduler_tracker SET status = 'cancelled', cancelled_at = NOW(), cancelled_by = 'test' WHERE schedule_id = $1`, schedID)
	require.NoError(t, err)

	t.Cleanup(func() { db.Exec(`DELETE FROM processed_events`) })

	h := statehandlers.NewRunEntriesDispatchedHandler(db, schedRepo, taskRepo, discardLogger())

	payload, _ := json.Marshal(events.RunEntriesDispatched{
		ScheduleID:   schedID.String(),
		ScheduleName: "test-schedule",
		AllTasks: []events.DispatchedTask{
			{TaskID: uuid.New().String(), ServiceName: "svc", SchemaName: "sc", TableName: "t", MaxRetries: 0},
		},
		TotalTaskCount: 1,
	})
	ack, err := h.Handle(ctx, "msg-noop-cancelled-1", string(payload))

	require.NoError(t, err)
	assert.True(t, ack, "should ACK (no-op) when scheduler already cancelled")

	// Scheduler status must remain 'cancelled', not revert to 'running'.
	got, err := schedRepo.GetByID(ctx, schedID)
	require.NoError(t, err)
	assert.Equal(t, model.SchedulerStatusCancelled, got.Status)
}
