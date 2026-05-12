package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/model"
	statehandlers "github.com/carolsimone/continuo/state/service/handlers"
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
	outboxRepo := postgres.NewOutboxRepository(db, logger)

	handler := statehandlers.NewRunEntriesDispatchedHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)

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
	outboxRepo := postgres.NewOutboxRepository(db, logger)

	handler := statehandlers.NewRunEntriesDispatchedHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)

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
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	handler := statehandlers.NewRunEntriesDispatchedHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)

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

	outboxRepo := postgres.NewOutboxRepository(db, discardLogger())
	h := statehandlers.NewRunEntriesDispatchedHandler(db, schedRepo, taskRepo, outboxRepo, discardLogger())

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

// TestRunEntriesDispatched_PR2_PerTaskStatus verifies that the handler honors
// the per-task Status and InheritedFromTaskID fields introduced by PR2 (rebase
// from failed). A mixed-status payload (one rebased + one inherited-succeeded)
// must produce task_tracker rows that mirror the wire-format values verbatim.
func TestRunEntriesDispatched_PR2_PerTaskStatus(t *testing.T) {
	db := setupRunEntriesDispatchedTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	h := statehandlers.NewRunEntriesDispatchedHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)

	scheduleID := uuid.New()
	seedSchedulerTracker(t, db, scheduleID, "in_progress")
	t.Cleanup(func() { db.Exec(`DELETE FROM processed_events`) })

	rootTaskID := uuid.New()
	rebasedTaskID := uuid.New()
	inheritedTaskID := uuid.New()

	evt := events.RunEntriesDispatched{
		ScheduleID:   scheduleID.String(),
		ScheduleName: "rebase-test",
		AllTasks: []events.DispatchedTask{
			{
				TaskID: rebasedTaskID.String(), ServiceName: "svc", SchemaName: "s", TableName: "rebased",
				NodeType: "dbt-model", MaxRetries: 2, ManifestVersion: "v2", ImageTag: "img:2",
				Status: "pending",
			},
			{
				TaskID: inheritedTaskID.String(), ServiceName: "svc", SchemaName: "s", TableName: "inherited",
				NodeType: "dbt-model", MaxRetries: 0, ManifestVersion: "v1", ImageTag: "img:1",
				Status: "succeeded", InheritedFromTaskID: rootTaskID.String(),
			},
		},
		TotalTaskCount: 2,
	}
	payload, err := json.Marshal(evt)
	require.NoError(t, err)

	ack, err := h.Handle(context.Background(), "msg-perTaskStatus-"+uuid.New().String(), string(payload))
	require.NoError(t, err)
	require.True(t, ack)

	rebased, err := taskRepo.GetByID(context.Background(), rebasedTaskID)
	require.NoError(t, err)
	assert.Equal(t, model.TaskStatusPending, rebased.Status)
	assert.Nil(t, rebased.InheritedFromTaskID)

	inherited, err := taskRepo.GetByID(context.Background(), inheritedTaskID)
	require.NoError(t, err)
	assert.Equal(t, model.TaskStatusSucceeded, inherited.Status)
	require.NotNil(t, inherited.InheritedFromTaskID)
	assert.Equal(t, rootTaskID, *inherited.InheritedFromTaskID)
}

// TestRunEntriesDispatched_PR2_BackwardCompatDefaultsPending verifies that a
// pre-PR2 producer (one that omits Status from the JSON payload) still
// produces task_tracker rows in 'pending' state — never the empty string,
// never an invalid zero-value.
func TestRunEntriesDispatched_PR2_BackwardCompatDefaultsPending(t *testing.T) {
	db := setupRunEntriesDispatchedTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	h := statehandlers.NewRunEntriesDispatchedHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)

	scheduleID := uuid.New()
	seedSchedulerTracker(t, db, scheduleID, "in_progress")
	t.Cleanup(func() { db.Exec(`DELETE FROM processed_events`) })

	taskID := uuid.New()
	// Pre-PR2 producer: omit Status entirely from JSON.
	payload := fmt.Sprintf(`{"schedule_id":%q,"schedule_name":"cron-test","total_task_count":1,"all_tasks":[{"task_id":%q,"service_name":"svc","schema_name":"s","table_name":"x","node_type":"dbt-model","max_retries":2,"manifest_version":"v1","image_tag":"img:1"}]}`,
		scheduleID.String(), taskID.String())

	ack, err := h.Handle(context.Background(), "msg-bc-"+uuid.New().String(), payload)
	require.NoError(t, err)
	require.True(t, ack)

	got, err := taskRepo.GetByID(context.Background(), taskID)
	require.NoError(t, err)
	assert.Equal(t, model.TaskStatusPending, got.Status, "missing Status must default to pending")
	assert.Nil(t, got.InheritedFromTaskID)
}

// newTestHandlerWithRepos builds a fresh handler with real Postgres-backed
// repos for tests that need to assert post-handle scheduler/task state.
func newTestHandlerWithRepos(t *testing.T) (*statehandlers.RunEntriesDispatchedHandler, *sqlx.DB, postgres.SchedulerTrackerRepository, postgres.TaskTrackerRepository, postgres.OutboxRepository) {
	t.Helper()
	db := setupRunEntriesDispatchedTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	h := statehandlers.NewRunEntriesDispatchedHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)
	t.Cleanup(func() { db.Exec(`DELETE FROM processed_events`) })
	return h, db, schedulerRepo, taskRepo, outboxRepo
}

// seedSchedulerWithKind inserts a scheduler_tracker row with explicit kind
// (cron / trigger / rerun / rebase / single_node_run) and returns its id.
func seedSchedulerWithKind(t *testing.T, db *sqlx.DB, name, kind string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(`
		INSERT INTO scheduler_tracker (
			schedule_id, schedule_name, status, created_at,
			initialization_status, service_metadata, total_task_count, terminal_task_count, kind
		) VALUES ($1, $2, $3, $4, $5, $6, NULL, 0, $7)
	`,
		id,
		name,
		string(model.SchedulerStatusPending),
		time.Now(),
		"in_progress",
		json.RawMessage(`{}`),
		kind,
	)
	require.NoError(t, err)
	return id
}

// TestRunEntriesDispatched_PR2_AllSucceededAutoRollup verifies that when every
// projected task arrives in a terminal-succeeded state (the rebase-of-only-
// inherited-tasks edge case), the scheduler is auto-rolled-up to SUCCEEDED
// with completed_at set, rather than being routed through RUNNING.
func TestRunEntriesDispatched_PR2_AllSucceededAutoRollup(t *testing.T) {
	h, db, schedulerRepo, _, _ := newTestHandlerWithRepos(t)
	scheduleID := seedSchedulerWithKind(t, db, "rebase-rollup-"+uuid.New().String()[:8], "rebase")
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DELETE FROM task_tracker WHERE schedule_id = $1", scheduleID)
		db.ExecContext(context.Background(), "DELETE FROM state_outbox WHERE aggregate_id = $1", scheduleID)
		db.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", scheduleID)
	})

	root1 := uuid.New()
	root2 := uuid.New()
	t1 := uuid.New()
	t2 := uuid.New()

	evt := events.RunEntriesDispatched{
		ScheduleID:   scheduleID.String(),
		ScheduleName: "rebase-rollup",
		AllTasks: []events.DispatchedTask{
			{TaskID: t1.String(), ServiceName: "svc", SchemaName: "s", TableName: "a",
				NodeType: "dbt-model", MaxRetries: 0, ManifestVersion: "v1", ImageTag: "img:1",
				Status: "succeeded", InheritedFromTaskID: root1.String()},
			{TaskID: t2.String(), ServiceName: "svc", SchemaName: "s", TableName: "b",
				NodeType: "dbt-model", MaxRetries: 0, ManifestVersion: "v1", ImageTag: "img:1",
				Status: "succeeded", InheritedFromTaskID: root2.String()},
		},
		TotalTaskCount: 2,
	}
	payload, err := json.Marshal(evt)
	require.NoError(t, err)

	ack, err := h.Handle(context.Background(), "msg-rollup-"+uuid.New().String(), string(payload))
	require.NoError(t, err)
	require.True(t, ack)

	scheduler, err := schedulerRepo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.Equal(t, model.SchedulerStatusSucceeded, scheduler.Status, "all-succeeded projection should auto-rollup to SUCCEEDED")
	require.NotNil(t, scheduler.CompletedAt, "auto-rollup must set CompletedAt")
}

// TestRunEntriesDispatched_PR2_MixedPendingDoesNotRollup verifies that a
// projection containing at least one PENDING task continues to route through
// RUNNING (no auto-rollup, no completed_at) — preserving cron, trigger, rerun
// and partial-rebase semantics.
func TestRunEntriesDispatched_PR2_MixedPendingDoesNotRollup(t *testing.T) {
	h, db, schedulerRepo, _, _ := newTestHandlerWithRepos(t)
	scheduleID := seedSchedulerWithKind(t, db, "rebase-mixed-"+uuid.New().String()[:8], "rebase")
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DELETE FROM task_tracker WHERE schedule_id = $1", scheduleID)
		db.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", scheduleID)
	})

	t1 := uuid.New()
	t2 := uuid.New()
	root := uuid.New()
	evt := events.RunEntriesDispatched{
		ScheduleID:   scheduleID.String(),
		ScheduleName: "rebase-mixed",
		AllTasks: []events.DispatchedTask{
			{TaskID: t1.String(), ServiceName: "svc", SchemaName: "s", TableName: "rebased",
				NodeType: "dbt-model", MaxRetries: 2, ManifestVersion: "v2", ImageTag: "img:2",
				Status: "pending"},
			{TaskID: t2.String(), ServiceName: "svc", SchemaName: "s", TableName: "inherited",
				NodeType: "dbt-model", MaxRetries: 0, ManifestVersion: "v1", ImageTag: "img:1",
				Status: "succeeded", InheritedFromTaskID: root.String()},
		},
		TotalTaskCount: 2,
	}
	payload, err := json.Marshal(evt)
	require.NoError(t, err)

	ack, err := h.Handle(context.Background(), "msg-mixed-rollup-"+uuid.New().String(), string(payload))
	require.NoError(t, err)
	require.True(t, ack)

	scheduler, err := schedulerRepo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.Equal(t, model.SchedulerStatusRunning, scheduler.Status, "mixed projection (>=1 PENDING) should still go to RUNNING")
	assert.Nil(t, scheduler.CompletedAt, "RUNNING transition must NOT set CompletedAt")
}

// TestRunEntriesDispatched_SeedsTerminalCountForInheritedTasks asserts that
// when a mixed projection (one PENDING + N SUCCEEDED inherits) is dispatched,
// scheduler_tracker.terminal_task_count is seeded with N — so that when the
// remaining PENDING task transitions to terminal via task.status.updated:v1,
// terminal_task_count == total_task_count and the run finalizes normally.
func TestRunEntriesDispatched_SeedsTerminalCountForInheritedTasks(t *testing.T) {
	h, db, schedulerRepo, _, _ := newTestHandlerWithRepos(t)
	scheduleID := seedSchedulerWithKind(t, db, "rebase-seedterm-"+uuid.New().String()[:8], "rebase")
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DELETE FROM task_tracker WHERE schedule_id = $1", scheduleID)
		db.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", scheduleID)
	})

	pending := uuid.New()
	inherit1 := uuid.New()
	inherit2 := uuid.New()
	root1 := uuid.New()
	root2 := uuid.New()

	evt := events.RunEntriesDispatched{
		ScheduleID:   scheduleID.String(),
		ScheduleName: "rebase-seedterm",
		AllTasks: []events.DispatchedTask{
			{TaskID: pending.String(), ServiceName: "svc", SchemaName: "s", TableName: "rebased",
				NodeType: "dbt-model", MaxRetries: 2, ManifestVersion: "v2", ImageTag: "img:2",
				Status: "pending"},
			{TaskID: inherit1.String(), ServiceName: "svc", SchemaName: "s", TableName: "ok1",
				NodeType: "dbt-model", MaxRetries: 0, ManifestVersion: "v1", ImageTag: "img:1",
				Status: "succeeded", InheritedFromTaskID: root1.String()},
			{TaskID: inherit2.String(), ServiceName: "svc", SchemaName: "s", TableName: "ok2",
				NodeType: "dbt-model", MaxRetries: 0, ManifestVersion: "v1", ImageTag: "img:1",
				Status: "succeeded", InheritedFromTaskID: root2.String()},
		},
		TotalTaskCount: 3,
	}
	payload, err := json.Marshal(evt)
	require.NoError(t, err)

	ack, err := h.Handle(context.Background(), "msg-seedterm-"+uuid.New().String(), string(payload))
	require.NoError(t, err)
	require.True(t, ack)

	tracker, err := schedulerRepo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.Equal(t, model.SchedulerStatusRunning, tracker.Status,
		"mixed pending+terminal must still go to RUNNING")
	assert.True(t, tracker.TotalTaskCount.Valid)
	assert.Equal(t, int32(3), tracker.TotalTaskCount.Int32)
	assert.Equal(t, int32(2), tracker.TerminalTaskCount,
		"terminal_task_count must be seeded with the count of pre-terminal task rows")
}

// TestRunEntriesDispatched_AutoRollupEmitsRunFinalized asserts that when the
// auto-rollup branch fires (every dispatched task is already terminal), the
// handler writes a run.finalized:v1 outbox row in the same tx — without it,
// orchestrator never finalizes the Neo4j :Run and the run stays "active".
func TestRunEntriesDispatched_AutoRollupEmitsRunFinalized(t *testing.T) {
	db := setupRunEntriesDispatchedTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	h := statehandlers.NewRunEntriesDispatchedHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)

	scheduleID := seedSchedulerWithKind(t, db, "rebase-rollup-emit-"+uuid.New().String()[:8], "rebase")
	t.Cleanup(func() {
		ctx := context.Background()
		db.ExecContext(ctx, "DELETE FROM task_tracker WHERE schedule_id = $1", scheduleID)
		db.ExecContext(ctx, "DELETE FROM state_outbox WHERE aggregate_id = $1", scheduleID)
		db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", scheduleID)
		db.Exec(`DELETE FROM processed_events`)
	})

	t1 := uuid.New()
	t2 := uuid.New()
	root1 := uuid.New()
	root2 := uuid.New()
	evt := events.RunEntriesDispatched{
		ScheduleID:   scheduleID.String(),
		ScheduleName: "rebase-rollup-emit",
		AllTasks: []events.DispatchedTask{
			{TaskID: t1.String(), ServiceName: "svc", SchemaName: "s", TableName: "a",
				NodeType: "dbt-model", MaxRetries: 0, ManifestVersion: "v1", ImageTag: "img:1",
				Status: "succeeded", InheritedFromTaskID: root1.String()},
			{TaskID: t2.String(), ServiceName: "svc", SchemaName: "s", TableName: "b",
				NodeType: "dbt-model", MaxRetries: 0, ManifestVersion: "v1", ImageTag: "img:1",
				Status: "succeeded", InheritedFromTaskID: root2.String()},
		},
		TotalTaskCount: 2,
	}
	payload, err := json.Marshal(evt)
	require.NoError(t, err)

	ack, err := h.Handle(context.Background(), "msg-rollup-emit-"+uuid.New().String(), string(payload))
	require.NoError(t, err)
	require.True(t, ack)

	// Assert run.finalized:v1 outbox row was written.
	var count int
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM state_outbox
		 WHERE aggregate_id = $1 AND stream_name = 'run.finalized:v1'`,
		scheduleID,
	).Scan(&count))
	assert.Equal(t, 1, count, "auto-rollup must emit exactly one run.finalized:v1")

	// Assert the payload reflects the terminal status.
	var rawPayload []byte
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT payload FROM state_outbox
		 WHERE aggregate_id = $1 AND stream_name = 'run.finalized:v1'`,
		scheduleID,
	).Scan(&rawPayload))
	var fin events.RunFinalized
	require.NoError(t, json.Unmarshal(rawPayload, &fin))
	assert.Equal(t, scheduleID.String(), fin.ScheduleID)
	assert.Equal(t, "succeeded", fin.Status)

	// Fold-in from T3 review: rollup must also seed terminal_task_count.
	tracker, err := schedulerRepo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.Equal(t, int32(2), tracker.TerminalTaskCount,
		"auto-rollup must seed terminal_task_count")
}

// TestRunEntriesDispatchedHandler_NoopWhenSchedulerTerminal verifies that a
// second delivery of run.entries.dispatched (different Redis message ID,
// bypassing processed_events dedup) is silently dropped when the scheduler is
// already in a terminal state (failed or succeeded). Without this guard the
// duplicate would call SetTerminalTaskCountTx and overwrite an already-correct
// terminal_task_count, breaking the B1 finalization invariant.
func TestRunEntriesDispatchedHandler_NoopWhenSchedulerTerminal(t *testing.T) {
	for _, terminalStatus := range []model.SchedulerStatus{
		model.SchedulerStatusFailed,
		model.SchedulerStatusSucceeded,
	} {
		terminalStatus := terminalStatus
		t.Run(string(terminalStatus), func(t *testing.T) {
			h, db, schedulerRepo, taskRepo, _ := newTestHandlerWithRepos(t)
			schedID := uuid.New()
			seedSchedulerTracker(t, db, schedID, "completed")

			// Advance scheduler to terminal status.
			_, err := db.ExecContext(context.Background(),
				`UPDATE scheduler_tracker
				    SET status = $2,
				        terminal_task_count = 8,
				        total_task_count = 8,
				        completed_at = NOW()
				  WHERE schedule_id = $1`, schedID, string(terminalStatus))
			require.NoError(t, err)

			t.Cleanup(func() {
				db.Exec(`DELETE FROM task_tracker WHERE schedule_id = $1`, schedID)
				db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, schedID)
			})

			// Simulate a duplicate delivery with a DIFFERENT message ID (bypasses dedup).
			evt := events.RunEntriesDispatched{
				ScheduleID:   schedID.String(),
				ScheduleName: "terminal-guard-test",
				AllTasks: []events.DispatchedTask{
					{TaskID: uuid.New().String(), ServiceName: "svc", SchemaName: "s", TableName: "t",
						NodeType: "dbt-model", MaxRetries: 0, Status: "succeeded"},
					{TaskID: uuid.New().String(), ServiceName: "svc", SchemaName: "s", TableName: "u",
						NodeType: "dbt-model", MaxRetries: 0, Status: "succeeded"},
				},
				TotalTaskCount: 2,
			}
			payload, marshalErr := json.Marshal(evt)
			require.NoError(t, marshalErr)

			ack, err := h.Handle(context.Background(), "duplicate-msg-"+uuid.New().String(), string(payload))
			require.NoError(t, err)
			assert.True(t, ack, "duplicate after terminal should ACK (no-op)")

			// terminal_task_count must remain 8 — the duplicate must not overwrite it.
			got, err := schedulerRepo.GetByID(context.Background(), schedID)
			require.NoError(t, err)
			assert.Equal(t, terminalStatus, got.Status, "status must remain %s", terminalStatus)
			assert.Equal(t, int32(8), got.TerminalTaskCount,
				"terminal_task_count must not be overwritten by duplicate delivery")

			// No new task rows should have been created.
			tasks, err := taskRepo.ListAllByScheduleID(context.Background(), schedID)
			require.NoError(t, err)
			assert.Empty(t, tasks, "duplicate delivery must not insert task rows")
		})
	}
}
