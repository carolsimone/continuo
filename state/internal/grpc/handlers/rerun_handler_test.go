package handlers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/model"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ─── stubs ───────────────────────────────────────────────────────────────────

type rerunSchedStub struct {
	scheduler   *model.SchedulerTracker
	getErr      error
	updateTxErr error
	initTxErr   error
	updated     *model.SchedulerTracker
}

func (s *rerunSchedStub) Create(_ context.Context, _ *model.SchedulerTracker) error { return nil }
func (s *rerunSchedStub) GetByID(_ context.Context, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return s.scheduler, s.getErr
}
func (s *rerunSchedStub) Update(_ context.Context, _ *model.SchedulerTracker) error { return nil }
func (s *rerunSchedStub) Cancel(_ context.Context, _ uuid.UUID, _, _ string) error  { return nil }
func (s *rerunSchedStub) List(_ context.Context, _ postgres.SchedulerFilters) ([]*model.SchedulerTracker, int, error) {
	return nil, 0, nil
}
func (s *rerunSchedStub) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (s *rerunSchedStub) UpdateInitializationStatus(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (s *rerunSchedStub) ResetInProgressInitializations(_ context.Context) (int, error) {
	return 0, nil
}
func (s *rerunSchedStub) GetLastRunPerSchedule(_ context.Context) (map[string]postgres.LastRunData, error) {
	return map[string]postgres.LastRunData{}, nil
}
func (s *rerunSchedStub) GetActiveScheduler(_ context.Context, _ string) (*model.SchedulerTracker, error) {
	return nil, nil
}
func (s *rerunSchedStub) UpdateTx(_ context.Context, _ *sqlx.Tx, t *model.SchedulerTracker) error {
	s.updated = t
	return s.updateTxErr
}
func (s *rerunSchedStub) UpdateInitializationStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return s.initTxErr
}
func (s *rerunSchedStub) CreateTx(_ context.Context, _ *sqlx.Tx, _ *model.SchedulerTracker) error {
	return nil
}
func (s *rerunSchedStub) IncrementTerminalCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (int32, int32, error) {
	return 0, 0, nil
}
func (s *rerunSchedStub) DecrementTerminalCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ int32) error {
	return nil
}
func (s *rerunSchedStub) SetTotalTaskCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ int32) error {
	return nil
}
func (s *rerunSchedStub) UpdateStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (s *rerunSchedStub) GetByIDForUpdateTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return s.scheduler, s.getErr
}
func (s *rerunSchedStub) FinalizeRunTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (s *rerunSchedStub) CancelTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _, _ string) error {
	return nil
}

type rerunTaskStub struct {
	task         *model.TaskTracker
	getNodeErr   error
	runningTasks []*model.TaskTracker
	updateTxErr  error
	updated      *model.TaskTracker
}

func (s *rerunTaskStub) Create(_ context.Context, _ *model.TaskTracker) error { return nil }
func (s *rerunTaskStub) GetByID(_ context.Context, _ uuid.UUID) (*model.TaskTracker, error) {
	return nil, postgres.ErrNotFound
}
func (s *rerunTaskStub) GetByScheduleAndNode(_ context.Context, _ uuid.UUID, _, _, _ string) (*model.TaskTracker, error) {
	return s.task, s.getNodeErr
}
func (s *rerunTaskStub) Update(_ context.Context, _ *model.TaskTracker) error { return nil }
func (s *rerunTaskStub) Delete(_ context.Context, _ uuid.UUID) error           { return nil }
func (s *rerunTaskStub) ListByScheduleID(_ context.Context, _ uuid.UUID, _ *model.TaskStatus, _, _ int) ([]*model.TaskTracker, int, error) {
	return nil, 0, nil
}
func (s *rerunTaskStub) List(_ context.Context, _ postgres.TaskFilters) ([]*model.TaskTracker, int, error) {
	return s.runningTasks, len(s.runningTasks), nil
}
func (s *rerunTaskStub) UpdateTx(_ context.Context, _ *sqlx.Tx, t *model.TaskTracker) error {
	s.updated = t
	return s.updateTxErr
}
func (s *rerunTaskStub) BulkCreateTx(_ context.Context, _ *sqlx.Tx, _ []*model.TaskTracker) error {
	return nil
}
func (s *rerunTaskStub) ListAllByScheduleID(_ context.Context, _ uuid.UUID) ([]*model.TaskTracker, error) {
	return nil, nil
}
func (s *rerunTaskStub) ResetTasksTx(_ context.Context, _ *sqlx.Tx, _ []uuid.UUID) (int32, error) {
	return 0, nil
}
func (s *rerunTaskStub) UpdateStatusIfChangedTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string, _ int32) (int32, error) {
	return 0, nil
}
func (s *rerunTaskStub) ExistsTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (s *rerunTaskStub) HasFailedTaskTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (s *rerunTaskStub) HasRetryableFailedTaskTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (s *rerunTaskStub) BulkCancelByScheduleIDTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) (int64, error) {
	return 0, nil
}

type rerunOutboxStub struct {
	created   *postgres.OutboxEntry
	createErr error
}

func (s *rerunOutboxStub) Create(_ context.Context, _ *sqlx.Tx, e *postgres.OutboxEntry) error {
	s.created = e
	return s.createErr
}
func (s *rerunOutboxStub) ListPending(_ context.Context, _ int) ([]*postgres.OutboxEntry, error) {
	return nil, nil
}
func (s *rerunOutboxStub) MarkPublished(_ context.Context, _ uuid.UUID) error  { return nil }
func (s *rerunOutboxStub) IncrementRetry(_ context.Context, _ uuid.UUID) error { return nil }

// ─── helpers ─────────────────────────────────────────────────────────────────

// buildRerunHandler builds a handler with a nil DB — safe for tests that never
// reach db.BeginTxx (all pre-flight error cases).
func buildRerunHandler(sched *rerunSchedStub, task *rerunTaskStub, outbox *rerunOutboxStub) *RerunHandler {
	return NewRerunHandler(nil, sched, task, outbox, newTestLogger())
}

// buildRerunHandlerWithDB builds a handler with a real DB connection.
// The test is skipped if the DB is not available.
func buildRerunHandlerWithDB(t *testing.T, sched *rerunSchedStub, task *rerunTaskStub, outbox *rerunOutboxStub) *RerunHandler {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	t.Cleanup(func() { db.Close() })
	return NewRerunHandler(db, sched, task, outbox, newTestLogger())
}

// ─── tests ───────────────────────────────────────────────────────────────────

func TestTriggerRerun_EmptyScheduleID(t *testing.T) {
	h := buildRerunHandler(&rerunSchedStub{}, &rerunTaskStub{}, &rerunOutboxStub{})
	_, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestTriggerRerun_InvalidScheduleIDFormat(t *testing.T) {
	h := buildRerunHandler(&rerunSchedStub{}, &rerunTaskStub{}, &rerunOutboxStub{})
	_, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId: "not-a-uuid",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestTriggerRerun_ScheduleNotFound(t *testing.T) {
	sched := &rerunSchedStub{getErr: postgres.ErrNotFound}
	h := buildRerunHandler(sched, &rerunTaskStub{}, &rerunOutboxStub{})
	_, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId: uuid.New().String(),
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestTriggerRerun_TaskNotFound(t *testing.T) {
	sched := &rerunSchedStub{scheduler: &model.SchedulerTracker{ScheduleID: uuid.New(), ScheduleName: "s"}}
	task := &rerunTaskStub{getNodeErr: postgres.ErrNotFound}
	h := buildRerunHandler(sched, task, &rerunOutboxStub{})
	_, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId: sched.scheduler.ScheduleID.String(),
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestTriggerRerun_RunningTasksExist(t *testing.T) {
	sched := &rerunSchedStub{scheduler: &model.SchedulerTracker{ScheduleID: uuid.New(), ScheduleName: "s"}}
	task := &rerunTaskStub{
		task:         &model.TaskTracker{TaskID: uuid.New(), Status: model.TaskStatusFailed},
		runningTasks: []*model.TaskTracker{{Status: model.TaskStatusRunning}},
	}
	h := buildRerunHandler(sched, task, &rerunOutboxStub{})
	_, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId: sched.scheduler.ScheduleID.String(),
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestTriggerRerun_TargetNotFailed(t *testing.T) {
	sched := &rerunSchedStub{scheduler: &model.SchedulerTracker{ScheduleID: uuid.New(), ScheduleName: "s"}}
	task := &rerunTaskStub{
		task: &model.TaskTracker{TaskID: uuid.New(), Status: model.TaskStatusSucceeded},
	}
	h := buildRerunHandler(sched, task, &rerunOutboxStub{})
	_, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId: sched.scheduler.ScheduleID.String(),
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestTriggerRerun_Success(t *testing.T) {
	scheduleID := uuid.New()
	sched := &rerunSchedStub{scheduler: &model.SchedulerTracker{
		ScheduleID:   scheduleID,
		ScheduleName: "my-schedule",
		Status:       model.SchedulerStatusFailed,
		CreatedAt:    time.Now(),
	}}
	task := &rerunTaskStub{
		task: &model.TaskTracker{
			TaskID:     uuid.New(),
			ScheduleID: scheduleID,
			Status:     model.TaskStatusFailed,
			RetryCount: 3,
		},
	}
	outbox := &rerunOutboxStub{}
	h := buildRerunHandlerWithDB(t, sched, task, outbox)

	resp, err := h.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  scheduleID.String(),
		Schema:      "public",
		TableName:   "orders",
		ServiceName: "svc-1",
	})

	require.NoError(t, err)
	assert.NotNil(t, resp)

	// Scheduler was reset to RUNNING
	require.NotNil(t, sched.updated)
	assert.Equal(t, model.SchedulerStatusRunning, sched.updated.Status)
	assert.Nil(t, sched.updated.CompletedAt)

	// Task reset is handled by run_rerun_dispatched_handler, not here.
	assert.Nil(t, task.updated)

	// Outbox entry written to trigger.rerun:v1
	require.NotNil(t, outbox.created)
	assert.Equal(t, "trigger.rerun:v1", outbox.created.StreamName)
	assert.Equal(t, "rerun_node", outbox.created.EventType)
	assert.Equal(t, scheduleID, outbox.created.AggregateID)
}

// buildRerunHandlerFullDB builds a handler backed entirely by real repositories and a
// real DB connection. Used for integration tests that need to assert DB state after
// the transaction commits.
func buildRerunHandlerFullDB(t *testing.T) (*RerunHandler, *sqlx.DB) {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	t.Cleanup(func() { db.Close() })
	logger := newTestLogger()
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	return NewRerunHandler(db, schedulerRepo, taskRepo, outboxRepo, logger), db
}

// seedSchedulerForKindTest inserts a scheduler_tracker row with kind='cron' (the default),
// returning its schedule_name for later assertions.
func seedSchedulerForKindTest(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID, scheduleName string) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO scheduler_tracker (
			schedule_id, schedule_name, status, created_at,
			initialization_status, service_metadata
		) VALUES ($1, $2, $3, $4, $5, $6)
	`,
		scheduleID,
		scheduleName,
		string(model.SchedulerStatusFailed),
		time.Now(),
		"completed",
		json.RawMessage(`{}`),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, scheduleID)
		db.ExecContext(context.Background(), `DELETE FROM task_tracker WHERE schedule_id = $1`, scheduleID)
		db.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
	})
}

// seedFailedTaskForKindTest inserts a task_tracker row in FAILED state tied to the given schedule.
func seedFailedTaskForKindTest(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID, serviceName, schemaName, tableName string) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO task_tracker (
			task_id, schedule_id, created_at, service_name, schema_name,
			table_name, job_name, status, retry_count, max_retries
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`,
		uuid.New(), scheduleID, time.Now(),
		serviceName, schemaName, tableName,
		"job_"+tableName,
		string(model.TaskStatusFailed), 0, 3,
	)
	require.NoError(t, err)
}

func TestTriggerRerun_SetsKindRerunAndOutboxPayload(t *testing.T) {
	handler, db := buildRerunHandlerFullDB(t)

	scheduleID := uuid.New()
	scheduleName := "rk-" + uuid.New().String()[:8]
	seedSchedulerForKindTest(t, db, scheduleID, scheduleName)
	seedFailedTaskForKindTest(t, db, scheduleID, "service-1", "schema-x", "table-y")

	_, err := handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  scheduleID.String(),
		ServiceName: "service-1",
		Schema:      "schema-x",
		TableName:   "table-y",
	})
	require.NoError(t, err)

	// Assert scheduler_tracker.kind was flipped to "rerun".
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, newTestLogger())
	tracker, err := schedulerRepo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.Equal(t, "rerun", tracker.Kind)

	// Assert outbox payload contains kind="rerun".
	var payloadRaw []byte
	require.NoError(t, db.GetContext(context.Background(), &payloadRaw,
		`SELECT payload FROM state_outbox WHERE aggregate_id = $1 ORDER BY created_at DESC LIMIT 1`, scheduleID))
	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal(payloadRaw, &payload))
	assert.Equal(t, "rerun", payload["kind"])
}
