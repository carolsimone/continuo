package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	statehttp "github.com/carolsimone/continuo/state/adapters/http"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- fakes ---

type fakeSchedulerRepo struct {
	tracker *model.SchedulerTracker
	err     error
	updated *model.SchedulerTracker
	txErr   error
}

func (f *fakeSchedulerRepo) Create(_ context.Context, _ *model.SchedulerTracker) error { return nil }
func (f *fakeSchedulerRepo) GetByID(_ context.Context, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return f.tracker, f.err
}
func (f *fakeSchedulerRepo) Update(_ context.Context, t *model.SchedulerTracker) error {
	f.updated = t
	return nil
}
func (f *fakeSchedulerRepo) Cancel(_ context.Context, _ uuid.UUID, _, _ string) error { return nil }
func (f *fakeSchedulerRepo) List(_ context.Context, _ postgres.SchedulerFilters) ([]*model.SchedulerTracker, int, error) {
	return nil, 0, nil
}
func (f *fakeSchedulerRepo) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (f *fakeSchedulerRepo) UpdateInitializationStatus(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (f *fakeSchedulerRepo) ResetInProgressInitializations(_ context.Context) (int, error) {
	return 0, nil
}
func (f *fakeSchedulerRepo) GetLastRunPerSchedule(_ context.Context) (map[string]postgres.LastRunData, error) {
	return nil, nil
}
func (f *fakeSchedulerRepo) GetActiveScheduler(_ context.Context, _ string) (*model.SchedulerTracker, error) {
	return nil, nil
}
func (f *fakeSchedulerRepo) UpdateTx(_ context.Context, _ *sqlx.Tx, t *model.SchedulerTracker) error {
	f.updated = t
	return f.txErr
}
func (f *fakeSchedulerRepo) UpdateInitializationStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return f.txErr
}
func (f *fakeSchedulerRepo) CreateTx(_ context.Context, _ *sqlx.Tx, _ *model.SchedulerTracker) error {
	return nil
}

type fakeTaskRepo struct {
	task         *model.TaskTracker
	err          error
	runningTasks []*model.TaskTracker
	txErr        error
	updatedTask  *model.TaskTracker
}

func (f *fakeTaskRepo) Create(_ context.Context, _ *model.TaskTracker) error { return nil }
func (f *fakeTaskRepo) GetByID(_ context.Context, _ uuid.UUID) (*model.TaskTracker, error) {
	return f.task, f.err
}
func (f *fakeTaskRepo) GetByScheduleAndNode(_ context.Context, _ uuid.UUID, _, _, _ string) (*model.TaskTracker, error) {
	return f.task, f.err
}
func (f *fakeTaskRepo) Update(_ context.Context, t *model.TaskTracker) error {
	f.updatedTask = t
	return nil
}
func (f *fakeTaskRepo) Delete(_ context.Context, _ uuid.UUID) error { return nil }
func (f *fakeTaskRepo) ListByScheduleID(_ context.Context, _ uuid.UUID, _ *model.TaskStatus, _, _ int) ([]*model.TaskTracker, int, error) {
	return nil, 0, nil
}
func (f *fakeTaskRepo) List(_ context.Context, _ postgres.TaskFilters) ([]*model.TaskTracker, int, error) {
	return f.runningTasks, len(f.runningTasks), nil
}
func (f *fakeTaskRepo) UpdateTx(_ context.Context, _ *sqlx.Tx, t *model.TaskTracker) error {
	f.updatedTask = t
	return f.txErr
}

type fakeOutboxRepo struct {
	createdEntry *postgres.OutboxEntry
	txErr        error
}

func (f *fakeOutboxRepo) Create(_ context.Context, _ *sqlx.Tx, entry *postgres.OutboxEntry) error {
	f.createdEntry = entry
	return f.txErr
}
func (f *fakeOutboxRepo) ListPending(_ context.Context, _ int) ([]*postgres.OutboxEntry, error) {
	return nil, nil
}
func (f *fakeOutboxRepo) MarkPublished(_ context.Context, _ uuid.UUID) error { return nil }
func (f *fakeOutboxRepo) IncrementRetry(_ context.Context, _ uuid.UUID) error { return nil }

// --- helpers ---

func newRerunBody(schema, tableName, serviceName string) *bytes.Buffer {
	body, _ := json.Marshal(map[string]string{
		"schema":       schema,
		"table_name":   tableName,
		"service_name": serviceName,
	})
	return bytes.NewBuffer(body)
}

func getTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func buildHandler(t *testing.T, schedulerRepo postgres.SchedulerTrackerRepository, taskRepo postgres.TaskTrackerRepository, outboxRepo postgres.OutboxRepository) http.Handler {
	return statehttp.NewRerunHandler(getTestDB(t), schedulerRepo, taskRepo, outboxRepo, discardLogger())
}

// --- tests ---

func TestRerunHandler_404_ScheduleNotFound(t *testing.T) {
	schedRepo := &fakeSchedulerRepo{err: postgres.ErrNotFound}
	h := buildHandler(t, schedRepo, &fakeTaskRepo{}, &fakeOutboxRepo{})

	id := uuid.New()
	req := httptest.NewRequest(http.MethodPost, "/schedules/"+id.String()+"/rerun", newRerunBody("schema", "tbl", "svc"))
	req.SetPathValue("schedule_id", id.String())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRerunHandler_404_TaskNotFound(t *testing.T) {
	tracker := &model.SchedulerTracker{
		ScheduleID:   uuid.New(),
		ScheduleName: "sched",
		Status:       model.SchedulerStatusFailed,
	}
	schedRepo := &fakeSchedulerRepo{tracker: tracker}
	taskRepo := &fakeTaskRepo{err: postgres.ErrNotFound}
	h := buildHandler(t, schedRepo, taskRepo, &fakeOutboxRepo{})

	req := httptest.NewRequest(http.MethodPost, "/schedules/"+tracker.ScheduleID.String()+"/rerun", newRerunBody("schema", "tbl", "svc"))
	req.SetPathValue("schedule_id", tracker.ScheduleID.String())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRerunHandler_409_RunningTasksExist(t *testing.T) {
	tracker := &model.SchedulerTracker{
		ScheduleID:   uuid.New(),
		ScheduleName: "sched",
		Status:       model.SchedulerStatusRunning,
	}
	task := &model.TaskTracker{
		TaskID:     uuid.New(),
		ScheduleID: tracker.ScheduleID,
		Status:     model.TaskStatusFailed,
	}
	runningTask := &model.TaskTracker{Status: model.TaskStatusRunning}
	schedRepo := &fakeSchedulerRepo{tracker: tracker}
	taskRepo := &fakeTaskRepo{task: task, runningTasks: []*model.TaskTracker{runningTask}}
	h := buildHandler(t, schedRepo, taskRepo, &fakeOutboxRepo{})

	req := httptest.NewRequest(http.MethodPost, "/schedules/"+tracker.ScheduleID.String()+"/rerun", newRerunBody("schema", "tbl", "svc"))
	req.SetPathValue("schedule_id", tracker.ScheduleID.String())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestRerunHandler_422_TargetNotFailed(t *testing.T) {
	tracker := &model.SchedulerTracker{
		ScheduleID:   uuid.New(),
		ScheduleName: "sched",
		Status:       model.SchedulerStatusFailed,
	}
	task := &model.TaskTracker{
		TaskID:     uuid.New(),
		ScheduleID: tracker.ScheduleID,
		Status:     model.TaskStatusSucceeded,
	}
	schedRepo := &fakeSchedulerRepo{tracker: tracker}
	taskRepo := &fakeTaskRepo{task: task}
	h := buildHandler(t, schedRepo, taskRepo, &fakeOutboxRepo{})

	req := httptest.NewRequest(http.MethodPost, "/schedules/"+tracker.ScheduleID.String()+"/rerun", newRerunBody("schema", "tbl", "svc"))
	req.SetPathValue("schedule_id", tracker.ScheduleID.String())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

func TestRerunHandler_202_ResetsAndWritesOutbox(t *testing.T) {
	scheduleID := uuid.New()
	tracker := &model.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "sched",
		Status:               model.SchedulerStatusFailed,
		InitializationStatus: "completed",
		CreatedAt:            time.Now(),
	}
	task := &model.TaskTracker{
		TaskID:     uuid.New(),
		ScheduleID: scheduleID,
		Status:     model.TaskStatusFailed,
		RetryCount: 2,
	}
	schedRepo := &fakeSchedulerRepo{tracker: tracker}
	taskRepo := &fakeTaskRepo{task: task}
	outboxRepo := &fakeOutboxRepo{}

	h := buildHandler(t, schedRepo, taskRepo, outboxRepo)

	req := httptest.NewRequest(http.MethodPost, "/schedules/"+scheduleID.String()+"/rerun", newRerunBody("schema", "tbl", "svc"))
	req.SetPathValue("schedule_id", scheduleID.String())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)

	// Scheduler reset to RUNNING with init_status=pending
	require.NotNil(t, schedRepo.updated)
	assert.Equal(t, model.SchedulerStatusRunning, schedRepo.updated.Status)
	assert.Nil(t, schedRepo.updated.CompletedAt)

	// Task reset to PENDING with retry_count=0
	require.NotNil(t, taskRepo.updatedTask)
	assert.Equal(t, model.TaskStatusPending, taskRepo.updatedTask.Status)
	assert.Equal(t, 0, taskRepo.updatedTask.RetryCount)

	// Outbox entry written
	require.NotNil(t, outboxRepo.createdEntry)
	assert.Equal(t, "command.rerun:v1", outboxRepo.createdEntry.StreamName)
	assert.Equal(t, "rerun_node", outboxRepo.createdEntry.EventType)
}
