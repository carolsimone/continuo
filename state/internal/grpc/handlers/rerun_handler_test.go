package handlers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/model"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// ─── fixture ─────────────────────────────────────────────────────────────────

type rerunFixture struct {
	Handler       *RerunHandler
	SchedulerRepo postgres.SchedulerTrackerRepository
	TaskRepo      postgres.TaskTrackerRepository
	OutboxRepo    postgres.OutboxRepository
	DB            *sqlx.DB
	Cleanup       func()
}

func setupRerunFixture(t *testing.T) *rerunFixture {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	logger := newTestLogger()
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	handler := NewRerunHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)
	return &rerunFixture{
		Handler: handler, SchedulerRepo: schedulerRepo, TaskRepo: taskRepo,
		OutboxRepo: outboxRepo, DB: db, Cleanup: func() { db.Close() },
	}
}

// seedRerunSource creates a scheduler_tracker (with given srcStatus) and one task_tracker
// row in the given task status. Cleanup deletes both rows.
func seedRerunSource(t *testing.T, fx *rerunFixture, scheduleName string, srcStatus model.SchedulerStatus, taskStatus model.TaskStatus, service, schema, table string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	completedClause := "NOW()"
	if srcStatus == model.SchedulerStatusRunning || srcStatus == model.SchedulerStatusPending {
		completedClause = "NULL"
	}
	_, err := fx.DB.ExecContext(context.Background(),
		"INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, completed_at, initialization_status, kind) VALUES ($1, $2, $3, NOW(), "+completedClause+", 'completed', 'cron')",
		id, scheduleName, string(srcStatus),
	)
	require.NoError(t, err)

	_, err = fx.DB.ExecContext(context.Background(), `
		INSERT INTO task_tracker (task_id, schedule_id, created_at, service_name, schema_name, table_name, job_name, status, retry_count, max_retries, manifest_version, image_tag)
		VALUES ($1, $2, NOW(), $3, $4, $5, 'job', $6, 0, 2, 'v1', 'img:1')`,
		uuid.New(), id, service, schema, table, string(taskStatus),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), "DELETE FROM task_tracker WHERE schedule_id = $1", id)
		fx.DB.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", id)
	})
	return id
}

// seedRerunSourceNoTask creates a scheduler_tracker only — no task row.
// Used for the "task not found" rejection.
func seedRerunSourceNoTask(t *testing.T, fx *rerunFixture, scheduleName string, srcStatus model.SchedulerStatus) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := fx.DB.ExecContext(context.Background(), `
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, completed_at, initialization_status, kind)
		VALUES ($1, $2, $3, NOW(), NOW(), 'completed', 'cron')`,
		id, scheduleName, string(srcStatus),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", id)
	})
	return id
}

// ─── happy-path tests ────────────────────────────────────────────────────────

func TestRerunHandler_HappyPath_FailedSource(t *testing.T) {
	fx := setupRerunFixture(t)
	defer fx.Cleanup()

	scheduleName := "rerun-happy-" + uuid.New().String()[:8]
	srcID := seedRerunSource(t, fx, scheduleName,
		model.SchedulerStatusFailed, model.TaskStatusFailed,
		"svc", "s", "t",
	)

	resp, err := fx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  srcID.String(),
		ServiceName: "svc",
		Schema:      "s",
		TableName:   "t",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.RunId)
	require.Equal(t, scheduleName, resp.ScheduleName, "new run inherits source's schedule_name")

	newID := uuid.MustParse(resp.RunId)
	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), "DELETE FROM state_outbox WHERE aggregate_id = $1", newID)
		fx.DB.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", newID)
	})

	// New scheduler row.
	tracker, err := fx.SchedulerRepo.GetByID(context.Background(), newID)
	require.NoError(t, err)
	assert.Equal(t, "rerun", tracker.Kind)
	require.NotNil(t, tracker.SourceRunID)
	assert.Equal(t, srcID, *tracker.SourceRunID)
	assert.Equal(t, scheduleName, tracker.ScheduleName)
	assert.Equal(t, model.SchedulerStatusPending, tracker.Status)

	// Source row unchanged.
	src, err := fx.SchedulerRepo.GetByID(context.Background(), srcID)
	require.NoError(t, err)
	assert.Equal(t, model.SchedulerStatusFailed, src.Status, "source row must NOT be mutated")

	// Outbox row on trigger.rerun:v1.
	var payloadRaw []byte
	err = fx.DB.GetContext(context.Background(), &payloadRaw,
		`SELECT payload FROM state_outbox WHERE aggregate_id = $1 AND stream_name = 'trigger.rerun:v1' LIMIT 1`,
		newID,
	)
	require.NoError(t, err)
	var payload map[string]string
	require.NoError(t, json.Unmarshal(payloadRaw, &payload))
	assert.Equal(t, newID.String(), payload["schedule_id"])
	assert.Equal(t, scheduleName, payload["schedule_name"])
	assert.Equal(t, "rerun", payload["kind"])
	assert.Equal(t, srcID.String(), payload["source_run_id"])
	assert.Equal(t, "svc", payload["service_name"])
	assert.Equal(t, "s", payload["schema_name"])
	assert.Equal(t, "t", payload["table_name"])
}

func TestRerunHandler_HappyPath_CancelledSource(t *testing.T) {
	fx := setupRerunFixture(t)
	defer fx.Cleanup()

	scheduleName := "rerun-cancelled-" + uuid.New().String()[:8]
	srcID := seedRerunSource(t, fx, scheduleName,
		model.SchedulerStatusCancelled, model.TaskStatusCancelled,
		"svc", "s", "t",
	)

	resp, err := fx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  srcID.String(),
		ServiceName: "svc",
		Schema:      "s",
		TableName:   "t",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.RunId)
	require.Equal(t, scheduleName, resp.ScheduleName)

	newID := uuid.MustParse(resp.RunId)
	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), "DELETE FROM state_outbox WHERE aggregate_id = $1", newID)
		fx.DB.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", newID)
	})

	tracker, err := fx.SchedulerRepo.GetByID(context.Background(), newID)
	require.NoError(t, err)
	assert.Equal(t, "rerun", tracker.Kind)
	require.NotNil(t, tracker.SourceRunID)
	assert.Equal(t, srcID, *tracker.SourceRunID)
	assert.Equal(t, model.SchedulerStatusPending, tracker.Status)

	// Source row unchanged.
	src, err := fx.SchedulerRepo.GetByID(context.Background(), srcID)
	require.NoError(t, err)
	assert.Equal(t, model.SchedulerStatusCancelled, src.Status, "source row must NOT be mutated")
}

// ─── rejection tests ─────────────────────────────────────────────────────────

func TestRerunHandler_RejectsEmptyScheduleID(t *testing.T) {
	fx := setupRerunFixture(t)
	defer fx.Cleanup()

	_, err := fx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  "",
		ServiceName: "svc",
		Schema:      "s",
		TableName:   "t",
	})
	requireGRPCCode(t, err, codes.InvalidArgument)
}

func TestRerunHandler_RejectsMalformedScheduleID(t *testing.T) {
	fx := setupRerunFixture(t)
	defer fx.Cleanup()

	_, err := fx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  "not-a-uuid",
		ServiceName: "svc",
		Schema:      "s",
		TableName:   "t",
	})
	requireGRPCCode(t, err, codes.InvalidArgument)
}

func TestRerunHandler_RejectsEmptyTargetIdentity(t *testing.T) {
	fx := setupRerunFixture(t)
	defer fx.Cleanup()

	_, err := fx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  uuid.New().String(),
		ServiceName: "",
		Schema:      "s",
		TableName:   "t",
	})
	requireGRPCCode(t, err, codes.InvalidArgument)
}

func TestRerunHandler_RejectsSourceNotFound(t *testing.T) {
	fx := setupRerunFixture(t)
	defer fx.Cleanup()

	_, err := fx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  uuid.New().String(),
		ServiceName: "svc",
		Schema:      "s",
		TableName:   "t",
	})
	requireGRPCCode(t, err, codes.NotFound)
}

func TestRerunHandler_RejectsRunningSource(t *testing.T) {
	fx := setupRerunFixture(t)
	defer fx.Cleanup()

	srcID := seedRerunSource(t, fx, "rerun-rej-running-"+uuid.New().String()[:8],
		model.SchedulerStatusRunning, model.TaskStatusRunning,
		"svc", "s", "t",
	)
	_, err := fx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  srcID.String(),
		ServiceName: "svc",
		Schema:      "s",
		TableName:   "t",
	})
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestRerunHandler_RejectsSucceededSource(t *testing.T) {
	fx := setupRerunFixture(t)
	defer fx.Cleanup()

	srcID := seedRerunSource(t, fx, "rerun-rej-succ-"+uuid.New().String()[:8],
		model.SchedulerStatusSucceeded, model.TaskStatusSucceeded,
		"svc", "s", "t",
	)
	_, err := fx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  srcID.String(),
		ServiceName: "svc",
		Schema:      "s",
		TableName:   "t",
	})
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestRerunHandler_RejectsTaskNotFound(t *testing.T) {
	fx := setupRerunFixture(t)
	defer fx.Cleanup()

	srcID := seedRerunSourceNoTask(t, fx,
		"rerun-rej-no-task-"+uuid.New().String()[:8],
		model.SchedulerStatusFailed,
	)
	_, err := fx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  srcID.String(),
		ServiceName: "svc",
		Schema:      "s",
		TableName:   "t",
	})
	requireGRPCCode(t, err, codes.NotFound)
}

func TestRerunHandler_RejectsTaskSucceeded(t *testing.T) {
	fx := setupRerunFixture(t)
	defer fx.Cleanup()

	// Source FAILED, but the named target task is SUCCEEDED — eligibility forbids.
	srcID := seedRerunSource(t, fx, "rerun-rej-task-succ-"+uuid.New().String()[:8],
		model.SchedulerStatusFailed, model.TaskStatusSucceeded,
		"svc", "s", "t",
	)
	_, err := fx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  srcID.String(),
		ServiceName: "svc",
		Schema:      "s",
		TableName:   "t",
	})
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestRerunHandler_RejectsActiveRunOnSameSchedule(t *testing.T) {
	fx := setupRerunFixture(t)
	defer fx.Cleanup()

	scheduleName := "rerun-rej-concurrent-" + uuid.New().String()[:8]
	srcID := seedRerunSource(t, fx, scheduleName,
		model.SchedulerStatusFailed, model.TaskStatusFailed,
		"svc", "s", "t",
	)
	// Reuse the helper from rebase_handler_test.go (same package).
	_ = seedRunningSchedulerOnSchedule(t, &rebaseFixture{DB: fx.DB}, scheduleName)

	_, err := fx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		ScheduleId:  srcID.String(),
		ServiceName: "svc",
		Schema:      "s",
		TableName:   "t",
	})
	requireGRPCCode(t, err, codes.FailedPrecondition)
}
