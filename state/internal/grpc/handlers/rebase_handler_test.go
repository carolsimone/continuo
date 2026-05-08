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
	"github.com/stretchr/testify/require"
)

type rebaseFixture struct {
	Handler       *RebaseHandler
	SchedulerRepo postgres.SchedulerTrackerRepository
	TaskRepo      postgres.TaskTrackerRepository
	OutboxRepo    postgres.OutboxRepository
	DB            *sqlx.DB
	Cleanup       func()
}

func setupRebaseFixture(t *testing.T) *rebaseFixture {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	logger := newTestLogger()
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	handler := NewRebaseHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)
	return &rebaseFixture{
		Handler: handler, SchedulerRepo: schedulerRepo, TaskRepo: taskRepo,
		OutboxRepo: outboxRepo, DB: db, Cleanup: func() { db.Close() },
	}
}

// seedTerminalRunWithFailedTask creates a scheduler_tracker (status=failed/cancelled)
// with one task_tracker row in 'failed' status. Returns the schedule_id.
// Cleanup deletes both rows.
func seedTerminalRunWithFailedTask(t *testing.T, fx *rebaseFixture, scheduleName string, srcStatus model.SchedulerStatus) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := fx.DB.ExecContext(context.Background(), `
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, completed_at, initialization_status, kind)
		VALUES ($1, $2, $3, NOW(), NOW(), 'completed', 'cron')`,
		id, scheduleName, string(srcStatus),
	)
	require.NoError(t, err)

	_, err = fx.DB.ExecContext(context.Background(), `
		INSERT INTO task_tracker (task_id, schedule_id, created_at, service_name, schema_name, table_name, job_name, status, retry_count, max_retries, manifest_version, image_tag)
		VALUES ($1, $2, NOW(), 'svc', 's', 't', 'job', 'failed', 0, 2, 'v1', 'img:1')`,
		uuid.New(), id,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), "DELETE FROM task_tracker WHERE schedule_id = $1", id)
		fx.DB.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", id)
	})
	return id
}

func TestRebaseHandler_HappyPath_FailedSource(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	scheduleName := "rebase-happy-" + uuid.New().String()[:8]
	srcID := seedTerminalRunWithFailedTask(t, fx, scheduleName, model.SchedulerStatusFailed)

	resp, err := fx.Handler.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{
		SourceRunId: srcID.String(),
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.RunId)
	require.Equal(t, scheduleName, resp.ScheduleName, "new run inherits source's schedule_name")

	newID := uuid.MustParse(resp.RunId)
	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), "DELETE FROM state_outbox WHERE aggregate_id = $1", newID)
		fx.DB.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", newID)
	})

	tracker, err := fx.SchedulerRepo.GetByID(context.Background(), newID)
	require.NoError(t, err)
	require.Equal(t, "rebase", tracker.Kind)
	require.NotNil(t, tracker.SourceRunID)
	require.Equal(t, srcID, *tracker.SourceRunID)
	require.Equal(t, scheduleName, tracker.ScheduleName)
	require.Equal(t, model.SchedulerStatusPending, tracker.Status)

	// Outbox row on trigger.rebase:v1 with the right payload.
	var payloadRaw []byte
	err = fx.DB.GetContext(context.Background(), &payloadRaw,
		`SELECT payload FROM state_outbox WHERE aggregate_id = $1 AND stream_name = 'trigger.rebase:v1' LIMIT 1`,
		newID,
	)
	require.NoError(t, err)
	var payload map[string]string
	require.NoError(t, json.Unmarshal(payloadRaw, &payload))
	require.Equal(t, newID.String(), payload["schedule_id"])
	require.Equal(t, scheduleName, payload["schedule_name"])
	require.Equal(t, "rebase", payload["kind"])
	require.Equal(t, srcID.String(), payload["source_run_id"])
}

func TestRebaseHandler_HappyPath_CancelledSource(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	scheduleName := "rebase-cancelled-" + uuid.New().String()[:8]
	srcID := seedTerminalRunWithFailedTask(t, fx, scheduleName, model.SchedulerStatusCancelled)

	resp, err := fx.Handler.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{
		SourceRunId: srcID.String(),
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.RunId)

	newID := uuid.MustParse(resp.RunId)
	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), "DELETE FROM state_outbox WHERE aggregate_id = $1", newID)
		fx.DB.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", newID)
	})

	tracker, err := fx.SchedulerRepo.GetByID(context.Background(), newID)
	require.NoError(t, err)
	require.Equal(t, "rebase", tracker.Kind)
	require.Equal(t, srcID, *tracker.SourceRunID)
}
