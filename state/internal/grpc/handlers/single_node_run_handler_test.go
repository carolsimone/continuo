package handlers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/model"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ─── fixture ─────────────────────────────────────────────────────────────────

type singleNodeRunFixture struct {
	Handler       *SingleNodeRunHandler
	SchedulerRepo postgres.SchedulerTrackerRepository
	TaskRepo      postgres.TaskTrackerRepository
	OutboxRepo    postgres.OutboxRepository
	DB            *sqlx.DB
	Cleanup       func()
}

// setupSingleNodeRunFixture builds a postgres-backed SingleNodeRunHandler
// using the same DB/repo construction as buildRerunHandlerFullDB.
func setupSingleNodeRunFixture(t *testing.T) *singleNodeRunFixture {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}

	logger := newTestLogger()
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	handler := NewSingleNodeRunHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)

	cleanup := func() { db.Close() }
	return &singleNodeRunFixture{
		Handler:       handler,
		SchedulerRepo: schedulerRepo,
		TaskRepo:      taskRepo,
		OutboxRepo:    outboxRepo,
		DB:            db,
		Cleanup:       cleanup,
	}
}

// ─── tests ───────────────────────────────────────────────────────────────────

func TestSingleNodeRunHandler_Latest_HappyPath(t *testing.T) {
	fx := setupSingleNodeRunFixture(t)
	defer fx.Cleanup()

	resp, err := fx.Handler.TriggerSingleNodeRun(context.Background(), &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    "svcA",
		SchemaName:     "public",
		TableName:      "users",
		MetadataSource: "latest",
		SourceRunId:    "",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.RunId)
	require.True(t, strings.HasPrefix(resp.ScheduleName, "single-node-run-"), "schedule_name should start with 'single-node-run-'")
	require.Len(t, resp.ScheduleName, len("single-node-run-")+8, "schedule_name suffix should be 8 hex chars")

	// Scheduler row exists with kind = "single_node_run", source_run_id NULL.
	scheduleID := uuid.MustParse(resp.RunId)
	tracker, err := fx.SchedulerRepo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	require.Equal(t, "single_node_run", tracker.Kind)
	require.Nil(t, tracker.SourceRunID)
	require.Equal(t, resp.ScheduleName, tracker.ScheduleName)

	// Outbox row exists with stream "trigger.single_node_run:v1" and
	// payload carries metadata_source = "latest".
	entries, err := fx.OutboxRepo.ListPending(context.Background(), 10)
	require.NoError(t, err)

	// Find the outbox entry for this specific schedule (may be mixed with others on shared DB).
	var found *postgres.OutboxEntry
	for i := range entries {
		if entries[i].AggregateID == scheduleID {
			found = entries[i]
			break
		}
	}
	require.NotNil(t, found, "expected outbox entry for schedule %s", scheduleID)
	require.Equal(t, "trigger.single_node_run:v1", found.StreamName)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(found.Payload, &payload))
	require.Equal(t, "latest", payload["metadata_source"])
	require.Equal(t, "single_node_run", payload["kind"])
	require.Equal(t, "svcA", payload["service_name"])
	require.Equal(t, "public", payload["schema_name"])
	require.Equal(t, "users", payload["table_name"])
	require.Equal(t, "", payload["source_run_id"])

	// Cleanup seeded rows.
	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, scheduleID)
		fx.DB.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
	})
}

func TestSingleNodeRunHandler_Stale_HappyPath(t *testing.T) {
	fx := setupSingleNodeRunFixture(t)
	defer fx.Cleanup()

	// Seed a terminal source run with a matching task.
	srcID := uuid.New()
	srcTracker := &model.SchedulerTracker{
		ScheduleID:           srcID,
		ScheduleName:         "src-schedule",
		Status:               model.SchedulerStatusSucceeded,
		CreatedAt:            time.Now().Add(-time.Hour),
		Kind:                 "cron",
		InitializationStatus: "completed",
	}
	require.NoError(t, fx.SchedulerRepo.Create(context.Background(), srcTracker))

	taskID := uuid.New()
	srcTask := &model.TaskTracker{
		TaskID:          taskID,
		ScheduleID:      srcID,
		ServiceName:     "svcA",
		SchemaName:      "public",
		TableName:       "users",
		Status:          model.TaskStatusSucceeded,
		ManifestVersion: "m1",
		ImageTag:        "v1",
		CreatedAt:       time.Now().Add(-time.Hour),
	}
	require.NoError(t, fx.TaskRepo.Create(context.Background(), srcTask))

	resp, err := fx.Handler.TriggerSingleNodeRun(context.Background(), &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    "svcA",
		SchemaName:     "public",
		TableName:      "users",
		MetadataSource: "snapshot_of_run",
		SourceRunId:    srcID.String(),
	})
	require.NoError(t, err)

	scheduleID := uuid.MustParse(resp.RunId)
	tracker, err := fx.SchedulerRepo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	require.Equal(t, "single_node_run", tracker.Kind)
	require.NotNil(t, tracker.SourceRunID)
	require.Equal(t, srcID, *tracker.SourceRunID)

	entries, err := fx.OutboxRepo.ListPending(context.Background(), 10)
	require.NoError(t, err)
	// Stale-mode entry is the most recent.
	var stale *postgres.OutboxEntry
	for i := range entries {
		if entries[i].AggregateID == scheduleID {
			stale = entries[i]
			break
		}
	}
	require.NotNil(t, stale)
	require.Equal(t, "trigger.single_node_run:v1", stale.StreamName)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(stale.Payload, &payload))
	require.Equal(t, "snapshot_of_run", payload["metadata_source"])
	require.Equal(t, srcID.String(), payload["source_run_id"])

	// Cleanup seeded rows.
	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, scheduleID)
		fx.DB.ExecContext(context.Background(), `DELETE FROM task_tracker WHERE task_id = $1`, taskID)
		fx.DB.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
		fx.DB.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, srcID)
	})
}

func TestSingleNodeRunHandler_Errors(t *testing.T) {
	fx := setupSingleNodeRunFixture(t)
	defer fx.Cleanup()

	// Seed a non-terminal source run for FAILED_PRECONDITION coverage.
	runningID := uuid.New()
	require.NoError(t, fx.SchedulerRepo.Create(context.Background(), &model.SchedulerTracker{
		ScheduleID:           runningID,
		ScheduleName:         "running-src",
		Status:               model.SchedulerStatusRunning,
		CreatedAt:            time.Now(),
		Kind:                 "cron",
		InitializationStatus: "completed",
	}))

	// Seed a terminal source run with NO matching task for NOT_FOUND coverage.
	terminalNoTaskID := uuid.New()
	require.NoError(t, fx.SchedulerRepo.Create(context.Background(), &model.SchedulerTracker{
		ScheduleID:           terminalNoTaskID,
		ScheduleName:         "terminal-no-task",
		Status:               model.SchedulerStatusSucceeded,
		CreatedAt:            time.Now(),
		Kind:                 "cron",
		InitializationStatus: "completed",
	}))

	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, runningID)
		fx.DB.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, terminalNoTaskID)
	})

	cases := []struct {
		name   string
		req    *statev1.TriggerSingleNodeRunRequest
		expect codes.Code
	}{
		{"empty service", &statev1.TriggerSingleNodeRunRequest{ServiceName: "", SchemaName: "s", TableName: "t", MetadataSource: "latest"}, codes.InvalidArgument},
		{"empty schema", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "", TableName: "t", MetadataSource: "latest"}, codes.InvalidArgument},
		{"empty table", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "", MetadataSource: "latest"}, codes.InvalidArgument},
		{"unknown source", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "bogus"}, codes.InvalidArgument},
		{"latest with src", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "latest", SourceRunId: uuid.NewString()}, codes.InvalidArgument},
		{"stale no src", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "snapshot_of_run", SourceRunId: ""}, codes.InvalidArgument},
		{"stale bad uuid", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "snapshot_of_run", SourceRunId: "not-a-uuid"}, codes.InvalidArgument},
		{"stale src missing", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "snapshot_of_run", SourceRunId: uuid.NewString()}, codes.NotFound},
		{"stale src not terminal", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "snapshot_of_run", SourceRunId: runningID.String()}, codes.FailedPrecondition},
		{"stale src no matching task", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "snapshot_of_run", SourceRunId: terminalNoTaskID.String()}, codes.NotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := fx.Handler.TriggerSingleNodeRun(context.Background(), tc.req)
			require.Error(t, err)
			require.Equal(t, tc.expect, status.Code(err), "wrong code for %s: %v", tc.name, err)
		})
	}
}
