package handlers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
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
