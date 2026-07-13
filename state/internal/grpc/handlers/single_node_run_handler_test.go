package handlers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
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
	DB            *sqlx.DB
}

// outboxRow holds the fields read back from state_outbox in tests.
type outboxRow struct {
	StreamName string `db:"stream_name"`
	Payload    []byte `db:"payload"`
}

// getOutboxByAggregate reads the state_outbox row for the given aggregate_id.
// Used instead of a ListPending call so that accumulated unrelated pending rows
// on the shared test DB don't cause limit-overflow flakes.
func getOutboxByAggregate(t *testing.T, db *sqlx.DB, aggregateID uuid.UUID) *outboxRow {
	t.Helper()
	var entry outboxRow
	err := db.GetContext(context.Background(), &entry,
		`SELECT stream_name, payload FROM state_outbox WHERE aggregate_id = $1 LIMIT 1`,
		aggregateID,
	)
	if err != nil {
		return nil
	}
	return &entry
}

// setupSingleNodeRunFixture builds a postgres-backed SingleNodeRunHandler
// wired with the UoW factory pattern used by the live server.
func setupSingleNodeRunFixture(t *testing.T) *singleNodeRunFixture {
	t.Helper()
	db, err := database.NewConnection(pkgconfig.LoadPostgres(&pkgconfig.Validator{}))
	if err != nil {
		t.Skip("no test DB available:", err)
	}

	logger := newTestLogger()
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	catalogRepo := postgres.NewScheduleCatalogRepository(db, logger)
	taskExecutionRepo := postgres.NewTaskExecutionRepository(db, logger)
	clk := ports.SystemClock{}
	factory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, schedulerRepo, taskRepo, taskExecutionRepo, catalogRepo, clk, logger)
	}
	useCase := svchandlers.NewTriggerSingleNodeRunHandler(logger)
	handler := NewSingleNodeRunHandler(useCase, factory, logger)

	// Register the DB close via t.Cleanup (not a bare defer in the calling
	// test) so it runs after this fixture's own t.Cleanup callbacks. A test
	// function's defers run as the function returns, before any t.Cleanup
	// fires; a bare `defer fx.Cleanup()` at the call site would therefore
	// close the DB before the per-test row-deletion cleanups below ran
	// against it, silently leaking seeded rows across test runs.
	t.Cleanup(func() { db.Close() })

	return &singleNodeRunFixture{
		Handler:       handler,
		SchedulerRepo: schedulerRepo,
		TaskRepo:      taskRepo,
		DB:            db,
	}
}

// ─── tests ───────────────────────────────────────────────────────────────────

func TestSingleNodeRunHandler_Latest_HappyPath(t *testing.T) {
	fx := setupSingleNodeRunFixture(t)

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
	// Query the outbox row directly by aggregate_id rather than ListPending(10),
	// because the shared test DB can accumulate stale pending rows from prior
	// runs that overflow the small limit and mask the row we're asserting on.
	found := getOutboxByAggregate(t, fx.DB, scheduleID)
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
	require.Equal(t, "", payload["operation"], "absent operation defaults to run (empty)")

	// Cleanup seeded rows.
	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, scheduleID)
		fx.DB.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
	})
}

func TestSingleNodeRunHandler_Operation_HappyPath(t *testing.T) {
	fx := setupSingleNodeRunFixture(t)

	resp, err := fx.Handler.TriggerSingleNodeRun(context.Background(), &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    "svcA",
		SchemaName:     "public",
		TableName:      "users",
		MetadataSource: "latest",
		Operation:      "test",
	})
	require.NoError(t, err)

	scheduleID := uuid.MustParse(resp.RunId)
	found := getOutboxByAggregate(t, fx.DB, scheduleID)
	require.NotNil(t, found, "expected outbox entry for schedule %s", scheduleID)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(found.Payload, &payload))
	require.Equal(t, "test", payload["operation"])

	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, scheduleID)
		fx.DB.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
	})
}

func TestSingleNodeRunHandler_Stale_HappyPath(t *testing.T) {
	fx := setupSingleNodeRunFixture(t)

	// Seed a terminal source run with a matching task.
	srcID := uuid.New()
	srcTracker := &postgres.SchedulerTracker{
		ScheduleID:           srcID,
		ScheduleName:         "src-schedule",
		Status:               run.SchedulerStatusSucceeded,
		CreatedAt:            time.Now().Add(-time.Hour),
		Kind:                 "cron",
		InitializationStatus: "completed",
	}
	require.NoError(t, fx.SchedulerRepo.Create(context.Background(), srcTracker))

	taskID := uuid.New()
	srcTask := &postgres.TaskTracker{
		TaskID:          taskID,
		ScheduleID:      srcID,
		ServiceName:     "svcA",
		SchemaName:      "public",
		TableName:       "users",
		Status:          run.TaskStatusSucceeded,
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

	// Query directly by aggregate_id (see getOutboxByAggregate rationale).
	stale := getOutboxByAggregate(t, fx.DB, scheduleID)
	require.NotNil(t, stale, "expected outbox entry for schedule %s", scheduleID)
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

	// Seed a non-terminal source run for FAILED_PRECONDITION coverage.
	runningID := uuid.New()
	require.NoError(t, fx.SchedulerRepo.Create(context.Background(), &postgres.SchedulerTracker{
		ScheduleID:           runningID,
		ScheduleName:         "running-src",
		Status:               run.SchedulerStatusRunning,
		CreatedAt:            time.Now(),
		Kind:                 "cron",
		InitializationStatus: "completed",
	}))

	// Seed a terminal source run with NO matching task for NOT_FOUND coverage.
	terminalNoTaskID := uuid.New()
	require.NoError(t, fx.SchedulerRepo.Create(context.Background(), &postgres.SchedulerTracker{
		ScheduleID:           terminalNoTaskID,
		ScheduleName:         "terminal-no-task",
		Status:               run.SchedulerStatusSucceeded,
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
		{"invalid operation", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "latest", Operation: "bogus"}, codes.InvalidArgument},
		{"build operation not supported", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "latest", Operation: "build"}, codes.InvalidArgument},
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
