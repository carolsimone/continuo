package handlers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/runstart"

	"github.com/carolsimone/continuo/pkg/testdeps"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/adapters/postgres"
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
	// Tenant is the tenant the fixture plans for; it holds release rel-svca
	// as its live release, with svcA's public.users (one data test) and
	// public.users_snap (a dbt snapshot).
	Tenant string
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
	db, err := pkgdb.Open(context.Background(), pkgconfig.LoadPostgres(&pkgconfig.Validator{}), pkgconfig.PoolConfig{MaxOpenConns: 10, MaxIdleConns: 2})
	if err != nil {
		testdeps.Unavailable(t, "no test DB available: %v", err)
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
	tenant := fixtureTenant(t, db)
	promoteUsers(t, db, tenant)
	useCase := svchandlers.NewTriggerSingleNodeRunHandler(runstart.Planner{TenantID: tenant}, logger)
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
		Tenant:        tenant,
	}
}

// promoteUsers stores release rel-svca of tenant, holding svcA's public.users
// with one data test and public.users_snap, a dbt snapshot, and makes it the
// tenant's live release.
func promoteUsers(t *testing.T, db *sqlx.DB, tenant string) {
	t.Helper()
	ctx := context.Background()
	one := 1
	rel, err := topology.NewRelease(tenant, "rel-svca", 1, "s3://continuo/k", "sha-svca", []topology.NodeSpec{
		{UniqueID: "model.svcA.users", ServiceName: "svcA", SchemaName: "public", TableName: "users", NodeType: "dbt-model",
			TestCount: &one, ContentHash: "h-users", ImageTag: "svcA:3", ScheduleName: "daily"},
		{UniqueID: "snapshot.svcA.users_snap", ServiceName: "svcA", SchemaName: "public", TableName: "users_snap", NodeType: "dbt-snapshot",
			ContentHash: "h-snap", ImageTag: "svcA:3", ScheduleName: "daily"},
	})
	require.NoError(t, err)
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck
	topo := postgres.NewTopologyRepository(db, tx)
	_, err = topo.SaveRelease(ctx, rel)
	require.NoError(t, err)
	require.NoError(t, topo.SetLivePointer(ctx, tenant, topology.Pointer{ReleaseID: "rel-svca", PromotionSeq: 1}, time.Now()))
	require.NoError(t, tx.Commit())
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

	require.Equal(t, run.SchedulerStatusRunning, tracker.Status)

	// The run's one task is dispatched on the live release: the outbox row is
	// its query.model:v1 dispatch, read directly by aggregate_id rather than
	// ListPending(10), because the shared test DB can accumulate stale pending
	// rows from prior runs that overflow the small limit.
	found := getOutboxByAggregate(t, fx.DB, scheduleID)
	require.NotNil(t, found, "expected outbox entry for schedule %s", scheduleID)
	require.Equal(t, streams.QueryModelV1, found.StreamName)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(found.Payload, &payload))
	require.Equal(t, run.TaskIDFor(fx.Tenant, scheduleID, "public.users").String(), payload["task_id"])
	require.Equal(t, "svcA", payload["service_name"])
	require.Equal(t, "public", payload["schema_name"])
	require.Equal(t, "users", payload["table_name"])
	require.Equal(t, "svcA:3", payload["image_tag"])
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

func TestSingleNodeRunHandler_Operation_Build(t *testing.T) {
	fx := setupSingleNodeRunFixture(t)

	resp, err := fx.Handler.TriggerSingleNodeRun(context.Background(), &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    "svcA",
		SchemaName:     "public",
		TableName:      "users",
		MetadataSource: "latest",
		Operation:      "build",
	})
	require.NoError(t, err)

	scheduleID := uuid.MustParse(resp.RunId)
	found := getOutboxByAggregate(t, fx.DB, scheduleID)
	require.NotNil(t, found, "expected outbox entry for schedule %s", scheduleID)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(found.Payload, &payload))
	require.Equal(t, string(model.OperationBuild), payload["operation"])

	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, scheduleID)
		fx.DB.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
	})
}

func TestSingleNodeRunHandler_Operation_FullRefresh(t *testing.T) {
	fx := setupSingleNodeRunFixture(t)

	resp, err := fx.Handler.TriggerSingleNodeRun(context.Background(), &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    "svcA",
		SchemaName:     "public",
		TableName:      "users",
		MetadataSource: "latest",
		Operation:      "full_refresh",
	})
	require.NoError(t, err)

	scheduleID := uuid.MustParse(resp.RunId)
	found := getOutboxByAggregate(t, fx.DB, scheduleID)
	require.NotNil(t, found, "expected outbox entry for schedule %s", scheduleID)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(found.Payload, &payload))
	require.Equal(t, string(model.OperationFullRefresh), payload["operation"])

	var stored string
	require.NoError(t, fx.DB.QueryRowContext(context.Background(),
		`SELECT operation FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID).Scan(&stored))
	require.Equal(t, "full_refresh", stored)

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
	_, err := fx.DB.ExecContext(context.Background(), `UPDATE scheduler_tracker SET release_id = 'rel-svca', promotion_seq = 1 WHERE schedule_id = $1`, srcID)
	require.NoError(t, err)

	taskID := uuid.New()
	srcTask := &postgres.TaskTracker{
		TaskID:      taskID,
		ScheduleID:  srcID,
		ServiceName: "svcA",
		SchemaName:  "public",
		TableName:   "users",
		Status:      run.TaskStatusSucceeded,
		ImageTag:    "v1",
		CreatedAt:   time.Now().Add(-time.Hour),
	}
	require.NoError(t, fx.TaskRepo.Create(context.Background(), srcTask))
	_, err = fx.DB.ExecContext(context.Background(), `UPDATE task_tracker SET node_key = 'public.users' WHERE task_id = $1`, taskID)
	require.NoError(t, err)

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
	require.NotNil(t, tracker.ReleaseID)
	require.Equal(t, "rel-svca", *tracker.ReleaseID, "a run of a node as a run built it runs on that run's release")
	stale := getOutboxByAggregate(t, fx.DB, scheduleID)
	require.NotNil(t, stale, "expected outbox entry for schedule %s", scheduleID)
	require.Equal(t, streams.QueryModelV1, stale.StreamName)

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

	// Seed a terminal source run that names no release, with a task at
	// svcA's public.users, for the FAILED_PRECONDITION of an unreadable source.
	noReleaseID := uuid.New()
	require.NoError(t, fx.SchedulerRepo.Create(context.Background(), &postgres.SchedulerTracker{
		ScheduleID:           noReleaseID,
		ScheduleName:         "no-release",
		Status:               run.SchedulerStatusSucceeded,
		CreatedAt:            time.Now(),
		Kind:                 "cron",
		InitializationStatus: "completed",
	}))
	require.NoError(t, fx.TaskRepo.Create(context.Background(), &postgres.TaskTracker{
		TaskID: uuid.New(), ScheduleID: noReleaseID, ServiceName: "svcA", SchemaName: "public", TableName: "users",
		Status: run.TaskStatusSucceeded, CreatedAt: time.Now(),
	}))

	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, runningID)
		fx.DB.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, terminalNoTaskID)
		fx.DB.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, noReleaseID)
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
		{"latest with src", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "latest", SourceRunId: uuid.NewString()}, codes.InvalidArgument},
		{"stale no src", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "snapshot_of_run", SourceRunId: ""}, codes.InvalidArgument},
		{"stale bad uuid", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "snapshot_of_run", SourceRunId: "not-a-uuid"}, codes.InvalidArgument},
		{"stale src missing", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "snapshot_of_run", SourceRunId: uuid.NewString()}, codes.NotFound},
		{"stale src not terminal", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "snapshot_of_run", SourceRunId: runningID.String()}, codes.FailedPrecondition},
		{"stale src no matching task", &statev1.TriggerSingleNodeRunRequest{ServiceName: "x", SchemaName: "s", TableName: "t", MetadataSource: "snapshot_of_run", SourceRunId: terminalNoTaskID.String()}, codes.NotFound},
		{"node not in the live release", &statev1.TriggerSingleNodeRunRequest{ServiceName: "svcA", SchemaName: "public", TableName: "absent", MetadataSource: "latest"}, codes.NotFound},
		{"node of another service", &statev1.TriggerSingleNodeRunRequest{ServiceName: "svcB", SchemaName: "public", TableName: "users", MetadataSource: "latest"}, codes.NotFound},
		{"test of a node without data tests", &statev1.TriggerSingleNodeRunRequest{ServiceName: "svcA", SchemaName: "public", TableName: "users_snap", MetadataSource: "latest", Operation: "test"}, codes.FailedPrecondition},
		{"full refresh of a snapshot", &statev1.TriggerSingleNodeRunRequest{ServiceName: "svcA", SchemaName: "public", TableName: "users_snap", MetadataSource: "latest", Operation: "full_refresh"}, codes.InvalidArgument},
		{"stale src without a release", &statev1.TriggerSingleNodeRunRequest{ServiceName: "svcA", SchemaName: "public", TableName: "users", MetadataSource: "snapshot_of_run", SourceRunId: noReleaseID.String()}, codes.FailedPrecondition},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var before int
			require.NoError(t, fx.DB.Get(&before, `SELECT count(*) FROM scheduler_tracker WHERE kind = 'single_node_run'`))
			_, err := fx.Handler.TriggerSingleNodeRun(context.Background(), tc.req)
			require.Error(t, err)
			require.Equal(t, tc.expect, status.Code(err), "wrong code for %s: %v", tc.name, err)
			var after int
			require.NoError(t, fx.DB.Get(&after, `SELECT count(*) FROM scheduler_tracker WHERE kind = 'single_node_run'`))
			require.Equal(t, before, after, "a refused trigger creates no run")
		})
	}
}
