package handlers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/testdeps"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rebaseFixture struct {
	Handler       *RebaseHandler
	SchedulerRepo postgres.SchedulerTrackerRepository
	TaskRepo      postgres.TaskTrackerRepository
	DB            *sqlx.DB
	// Tenant is the tenant the fixture's planner plans for; its releases and
	// live pointer are deleted when the test ends.
	Tenant  string
	Cleanup func()
}

// fixtureTenant returns a tenant id no other test uses and deletes its
// topology when the test ends.
func fixtureTenant(t *testing.T, db *sqlx.DB) string {
	t.Helper()
	tenant := "grpc-" + uuid.NewString()
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM live_release WHERE tenant_id = $1`, tenant)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM topology_releases WHERE tenant_id = $1`, tenant)
	})
	return tenant
}

func setupRebaseFixture(t *testing.T) *rebaseFixture {
	t.Helper()
	db, err := pkgdb.Open(context.Background(), pkgconfig.LoadPostgres(&pkgconfig.Validator{}), pkgconfig.PoolConfig{MaxOpenConns: 10, MaxIdleConns: 2})
	if err != nil {
		testdeps.Unavailable(t, "no test DB available: %v", err)
	}
	logger := newTestLogger()
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, logger)
	catalogRepo := postgres.NewScheduleCatalogRepository(db, logger)
	clk := ports.SystemClock{}
	factory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, schedulerRepo, taskRepo, catalogRepo, clk, logger)
	}
	tenant := fixtureTenant(t, db)
	useCase := svchandlers.NewTriggerRebaseHandler(runstart.Planner{TenantID: tenant, Tasks: run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}}, logger)
	handler := NewRebaseHandler(useCase, factory, logger)
	return &rebaseFixture{
		Handler: handler, SchedulerRepo: schedulerRepo, TaskRepo: taskRepo,
		DB: db, Tenant: tenant, Cleanup: func() { db.Close() },
	}
}

// promoteNode stores a release of fx's tenant holding node s.t of service svc
// in scheduleName and makes it the tenant's live release. It returns the
// release id.
func promoteNode(t *testing.T, fx *rebaseFixture, scheduleName string) string {
	t.Helper()
	ctx := context.Background()
	releaseID := "rel-" + uuid.NewString()[:8]
	rel, err := topology.NewRelease(fx.Tenant, releaseID, 1, "s3://continuo/k", "sha-"+releaseID, []topology.NodeSpec{{
		UniqueID: "model.svc.t", ServiceName: "svc", SchemaName: "s", TableName: "t", NodeType: "dbt-model",
		ContentHash: "h-t", ImageTag: "img:2", ScheduleName: scheduleName,
	}})
	require.NoError(t, err)
	tx, err := fx.DB.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck
	topo := postgres.NewTopologyRepository(fx.DB, tx)
	_, err = topo.SaveRelease(ctx, rel)
	require.NoError(t, err)
	require.NoError(t, topo.SetLivePointer(ctx, fx.Tenant, topology.Pointer{ReleaseID: releaseID, PromotionSeq: 1}, time.Now()))
	require.NoError(t, tx.Commit())
	return releaseID
}

// seedTerminalRunWithFailedTask creates a scheduler_tracker (status=failed/cancelled)
// on a release of fx's tenant, the live one, with one task_tracker row in
// 'failed' status at node s.t. Returns the schedule_id. Cleanup deletes both
// rows.
func seedTerminalRunWithFailedTask(t *testing.T, fx *rebaseFixture, scheduleName string, srcStatus run.SchedulerStatus) uuid.UUID {
	t.Helper()
	releaseID := promoteNode(t, fx, scheduleName)
	id := uuid.New()
	_, err := fx.DB.ExecContext(context.Background(), `
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, completed_at, initialization_status, kind, release_id, promotion_seq)
		VALUES ($1, $2, $3, NOW(), NOW(), 'completed', 'cron', $4, 1)`,
		id, scheduleName, string(srcStatus), releaseID,
	)
	require.NoError(t, err)

	_, err = fx.DB.ExecContext(context.Background(), `
		INSERT INTO task_tracker (task_id, schedule_id, created_at, service_name, schema_name, table_name, job_name, status, attempt, max_retries, image_tag, node_key)
		VALUES ($1, $2, NOW(), 'svc', 's', 't', 'job', 'failed', 0, 2, 'img:1', 's.t')`,
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
	srcID := seedTerminalRunWithFailedTask(t, fx, scheduleName, run.SchedulerStatusFailed)

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
	require.Equal(t, run.SchedulerStatusRunning, tracker.Status)

	// The failed task is dispatched again on the live release.
	requireDispatched(t, fx.DB, newID, run.TaskIDFor(fx.Tenant, newID, "s.t"), scheduleName)
}

// requireDispatched asserts run runID wrote one task.dispatch:v1 dispatch, of
// attempt 0 of taskID.
func requireDispatched(t *testing.T, db *sqlx.DB, runID, taskID uuid.UUID, scheduleName string) {
	t.Helper()
	var payloads [][]byte
	require.NoError(t, db.SelectContext(context.Background(), &payloads,
		`SELECT payload FROM state_outbox WHERE aggregate_id = $1 AND stream_name = $2`, runID, streams.TaskDispatchV1))
	require.Len(t, payloads, 1)
	var payload pkgevents.TaskDispatch
	require.NoError(t, json.Unmarshal(payloads[0], &payload))
	require.Equal(t, runID.String(), payload.RunID)
	require.Equal(t, scheduleName, payload.ScheduleName)
	require.Equal(t, taskID.String(), payload.TaskID)
	require.Equal(t, int32(0), payload.Attempt)
	require.Equal(t, model.AttemptStatusDispatched, payload.KnownStatus)
}

func TestRebaseHandler_HappyPath_CancelledSource(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	scheduleName := "rebase-cancelled-" + uuid.New().String()[:8]
	srcID := seedTerminalRunWithFailedTask(t, fx, scheduleName, run.SchedulerStatusCancelled)

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

// seedRunWithTaskStatus creates a scheduler_tracker with the given status and a single
// task in the given status. Used for rejection-case fixtures.
func seedRunWithTaskStatus(t *testing.T, fx *rebaseFixture, scheduleName string, srcStatus run.SchedulerStatus, taskStatus run.TaskStatus) uuid.UUID {
	t.Helper()
	id := uuid.New()
	completedClause := "NOW()"
	if srcStatus == run.SchedulerStatusRunning || srcStatus == run.SchedulerStatusPending {
		completedClause = "NULL"
	}
	_, err := fx.DB.ExecContext(context.Background(),
		"INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, completed_at, initialization_status, kind) VALUES ($1, $2, $3, NOW(), "+completedClause+", 'completed', 'cron')",
		id, scheduleName, string(srcStatus),
	)
	require.NoError(t, err)

	_, err = fx.DB.ExecContext(context.Background(), `
		INSERT INTO task_tracker (task_id, schedule_id, created_at, service_name, schema_name, table_name, job_name, status, attempt, max_retries, image_tag)
		VALUES ($1, $2, NOW(), 'svc', 's', 't', 'job', $3, 0, 2, 'img:1')`,
		uuid.New(), id, string(taskStatus),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), "DELETE FROM task_tracker WHERE schedule_id = $1", id)
		fx.DB.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", id)
	})
	return id
}

// seedRunningSchedulerOnSchedule creates an additional RUNNING scheduler_tracker
// row on the given schedule_name (no tasks). Used to test the active-schedule guard.
func seedRunningSchedulerOnSchedule(t *testing.T, fx *rebaseFixture, scheduleName string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := fx.DB.ExecContext(context.Background(), `
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, initialization_status, kind)
		VALUES ($1, $2, 'running', NOW(), 'in_progress', 'cron')`,
		id, scheduleName,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", id)
	})
	return id
}

func TestRebaseHandler_RejectsEmptySourceRunID(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	_, err := fx.Handler.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{SourceRunId: ""})
	requireGRPCCode(t, err, codes.InvalidArgument)
}

func TestRebaseHandler_RejectsMalformedSourceRunID(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	_, err := fx.Handler.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{SourceRunId: "not-a-uuid"})
	requireGRPCCode(t, err, codes.InvalidArgument)
}

func TestRebaseHandler_RejectsSourceNotFound(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	_, err := fx.Handler.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{SourceRunId: uuid.New().String()})
	requireGRPCCode(t, err, codes.NotFound)
}

func TestRebaseHandler_RejectsRunningSource(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	srcID := seedRunWithTaskStatus(t, fx, "rebase-rej-running-"+uuid.New().String()[:8], run.SchedulerStatusRunning, run.TaskStatusRunning)
	_, err := fx.Handler.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{SourceRunId: srcID.String()})
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestRebaseHandler_RejectsSucceededSource(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	srcID := seedRunWithTaskStatus(t, fx, "rebase-rej-succ-"+uuid.New().String()[:8], run.SchedulerStatusSucceeded, run.TaskStatusSucceeded)
	_, err := fx.Handler.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{SourceRunId: srcID.String()})
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestRebaseHandler_RejectsFailedSourceWithAllSucceededTasks(t *testing.T) {
	// Edge case: source row has status=failed but no failed tasks (data-corruption /
	// race scenario). Eligibility requires >=1 non-SUCCEEDED task — must reject.
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	srcID := seedRunWithTaskStatus(t, fx, "rebase-rej-failed-allsucc-"+uuid.New().String()[:8], run.SchedulerStatusFailed, run.TaskStatusSucceeded)
	_, err := fx.Handler.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{SourceRunId: srcID.String()})
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

// A source run that names no release cannot be rebased.
func TestRebaseHandler_RejectsASourceWithoutARelease(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	srcID := seedRunWithTaskStatus(t, fx, "rebase-rej-norelease-"+uuid.New().String()[:8], run.SchedulerStatusFailed, run.TaskStatusFailed)
	_, err := fx.Handler.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{SourceRunId: srcID.String()})
	requireGRPCCode(t, err, codes.FailedPrecondition)
	assert.Contains(t, status.Convert(err).Message(), "names no release")
}

func TestRebaseHandler_RejectsActiveRunOnSameSchedule(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	scheduleName := "rebase-rej-concurrent-" + uuid.New().String()[:8]
	srcID := seedRunWithTaskStatus(t, fx, scheduleName, run.SchedulerStatusFailed, run.TaskStatusFailed)
	_ = seedRunningSchedulerOnSchedule(t, fx, scheduleName)

	_, err := fx.Handler.TriggerRebase(context.Background(), &statev1.TriggerRebaseRequest{SourceRunId: srcID.String()})
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func requireGRPCCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "error is not a gRPC status: %v", err)
	require.Equal(t, want, st.Code(), "wrong gRPC code: got %s want %s (msg: %q)", st.Code(), want, st.Message())
}
