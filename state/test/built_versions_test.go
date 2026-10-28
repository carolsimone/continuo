package test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/topology"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	svcports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
)

// builtHarness runs state's handlers against a real Postgres through the unit
// of work state's consumers use.
type builtHarness struct {
	db      *sqlx.DB
	factory func() uow.UnitOfWork
	logger  *slog.Logger
}

func newBuiltHarness(db *sqlx.DB) *builtHarness {
	logger := slog.New(slog.NewTextHandler(nopWriter{}, nil))
	return &builtHarness{db: db, logger: logger, factory: func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, svcports.SystemClock{}, logger)
	}}
}

// inTx runs fn in one committed unit of work.
func (h *builtHarness) inTx(t *testing.T, fn func(ctx context.Context, u uow.UnitOfWork) error) {
	t.Helper()
	ctx := context.Background()
	u := h.factory()
	require.NoError(t, u.Begin(ctx))
	defer u.Rollback() //nolint:errcheck
	require.NoError(t, fn(ctx, u))
	require.NoError(t, u.Commit())
}

// commands returns the RunService commands over h's database: every
// run-creating command claims its request id and creates its run in one
// transaction of RunCommands.
func (h *builtHarness) commands() *svchandlers.RunCommands {
	return svchandlers.NewRunCommands(h.factory, noRunMetrics{}, defaultPlanner, h.logger)
}

// rerun reruns the default tenant's run source for initiator under a fresh
// request id and returns the rerun's id.
func (h *builtHarness) rerun(ctx context.Context, source uuid.UUID, initiator identity.Identity) (uuid.UUID, error) {
	res, err := h.commands().TriggerRerun(ctx, svchandlers.TriggerRerunCommand{
		TenantID: pkgevents.DefaultTenantID, RequestID: uuid.New(), SourceRunID: source, Initiator: initiator,
	})
	return res.RunID, err
}

// rebase creates the rebase in asks for under a fresh request id and returns
// its id.
func (h *builtHarness) rebase(ctx context.Context, in svchandlers.TriggerRebaseInput) (uuid.UUID, error) {
	res, err := h.commands().TriggerRebase(ctx, svchandlers.TriggerRebaseCommand{RequestID: uuid.New(), TriggerRebaseInput: in})
	return res.RunID, err
}

// singleNode creates the single-node run in asks for under a fresh request id
// and returns its id.
func (h *builtHarness) singleNode(ctx context.Context, in svchandlers.TriggerSingleNodeRunInput) (uuid.UUID, error) {
	res, err := h.commands().TriggerSingleNodeRun(ctx, svchandlers.TriggerSingleNodeRunCommand{RequestID: uuid.New(), TriggerSingleNodeRunInput: in})
	return res.RunID, err
}

// storeRelease stores releaseID for the default tenant.
func (h *builtHarness) storeRelease(t *testing.T, releaseID string, specs ...topology.NodeSpec) {
	t.Helper()
	rel, err := topology.NewRelease(pkgevents.DefaultTenantID, releaseID, 1,
		"s3://continuo/tenants/default/topologies/"+releaseID+"/topology.json.gz", "sha-"+releaseID, specs)
	require.NoError(t, err)
	h.inTx(t, func(ctx context.Context, u uow.UnitOfWork) error {
		if err := u.Topology().LockTopology(ctx); err != nil {
			return err
		}
		_, err := u.Topology().SaveRelease(ctx, rel)
		return err
	})
}

// saveRun persists rn.
func (h *builtHarness) saveRun(t *testing.T, rn *run.Run) uuid.UUID {
	t.Helper()
	h.inTx(t, func(ctx context.Context, u uow.UnitOfWork) error { return u.Run().SaveRun(ctx, rn) })
	return rn.RunID()
}

// trigger triggers schedule, which a model of the live release carries, with
// operation op on the live release, and returns the run.
func (h *builtHarness) trigger(t *testing.T, schedule string, op model.Operation) uuid.UUID {
	t.Helper()
	id, err := h.start(context.Background(), pkgevents.DefaultTenantID, schedule, identity.FromUserID("okta|alice"), op)
	require.NoError(t, err)
	return id
}

// start starts a triggered run of tenantID's schedule with operation op for
// initiator, as StartRun starts one, in a transaction of its own, and returns
// it.
func (h *builtHarness) start(ctx context.Context, tenantID, schedule string, initiator identity.Identity, op model.Operation) (uuid.UUID, error) {
	id := uuid.New()
	u := h.factory()
	if err := u.Begin(ctx); err != nil {
		return uuid.Nil, err
	}
	defer u.Rollback() //nolint:errcheck
	if err := svchandlers.NewStartRunHandler(defaultPlanner, h.logger).StartInTransaction(ctx, u, svchandlers.StartRunInput{
		RunID: id, TenantID: tenantID, ScheduleName: schedule, Kind: run.KindTrigger, Initiator: initiator, Operation: op,
	}); err != nil {
		return uuid.Nil, err
	}
	return id, u.Commit()
}

// fire fires the slot of tenantID's schedule as the cron loop does, in a
// transaction of its own that it commits unless the fire fails.
func (h *builtHarness) fire(ctx context.Context, tenantID, schedule string, slot time.Time) (uuid.UUID, svchandlers.ActivationOutcome, error) {
	u := h.factory()
	if err := u.Begin(ctx); err != nil {
		return uuid.Nil, 0, err
	}
	defer u.Rollback() //nolint:errcheck
	id, outcome, err := svchandlers.NewStartRunHandler(defaultPlanner, h.logger).HandleCronFire(ctx, u, tenantID, schedule, slot)
	if err != nil {
		return uuid.Nil, 0, err
	}
	return id, outcome, u.Commit()
}

// legacyRun saves a running run on releaseID whose tasks state did not plan:
// planned_at is NULL, and its one pending task at raw.table records no
// dispatch. It returns the run and its task.
func (h *builtHarness) legacyRun(t *testing.T, releaseID, table string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	runID, taskID := uuid.New(), uuid.New()
	_, err := h.db.Exec(`
		INSERT INTO runs (tenant_id, run_id, schedule_name, status, total_task_count, release_id, promotion_seq)
		VALUES ('default', $1, $2, 'running', 1, $3, 1)`, runID, "legacy-"+runID.String()[:8], releaseID)
	require.NoError(t, err)
	_, err = h.db.Exec(`
		INSERT INTO tasks (tenant_id, task_id, run_id, service_name, schema_name, table_name, job_name, status, max_retries, image_tag, node_key)
		VALUES ((SELECT tenant_id FROM runs WHERE run_id = $2), $1, $2, 'service-1', 'raw', $3, $3, 'pending', 2, 'service-1:v1', $4)`, taskID, runID, table, "raw."+table)
	require.NoError(t, err)
	return runID, taskID
}

// finish reports how attempt 0 of the task ended, as task.status.updated:v2
// does; a failure is not retried.
func (h *builtHarness) finish(t *testing.T, runID, taskID uuid.UUID, status run.AttemptStatus) {
	t.Helper()
	h.inTx(t, func(ctx context.Context, u uow.UnitOfWork) error {
		return h.reports().Handle(ctx, u, finished(runID, taskID, status), uuid.Nil)
	})
}

// reports is the handler of task.status.updated:v2 the harness reports
// through.
func (h *builtHarness) reports() *svchandlers.AttemptReportHandler {
	return svchandlers.NewAttemptReportHandler(defaultPlanner.Tasks, noRunMetrics{}, h.logger)
}

// finished is the report that attempt 0 of taskID ended with status; a
// failure names a reason that is not retried.
func finished(runID, taskID uuid.UUID, status run.AttemptStatus) events.AttemptReported {
	rep := run.AttemptReport{TaskID: taskID, Attempt: 0, Status: status}
	if status == run.AttemptFailed {
		rep.FailureReason = model.FailureReasonDeadlineExceeded
	}
	return events.AttemptReported{TenantID: pkgevents.DefaultTenantID, RunID: runID, Report: rep}
}

// task is the id of runID's task at key, for the default tenant.
func task(runID uuid.UUID, key topology.NodeKey) uuid.UUID {
	return run.TaskIDFor(pkgevents.DefaultTenantID, runID, key)
}

type loadedRow struct {
	ContentHash string        `db:"content_hash"`
	ImageTag    string        `db:"image_tag"`
	ReleaseID   string        `db:"release_id"`
	RunID       uuid.NullUUID `db:"run_id"`
	TaskID      uuid.NullUUID `db:"task_id"`
	Source      *string       `db:"source"`
}

// loaded reads the default tenant's row for key and whether it exists.
func (h *builtHarness) loaded(t *testing.T, key string) (loadedRow, bool) {
	t.Helper()
	var row loadedRow
	err := h.db.Get(&row, `
		SELECT content_hash, image_tag, release_id, run_id, task_id, source
		FROM node_materializations WHERE tenant_id = $1 AND node_key = $2`, pkgevents.DefaultTenantID, key)
	if errors.Is(err, sql.ErrNoRows) {
		return loadedRow{}, false
	}
	require.NoError(t, err)
	return row, true
}

// TestBuiltVersions drives task.status.updated:v2 against a real Postgres: a
// succeeded task of a run state planned with a writing operation records the
// version it loaded, the content hash of its node in the run's release, and
// the other cases record nothing. Every subtest uses its own release and node.
func TestBuiltVersions(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, cleanup := setupPostgres(t)
	defer cleanup()
	h := newBuiltHarness(db)

	t.Run("a succeeded task of a planned run records the release's content hash", func(t *testing.T) {
		stamp := promoteRelease(t, db, scheduledNode("raw", "orders_built", "dbt-model", "built-orders"))
		runID := h.trigger(t, "built-orders", model.OperationRun)
		taskID := task(runID, "raw.orders_built")

		h.finish(t, runID, taskID, run.AttemptSucceeded)

		row, ok := h.loaded(t, "raw.orders_built")
		require.True(t, ok)
		assert.Equal(t, "sha256:orders_built", row.ContentHash)
		assert.Equal(t, "service-1:v1", row.ImageTag)
		assert.Equal(t, stamp.ReleaseID, row.ReleaseID)
		assert.Equal(t, uuid.NullUUID{UUID: runID, Valid: true}, row.RunID)
		assert.Equal(t, uuid.NullUUID{UUID: taskID, Valid: true}, row.TaskID)
		require.NotNil(t, row.Source)
		assert.Equal(t, "build", *row.Source)
	})

	t.Run("a succeeded task of a test run records nothing", func(t *testing.T) {
		tested := scheduledNode("raw", "tested_built", "dbt-model", "built-tested")
		one := 1
		tested.TestCount = &one
		promoteRelease(t, db, tested)
		runID := h.trigger(t, "built-tested", model.OperationTest)

		h.finish(t, runID, task(runID, "raw.tested_built"), run.AttemptSucceeded)

		_, ok := h.loaded(t, "raw.tested_built")
		assert.False(t, ok, "a test reads the table, it does not load it")
	})

	t.Run("a failed task records nothing", func(t *testing.T) {
		promoteRelease(t, db, scheduledNode("raw", "failing", "dbt-model", "built-failing"))
		runID := h.trigger(t, "built-failing", model.OperationRun)

		h.finish(t, runID, task(runID, "raw.failing"), run.AttemptFailed)

		_, ok := h.loaded(t, "raw.failing")
		assert.False(t, ok)
	})

	t.Run("a node without a content hash records nothing", func(t *testing.T) {
		blank := scheduledNode("raw", "unhashed", "dbt-model", "built-unhashed")
		blank.ContentHash = ""
		promoteRelease(t, db, blank)
		runID := h.trigger(t, "built-unhashed", model.OperationRun)

		h.finish(t, runID, task(runID, "raw.unhashed"), run.AttemptSucceeded)

		_, ok := h.loaded(t, "raw.unhashed")
		assert.False(t, ok)
	})

	t.Run("the version is recorded in the task status transaction, so a rollback records nothing", func(t *testing.T) {
		promoteRelease(t, db, scheduledNode("raw", "rolled_back", "dbt-model", "built-rollback"))
		runID := h.trigger(t, "built-rollback", model.OperationRun)
		taskID := task(runID, "raw.rolled_back")

		ctx := context.Background()
		u := h.factory()
		require.NoError(t, u.Begin(ctx))
		require.NoError(t, h.reports().Handle(ctx, u, finished(runID, taskID, run.AttemptSucceeded), uuid.Nil))
		require.NoError(t, u.Rollback())

		_, ok := h.loaded(t, "raw.rolled_back")
		assert.False(t, ok, "the rolled-back delivery recorded a version")

		h.finish(t, runID, taskID, run.AttemptSucceeded)

		row, ok := h.loaded(t, "raw.rolled_back")
		require.True(t, ok, "the redelivery did not record the version")
		assert.Equal(t, "sha256:rolled_back", row.ContentHash)
	})

	t.Run("a rerun records the content hash of its source's release", func(t *testing.T) {
		source := promoteRelease(t, db, scheduledNode("raw", "rerun_orders", "dbt-model", "built-rerun"))
		sourceID := h.trigger(t, "built-rerun", model.OperationRun)
		_, err := db.Exec(`UPDATE tasks SET status = 'failed', attempt = 2 WHERE run_id = $1`, sourceID)
		require.NoError(t, err)
		_, err = db.Exec(`UPDATE runs SET status = 'failed', completed_at = now() WHERE run_id = $1`, sourceID)
		require.NoError(t, err)
		releaseLeasesOf(t, db, sourceID)
		changed := scheduledNode("raw", "rerun_orders", "dbt-model", "built-rerun")
		changed.ContentHash = "sha256:rerun_orders-changed"
		promoteRelease(t, db, changed)

		rerunID, err := h.rerun(context.Background(), sourceID, identity.FromUserID("okta|alice"))
		require.NoError(t, err)
		h.finish(t, rerunID, task(rerunID, "raw.rerun_orders"), run.AttemptSucceeded)

		row, ok := h.loaded(t, "raw.rerun_orders")
		require.True(t, ok)
		assert.Equal(t, "sha256:rerun_orders", row.ContentHash, "the rerun loaded the source release's version")
		assert.Equal(t, source.ReleaseID, row.ReleaseID)
		assert.Equal(t, uuid.NullUUID{UUID: rerunID, Valid: true}, row.RunID)
	})
}

// noRunMetrics counts nothing.
type noRunMetrics struct{}

func (noRunMetrics) RunStalled()                   {}
func (noRunMetrics) RunsCancelling(_, _ int)       {}
func (noRunMetrics) RunCancelled(time.Duration)    {}
func (noRunMetrics) LeaseWaiters(int)              {}
func (noRunMetrics) LeaseGranted(time.Duration)    {}
func (noRunMetrics) LeaseRepaired()                {}
func (noRunMetrics) CommandRequest(string, string) {}
func (noRunMetrics) CronFire(string)               {}
func (noRunMetrics) Schedules(int, int)            {}
