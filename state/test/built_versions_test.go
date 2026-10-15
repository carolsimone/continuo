package test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
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
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	execRepo := postgres.NewTaskExecutionRepository(db, logger)
	catalogRepo := postgres.NewScheduleCatalogRepository(db, logger)
	return &builtHarness{db: db, logger: logger, factory: func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, schedulerRepo, taskRepo, execRepo, catalogRepo, svcports.SystemClock{}, logger)
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
	return rn.ScheduleID()
}

// seedRun saves a promote-seed run of releaseID that loads raw.table.
func (h *builtHarness) seedRun(t *testing.T, releaseID, table string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	rn, _, err := run.NewPromotedSeedsRun(id, "promote-seed-"+strings.ReplaceAll(id.String(), "-", "")[:8], run.ReleaseStamp{ReleaseID: releaseID, PromotionSeq: 1},
		[]run.SeedNode{{NodeID: run.NodeID{ServiceName: "service-1", SchemaName: "raw", TableName: table}, NodeType: "dbt-seed", ImageTag: "service-1:v1"}},
		time.Now())
	require.NoError(t, err)
	return h.saveRun(t, rn)
}

// dispatch creates the run's one task on raw.table, as run.entries.dispatched:v1
// does, and returns its id.
func (h *builtHarness) dispatch(t *testing.T, runID uuid.UUID, table, imageTag string) uuid.UUID {
	t.Helper()
	taskID := uuid.New()
	h.inTx(t, func(ctx context.Context, u uow.UnitOfWork) error {
		return svchandlers.NewRunEntriesDispatchedHandler(h.logger).Handle(ctx, u, events.RunEntriesDispatched{
			ScheduleID: runID, TotalTaskCount: 1,
			AllTasks: []events.RunEntriesDispatchedTask{{
				TaskID: taskID, ServiceName: "service-1", SchemaName: "raw", TableName: table,
				Status: run.TaskStatusPending, ImageTag: imageTag,
			}},
		}, uuid.Nil)
	})
	return taskID
}

// finish reports the task's status, as task.status.updated:v1 does.
func (h *builtHarness) finish(t *testing.T, runID, taskID uuid.UUID, status run.TaskStatus) {
	t.Helper()
	h.inTx(t, func(ctx context.Context, u uow.UnitOfWork) error {
		return svchandlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, h.logger).Handle(ctx, u,
			events.TaskStatusUpdated{TaskID: taskID, ScheduleID: runID, Status: status}, uuid.Nil)
	})
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

// TestBuiltVersions drives task.status.updated:v1 against a real Postgres: a
// succeeded task of a run that names a release records the version it loaded,
// a rerun of such a run carries its release, and the other cases record
// nothing. Every subtest uses its own release and node.
func TestBuiltVersions(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, cleanup := setupPostgres(t)
	defer cleanup()
	h := newBuiltHarness(db)

	t.Run("a succeeded promote-seed task records the release's content hash", func(t *testing.T) {
		releaseID := "rel-" + uuid.NewString()[:8]
		h.storeRelease(t, releaseID, promotedNode("raw", "orders_built", "dbt-seed"))
		runID := h.seedRun(t, releaseID, "orders_built")
		taskID := h.dispatch(t, runID, "orders_built", "service-1:v1")

		h.finish(t, runID, taskID, run.TaskStatusSucceeded)

		row, ok := h.loaded(t, "raw.orders_built")
		require.True(t, ok)
		assert.Equal(t, "sha256:orders_built", row.ContentHash)
		assert.Equal(t, "service-1:v1", row.ImageTag)
		assert.Equal(t, releaseID, row.ReleaseID)
		assert.Equal(t, uuid.NullUUID{UUID: runID, Valid: true}, row.RunID)
		assert.Equal(t, uuid.NullUUID{UUID: taskID, Valid: true}, row.TaskID)
		require.NotNil(t, row.Source)
		assert.Equal(t, "build", *row.Source)
	})

	t.Run("a run that names no release records nothing", func(t *testing.T) {
		rn, _, err := run.NewPendingRun("trigger-"+uuid.NewString()[:8], run.KindTrigger, nil, "", nil, model.OperationRun, run.ReleaseStamp{}, time.Now())
		require.NoError(t, err)
		runID := h.saveRun(t, rn)
		taskID := h.dispatch(t, runID, "no_release", "service-1:v1")

		h.finish(t, runID, taskID, run.TaskStatusSucceeded)

		_, ok := h.loaded(t, "raw.no_release")
		assert.False(t, ok)
	})

	t.Run("a failed task records nothing", func(t *testing.T) {
		releaseID := "rel-" + uuid.NewString()[:8]
		h.storeRelease(t, releaseID, promotedNode("raw", "failing", "dbt-seed"))
		runID := h.seedRun(t, releaseID, "failing")
		taskID := h.dispatch(t, runID, "failing", "service-1:v1")

		h.finish(t, runID, taskID, run.TaskStatusFailed)

		_, ok := h.loaded(t, "raw.failing")
		assert.False(t, ok)
	})

	t.Run("a node absent from the run's release records nothing", func(t *testing.T) {
		releaseID := "rel-" + uuid.NewString()[:8]
		h.storeRelease(t, releaseID, promotedNode("raw", "present", "dbt-seed"))
		runID := h.seedRun(t, releaseID, "absent")
		taskID := h.dispatch(t, runID, "absent", "service-1:v1")

		h.finish(t, runID, taskID, run.TaskStatusSucceeded)

		_, ok := h.loaded(t, "raw.absent")
		assert.False(t, ok)
	})

	t.Run("the version is recorded in the task status transaction, so a rollback records nothing", func(t *testing.T) {
		releaseID := "rel-" + uuid.NewString()[:8]
		h.storeRelease(t, releaseID, promotedNode("raw", "rolled_back", "dbt-seed"))
		runID := h.seedRun(t, releaseID, "rolled_back")
		taskID := h.dispatch(t, runID, "rolled_back", "service-1:v1")

		ctx := context.Background()
		u := h.factory()
		require.NoError(t, u.Begin(ctx))
		require.NoError(t, svchandlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, h.logger).Handle(ctx, u,
			events.TaskStatusUpdated{TaskID: taskID, ScheduleID: runID, Status: run.TaskStatusSucceeded}, uuid.Nil))
		require.NoError(t, u.Rollback())

		_, ok := h.loaded(t, "raw.rolled_back")
		assert.False(t, ok, "the rolled-back delivery recorded a version")

		h.finish(t, runID, taskID, run.TaskStatusSucceeded)

		row, ok := h.loaded(t, "raw.rolled_back")
		require.True(t, ok, "the redelivery did not record the version")
		assert.Equal(t, "sha256:rolled_back", row.ContentHash)
	})

	t.Run("a rerun of a promote-seed run carries its release and records on success", func(t *testing.T) {
		releaseID := "rel-" + uuid.NewString()[:8]
		h.storeRelease(t, releaseID, promotedNode("raw", "rerun_orders", "dbt-seed"))
		sourceID := h.seedRun(t, releaseID, "rerun_orders")
		sourceTask := h.dispatch(t, sourceID, "rerun_orders", "service-1:v1")
		h.finish(t, sourceID, sourceTask, run.TaskStatusFailed)
		_, ok := h.loaded(t, "raw.rerun_orders")
		require.False(t, ok, "the failed source loaded nothing")

		rerunID, _, err := svchandlers.NewTriggerRerunHandler(defaultPlanner, h.logger).Handle(context.Background(), h.factory(), sourceID, identity.FromUserID("okta|alice"))
		require.NoError(t, err)
		var stored struct {
			ReleaseID *string `db:"release_id"`
			Kind      string  `db:"kind"`
		}
		require.NoError(t, db.Get(&stored, `SELECT release_id, kind FROM scheduler_tracker WHERE schedule_id = $1`, rerunID))
		require.NotNil(t, stored.ReleaseID)
		assert.Equal(t, releaseID, *stored.ReleaseID)
		assert.Equal(t, "rerun", stored.Kind)

		rerunTask := run.TaskIDFor(pkgevents.DefaultTenantID, rerunID, "raw.rerun_orders")
		h.finish(t, rerunID, rerunTask, run.TaskStatusSucceeded)

		row, ok := h.loaded(t, "raw.rerun_orders")
		require.True(t, ok)
		assert.Equal(t, "sha256:rerun_orders", row.ContentHash)
		assert.Equal(t, uuid.NullUUID{UUID: rerunID, Valid: true}, row.RunID)
		assert.Equal(t, releaseID, row.ReleaseID)
	})
}
