package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/seeds"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// insertSeedRun inserts a promote-seed run in status and removes it when the
// test ends.
func insertSeedRun(t *testing.T, db *sqlx.DB, status run.SchedulerStatus) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(`
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, initialization_status, kind)
		VALUES ($1, $2, $3, now(), 'completed', 'promote_seed')`, id, "promote-seed-"+id.String()[:8], string(status))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, id) })
	return id
}

var materializedAt = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

// A build request's state is its run's status: pending or running is in
// flight, succeeded is succeeded, and every other ending is failed.
func TestMaterializationRepository_ListDerivesTheAttemptStateFromTheRun(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	repo := postgres.NewMaterializationRepository(db, nil)
	cases := []struct {
		status run.SchedulerStatus
		want   seeds.AttemptState
	}{
		{run.SchedulerStatusPending, seeds.AttemptInFlight},
		{run.SchedulerStatusRunning, seeds.AttemptInFlight},
		{run.SchedulerStatusSucceeded, seeds.AttemptSucceeded},
		{run.SchedulerStatusFailed, seeds.AttemptFailed},
		{run.SchedulerStatusCancelled, seeds.AttemptFailed},
		{run.SchedulerStatusSkipped, seeds.AttemptFailed},
	}
	runs := map[run.SchedulerStatus]uuid.UUID{}
	for _, tc := range cases {
		runs[tc.status] = insertSeedRun(t, db, tc.status)
		key := topology.KeyOf("raw", string(tc.status))
		require.NoError(t, repo.RecordAttempt(ctx, tenant, key, "h-"+string(tc.status), runs[tc.status], materializedAt))
	}

	got, err := repo.List(ctx, tenant)
	require.NoError(t, err)
	require.Len(t, got, len(cases))
	for _, tc := range cases {
		m := got[topology.KeyOf("raw", string(tc.status))]
		assert.Equal(t, tc.want, m.AttemptState, string(tc.status))
		assert.Equal(t, "h-"+string(tc.status), m.AttemptContentHash)
		assert.Equal(t, runs[tc.status], m.AttemptRunID)
		assert.Empty(t, m.ContentHash, "no version is recorded by a request")
	}
}

// No request, or a request whose run no longer exists, is AttemptNone.
func TestMaterializationRepository_ListWithoutALiveRequestIsNone(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	repo := postgres.NewMaterializationRepository(db, nil)
	require.NoError(t, repo.RecordBuilt(ctx, tenant, seeds.Built{
		Key: "raw.adopted", ContentHash: "h1", ImageTag: "svc:1", ReleaseID: "rel-1", At: materializedAt, Source: seeds.SourceAdopted,
	}))
	gone := uuid.New()
	require.NoError(t, repo.RecordAttempt(ctx, tenant, "raw.gone", "h2", gone, materializedAt))

	got, err := repo.List(ctx, tenant)
	require.NoError(t, err)
	assert.Equal(t, seeds.Materialization{Key: "raw.adopted", ContentHash: "h1", AttemptState: seeds.AttemptNone}, got["raw.adopted"])
	assert.Equal(t, seeds.Materialization{Key: "raw.gone", AttemptContentHash: "h2", AttemptRunID: gone, AttemptState: seeds.AttemptNone}, got["raw.gone"])
}

func TestMaterializationRepository_RecordBuiltLeavesTheRequestAlone(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	repo := postgres.NewMaterializationRepository(db, nil)
	runID := insertSeedRun(t, db, run.SchedulerStatusRunning)
	taskID := uuid.New()
	require.NoError(t, repo.RecordAttempt(ctx, tenant, "raw.orders", "h2", runID, materializedAt))
	require.NoError(t, repo.RecordBuilt(ctx, tenant, seeds.Built{
		Key: "raw.orders", ContentHash: "h2", ImageTag: "svc:2", ReleaseID: "rel-2",
		RunID: runID, TaskID: taskID, At: materializedAt.Add(time.Minute), Source: seeds.SourceBuild,
	}))

	got, err := repo.List(ctx, tenant)
	require.NoError(t, err)
	assert.Equal(t, seeds.Materialization{
		Key: "raw.orders", ContentHash: "h2", AttemptContentHash: "h2", AttemptRunID: runID, AttemptState: seeds.AttemptInFlight,
	}, got["raw.orders"])

	var row struct {
		ImageTag       string         `db:"image_tag"`
		ReleaseID      string         `db:"release_id"`
		RunID          uuid.NullUUID  `db:"run_id"`
		TaskID         uuid.NullUUID  `db:"task_id"`
		Source         sql.NullString `db:"source"`
		MaterializedAt time.Time      `db:"materialized_at"`
		AttemptedAt    time.Time      `db:"attempted_at"`
	}
	require.NoError(t, db.Get(&row, `
		SELECT image_tag, release_id, run_id, task_id, source, materialized_at, attempted_at
		FROM node_materializations WHERE tenant_id = $1 AND node_key = 'raw.orders'`, tenant))
	assert.Equal(t, "svc:2", row.ImageTag)
	assert.Equal(t, "rel-2", row.ReleaseID)
	assert.Equal(t, uuid.NullUUID{UUID: runID, Valid: true}, row.RunID)
	assert.Equal(t, uuid.NullUUID{UUID: taskID, Valid: true}, row.TaskID)
	assert.Equal(t, sql.NullString{String: "build", Valid: true}, row.Source)
	assert.True(t, row.MaterializedAt.Equal(materializedAt.Add(time.Minute)))
	assert.True(t, row.AttemptedAt.Equal(materializedAt))
}

// An adopted version has no run or task; a later request keeps it.
func TestMaterializationRepository_RecordAttemptLeavesTheVersionAlone(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	repo := postgres.NewMaterializationRepository(db, nil)
	require.NoError(t, repo.RecordBuilt(ctx, tenant, seeds.Adopted(seeds.Desired{
		Key: "raw.orders", ServiceName: "svc", SchemaName: "raw", TableName: "orders", NodeType: "dbt-seed",
		ContentHash: "h1", ImageTag: "svc:1",
	}, "rel-1", materializedAt)))
	runID := insertSeedRun(t, db, run.SchedulerStatusFailed)
	require.NoError(t, repo.RecordAttempt(ctx, tenant, "raw.orders", "h3", runID, materializedAt.Add(time.Hour)))

	got, err := repo.List(ctx, tenant)
	require.NoError(t, err)
	assert.Equal(t, seeds.Materialization{
		Key: "raw.orders", ContentHash: "h1", AttemptContentHash: "h3", AttemptRunID: runID, AttemptState: seeds.AttemptFailed,
	}, got["raw.orders"])

	var row struct {
		RunID  uuid.NullUUID  `db:"run_id"`
		TaskID uuid.NullUUID  `db:"task_id"`
		Source sql.NullString `db:"source"`
	}
	require.NoError(t, db.Get(&row, `
		SELECT run_id, task_id, source FROM node_materializations WHERE tenant_id = $1 AND node_key = 'raw.orders'`, tenant))
	assert.False(t, row.RunID.Valid, "an adopted version has no run")
	assert.False(t, row.TaskID.Valid, "an adopted version has no task")
	assert.Equal(t, sql.NullString{String: "adopted", Valid: true}, row.Source)
}

// A row whose node the release no longer has is deleted; the others stay.
func TestMaterializationRepository_DeleteAbsentFromKeepsTheReleasesNodes(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "aa",
		topoSpec("raw", "orders", "dbt-seed", "h1"), topoSpec("raw", "users", "dbt-seed", "h2")))
	require.NoError(t, err)
	repo := postgres.NewMaterializationRepository(db, nil)
	for _, key := range []topology.NodeKey{"raw.orders", "raw.users", "raw.gone"} {
		require.NoError(t, repo.RecordBuilt(ctx, tenant, seeds.Built{
			Key: key, ContentHash: "h", ImageTag: "svc:1", ReleaseID: "rel-0", At: materializedAt, Source: seeds.SourceAdopted,
		}))
	}

	deleted, err := repo.DeleteAbsentFrom(ctx, tenant, "rel-1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	got, err := repo.List(ctx, tenant)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Contains(t, got, topology.NodeKey("raw.orders"))
	assert.Contains(t, got, topology.NodeKey("raw.users"))
}

// A row whose node the release lacks stays while its build request is still
// pending or running, so a later release that brings the node back finds the
// build in flight rather than starting a second one. Once the run ends the row
// is deleted like any other absent node's.
func TestMaterializationRepository_DeleteAbsentFromKeepsARowWhoseBuildIsInFlight(t *testing.T) {
	ctx := context.Background()
	for _, inFlight := range []run.SchedulerStatus{run.SchedulerStatusPending, run.SchedulerStatusRunning} {
		t.Run(string(inFlight), func(t *testing.T) {
			db := newTestDB(t)
			tenant := newTopoTenant(t, db)
			_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-2", 2, "bb", topoSpec("raw", "users", "dbt-seed", "h2")))
			require.NoError(t, err)
			repo := postgres.NewMaterializationRepository(db, nil)
			runID := insertSeedRun(t, db, inFlight)
			require.NoError(t, repo.RecordAttempt(ctx, tenant, "raw.orders", "h1", runID, materializedAt))
			// A row with no build request, and one whose run is over, go
			// as soon as their node is absent.
			require.NoError(t, repo.RecordBuilt(ctx, tenant, seeds.Built{
				Key: "raw.adopted", ContentHash: "h", ImageTag: "svc:1", ReleaseID: "rel-1", At: materializedAt, Source: seeds.SourceAdopted,
			}))
			require.NoError(t, repo.RecordAttempt(ctx, tenant, "raw.failed", "h", insertSeedRun(t, db, run.SchedulerStatusFailed), materializedAt))

			deleted, err := repo.DeleteAbsentFrom(ctx, tenant, "rel-2")
			require.NoError(t, err)
			assert.Equal(t, int64(2), deleted, "only the rows with no build in flight go")
			got, err := repo.List(ctx, tenant)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, seeds.Materialization{
				Key: "raw.orders", AttemptContentHash: "h1", AttemptRunID: runID, AttemptState: seeds.AttemptInFlight,
			}, got["raw.orders"], "the build request of the absent node survives")

			_, err = db.Exec(`UPDATE scheduler_tracker SET status = 'succeeded', completed_at = now() WHERE schedule_id = $1`, runID)
			require.NoError(t, err)
			deleted, err = repo.DeleteAbsentFrom(ctx, tenant, "rel-2")
			require.NoError(t, err)
			assert.Equal(t, int64(1), deleted, "the row goes once its run has ended")
			got, err = repo.List(ctx, tenant)
			require.NoError(t, err)
			assert.Empty(t, got)
		})
	}
}

// The build request of an absent node survives while its run is cancelling:
// the run has not ended, and its attempt may still load the table. The row
// goes once the run is cancelled.
func TestMaterializations_DeleteAbsentFromKeepsACancellingRunsRequest(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-2", 2, "bb", topoSpec("raw", "users", "dbt-seed", "h2")))
	require.NoError(t, err)
	repo := postgres.NewMaterializationRepository(db, nil)
	runID := insertSeedRun(t, db, run.SchedulerStatusCancelling)
	require.NoError(t, repo.RecordAttempt(ctx, tenant, "raw.orders", "h1", runID, materializedAt))

	deleted, err := repo.DeleteAbsentFrom(ctx, tenant, "rel-2")
	require.NoError(t, err)
	assert.Zero(t, deleted)
	got, err := repo.List(ctx, tenant)
	require.NoError(t, err)
	assert.Contains(t, got, topology.NodeKey("raw.orders"))

	_, err = db.Exec(`UPDATE scheduler_tracker SET status = 'cancelled', completed_at = now() WHERE schedule_id = $1`, runID)
	require.NoError(t, err)
	deleted, err = repo.DeleteAbsentFrom(ctx, tenant, "rel-2")
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
}

// The unit of work's Materializations() writes inside its transaction.
func TestPostgresUnitOfWork_MaterializationWritesRollBackWithTheTransaction(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	logger := discardLogger()
	u := postgres.NewPostgresUnitOfWork(db,
		postgres.NewSchedulerTrackerRepository(db, logger), postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, logger),
		postgres.NewScheduleCatalogRepository(db, logger),
		ports.SystemClock{}, logger)

	require.NoError(t, u.Begin(ctx))
	require.NoError(t, u.Materializations().RecordAttempt(ctx, tenant, "raw.orders", "h1", uuid.New(), materializedAt))
	require.NoError(t, u.Rollback())

	got, err := postgres.NewMaterializationRepository(db, nil).List(ctx, tenant)
	require.NoError(t, err)
	assert.Empty(t, got)
}
