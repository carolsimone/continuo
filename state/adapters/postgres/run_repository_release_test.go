package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newReleaseRunUoW(db *sqlx.DB) *postgres.PostgresUnitOfWork {
	logger := discardLogger()
	return postgres.NewPostgresUnitOfWork(db,
		postgres.NewSchedulerTrackerRepository(db, logger), postgres.NewTaskTrackerRepository(db, logger),
		postgres.NewTaskExecutionRepository(db, logger), postgres.NewScheduleCatalogRepository(db, logger),
		ports.SystemClock{}, logger)
}

// promoteSeedRun builds an unsaved promote-seed run of releaseID with one seed.
func promoteSeedRun(t *testing.T, id uuid.UUID, releaseID string) *run.Run {
	t.Helper()
	rn, _, err := run.NewPromotedSeedsRun(id, "promote-seed-"+id.String()[:8], releaseID, 4,
		[]run.SeedNode{{NodeID: run.NodeID{ServiceName: "svc", SchemaName: "raw", TableName: "orders"}, NodeType: "dbt-seed", ImageTag: "svc:1"}},
		time.Now())
	require.NoError(t, err)
	return rn
}

func saveRuns(t *testing.T, u *postgres.PostgresUnitOfWork, runs ...*run.Run) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, u.Begin(ctx))
	for _, rn := range runs {
		require.NoError(t, u.Run().SaveRun(ctx, rn))
	}
	require.NoError(t, u.Commit())
}

// A promote-seed run stores the release it builds, and every load brings it back.
func TestRunRepository_PromoteSeedRunKeepsItsRelease(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	releaseID := "rel-" + uuid.NewString()
	id := uuid.New()
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, id) })
	u := newReleaseRunUoW(db)
	saveRuns(t, u, promoteSeedRun(t, id, releaseID))

	var stored sql.NullString
	require.NoError(t, db.Get(&stored, `SELECT release_id FROM scheduler_tracker WHERE schedule_id = $1`, id))
	assert.Equal(t, sql.NullString{String: releaseID, Valid: true}, stored)

	read, err := u.Run().GetRun(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, releaseID, read.ReleaseID())

	require.NoError(t, u.Begin(ctx))
	defer u.Rollback() //nolint:errcheck
	locked, err := u.Run().LoadRunForUpdate(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, releaseID, locked.ReleaseID())
}

// A run that names no release stores NULL and loads with an empty release id.
func TestRunRepository_RunWithoutAReleaseStoresNull(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	rn, _, err := run.NewPendingRun("no-release-"+uuid.NewString()[:8], run.KindTrigger, nil, "", nil, model.OperationRun, time.Now())
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, rn.ScheduleID()) })
	u := newReleaseRunUoW(db)
	saveRuns(t, u, rn)

	var stored sql.NullString
	require.NoError(t, db.Get(&stored, `SELECT release_id FROM scheduler_tracker WHERE schedule_id = $1`, rn.ScheduleID()))
	assert.False(t, stored.Valid)
	read, err := u.Run().GetRun(ctx, rn.ScheduleID())
	require.NoError(t, err)
	assert.Empty(t, read.ReleaseID())
}

// RunExists and CountReleaseRuns read inside the unit of work's transaction,
// so they see the runs it has written, and refuse to run outside one.
func TestRunRepository_RunExistsAndCountReleaseRuns(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	releaseID := "rel-" + uuid.NewString()
	other := "rel-" + uuid.NewString()
	first, second, third, elsewhere := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM scheduler_tracker WHERE release_id IN ($1, $2)`, releaseID, other)
	})
	u := newReleaseRunUoW(db)
	saveRuns(t, u, promoteSeedRun(t, first, releaseID), promoteSeedRun(t, second, releaseID), promoteSeedRun(t, elsewhere, other))

	require.NoError(t, u.Begin(ctx))
	defer u.Rollback() //nolint:errcheck
	exists, err := u.Run().RunExists(ctx, first)
	require.NoError(t, err)
	assert.True(t, exists)
	exists, err = u.Run().RunExists(ctx, uuid.New())
	require.NoError(t, err)
	assert.False(t, exists)

	n, err := u.Run().CountReleaseRuns(ctx, releaseID, run.KindPromoteSeed)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	n, err = u.Run().CountReleaseRuns(ctx, releaseID, run.KindRerun)
	require.NoError(t, err)
	assert.Zero(t, n, "only runs of the asked kind count")

	require.NoError(t, u.Run().SaveRun(ctx, promoteSeedRun(t, third, releaseID)))
	n, err = u.Run().CountReleaseRuns(ctx, releaseID, run.KindPromoteSeed)
	require.NoError(t, err)
	assert.Equal(t, 3, n, "a run written in the same transaction counts")

	outside := newReleaseRunUoW(db)
	_, err = outside.Run().RunExists(ctx, first)
	assert.Error(t, err, "RunExists needs a transaction")
	_, err = outside.Run().CountReleaseRuns(ctx, releaseID, run.KindPromoteSeed)
	assert.Error(t, err, "CountReleaseRuns needs a transaction")
}
