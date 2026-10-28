package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
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
	return postgres.NewPostgresUnitOfWork(db, ports.SystemClock{}, logger)
}

// promoteSeedRun builds an unsaved promote-seed run of releaseID with one seed.
func promoteSeedRun(t *testing.T, id uuid.UUID, releaseID string) *run.Run {
	t.Helper()
	rn, err := run.NewPromotedSeedsRun(pkgevents.DefaultTenantID, id, "promote-seed-"+id.String()[:8], run.ReleaseStamp{ReleaseID: releaseID, PromotionSeq: 4}, time.Now())
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
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, id) })
	u := newReleaseRunUoW(db)
	saveRuns(t, u, promoteSeedRun(t, id, releaseID))

	var stored sql.NullString
	require.NoError(t, db.Get(&stored, `SELECT release_id FROM runs WHERE run_id = $1`, id))
	assert.Equal(t, sql.NullString{String: releaseID, Valid: true}, stored)

	read, err := u.Run().GetRun(ctx, pkgevents.DefaultTenantID, id)
	require.NoError(t, err)
	assert.Equal(t, releaseID, read.ReleaseID())

	require.NoError(t, u.Begin(ctx))
	defer u.Rollback() //nolint:errcheck
	locked, err := u.Run().LoadRunForUpdate(ctx, pkgevents.DefaultTenantID, id)
	require.NoError(t, err)
	assert.Equal(t, releaseID, locked.ReleaseID())
}

// A run that names no release stores NULL and loads with an empty release id.
func TestRunRepository_RunWithoutAReleaseStoresNull(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	rn, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "no-release-"+uuid.NewString()[:8], run.KindTrigger, nil, "", model.OperationRun, run.ReleaseStamp{}, time.Now())
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, rn.RunID()) })
	u := newReleaseRunUoW(db)
	saveRuns(t, u, rn)

	var stored sql.NullString
	require.NoError(t, db.Get(&stored, `SELECT release_id FROM runs WHERE run_id = $1`, rn.RunID()))
	assert.False(t, stored.Valid)
	read, err := u.Run().GetRun(ctx, pkgevents.DefaultTenantID, rn.RunID())
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
		_, _ = db.Exec(`DELETE FROM runs WHERE release_id IN ($1, $2)`, releaseID, other)
	})
	u := newReleaseRunUoW(db)
	saveRuns(t, u, promoteSeedRun(t, first, releaseID), promoteSeedRun(t, second, releaseID), promoteSeedRun(t, elsewhere, other))

	require.NoError(t, u.Begin(ctx))
	defer u.Rollback() //nolint:errcheck
	exists, err := u.Run().RunExists(ctx, pkgevents.DefaultTenantID, first)
	require.NoError(t, err)
	assert.True(t, exists)
	exists, err = u.Run().RunExists(ctx, pkgevents.DefaultTenantID, uuid.New())
	require.NoError(t, err)
	assert.False(t, exists)

	n, err := u.Run().CountReleaseRuns(ctx, pkgevents.DefaultTenantID, releaseID, run.KindPromoteSeed)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	n, err = u.Run().CountReleaseRuns(ctx, pkgevents.DefaultTenantID, releaseID, run.KindRerun)
	require.NoError(t, err)
	assert.Zero(t, n, "only runs of the asked kind count")

	require.NoError(t, u.Run().SaveRun(ctx, promoteSeedRun(t, third, releaseID)))
	n, err = u.Run().CountReleaseRuns(ctx, pkgevents.DefaultTenantID, releaseID, run.KindPromoteSeed)
	require.NoError(t, err)
	assert.Equal(t, 3, n, "a run written in the same transaction counts")

	outside := newReleaseRunUoW(db)
	_, err = outside.Run().RunExists(ctx, pkgevents.DefaultTenantID, first)
	assert.Error(t, err, "RunExists needs a transaction")
	_, err = outside.Run().CountReleaseRuns(ctx, pkgevents.DefaultTenantID, releaseID, run.KindPromoteSeed)
	assert.Error(t, err, "CountReleaseRuns needs a transaction")
}

// A run stores its release's promotion seq beside the release id, a run with
// no release stores NULLs, and every load restores the stamp.
func TestRunRepository_RunKeepsItsReleaseStamp(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	stamped, unstamped := uuid.New(), uuid.New()
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM runs WHERE run_id IN ($1, $2)`, stamped, unstamped)
	})
	u := newReleaseRunUoW(db)

	stamp := run.ReleaseStamp{ReleaseID: uuid.NewString(), PromotionSeq: 12}
	withStamp, err := run.NewDerivedRun(pkgevents.DefaultTenantID, uuid.New(), "stamp-"+stamped.String()[:8], run.KindRerun, uuid.New(), stamp, "alice", model.OperationRun, time.Now())
	require.NoError(t, err)
	withoutStamp, err := run.NewDerivedRun(pkgevents.DefaultTenantID, uuid.New(), "stamp-"+unstamped.String()[:8], run.KindRerun, uuid.New(), run.ReleaseStamp{}, "alice", model.OperationRun, time.Now())
	require.NoError(t, err)
	stamped, unstamped = withStamp.RunID(), withoutStamp.RunID()
	saveRuns(t, u, withStamp, withoutStamp)

	var seq sql.NullInt64
	require.NoError(t, db.Get(&seq, `SELECT promotion_seq FROM runs WHERE run_id = $1`, stamped))
	assert.Equal(t, sql.NullInt64{Int64: 12, Valid: true}, seq)
	require.NoError(t, db.Get(&seq, `SELECT promotion_seq FROM runs WHERE run_id = $1`, unstamped))
	assert.False(t, seq.Valid)

	read, err := u.Run().GetRun(ctx, pkgevents.DefaultTenantID, stamped)
	require.NoError(t, err)
	assert.Equal(t, stamp, read.Release())
	read, err = u.Run().GetRun(ctx, pkgevents.DefaultTenantID, unstamped)
	require.NoError(t, err)
	assert.True(t, read.Release().IsZero())
}
