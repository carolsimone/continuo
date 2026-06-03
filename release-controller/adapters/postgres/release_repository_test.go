//go:build integration

package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("RELEASE_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("RELEASE_TEST_PG_DSN not set")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	_, _ = db.Exec("TRUNCATE releases, current_prod, release_controller_outbox, message_processing RESTART IDENTITY CASCADE")
	return db
}

func TestReleaseRepository_SaveAndGet(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db)
	r := release.New("rA", map[string]string{"s": "t"}, "u", false, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(context.Background(), r))

	got, err := repo.Get(context.Background(), "rA")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "rA", got.ID())
	assert.Equal(t, release.StatusReceived, got.Status())
}

func TestReleaseRepository_BootstrapRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db)
	ctx := context.Background()

	boot := release.New("r-boot", map[string]string{"svc-a": "sha-a"}, "s3://b/r-boot/", true, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, boot))
	plain := release.New("r-plain", map[string]string{"svc-a": "sha-a"}, "s3://b/r-plain/", false, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, plain))

	gotBoot, err := repo.Get(ctx, "r-boot")
	require.NoError(t, err)
	require.NotNil(t, gotBoot)
	assert.True(t, gotBoot.IsBootstrap())

	gotPlain, err := repo.Get(ctx, "r-plain")
	require.NoError(t, err)
	require.NotNil(t, gotPlain)
	assert.False(t, gotPlain.IsBootstrap())
}

func TestReleaseRepository_NextQueuedAndActive(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db)
	older := release.New("rOLD", map[string]string{"s": "t"}, "u", false, time.Unix(100, 0).UTC())
	newer := release.New("rNEW", map[string]string{"s": "t"}, "u", false, time.Unix(200, 0).UTC())
	require.NoError(t, repo.Save(context.Background(), older))
	require.NoError(t, repo.Save(context.Background(), newer))

	q, err := repo.NextQueuedRelease(context.Background())
	require.NoError(t, err)
	require.NotNil(t, q)
	assert.Equal(t, "rOLD", q.ID())

	require.NoError(t, q.TransitionToParsing(time.Unix(300, 0).UTC()))
	require.NoError(t, repo.Save(context.Background(), q))

	a, err := repo.ActiveRelease(context.Background())
	require.NoError(t, err)
	require.NotNil(t, a)
	assert.Equal(t, "rOLD", a.ID())
}

func TestReleaseRepository_ListPaginatesNewestFirst(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db)
	ctx := context.Background()
	for i, id := range []string{"r1", "r2", "r3"} {
		r := release.New(id, map[string]string{"s": "t"}, "u", false, time.Unix(int64(100+i), 0).UTC())
		require.NoError(t, repo.Save(ctx, r))
	}
	page1, next, err := repo.List(ctx, repository.ListFilter{Limit: 2})
	require.NoError(t, err)
	require.Len(t, page1, 2)
	assert.Equal(t, "r3", page1[0].ID())
	assert.Equal(t, "r2", page1[1].ID())
	require.NotNil(t, next)

	page2, next2, err := repo.List(ctx, repository.ListFilter{Limit: 2, Cursor: next})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, "r1", page2[0].ID())
	assert.Nil(t, next2)
}

func TestReleaseRepository_ListTiebreaksByReleaseIDOnEqualTimestamp(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db)
	ctx := context.Background()
	ts := time.Unix(200, 0).UTC()
	ra := release.New("ra", map[string]string{"s": "t"}, "u", false, ts)
	rb := release.New("rb", map[string]string{"s": "t"}, "u", false, ts)
	require.NoError(t, repo.Save(ctx, ra))
	require.NoError(t, repo.Save(ctx, rb))

	page1, next, err := repo.List(ctx, repository.ListFilter{Limit: 1})
	require.NoError(t, err)
	require.Len(t, page1, 1)
	assert.Equal(t, "rb", page1[0].ID(), "higher release_id sorts first under DESC when timestamps are equal")
	require.NotNil(t, next)

	page2, next2, err := repo.List(ctx, repository.ListFilter{Limit: 1, Cursor: next})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, "ra", page2[0].ID())
	assert.Nil(t, next2)
}

func TestReleaseRepository_ListFiltersByStatus(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db)
	ctx := context.Background()
	a := release.New("ra", map[string]string{"s": "t"}, "u", false, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, a))
	b := release.New("rb", map[string]string{"s": "t"}, "u", false, time.Unix(101, 0).UTC())
	require.NoError(t, b.TransitionToParsing(time.Unix(102, 0).UTC()))
	require.NoError(t, b.TransitionToValidating(nil, nil, time.Unix(103, 0).UTC()))
	require.NoError(t, b.TransitionToRejected("validation_failed", []string{"x"}, time.Unix(104, 0).UTC()))
	require.NoError(t, repo.Save(ctx, b))

	rejected := "rejected"
	items, _, err := repo.List(ctx, repository.ListFilter{Status: &rejected, Limit: 10})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "rb", items[0].ID())
}

func TestReleaseRepository_PerNodeResultsRoundTrip(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db)
	ctx := context.Background()
	r := release.New("rp", map[string]string{"s": "t"}, "u", false, time.Unix(100, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(102, 0).UTC()))
	r.RecordValidationResults([]release.NodeValidationResult{{NodeID: "a", Status: "failed", DBTLogURI: "k/a.log", DurationMS: 9}})
	require.NoError(t, r.TransitionToRejected("validation_failed", []string{"a"}, time.Unix(103, 0).UTC()))
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rp")
	require.NoError(t, err)
	require.Len(t, got.PerNodeResults(), 1)
	assert.Equal(t, "k/a.log", got.PerNodeResults()[0].DBTLogURI)
}

func TestReleaseRepository_DeleteResolvedBeforeKeepsCurrentProd(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db)
	ctx := context.Background()
	mkRejected := func(id string, ts int64) {
		r := release.New(id, map[string]string{"s": "t"}, "u", false, time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.TransitionToRejected("validation_failed", nil, time.Unix(ts+3, 0).UTC()))
		require.NoError(t, repo.Save(ctx, r))
	}
	mkPromoted := func(id string, ts int64) {
		r := release.New(id, map[string]string{"s": "t"}, "u", false, time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.TransitionToPromoted(time.Unix(ts+3, 0).UTC()))
		require.NoError(t, repo.Save(ctx, r))
	}
	mkRejected("old-rejected", 100)
	mkPromoted("old-promoted", 100)
	mkRejected("old-keep", 100)
	r := release.New("received-young", map[string]string{"s": "t"}, "u", false, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	cutoff := time.Unix(1000, 0).UTC()
	n, err := repo.DeleteResolvedBefore(ctx, cutoff, "old-keep")
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	gone, _ := repo.Get(ctx, "old-rejected")
	assert.Nil(t, gone)
	promotedGone, _ := repo.Get(ctx, "old-promoted")
	assert.Nil(t, promotedGone, "promoted releases older than cutoff should be deleted")
	kept, _ := repo.Get(ctx, "old-keep")
	assert.NotNil(t, kept)
	young, _ := repo.Get(ctx, "received-young")
	assert.NotNil(t, young)
}
