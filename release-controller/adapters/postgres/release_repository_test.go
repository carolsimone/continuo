//go:build integration

package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/domain/release"
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
	r := release.New("rA", []string{"a"}, map[string]string{"s": "t"}, "u", time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(context.Background(), r))

	got, err := repo.Get(context.Background(), "rA")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "rA", got.ID())
	assert.Equal(t, release.StatusReceived, got.Status())
	assert.Equal(t, []string{"a"}, got.ChangedNodeIDs())
}

func TestReleaseRepository_NextQueuedAndActive(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db)
	older := release.New("rOLD", []string{"a"}, map[string]string{"s": "t"}, "u", time.Unix(100, 0).UTC())
	newer := release.New("rNEW", []string{"a"}, map[string]string{"s": "t"}, "u", time.Unix(200, 0).UTC())
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
