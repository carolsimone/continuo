package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/stretchr/testify/require"
)

// TestCatalogRepositoryAdapter_LoadForUpdate_SerialisesOnEmptyCatalog proves the
// reconcile advisory lock holds when schedule_catalog is empty — the case where
// SELECT … FOR UPDATE locks nothing. A second reconciler's LoadCatalogForUpdate
// must block until the first transaction releases, so two replicas can never
// both read an empty catalog and upsert a divergent snapshot.
func TestCatalogRepositoryAdapter_LoadForUpdate_SerialisesOnEmptyCatalog(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	repo := postgres.NewScheduleCatalogRepository(db, discardLogger())

	// Guarantee the empty-table case this test is about.
	_, err := db.ExecContext(ctx, "DELETE FROM schedule_catalog")
	require.NoError(t, err)

	tx1, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx1.Rollback()

	// First reconciler takes the advisory lock.
	_, err = postgres.NewCatalogRepositoryAdapter(db, tx1, repo, discardLogger()).LoadCatalogForUpdate(ctx)
	require.NoError(t, err)

	// Second reconciler should block on LoadCatalogForUpdate until tx1 ends.
	done := make(chan error, 1)
	go func() {
		tx2, err := db.BeginTxx(ctx, nil)
		if err != nil {
			done <- err
			return
		}
		defer tx2.Rollback()
		_, err = postgres.NewCatalogRepositoryAdapter(db, tx2, repo, discardLogger()).LoadCatalogForUpdate(ctx)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("second LoadCatalogForUpdate must block while tx1 holds the lock, but returned early: %v", err)
	case <-time.After(500 * time.Millisecond):
		// Still blocked — expected.
	}

	require.NoError(t, tx1.Rollback()) // release the advisory lock

	select {
	case err := <-done:
		require.NoError(t, err, "second reconciler should proceed once the lock is released")
	case <-time.After(3 * time.Second):
		t.Fatal("second LoadCatalogForUpdate did not proceed after the lock was released")
	}
}
