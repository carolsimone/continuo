//go:build integration

package postgres_test

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPromotionSequenceRepository_NextIncrements(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewPromotionSequenceRepository(db)

	first, err := repo.Next(context.Background())
	require.NoError(t, err)
	second, err := repo.Next(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(1), first)
	assert.Equal(t, int64(2), second)
}

// A seq taken in a transaction that rolls back is not consumed: the next
// caller takes the same number.
func TestPromotionSequenceRepository_RolledBackSeqIsTakenAgain(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	seq, err := postgres.NewPromotionSequenceRepository(tx).Next(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), seq)
	require.NoError(t, tx.Rollback())

	again, err := postgres.NewPromotionSequenceRepository(db).Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), again)
}

// Concurrent transactions serialise on the sequence row: every committed
// caller gets its own number, with no gap and no repeat.
func TestPromotionSequenceRepository_ConcurrentCallersGetDistinctSeqs(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	const callers = 8

	seqs := make(chan int64, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := db.BeginTxx(ctx, nil)
			if err != nil {
				t.Error(err)
				return
			}
			seq, err := postgres.NewPromotionSequenceRepository(tx).Next(ctx)
			if err != nil {
				_ = tx.Rollback()
				t.Error(err)
				return
			}
			if err := tx.Commit(); err != nil {
				t.Error(err)
				return
			}
			seqs <- seq
		}()
	}
	wg.Wait()
	close(seqs)

	got := make([]int64, 0, callers)
	for s := range seqs {
		got = append(got, s)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8}, got)
}
