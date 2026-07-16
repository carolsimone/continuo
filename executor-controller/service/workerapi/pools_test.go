package workerapi_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/service/workerapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubClock reports a fixed instant.
type stubClock struct{ now time.Time }

func (c stubClock) Now() time.Time { return c.now }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newPools(t *testing.T, repo *fakePools) *workerapi.Pools {
	t.Helper()
	return workerapi.NewPools(repo, stubClock{now: time.Unix(5000, 0).UTC()}, testLogger())
}

// TestPools_RecordInitializationFailureMarksThePoolNotReady is what stops a
// pool whose artifact cannot be hydrated from being handed work.
func TestPools_RecordInitializationFailureMarksThePoolNotReady(t *testing.T) {
	repo := newFakePools()
	repo.add(registeredPool())
	pools := newPools(t, repo)

	err := pools.RecordInitialization(context.Background(), workerapi.InitializationReport{
		PoolKey:   "pool-abc",
		OK:        false,
		ErrorCode: "artifact_rejected",
		Message:   "sha256 mismatch",
	})

	require.NoError(t, err)
	stored, err := repo.Get(context.Background(), "pool-abc")
	require.NoError(t, err)
	assert.False(t, stored.Ready())
	assert.Equal(t, "artifact_rejected: sha256 mismatch", stored.InitializationError)
	assert.Equal(t, time.Unix(5000, 0).UTC(), stored.UpdatedAt)
}

// TestPools_RecordInitializationSuccessClearsAPriorError lets a pool recover
// without an operator editing the row: the next worker that hydrates cleanly
// returns the pool to ready.
func TestPools_RecordInitializationSuccessClearsAPriorError(t *testing.T) {
	repo := newFakePools()
	pool := registeredPool()
	pool.InitializationError = "artifact_rejected: sha256 mismatch"
	repo.add(pool)
	pools := newPools(t, repo)

	err := pools.RecordInitialization(context.Background(), workerapi.InitializationReport{
		PoolKey:          "pool-abc",
		OK:               true,
		HydrationSeconds: 4.5,
	})

	require.NoError(t, err)
	stored, err := repo.Get(context.Background(), "pool-abc")
	require.NoError(t, err)
	assert.True(t, stored.Ready())
	assert.Empty(t, stored.InitializationError)
}

// TestPools_RecordInitializationDoesNotWriteBackTheRestOfThePool is the
// boundary a worker's report must not cross. The report says only whether the
// worker hydrated its artifact; a pool's credential is rotated by whoever
// registers the pool. Writing the whole pool back from the read the report
// started with would restore a retired credential digest and let it
// authenticate again.
func TestPools_RecordInitializationDoesNotWriteBackTheRestOfThePool(t *testing.T) {
	repo := newFakePools()
	repo.add(registeredPool())
	rotated := sha256Hex("rotated-credential")
	// The rotation lands after the report's read and before its write.
	repo.afterGet = func() {
		pool := repo.rows["pool-abc"]
		pool.CredentialSHA256 = rotated
		pool.DesiredReplicas = 7
		repo.rows["pool-abc"] = pool
	}
	pools := newPools(t, repo)

	err := pools.RecordInitialization(context.Background(), workerapi.InitializationReport{
		PoolKey:   "pool-abc",
		OK:        false,
		ErrorCode: "artifact_rejected",
		Message:   "sha256 mismatch",
	})

	require.NoError(t, err)
	repo.afterGet = nil
	stored, err := repo.Get(context.Background(), "pool-abc")
	require.NoError(t, err)
	assert.Equal(t, rotated, stored.CredentialSHA256, "the report must not restore the rotated-out credential")
	assert.Equal(t, 7, stored.DesiredReplicas)
	// The report's own field still lands.
	assert.Equal(t, "artifact_rejected: sha256 mismatch", stored.InitializationError)
}

// TestPools_RecordInitializationForAnUnknownPool fails rather than inventing a
// pool row from an unauthenticated-looking report.
func TestPools_RecordInitializationForAnUnknownPool(t *testing.T) {
	repo := newFakePools()
	pools := newPools(t, repo)

	err := pools.RecordInitialization(context.Background(), workerapi.InitializationReport{
		PoolKey: "pool-missing",
		OK:      true,
	})

	require.Error(t, err)
	assert.Empty(t, repo.saved)
}
