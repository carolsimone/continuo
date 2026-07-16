package model_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/stretchr/testify/assert"
)

func testPool() model.WorkerPool {
	return model.WorkerPool{
		PoolKey:          "pool-abc",
		ServiceName:      "finance",
		ImageTag:         "sha-abc",
		CredentialSHA256: "digest",
		LastActivityAt:   time.Unix(0, 0).UTC(),
		CreatedAt:        time.Unix(0, 0).UTC(),
		UpdatedAt:        time.Unix(0, 0).UTC(),
	}
}

// TestWorkerPool_ReadyWhenNoInitializationError pins that readiness is decided
// by the recorded initialization error alone.
func TestWorkerPool_ReadyWhenNoInitializationError(t *testing.T) {
	pool := testPool()
	assert.True(t, pool.Ready())

	pool.InitializationError = "artifact_rejected: bad digest"
	assert.False(t, pool.Ready())
}

// TestWorkerPool_RecordInitializationFailure keeps the reported code and message
// together, so an operator reading the row sees both what failed and why.
func TestWorkerPool_RecordInitializationFailure(t *testing.T) {
	pool := testPool()
	now := time.Unix(1000, 0).UTC()

	pool.RecordInitializationFailure("artifact_rejected", "sha256 mismatch", now)

	assert.False(t, pool.Ready())
	assert.Equal(t, "artifact_rejected: sha256 mismatch", pool.InitializationError)
	assert.Equal(t, now, pool.UpdatedAt)
}

// TestWorkerPool_RecordInitializationFailureWithoutMessage records the code
// alone rather than a code with an empty tail.
func TestWorkerPool_RecordInitializationFailureWithoutMessage(t *testing.T) {
	pool := testPool()

	pool.RecordInitializationFailure("artifact_rejected", "", time.Unix(1000, 0).UTC())

	assert.Equal(t, "artifact_rejected", pool.InitializationError)
	assert.False(t, pool.Ready())
}

// TestWorkerPool_RecordInitializationFailureWithoutCode still records something
// an operator can act on: a pool reported as failed never reads as ready.
func TestWorkerPool_RecordInitializationFailureWithoutCode(t *testing.T) {
	pool := testPool()

	pool.RecordInitializationFailure("", "", time.Unix(1000, 0).UTC())

	assert.False(t, pool.Ready())
	assert.NotEmpty(t, pool.InitializationError)
}

// TestWorkerPool_ClearInitializationError returns a pool to ready once a worker
// reports it hydrated its artifact successfully.
func TestWorkerPool_ClearInitializationError(t *testing.T) {
	pool := testPool()
	pool.InitializationError = "artifact_rejected: sha256 mismatch"
	now := time.Unix(2000, 0).UTC()

	pool.ClearInitializationError(now)

	assert.True(t, pool.Ready())
	assert.Empty(t, pool.InitializationError)
	assert.Equal(t, now, pool.UpdatedAt)
}
