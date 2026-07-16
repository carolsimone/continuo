package workerapi_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/executor-controller/service/workerapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// fakePools is a map-backed WorkerPoolRepository.
type fakePools struct {
	repository.WorkerPoolRepository

	rows   map[string]model.WorkerPool
	getErr error
	saved  []model.WorkerPool
}

func newFakePools() *fakePools {
	return &fakePools{rows: map[string]model.WorkerPool{}}
}

func (p *fakePools) add(pool model.WorkerPool) { p.rows[pool.PoolKey] = pool }

func (p *fakePools) Get(_ context.Context, poolKey string) (*model.WorkerPool, error) {
	if p.getErr != nil {
		return nil, p.getErr
	}
	pool, ok := p.rows[poolKey]
	if !ok {
		return nil, nil
	}
	return &pool, nil
}

func (p *fakePools) Save(_ context.Context, pool model.WorkerPool) error {
	p.rows[pool.PoolKey] = pool
	p.saved = append(p.saved, pool)
	return nil
}

var _ repository.WorkerPoolRepository = (*fakePools)(nil)

func registeredPool() model.WorkerPool {
	return model.WorkerPool{
		PoolKey:          "pool-abc",
		ServiceName:      "finance",
		ImageTag:         "sha-abc",
		CredentialSHA256: sha256Hex("correct-credential"),
		LastActivityAt:   time.Unix(0, 0).UTC(),
	}
}

// TestVerifyCredential_MatchesOnlyTheRegisteredSecret is the core of the auth
// boundary: only the raw secret whose digest the pool stores verifies.
func TestVerifyCredential_MatchesOnlyTheRegisteredSecret(t *testing.T) {
	expected := sha256Hex("correct-credential")

	assert.True(t, workerapi.VerifyCredential("correct-credential", expected))
	assert.False(t, workerapi.VerifyCredential("wrong-credential", expected))
	assert.False(t, workerapi.VerifyCredential("", expected))
	// The stored digest is never a raw secret: presenting it must not verify.
	assert.False(t, workerapi.VerifyCredential(expected, expected))
}

// TestVerifyCredential_RejectsAnUnsetDigest stops a pool row with no credential
// from authenticating every caller, including one presenting nothing.
func TestVerifyCredential_RejectsAnUnsetDigest(t *testing.T) {
	assert.False(t, workerapi.VerifyCredential("anything", ""))
	assert.False(t, workerapi.VerifyCredential("", ""))
}

func TestAuthenticator_AcceptsTheRegisteredCredential(t *testing.T) {
	pools := newFakePools()
	pools.add(registeredPool())
	auth := workerapi.NewAuthenticator(pools)

	pool, err := auth.Authenticate(context.Background(), "pool-abc", "correct-credential")

	require.NoError(t, err)
	require.NotNil(t, pool)
	assert.Equal(t, "pool-abc", pool.PoolKey)
	assert.Equal(t, "finance", pool.ServiceName)
}

// TestAuthenticator_RejectsAWrongCredential is the test that must fail if the
// credential check is removed.
func TestAuthenticator_RejectsAWrongCredential(t *testing.T) {
	pools := newFakePools()
	pools.add(registeredPool())
	auth := workerapi.NewAuthenticator(pools)

	pool, err := auth.Authenticate(context.Background(), "pool-abc", "wrong-credential")

	assert.Nil(t, pool)
	assert.ErrorIs(t, err, workerapi.ErrUnauthenticated)
}

// TestAuthenticator_RejectsAnUnknownPool answers an unregistered pool with the
// same error a wrong credential earns, so a caller cannot enumerate pools.
func TestAuthenticator_RejectsAnUnknownPool(t *testing.T) {
	pools := newFakePools()
	pools.add(registeredPool())
	auth := workerapi.NewAuthenticator(pools)

	pool, err := auth.Authenticate(context.Background(), "pool-unknown", "correct-credential")

	assert.Nil(t, pool)
	assert.ErrorIs(t, err, workerapi.ErrUnauthenticated)
}

func TestAuthenticator_RejectsAnEmptyCredential(t *testing.T) {
	pools := newFakePools()
	pools.add(registeredPool())
	auth := workerapi.NewAuthenticator(pools)

	_, err := auth.Authenticate(context.Background(), "pool-abc", "")

	assert.ErrorIs(t, err, workerapi.ErrUnauthenticated)
}

// TestAuthenticator_ReportsALookupFailureSeparately keeps a database outage
// from reading as a rejected credential: one is retriable, the other is not.
func TestAuthenticator_ReportsALookupFailureSeparately(t *testing.T) {
	pools := newFakePools()
	pools.getErr = errors.New("connection refused")
	auth := workerapi.NewAuthenticator(pools)

	_, err := auth.Authenticate(context.Background(), "pool-abc", "correct-credential")

	require.Error(t, err)
	assert.NotErrorIs(t, err, workerapi.ErrUnauthenticated)
}
