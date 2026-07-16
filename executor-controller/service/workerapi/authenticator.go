// Package workerapi authenticates worker pods against the pool they serve and
// records what they report about their own initialization. It is the identity
// half of the internal worker API: which pool a caller is, and whether that
// pool's workers can run anything.
package workerapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
)

// ErrUnauthenticated rejects a caller that presented no valid pool credential:
// the pool is not registered, or the credential is not that pool's. Both answer
// the same, so a caller cannot learn which pools exist by trying keys.
var ErrUnauthenticated = errors.New("unauthenticated")

// Authenticator resolves a caller to the worker pool it serves.
type Authenticator struct {
	pools repository.WorkerPoolRepository
}

// NewAuthenticator constructs the worker authenticator.
func NewAuthenticator(pools repository.WorkerPoolRepository) *Authenticator {
	return &Authenticator{pools: pools}
}

// Authenticate returns the pool registered under poolKey when credential is
// that pool's, and ErrUnauthenticated otherwise. A lookup failure is reported
// as itself: an unreachable database is a transient fault the caller may retry,
// not a rejected credential it never should.
func (a *Authenticator) Authenticate(ctx context.Context, poolKey, credential string) (*model.WorkerPool, error) {
	pool, err := a.pools.Get(ctx, poolKey)
	if err != nil {
		return nil, fmt.Errorf("load worker pool: %w", err)
	}
	if pool == nil {
		return nil, ErrUnauthenticated
	}
	if !VerifyCredential(credential, pool.CredentialSHA256) {
		return nil, ErrUnauthenticated
	}
	return pool, nil
}

// VerifyCredential reports whether raw is the credential whose digest a pool
// stores. Only the digest is ever stored, so a read of the pool row yields
// nothing that authenticates. The comparison is constant time, so a rejected
// credential leaks nothing about how much of it was right.
//
// An unset expectation verifies nothing: a pool row with no credential
// authenticates no caller rather than every caller.
func VerifyCredential(raw string, expectedSHA string) bool {
	if expectedSHA == "" {
		return false
	}
	sum := sha256.Sum256([]byte(raw))
	actual := hex.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(actual), []byte(expectedSHA)) == 1
}
