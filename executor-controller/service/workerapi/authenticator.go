// Package workerapi authenticates worker pods against the pool they serve and
// records what they report about their own initialization. It is the identity
// half of the internal worker API: which pool a caller is, and whether that
// pool's workers can run anything.
package workerapi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
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

// credentialBytes is how much entropy a pool credential carries. 32 bytes is
// 256 bits, which is not guessable and not worth trying to guess.
const credentialBytes = 32

// NewCredential mints a pool credential.
//
// It is base64url-encoded so the value is safe to carry in an HTTP header and in
// an environment variable without any escaping, which is where it travels: the
// pod reads it from its environment and presents it as a bearer token.
//
// The raw value is returned to exactly one caller, which places it in the pool's
// Secret and forgets it. Only HashCredential's digest is ever stored, so the
// value this returns is the only copy that will exist outside the cluster's
// Secret — it must never be logged or written anywhere else.
func NewCredential() (string, error) {
	buf := make([]byte, credentialBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate pool credential: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// HashCredential digests a raw pool credential into the value a pool row stores.
//
// It is the single definition of that mapping: whoever mints a credential and
// whoever verifies one both come through here, so the stored digest and the
// check against it cannot drift into disagreeing — which would present as every
// worker in a pool failing to authenticate.
func HashCredential(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
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
	return subtle.ConstantTimeCompare([]byte(HashCredential(raw)), []byte(expectedSHA)) == 1
}
