package model

import (
	"time"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
)

// unspecifiedInitializationError stands in when a worker reports a failed
// initialization without naming it. A pool reported as failed must never read
// as ready, so an unnamed failure still records something.
const unspecifiedInitializationError = "unspecified initialization failure"

// WorkerPool registers one pool of reusable worker pods. A pool serves exactly
// one (service, image, runtime manifest, credential) combination, so every
// worker in it can execute any task routed to its PoolKey.
//
// CredentialSHA256 is the SHA-256 digest of the pool's credential; the raw
// credential is held only by the pool's pods and is never stored here, so a
// read of the row cannot impersonate a worker.
type WorkerPool struct {
	PoolKey          string
	ServiceName      string
	ImageTag         string
	RuntimeManifest  pkgmodel.RuntimeManifestRef
	CredentialSHA256 string
	DesiredReplicas  int
	LastActivityAt   time.Time
	// InitializationError is what stopped this pool's workers from hydrating
	// their runtime artifact, or empty when they hydrated it. A pool carrying
	// one is handed no work: its workers cannot execute anything.
	InitializationError string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// Ready reports whether this pool's workers can execute tasks.
func (p *WorkerPool) Ready() bool { return p.InitializationError == "" }

// RecordInitializationFailure marks the pool unable to run work, keeping the
// reported code and message together so an operator sees both what failed and
// why.
func (p *WorkerPool) RecordInitializationFailure(code, message string, now time.Time) {
	p.InitializationError = joinInitializationError(code, message)
	p.UpdatedAt = now
}

// ClearInitializationError returns the pool to ready, which is how a pool
// recovers once a worker hydrates its artifact cleanly.
func (p *WorkerPool) ClearInitializationError(now time.Time) {
	p.InitializationError = ""
	p.UpdatedAt = now
}

// joinInitializationError renders a reported failure as a single line, never
// empty: an empty string means ready, so a failure must not spell one.
func joinInitializationError(code, message string) string {
	switch {
	case code != "" && message != "":
		return code + ": " + message
	case code != "":
		return code
	case message != "":
		return message
	default:
		return unspecifiedInitializationError
	}
}
