package ports

import (
	"context"
	"errors"
	"time"
)

// LeaseKind says what a tenant's backup lease is held for.
type LeaseKind string

const (
	// LeaseBackup is held while catalog backs up a tenant's graph.
	LeaseBackup LeaseKind = "backup"
	// LeaseRestore is held while catalog restores a tenant's graph.
	LeaseRestore LeaseKind = "restore"
)

// ErrLeaseHeld means another backup or restore holds the tenant's lease.
var ErrLeaseHeld = errors.New("another backup or restore holds the lease")

// ErrWritesInFlight means a message's writes span several transactions and sit
// between two of them, so a backup taken now would hold only part of the
// message. The tenant's lease was not taken; the caller tries again later. The
// mark behind it lapses on its own, so a handler that died mid-message holds
// a backup back for a bounded time.
var ErrWritesInFlight = errors.New("a message's writes are in flight")

// ErrLeaseLost means the caller no longer holds the tenant's lease: another
// holder owns it, or it expired.
var ErrLeaseLost = errors.New("the lease is no longer held by this holder")

// BackupLease is the per-tenant lease a backup or a restore holds. While it
// is held (its until lies ahead on the database clock) every catalog write of
// the tenant backs off, so the tenant's graph does not change under the
// backup or restore. A lease nobody renews expires, so a holder that crashed
// pauses the tenant's writes for at most one ttl.
type BackupLease interface {
	// Acquire takes the tenant's lease for holder for ttl. It waits for the
	// tenant's in-flight writes to commit, and returns ErrLeaseHeld while
	// another live lease exists, whoever holds it. A backup (LeaseBackup) also
	// returns ErrWritesInFlight, and takes nothing, while a message sits
	// between two of its writes; a restore (LeaseRestore) ignores that.
	Acquire(ctx context.Context, tenantID string, kind LeaseKind, holder string, ttl time.Duration) error
	// Renew extends holder's live lease to ttl from now. It returns
	// ErrLeaseLost when holder is not the owner of a live lease.
	Renew(ctx context.Context, tenantID, holder string, ttl time.Duration) error
	// Release ends holder's lease. A lease another holder owns is left as it
	// is.
	Release(ctx context.Context, tenantID, holder string) error
}
