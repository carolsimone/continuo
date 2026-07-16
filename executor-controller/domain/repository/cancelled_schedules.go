package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// CancelledSchedulesRepository tracks schedule IDs whose deploys should be
// dropped on receipt. Implementations operate against the cancelled_schedules
// table (id, schedule_id UNIQUE, cancelled_at).
type CancelledSchedulesRepository interface {
	// LockSchedule takes a transaction-scoped lock serializing every path that
	// reads or writes one schedule's cancellation state. Enqueueing a deployment
	// takes it before Exists; cancelling a schedule takes it before Insert. It
	// releases at commit/rollback and must be called inside a transaction.
	//
	// It is what makes the two paths mutually exclusive. Both settle a schedule
	// by reading a table the other writes — the enqueue guard reads
	// cancelled_schedules, the cancel scan reads executor_deployments — so under
	// READ COMMITTED nothing serializes them on its own. A row locking clause on
	// the guard's lookup cannot substitute: it locks only rows that already
	// exist, and in the ordering that matters the cancelled_schedules row is not
	// there to be locked yet. Without the lock an enqueue whose guard has read
	// 'not cancelled' but has not yet committed is invisible to a concurrent
	// cancel's scan, so its deployment outlives the cancellation, gets
	// dispatched, and has its terminal absorbed — stranding its execution slot
	// for good.
	//
	// The lock is always taken before any row lock, and no transaction holding a
	// row lock ever waits for it, so it orders consistently with the capacity
	// lock the dispatcher takes and forms no cycle.
	LockSchedule(ctx context.Context, scheduleID uuid.UUID) error
	Insert(ctx context.Context, scheduleID uuid.UUID) error
	Exists(ctx context.Context, scheduleID uuid.UUID) (bool, error)
	DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error)
}
