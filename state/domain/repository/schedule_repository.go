package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/state/domain/schedule"
)

// ScheduleRepository stores the schedules state fires. A schedule belongs to
// its tenant: the same name in two tenants is two schedules. Inside a
// transaction a call sees, and a write belongs to, that transaction.
type ScheduleRepository interface {
	// Get returns the tenant's schedule of that name and whether there is one.
	// A schedule whose stored cron or timezone no longer parses is a
	// *schedule.StoredCronInvalidError (schedule.ErrStoredCronInvalid to
	// errors.Is) carrying the row's version, so a lookup fails loudly and an
	// edit can still save over the row.
	Get(ctx context.Context, tenantID, name string) (*schedule.Schedule, bool, error)

	// GetForUpdate is Get inside the open transaction, and locks the
	// schedule's row until the transaction ends: it waits for a transaction
	// that holds the row, then reads the row as that transaction left it. A
	// call outside a transaction is an error.
	GetForUpdate(ctx context.Context, tenantID, name string) (*schedule.Schedule, bool, error)

	// List returns the tenant's schedules, ordered by name. A schedule whose
	// stored cron or timezone no longer parses is left out and logged, not
	// returned.
	List(ctx context.Context, tenantID string) ([]*schedule.Schedule, error)

	// ListStoredCronInvalid returns, ordered by name, the names of the
	// tenant's schedules whose stored cron or timezone no longer parses: the
	// ones List leaves out. It logs nothing per schedule.
	ListStoredCronInvalid(ctx context.Context, tenantID string) ([]string, error)

	// Save stores s exactly as it stands, its version, author and edit time
	// included: the aggregate counts its own versions, and a save of a fire
	// leaves them as they were.
	//
	// With expectedVersion above 0, Save writes only while the stored row is
	// at expectedVersion, the version its caller read before editing, and
	// otherwise writes nothing and returns schedule.ErrStaleVersion; a row
	// that no longer exists is stale too. With expectedVersion 0 there is no
	// check: Save inserts the schedule when it has no row and overwrites the
	// row when it has one, so of two saves that create the same schedule the
	// later one wins, and neither fails. Save reads no stored cron, so a save
	// over a row whose cron or timezone no longer parses repairs it.
	Save(ctx context.Context, s *schedule.Schedule, expectedVersion int64) error

	// Delete removes the tenant's schedule of that name and reports whether
	// there was one, whether or not its stored cron still parses.
	Delete(ctx context.Context, tenantID, name string) (bool, error)

	// LockDue returns, oldest next fire first, up to limit schedules of any
	// tenant whose next fire is at or before now, and locks them until the
	// transaction ends. A schedule another transaction has locked is left out
	// rather than waited for, so concurrent passes take different schedules. A
	// schedule whose stored cron or timezone no longer parses is left out and
	// logged, not returned, so one bad row does not stop the others; its row
	// stays locked and counts toward limit.
	LockDue(ctx context.Context, now time.Time, limit int) ([]*schedule.Schedule, error)

	// EarliestFire returns the earliest next fire of any tenant's schedules,
	// and false when none has one: a paused schedule has none.
	EarliestFire(ctx context.Context) (time.Time, bool, error)

	// Count returns how many schedules of every tenant are scheduled, which
	// is every schedule not paused, and how many are paused.
	Count(ctx context.Context) (scheduled, paused int, err error)
}
