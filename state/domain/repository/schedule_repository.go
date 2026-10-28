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

	// Exists reports whether the tenant has a schedule of that name, whether
	// or not its stored cron still parses.
	Exists(ctx context.Context, tenantID, name string) (bool, error)

	// LoadForUpdate is the write path's read: Get inside the open
	// transaction, which locks the schedule's row until the transaction ends.
	// It waits for a transaction that holds the row, then reads the row as
	// that transaction left it. A call outside a transaction is an error.
	LoadForUpdate(ctx context.Context, tenantID, name string) (*schedule.Schedule, bool, error)

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
	// expectedVersion is the version of the row its caller read before
	// changing s, and 0 when it read none. Save writes only while the stored
	// row is at expectedVersion, and otherwise writes nothing and returns
	// schedule.ErrStaleVersion: with a version above 0, a row at another
	// version or no longer there is stale; with 0, Save only inserts, and a
	// row that exists by then is stale. So of two saves that read the same
	// row, or the same absence of one, one writes and the other is stale, and
	// the version of one stored row never repeats or goes back while that row
	// lasts. A row that Delete removes and a later create inserts again starts
	// over at version 1, so an expectedVersion read before the delete can
	// match the new row. Save reads no stored cron, so a save at the version
	// of a row whose cron or timezone no longer parses repairs it.
	Save(ctx context.Context, s *schedule.Schedule, expectedVersion int64) error

	// Delete removes the tenant's schedule of that name and reports whether
	// there was one, whether or not its stored cron still parses.
	Delete(ctx context.Context, tenantID, name string) (bool, error)

	// ListDue returns, oldest next fire first, up to limit schedules of any
	// tenant whose next fire is at or before now. A paused schedule is never
	// due, whatever next fire its row holds. A schedule whose row another
	// transaction holds at that moment, such as one a fire has locked, is left
	// out rather than waited for, so a pass does not queue behind another
	// pass's fire. Outside a transaction ListDue holds no lock once it has
	// returned; inside one, the rows it returns stay locked until the
	// transaction ends. A schedule whose stored cron or timezone no longer
	// parses is left out and logged, not returned, and takes none of limit's
	// places: however many such schedules are due, and however early their
	// next fires, the due schedules that parse are returned, up to limit.
	ListDue(ctx context.Context, now time.Time, limit int) ([]*schedule.Schedule, error)

	// EarliestFireAfter returns the earliest next fire of any tenant's
	// schedules that is strictly after `after`, and false when none has one.
	// A paused schedule counts for none, whatever next fire its row holds.
	// Every other row counts, whether or not its stored cron still parses; a
	// next fire at or before `after` does not.
	EarliestFireAfter(ctx context.Context, after time.Time) (time.Time, bool, error)

	// Count returns how many schedules of every tenant are scheduled, which
	// is every schedule not paused, and how many are paused.
	Count(ctx context.Context) (scheduled, paused int, err error)
}
