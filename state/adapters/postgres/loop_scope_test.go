package postgres_test

import (
	"context"
	"slices"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// tenantsOnly is a unit of work whose loop scans keep only the rows of the
// tenants it names, so a background loop under test acts on its own test's
// rows of the shared database and its waiter gauge counts only them. Every
// scan still runs the real statement over every tenant, and every step a loop
// takes for one row is the real one.
type tenantsOnly struct {
	*postgres.PostgresUnitOfWork
	db      *sqlx.DB
	tenants []string
}

// scopedUoW returns a unit-of-work factory over db whose loop scans keep only
// the rows of tenants.
func scopedUoW(db *sqlx.DB, tenants ...string) func() uow.UnitOfWork {
	return func() uow.UnitOfWork {
		return tenantsOnly{PostgresUnitOfWork: newPlanningUoW(db), db: db, tenants: tenants}
	}
}

func (u tenantsOnly) Attempts() repository.AttemptRepository {
	return scopedAttempts{AttemptRepository: u.PostgresUnitOfWork.Attempts(), tenants: u.tenants}
}

func (u tenantsOnly) Leases() repository.LeaseRepository {
	return scopedLeases{LeaseRepository: u.PostgresUnitOfWork.Leases(), db: u.db, tenants: u.tenants}
}

// scopedAttempts keeps the due attempts, stalled runs and due stops of its
// tenants.
type scopedAttempts struct {
	repository.AttemptRepository
	tenants []string
}

func (a scopedAttempts) DueAttempts(ctx context.Context, limit int) ([]repository.AttemptKey, error) {
	all, err := a.AttemptRepository.DueAttempts(ctx, limit)
	return keep(all, func(k repository.AttemptKey) string { return k.TenantID }, a.tenants), err
}

func (a scopedAttempts) StalledRuns(ctx context.Context, idle time.Duration, limit int) ([]repository.RunRef, error) {
	all, err := a.AttemptRepository.StalledRuns(ctx, idle, limit)
	return keep(all, func(r repository.RunRef) string { return r.TenantID }, a.tenants), err
}

func (a scopedAttempts) DueStops(ctx context.Context, limit int) ([]repository.RunRef, error) {
	all, err := a.AttemptRepository.DueStops(ctx, limit)
	return keep(all, func(r repository.RunRef) string { return r.TenantID }, a.tenants), err
}

// scopedLeases keeps the grantable waiters and repaired leases of its tenants
// and counts only their waiters. A repair still deletes the ended leases of
// every tenant, as it always may.
type scopedLeases struct {
	repository.LeaseRepository
	db      *sqlx.DB
	tenants []string
}

func (l scopedLeases) Grantable(ctx context.Context, limit int) ([]repository.LeaseWaiter, error) {
	all, err := l.LeaseRepository.Grantable(ctx, limit)
	return keep(all, func(w repository.LeaseWaiter) string { return w.TenantID }, l.tenants), err
}

func (l scopedLeases) RepairEnded(ctx context.Context) ([]repository.RepairedLease, error) {
	all, err := l.LeaseRepository.RepairEnded(ctx)
	return keep(all, func(r repository.RepairedLease) string { return r.TenantID }, l.tenants), err
}

func (l scopedLeases) CountWaiters(ctx context.Context) (int, error) {
	var n int
	err := l.db.GetContext(ctx, &n, `
		SELECT count(*) FROM tasks
		WHERE tenant_id = ANY($1) AND wait_reason = 'node_lease' AND status = 'pending'`, pq.Array(l.tenants))
	return n, err
}

// keep returns the items of all whose tenant is one of tenants, in order.
func keep[T any](all []T, tenantOf func(T) string, tenants []string) []T {
	out := make([]T, 0, len(all))
	for _, item := range all {
		if slices.Contains(tenants, tenantOf(item)) {
			out = append(out, item)
		}
	}
	return out
}
