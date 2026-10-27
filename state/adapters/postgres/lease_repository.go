package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// NodeLeasesChannel is the Postgres channel node_leases' statement trigger
// notifies when a statement deletes at least one lease, once its transaction
// commits.
const NodeLeasesChannel = "node_leases"

// LeaseRepository implements repository.LeaseRepository across every tenant.
// Each method is one statement, which runs in the unit of work's transaction
// when one is open, else on the autocommit db.
type LeaseRepository struct {
	db *sqlx.DB
	tx *sqlx.Tx
}

var _ repository.LeaseRepository = (*LeaseRepository)(nil)

// NewLeaseRepository binds the repository to db and tx (nil outside a
// transaction).
func NewLeaseRepository(db *sqlx.DB, tx *sqlx.Tx) *LeaseRepository {
	return &LeaseRepository{db: db, tx: tx}
}

func (r *LeaseRepository) conn() sqlx.ExtContext {
	if r.tx != nil {
		return r.tx
	}
	return r.db
}

// waitersFrom names the pending tasks of every tenant waiting for a node
// lease, each with its run. A query that adds a condition on w.tenant_id reads
// one tenant's waiters of a node through idx_tasks_lease_waiters.
const waitersFrom = `
	FROM tasks w
	JOIN runs s ON s.tenant_id = w.tenant_id AND s.run_id = w.run_id
	WHERE w.wait_reason = 'node_lease' AND w.status = 'pending'`

// nextWaiters selects, with nextWaitersOrder closing it, for each node of each
// tenant that no task of the tenant holds, its oldest waiter of the tenant
// whose run is running: the earliest lease_wait_since, then the lowest task
// id. That waiter is the one a grant serves; runWorkInFlight queues a node's
// waiters in the same order. A query may add a condition on w between the two.
const nextWaiters = `
	SELECT DISTINCT ON (w.tenant_id, w.node_key) w.tenant_id, w.task_id, w.run_id, w.node_key,
	       w.lease_wait_since AS waiting_since` +
	waitersFrom + `
	  AND s.status = 'running'
	  AND NOT EXISTS (SELECT 1 FROM node_leases l WHERE l.tenant_id = w.tenant_id AND l.node_key = w.node_key)`

// nextWaitersOrder closes nextWaiters: DISTINCT ON keeps the first row of each
// tenant's node in this order.
const nextWaitersOrder = `
	ORDER BY w.tenant_id, w.node_key, w.lease_wait_since, w.task_id`

// grantableQuery keeps the next waiter of each free node of every tenant
// (nextWaiters), then orders those by how long they have waited and keeps the
// first $1.
const grantableQuery = `
SELECT tenant_id, task_id, run_id, node_key, waiting_since FROM (` + nextWaiters + nextWaitersOrder + `
) oldest
ORDER BY waiting_since, tenant_id, task_id
LIMIT $1`

type leaseWaiterRow struct {
	TenantID     string    `db:"tenant_id"`
	TaskID       uuid.UUID `db:"task_id"`
	RunID        uuid.UUID `db:"run_id"`
	NodeKey      string    `db:"node_key"`
	WaitingSince time.Time `db:"waiting_since"`
}

// Grantable reads the oldest waiter of each free node of every tenant whose
// run is running, the longest waiting first.
func (r *LeaseRepository) Grantable(ctx context.Context, limit int) ([]repository.LeaseWaiter, error) {
	var rows []leaseWaiterRow
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, grantableQuery, limit); err != nil {
		return nil, fmt.Errorf("read the grantable lease waiters: %w", err)
	}
	out := make([]repository.LeaseWaiter, 0, len(rows))
	for _, row := range rows {
		out = append(out, repository.LeaseWaiter{
			TenantID: row.TenantID, TaskID: row.TaskID, RunID: row.RunID, NodeKey: topology.NodeKey(row.NodeKey),
			WaitingSince: row.WaitingSince,
		})
	}
	return out, nil
}

type repairedLeaseRow struct {
	TenantID   string    `db:"tenant_id"`
	TaskID     uuid.UUID `db:"task_id"`
	RunID      uuid.UUID `db:"run_id"`
	NodeKey    string    `db:"node_key"`
	TaskStatus string    `db:"task_status"`
	AcquiredAt time.Time `db:"acquired_at"`
}

// RepairEnded deletes the leases of every tenant whose holder task has ended.
// A delete that removes a lease notifies NodeLeasesChannel when it commits.
func (r *LeaseRepository) RepairEnded(ctx context.Context) ([]repository.RepairedLease, error) {
	var rows []repairedLeaseRow
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, `
		DELETE FROM node_leases l
		USING tasks t
		WHERE t.tenant_id = l.tenant_id AND t.task_id = l.task_id
		  AND t.status = ANY($1::text[])
		RETURNING l.tenant_id, l.task_id, l.run_id, l.node_key, t.status AS task_status, l.acquired_at`,
		pq.Array(endedTaskStatuses())); err != nil {
		return nil, fmt.Errorf("delete the leases of ended tasks: %w", err)
	}
	out := make([]repository.RepairedLease, 0, len(rows))
	for _, row := range rows {
		out = append(out, repository.RepairedLease{
			TenantID: row.TenantID, TaskID: row.TaskID, RunID: row.RunID, NodeKey: topology.NodeKey(row.NodeKey),
			TaskStatus: run.TaskStatus(row.TaskStatus), AcquiredAt: row.AcquiredAt,
		})
	}
	return out, nil
}

// CountWaiters counts the tasks of every tenant that wait for a node lease.
func (r *LeaseRepository) CountWaiters(ctx context.Context) (int, error) {
	var n int
	if err := sqlx.GetContext(ctx, r.conn(), &n, `SELECT count(*)`+waitersFrom); err != nil {
		return 0, fmt.Errorf("count the lease waiters: %w", err)
	}
	return n, nil
}

// endedTaskStatuses returns the domain's statuses of a task that has ended
// (succeeded, failed, cancelled and skipped) as column values.
func endedTaskStatuses() []string {
	var out []string
	for _, s := range run.TaskStatuses() {
		if s.IsTerminal() {
			out = append(out, string(s))
		}
	}
	return out
}
