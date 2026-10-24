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

// LeaseRepository implements repository.LeaseRepository for one tenant. Each
// method is one statement, which runs in the unit of work's transaction when
// one is open, else on the autocommit db.
type LeaseRepository struct {
	db       *sqlx.DB
	tx       *sqlx.Tx
	tenantID string
}

var _ repository.LeaseRepository = (*LeaseRepository)(nil)

// NewLeaseRepository binds the repository to db, tx (nil outside a
// transaction) and tenantID, whose leases and waiters it serves.
func NewLeaseRepository(db *sqlx.DB, tx *sqlx.Tx, tenantID string) *LeaseRepository {
	return &LeaseRepository{db: db, tx: tx, tenantID: tenantID}
}

func (r *LeaseRepository) conn() sqlx.ExtContext {
	if r.tx != nil {
		return r.tx
	}
	return r.db
}

// tenantWaitersFrom names the pending tasks waiting for a node lease whose node
// the tenant ($1) holds in their run's release: task_tracker has no tenant
// column, so the topology row of the task's node is what makes a waiter the
// tenant's, as it is what a grant reads to dispatch it. The waiters of a node
// are read through idx_task_tracker_lease_waiters.
const tenantWaitersFrom = `
	FROM task_tracker w
	JOIN scheduler_tracker s ON s.schedule_id = w.schedule_id
	JOIN topology_nodes n ON n.tenant_id = $1 AND n.release_id = s.release_id AND n.node_key = w.node_key
	WHERE w.wait_reason = 'node_lease' AND w.status = 'pending'`

// grantableQuery keeps, for each node of the tenant no task holds, the oldest
// waiter whose run is running, then orders those by how long they have waited
// and keeps the first $2.
const grantableQuery = `
SELECT task_id, run_id, node_key, waiting_since FROM (
	SELECT DISTINCT ON (w.node_key) w.task_id, w.schedule_id AS run_id, w.node_key, w.lease_wait_since AS waiting_since` +
	tenantWaitersFrom + `
	  AND s.status = 'running'
	  AND NOT EXISTS (SELECT 1 FROM node_leases l WHERE l.tenant_id = $1 AND l.node_key = w.node_key)
	ORDER BY w.node_key, w.lease_wait_since, w.task_id
) oldest
ORDER BY waiting_since, task_id
LIMIT $2`

type leaseWaiterRow struct {
	TaskID       uuid.UUID `db:"task_id"`
	RunID        uuid.UUID `db:"run_id"`
	NodeKey      string    `db:"node_key"`
	WaitingSince time.Time `db:"waiting_since"`
}

// Grantable reads the oldest waiter of each free node of the tenant whose run
// is running, the longest waiting first.
func (r *LeaseRepository) Grantable(ctx context.Context, limit int) ([]repository.LeaseWaiter, error) {
	var rows []leaseWaiterRow
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, grantableQuery, r.tenantID, limit); err != nil {
		return nil, fmt.Errorf("read the grantable lease waiters: %w", err)
	}
	out := make([]repository.LeaseWaiter, 0, len(rows))
	for _, row := range rows {
		out = append(out, repository.LeaseWaiter{
			TaskID: row.TaskID, RunID: row.RunID, NodeKey: topology.NodeKey(row.NodeKey), WaitingSince: row.WaitingSince,
		})
	}
	return out, nil
}

type repairedLeaseRow struct {
	TaskID     uuid.UUID `db:"task_id"`
	RunID      uuid.UUID `db:"run_id"`
	NodeKey    string    `db:"node_key"`
	TaskStatus string    `db:"task_status"`
	AcquiredAt time.Time `db:"acquired_at"`
}

// RepairEnded deletes the tenant's leases whose holder task has ended. A
// delete that removes a lease notifies NodeLeasesChannel when it commits.
func (r *LeaseRepository) RepairEnded(ctx context.Context) ([]repository.RepairedLease, error) {
	var rows []repairedLeaseRow
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, `
		DELETE FROM node_leases l
		USING task_tracker t
		WHERE l.tenant_id = $1 AND t.task_id = l.task_id AND t.status = ANY($2::text[])
		RETURNING l.task_id, l.run_id, l.node_key, t.status AS task_status, l.acquired_at`,
		r.tenantID, pq.Array(endedTaskStatuses())); err != nil {
		return nil, fmt.Errorf("delete the leases of ended tasks: %w", err)
	}
	out := make([]repository.RepairedLease, 0, len(rows))
	for _, row := range rows {
		out = append(out, repository.RepairedLease{
			TaskID: row.TaskID, RunID: row.RunID, NodeKey: topology.NodeKey(row.NodeKey),
			TaskStatus: run.TaskStatus(row.TaskStatus), AcquiredAt: row.AcquiredAt,
		})
	}
	return out, nil
}

// CountWaiters counts the tenant's tasks that wait for a node lease.
func (r *LeaseRepository) CountWaiters(ctx context.Context) (int, error) {
	var n int
	if err := sqlx.GetContext(ctx, r.conn(), &n, `SELECT count(*)`+tenantWaitersFrom, r.tenantID); err != nil {
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
