package postgres

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// takeLeasesQuery inserts the tenant's ($1) lease of each claim ($2 node keys,
// $3 tasks, $4 runs, element by element) whose node is free and waited for by
// no task of the tenant but the claiming one, in node-key order, and returns
// the claiming tasks that hold their node's lease: those it inserted, then
// those that held it before the statement (the outer SELECT reads node_leases
// as it was when the statement began, so the two sets never overlap).
//
// Inserting in node-key order keeps two transactions that take overlapping
// nodes from deadlocking: the later one waits for the earlier one on the
// first node they share, before it holds any node the earlier one still
// needs, and skips each node the earlier one committed.
//
// The waiter check reads the tenant's own queue for the node: a pending task
// of another tenant waiting on a node of the same key waits for that tenant's
// lease, and leaves this tenant's node to the claim.
const takeLeasesQuery = `
WITH taken AS (
	INSERT INTO node_leases (tenant_id, node_key, task_id, run_id)
	SELECT $1, k, t, r FROM unnest($2::text[], $3::uuid[], $4::uuid[]) AS c(k, t, r)
	WHERE NOT EXISTS (
		SELECT 1 FROM tasks w
		WHERE w.tenant_id = $1 AND w.node_key = c.k
		  AND w.wait_reason = 'node_lease' AND w.status = 'pending' AND w.task_id <> c.t)
	ORDER BY k
	ON CONFLICT (tenant_id, node_key) DO NOTHING
	RETURNING task_id
)
SELECT task_id FROM taken
UNION ALL
SELECT l.task_id
FROM node_leases l
JOIN unnest($2::text[], $3::uuid[]) AS c(k, t) ON l.node_key = c.k AND l.task_id = c.t
WHERE l.tenant_id = $1`

// takeLeases takes tenantID's write lease on the node of each claim that no
// task holds and no other pending task of the tenant waits for, and returns the claiming
// tasks that hold their node's lease once it has run, a task that already
// held it included. A node another task waits for is left to that waiter, so
// a newly ready task never goes ahead of it.
func takeLeases(ctx context.Context, tx *sqlx.Tx, tenantID string, claims []run.LeaseClaim) ([]uuid.UUID, error) {
	if len(claims) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(claims))
	tasks := make([]uuid.UUID, 0, len(claims))
	runs := make([]uuid.UUID, 0, len(claims))
	for _, c := range claims {
		keys = append(keys, string(c.NodeKey))
		tasks = append(tasks, c.TaskID)
		runs = append(runs, c.RunID)
	}
	var taken []uuid.UUID
	if err := tx.SelectContext(ctx, &taken, takeLeasesQuery, tenantID, pq.Array(keys), pq.Array(tasks), pq.Array(runs)); err != nil {
		return nil, fmt.Errorf("take the leases of %d nodes: %w", len(claims), err)
	}
	return taken, nil
}

// takeLeaseForWaiterQuery is takeLeasesQuery for one claim, without the look
// at the node's other waiters: it reports whether the claiming task holds the
// node's lease, inserted now or held before.
const takeLeaseForWaiterQuery = `
WITH taken AS (
	INSERT INTO node_leases (tenant_id, node_key, task_id, run_id)
	VALUES ($1, $2, $3, $4)
	ON CONFLICT (tenant_id, node_key) DO NOTHING
	RETURNING task_id
)
SELECT EXISTS (SELECT 1 FROM taken)
    OR EXISTS (SELECT 1 FROM node_leases WHERE tenant_id = $1 AND node_key = $2 AND task_id = $3)`

// takeLeaseForWaiter takes tenantID's write lease on the claim's node for the
// claiming task, the node's waiter being served, when no task holds it, and
// reports whether the claiming task holds it.
func takeLeaseForWaiter(ctx context.Context, tx *sqlx.Tx, tenantID string, c run.LeaseClaim) (bool, error) {
	var holds bool
	if err := tx.GetContext(ctx, &holds, takeLeaseForWaiterQuery, tenantID, string(c.NodeKey), c.TaskID, c.RunID); err != nil {
		return false, fmt.Errorf("take the lease of %s for task %s: %w", c.NodeKey, c.TaskID, err)
	}
	return holds, nil
}

// runGrantableWaitsQuery keeps, among the nodes the waiting tasks of the
// tenant's ($1) run $2 wait for, the next waiter of each of the tenant's free
// ones (nextWaiters) when it is a task of the run, in node-key order: the
// order takeLeasesQuery inserts in, so a transaction that grants several of
// them takes their leases in that order too.
const runGrantableWaitsQuery = `
SELECT tenant_id, task_id, run_id, node_key, waiting_since FROM (` + nextWaiters + `
	  AND w.tenant_id = $1
	  AND w.node_key IN (
		SELECT m.node_key FROM tasks m
		WHERE m.tenant_id = $1 AND m.run_id = $2 AND m.status = 'pending' AND m.wait_reason = 'node_lease')` + nextWaitersOrder + `
) oldest
WHERE run_id = $2
ORDER BY node_key`

// grantableWaitsOf returns the tasks of runID that are the next waiter of a
// node tenantID's lease leaves free, in node-key order.
func grantableWaitsOf(ctx context.Context, tx *sqlx.Tx, tenantID string, runID uuid.UUID) ([]run.LeaseWait, error) {
	var rows []leaseWaiterRow
	if err := tx.SelectContext(ctx, &rows, runGrantableWaitsQuery, tenantID, runID); err != nil {
		return nil, fmt.Errorf("read the lease waits of run %s next in line: %w", runID, err)
	}
	out := make([]run.LeaseWait, 0, len(rows))
	for _, row := range rows {
		out = append(out, run.LeaseWait{TaskID: row.TaskID, NodeKey: topology.NodeKey(row.NodeKey), WaitingSince: row.WaitingSince})
	}
	return out, nil
}

// waitForLeases makes each of the tenant's pending tasks of taskIDs wait for
// its node's write lease: wait_reason node_lease and lease_wait_since the
// database clock. A task already waiting keeps the time it began to wait, and
// with it its place among the node's waiters.
func waitForLeases(ctx context.Context, tx *sqlx.Tx, tenantID string, taskIDs []uuid.UUID) error {
	if len(taskIDs) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE tasks
		SET wait_reason = 'node_lease', lease_wait_since = COALESCE(lease_wait_since, now())
		WHERE tenant_id = $2 AND task_id = ANY($1) AND status = 'pending'`,
		pq.Array(taskIDs), tenantID); err != nil {
		return fmt.Errorf("make %d tasks wait for their node leases: %w", len(taskIDs), err)
	}
	return nil
}

// releaseLeases deletes the write leases the tenant's tasks of taskIDs hold; a
// task that holds none is left as it is. A delete that releases a lease
// notifies the node_leases channel when its transaction commits.
func releaseLeases(ctx context.Context, tx *sqlx.Tx, tenantID string, taskIDs []uuid.UUID) error {
	if len(taskIDs) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM node_leases WHERE tenant_id = $2 AND task_id = ANY($1)`,
		pq.Array(taskIDs), tenantID); err != nil {
		return fmt.Errorf("release the leases of %d tasks: %w", len(taskIDs), err)
	}
	return nil
}
