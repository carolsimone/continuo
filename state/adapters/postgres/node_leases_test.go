package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// leaseTenant returns a tenant no other test uses and deletes its leases when
// the test ends.
func leaseTenant(t *testing.T, db *sqlx.DB) string {
	t.Helper()
	tenant := "lease-" + uuid.NewString()
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM node_leases WHERE tenant_id = $1`, tenant)
	})
	return tenant
}

// leaseTables returns tables named after a prefix no other test uses, one per
// suffix, so the node keys of a test's tasks are its own.
func leaseTables(suffixes ...string) []string {
	prefix := "lease_" + uuid.NewString()[:8]
	out := make([]string, 0, len(suffixes))
	for _, s := range suffixes {
		out = append(out, prefix+"_"+s)
	}
	return out
}

// pendingTasks stores a run holding one pending, undispatched task per table
// and returns the run id and each task's id.
func pendingTasks(t *testing.T, db *sqlx.DB, tables ...string) (uuid.UUID, map[string]uuid.UUID) {
	t.Helper()
	statuses := make(map[string]run.TaskStatus, len(tables))
	for _, table := range tables {
		statuses[table] = run.TaskStatusPending
	}
	return insertTasks(t, db, statuses)
}

// claim is the lease claim of the task of runID at table.
func claim(runID, taskID uuid.UUID, table string) run.LeaseClaim {
	return run.LeaseClaim{TaskID: taskID, RunID: runID, NodeKey: topology.NodeKey("e2e_schema." + table)}
}

// inLeaseTx runs fn with a TaskCollection of tenant bound to a transaction it
// commits.
func inLeaseTx(t *testing.T, db *sqlx.DB, tenant string, fn func(tasks run.TaskCollection)) {
	t.Helper()
	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	fn(postgres.NewTaskCollectionAdapter(postgres.NewTaskTrackerRepository(db, discardLogger()), tx, tenant))
	require.NoError(t, tx.Commit())
}

// takeLeases takes the leases of claims for tenant in a transaction of its
// own and returns the tasks that hold their node's lease.
func takeLeases(t *testing.T, db *sqlx.DB, tenant string, claims ...run.LeaseClaim) []uuid.UUID {
	t.Helper()
	var taken []uuid.UUID
	inLeaseTx(t, db, tenant, func(tasks run.TaskCollection) {
		var err error
		taken, err = tasks.TakeLeases(context.Background(), claims)
		require.NoError(t, err)
	})
	return taken
}

// heldLease is one node_leases row as these tests read it.
type heldLease struct {
	TaskID uuid.UUID `db:"task_id"`
	RunID  uuid.UUID `db:"run_id"`
}

// leaseOn returns the lease tenant holds on the node of table; ok is false
// when no task holds it.
func leaseOn(t *testing.T, db *sqlx.DB, tenant, table string) (heldLease, bool) {
	t.Helper()
	var rows []heldLease
	require.NoError(t, db.Select(&rows, `SELECT task_id, run_id FROM node_leases WHERE tenant_id = $1 AND node_key = $2`,
		tenant, "e2e_schema."+table))
	if len(rows) == 0 {
		return heldLease{}, false
	}
	return rows[0], true
}

// leaseOf reports whether the task holds a lease.
func leaseOf(t *testing.T, db *sqlx.DB, taskID uuid.UUID) bool {
	t.Helper()
	var held bool
	require.NoError(t, db.Get(&held, `SELECT EXISTS (SELECT 1 FROM node_leases WHERE task_id = $1)`, taskID))
	return held
}

// leaseWait is what a task_tracker row says a task waits for.
type leaseWait struct {
	Status run.TaskStatus `db:"status"`
	Reason *string        `db:"wait_reason"`
	Since  *time.Time     `db:"lease_wait_since"`
}

func waitOf(t *testing.T, db *sqlx.DB, taskID uuid.UUID) leaseWait {
	t.Helper()
	var w leaseWait
	require.NoError(t, db.Get(&w, `SELECT status, wait_reason, lease_wait_since FROM task_tracker WHERE task_id = $1`, taskID))
	return w
}

// A take gets every claimed node that is free, leaves a node another task
// holds to it, and counts a claiming task that already holds its node as
// holding it; each lease names its task's run.
func TestNodeLeases_TakesFreeNodesAndLeavesHeldOnes(t *testing.T) {
	db := newTestDB(t)
	tenant := leaseTenant(t, db)
	tables := leaseTables("x", "y", "z")
	x, y, z := tables[0], tables[1], tables[2]
	holderRun, holder := pendingTasks(t, db, x)
	runID, ids := pendingTasks(t, db, x, y, z)
	require.NoError(t, insertLease(db, tenant, "e2e_schema."+x, holder[x], holderRun))
	require.NoError(t, insertLease(db, tenant, "e2e_schema."+z, ids[z], runID))

	taken := takeLeases(t, db, tenant, claim(runID, ids[z], z), claim(runID, ids[y], y), claim(runID, ids[x], x))

	assert.ElementsMatch(t, []uuid.UUID{ids[y], ids[z]}, taken)
	lx, ok := leaseOn(t, db, tenant, x)
	require.True(t, ok)
	assert.Equal(t, heldLease{TaskID: holder[x], RunID: holderRun}, lx, "a held node stays with its holder")
	ly, ok := leaseOn(t, db, tenant, y)
	require.True(t, ok)
	assert.Equal(t, heldLease{TaskID: ids[y], RunID: runID}, ly, "a free node is taken for its claiming task's run")
	lz, ok := leaseOn(t, db, tenant, z)
	require.True(t, ok)
	assert.Equal(t, heldLease{TaskID: ids[z], RunID: runID}, lz)

	other := leaseTenant(t, db)
	assert.Equal(t, []uuid.UUID{ids[x]}, takeLeases(t, db, other, claim(runID, ids[x], x)),
		"a node is held per tenant: another tenant's node of the same key is free")
}

// A node released while a task waits for it goes to that waiter: a task made
// ready after the release does not take it but waits behind, and the waiter
// served takes it. A waiter's own wait does not keep it from its node.
func TestNodeLeases_ANewlyReadyTaskWaitsBehindAnOlderWaiter(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := leaseTenant(t, db)
	x := leaseTables("x")[0]
	holderRun, holder := pendingTasks(t, db, x)
	waiterRun, waiter := pendingTasks(t, db, x)
	newRun, newer := pendingTasks(t, db, x)
	require.Equal(t, []uuid.UUID{holder[x]}, takeLeases(t, db, tenant, claim(holderRun, holder[x], x)))

	assert.Empty(t, takeLeases(t, db, tenant, claim(waiterRun, waiter[x], x)), "the node is held")
	inLeaseTx(t, db, tenant, func(tasks run.TaskCollection) {
		require.NoError(t, tasks.WaitForLeases(ctx, []uuid.UUID{waiter[x]}))
	})
	older := waitOf(t, db, waiter[x])
	require.NotNil(t, older.Reason)
	assert.Equal(t, string(run.WaitReasonNodeLease), *older.Reason)
	require.NotNil(t, older.Since)
	inLeaseTx(t, db, tenant, func(tasks run.TaskCollection) {
		require.NoError(t, tasks.ReleaseLease(ctx, holder[x]))
	})
	_, held := leaseOn(t, db, tenant, x)
	require.False(t, held, "the holder released the node")

	assert.Empty(t, takeLeases(t, db, tenant, claim(newRun, newer[x], x)), "a newly ready task does not jump the waiter")
	_, held = leaseOn(t, db, tenant, x)
	assert.False(t, held, "the node stays free for its waiter")
	inLeaseTx(t, db, tenant, func(tasks run.TaskCollection) {
		require.NoError(t, tasks.WaitForLeases(ctx, []uuid.UUID{newer[x]}))
	})
	later := waitOf(t, db, newer[x])
	require.NotNil(t, later.Since)
	assert.True(t, older.Since.Before(*later.Since), "the newly ready task waits behind the older waiter")
	inLeaseTx(t, db, tenant, func(tasks run.TaskCollection) {
		require.NoError(t, tasks.WaitForLeases(ctx, []uuid.UUID{newer[x]}))
	})
	assert.Equal(t, later.Since, waitOf(t, db, newer[x]).Since, "a task already waiting keeps its place")

	inLeaseTx(t, db, tenant, func(tasks run.TaskCollection) {
		taken, err := tasks.TakeLeaseForWaiter(ctx, claim(waiterRun, waiter[x], x))
		require.NoError(t, err)
		assert.True(t, taken, "the waiter served takes the free node")
		taken, err = tasks.TakeLeaseForWaiter(ctx, claim(waiterRun, waiter[x], x))
		require.NoError(t, err)
		assert.True(t, taken, "a waiter that already holds its node holds it")
		taken, err = tasks.TakeLeaseForWaiter(ctx, claim(newRun, newer[x], x))
		require.NoError(t, err)
		assert.False(t, taken, "a held node is not taken for another waiter")
	})
	lx, ok := leaseOn(t, db, tenant, x)
	require.True(t, ok)
	assert.Equal(t, heldLease{TaskID: waiter[x], RunID: waiterRun}, lx)

	_, err := db.Exec(`UPDATE task_tracker SET status = 'succeeded', wait_reason = NULL, lease_wait_since = NULL WHERE task_id = $1`, waiter[x])
	require.NoError(t, err)
	inLeaseTx(t, db, tenant, func(tasks run.TaskCollection) {
		require.NoError(t, tasks.ReleaseLease(ctx, waiter[x]))
	})
	assert.Equal(t, []uuid.UUID{newer[x]}, takeLeases(t, db, tenant, claim(newRun, newer[x], x)),
		"the only waiter of a free node is not kept from it by its own wait")
}

// Two transactions that take the leases of the same two nodes, one claiming
// them in the order (a, b) and the other (b, a), both commit, 200 times over:
// neither is aborted as a deadlock, and each node ends held by exactly one of
// the tasks the two takes returned.
func TestNodeLeases_OverlappingTakesInOppositeOrdersDoNotDeadlock(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := leaseTenant(t, db)
	tables := leaseTables("a", "b")
	a, b := tables[0], tables[1]
	firstRun, first := pendingTasks(t, db, a, b)
	secondRun, second := pendingTasks(t, db, a, b)
	repo := postgres.NewTaskTrackerRepository(db, discardLogger())
	orders := [2][]run.LeaseClaim{
		{claim(firstRun, first[a], a), claim(firstRun, first[b], b)},
		{claim(secondRun, second[b], b), claim(secondRun, second[a], a)},
	}

	for i := range 200 {
		var (
			wg      sync.WaitGroup
			start   = make(chan struct{})
			taken   [2][]uuid.UUID
			errs    [2]error
			holders = map[uuid.UUID]bool{}
		)
		for side, claims := range orders {
			wg.Add(1)
			go func() {
				defer wg.Done()
				taken[side], errs[side] = takeInOwnTx(ctx, db, repo, tenant, claims, start)
			}()
		}
		close(start)
		wg.Wait()

		for side, err := range errs {
			var pqErr *pq.Error
			if errors.As(err, &pqErr) && pqErr.Code == "40P01" {
				t.Fatalf("round %d: take %d was aborted as a deadlock: %v", i, side, err)
			}
			require.NoError(t, err, "round %d: take %d commits", i, side)
			for _, id := range taken[side] {
				holders[id] = true
			}
		}
		for _, table := range tables {
			var rows []uuid.UUID
			require.NoError(t, db.Select(&rows, `SELECT task_id FROM node_leases WHERE tenant_id = $1 AND node_key = $2`,
				tenant, "e2e_schema."+table))
			require.Len(t, rows, 1, "round %d: %s is held once", i, table)
			require.True(t, holders[rows[0]], "round %d: %s is held by a task a take returned", i, table)
		}
		require.Len(t, holders, 2, "round %d: the takes returned exactly the two holders", i)
		_, err := db.Exec(`DELETE FROM node_leases WHERE tenant_id = $1`, tenant)
		require.NoError(t, err)
	}
}

// takeInOwnTx begins a transaction, waits for start, takes the leases of
// claims for tenant and commits.
func takeInOwnTx(ctx context.Context, db *sqlx.DB, repo postgres.TaskTrackerRepository, tenant string, claims []run.LeaseClaim, start <-chan struct{}) ([]uuid.UUID, error) {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	<-start
	taken, err := postgres.NewTaskCollectionAdapter(repo, tx, tenant).TakeLeases(ctx, claims)
	if err != nil {
		return nil, fmt.Errorf("take: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return taken, nil
}

// reportTo delivers one report through AttemptReportHandler and a real unit
// of work and runs check before the transaction commits, while the report's
// writes are visible to no other transaction.
func (f statusFlow) reportTo(t *testing.T, rep run.AttemptReport, beforeCommit func()) {
	t.Helper()
	ctx := context.Background()
	u := newPlanningUoW(f.db)
	require.NoError(t, u.Begin(ctx))
	defer u.Rollback()
	require.NoError(t, handlers.NewAttemptReportHandler(f.tenant, testTaskDefaults, noRunMetrics{}, discardLogger()).Handle(ctx, u,
		events.AttemptReported{RunID: f.r.ScheduleID(), Report: rep}, uuid.Nil))
	beforeCommit()
	require.NoError(t, u.Commit())
}

// A writing run's dispatched tasks hold their nodes' leases. A task keeps its
// lease across a retry and while it runs; the report transaction that ends it,
// succeeded or failed with its retries spent, deletes the lease, which no
// other transaction sees gone before the report commits.
func TestNodeLeases_ReleasedInTheReportTransactionThatEndsTheTask(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	taskA := run.TaskIDFor(tenant, f.r.ScheduleID(), "e2e_schema.ftable_a")
	taskB := run.TaskIDFor(tenant, f.r.ScheduleID(), "e2e_schema.ftable_b")
	taskG := run.TaskIDFor(tenant, f.r.ScheduleID(), "e2e_schema.ftable_g")
	holderOf := func(table string) uuid.UUID {
		l, ok := leaseOn(t, db, tenant, table)
		if !ok {
			return uuid.Nil
		}
		return l.TaskID
	}
	require.Equal(t, taskA, holderOf("ftable_a"), "the run dispatched ftable_a holding its lease")
	require.Equal(t, taskB, holderOf("ftable_b"))

	f.report(t, "e2e_schema.ftable_a", 0, run.AttemptFailed, model.FailureReasonExitCode)
	_, status, attempt := f.task(t, "e2e_schema.ftable_a")
	require.Equal(t, int32(1), attempt, "the failure was retried")
	assert.Equal(t, run.TaskStatusDispatched, status)
	assert.Equal(t, taskA, holderOf("ftable_a"), "a retry keeps the lease")
	f.report(t, "e2e_schema.ftable_a", 1, run.AttemptRunning, "")
	assert.Equal(t, taskA, holderOf("ftable_a"), "a running task keeps the lease")

	f.reportTo(t, run.AttemptReport{TaskID: taskA, Attempt: 1, Status: run.AttemptSucceeded}, func() {
		assert.Equal(t, taskA, holderOf("ftable_a"), "the release is not seen before the report commits")
	})
	_, status, _ = f.task(t, "e2e_schema.ftable_a")
	assert.Equal(t, run.TaskStatusSucceeded, status)
	assert.False(t, leaseOf(t, db, taskA), "the success released the lease")
	assert.Equal(t, taskG, holderOf("ftable_g"), "the task the success made ready holds its own lease")

	for n := int32(0); n < 2; n++ {
		f.report(t, "e2e_schema.ftable_b", n, run.AttemptFailed, model.FailureReasonExitCode)
		assert.Equal(t, taskB, holderOf("ftable_b"), "attempt %d's failure is retried and keeps the lease", n)
	}
	f.reportTo(t, run.AttemptReport{TaskID: taskB, Attempt: 2, Status: run.AttemptFailed, FailureReason: model.FailureReasonExitCode}, func() {
		assert.Equal(t, taskB, holderOf("ftable_b"), "the release is not seen before the report commits")
	})
	_, status, _ = f.task(t, "e2e_schema.ftable_b")
	assert.Equal(t, run.TaskStatusFailed, status)
	assert.False(t, leaseOf(t, db, taskB), "the failure that spent the retries released the lease")
}

// A cancel releases the lease of each task it cancels, a task in flight with
// no open attempt among them, in the cancel's transaction; a task it makes
// stopping keeps its lease until its own report ends it.
func TestNodeLeases_ACancelReleasesTheLeaseOfEachTaskItCancels(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	taskA := run.TaskIDFor(tenant, f.r.ScheduleID(), "e2e_schema.ftable_a")
	taskB := run.TaskIDFor(tenant, f.r.ScheduleID(), "e2e_schema.ftable_b")
	f.report(t, "e2e_schema.ftable_a", 0, run.AttemptRunning, "")
	// A defect leaves ftable_b dispatched with no attempt open to answer a stop.
	_, err := db.Exec(`DELETE FROM task_attempts WHERE task_id = $1`, taskB)
	require.NoError(t, err)
	require.True(t, leaseOf(t, db, taskB))

	cancelByID(t, db, f.r.ScheduleID(), "alice", "bad data")

	statuses := f.statuses(t)
	assert.Equal(t, run.TaskStatusStopping, statuses["e2e_schema.ftable_a"])
	assert.Equal(t, run.TaskStatusCancelled, statuses["e2e_schema.ftable_b"])
	assert.True(t, leaseOf(t, db, taskA), "a stopping task keeps its lease")
	assert.False(t, leaseOf(t, db, taskB), "the cancel released the lease of the task it cancelled")

	f.report(t, "e2e_schema.ftable_a", 0, run.AttemptStopped, "")
	assert.False(t, leaseOf(t, db, taskA), "the stopped report that ends the task releases its lease")
}

// A task that leaves pending stops waiting for its node's lease: dispatched,
// skipped one by one or with the rest of its run, or cancelled.
func TestNodeLeases_LeavingPendingClearsTheWait(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := leaseTenant(t, db)
	tables := leaseTables("d", "s", "p", "c")
	d, s, p, c := tables[0], tables[1], tables[2], tables[3]
	_, first := pendingTasks(t, db, d, s)
	skipRun, skip := pendingTasks(t, db, p)
	cancelRun, cancel := pendingTasks(t, db, c)
	all := []uuid.UUID{first[d], first[s], skip[p], cancel[c]}
	inLeaseTx(t, db, tenant, func(tasks run.TaskCollection) {
		require.NoError(t, tasks.WaitForLeases(ctx, all))
	})
	for _, id := range all {
		w := waitOf(t, db, id)
		require.NotNil(t, w.Reason)
		require.NotNil(t, w.Since)
	}

	inLeaseTx(t, db, tenant, func(tasks run.TaskCollection) {
		stamped, err := tasks.MarkDispatched(ctx, []uuid.UUID{first[d]}, time.Now())
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{first[d]}, stamped)
		n, err := tasks.MarkSkipped(ctx, []uuid.UUID{first[s]})
		require.NoError(t, err)
		require.Equal(t, 1, n)
		n, err = tasks.SkipPending(ctx, skipRun)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		counts, err := tasks.BeginStop(ctx, cancelRun, "alice", time.Now())
		require.NoError(t, err)
		require.Equal(t, run.StopCounts{Cancelled: 1}, counts)
	})

	for id, want := range map[uuid.UUID]run.TaskStatus{
		first[d]: run.TaskStatusDispatched, first[s]: run.TaskStatusSkipped,
		skip[p]: run.TaskStatusSkipped, cancel[c]: run.TaskStatusCancelled,
	} {
		assert.Equal(t, leaseWait{Status: want}, waitOf(t, db, id), "a %s task waits for nothing", want)
	}
}

// A report whose transaction Postgres aborts as a deadlock is redelivered: the
// consumer classifies SQLSTATE 40P01, bare or wrapped, as transient.
func TestReportConsumer_ADeadlockIsTransient(t *testing.T) {
	deadlock := &pq.Error{Code: "40P01", Message: "deadlock detected"}

	assert.Equal(t, pkgredis.ClassTransient, pkgredis.Classify(deadlock))
	assert.Equal(t, pkgredis.ClassTransient, pkgredis.Classify(fmt.Errorf("record the report: %w", deadlock)))
}
