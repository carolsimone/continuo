package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/events"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/carolsimone/continuo/run-controller/service/handlers"
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

// pendingTasks stores a run of tenant holding one pending, undispatched task
// per table and returns the run id and each task's id.
func pendingTasks(t *testing.T, db *sqlx.DB, tenant string, tables ...string) (uuid.UUID, map[string]uuid.UUID) {
	t.Helper()
	statuses := make(map[string]run.TaskStatus, len(tables))
	for _, table := range tables {
		statuses[table] = run.TaskStatusPending
	}
	return insertTasks(t, db, tenant, statuses)
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
	fn(postgres.NewTaskCollection(db, discardLogger()).ForTenant(tenant).WithTx(tx))
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

// leaseWait is what a task row says a task waits for.
type leaseWait struct {
	Status run.TaskStatus `db:"status"`
	Reason *string        `db:"wait_reason"`
	Since  *time.Time     `db:"lease_wait_since"`
}

func waitOf(t *testing.T, db *sqlx.DB, taskID uuid.UUID) leaseWait {
	t.Helper()
	var w leaseWait
	require.NoError(t, db.Get(&w, `SELECT status, wait_reason, lease_wait_since FROM tasks WHERE task_id = $1`, taskID))
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
	holderRun, holder := pendingTasks(t, db, tenant, x)
	runID, ids := pendingTasks(t, db, tenant, x, y, z)
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
	otherRun, otherIDs := pendingTasks(t, db, other, x)
	assert.Equal(t, []uuid.UUID{otherIDs[x]}, takeLeases(t, db, other, claim(otherRun, otherIDs[x], x)),
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
	holderRun, holder := pendingTasks(t, db, tenant, x)
	waiterRun, waiter := pendingTasks(t, db, tenant, x)
	newRun, newer := pendingTasks(t, db, tenant, x)
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

	_, err := db.Exec(`UPDATE tasks SET status = 'succeeded', wait_reason = NULL, lease_wait_since = NULL WHERE task_id = $1`, waiter[x])
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
	firstRun, first := pendingTasks(t, db, tenant, a, b)
	secondRun, second := pendingTasks(t, db, tenant, a, b)
	repo := postgres.NewTaskCollection(db, discardLogger())
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
func takeInOwnTx(ctx context.Context, db *sqlx.DB, repo *postgres.TaskCollection, tenant string, claims []run.LeaseClaim, start <-chan struct{}) ([]uuid.UUID, error) {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	<-start
	taken, err := repo.ForTenant(tenant).WithTx(tx).TakeLeases(ctx, claims)
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
	require.NoError(t, handlers.NewAttemptReportHandler(testTaskDefaults, noRunMetrics{}, discardLogger()).Handle(ctx, u,
		events.AttemptReported{TenantID: f.tenant, RunID: f.r.RunID(), Report: rep}, uuid.Nil))
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
	taskA := run.TaskIDFor(tenant, f.r.RunID(), "e2e_schema.ftable_a")
	taskB := run.TaskIDFor(tenant, f.r.RunID(), "e2e_schema.ftable_b")
	taskG := run.TaskIDFor(tenant, f.r.RunID(), "e2e_schema.ftable_g")
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

// leaseWriteGate wraps the task collection of a report's transaction to order
// a concurrent take against the report's lease writes (its takes and
// releases): right after the first it closes first, and before the second it
// runs beforeNext.
type leaseWriteGate struct {
	run.TaskCollection
	writes     int
	first      chan struct{}
	beforeNext func()
}

func (g *leaseWriteGate) write(fn func() error) error {
	g.writes++
	if g.writes == 2 {
		g.beforeNext()
	}
	err := fn()
	if g.writes == 1 {
		close(g.first)
	}
	return err
}

func (g *leaseWriteGate) TakeLeases(ctx context.Context, claims []run.LeaseClaim) ([]uuid.UUID, error) {
	var taken []uuid.UUID
	err := g.write(func() error {
		var err error
		taken, err = g.TaskCollection.TakeLeases(ctx, claims)
		return err
	})
	return taken, err
}

func (g *leaseWriteGate) ReleaseLease(ctx context.Context, taskID uuid.UUID) error {
	return g.write(func() error { return g.TaskCollection.ReleaseLease(ctx, taskID) })
}

// gatedUoW is a unit of work whose task collection goes through gate.
type gatedUoW struct {
	*postgres.PostgresUnitOfWork
	gate *leaseWriteGate
}

func (u gatedUoW) TaskCollection(tenantID string) run.TaskCollection {
	u.gate.TaskCollection = u.PostgresUnitOfWork.TaskCollection(tenantID)
	return u.gate
}

// reportThrough delivers one report through AttemptReportHandler, with its
// unit of work's task collection going through gate, and commits.
func reportThrough(ctx context.Context, db *sqlx.DB, gate *leaseWriteGate, tenant string, runID uuid.UUID, rep run.AttemptReport) error {
	u := newPlanningUoW(db)
	if err := u.Begin(ctx); err != nil {
		return err
	}
	defer func() { _ = u.Rollback() }()
	if err := handlers.NewAttemptReportHandler(testTaskDefaults, noRunMetrics{}, discardLogger()).Handle(ctx,
		gatedUoW{PostgresUnitOfWork: u, gate: gate}, events.AttemptReported{TenantID: tenant, RunID: runID, Report: rep}, uuid.Nil); err != nil {
		return err
	}
	return u.Commit()
}

// takeAfter begins a transaction, sends its backend's pid (0 when it has
// none), waits for start, or for stop when the report it races ends first,
// then takes the leases of claims for tenant and commits.
func takeAfter(ctx context.Context, db *sqlx.DB, tenant string, claims []run.LeaseClaim, pids chan<- int,
	start, stop <-chan struct{}) ([]uuid.UUID, error) {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		pids <- 0
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	var pid int
	if err := tx.GetContext(ctx, &pid, `SELECT pg_backend_pid()`); err != nil {
		pids <- 0
		return nil, err
	}
	pids <- pid
	select {
	case <-start:
	case <-stop:
	}
	taken, err := postgres.NewTaskCollection(db, discardLogger()).ForTenant(tenant).WithTx(tx).TakeLeases(ctx, claims)
	if err != nil {
		return nil, fmt.Errorf("take: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return taken, nil
}

// awaitLockWait waits until the backend pid waits on a lock, or done is
// closed, and fails the test after five seconds of neither.
func awaitLockWait(t *testing.T, db *sqlx.DB, pid int, done <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-done:
			return
		default:
		}
		var waiting bool
		require.NoError(t, db.Get(&waiting,
			`SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND wait_event_type = 'Lock')`, pid))
		if waiting {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("backend %d neither waited on a lock nor ended", pid)
}

// isDeadlock reports whether Postgres aborted the transaction behind err as a
// deadlock (40P01).
func isDeadlock(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "40P01" || err != nil && strings.Contains(err.Error(), "deadlock detected")
}

// A report ends the task that holds node Y and makes ready a task of node X
// while another transaction takes X and Y in node-key order (X sorts first),
// as a run's start does. The take begins right after the report's first lease
// write, and the report's second lease write waits until the take waits on a
// lock. A report that released Y before it took X would hold Y's row while
// the take held X and waited for Y, then wait for X itself: a deadlock. The
// report takes X before it releases Y, so the take waits for the report and
// both commit: X goes to the report's ready task and Y, once released, to the
// take. Each round has a release, a run and node keys of its own.
func TestNodeLeases_AReportAndAStartTakingTheSameNodesDoNotDeadlock(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	for i := range 10 {
		tables := leaseTables("x_down", "y_up")
		down, up := tables[0], tables[1]
		schedule := "lease-order-" + uuid.NewString()[:8]
		tenant := storeEdges(t, db, schedule, [][2]string{{up, down}}, up, down)
		t.Cleanup(func() {
			_, _ = db.ExecContext(context.Background(), `DELETE FROM node_leases WHERE tenant_id = $1`, tenant)
			_, _ = db.ExecContext(context.Background(), `DELETE FROM node_materializations WHERE tenant_id = $1`, tenant)
		})
		f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, schedule, model.OperationRun)}
		upTask, downTask := f.taskID(up), f.taskID(down)
		held, ok := leaseOn(t, db, tenant, up)
		require.True(t, ok)
		require.Equal(t, heldLease{TaskID: upTask, RunID: f.r.RunID()}, held, "round %d: the run's start took Y", i)
		otherRun, other := pendingTasks(t, db, tenant, down, up)
		claims := []run.LeaseClaim{claim(otherRun, other[down], down), claim(otherRun, other[up], up)}

		var (
			first      = make(chan struct{})
			reportDone = make(chan struct{})
			takeDone   = make(chan struct{})
			pids       = make(chan int, 1)
			taken      []uuid.UUID
			takeErr    error
		)
		go func() {
			defer close(takeDone)
			taken, takeErr = takeAfter(ctx, db, tenant, claims, pids, first, reportDone)
		}()
		pid := <-pids
		gate := &leaseWriteGate{first: first, beforeNext: func() { awaitLockWait(t, db, pid, takeDone) }}
		reportErr := reportThrough(ctx, db, gate, tenant, f.r.RunID(),
			run.AttemptReport{TaskID: upTask, Attempt: 0, Status: run.AttemptSucceeded})
		close(reportDone)
		<-takeDone

		require.False(t, isDeadlock(reportErr), "round %d: the report was aborted as a deadlock: %v", i, reportErr)
		require.False(t, isDeadlock(takeErr), "round %d: the take was aborted as a deadlock: %v", i, takeErr)
		require.NoError(t, reportErr, "round %d: the report commits", i)
		require.NoError(t, takeErr, "round %d: the take commits", i)
		x, ok := leaseOn(t, db, tenant, down)
		require.True(t, ok, "round %d: X is held", i)
		assert.Equal(t, heldLease{TaskID: downTask, RunID: f.r.RunID()}, x, "round %d: the report took X first", i)
		y, ok := leaseOn(t, db, tenant, up)
		require.True(t, ok, "round %d: Y is held", i)
		assert.Equal(t, heldLease{TaskID: other[up], RunID: otherRun}, y, "round %d: the take got Y once the report released it", i)
		assert.Equal(t, []uuid.UUID{other[up]}, taken, "round %d", i)
	}
}

// A cancel releases the lease of each task it cancels, a task in flight with
// no open attempt among them, in the cancel's transaction; a task it makes
// stopping keeps its lease until its own report ends it.
func TestNodeLeases_ACancelReleasesTheLeaseOfEachTaskItCancels(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	taskA := run.TaskIDFor(tenant, f.r.RunID(), "e2e_schema.ftable_a")
	taskB := run.TaskIDFor(tenant, f.r.RunID(), "e2e_schema.ftable_b")
	f.report(t, "e2e_schema.ftable_a", 0, run.AttemptRunning, "")
	// A defect leaves ftable_b dispatched with no attempt open to answer a stop.
	_, err := db.Exec(`DELETE FROM task_attempts WHERE task_id = $1`, taskB)
	require.NoError(t, err)
	require.True(t, leaseOf(t, db, taskB))

	cancelByID(t, db, f.tenant, f.r.RunID(), "alice", "bad data")

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
	_, first := pendingTasks(t, db, tenant, d, s)
	skipRun, skip := pendingTasks(t, db, tenant, p)
	cancelRun, cancel := pendingTasks(t, db, tenant, c)
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
