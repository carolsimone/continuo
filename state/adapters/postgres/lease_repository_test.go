package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/attemptreconcile"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/leasewaker"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// leaseKey is the node key of the e2e_schema table.
func leaseKey(table string) topology.NodeKey { return topology.NodeKey("e2e_schema." + table) }

// waitSince makes the pending task wait for its node's write lease since
// since.
func waitSince(t *testing.T, db *sqlx.DB, taskID uuid.UUID, since time.Time) {
	t.Helper()
	_, err := db.Exec(`UPDATE tasks SET wait_reason = 'node_lease', lease_wait_since = $2 WHERE task_id = $1 AND status = 'pending'`,
		taskID, since)
	require.NoError(t, err)
}

// inUTC returns waiters with their wait times in UTC, so they compare equal to
// the times a test wrote.
func inUTC(waiters []repository.LeaseWaiter) []repository.LeaseWaiter {
	for i := range waiters {
		waiters[i].WaitingSince = waiters[i].WaitingSince.UTC()
	}
	return waiters
}

// For each node no task holds, the oldest of its waiters whose run is running
// is grantable, the longest waiting first; ties go to the lower task id. A
// held node, a waiter of a run that is not running and a pending task that
// waits only for its upstream tasks are not grantable, and every waiter
// counts. The scan reads every tenant and names each waiter with its tenant: a
// node of the same key is another tenant's own node, whose oldest waiter is
// grantable beside this tenant's.
func TestLeaseRepository_GrantableIsTheOldestWaiterOfEachFreeNode(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tables := leaseTables("x", "y", "z", "v", "u")
	x, y, z, v, u := tables[0], tables[1], tables[2], tables[3], tables[4]
	tenant := storeEdges(t, db, "grantable-"+x, nil, tables...)
	other := storeEdges(t, db, "grantable-other-"+x, nil, x)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM node_leases WHERE tenant_id = ANY($1)`, pq.Array([]string{tenant, other}))
	})
	repo := postgres.NewLeaseRepository(db, nil)
	waitersBefore, err := repo.CountWaiters(ctx)
	require.NoError(t, err)
	// The oldest wait is a month old, older than any other test's, so the
	// limit keeps it.
	base := time.Now().UTC().Truncate(time.Microsecond).Add(-30 * 24 * time.Hour)

	xOldRun, xOld := pendingTasks(t, db, tenant, x)
	waitSince(t, db, xOld[x], base)
	_, xNew := pendingTasks(t, db, tenant, x)
	waitSince(t, db, xNew[x], base.Add(2*time.Minute))

	holderRun, holder := insertTasks(t, db, tenant, map[string]run.TaskStatus{y: run.TaskStatusRunning})
	require.NoError(t, insertLease(db, tenant, string(leaseKey(y)), holder[y], holderRun))
	_, yWaiter := pendingTasks(t, db, tenant, y)
	waitSince(t, db, yWaiter[y], base.Add(-time.Minute))

	zStuckRun, zStuck := pendingTasks(t, db, tenant, z)
	waitSince(t, db, zStuck[z], base.Add(time.Minute))
	_, err = db.Exec(`UPDATE runs SET status = 'cancelling' WHERE run_id = $1`, zStuckRun)
	require.NoError(t, err)
	zRun, zNext := pendingTasks(t, db, tenant, z)
	waitSince(t, db, zNext[z], base.Add(3*time.Minute))

	vaRun, va := pendingTasks(t, db, tenant, v)
	vbRun, vb := pendingTasks(t, db, tenant, v)
	waitSince(t, db, va[v], base.Add(4*time.Minute))
	waitSince(t, db, vb[v], base.Add(4*time.Minute))
	vFirst := repository.LeaseWaiter{TenantID: tenant, TaskID: va[v], RunID: vaRun, NodeKey: leaseKey(v), WaitingSince: base.Add(4 * time.Minute)}
	if vb[v].String() < va[v].String() {
		vFirst = repository.LeaseWaiter{TenantID: tenant, TaskID: vb[v], RunID: vbRun, NodeKey: leaseKey(v), WaitingSince: base.Add(4 * time.Minute)}
	}

	pendingTasks(t, db, tenant, u)

	theirRun, theirs := pendingTasks(t, db, other, x)
	waitSince(t, db, theirs[x], base.Add(time.Minute))

	got, err := repo.Grantable(ctx, 1000)
	require.NoError(t, err)
	assert.Equal(t, []repository.LeaseWaiter{
		{TenantID: tenant, TaskID: xOld[x], RunID: xOldRun, NodeKey: leaseKey(x), WaitingSince: base},
		{TenantID: other, TaskID: theirs[x], RunID: theirRun, NodeKey: leaseKey(x), WaitingSince: base.Add(time.Minute)},
		{TenantID: tenant, TaskID: zNext[z], RunID: zRun, NodeKey: leaseKey(z), WaitingSince: base.Add(3 * time.Minute)},
		vFirst,
	}, waitersOf(inUTC(got), tenant, other), "each tenant's node is its own, so each tenant's oldest waiter of x is grantable")

	oldest, err := repo.Grantable(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, []repository.LeaseWaiter{{TenantID: tenant, TaskID: xOld[x], RunID: xOldRun, NodeKey: leaseKey(x), WaitingSince: base}},
		inUTC(oldest), "the limit keeps the longest waiting")

	n, err := repo.CountWaiters(ctx)
	require.NoError(t, err)
	assert.Equal(t, waitersBefore+8, n, "every waiting task of every tenant counts, whether its node is free or not")
}

// waitersOf returns, in order, the waiters of waiters that are of tenants.
func waitersOf(waiters []repository.LeaseWaiter, tenants ...string) []repository.LeaseWaiter {
	return keep(waiters, func(w repository.LeaseWaiter) string { return w.TenantID }, tenants)
}

// The grantable waits of a run are its waiting tasks that are the next waiter
// of a free node, as Grantable picks them, in node-key order: a wait on
// a node another task holds, one queued behind an older waiter of a running
// run and a pending task that waits only for its upstream tasks are left out,
// while an older waiter of a cancelling run queues no one. Another tenant's
// collection finds none of them.
func TestTaskCollection_GrantableWaitsAreTheRunsWaitsNextInLine(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tables := leaseTables("x", "y", "z", "v", "u")
	x, y, z, v, u := tables[0], tables[1], tables[2], tables[3], tables[4]
	tenant := storeEdges(t, db, "next-in-line-"+x, nil, tables...)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM node_leases WHERE tenant_id = $1`, tenant)
	})
	base := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	runID, ids := pendingTasks(t, db, tenant, x, y, z, v, u)
	waitSince(t, db, ids[x], base.Add(time.Minute))
	waitSince(t, db, ids[y], base)
	waitSince(t, db, ids[z], base)
	waitSince(t, db, ids[v], base.Add(2*time.Minute))

	holderRun, holder := insertTasks(t, db, tenant, map[string]run.TaskStatus{y: run.TaskStatusRunning})
	require.NoError(t, insertLease(db, tenant, string(leaseKey(y)), holder[y], holderRun))
	_, zOlder := pendingTasks(t, db, tenant, z)
	waitSince(t, db, zOlder[z], base.Add(-time.Minute))
	vOlderRun, vOlder := pendingTasks(t, db, tenant, v)
	waitSince(t, db, vOlder[v], base.Add(-time.Minute))
	_, err := db.Exec(`UPDATE runs SET status = 'cancelling' WHERE run_id = $1`, vOlderRun)
	require.NoError(t, err)

	var got []run.LeaseWait
	inLeaseTx(t, db, tenant, func(tasks run.TaskCollection) {
		got, err = tasks.GrantableWaits(ctx, runID)
		require.NoError(t, err)
	})
	for i := range got {
		got[i].WaitingSince = got[i].WaitingSince.UTC()
	}
	assert.Equal(t, []run.LeaseWait{
		{TaskID: ids[v], NodeKey: leaseKey(v), WaitingSince: base.Add(2 * time.Minute)},
		{TaskID: ids[x], NodeKey: leaseKey(x), WaitingSince: base.Add(time.Minute)},
	}, got, "in node-key order, whichever waited longer")

	inLeaseTx(t, db, planningTenant(t, db), func(tasks run.TaskCollection) {
		theirs, err := tasks.GrantableWaits(ctx, runID)
		require.NoError(t, err)
		assert.Empty(t, theirs, "a waiter of a run on this tenant's release is not another tenant's")
	})
}

// A lease whose holder task has ended is deleted and returned with the
// holder's status and tenant; a lease whose holder is in flight is kept. One
// repair covers every tenant, so another tenant's ended lease goes in the
// same repair. A second repair finds nothing.
func TestLeaseRepository_RepairEndedDeletesALeaseWhoseHolderEnded(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := leaseTenant(t, db)
	statuses := []run.TaskStatus{
		run.TaskStatusSucceeded, run.TaskStatusFailed, run.TaskStatusCancelled, run.TaskStatusSkipped,
		run.TaskStatusDispatched, run.TaskStatusQueued, run.TaskStatusRunning, run.TaskStatusStopping,
	}
	suffixes := make([]string, 0, len(statuses))
	for _, s := range statuses {
		suffixes = append(suffixes, string(s))
	}
	tables := leaseTables(suffixes...)
	byTable := make(map[string]run.TaskStatus, len(tables))
	for i, table := range tables {
		byTable[table] = statuses[i]
	}
	runID, ids := insertTasks(t, db, tenant, byTable)
	for _, table := range tables {
		require.NoError(t, insertLease(db, tenant, string(leaseKey(table)), ids[table], runID))
	}
	otherTenant := leaseTenant(t, db)
	otherTable := leaseTables("other")[0]
	otherRun, other := insertTasks(t, db, otherTenant, map[string]run.TaskStatus{otherTable: run.TaskStatusSucceeded})
	require.NoError(t, insertLease(db, otherTenant, string(leaseKey(otherTable)), other[otherTable], otherRun))

	repaired, err := postgres.NewLeaseRepository(db, nil).RepairEnded(ctx)
	require.NoError(t, err)

	var want, gotLeases []repository.RepairedLease
	for _, table := range tables[:4] {
		want = append(want, repository.RepairedLease{TenantID: tenant, TaskID: ids[table], RunID: runID, NodeKey: leaseKey(table), TaskStatus: byTable[table]})
	}
	want = append(want, repository.RepairedLease{TenantID: otherTenant, TaskID: other[otherTable], RunID: otherRun, NodeKey: leaseKey(otherTable),
		TaskStatus: run.TaskStatusSucceeded})
	for _, l := range repairedOf(repaired, tenant, otherTenant) {
		assert.False(t, l.AcquiredAt.IsZero(), "a repaired lease carries when it was taken")
		l.AcquiredAt = time.Time{}
		gotLeases = append(gotLeases, l)
	}
	assert.ElementsMatch(t, want, gotLeases)
	for _, table := range tables[:4] {
		assert.False(t, leaseOf(t, db, ids[table]), "the lease of a %s holder is deleted", byTable[table])
	}
	for _, table := range tables[4:] {
		assert.True(t, leaseOf(t, db, ids[table]), "the lease of a %s holder is kept", byTable[table])
	}
	assert.False(t, leaseOf(t, db, other[otherTable]), "another tenant's ended lease goes in the same repair")

	again, err := postgres.NewLeaseRepository(db, nil).RepairEnded(ctx)
	require.NoError(t, err)
	assert.Empty(t, repairedOf(again, tenant, otherTenant))
}

// repairedOf returns, in order, the repaired leases of tenants.
func repairedOf(leases []repository.RepairedLease, tenants ...string) []repository.RepairedLease {
	return keep(leases, func(l repository.RepairedLease) string { return l.TenantID }, tenants)
}

// leaseMetrics records what the lease waker observes.
type leaseMetrics struct {
	noRunMetrics
	waiters  []int
	granted  []time.Duration
	repaired int
}

func (m *leaseMetrics) LeaseWaiters(n int)              { m.waiters = append(m.waiters, n) }
func (m *leaseMetrics) LeaseGranted(wait time.Duration) { m.granted = append(m.granted, wait) }
func (m *leaseMetrics) LeaseRepaired()                  { m.repaired++ }

// leaseFlow is a tenant whose live release rel-1 holds one schedule, in which
// seed_table_1 feeds table_a, under table names no other test uses.
type leaseFlow struct {
	db       *sqlx.DB
	tenant   string
	schedule string
	seed     string
	tableA   string
}

// newLeaseFlow stores the flow's release, makes it live, and deletes the
// tenant's leases and loaded versions when the test ends.
func newLeaseFlow(t *testing.T, db *sqlx.DB) leaseFlow {
	t.Helper()
	ctx := context.Background()
	prefix := "lw_" + uuid.NewString()[:8]
	lf := leaseFlow{db: db, schedule: "lease-" + prefix, seed: prefix + "_seed_table_1", tableA: prefix + "_table_a"}
	lf.tenant = storeEdges(t, db, lf.schedule, [][2]string{{lf.seed, lf.tableA}}, lf.seed, lf.tableA)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM node_leases WHERE tenant_id = $1`, lf.tenant)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM node_materializations WHERE tenant_id = $1`, lf.tenant)
	})
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, postgres.NewTopologyRepository(db, tx).SetLivePointer(ctx, lf.tenant, topology.Pointer{ReleaseID: "rel-1", PromotionSeq: 1}, time.Now()))
	require.NoError(t, tx.Commit())
	return lf
}

// cronHoldingTableA starts a run of the flow's schedule and reports its
// seed_table_1 succeeded, which dispatches its table_a holding table_a's
// lease.
func (lf leaseFlow) cronHoldingTableA(t *testing.T) statusFlow {
	t.Helper()
	cron := statusFlow{db: lf.db, tenant: lf.tenant, r: startScheduleRun(t, lf.db, lf.tenant, lf.schedule, model.OperationRun)}
	cron.report(t, string(leaseKey(lf.seed)), 0, run.AttemptSucceeded, "")
	held, ok := leaseOn(t, lf.db, lf.tenant, lf.tableA)
	require.True(t, ok, "the report that made table_a ready dispatched it holding its lease")
	require.Equal(t, heldLease{TaskID: cron.taskID(lf.tableA), RunID: cron.r.RunID()}, held)
	return cron
}

// triggerSingleNode creates a single-node run of table on the live release
// through TriggerSingleNodeRunHandler; the run, its tasks and its outbox rows
// are deleted when the test ends.
func (lf leaseFlow) triggerSingleNode(t *testing.T, table string) statusFlow {
	t.Helper()
	ctx := context.Background()
	h := handlers.NewTriggerSingleNodeRunHandler(runstart.Planner{Tasks: testTaskDefaults}, discardLogger())
	id, _, err := h.Handle(ctx, newPlanningUoW(lf.db), handlers.TriggerSingleNodeRunInput{
		TenantID:       lf.tenant,
		Target:         run.NodeID{ServiceName: "service-1", SchemaName: "e2e_schema", TableName: table},
		MetadataSource: run.MetadataSourceLatest,
		Operation:      model.OperationRun,
		Initiator:      identity.System(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = lf.db.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, id)
		_, _ = lf.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = $1`, id)
	})
	r, err := newPlanningUoW(lf.db).Run().GetRun(ctx, lf.tenant, id)
	require.NoError(t, err)
	return statusFlow{db: lf.db, tenant: lf.tenant, r: r}
}

// waker returns a lease waker that acts on the flow's tenant's rows.
func (lf leaseFlow) waker(metrics *leaseMetrics, logger *slog.Logger) *leasewaker.Waker {
	return leasewaker.New(scopedUoW(lf.db, lf.tenant), testTaskDefaults, metrics, logger)
}

// builtBy returns the run node_materializations names as the last to load
// table for the flow's tenant.
func (lf leaseFlow) builtBy(t *testing.T, table string) uuid.UUID {
	t.Helper()
	var runID uuid.NullUUID
	require.NoError(t, lf.db.Get(&runID, `SELECT run_id FROM node_materializations WHERE tenant_id = $1 AND node_key = $2`,
		lf.tenant, string(leaseKey(table))))
	return runID.UUID
}

// taskID is the id of the run's task at table.
func (f statusFlow) taskID(table string) uuid.UUID {
	return run.TaskIDFor(f.tenant, f.r.RunID(), leaseKey(table))
}

// dispatches returns the task.dispatch:v1 rows the run wrote to the outbox.
func (f statusFlow) dispatches(t *testing.T) []pkgevents.TaskDispatch {
	t.Helper()
	var payloads [][]byte
	require.NoError(t, f.db.Select(&payloads, `SELECT payload FROM state_outbox WHERE aggregate_id = $1 AND stream_name = $2`,
		f.r.RunID(), streams.TaskDispatchV1))
	out := make([]pkgevents.TaskDispatch, 0, len(payloads))
	for _, p := range payloads {
		_, wire, err := pkgoutbox.UnwrapTenant(p)
		require.NoError(t, err)
		var d pkgevents.TaskDispatch
		require.NoError(t, json.Unmarshal(wire, &d))
		out = append(out, d)
	}
	return out
}

// requireWaiting asserts the task is pending and waits for its node's lease.
func requireWaiting(t *testing.T, db *sqlx.DB, taskID uuid.UUID, msg string) {
	t.Helper()
	w := waitOf(t, db, taskID)
	require.Equal(t, run.TaskStatusPending, w.Status, msg)
	require.NotNil(t, w.Reason, msg)
	require.Equal(t, string(run.WaitReasonNodeLease), *w.Reason, msg)
	require.NotNil(t, w.Since, msg)
}

// The design's trace of a single-node run behind a cron run: the cron run's
// table_a holds the node's lease, so the single-node run created next stores
// its task waiting and dispatches nothing, and so does a second single-node run
// after it. The cron task's success releases the lease; one pass grants it to
// the older single-node task, which is dispatched with attempt 0 while the
// newer one keeps waiting. The single-node task's success then records its
// load as table_a's, the later write, and the next pass serves the newer
// waiter.
func TestLeaseWaker_GrantsAFreedNodeToItsOldestWaiter(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	lf := newLeaseFlow(t, db)
	keyA := string(leaseKey(lf.tableA))
	cron := lf.cronHoldingTableA(t)

	single := lf.triggerSingleNode(t, lf.tableA)
	singleTask := single.taskID(lf.tableA)
	requireWaiting(t, db, singleTask, "the single-node task waits for the cron task's lease")
	assert.Empty(t, single.attempts(t, keyA), "its creation dispatched nothing")
	assert.Empty(t, single.dispatches(t))
	later := lf.triggerSingleNode(t, lf.tableA)
	laterTask := later.taskID(lf.tableA)
	requireWaiting(t, db, laterTask, "a second single-node task waits too")
	metrics := &leaseMetrics{}
	waker := lf.waker(metrics, discardLogger())

	require.NoError(t, waker.Pass(ctx))
	assert.Empty(t, metrics.granted, "nothing is granted while the cron task holds table_a")
	requireWaiting(t, db, singleTask, "the node is still held")

	cron.report(t, keyA, 0, run.AttemptSucceeded, "")
	_, held := leaseOn(t, db, lf.tenant, lf.tableA)
	require.False(t, held, "the cron task's success released table_a")
	assert.Equal(t, cron.r.RunID(), lf.builtBy(t, lf.tableA))

	require.NoError(t, waker.Pass(ctx))

	assert.Equal(t, leaseWait{Status: run.TaskStatusDispatched}, waitOf(t, db, singleTask), "the grant dispatched the task and ended its wait")
	lease, held := leaseOn(t, db, lf.tenant, lf.tableA)
	require.True(t, held)
	assert.Equal(t, heldLease{TaskID: singleTask, RunID: single.r.RunID()}, lease)
	job, _, _ := single.task(t, keyA)
	assert.Equal(t, []attemptRow{{Attempt: 0, JobName: job, Status: "dispatched"}}, single.attempts(t, keyA))
	dispatches := single.dispatches(t)
	require.Len(t, dispatches, 1)
	assert.Equal(t, singleTask.String(), dispatches[0].TaskID)
	assert.Equal(t, int32(0), dispatches[0].Attempt)
	assert.Equal(t, keyA, dispatches[0].NodeKey)
	requireWaiting(t, db, laterTask, "the newer waiter keeps waiting")
	assert.Empty(t, later.dispatches(t))
	assert.Len(t, metrics.granted, 1)
	assert.Equal(t, []int{2, 1}, metrics.waiters)

	single.report(t, keyA, 0, run.AttemptSucceeded, "")
	assert.Equal(t, single.r.RunID(), lf.builtBy(t, lf.tableA), "the later write is the one recorded")

	require.NoError(t, waker.Pass(ctx))
	assert.Equal(t, leaseWait{Status: run.TaskStatusDispatched}, waitOf(t, db, laterTask))
	lease, held = leaseOn(t, db, lf.tenant, lf.tableA)
	require.True(t, held)
	assert.Equal(t, heldLease{TaskID: laterTask, RunID: later.r.RunID()}, lease)
	assert.Equal(t, []int{2, 1, 0}, metrics.waiters)
}

// A freed node is granted to no task of a run that is not running: a waiting
// task of a cancelling run stays waiting with nothing dispatched, a cancel of a
// waiting run cancels its task, and the node stays free.
func TestLeaseWaker_SkipsARunThatIsNotRunning(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	lf := newLeaseFlow(t, db)
	keyA := string(leaseKey(lf.tableA))
	cron := lf.cronHoldingTableA(t)
	stuck := lf.triggerSingleNode(t, lf.tableA)
	cancelled := lf.triggerSingleNode(t, lf.tableA)
	cancelByID(t, db, cancelled.tenant, cancelled.r.RunID(), "alice", "not needed")
	// A cancel ends a run's pending tasks, so only a defect leaves a cancelling
	// run with a task still waiting.
	_, err := db.Exec(`UPDATE runs SET status = 'cancelling', cancelled_at = now() WHERE run_id = $1`, stuck.r.RunID())
	require.NoError(t, err)
	cron.report(t, keyA, 0, run.AttemptSucceeded, "")
	metrics := &leaseMetrics{}

	require.NoError(t, lf.waker(metrics, discardLogger()).Pass(ctx))

	_, held := leaseOn(t, db, lf.tenant, lf.tableA)
	assert.False(t, held, "no lease is taken for a run that is not running")
	requireWaiting(t, db, stuck.taskID(lf.tableA), "the cancelling run's task is left as it was")
	assert.Empty(t, stuck.attempts(t, keyA))
	assert.Empty(t, stuck.dispatches(t))
	assert.Equal(t, leaseWait{Status: run.TaskStatusCancelled}, waitOf(t, db, cancelled.taskID(lf.tableA)), "the cancel ended the task's wait")
	assert.Empty(t, cancelled.dispatches(t))
	assert.Empty(t, metrics.granted)
	assert.Equal(t, []int{1}, metrics.waiters, "the cancelling run's task still waits")
}

// failStalledUnderLock does for runID what the attempt reconcile does for a
// run it listed as stalled: in one transaction it locks the run, checks it
// under the lock (granting its waiting tasks next in line for a free node, or
// failing it as stalled when it still is), and saves it with its events. It
// reports whether the run was failed.
func failStalledUnderLock(t *testing.T, db *sqlx.DB, tenant string, runID uuid.UUID) bool {
	t.Helper()
	ctx := context.Background()
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, runID)
	})
	var check run.StallCheck
	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		r, err := u.Run().LoadRunForUpdate(ctx, tenant, runID)
		require.NoError(t, err)
		check, err = r.FailStalled(ctx, u.TaskCollection(tenant), u.TaskGraph(tenant), testTaskDefaults, u.Clock().Now())
		require.NoError(t, err)
		if !check.Stalled && len(check.Granted) == 0 {
			return
		}
		require.NoError(t, u.Run().SaveRun(ctx, r))
		require.NoError(t, u.Outbox().Append(ctx, check.Events, uuid.Nil))
	})
	return check.Stalled
}

// Two single-node runs wait for the cron run's table_a, and the younger one has
// been idle longer than run.StallAfter. While the cron task holds table_a the
// younger run waits behind a holder in flight; once the cron task's success
// frees the node it waits behind the older single-node run's task, the node's
// next waiter; and once the lease waker grants table_a to that task it waits
// behind a holder in flight again. The stall listing leaves it out throughout,
// and the re-check under its lock counts its task as waiting behind the older
// run's task, so the run is not failed and its task keeps waiting.
func TestFailStalled_AWaitBehindAnOlderWaiterGrantedTheNodeIsNotFailed(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	lf := newLeaseFlow(t, db)
	cron := lf.cronHoldingTableA(t)
	older := lf.triggerSingleNode(t, lf.tableA)
	younger := lf.triggerSingleNode(t, lf.tableA)
	_, err := db.Exec(`UPDATE runs SET last_progress_at = now() - interval '10 minutes' WHERE run_id = $1`,
		younger.r.RunID())
	require.NoError(t, err)
	listed := stalledRunsOf(t, db, lf.tenant, run.StallAfter)
	require.NotContains(t, listed, younger.r.RunID(), "the cron task holding table_a is in flight")

	cron.report(t, string(leaseKey(lf.tableA)), 0, run.AttemptSucceeded, "")
	listed = stalledRunsOf(t, db, lf.tenant, run.StallAfter)
	require.NotContains(t, listed, younger.r.RunID(), "with table_a free, the younger run waits behind the older one")
	require.NoError(t, lf.waker(&leaseMetrics{}, discardLogger()).Pass(ctx))
	lease, held := leaseOn(t, db, lf.tenant, lf.tableA)
	require.True(t, held)
	require.Equal(t, heldLease{TaskID: older.taskID(lf.tableA), RunID: older.r.RunID()}, lease,
		"the waker granted table_a to the older waiter")
	listed = stalledRunsOf(t, db, lf.tenant, run.StallAfter)
	require.NotContains(t, listed, younger.r.RunID(), "the older run's task holding table_a is in flight")

	assert.False(t, failStalledUnderLock(t, db, lf.tenant, younger.r.RunID()),
		"under the lock the younger run waits behind the older run's task in flight")
	status, _ := younger.run(t)
	assert.Equal(t, run.RunStatusRunning, status)
	requireWaiting(t, db, younger.taskID(lf.tableA), "the waiting task is not skipped")
	assert.Empty(t, younger.dispatches(t))
}

// A single-node run has waited longer than run.StallAfter behind the cron run's
// table_a. The cron task's success releases the node, and no notification
// drives a waker pass (no listener runs), so the stall listing names the
// waiting run. The attempt reconcile runs the lease waker's pass before it
// looks for stalled runs: the pass grants table_a to the waiting task and
// dispatches it, so the run is not failed.
func TestAttemptReconcile_GrantsAFreedNodeBeforeItLooksForStalledRuns(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	lf := newLeaseFlow(t, db)
	keyA := string(leaseKey(lf.tableA))
	cron := lf.cronHoldingTableA(t)
	single := lf.triggerSingleNode(t, lf.tableA)
	singleTask := single.taskID(lf.tableA)
	requireWaiting(t, db, singleTask, "the single-node task waits for the cron task's lease")
	waitedTenMinutes(t, db, single.r.RunID(), singleTask)
	cron.report(t, keyA, 0, run.AttemptSucceeded, "")
	_, held := leaseOn(t, db, lf.tenant, lf.tableA)
	require.False(t, held, "the cron task's success released table_a")
	listed := stalledRunsOf(t, db, lf.tenant, run.StallAfter)
	require.Contains(t, listed, single.r.RunID(), "with table_a free and not yet granted, the run waits on nothing in flight")
	metrics := &leaseMetrics{}
	loop := attemptreconcile.New(scopedUoW(db, lf.tenant), testTaskDefaults, metrics,
		lf.waker(metrics, discardLogger()).Pass, discardLogger())

	require.NoError(t, loop.Pass(ctx))

	status, _ := single.run(t)
	assert.Equal(t, run.RunStatusRunning, status, "the waiting run is not failed as stalled")
	assert.Equal(t, leaseWait{Status: run.TaskStatusDispatched}, waitOf(t, db, singleTask), "the waker's pass dispatched the task")
	lease, held := leaseOn(t, db, lf.tenant, lf.tableA)
	require.True(t, held)
	assert.Equal(t, heldLease{TaskID: singleTask, RunID: single.r.RunID()}, lease)
	dispatches := single.dispatches(t)
	require.Len(t, dispatches, 1)
	assert.Equal(t, singleTask.String(), dispatches[0].TaskID)
	assert.Equal(t, int32(0), dispatches[0].Attempt)
	assert.Len(t, metrics.granted, 1)
}

// waitedTenMinutes moves the run's progress clock, and the time its task began
// to wait for its node's lease, ten minutes into the past: past
// run.StallAfter.
func waitedTenMinutes(t *testing.T, db *sqlx.DB, runID, taskID uuid.UUID) {
	t.Helper()
	_, err := db.Exec(`UPDATE runs SET last_progress_at = now() - interval '10 minutes' WHERE run_id = $1`, runID)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE tasks SET lease_wait_since = now() - interval '10 minutes' WHERE task_id = $1`, taskID)
	require.NoError(t, err)
}

// A single-node run has waited longer than run.StallAfter behind the cron run's
// table_a. The waker's pass before the stall scan finds table_a still held; the
// cron task's success releases it right after, and the attempt reconcile takes
// the waiting run's lock before any waker pass serves the node. The stall
// listing names the run, and under its lock the reconcile grants table_a to the
// waiting task, the node's oldest waiter, and dispatches it instead of failing
// the run.
func TestAttemptReconcile_GrantsANodeFreedAfterTheWakersPassUnderTheRunsLock(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	lf := newLeaseFlow(t, db)
	keyA := string(leaseKey(lf.tableA))
	cron := lf.cronHoldingTableA(t)
	single := lf.triggerSingleNode(t, lf.tableA)
	singleTask := single.taskID(lf.tableA)
	requireWaiting(t, db, singleTask, "the single-node task waits for the cron task's lease")
	waitedTenMinutes(t, db, single.r.RunID(), singleTask)
	metrics := &leaseMetrics{}
	waker := lf.waker(metrics, discardLogger())
	var listed []uuid.UUID
	beforeStall := func(ctx context.Context) error {
		if err := waker.Pass(ctx); err != nil {
			return err
		}
		require.Empty(t, metrics.granted, "the waker's pass finds table_a held")
		cron.report(t, keyA, 0, run.AttemptSucceeded, "")
		listed = stalledRunsOf(t, db, lf.tenant, run.StallAfter)
		return nil
	}
	loop := attemptreconcile.New(scopedUoW(db, lf.tenant), testTaskDefaults, metrics,
		beforeStall, discardLogger())

	require.NoError(t, loop.Pass(ctx))

	require.Contains(t, listed, single.r.RunID(), "with table_a free and not yet granted, the listing names the waiting run")
	status, _ := single.run(t)
	assert.Equal(t, run.RunStatusRunning, status, "the waiting run is not failed as stalled")
	assert.Equal(t, leaseWait{Status: run.TaskStatusDispatched}, waitOf(t, db, singleTask), "the reconcile dispatched the task")
	lease, held := leaseOn(t, db, lf.tenant, lf.tableA)
	require.True(t, held)
	assert.Equal(t, heldLease{TaskID: singleTask, RunID: single.r.RunID()}, lease)
	job, _, _ := single.task(t, keyA)
	assert.Equal(t, []attemptRow{{Attempt: 0, JobName: job, Status: "dispatched"}}, single.attempts(t, keyA))
	dispatches := single.dispatches(t)
	require.Len(t, dispatches, 1)
	assert.Equal(t, singleTask.String(), dispatches[0].TaskID)
	assert.Equal(t, int32(0), dispatches[0].Attempt)
	require.Len(t, metrics.granted, 1, "the reconcile's grant observes the wait")
	assert.GreaterOrEqual(t, metrics.granted[0], 9*time.Minute)
}

// More runs wait than one waker pass grants: leasewaker.GrantLimit+1 running
// runs, each waiting longer than run.StallAfter for a node of its own, and
// every node free. The waker's pass before the stall scan grants GrantLimit of
// them, the longest waiting first; the stall listing names the run left, and
// under its lock the reconcile grants that run's node and dispatches its task.
// No run is failed.
func TestAttemptReconcile_GrantsTheWaiterBeyondTheWakersLimitInsteadOfFailingIt(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	n := leasewaker.GrantLimit + 1
	suffixes := make([]string, 0, n)
	for i := range n {
		suffixes = append(suffixes, fmt.Sprintf("n%03d", i))
	}
	tables := leaseTables(suffixes...)
	tenant := storeEdges(t, db, "limit-"+tables[0], nil, tables...)
	base := time.Now().UTC().Add(-time.Hour)
	runIDs := make([]uuid.UUID, 0, n)
	taskIDs := make([]uuid.UUID, 0, n)
	for i, table := range tables {
		runID, ids := pendingTasks(t, db, tenant, table)
		waitSince(t, db, ids[table], base.Add(time.Duration(i)*time.Millisecond))
		runIDs = append(runIDs, runID)
		taskIDs = append(taskIDs, ids[table])
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM node_leases WHERE tenant_id = $1`, tenant)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = ANY($1)`, pq.Array(runIDs))
	})
	_, err := db.Exec(`UPDATE runs SET last_progress_at = now() - interval '10 minutes' WHERE run_id = ANY($1)`,
		pq.Array(runIDs))
	require.NoError(t, err)
	metrics := &leaseMetrics{}
	waker := leasewaker.New(scopedUoW(db, tenant), testTaskDefaults, metrics, discardLogger())
	loop := attemptreconcile.New(scopedUoW(db, tenant), testTaskDefaults, metrics,
		waker.Pass, discardLogger())

	require.NoError(t, loop.Pass(ctx))

	var runStatuses []string
	require.NoError(t, db.Select(&runStatuses, `SELECT DISTINCT status FROM runs WHERE run_id = ANY($1)`,
		pq.Array(runIDs)))
	assert.Equal(t, []string{string(run.RunStatusRunning)}, runStatuses, "no waiting run is failed as stalled")
	var dispatched, leases, rows int
	require.NoError(t, db.Get(&dispatched, `SELECT count(*) FROM tasks WHERE task_id = ANY($1) AND status = 'dispatched'
		AND wait_reason IS NULL`, pq.Array(taskIDs)))
	require.NoError(t, db.Get(&leases, `SELECT count(*) FROM node_leases WHERE tenant_id = $1 AND task_id = ANY($2)`,
		tenant, pq.Array(taskIDs)))
	require.NoError(t, db.Get(&rows, `SELECT count(*) FROM state_outbox WHERE aggregate_id = ANY($1) AND stream_name = $2`,
		pq.Array(runIDs), streams.TaskDispatchV1))
	assert.Equal(t, n, dispatched, "every waiting task is dispatched")
	assert.Equal(t, n, leases, "and holds its node's lease")
	assert.Equal(t, n, rows, "one dispatch each")
	assert.Equal(t, leaseWait{Status: run.TaskStatusDispatched}, waitOf(t, db, taskIDs[n-1]),
		"the waiter the waker's limit left is dispatched under its run's lock")
	assert.Len(t, metrics.granted, n)
}

// staleWaitOn stores a running run of tenant whose only task waits for the
// lease of table's node since since, with a progress clock ten minutes old, and
// returns the run and the task.
func staleWaitOn(t *testing.T, db *sqlx.DB, tenant, table string, since time.Time) (uuid.UUID, uuid.UUID) {
	t.Helper()
	runID, ids := pendingTasks(t, db, tenant, table)
	waitSince(t, db, ids[table], since)
	_, err := db.Exec(`UPDATE runs SET last_progress_at = now() - interval '10 minutes' WHERE run_id = $1`, runID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, runID)
	})
	return runID, ids[table]
}

// A task waiting for a free node behind an older waiter of a running run is
// queued: the next grant of the node serves the older waiter. It counts as in
// flight, so the stall listing leaves its run out and the re-check under the
// run's lock neither fails the run nor grants the node. The older waiter is
// the one with the earlier wait, or, for waits that began together, the lower
// task id, the order the lease waker grants in.
func TestStalledRuns_AWaitQueuedBehindAnOlderWaiterOfARunningRunIsNotStalled(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	since := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	for name, gap := range map[string]time.Duration{"an earlier wait": time.Minute, "a wait that began together": 0} {
		t.Run(name, func(t *testing.T) {
			table := leaseTables("queued")[0]
			tenant := storeEdges(t, db, "queued-"+table, nil, table)
			t.Cleanup(func() {
				_, _ = db.ExecContext(context.Background(), `DELETE FROM node_leases WHERE tenant_id = $1`, tenant)
			})
			firstRun, firstTask := staleWaitOn(t, db, tenant, table, since)
			secondRun, secondTask := staleWaitOn(t, db, tenant, table, since.Add(gap))
			olderRun, olderTask, youngerRun, youngerTask := firstRun, firstTask, secondRun, secondTask
			if gap == 0 && secondTask.String() < firstTask.String() {
				olderRun, olderTask, youngerRun, youngerTask = secondRun, secondTask, firstRun, firstTask
			}
			all, err := postgres.NewLeaseRepository(db, nil).Grantable(ctx, 1000)
			require.NoError(t, err)
			head := waitersOf(all, tenant)
			require.Len(t, head, 1)
			require.Equal(t, olderTask, head[0].TaskID, "the lease waker grants the node to the waiter the in-flight check calls older")
			require.Equal(t, olderRun, head[0].RunID)

			listed := stalledRunsOf(t, db, tenant, run.StallAfter)
			assert.NotContains(t, listed, youngerRun, "the younger waiter is queued behind the older one")
			assert.Contains(t, listed, olderRun, "the oldest waiter of a free node is a candidate its lock grants")

			assert.False(t, failStalledUnderLock(t, db, tenant, youngerRun), "the re-check under the lock leaves the queued wait")
			f := statusFlow{db: db, tenant: tenant, r: hydrated(t, db, tenant, youngerRun)}
			status, _ := f.run(t)
			assert.Equal(t, run.RunStatusRunning, status)
			requireWaiting(t, db, youngerTask, "the queued task keeps waiting")
			assert.False(t, leaseOf(t, db, youngerTask), "the node is left to the older waiter")
		})
	}
}

// A task waits for a free node behind an older waiter whose run is cancelling:
// a grant never serves a run that is not running, so the older wait queues no
// one. The younger waiter's run, idle past run.StallAfter, is listed, and under
// its lock its task is granted the node and dispatched rather than failed.
func TestFailStalled_AWaitBehindAnOlderWaiterOfACancellingRunIsGranted(t *testing.T) {
	db := newTestDB(t)
	table := leaseTables("dead_head")[0]
	tenant := storeEdges(t, db, "dead-head-"+table, nil, table)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM node_leases WHERE tenant_id = $1`, tenant)
	})
	since := time.Now().UTC().Add(-time.Hour)
	cancellingRun, cancellingTask := staleWaitOn(t, db, tenant, table, since)
	// A cancel ends a run's pending tasks, so only a defect leaves a cancelling
	// run with a task still waiting.
	_, err := db.Exec(`UPDATE runs SET status = 'cancelling', cancelled_at = now(), last_progress_at = now()
		WHERE run_id = $1`, cancellingRun)
	require.NoError(t, err)
	waitingRun, waitingTask := staleWaitOn(t, db, tenant, table, since.Add(time.Minute))

	listed := stalledRunsOf(t, db, tenant, run.StallAfter)
	require.Contains(t, listed, waitingRun, "a waiter of a cancelling run queues no one")

	assert.False(t, failStalledUnderLock(t, db, tenant, waitingRun), "the run is granted, not failed")

	f := statusFlow{db: db, tenant: tenant, r: hydrated(t, db, tenant, waitingRun)}
	status, _ := f.run(t)
	assert.Equal(t, run.RunStatusRunning, status)
	assert.Equal(t, leaseWait{Status: run.TaskStatusDispatched}, waitOf(t, db, waitingTask))
	lease, held := leaseOn(t, db, tenant, table)
	require.True(t, held)
	assert.Equal(t, heldLease{TaskID: waitingTask, RunID: waitingRun}, lease)
	dispatches := f.dispatches(t)
	require.Len(t, dispatches, 1)
	assert.Equal(t, waitingTask.String(), dispatches[0].TaskID)
	requireWaiting(t, db, cancellingTask, "the cancelling run's task is left as it was")
}

// hydrated loads the run runID as the aggregate.
func hydrated(t *testing.T, db *sqlx.DB, tenant string, runID uuid.UUID) *run.Run {
	t.Helper()
	r, err := newPlanningUoW(db).Run().GetRun(context.Background(), tenant, runID)
	require.NoError(t, err)
	return r
}

// grantFailingQueue is a node of its own tenant whose oldest waiter cannot be
// granted the node: a defect stored an attempt 0 of the never-dispatched task,
// so every dispatch of it collides with that row. A younger waiter of another
// running run queues behind it.
type grantFailingQueue struct {
	tenant, table     string
	headRun, headTask uuid.UUID
	nextRun, nextTask uuid.UUID
}

// newGrantFailingQueue stores the queue, with the head's run idle for idle.
func newGrantFailingQueue(t *testing.T, db *sqlx.DB, idle time.Duration) grantFailingQueue {
	t.Helper()
	q := grantFailingQueue{table: leaseTables("grant_fails")[0]}
	q.tenant = storeEdges(t, db, "grant-fails-"+q.table, nil, q.table)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM node_leases WHERE tenant_id = $1`, q.tenant)
	})
	since := time.Now().UTC().Add(-time.Hour)
	q.headRun, q.headTask = staleWaitOn(t, db, q.tenant, q.table, since)
	_, err := db.Exec(`INSERT INTO task_attempts (tenant_id, task_id, attempt, job_name, status) VALUES ((SELECT tenant_id FROM tasks WHERE task_id = $1), $1, 0, $2, 'failed')`, q.headTask, q.table)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE runs SET last_progress_at = now() - make_interval(secs => $2::float8) WHERE run_id = $1`,
		q.headRun, idle.Seconds())
	require.NoError(t, err)
	q.nextRun, q.nextTask = staleWaitOn(t, db, q.tenant, q.table, since.Add(time.Minute))
	return q
}

// runStatusOf reads the run's status and failure reason.
func runStatusOf(t *testing.T, db *sqlx.DB, runID uuid.UUID) (string, *string) {
	t.Helper()
	var row struct {
		Status string  `db:"status"`
		Reason *string `db:"failure_reason"`
	}
	require.NoError(t, db.Get(&row, `SELECT status, failure_reason FROM runs WHERE run_id = $1`, runID))
	return row.Status, row.Reason
}

// A defect makes the grant of a node's oldest waiter fail on every try. The
// waker's pass fails on it and serves no one behind it, and under the run's
// lock the stall check's grant fails too. Once the run has made no progress
// for twice run.StallAfter the reconcile checks it again without the grant and
// fails it as stalled, logging the grant's error: its waiting task is skipped
// and leaves the node's queue, and the next waker pass grants the node to the
// younger waiter.
func TestAttemptReconcile_FailsAWaiterWhoseGrantKeepsFailingSoItsQueueMoves(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	q := newGrantFailingQueue(t, db, 2*run.StallAfter+time.Minute)
	metrics := &leaseMetrics{}
	waker := leasewaker.New(scopedUoW(db, q.tenant), testTaskDefaults, metrics, discardLogger())
	var logs bytes.Buffer
	loop := attemptreconcile.New(scopedUoW(db, q.tenant), testTaskDefaults, metrics,
		waker.Pass, slog.New(slog.NewTextHandler(&logs, nil)))

	require.NoError(t, loop.Pass(ctx))

	status, reason := runStatusOf(t, db, q.headRun)
	assert.Equal(t, string(run.RunStatusFailed), status)
	require.NotNil(t, reason)
	assert.Equal(t, string(run.RunFailureStalled), *reason)
	assert.Equal(t, leaseWait{Status: run.TaskStatusSkipped}, waitOf(t, db, q.headTask), "the head leaves the node's queue")
	_, held := leaseOn(t, db, q.tenant, q.table)
	assert.False(t, held, "no grant committed")
	requireWaiting(t, db, q.nextTask, "the younger waiter waits for the next grant")
	var failedLines []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "level=ERROR") && strings.Contains(line, q.headRun.String()) && strings.Contains(line, "grant_error=") {
			failedLines = append(failedLines, line)
		}
	}
	assert.Len(t, failedLines, 1, "the failure is logged once, with the grant's error")

	require.NoError(t, waker.Pass(ctx))

	assert.Equal(t, leaseWait{Status: run.TaskStatusDispatched}, waitOf(t, db, q.nextTask), "the queue moved to the younger waiter")
	lease, held := leaseOn(t, db, q.tenant, q.table)
	require.True(t, held)
	assert.Equal(t, heldLease{TaskID: q.nextTask, RunID: q.nextRun}, lease)
	status, _ = runStatusOf(t, db, q.nextRun)
	assert.Equal(t, string(run.RunStatusRunning), status)
}

// A run idle for more than run.StallAfter but less than twice that whose
// waiting task's grant fails is left as it is: the stall listing names it and
// the check under its lock tries the grant, but a failed grant ends a run only
// once it has been idle for twice run.StallAfter, so a grant that failed on a
// transient error is tried again by the next pass. The pass returns the
// grant's error.
func TestAttemptReconcile_AFailedGrantLeavesARunIdleForLessThanTheBound(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	q := newGrantFailingQueue(t, db, run.StallAfter+time.Minute)
	loop := attemptreconcile.New(scopedUoW(db, q.tenant), testTaskDefaults, &leaseMetrics{},
		nil, discardLogger())
	listed := stalledRunsOf(t, db, q.tenant, run.StallAfter)
	require.Contains(t, listed, q.headRun)

	err := loop.Pass(ctx)

	require.ErrorIs(t, err, run.ErrGrantFailed)
	status, reason := runStatusOf(t, db, q.headRun)
	assert.Equal(t, string(run.RunStatusRunning), status)
	assert.Nil(t, reason)
	requireWaiting(t, db, q.headTask, "the head keeps waiting")
	requireWaiting(t, db, q.nextTask, "and so does the waiter behind it")
	_, held := leaseOn(t, db, q.tenant, q.table)
	assert.False(t, held)
}

// A pass deletes the lease of a task that ended without releasing it, counts
// and logs the repair as an error, and grants the freed node to its oldest
// waiter in the same pass; the waiter gauge counts the task still waiting. A
// later pass finds nothing to repair.
func TestLeaseWaker_RepairsAndCounts(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	lf := newLeaseFlow(t, db)
	cron := lf.cronHoldingTableA(t)
	cronTask := cron.taskID(lf.tableA)
	first := lf.triggerSingleNode(t, lf.tableA)
	second := lf.triggerSingleNode(t, lf.tableA)
	// A defect ends the cron task without the report that releases its lease.
	_, err := db.Exec(`UPDATE tasks SET status = 'succeeded' WHERE task_id = $1`, cronTask)
	require.NoError(t, err)
	require.True(t, leaseOf(t, db, cronTask))
	var logs bytes.Buffer
	metrics := &leaseMetrics{}
	waker := lf.waker(metrics, slog.New(slog.NewTextHandler(&logs, nil)))

	require.NoError(t, waker.Pass(ctx))

	assert.False(t, leaseOf(t, db, cronTask), "the repair deleted the ended task's lease")
	assert.Equal(t, 1, metrics.repaired)
	var errorLines []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "level=ERROR") {
			errorLines = append(errorLines, line)
		}
	}
	require.Len(t, errorLines, 1, "one error per repaired lease")
	assert.Contains(t, errorLines[0], cronTask.String())
	assert.Contains(t, errorLines[0], string(leaseKey(lf.tableA)))
	lease, held := leaseOn(t, db, lf.tenant, lf.tableA)
	require.True(t, held)
	assert.Equal(t, heldLease{TaskID: first.taskID(lf.tableA), RunID: first.r.RunID()}, lease,
		"the repaired node went to its oldest waiter in the same pass")
	requireWaiting(t, db, second.taskID(lf.tableA), "the second waiter waits behind the first")
	assert.Equal(t, []int{1}, metrics.waiters)

	require.NoError(t, waker.Pass(ctx))
	assert.Equal(t, 1, metrics.repaired, "nothing is left to repair")
	assert.Equal(t, []int{1, 1}, metrics.waiters)
}
