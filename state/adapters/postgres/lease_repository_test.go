package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/leasewaker"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// leaseKey is the node key of the e2e_schema table.
func leaseKey(table string) topology.NodeKey { return topology.NodeKey("e2e_schema." + table) }

// waitSince makes the pending task wait for its node's write lease since
// since.
func waitSince(t *testing.T, db *sqlx.DB, taskID uuid.UUID, since time.Time) {
	t.Helper()
	_, err := db.Exec(`UPDATE task_tracker SET wait_reason = 'node_lease', lease_wait_since = $2 WHERE task_id = $1 AND status = 'pending'`,
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
// waits only for its upstream tasks are not grantable, and every waiter of the
// tenant counts. Another tenant's waker sees none of them.
func TestLeaseRepository_GrantableIsTheOldestWaiterOfEachFreeNode(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tables := leaseTables("x", "y", "z", "v", "u")
	x, y, z, v, u := tables[0], tables[1], tables[2], tables[3], tables[4]
	tenant := storeEdges(t, db, "grantable-"+x, nil, tables...)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM node_leases WHERE tenant_id = $1`, tenant)
	})
	base := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)

	xOldRun, xOld := pendingTasks(t, db, x)
	waitSince(t, db, xOld[x], base)
	_, xNew := pendingTasks(t, db, x)
	waitSince(t, db, xNew[x], base.Add(2*time.Minute))

	holderRun, holder := insertTasks(t, db, map[string]run.TaskStatus{y: run.TaskStatusRunning})
	require.NoError(t, insertLease(db, tenant, string(leaseKey(y)), holder[y], holderRun))
	_, yWaiter := pendingTasks(t, db, y)
	waitSince(t, db, yWaiter[y], base.Add(-time.Minute))

	zStuckRun, zStuck := pendingTasks(t, db, z)
	waitSince(t, db, zStuck[z], base.Add(time.Minute))
	_, err := db.Exec(`UPDATE scheduler_tracker SET status = 'cancelling' WHERE schedule_id = $1`, zStuckRun)
	require.NoError(t, err)
	zRun, zNext := pendingTasks(t, db, z)
	waitSince(t, db, zNext[z], base.Add(3*time.Minute))

	vaRun, va := pendingTasks(t, db, v)
	vbRun, vb := pendingTasks(t, db, v)
	waitSince(t, db, va[v], base.Add(4*time.Minute))
	waitSince(t, db, vb[v], base.Add(4*time.Minute))
	vFirst := repository.LeaseWaiter{TaskID: va[v], RunID: vaRun, NodeKey: leaseKey(v), WaitingSince: base.Add(4 * time.Minute)}
	if vb[v].String() < va[v].String() {
		vFirst = repository.LeaseWaiter{TaskID: vb[v], RunID: vbRun, NodeKey: leaseKey(v), WaitingSince: base.Add(4 * time.Minute)}
	}

	pendingTasks(t, db, u)

	repo := postgres.NewLeaseRepository(db, nil, tenant)
	got, err := repo.Grantable(ctx, 100)
	require.NoError(t, err)
	assert.Equal(t, []repository.LeaseWaiter{
		{TaskID: xOld[x], RunID: xOldRun, NodeKey: leaseKey(x), WaitingSince: base},
		{TaskID: zNext[z], RunID: zRun, NodeKey: leaseKey(z), WaitingSince: base.Add(3 * time.Minute)},
		vFirst,
	}, inUTC(got))

	oldest, err := repo.Grantable(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, []repository.LeaseWaiter{{TaskID: xOld[x], RunID: xOldRun, NodeKey: leaseKey(x), WaitingSince: base}}, inUTC(oldest),
		"the limit keeps the longest waiting")

	n, err := repo.CountWaiters(ctx)
	require.NoError(t, err)
	assert.Equal(t, 7, n, "every waiting task of the tenant counts, whether its node is free or not")

	other := planningTenant(t, db)
	theirs, err := postgres.NewLeaseRepository(db, nil, other).Grantable(ctx, 100)
	require.NoError(t, err)
	assert.Empty(t, theirs, "a waiter of a run on this tenant's release is not another tenant's")
	n, err = postgres.NewLeaseRepository(db, nil, other).CountWaiters(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
}

// A lease whose holder task has ended is deleted and returned with the
// holder's status; a lease whose holder is in flight is kept, and another
// tenant's lease is left to that tenant's repair. A second repair finds
// nothing.
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
	runID, ids := insertTasks(t, db, byTable)
	for _, table := range tables {
		require.NoError(t, insertLease(db, tenant, string(leaseKey(table)), ids[table], runID))
	}
	otherTenant := leaseTenant(t, db)
	otherTable := leaseTables("other")[0]
	otherRun, other := insertTasks(t, db, map[string]run.TaskStatus{otherTable: run.TaskStatusSucceeded})
	require.NoError(t, insertLease(db, otherTenant, string(leaseKey(otherTable)), other[otherTable], otherRun))

	repaired, err := postgres.NewLeaseRepository(db, nil, tenant).RepairEnded(ctx)
	require.NoError(t, err)

	var want, gotLeases []repository.RepairedLease
	for _, table := range tables[:4] {
		want = append(want, repository.RepairedLease{TaskID: ids[table], RunID: runID, NodeKey: leaseKey(table), TaskStatus: byTable[table]})
	}
	for _, l := range repaired {
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
	assert.True(t, leaseOf(t, db, other[otherTable]), "another tenant's lease is its own repair's")

	again, err := postgres.NewLeaseRepository(db, nil, tenant).RepairEnded(ctx)
	require.NoError(t, err)
	assert.Empty(t, again)
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
	require.Equal(t, heldLease{TaskID: cron.taskID(lf.tableA), RunID: cron.r.ScheduleID()}, held)
	return cron
}

// triggerSingleNode creates a single-node run of table on the live release
// through TriggerSingleNodeRunHandler; the run, its tasks and its outbox rows
// are deleted when the test ends.
func (lf leaseFlow) triggerSingleNode(t *testing.T, table string) statusFlow {
	t.Helper()
	ctx := context.Background()
	h := handlers.NewTriggerSingleNodeRunHandler(runstart.Planner{TenantID: lf.tenant, Tasks: testTaskDefaults}, discardLogger())
	id, _, err := h.Handle(ctx, newPlanningUoW(lf.db), handlers.TriggerSingleNodeRunInput{
		Target:         run.NodeID{ServiceName: "service-1", SchemaName: "e2e_schema", TableName: table},
		MetadataSource: run.MetadataSourceLatest,
		Operation:      model.OperationRun,
		Initiator:      identity.System(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = lf.db.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, id)
		_, _ = lf.db.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, id)
	})
	r, err := newPlanningUoW(lf.db).Run().GetRun(ctx, id)
	require.NoError(t, err)
	return statusFlow{db: lf.db, tenant: lf.tenant, r: r}
}

// waker returns the lease waker of the flow's tenant.
func (lf leaseFlow) waker(metrics *leaseMetrics, logger *slog.Logger) *leasewaker.Waker {
	return leasewaker.New(func() uow.UnitOfWork { return newPlanningUoW(lf.db) }, lf.tenant, testTaskDefaults, metrics, logger)
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
	return run.TaskIDFor(f.tenant, f.r.ScheduleID(), leaseKey(table))
}

// dispatches returns the task.dispatch:v1 rows the run wrote to the outbox.
func (f statusFlow) dispatches(t *testing.T) []pkgevents.TaskDispatch {
	t.Helper()
	var payloads [][]byte
	require.NoError(t, f.db.Select(&payloads, `SELECT payload FROM state_outbox WHERE aggregate_id = $1 AND stream_name = $2`,
		f.r.ScheduleID(), streams.TaskDispatchV1))
	out := make([]pkgevents.TaskDispatch, 0, len(payloads))
	for _, p := range payloads {
		var d pkgevents.TaskDispatch
		require.NoError(t, json.Unmarshal(p, &d))
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
	assert.Equal(t, cron.r.ScheduleID(), lf.builtBy(t, lf.tableA))

	require.NoError(t, waker.Pass(ctx))

	assert.Equal(t, leaseWait{Status: run.TaskStatusDispatched}, waitOf(t, db, singleTask), "the grant dispatched the task and ended its wait")
	lease, held := leaseOn(t, db, lf.tenant, lf.tableA)
	require.True(t, held)
	assert.Equal(t, heldLease{TaskID: singleTask, RunID: single.r.ScheduleID()}, lease)
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
	assert.Equal(t, single.r.ScheduleID(), lf.builtBy(t, lf.tableA), "the later write is the one recorded")

	require.NoError(t, waker.Pass(ctx))
	assert.Equal(t, leaseWait{Status: run.TaskStatusDispatched}, waitOf(t, db, laterTask))
	lease, held = leaseOn(t, db, lf.tenant, lf.tableA)
	require.True(t, held)
	assert.Equal(t, heldLease{TaskID: laterTask, RunID: later.r.ScheduleID()}, lease)
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
	cancelByID(t, db, cancelled.r.ScheduleID(), "alice", "not needed")
	// A cancel ends a run's pending tasks, so only a defect leaves a cancelling
	// run with a task still waiting.
	_, err := db.Exec(`UPDATE scheduler_tracker SET status = 'cancelling', cancelled_at = now() WHERE schedule_id = $1`, stuck.r.ScheduleID())
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
	_, err := db.Exec(`UPDATE task_tracker SET status = 'succeeded' WHERE task_id = $1`, cronTask)
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
	assert.Equal(t, heldLease{TaskID: first.taskID(lf.tableA), RunID: first.r.ScheduleID()}, lease,
		"the repaired node went to its oldest waiter in the same pass")
	requireWaiting(t, db, second.taskID(lf.tableA), "the second waiter waits behind the first")
	assert.Equal(t, []int{1}, metrics.waiters)

	require.NoError(t, waker.Pass(ctx))
	assert.Equal(t, 1, metrics.repaired, "nothing is left to repair")
	assert.Equal(t, []int{1, 1}, metrics.waiters)
}
