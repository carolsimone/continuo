package postgres_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/attemptreconcile"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// keyOf names attempt n of the run's task at key, with the run's tenant.
func (f statusFlow) keyOf(key string, n int32) repository.AttemptKey {
	return repository.AttemptKey{TenantID: f.tenant, TaskID: run.TaskIDFor(f.tenant, f.r.RunID(), topology.NodeKey(key)), Attempt: n}
}

// ref names the run with its tenant.
func (f statusFlow) ref() repository.RunRef {
	return repository.RunRef{TenantID: f.tenant, RunID: f.r.RunID()}
}

// indexOf returns the position of item in items, or -1.
func indexOf[T comparable](items []T, item T) int {
	for i, x := range items {
		if x == item {
			return i
		}
	}
	return -1
}

// makeDue moves the next check of every open attempt of the run into the past.
func (f statusFlow) makeDue(t *testing.T) {
	t.Helper()
	_, err := f.db.Exec(`
		UPDATE task_attempts a SET next_check_at = now() - interval '1 second'
		FROM tasks t WHERE t.task_id = a.task_id AND t.run_id = $1
		  AND a.status IN ('dispatched', 'queued', 'running')`, f.r.RunID())
	require.NoError(t, err)
}

// An attempt is due once its next check has passed, while it is open and its
// run active, the longest due first.
func TestAttemptRepository_DueAttemptsAreTheOpenDueAttemptsOfActiveRuns(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	repo := postgres.NewAttemptRepository(db, nil)

	due, err := repo.DueAttempts(ctx, 500)
	require.NoError(t, err)
	assert.NotContains(t, due, f.keyOf("e2e_schema.ftable_a", 0), "dispatched under five minutes ago")

	f.makeDue(t)
	f.report(t, "e2e_schema.ftable_b", 0, run.AttemptSucceeded, "")
	due, err = repo.DueAttempts(ctx, 500)
	require.NoError(t, err)
	assert.Contains(t, due, f.keyOf("e2e_schema.ftable_a", 0))
	assert.NotContains(t, due, f.keyOf("e2e_schema.ftable_b", 0), "an ended attempt is never asked about")

	cancelRun(t, db, f.r)
	due, err = repo.DueAttempts(ctx, 500)
	require.NoError(t, err)
	assert.NotContains(t, due, f.keyOf("e2e_schema.ftable_a", 0), "nothing of a cancelled run is asked about with a dispatch")
}

// The due attempts of every tenant are listed in one due order, each named
// with its own tenant, the longest due first whatever its tenant; each one
// listed can be locked under the tenant it is named with, and under no other.
func TestAttemptRepository_DueAttemptsSpanEveryTenant(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	mine := storeFailureSchedule(t, db)
	shape := releaseShape{name: "due-" + uuid.NewString()[:8], nodes: 2, levels: 1, fanIn: 1}
	other := storeShape(t, db, shape)
	f := statusFlow{db: db, tenant: mine, r: startScheduleRun(t, db, mine, "e2e-schedule-failure", model.OperationRun)}
	g := statusFlow{db: db, tenant: other, r: startScheduleRun(t, db, other, shape.schedule(), model.OperationRun)}
	_, err := db.Exec(`UPDATE task_attempts a SET next_check_at = now() - interval '1 hour'
		FROM tasks t WHERE t.task_id = a.task_id AND t.run_id = $1`, g.r.RunID())
	require.NoError(t, err)
	f.makeDue(t)
	var taskIDs []uuid.UUID
	require.NoError(t, db.Select(&taskIDs, `SELECT task_id FROM tasks WHERE tenant_id = $1 AND run_id = $2 AND dispatched_at IS NOT NULL`,
		other, g.r.RunID()))
	require.NotEmpty(t, taskIDs)
	theirs := make([]repository.AttemptKey, 0, len(taskIDs))
	for _, id := range taskIDs {
		theirs = append(theirs, repository.AttemptKey{TenantID: other, TaskID: id, Attempt: 0})
	}

	due, err := postgres.NewAttemptRepository(db, nil).DueAttempts(ctx, 500)
	require.NoError(t, err)

	ours := f.keyOf("e2e_schema.ftable_a", 0)
	require.Contains(t, due, ours, "the first tenant's due attempt is listed with its tenant")
	for _, key := range theirs {
		require.Contains(t, due, key, "the other tenant's due attempt is listed with its tenant")
		assert.Less(t, indexOf(due, key), indexOf(due, ours), "an attempt due longer comes first, whatever its tenant")
	}
	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		q, found, err := u.Attempts().LockQuestion(ctx, theirs[0])
		require.NoError(t, err)
		require.True(t, found, "a listed attempt is locked under the tenant it is named with")
		assert.Equal(t, g.r.RunID(), q.RunID)
		_, found, err = u.Attempts().LockQuestion(ctx, repository.AttemptKey{TenantID: mine, TaskID: theirs[0].TaskID, Attempt: theirs[0].Attempt})
		require.NoError(t, err)
		assert.False(t, found, "no other tenant can lock it")
	})
}

// A question carries what the attempt's dispatch carries, read from its task,
// its run and the node of its run's release.
func TestAttemptRepository_LockQuestionCarriesTheAttemptsDispatch(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	f.report(t, "e2e_schema.ftable_a", 0, run.AttemptQueued, "")
	f.makeDue(t)
	jobName, _, _ := f.task(t, "e2e_schema.ftable_a")

	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		q, found, err := u.Attempts().LockQuestion(ctx, f.keyOf("e2e_schema.ftable_a", 0))
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, run.Question{
			RunID: f.r.RunID(), TenantID: tenant, ScheduleName: "e2e-schedule-failure", TaskID: f.keyOf("e2e_schema.ftable_a", 0).TaskID,
			Attempt: 0, Status: run.AttemptQueued, JobName: jobName, Operation: model.OperationRun,
			Node: topology.Node{Key: "e2e_schema.ftable_a", UniqueID: "e2e_schema.ftable_a", ServiceName: "service-1",
				SchemaName: "e2e_schema", TableName: "ftable_a", NodeType: "dbt-model", ContentHash: "h-ftable_a",
				ImageTag: "service-1:5", ScheduleName: "e2e-schedule-failure"},
		}, q)
	})
}

// An attempt another transaction holds is skipped, not waited for.
func TestAttemptRepository_LockQuestionSkipsAnAttemptAnotherTransactionHolds(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	f.makeDue(t)
	key := f.keyOf("e2e_schema.ftable_a", 0)

	holder, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer holder.Rollback() //nolint:errcheck
	_, err = holder.Exec(`SELECT 1 FROM task_attempts WHERE task_id = $1 AND attempt = 0 FOR UPDATE`, key.TaskID)
	require.NoError(t, err)

	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		start := time.Now()
		_, found, err := u.Attempts().LockQuestion(ctx, key)
		require.NoError(t, err)
		assert.False(t, found)
		assert.Less(t, time.Since(start), 2*time.Second, "the lock is skipped, not waited for")
	})
}

// A question is counted and moves the next check on the database clock.
func TestAttemptRepository_MarkQuestionedCountsTheQuestionAndMovesTheNextCheck(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	key := f.keyOf("e2e_schema.ftable_a", 0)

	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		require.NoError(t, u.Attempts().MarkQuestioned(ctx, key, 10*time.Minute))
	})

	var checks int
	require.NoError(t, db.Get(&checks, `SELECT checks_sent FROM task_attempts WHERE task_id = $1 AND attempt = 0`, key.TaskID))
	assert.Equal(t, 1, checks)
	require.NotNil(t, f.dueIn(t, "e2e_schema.ftable_a", 0))
	assert.InDelta(t, (10 * time.Minute).Seconds(), f.dueIn(t, "e2e_schema.ftable_a", 0).Seconds(), 5)
}

// A run is stalled when it is active, has no task in flight and its progress
// clock is older than the idle time; a run with a queued attempt never is.
// One scan lists the stalled runs of every tenant, each named with its own.
func TestAttemptRepository_StalledRunsHaveNothingInFlightAndNoRecentProgress(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	idleTenant := storeEdges(t, db, "e2e-schedule-idle", [][2]string{{"ftable_a", "ftable_b"}}, "ftable_a", "ftable_b")
	idle := startScheduleRun(t, db, idleTenant, "e2e-schedule-idle", model.OperationRun)
	waiting := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	_, err := db.Exec(`UPDATE tasks SET status = 'pending', dispatched_at = NULL WHERE run_id = $1`, idle.RunID())
	require.NoError(t, err)
	waiting.report(t, "e2e_schema.ftable_a", 0, run.AttemptQueued, "")
	_, err = db.Exec(`UPDATE runs SET last_progress_at = now() - interval '10 minutes' WHERE run_id = ANY($1)`,
		pq.Array([]uuid.UUID{idle.RunID(), waiting.r.RunID()}))
	require.NoError(t, err)

	repo := postgres.NewAttemptRepository(db, nil)
	got, err := repo.StalledRuns(ctx, 5*time.Minute, 500)
	require.NoError(t, err)
	assert.Contains(t, got, repository.RunRef{TenantID: idleTenant, RunID: idle.RunID()}, "a stalled run is named with its tenant")
	assert.NotContains(t, got, waiting.ref(), "a run waiting for a slot is never stalled")
	assert.NotContains(t, got, repository.RunRef{TenantID: tenant, RunID: idle.RunID()}, "a run is named with its own tenant only")

	got, err = repo.StalledRuns(ctx, 15*time.Minute, 500)
	require.NoError(t, err)
	assert.NotContains(t, got, repository.RunRef{TenantID: idleTenant, RunID: idle.RunID()}, "a run that moved within the idle time is not stalled")
}

// An attempt a stop was requested of is never asked about with a dispatch,
// whether it is handed out by DueAttempts or locked by LockQuestion; the
// run's other due attempts still are.
func TestAttemptRepository_DueAttemptsSkipStopRequestedAttempts(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	f.makeDue(t)
	asked, other := f.keyOf("e2e_schema.ftable_a", 0), f.keyOf("e2e_schema.ftable_b", 0)
	_, err := db.Exec(`UPDATE task_attempts SET stop_requested_at = now() WHERE task_id = $1 AND attempt = $2`, asked.TaskID, asked.Attempt)
	require.NoError(t, err)

	due, err := postgres.NewAttemptRepository(db, nil).DueAttempts(ctx, 500)
	require.NoError(t, err)
	assert.NotContains(t, due, asked)
	assert.Contains(t, due, other)

	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		_, found, err := u.Attempts().LockQuestion(ctx, asked)
		require.NoError(t, err)
		assert.False(t, found, "a stop-requested attempt cannot be locked for a dispatch question")
		_, found, err = u.Attempts().LockQuestion(ctx, other)
		require.NoError(t, err)
		assert.True(t, found)
	})
}

// A cancelling run with nothing in flight and an old progress clock is
// stalled; one with a task still stopping never is.
func TestAttemptRepository_StalledRunsCoverACancellingRunWithNothingStopping(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	nothingStopping, _ := insertTasks(t, db, tenant, map[string]run.TaskStatus{"ftable_a": run.TaskStatusCancelled, "ftable_b": run.TaskStatusSucceeded})
	stillStopping, _ := insertTasks(t, db, tenant, map[string]run.TaskStatus{"ftable_a": run.TaskStatusStopping, "ftable_b": run.TaskStatusCancelled})
	_, err := db.Exec(`UPDATE runs SET status = 'cancelling', last_progress_at = now() - interval '10 minutes'
		WHERE run_id = ANY($1)`, pq.Array([]uuid.UUID{nothingStopping, stillStopping}))
	require.NoError(t, err)

	got := stalledRunsOf(t, db, tenant, 5*time.Minute)

	assert.Contains(t, got, nothingStopping)
	assert.NotContains(t, got, stillStopping, "a stopping task is in flight until its attempt reports")
}

// leaseWaitRun stores a running run of tenant whose only task, at table, is
// pending and waits for its node's write lease, with a progress clock ten
// minutes old, and returns the run id.
func leaseWaitRun(t *testing.T, db *sqlx.DB, tenant, table string) uuid.UUID {
	t.Helper()
	runID, ids := pendingTasks(t, db, tenant, table)
	_, err := db.Exec(`UPDATE tasks SET wait_reason = 'node_lease', lease_wait_since = now() - interval '10 minutes'
		WHERE task_id = $1`, ids[table])
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE runs SET last_progress_at = now() - interval '10 minutes' WHERE run_id = $1`, runID)
	require.NoError(t, err)
	return runID
}

// A run whose task waits for a node lease held by a task in flight
// (dispatched, queued, running or stopping) is waiting on that task, not
// stalled, however old its progress clock; the same run is stalled while no
// task holds the node.
func TestStalledRuns_ALeaseWaitBehindAnInFlightHolderIsNotStalled(t *testing.T) {
	db := newTestDB(t)
	tenant := leaseTenant(t, db)
	for _, status := range run.InFlightTaskStatuses() {
		t.Run(string(status), func(t *testing.T) {
			table := leaseTables("held")[0]
			waiting := leaseWaitRun(t, db, tenant, table)
			got := stalledRunsOf(t, db, tenant, 5*time.Minute)
			require.Contains(t, got, waiting, "no task holds the node yet")

			holderRun, holder := insertTasks(t, db, tenant, map[string]run.TaskStatus{table: status})
			require.NoError(t, insertLease(db, tenant, "e2e_schema."+table, holder[table], holderRun))

			got = stalledRunsOf(t, db, tenant, 5*time.Minute)
			assert.NotContains(t, got, waiting, "a task waiting behind a %s holder counts as in flight", status)
		})
	}
}

// stalledRunsOf lists, among the stalled runs of every tenant, the ids of
// tenant's runs idle for longer than idle.
func stalledRunsOf(t *testing.T, db *sqlx.DB, tenant string, idle time.Duration) []uuid.UUID {
	t.Helper()
	refs, err := postgres.NewAttemptRepository(db, nil).StalledRuns(context.Background(), idle, 500)
	require.NoError(t, err)
	return runIDsOf(tenant, refs)
}

// runIDsOf returns, in order, the ids of the runs of refs that are tenant's.
func runIDsOf(tenant string, refs []repository.RunRef) []uuid.UUID {
	var ids []uuid.UUID
	for _, r := range refs {
		if r.TenantID == tenant {
			ids = append(ids, r.RunID)
		}
	}
	return ids
}

// onlyTask returns the id of the run's one task.
func onlyTask(t *testing.T, db *sqlx.DB, runID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	require.NoError(t, db.Get(&id, `SELECT task_id FROM tasks WHERE run_id = $1`, runID))
	return id
}

// A run whose task waits for the lease of a node no task holds, or of one held
// by a task that has ended (a lease the repair deletes), waits on nothing in
// flight: once its progress clock is old the stall listing names it. Under the
// run's lock the two differ. The task waiting on the free node is that node's
// next waiter, so the re-check grants it the node and dispatches it rather
// than failing the run. No grant can serve the wait behind the ended holder,
// which only a defect in state leaves, so the re-check fails that run.
func TestStalledRuns_ALeaseWaitOnAFreeNodeIsListedAndGrantedUnderTheLock(t *testing.T) {
	db := newTestDB(t)
	tables := leaseTables("free", "ended")
	free, ended := tables[0], tables[1]
	tenant := storeEdges(t, db, "free-"+free, nil, free, ended)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM node_leases WHERE tenant_id = $1`, tenant)
	})
	waitingOnFree := leaseWaitRun(t, db, tenant, free)
	holderRun, holder := insertTasks(t, db, tenant, map[string]run.TaskStatus{ended: run.TaskStatusSucceeded})
	require.NoError(t, insertLease(db, tenant, "e2e_schema."+ended, holder[ended], holderRun))
	waitingOnEnded := leaseWaitRun(t, db, tenant, ended)

	got := stalledRunsOf(t, db, tenant, 5*time.Minute)

	assert.Contains(t, got, waitingOnFree, "a wait on a node no task holds is a wait on nothing in flight")
	assert.Contains(t, got, waitingOnEnded, "a lease whose holder has ended keeps nothing in flight")

	assert.False(t, failStalledUnderLock(t, db, tenant, waitingOnFree), "the re-check under the lock grants the free node")
	freeTask := onlyTask(t, db, waitingOnFree)
	assert.Equal(t, leaseWait{Status: run.TaskStatusDispatched}, waitOf(t, db, freeTask))
	lease, held := leaseOn(t, db, tenant, free)
	require.True(t, held)
	assert.Equal(t, heldLease{TaskID: freeTask, RunID: waitingOnFree}, lease)
	var status string
	require.NoError(t, db.Get(&status, `SELECT status FROM runs WHERE run_id = $1`, waitingOnFree))
	assert.Equal(t, string(run.RunStatusRunning), status)

	assert.True(t, failStalledUnderLock(t, db, tenant, waitingOnEnded), "the re-check fails a wait behind a holder that has ended")
}

// makeStopsDue moves the next check of every attempt of the run into the past.
func (f statusFlow) makeStopsDue(t *testing.T, ago time.Duration) {
	t.Helper()
	_, err := f.db.Exec(`
		UPDATE task_attempts a SET next_check_at = now() - make_interval(secs => $2::float8)
		FROM tasks t WHERE t.task_id = a.task_id AND t.run_id = $1
		  AND a.status IN ('dispatched', 'queued', 'running')`, f.r.RunID(), ago.Seconds())
	require.NoError(t, err)
}

// A run is due a re-sent cancel once it is cancelling and an attempt asked to
// stop has a next check that has passed, the longest due first; a running run
// and a cancelling run whose stops are not yet due are not. One scan lists the
// due cancels of every tenant in one due order, each run named with its own
// tenant.
func TestAttemptRepository_DueStopsAreCancellingRunsWithAQuietStop(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	mine := storeFailureSchedule(t, db)
	shape := releaseShape{name: "stops-" + uuid.NewString()[:8], nodes: 2, levels: 1, fanIn: 1}
	other := storeShape(t, db, shape)
	f := statusFlow{db: db, tenant: mine, r: startScheduleRun(t, db, mine, "e2e-schedule-failure", model.OperationRun)}
	g := statusFlow{db: db, tenant: other, r: startScheduleRun(t, db, other, shape.schedule(), model.OperationRun)}
	repo := postgres.NewAttemptRepository(db, nil)

	f.makeStopsDue(t, time.Minute)
	got, err := repo.DueStops(ctx, 500)
	require.NoError(t, err)
	assert.NotContains(t, got, f.ref(), "a running run is asked about with dispatches, not a cancel")

	cancelRun(t, db, f.r)
	cancelRun(t, db, g.r)
	got, err = repo.DueStops(ctx, 500)
	require.NoError(t, err)
	assert.NotContains(t, got, f.ref(), "the stops were requested just now: the first re-send is five minutes away")
	assert.NotContains(t, got, g.ref())

	f.makeStopsDue(t, time.Hour)
	g.makeStopsDue(t, 2*time.Hour)
	got, err = repo.DueStops(ctx, 500)
	require.NoError(t, err)
	require.Contains(t, got, f.ref(), "a due cancel is named with its run's tenant")
	require.Contains(t, got, g.ref(), "and so is another tenant's")
	assert.Less(t, indexOf(got, g.ref()), indexOf(got, f.ref()), "the longest due first, whatever its tenant")
	assert.Len(t, uniqueRefs(got), len(got), "a run with several quiet stops is listed once")

	pair := leaseTables("first", "second")
	pairTenant := storeSchedules(t, db, pair...)
	first := statusFlow{db: db, tenant: pairTenant, r: startScheduleRun(t, db, pairTenant, pair[0], model.OperationRun)}
	second := statusFlow{db: db, tenant: pairTenant, r: startScheduleRun(t, db, pairTenant, pair[1], model.OperationRun)}
	cancelRun(t, db, first.r)
	cancelRun(t, db, second.r)
	first.makeStopsDue(t, time.Hour)
	second.makeStopsDue(t, 30*24*time.Hour)
	got, err = repo.DueStops(ctx, 500)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{second.r.RunID(), first.r.RunID()}, runIDsOf(pairTenant, got), "the longest due first")

	only, err := repo.DueStops(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, []repository.RunRef{second.ref()}, only, "the limit keeps the longest due")
}

// storeSchedules stores release rel-1 of a new tenant holding one dbt model of
// schema e2e_schema per table, each in a schedule named after its table, and
// returns the tenant.
func storeSchedules(t *testing.T, db *sqlx.DB, tables ...string) string {
	t.Helper()
	specs := make([]topology.NodeSpec, 0, len(tables))
	for _, table := range tables {
		specs = append(specs, topology.NodeSpec{
			UniqueID: "e2e_schema." + table, ServiceName: "service-1", SchemaName: "e2e_schema", TableName: table,
			NodeType: "dbt-model", ContentHash: "h-" + table, ImageTag: "service-1:5", ScheduleName: table,
		})
	}
	tenant := planningTenant(t, db)
	rel, err := topology.NewRelease(tenant, "rel-1", 1, "s3://continuo/k", "sha", specs)
	require.NoError(t, err)
	ctx := context.Background()
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = postgres.NewTopologyRepository(db, tx).SaveRelease(ctx, rel)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return tenant
}

func uniqueRefs(refs []repository.RunRef) map[repository.RunRef]struct{} {
	out := map[repository.RunRef]struct{}{}
	for _, r := range refs {
		out[r] = struct{}{}
	}
	return out
}

// LockStops returns the cancel the run was given and every open attempt asked
// to stop, with how often each was re-sent; an attempt that ended is not
// named, and a run that is not cancelling yields nothing.
func TestAttemptRepository_LockStopsCarriesTheRunsCancelAndItsOpenStops(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	f.makeStopsDue(t, time.Minute)

	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		_, found, err := u.Attempts().LockStops(ctx, f.ref())
		require.NoError(t, err)
		assert.False(t, found, "a running run has no stop to send again")
	})

	cancelRun(t, db, f.r)
	a, b := f.keyOf("e2e_schema.ftable_a", 0), f.keyOf("e2e_schema.ftable_b", 0)
	jobA, _, _ := f.task(t, "e2e_schema.ftable_a")
	jobB, _, _ := f.task(t, "e2e_schema.ftable_b")
	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		_, found, err := u.Attempts().LockStops(ctx, f.ref())
		require.NoError(t, err)
		assert.False(t, found, "no stop is due yet")
	})

	f.makeStopsDue(t, time.Minute)
	_, err := db.Exec(`UPDATE task_attempts SET checks_sent = 2 WHERE task_id = $1 AND attempt = 0`, b.TaskID)
	require.NoError(t, err)
	want := []run.StopTarget{{TaskID: a.TaskID, Attempt: 0, JobName: jobA}, {TaskID: b.TaskID, Attempt: 0, JobName: jobB, ChecksSent: 2}}
	slices.SortFunc(want, func(x, y run.StopTarget) int { return strings.Compare(x.TaskID.String(), y.TaskID.String()) })
	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		_, found, err := u.Attempts().LockStops(ctx, repository.RunRef{TenantID: "stops-" + uuid.NewString(), RunID: f.r.RunID()})
		require.NoError(t, err)
		assert.False(t, found, "another tenant has no stop of the run to send")
		q, found, err := u.Attempts().LockStops(ctx, f.ref())
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, f.r.RunID(), q.RunID)
		assert.Equal(t, "e2e-schedule-failure", q.ScheduleName)
		assert.Equal(t, "user", q.CancelledBy)
		assert.Equal(t, want, q.Attempts)
		assert.WithinDuration(t, time.Now(), q.CancelledAt, time.Minute)
	})

	_, err = db.Exec(`UPDATE task_attempts SET status = 'stopped', next_check_at = NULL WHERE task_id = $1 AND attempt = 0`, a.TaskID)
	require.NoError(t, err)
	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		q, found, err := u.Attempts().LockStops(ctx, f.ref())
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, q.Attempts, 1, "an attempt that answered is not named")
		assert.Equal(t, b.TaskID, q.Attempts[0].TaskID)
	})
}

// An attempt another transaction holds is skipped: the run's cancel is sent
// again by the pass that finds it free.
func TestAttemptRepository_LockStopsSkipsARunWhoseAttemptsAreHeld(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	cancelRun(t, db, f.r)
	f.makeStopsDue(t, time.Minute)
	held := f.keyOf("e2e_schema.ftable_a", 0)

	holder, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer holder.Rollback() //nolint:errcheck
	_, err = holder.Exec(`SELECT 1 FROM task_attempts WHERE task_id = $1 AND attempt = 0 FOR UPDATE`, held.TaskID)
	require.NoError(t, err)

	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		start := time.Now()
		q, found, err := u.Attempts().LockStops(ctx, f.ref())
		require.NoError(t, err)
		require.True(t, found)
		assert.Less(t, time.Since(start), 2*time.Second, "the lock is skipped, not waited for")
		for _, a := range q.Attempts {
			assert.NotEqual(t, held.TaskID, a.TaskID, "the held attempt is left for the next pass")
		}
	})
}

// The cancelling runs are counted, and those cancelled longer ago than the
// limit are counted as overdue.
func TestAttemptRepository_CountCancellingCountsTheOverdue(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	repo := postgres.NewAttemptRepository(db, nil)
	total0, overdue0, err := repo.CountCancelling(ctx, 15*time.Minute)
	require.NoError(t, err)

	fresh, _ := insertTasks(t, db, tenant, map[string]run.TaskStatus{"ftable_a": run.TaskStatusStopping})
	old, _ := insertTasks(t, db, tenant, map[string]run.TaskStatus{"ftable_a": run.TaskStatusStopping})
	_, err = db.Exec(`UPDATE runs SET status = 'cancelling', cancelled_at = now() WHERE run_id = $1`, fresh)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE runs SET status = 'cancelling', cancelled_at = now() - interval '16 minutes' WHERE run_id = $1`, old)
	require.NoError(t, err)

	total, overdue, err := repo.CountCancelling(ctx, 15*time.Minute)
	require.NoError(t, err)
	assert.Equal(t, total0+2, total)
	assert.Equal(t, overdue0+1, overdue)
}

// A pass against the real tables sends the cancel of a cancelling run again
// once, counts the re-send on each stopped attempt and moves its next check to
// the second stop window, and leaves the run alone until then.
func TestAttemptReconcile_AResentCancelMovesEachStopToItsNextWindow(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	cancelRun(t, db, f.r)
	f.makeStopsDue(t, time.Second)
	loop := attemptreconcile.New(scopedUoW(db, tenant), testTaskDefaults, noRunMetrics{}, nil, discardLogger())
	outboxRows := func() int {
		var n int
		require.NoError(t, db.Get(&n, `SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, f.r.RunID()))
		return n
	}
	before := outboxRows()

	require.NoError(t, loop.Pass(ctx))

	assert.Equal(t, before+1, outboxRows(), "one cancel for the run, however many attempts it names")
	for _, key := range []string{"e2e_schema.ftable_a", "e2e_schema.ftable_b"} {
		var checks int
		require.NoError(t, db.Get(&checks, `SELECT checks_sent FROM task_attempts WHERE task_id = $1 AND attempt = 0`,
			f.keyOf(key, 0).TaskID))
		assert.Equal(t, 1, checks, key)
		require.NotNil(t, f.dueIn(t, key, 0), key)
		assert.InDelta(t, run.NextStopCheckAfter(1).Seconds(), f.dueIn(t, key, 0).Seconds(), 5, key)
	}

	require.NoError(t, loop.Pass(ctx))
	assert.Equal(t, before+1, outboxRows(), "the stops are not due again until their window passes")
}

// The re-sent cancel is stored as a run.cancelled:v1 row that names every
// attempt of the run asked to stop that has not answered, each with its Job,
// so execution-controller can stop an attempt it holds no row for.
func TestAttemptReconcile_AResentCancelRowNamesTheUnansweredAttempts(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	cancelRun(t, db, f.r)
	f.makeStopsDue(t, time.Second)
	var requested []struct {
		TaskID  string `db:"task_id"`
		Attempt int32  `db:"attempt"`
		JobName string `db:"job_name"`
	}
	require.NoError(t, db.Select(&requested, `
		SELECT a.task_id::text AS task_id, a.attempt, a.job_name FROM task_attempts a
		JOIN tasks t ON t.task_id = a.task_id
		WHERE t.run_id = $1 AND a.stop_requested_at IS NOT NULL
		ORDER BY a.task_id::text`, f.r.RunID()))
	require.Len(t, requested, 2, "both running attempts were asked to stop")
	stops := make([]pkgevents.CancelledAttempt, 0, len(requested))
	for _, a := range requested {
		stops = append(stops, pkgevents.CancelledAttempt{TaskID: a.TaskID, Attempt: a.Attempt, JobName: a.JobName})
	}
	loop := attemptreconcile.New(scopedUoW(db, tenant), testTaskDefaults, noRunMetrics{}, nil, discardLogger())

	require.NoError(t, loop.Pass(ctx))

	var rows []struct {
		EventType string `db:"event_type"`
		Payload   []byte `db:"payload"`
	}
	require.NoError(t, db.Select(&rows, `SELECT event_type, payload FROM state_outbox WHERE aggregate_id = $1 AND stream_name = $2`,
		f.r.RunID(), streams.RunCancelledV1))
	require.Len(t, rows, 1, "one re-sent cancel")
	assert.Equal(t, streams.RunCancelledV1, rows[0].EventType)
	tenantID, wire, err := pkgoutbox.UnwrapTenant(rows[0].Payload)
	require.NoError(t, err)
	assert.Equal(t, f.tenant, tenantID, "the row stores the run's tenant")
	var cancel pkgevents.RunCancelled
	require.NoError(t, json.Unmarshal(wire, &cancel))
	assert.Equal(t, f.r.RunID().String(), cancel.RunID)
	assert.Equal(t, "user", cancel.CancelledBy)
	assert.Equal(t, stops, cancel.Attempts)
	for _, a := range cancel.Attempts {
		assert.NotEmpty(t, a.JobName, "each attempt names its Job")
	}
}

// inUoW runs fn inside a unit of work's transaction and commits it.
func inUoW(t *testing.T, db *sqlx.DB, fn func(u *postgres.PostgresUnitOfWork)) {
	t.Helper()
	u := newPlanningUoW(db)
	require.NoError(t, u.Begin(context.Background()))
	defer u.Rollback()
	fn(u)
	require.NoError(t, u.Commit())
}

// cancelRun cancels r through its aggregate.
func cancelRun(t *testing.T, db *sqlx.DB, r *run.Run) {
	t.Helper()
	ctx := context.Background()
	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		loaded, err := u.Run().LoadRunForUpdate(ctx, r.TenantID(), r.RunID())
		require.NoError(t, err)
		_, err = loaded.Cancel(ctx, u.TaskCollection(loaded.TenantID()), "user", "", u.Clock().Now())
		require.NoError(t, err)
		require.NoError(t, u.Run().SaveRun(ctx, loaded))
	})
}
