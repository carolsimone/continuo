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
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/attemptreconcile"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// keyOf names attempt n of the run's task at key.
func (f statusFlow) keyOf(key string, n int32) repository.AttemptKey {
	return repository.AttemptKey{TaskID: run.TaskIDFor(f.tenant, f.r.ScheduleID(), topology.NodeKey(key)), Attempt: n}
}

// makeDue moves the next check of every open attempt of the run into the past.
func (f statusFlow) makeDue(t *testing.T) {
	t.Helper()
	_, err := f.db.Exec(`
		UPDATE task_attempts a SET next_check_at = now() - interval '1 second'
		FROM task_tracker t WHERE t.task_id = a.task_id AND t.schedule_id = $1
		  AND a.status IN ('dispatched', 'queued', 'running')`, f.r.ScheduleID())
	require.NoError(t, err)
}

// An attempt is due once its next check has passed, while it is open and its
// run active, the longest due first.
func TestAttemptRepository_DueAttemptsAreTheOpenDueAttemptsOfActiveRuns(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	repo := postgres.NewAttemptRepository(db, nil, tenant)

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

// A tenant's loop is handed only the due attempts it can ask about: those of
// its own runs. An attempt it cannot lock is never handed out, so it can never
// hold the head of the due order and starve the rest.
func TestAttemptRepository_DueAttemptsAreTheTenantsOwn(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	mine := storeFailureSchedule(t, db)
	shape := releaseShape{name: "due-" + uuid.NewString()[:8], nodes: 2, levels: 1, fanIn: 1}
	other := storeShape(t, db, shape)
	f := statusFlow{db: db, tenant: mine, r: startScheduleRun(t, db, mine, "e2e-schedule-failure", model.OperationRun)}
	g := statusFlow{db: db, tenant: other, r: startScheduleRun(t, db, other, shape.schedule(), model.OperationRun)}
	_, err := db.Exec(`UPDATE task_attempts a SET next_check_at = now() - interval '1 hour'
		FROM task_tracker t WHERE t.task_id = a.task_id AND t.schedule_id = $1`, g.r.ScheduleID())
	require.NoError(t, err)
	f.makeDue(t)

	due, err := postgres.NewAttemptRepository(db, nil, mine).DueAttempts(ctx, 1)
	require.NoError(t, err)
	require.Len(t, due, 1)
	assert.Contains(t, []repository.AttemptKey{f.keyOf("e2e_schema.ftable_a", 0), f.keyOf("e2e_schema.ftable_b", 0)}, due[0],
		"the other tenant's longer-due attempts are not handed to this tenant's loop")
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
		q, found, err := u.Attempts(tenant).LockQuestion(ctx, f.keyOf("e2e_schema.ftable_a", 0))
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, run.Question{
			RunID: f.r.ScheduleID(), ScheduleName: "e2e-schedule-failure", TaskID: f.keyOf("e2e_schema.ftable_a", 0).TaskID,
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
		_, found, err := u.Attempts(tenant).LockQuestion(ctx, key)
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
		require.NoError(t, u.Attempts(tenant).MarkQuestioned(ctx, key, 10*time.Minute))
	})

	var checks int
	require.NoError(t, db.Get(&checks, `SELECT checks_sent FROM task_attempts WHERE task_id = $1 AND attempt = 0`, key.TaskID))
	assert.Equal(t, 1, checks)
	require.NotNil(t, f.dueIn(t, "e2e_schema.ftable_a", 0))
	assert.InDelta(t, (10 * time.Minute).Seconds(), f.dueIn(t, "e2e_schema.ftable_a", 0).Seconds(), 5)
}

// A run is stalled when it is active, has no task in flight and its progress
// clock is older than the idle time; a run with a queued attempt never is.
func TestAttemptRepository_StalledRunsHaveNothingInFlightAndNoRecentProgress(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	idleTenant := storeEdges(t, db, "e2e-schedule-idle", [][2]string{{"ftable_a", "ftable_b"}}, "ftable_a", "ftable_b")
	idle := startScheduleRun(t, db, idleTenant, "e2e-schedule-idle", model.OperationRun)
	waiting := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	_, err := db.Exec(`UPDATE task_tracker SET status = 'pending', dispatched_at = NULL WHERE schedule_id = $1`, idle.ScheduleID())
	require.NoError(t, err)
	waiting.report(t, "e2e_schema.ftable_a", 0, run.AttemptQueued, "")
	_, err = db.Exec(`UPDATE scheduler_tracker SET last_heartbeat_at = now() - interval '10 minutes' WHERE schedule_id = ANY($1)`,
		pq.Array([]uuid.UUID{idle.ScheduleID(), waiting.r.ScheduleID()}))
	require.NoError(t, err)

	got, err := postgres.NewAttemptRepository(db, nil, tenant).StalledRuns(ctx, 5*time.Minute, 100)
	require.NoError(t, err)

	assert.Contains(t, got, idle.ScheduleID())
	assert.NotContains(t, got, waiting.r.ScheduleID(), "a run waiting for a slot is never stalled")

	got, err = postgres.NewAttemptRepository(db, nil, tenant).StalledRuns(ctx, 15*time.Minute, 100)
	require.NoError(t, err)
	assert.NotContains(t, got, idle.ScheduleID(), "a run that moved within the idle time is not stalled")
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

	due, err := postgres.NewAttemptRepository(db, nil, tenant).DueAttempts(ctx, 500)
	require.NoError(t, err)
	assert.NotContains(t, due, asked)
	assert.Contains(t, due, other)

	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		_, found, err := u.Attempts(tenant).LockQuestion(ctx, asked)
		require.NoError(t, err)
		assert.False(t, found, "a stop-requested attempt cannot be locked for a dispatch question")
		_, found, err = u.Attempts(tenant).LockQuestion(ctx, other)
		require.NoError(t, err)
		assert.True(t, found)
	})
}

// A cancelling run with nothing in flight and an old progress clock is
// stalled; one with a task still stopping never is.
func TestAttemptRepository_StalledRunsCoverACancellingRunWithNothingStopping(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	nothingStopping, _ := insertTasks(t, db, map[string]run.TaskStatus{"ftable_a": run.TaskStatusCancelled, "ftable_b": run.TaskStatusSucceeded})
	stillStopping, _ := insertTasks(t, db, map[string]run.TaskStatus{"ftable_a": run.TaskStatusStopping, "ftable_b": run.TaskStatusCancelled})
	_, err := db.Exec(`UPDATE scheduler_tracker SET status = 'cancelling', last_heartbeat_at = now() - interval '10 minutes'
		WHERE schedule_id = ANY($1)`, pq.Array([]uuid.UUID{nothingStopping, stillStopping}))
	require.NoError(t, err)

	got, err := postgres.NewAttemptRepository(db, nil, tenant).StalledRuns(ctx, 5*time.Minute, 500)
	require.NoError(t, err)

	assert.Contains(t, got, nothingStopping)
	assert.NotContains(t, got, stillStopping, "a stopping task is in flight until its attempt reports")
}

// leaseWaitRun stores a running run whose only task, at table, is pending and
// waits for its node's write lease, with a progress clock ten minutes old, and
// returns the run id.
func leaseWaitRun(t *testing.T, db *sqlx.DB, table string) uuid.UUID {
	t.Helper()
	runID, ids := pendingTasks(t, db, table)
	_, err := db.Exec(`UPDATE task_tracker SET wait_reason = 'node_lease', lease_wait_since = now() - interval '10 minutes'
		WHERE task_id = $1`, ids[table])
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE scheduler_tracker SET last_heartbeat_at = now() - interval '10 minutes' WHERE schedule_id = $1`, runID)
	require.NoError(t, err)
	return runID
}

// A run whose task waits for a node lease held by a task in flight
// (dispatched, queued, running or stopping) is waiting on that task, not
// stalled, however old its progress clock; the same run is stalled while no
// task holds the node.
func TestStalledRuns_ALeaseWaitBehindAnInFlightHolderIsNotStalled(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := leaseTenant(t, db)
	repo := postgres.NewAttemptRepository(db, nil, tenant)
	for _, status := range run.InFlightTaskStatuses() {
		t.Run(string(status), func(t *testing.T) {
			table := leaseTables("held")[0]
			waiting := leaseWaitRun(t, db, table)
			got, err := repo.StalledRuns(ctx, 5*time.Minute, 500)
			require.NoError(t, err)
			require.Contains(t, got, waiting, "no task holds the node yet")

			holderRun, holder := insertTasks(t, db, map[string]run.TaskStatus{table: status})
			require.NoError(t, insertLease(db, tenant, "e2e_schema."+table, holder[table], holderRun))

			got, err = repo.StalledRuns(ctx, 5*time.Minute, 500)
			require.NoError(t, err)
			assert.NotContains(t, got, waiting, "a task waiting behind a %s holder counts as in flight", status)
		})
	}
}

// A run whose task waits for the lease of a node no task holds, or one held by
// a task that has ended (a lease the repair deletes), waits on nothing in
// flight: once its progress clock is old it is stalled, and the re-check under
// the run's lock fails it.
func TestStalledRuns_ALeaseWaitOnAFreeNodeIsStalled(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := leaseTenant(t, db)
	tables := leaseTables("free", "ended")
	free, ended := tables[0], tables[1]
	waitingOnFree := leaseWaitRun(t, db, free)
	holderRun, holder := insertTasks(t, db, map[string]run.TaskStatus{ended: run.TaskStatusSucceeded})
	require.NoError(t, insertLease(db, tenant, "e2e_schema."+ended, holder[ended], holderRun))
	waitingOnEnded := leaseWaitRun(t, db, ended)

	got, err := postgres.NewAttemptRepository(db, nil, tenant).StalledRuns(ctx, 5*time.Minute, 500)
	require.NoError(t, err)

	assert.Contains(t, got, waitingOnFree, "a wait on a node no task holds is a wait on nothing in flight")
	assert.Contains(t, got, waitingOnEnded, "a lease whose holder has ended keeps nothing in flight")
	assert.True(t, failStalledUnderLock(t, db, tenant, waitingOnFree), "the re-check under the lock fails a wait on a free node")
	assert.True(t, failStalledUnderLock(t, db, tenant, waitingOnEnded), "and a wait behind a holder that has ended")
}

// makeStopsDue moves the next check of every attempt of the run into the past.
func (f statusFlow) makeStopsDue(t *testing.T, ago time.Duration) {
	t.Helper()
	_, err := f.db.Exec(`
		UPDATE task_attempts a SET next_check_at = now() - make_interval(secs => $2::float8)
		FROM task_tracker t WHERE t.task_id = a.task_id AND t.schedule_id = $1
		  AND a.status IN ('dispatched', 'queued', 'running')`, f.r.ScheduleID(), ago.Seconds())
	require.NoError(t, err)
}

// A run is due a re-sent cancel once it is cancelling and an attempt asked to
// stop has a next check that has passed, the longest due first; a running run
// and a cancelling run whose stops are not yet due are not.
func TestAttemptRepository_DueStopsAreCancellingRunsWithAQuietStop(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	mine := storeFailureSchedule(t, db)
	shape := releaseShape{name: "stops-" + uuid.NewString()[:8], nodes: 2, levels: 1, fanIn: 1}
	other := storeShape(t, db, shape)
	f := statusFlow{db: db, tenant: mine, r: startScheduleRun(t, db, mine, "e2e-schedule-failure", model.OperationRun)}
	g := statusFlow{db: db, tenant: other, r: startScheduleRun(t, db, other, shape.schedule(), model.OperationRun)}
	repo := postgres.NewAttemptRepository(db, nil, mine)

	f.makeStopsDue(t, time.Minute)
	got, err := repo.DueStops(ctx, 500)
	require.NoError(t, err)
	assert.NotContains(t, got, f.r.ScheduleID(), "a running run is asked about with dispatches, not a cancel")

	cancelRun(t, db, f.r)
	cancelRun(t, db, g.r)
	got, err = repo.DueStops(ctx, 500)
	require.NoError(t, err)
	assert.NotContains(t, got, f.r.ScheduleID(), "the stops were requested just now: the first re-send is five minutes away")
	assert.NotContains(t, got, g.r.ScheduleID())

	f.makeStopsDue(t, time.Hour)
	g.makeStopsDue(t, 2*time.Hour)
	got, err = repo.DueStops(ctx, 500)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(got), 2)
	assert.Less(t, indexOf(got, g.r.ScheduleID()), indexOf(got, f.r.ScheduleID()), "the longest due first, of every tenant")
	assert.Len(t, uniqueIDs(got), len(got), "a run with several quiet stops is listed once")

	only, err := repo.DueStops(ctx, 1)
	require.NoError(t, err)
	assert.Len(t, only, 1)
}

func indexOf(ids []uuid.UUID, id uuid.UUID) int {
	for i, x := range ids {
		if x == id {
			return i
		}
	}
	return len(ids)
}

func uniqueIDs(ids []uuid.UUID) map[uuid.UUID]struct{} {
	out := map[uuid.UUID]struct{}{}
	for _, id := range ids {
		out[id] = struct{}{}
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
		_, found, err := u.Attempts(tenant).LockStops(ctx, f.r.ScheduleID())
		require.NoError(t, err)
		assert.False(t, found, "a running run has no stop to send again")
	})

	cancelRun(t, db, f.r)
	a, b := f.keyOf("e2e_schema.ftable_a", 0), f.keyOf("e2e_schema.ftable_b", 0)
	jobA, _, _ := f.task(t, "e2e_schema.ftable_a")
	jobB, _, _ := f.task(t, "e2e_schema.ftable_b")
	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		_, found, err := u.Attempts(tenant).LockStops(ctx, f.r.ScheduleID())
		require.NoError(t, err)
		assert.False(t, found, "no stop is due yet")
	})

	f.makeStopsDue(t, time.Minute)
	_, err := db.Exec(`UPDATE task_attempts SET checks_sent = 2 WHERE task_id = $1 AND attempt = 0`, b.TaskID)
	require.NoError(t, err)
	want := []run.StopTarget{{TaskID: a.TaskID, Attempt: 0, JobName: jobA}, {TaskID: b.TaskID, Attempt: 0, JobName: jobB, ChecksSent: 2}}
	slices.SortFunc(want, func(x, y run.StopTarget) int { return strings.Compare(x.TaskID.String(), y.TaskID.String()) })
	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		q, found, err := u.Attempts(tenant).LockStops(ctx, f.r.ScheduleID())
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, f.r.ScheduleID(), q.RunID)
		assert.Equal(t, "e2e-schedule-failure", q.ScheduleName)
		assert.Equal(t, "user", q.CancelledBy)
		assert.Equal(t, want, q.Attempts)
		assert.WithinDuration(t, time.Now(), q.CancelledAt, time.Minute)
	})

	_, err = db.Exec(`UPDATE task_attempts SET status = 'stopped', next_check_at = NULL WHERE task_id = $1 AND attempt = 0`, a.TaskID)
	require.NoError(t, err)
	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		q, found, err := u.Attempts(tenant).LockStops(ctx, f.r.ScheduleID())
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
		q, found, err := u.Attempts(tenant).LockStops(ctx, f.r.ScheduleID())
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
	repo := postgres.NewAttemptRepository(db, nil, tenant)
	total0, overdue0, err := repo.CountCancelling(ctx, 15*time.Minute)
	require.NoError(t, err)

	fresh, _ := insertTasks(t, db, map[string]run.TaskStatus{"ftable_a": run.TaskStatusStopping})
	old, _ := insertTasks(t, db, map[string]run.TaskStatus{"ftable_a": run.TaskStatusStopping})
	_, err = db.Exec(`UPDATE scheduler_tracker SET status = 'cancelling', cancelled_at = now() WHERE schedule_id = $1`, fresh)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE scheduler_tracker SET status = 'cancelling', cancelled_at = now() - interval '16 minutes' WHERE schedule_id = $1`, old)
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
	loop := attemptreconcile.New(func() uow.UnitOfWork { return newPlanningUoW(db) }, tenant, testTaskDefaults, noRunMetrics{}, nil, discardLogger())
	outboxRows := func() int {
		var n int
		require.NoError(t, db.Get(&n, `SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, f.r.ScheduleID()))
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
		JOIN task_tracker t ON t.task_id = a.task_id
		WHERE t.schedule_id = $1 AND a.stop_requested_at IS NOT NULL
		ORDER BY a.task_id::text`, f.r.ScheduleID()))
	require.Len(t, requested, 2, "both running attempts were asked to stop")
	stops := make([]pkgevents.CancelledAttempt, 0, len(requested))
	for _, a := range requested {
		stops = append(stops, pkgevents.CancelledAttempt{TaskID: a.TaskID, Attempt: a.Attempt, JobName: a.JobName})
	}
	loop := attemptreconcile.New(func() uow.UnitOfWork { return newPlanningUoW(db) }, tenant, testTaskDefaults, noRunMetrics{}, nil, discardLogger())

	require.NoError(t, loop.Pass(ctx))

	var rows []struct {
		EventType string `db:"event_type"`
		Payload   []byte `db:"payload"`
	}
	require.NoError(t, db.Select(&rows, `SELECT event_type, payload FROM state_outbox WHERE aggregate_id = $1 AND stream_name = $2`,
		f.r.ScheduleID(), streams.RunCancelledV1))
	require.Len(t, rows, 1, "one re-sent cancel")
	assert.Equal(t, streams.RunCancelledV1, rows[0].EventType)
	var cancel pkgevents.RunCancelled
	require.NoError(t, json.Unmarshal(rows[0].Payload, &cancel))
	assert.Equal(t, f.r.ScheduleID().String(), cancel.RunID)
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
		loaded, err := u.Run().LoadRunForUpdate(ctx, r.ScheduleID())
		require.NoError(t, err)
		_, err = loaded.Cancel(ctx, u.TaskCollection(pkgevents.DefaultTenantID), "user", "", u.Clock().Now())
		require.NoError(t, err)
		require.NoError(t, u.Run().SaveRun(ctx, loaded))
	})
}
