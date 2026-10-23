package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
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
		_, err = loaded.Cancel(ctx, u.TaskCollection(), "user", "", u.Clock().Now())
		require.NoError(t, err)
		require.NoError(t, u.Run().SaveRun(ctx, loaded))
	})
}
