//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/routing"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jobsRoutingPolicy is the shipped default: every record takes the Kubernetes
// Job path.
func jobsRoutingPolicy() routing.Policy {
	return routing.NewPolicy(model.ExecutionModeJobs, nil)
}

// addDeployedJob inserts a Jobs-mode deployment for scheduleID that holds an
// execution slot and whose Kubernetes Job is running — the state every in-flight
// task of a schedule sits in.
func addDeployedJob(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID, table string, now time.Time) *model.Deployment {
	t.Helper()
	cmd := command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: scheduleID.String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public", TableName: table,
		JobName: "dbt-public-" + table, NodeType: "dbt-model", ImageTag: "sha-abc",
	}
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	dep := model.NewDeployment(cmd, nil, now)
	require.NoError(t, repo.Add(context.Background(), dep))
	require.NoError(t, dep.ReserveForDispatch(now))
	require.NoError(t, dep.MarkDeployed(now))
	require.NoError(t, repo.Save(context.Background(), dep))
	return dep
}

// TestScheduleCancelled_ReleasesEveryInFlightJobSlot drives the real
// schedule.cancelled:v1 handler over Postgres and pins that a cancelled
// schedule's in-flight Jobs give their execution slots back.
//
// Nothing else would: a deployment holds its slot until a transition releases
// it, and k8s-controller absorbs a cancelled schedule's Job result without
// emitting the terminal that would free it. Left in 'deployed', those rows would
// count against MAX_CONCURRENT_EXECUTIONS forever and the executor would
// eventually stop dispatching.
func TestScheduleCancelled_ReleasesEveryInFlightJobSlot(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	now := time.Now().UTC()

	scheduleID := uuid.New()
	inFlight := []*model.Deployment{
		addDeployedJob(t, db, scheduleID, "orders", now),
		addDeployedJob(t, db, scheduleID, "customers", now),
	}
	// A second schedule's in-flight Job must keep its slot: cancelling one
	// schedule may not disturb another's accounting.
	other := addDeployedJob(t, db, uuid.New(), "payments", now)

	before, err := repo.ActiveSlotCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, before)

	u := postgres.NewUnitOfWork(db, testLogger())
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, handlers.NewScheduleCancelledHandler(testLogger(), unusedPodTerminator{t: t}).Handle(
		ctx, u, events.ScheduleCancelled{ScheduleID: scheduleID}, uuid.Nil))
	require.NoError(t, u.Commit())

	for _, dep := range inFlight {
		got, err := repo.GetByID(ctx, dep.ID())
		require.NoError(t, err)
		assert.Equal(t, model.StatusCancelled, got.Status())
		assert.NotNil(t, got.SlotReleasedAt(), "a cancelled Job must hand its slot back")
	}

	untouched, err := repo.GetByID(ctx, other.ID())
	require.NoError(t, err)
	assert.Equal(t, model.StatusDeployed, untouched.Status())
	assert.Nil(t, untouched.SlotReleasedAt())

	after, err := repo.ActiveSlotCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, after, "only the other schedule's Job still holds a slot")

	var exists bool
	require.NoError(t, db.QueryRow(
		`SELECT EXISTS (SELECT 1 FROM cancelled_schedules WHERE schedule_id=$1)`, scheduleID).Scan(&exists))
	assert.True(t, exists, "the schedule is still recorded so later messages are dropped")
}

// waitUntilScheduleLockContended blocks until some backend is waiting on an
// ungranted advisory lock, i.e. a second transaction has genuinely reached the
// schedule lock and is parked on it. Polling pg_locks observes that state rather
// than guessing at it with a sleep: when the lock is not taken the wait simply
// never satisfies and the test fails on the timeout instead of passing by luck.
func waitUntilScheduleLockContended(t *testing.T, db *sqlx.DB) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		require.NoError(t, db.QueryRow(
			`SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted`).Scan(&waiting))
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no transaction ever blocked on the schedule lock — the enqueue guard " +
		"does not hold it, so a concurrent cancellation can miss the row it enqueues")
}

// TestScheduleCancelled_ReachesADeploymentEnqueuedConcurrently drives the exact
// interleaving that would otherwise strand an execution slot: a query.model
// enqueue reads the cancelled-schedule guard, and the schedule is cancelled
// before that enqueue commits.
//
// The cancel scan reads executor_deployments while the guard reads
// cancelled_schedules, so under READ COMMITTED nothing orders them by itself.
// Left unordered, the enqueued row is invisible to the scan, survives the
// cancellation, dispatches, and has its terminal absorbed — holding its slot
// with no event left to release it. The schedule lock is what makes the cancel
// wait for the enqueue and then see its row.
func TestScheduleCancelled_ReachesADeploymentEnqueuedConcurrently(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	scheduleID := uuid.New()

	// The enqueue runs its guard and writes its row, but does not commit: the
	// window this test targets is the one between the guard's read and that
	// commit.
	enqueue := postgres.NewUnitOfWork(db, testLogger())
	require.NoError(t, enqueue.Begin(ctx))
	evt := events.QueryModel{
		TaskID: uuid.New(), ScheduleID: scheduleID, ScheduleName: "daily",
		ServiceName: "dbt", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: "dbt-model", ImageTag: "sha-abc",
	}
	require.NoError(t, handlers.NewQueryModelHandler(jobsRoutingPolicy(), testLogger()).
		Handle(ctx, enqueue, evt, uuid.Nil))

	// The cancellation arrives while the enqueue is still open, and parks on the
	// schedule lock the guard holds.
	cancelled := make(chan error, 1)
	go func() {
		u := postgres.NewUnitOfWork(db, testLogger())
		if err := u.Begin(ctx); err != nil {
			cancelled <- err
			return
		}
		if err := handlers.NewScheduleCancelledHandler(testLogger(), unusedPodTerminator{t: t}).Handle(
			ctx, u, events.ScheduleCancelled{ScheduleID: scheduleID}, uuid.Nil); err != nil {
			_ = u.Rollback()
			cancelled <- err
			return
		}
		cancelled <- u.Commit()
	}()

	waitUntilScheduleLockContended(t, db)
	require.NoError(t, enqueue.Commit())
	require.NoError(t, <-cancelled)

	deps, err := repo.GetNonTerminalByScheduleForUpdate(ctx, scheduleID)
	require.NoError(t, err)
	assert.Empty(t, deps, "the concurrently enqueued deployment must not outlive the cancellation")

	count, err := repo.ActiveSlotCount(ctx)
	require.NoError(t, err)
	assert.Zero(t, count, "no execution slot is left stranded")
}

// TestQueryModel_DropsAScheduleCancelledFirst pins the other ordering: once a
// cancellation has committed, the guard sees it and enqueues nothing.
func TestQueryModel_DropsAScheduleCancelledFirst(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	scheduleID := uuid.New()

	cancel := postgres.NewUnitOfWork(db, testLogger())
	require.NoError(t, cancel.Begin(ctx))
	require.NoError(t, handlers.NewScheduleCancelledHandler(testLogger(), unusedPodTerminator{t: t}).Handle(
		ctx, cancel, events.ScheduleCancelled{ScheduleID: scheduleID}, uuid.Nil))
	require.NoError(t, cancel.Commit())

	u := postgres.NewUnitOfWork(db, testLogger())
	require.NoError(t, u.Begin(ctx))
	evt := events.QueryModel{
		TaskID: uuid.New(), ScheduleID: scheduleID, ScheduleName: "daily",
		ServiceName: "dbt", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: "dbt-model", ImageTag: "sha-abc",
	}
	require.NoError(t, handlers.NewQueryModelHandler(jobsRoutingPolicy(), testLogger()).
		Handle(ctx, u, evt, uuid.Nil))
	require.NoError(t, u.Commit())

	deps, err := repo.GetNonTerminalByScheduleForUpdate(ctx, scheduleID)
	require.NoError(t, err)
	assert.Empty(t, deps, "a cancelled schedule enqueues nothing")
}

// TestGetNonTerminalByScheduleForUpdate_ExcludesSettledRows pins the lookup's
// filter: a row that has already settled admits no transition, so returning it
// would only make the caller fail on it.
func TestGetNonTerminalByScheduleForUpdate_ExcludesSettledRows(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	now := time.Now().UTC()

	scheduleID := uuid.New()
	live := addDeployedJob(t, db, scheduleID, "orders", now)
	settled := addDeployedJob(t, db, scheduleID, "customers", now)
	require.NoError(t, settled.Cancel("done", now))
	require.NoError(t, repo.Save(ctx, settled))

	got, err := repo.GetNonTerminalByScheduleForUpdate(ctx, scheduleID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, live.ID(), got[0].ID())
}

// unusedPodTerminator fails the test if a pod deletion is ever requested. The
// cancellations exercised here are of Kubernetes Jobs, which no worker holds
// under a lease: there is no worker pod to stop, and asking a pool runtime to
// delete one would name a pod that does not exist.
type unusedPodTerminator struct{ t *testing.T }

func (p unusedPodTerminator) DeletePod(_ context.Context, podName, _ string) error {
	p.t.Fatalf("no worker pod should be deleted for a Jobs-path cancellation, got %q", podName)
	return nil
}
