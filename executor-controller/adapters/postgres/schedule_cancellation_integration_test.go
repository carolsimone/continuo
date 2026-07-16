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
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	require.NoError(t, handlers.NewScheduleCancelledHandler(testLogger()).Handle(
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
