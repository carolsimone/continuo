package test

import (
	"context"
	"slices"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/service/runsettle"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ownRuns is a unit of work whose run list holds only the runs a test made,
// so a settle leaves every other test's runs alone.
type ownRuns struct {
	uow.UnitOfWork
	ids []uuid.UUID
}

func (o ownRuns) Run() repository.RunRepository {
	return ownRunList{RunRepository: o.UnitOfWork.Run(), ids: o.ids}
}

type ownRunList struct {
	repository.RunRepository
	ids []uuid.UUID
}

func (o ownRunList) ListUnplannedActive(ctx context.Context) ([]uuid.UUID, error) {
	all, err := o.RunRepository.ListUnplannedActive(ctx)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(all, func(id uuid.UUID) bool { return !slices.Contains(o.ids, id) }), nil
}

// TestSettle cancels, against a real Postgres, the active runs state did not
// plan and leaves the runs it planned.
func TestSettle(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, cleanup := setupPostgres(t)
	defer cleanup()
	h := newBuiltHarness(db)
	ctx := context.Background()

	releaseID := "rel-" + uuid.NewString()[:8]
	h.storeRelease(t, releaseID, promotedNode("raw", "settle_orders", "dbt-model"))
	unplanned, unplannedTask := h.legacyRun(t, releaseID, "settle_orders")
	promoteRelease(t, db, scheduledNode("raw", "settle_planned", "dbt-model", "settle-planned"))
	planned := h.trigger(t, "settle-planned", model.OperationRun)
	newUoW := func() uow.UnitOfWork { return ownRuns{UnitOfWork: h.factory(), ids: []uuid.UUID{unplanned, planned}} }

	n, err := runsettle.Settle(ctx, newUoW, h.logger)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	var row struct {
		Status      string  `db:"status"`
		CancelledBy *string `db:"cancelled_by"`
		Reason      *string `db:"cancellation_reason"`
	}
	require.NoError(t, db.Get(&row, `SELECT status, cancelled_by, cancellation_reason FROM scheduler_tracker WHERE schedule_id = $1`, unplanned))
	assert.Equal(t, "cancelled", row.Status)
	require.NotNil(t, row.CancelledBy)
	assert.Equal(t, "system", *row.CancelledBy)
	require.NotNil(t, row.Reason)
	assert.Equal(t, runsettle.Reason, *row.Reason)
	var taskStatus string
	require.NoError(t, db.Get(&taskStatus, `SELECT status FROM task_tracker WHERE task_id = $1`, unplannedTask))
	assert.Equal(t, "cancelled", taskStatus)
	assert.Equal(t, 1, countOf(t, db, `SELECT count(*) FROM state_outbox WHERE aggregate_id = $1 AND stream_name = $2`,
		unplanned, streams.ScheduleCancelledV1), "execution-controller is told to suppress the run's Jobs")

	var plannedStatus string
	require.NoError(t, db.Get(&plannedStatus, `SELECT status FROM scheduler_tracker WHERE schedule_id = $1`, planned))
	assert.Equal(t, "running", plannedStatus)

	n, err = runsettle.Settle(ctx, newUoW, h.logger)
	require.NoError(t, err)
	assert.Zero(t, n, "a second settle finds nothing to cancel")
}
