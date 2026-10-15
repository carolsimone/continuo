package test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/runplan"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createdRun is a run as its creating transaction left it.
type createdRun struct {
	Status    run.SchedulerStatus
	Planned   bool
	Tasks     int
	Dispatch  []string
	Completed bool
}

func (h *builtHarness) created(t *testing.T, id uuid.UUID) createdRun {
	t.Helper()
	var row struct {
		Status    run.SchedulerStatus `db:"status"`
		Planned   bool                `db:"planned"`
		Completed bool                `db:"completed"`
	}
	require.NoError(t, h.db.Get(&row, `
		SELECT status, planned_at IS NOT NULL AS planned, completed_at IS NOT NULL AS completed
		FROM scheduler_tracker WHERE schedule_id = $1`, id))
	out := createdRun{Status: row.Status, Planned: row.Planned, Completed: row.Completed}
	require.NoError(t, h.db.Get(&out.Tasks, `SELECT count(*) FROM task_tracker WHERE schedule_id = $1`, id))
	require.NoError(t, h.db.Select(&out.Dispatch, `
		SELECT t.node_key FROM state_outbox o
		JOIN task_tracker t ON t.task_id = (o.payload->>'task_id')::uuid
		WHERE o.aggregate_id = $1 AND o.stream_name = $2
		ORDER BY t.node_key COLLATE "C"`, id, streams.QueryModelV1))
	return out
}

func (h *builtHarness) catalog(t *testing.T, schedules ...string) {
	t.Helper()
	for _, s := range schedules {
		_, err := h.db.Exec(`
			INSERT INTO schedule_catalog (schedule_name, first_seen_at, last_seen_at) VALUES ($1, now(), now())
			ON CONFLICT (schedule_name) DO UPDATE SET removed_at = NULL`, s)
		require.NoError(t, err)
	}
}

// TestRunCreation creates runs through the entry points against a real
// Postgres: a run's tasks are chosen, stored and its ready tasks dispatched in
// the transaction that creates it, and a selection that cannot make a run is
// refused or recorded per the run's kind.
func TestRunCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, cleanup := setupPostgres(t)
	defer cleanup()
	h := newBuiltHarness(db)
	ctx := context.Background()
	alice := identity.FromUserID("okta|alice")
	activate := svchandlers.NewActivateScheduleHandler(h.logger)

	h.catalog(t, "creation-daily", "creation-empty")
	promoteRelease(t, db,
		scheduledNode("raw", "creation_a", "dbt-model", "creation-daily"),
		scheduledNode("mart", "creation_b", "dbt-model", "creation-daily", "raw.creation_a"),
		scheduledNode("mart", "creation_c", "dbt-model", "creation-daily"))

	t.Run("a triggered run is planned and dispatched in its creating transaction", func(t *testing.T) {
		id, outcome, err := activate.Handle(ctx, h.factory(), "creation-daily", run.KindTrigger, nil, alice, model.OperationRun)
		require.NoError(t, err)
		require.Equal(t, svchandlers.OutcomeActivated, outcome)

		assert.Equal(t, createdRun{
			Status: run.SchedulerStatusRunning, Planned: true, Tasks: 3,
			Dispatch: []string{"mart.creation_c", "raw.creation_a"},
		}, h.created(t, id))
		_, err = db.Exec(`UPDATE scheduler_tracker SET status = 'succeeded' WHERE schedule_id = $1`, id)
		require.NoError(t, err)
	})

	t.Run("a test run of a schedule without data tests is recorded skipped", func(t *testing.T) {
		id, _, err := activate.Handle(ctx, h.factory(), "creation-daily", run.KindTrigger, nil, alice, model.OperationTest)
		require.NoError(t, err)

		assert.Equal(t, createdRun{Status: run.SchedulerStatusSkipped, Completed: true}, h.created(t, id))
	})

	t.Run("a cron fire on a schedule with no node in the live release records a failed run", func(t *testing.T) {
		id, _, err := activate.Handle(ctx, h.factory(), "creation-empty", run.KindCron, nil, identity.System(), model.OperationRun)
		require.NoError(t, err)

		assert.Equal(t, createdRun{Status: run.SchedulerStatusFailed, Completed: true}, h.created(t, id))
	})

	t.Run("a trigger of a schedule with no node in the live release creates no run, whatever its operation", func(t *testing.T) {
		var before int
		require.NoError(t, db.Get(&before, `SELECT count(*) FROM scheduler_tracker WHERE schedule_name = 'creation-empty'`))

		_, _, err := activate.Handle(ctx, h.factory(), "creation-empty", run.KindTrigger, nil, alice, model.OperationRun)
		require.ErrorIs(t, err, runplan.ErrEmptySelection)
		_, _, err = activate.Handle(ctx, h.factory(), "creation-empty", run.KindTrigger, nil, alice, model.OperationTest)
		require.ErrorIs(t, err, runplan.ErrEmptySelection, "a test run of an empty schedule is refused, not skipped")

		var after int
		require.NoError(t, db.Get(&after, `SELECT count(*) FROM scheduler_tracker WHERE schedule_name = 'creation-empty'`))
		assert.Equal(t, before, after)
	})
}
