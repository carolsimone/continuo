package test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/runplan"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/runreads"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createdRun is a run as its creating transaction left it.
type createdRun struct {
	Status    run.RunStatus
	Planned   bool
	Tasks     int
	Dispatch  []string
	Completed bool
	// FailureReason is the run's failure_reason; empty when it has none.
	FailureReason run.RunFailureReason
}

func (h *builtHarness) created(t *testing.T, id uuid.UUID) createdRun {
	t.Helper()
	var row struct {
		Status        run.RunStatus        `db:"status"`
		Planned       bool                 `db:"planned"`
		Completed     bool                 `db:"completed"`
		FailureReason run.RunFailureReason `db:"failure_reason"`
	}
	require.NoError(t, h.db.Get(&row, `
		SELECT status, planned_at IS NOT NULL AS planned, completed_at IS NOT NULL AS completed,
		       COALESCE(failure_reason, '') AS failure_reason
		FROM runs WHERE run_id = $1`, id))
	out := createdRun{Status: row.Status, Planned: row.Planned, Completed: row.Completed, FailureReason: row.FailureReason}
	require.NoError(t, h.db.Get(&out.Tasks, `SELECT count(*) FROM tasks WHERE run_id = $1`, id))
	require.NoError(t, h.db.Select(&out.Dispatch, `
		SELECT t.node_key FROM state_outbox o
		JOIN tasks t ON t.task_id = (o.payload->>'task_id')::uuid
		WHERE o.aggregate_id = $1 AND o.stream_name = $2
		ORDER BY t.node_key COLLATE "C"`, id, streams.TaskDispatchV1))
	return out
}

func (h *builtHarness) catalog(t *testing.T, schedules ...string) {
	t.Helper()
	for _, s := range schedules {
		_, err := h.db.Exec(`
			INSERT INTO schedule_catalog (tenant_id, schedule_name, first_seen_at, last_seen_at) VALUES ('default', $1, now(), now())
			ON CONFLICT (tenant_id, schedule_name) DO UPDATE SET removed_at = NULL`, s)
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
	activate := svchandlers.NewStartRunHandler(defaultPlanner, h.logger)

	h.catalog(t, "creation-daily", "creation-empty")
	promoteRelease(t, db,
		scheduledNode("raw", "creation_a", "dbt-model", "creation-daily"),
		scheduledNode("mart", "creation_b", "dbt-model", "creation-daily", "raw.creation_a"),
		scheduledNode("mart", "creation_c", "dbt-model", "creation-daily"))

	t.Run("a triggered run is planned and dispatched in its creating transaction", func(t *testing.T) {
		id, outcome, err := activate.Handle(ctx, h.factory(), "creation-daily", run.KindTrigger, nil, alice, model.OperationRun)
		require.NoError(t, err)
		require.Equal(t, svchandlers.OutcomeRunStarted, outcome)

		assert.Equal(t, createdRun{
			Status: run.RunStatusRunning, Planned: true, Tasks: 3,
			Dispatch: []string{"mart.creation_c", "raw.creation_a"},
		}, h.created(t, id))
		_, err = db.Exec(`UPDATE runs SET status = 'succeeded' WHERE run_id = $1`, id)
		require.NoError(t, err)
	})

	t.Run("a test run of a schedule without data tests is recorded skipped", func(t *testing.T) {
		id, _, err := activate.Handle(ctx, h.factory(), "creation-daily", run.KindTrigger, nil, alice, model.OperationTest)
		require.NoError(t, err)

		assert.Equal(t, createdRun{Status: run.RunStatusSkipped, Completed: true}, h.created(t, id))
	})

	t.Run("a cron fire on a schedule with no node in the live release records a failed run", func(t *testing.T) {
		id, _, err := activate.Handle(ctx, h.factory(), "creation-empty", run.KindCron, nil, identity.System(), model.OperationRun)
		require.NoError(t, err)

		assert.Equal(t, createdRun{Status: run.RunStatusFailed, Completed: true, FailureReason: run.RunFailureEmptySelection},
			h.created(t, id), "the run failed with no failed task, so it records why")
	})

	t.Run("a trigger of a schedule with no node in the live release creates no run, whatever its operation", func(t *testing.T) {
		var before int
		require.NoError(t, db.Get(&before, `SELECT count(*) FROM runs WHERE schedule_name = 'creation-empty'`))

		_, _, err := activate.Handle(ctx, h.factory(), "creation-empty", run.KindTrigger, nil, alice, model.OperationRun)
		require.ErrorIs(t, err, runplan.ErrEmptySelection)
		_, _, err = activate.Handle(ctx, h.factory(), "creation-empty", run.KindTrigger, nil, alice, model.OperationTest)
		require.ErrorIs(t, err, runplan.ErrEmptySelection, "a test run of an empty schedule is refused, not skipped")

		var after int
		require.NoError(t, db.Get(&after, `SELECT count(*) FROM runs WHERE schedule_name = 'creation-empty'`))
		assert.Equal(t, before, after)
	})

	t.Run("a rerun replays its source on the source's release; a rebase restarts it on the live one", func(t *testing.T) {
		h.catalog(t, "creation-derived")
		source := promoteRelease(t, db,
			scheduledNode("raw", "derived_a", "dbt-model", "creation-derived"),
			scheduledNode("mart", "derived_b", "dbt-model", "creation-derived", "raw.derived_a"))
		srcID, _, err := activate.Handle(ctx, h.factory(), "creation-derived", run.KindTrigger, nil, alice, model.OperationRun)
		require.NoError(t, err)
		_, err = db.Exec(`UPDATE tasks SET status = CASE node_key WHEN 'raw.derived_a' THEN 'succeeded' ELSE 'failed' END, attempt = 2 WHERE run_id = $1`, srcID)
		require.NoError(t, err)
		_, err = db.Exec(`UPDATE runs SET status = 'failed', completed_at = now() WHERE run_id = $1`, srcID)
		require.NoError(t, err)
		releaseLeasesOf(t, db, srcID)
		live := promoteRelease(t, db,
			scheduledNode("raw", "derived_a", "dbt-model", "creation-derived"),
			scheduledNode("mart", "derived_b", "dbt-model", "creation-derived", "raw.derived_a"),
			scheduledNode("mart", "derived_c", "dbt-model", "creation-derived"))

		rerunID, _, err := svchandlers.NewTriggerRerunHandler(defaultPlanner, h.logger).Handle(ctx, h.factory(), srcID, alice)
		require.NoError(t, err)
		assert.Equal(t, source, h.stampOf(t, rerunID))
		assert.Equal(t, createdRun{Status: run.RunStatusRunning, Planned: true, Tasks: 2, Dispatch: []string{"mart.derived_b"}}, h.created(t, rerunID))
		_, err = db.Exec(`UPDATE runs SET status = 'failed', completed_at = now() WHERE run_id = $1`, rerunID)
		require.NoError(t, err)
		releaseLeasesOf(t, db, rerunID)

		rebaseID, _, err := svchandlers.NewTriggerRebaseHandler(defaultPlanner, h.logger).Handle(ctx, h.factory(), svchandlers.TriggerRebaseInput{
			SourceRunID: srcID, IncludeChangedUpstream: true, Initiator: alice,
		})
		require.NoError(t, err)
		assert.Equal(t, live, h.stampOf(t, rebaseID))
		assert.Equal(t, createdRun{Status: run.RunStatusRunning, Planned: true, Tasks: 3, Dispatch: []string{"mart.derived_b", "mart.derived_c"}}, h.created(t, rebaseID))
	})

	t.Run("a rerun of a test run is refused and creates no run", func(t *testing.T) {
		stamp := promoteSchedules(t, db, "creation-tested")
		src := h.finished(t, "creation-tested", stamp)
		_, err := db.Exec(`UPDATE runs SET operation = 'test' WHERE run_id = $1`, src)
		require.NoError(t, err)

		_, _, err = svchandlers.NewTriggerRerunHandler(defaultPlanner, h.logger).Handle(ctx, h.factory(), src, alice)
		require.ErrorIs(t, err, runplan.ErrRerunOfTestUnsupported)
		var n int
		require.NoError(t, db.Get(&n, `SELECT count(*) FROM runs WHERE source_run_id = $1`, src))
		assert.Zero(t, n)
	})
}

// A derived run plans inside one transaction, so reading its source's tasks
// and the releases of the runs its source inherited from must reuse the
// transaction's connection: with a pool of one, a second connection for those
// reads would wait on the connection its own caller holds. A rebase preview
// reads the changed nodes' durations through the pool, so it must read them
// after its transaction is rolled back. The live release changes raw.pool_a;
// a rebase without the option inherits it from the source run, which built it
// on the earlier release, so a preview of that rebase names raw.pool_a as a
// changed upstream node.
func TestRunCreation_DerivedRunsNeedOneConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, cleanup := setupPostgres(t)
	defer cleanup()
	h := newBuiltHarness(db)
	alice := identity.FromUserID("okta|alice")
	activate := svchandlers.NewStartRunHandler(defaultPlanner, h.logger)

	h.catalog(t, "pool-derived")
	promoteRelease(t, db,
		scheduledNode("raw", "pool_a", "dbt-model", "pool-derived"),
		scheduledNode("mart", "pool_b", "dbt-model", "pool-derived", "raw.pool_a"))
	srcID, _, err := activate.Handle(context.Background(), h.factory(), "pool-derived", run.KindTrigger, nil, alice, model.OperationRun)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE tasks SET status = CASE node_key WHEN 'raw.pool_a' THEN 'succeeded' ELSE 'failed' END, attempt = 2 WHERE run_id = $1`, srcID)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE runs SET status = 'failed', completed_at = now() WHERE run_id = $1`, srcID)
	require.NoError(t, err)
	releaseLeasesOf(t, db, srcID)
	changedA := scheduledNode("raw", "pool_a", "dbt-model", "pool-derived")
	changedA.ContentHash += "-changed"
	promoteRelease(t, db, changedA, scheduledNode("mart", "pool_b", "dbt-model", "pool-derived", "raw.pool_a"))

	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.SetMaxOpenConns(0) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("rerun", func(t *testing.T) {
		rerunID, _, err := svchandlers.NewTriggerRerunHandler(defaultPlanner, h.logger).Handle(ctx, h.factory(), srcID, alice)
		require.NoError(t, err)
		_, err = db.Exec(`UPDATE runs SET status = 'failed', completed_at = now() WHERE run_id = $1`, rerunID)
		require.NoError(t, err)
		releaseLeasesOf(t, db, rerunID)
	})
	var rebaseID uuid.UUID
	t.Run("rebase", func(t *testing.T) {
		id, _, err := svchandlers.NewTriggerRebaseHandler(defaultPlanner, h.logger).Handle(ctx, h.factory(), svchandlers.TriggerRebaseInput{
			SourceRunID: srcID, Initiator: alice,
		})
		require.NoError(t, err)
		rebaseID = id
		_, err = db.Exec(`UPDATE tasks SET status = 'failed', attempt = 2 WHERE run_id = $1 AND node_key = 'mart.pool_b'`, rebaseID)
		require.NoError(t, err)
		_, err = db.Exec(`UPDATE runs SET status = 'failed', completed_at = now() WHERE run_id = $1`, rebaseID)
		require.NoError(t, err)
		releaseLeasesOf(t, db, rebaseID)
	})
	t.Run("preview", func(t *testing.T) {
		require.NotEqual(t, uuid.Nil, rebaseID, "the rebase subtest created the source")
		previewer := runreads.NewRebasePreviewer(defaultPlanner, h.factory, postgres.NewRunReadRepository(db))
		pv, err := previewer.Preview(ctx, rebaseID, true)
		require.NoError(t, err)
		require.Len(t, pv.Changed, 1, "the preview reads the release that built raw.pool_a and the duration of raw.pool_a")
		assert.Equal(t, "pool_a", pv.Changed[0].TableName)
	})
	t.Run("rebase of a rebase", func(t *testing.T) {
		require.NotEqual(t, uuid.Nil, rebaseID, "the rebase subtest created the source")
		id, _, err := svchandlers.NewTriggerRebaseHandler(defaultPlanner, h.logger).Handle(ctx, h.factory(), svchandlers.TriggerRebaseInput{
			SourceRunID: rebaseID, IncludeChangedUpstream: true, Initiator: alice,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"raw.pool_a"}, h.created(t, id).Dispatch)
	})
}
