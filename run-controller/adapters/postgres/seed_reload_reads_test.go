//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
)

// A release's run list holds every run of the release, active ones included,
// newest first, only those of the kind asked for, and no run of another release
// or tenant.
func TestRunReadRepository_ListReleaseRuns(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := schemaTenant(t, db)
	rel, otherRel := "rel-list-"+uuid.NewString()[:8], "rel-other-"+uuid.NewString()[:8]
	insertSchemaRelease(t, db, tenant, rel, "raw.a")
	insertSchemaRelease(t, db, tenant, otherRel, "raw.a")
	seq := int64(3)

	oldest := readRun(t, db, tenant, "promote-seed-"+uuid.NewString()[:8], &rel, &seq, "succeeded", true)
	_, err := db.Exec(`UPDATE runs SET kind = 'promote_seed', created_at = created_at - interval '2 hours' WHERE run_id = $1`, oldest)
	require.NoError(t, err)
	active := readRun(t, db, tenant, "promote-seed-"+uuid.NewString()[:8], &rel, &seq, "running", false)
	_, err = db.Exec(`UPDATE runs SET kind = 'promote_seed' WHERE run_id = $1`, active)
	require.NoError(t, err)
	cron := readRun(t, db, tenant, "daily-"+uuid.NewString()[:8], &rel, &seq, "failed", true)
	_, err = db.Exec(`UPDATE runs SET kind = 'cron', created_at = created_at - interval '1 hour' WHERE run_id = $1`, cron)
	require.NoError(t, err)
	readRun(t, db, tenant, "elsewhere-"+uuid.NewString()[:8], &otherRel, &seq, "running", false)
	readRun(t, db, "other-"+uuid.NewString(), "foreign-"+uuid.NewString()[:8], &rel, &seq, "running", false)

	repo := postgres.NewRunReadRepository(db)

	all, total, err := repo.ListReleaseRuns(ctx, tenant, rel, "", 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	require.Len(t, all, 3)
	assert.Equal(t, []uuid.UUID{active, cron, oldest}, []uuid.UUID{all[0].RunID, all[1].RunID, all[2].RunID})
	assert.Equal(t, run.RunStatusRunning, all[0].Status)
	assert.Equal(t, run.KindPromoteSeed, all[0].Kind)
	assert.Equal(t, rel, all[0].ReleaseID)
	assert.Nil(t, all[0].CompletedAt, "an active run has no completion time")
	assert.NotNil(t, all[1].CompletedAt)

	seeds, total, err := repo.ListReleaseRuns(ctx, tenant, rel, "promote_seed", 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, seeds, 2)
	assert.Equal(t, []uuid.UUID{active, oldest}, []uuid.UUID{seeds[0].RunID, seeds[1].RunID})

	page, total, err := repo.ListReleaseRuns(ctx, tenant, rel, "", 1, 1)
	require.NoError(t, err)
	assert.Equal(t, 3, total, "the total ignores paging")
	require.Len(t, page, 1)
	assert.Equal(t, cron, page[0].RunID)

	none, total, err := repo.ListReleaseRuns(ctx, pkgevents.DefaultTenantID, rel, "", 50, 0)
	require.NoError(t, err)
	assert.Zero(t, total, "another tenant has no run of the release")
	assert.Empty(t, none)
}

// A schedule's completed-run list carries each run's kind and release too.
func TestRunReadRepository_ListCompletedRunsCarriesKindAndRelease(t *testing.T) {
	db := newTestDB(t)
	tenant := schemaTenant(t, db)
	rel := "rel-c-" + uuid.NewString()[:8]
	insertSchemaRelease(t, db, tenant, rel, "raw.a")
	seq := int64(1)
	schedule := "kind-" + uuid.NewString()[:8]
	id := readRun(t, db, tenant, schedule, &rel, &seq, "succeeded", true)
	_, err := db.Exec(`UPDATE runs SET kind = 'trigger' WHERE run_id = $1`, id)
	require.NoError(t, err)

	runs, _, err := postgres.NewRunReadRepository(db).ListCompletedRuns(context.Background(), tenant, schedule, 10, 0)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, run.KindTrigger, runs[0].Kind)
	assert.Equal(t, rel, runs[0].ReleaseID)
	assert.NotNil(t, runs[0].CompletedAt)
}

// The run graph and the task list return each task's operation and plan
// reason; a task that runs its node reads as the empty operation.
func TestTaskReads_ReturnTheOperationAndPlanReason(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := schemaTenant(t, db)
	insertSchemaRelease(t, db, tenant, "rel-op", "raw.a", "raw.b")
	rel, seq := "rel-op", int64(1)
	runID := readRun(t, db, tenant, "ops-"+uuid.NewString()[:8], &rel, &seq, "running", false)
	reloaded := readTask(t, db, runID, "a", "pending")
	plain := readTask(t, db, runID, "b", "pending")
	_, err := db.Exec(`UPDATE tasks SET operation = 'full_refresh', plan_reason = 'columns changed' WHERE task_id = $1`, reloaded)
	require.NoError(t, err)

	g, err := postgres.NewRunReadRepository(db).GetRunGraph(ctx, tenant, runID)
	require.NoError(t, err)
	require.Len(t, g.Nodes, 2)
	assert.Equal(t, reloaded, g.Nodes[0].TaskID)
	assert.Equal(t, model.OperationFullRefresh, g.Nodes[0].Operation)
	assert.Equal(t, "columns changed", g.Nodes[0].PlanReason)
	assert.Equal(t, plain, g.Nodes[1].TaskID)
	assert.Equal(t, model.OperationRun, g.Nodes[1].Operation)
	assert.Empty(t, g.Nodes[1].PlanReason)

	tasks, _, err := postgres.NewTaskReadRepository(postgres.NewTaskCollection(db, discardLogger())).ListRunTasks(ctx, tenant, runID, nil, 50, 0)
	require.NoError(t, err)
	byID := map[uuid.UUID]int{}
	for i, task := range tasks {
		byID[task.TaskID] = i
	}
	require.Len(t, tasks, 2)
	assert.Equal(t, model.OperationFullRefresh, tasks[byID[reloaded]].Operation)
	assert.Equal(t, "columns changed", tasks[byID[reloaded]].PlanReason)
	assert.Equal(t, model.OperationRun, tasks[byID[plain]].Operation)
	assert.Empty(t, tasks[byID[plain]].PlanReason)
}

// storeFindings stores f as the seed preflight findings of the attempt, in the
// JSON a report carries.
func storeFindings(t *testing.T, db *sqlx.DB, taskID uuid.UUID, attempt int, f pkgevents.SeedPreflightFindings) {
	t.Helper()
	b, err := json.Marshal(f)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE task_attempts SET seed_preflight_findings = $3::jsonb WHERE task_id = $1 AND attempt = $2`,
		taskID, attempt, string(b))
	require.NoError(t, err)
}

// An attempt that ran a preflight carries its findings; one that ran none
// carries nil.
func TestTaskAttemptReadRepository_ReturnsTheSeedPreflightFindings(t *testing.T) {
	db := newTestDB(t)
	runID, tasks := newRunWithTasks(t, db, 1)
	insertAttemptRow(t, db, storedAttempt{TaskID: tasks[0], Attempt: 0, Status: "failed"})
	insertAttemptRow(t, db, storedAttempt{TaskID: tasks[0], Attempt: 1, Status: "succeeded"})
	storeFindings(t, db, tasks[0], 0, pkgevents.SeedPreflightFindings{
		DropSemantics: "cascade", Completeness: "complete", Checked: true,
		Dependents: []pkgevents.PreflightDependent{{Schema: "bi", Name: "fx_rates_report", Kind: "view", Owner: "bi_owner", Depth: 1, Allowed: false}},
		Probes:     []pkgevents.ViewProbe{{Schema: "bi", Name: "fx_rates_report", OK: false, Error: "relation does not exist"}},
	})

	rows, _, err := postgres.NewTaskAttemptReadRepository(db).ListRunAttempts(context.Background(), pkgevents.DefaultTenantID, runID, nil, 50, 0)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	require.NotNil(t, rows[0].SeedPreflight)
	assert.Equal(t, &seedreload.Findings{
		DropSemantics: "cascade", Completeness: "complete", Checked: true,
		Dependents: []seedreload.Dependent{{Schema: "bi", Name: "fx_rates_report", Kind: "view", Owner: "bi_owner", Depth: 1}},
		Probes:     []seedreload.Probe{{Schema: "bi", Name: "fx_rates_report", Error: "relation does not exist"}},
	}, rows[0].SeedPreflight)
	assert.Nil(t, rows[1].SeedPreflight)
}

// A seed task whose latest attempt failed unowned_dependents names the
// dependents its preflight did not allow; a task that failed for another reason,
// or whose latest attempt did not fail so, names none even when an earlier
// attempt's findings blocked.
func TestNodeRunRepository_List_NamesTheBlockingObjectsOfAnUnownedDependentsFailure(t *testing.T) {
	db := newTestDB(t)
	svc, sch := "svc-bo-"+uuid.NewString()[:8], "sch"
	blocked := pkgevents.SeedPreflightFindings{
		Checked: true,
		Dependents: []pkgevents.PreflightDependent{
			{Schema: "bi", Name: "fx_rates_report", Kind: "view", Allowed: false},
			{Schema: "analytics", Name: "fx_rates_latest_eur", Kind: "view", Allowed: true},
			{Schema: "BI", Name: "Daily_Totals", Kind: "view", Allowed: false},
		},
	}
	class := "invalid"
	unowned, other := string(model.FailureReasonUnownedDependents), "seed_full_refresh_disabled"

	stopped := nodeRunTask(t, db, svc, sch, "stopped", run.TaskStatusFailed, 0, 10)
	insertAttemptRow(t, db, storedAttempt{TaskID: stopped, Attempt: 0, Status: "failed", FailureClass: &class, FailureReason: &unowned})
	storeFindings(t, db, stopped, 0, blocked)

	otherFailure := nodeRunTask(t, db, svc, sch, "other", run.TaskStatusFailed, 0, 9)
	insertAttemptRow(t, db, storedAttempt{TaskID: otherFailure, Attempt: 0, Status: "failed", FailureClass: &class, FailureReason: &other})
	storeFindings(t, db, otherFailure, 0, blocked)

	retried := nodeRunTask(t, db, svc, sch, "retried", run.TaskStatusSucceeded, 1, 8)
	insertAttemptRow(t, db, storedAttempt{TaskID: retried, Attempt: 0, Status: "failed", FailureClass: &class, FailureReason: &unowned})
	storeFindings(t, db, retried, 0, blocked)
	insertAttemptRow(t, db, storedAttempt{TaskID: retried, Attempt: 1, Status: "succeeded"})

	noFindings := nodeRunTask(t, db, svc, sch, "bare", run.TaskStatusFailed, 0, 7)
	insertAttemptRow(t, db, storedAttempt{TaskID: noFindings, Attempt: 0, Status: "failed", FailureClass: &class, FailureReason: &unowned})

	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())
	list := func(table string) []string {
		rows, err := repo.List(context.Background(), svc, sch, table, "run", 50)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		return rows[0].BlockingObjects
	}

	assert.Equal(t, []string{"bi.fx_rates_report", "bi.daily_totals"}, list("stopped"))
	assert.Empty(t, list("other"))
	assert.Empty(t, list("retried"))
	assert.Empty(t, list("bare"))
}
