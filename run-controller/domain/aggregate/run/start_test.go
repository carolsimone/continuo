package run_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var startNow = time.Date(2026, 10, 14, 23, 0, 0, 0, time.UTC)

// failureSchedule is e2e-schedule-failure on release R5: ftable_a and ftable_b
// feed ftable_c, which feeds ftable_d and ftable_e, which feed ftable_f;
// ftable_a feeds ftable_g, which feeds ftable_h.
func failureSchedule() topology.Graph {
	service := map[string]string{
		"ftable_a": "service-1", "ftable_b": "service-1", "ftable_c": "service-3", "ftable_d": "service-2",
		"ftable_e": "service-2", "ftable_f": "service-3", "ftable_g": "service-3", "ftable_h": "service-2",
	}
	var nodes []topology.Node
	for table, svc := range service {
		nodes = append(nodes, topology.Node{
			Key: topology.KeyOf("e2e_schema", table), ServiceName: svc, SchemaName: "e2e_schema", TableName: table,
			NodeType: "dbt-model", ImageTag: svc + ":5", ScheduleName: "e2e-schedule-failure",
		})
	}
	e := func(up, down string) topology.Edge {
		return topology.Edge{Upstream: topology.KeyOf("e2e_schema", up), Downstream: topology.KeyOf("e2e_schema", down)}
	}
	return topology.NewGraph(nodes, []topology.Edge{
		e("ftable_a", "ftable_c"), e("ftable_b", "ftable_c"), e("ftable_c", "ftable_d"), e("ftable_c", "ftable_e"),
		e("ftable_d", "ftable_f"), e("ftable_e", "ftable_f"), e("ftable_a", "ftable_g"), e("ftable_g", "ftable_h"),
	})
}

func fkey(table string) topology.NodeKey { return topology.KeyOf("e2e_schema", table) }

func newCronRun(t *testing.T, op model.Operation) *run.Run {
	t.Helper()
	r, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "e2e-schedule-failure", run.KindCron, nil, identity.SystemUserID, op,
		run.ReleaseStamp{ReleaseID: "R5", PromotionSeq: 12}, startNow)
	require.NoError(t, err)
	return r
}

// createdByKey returns each task tc stores, as it stands now, by its node.
func createdByKey(tc *fakeTaskCollection) map[topology.NodeKey]run.Task {
	out := make(map[topology.NodeKey]run.Task, len(tc.created))
	for _, task := range tc.created {
		out[task.NodeKey] = task
	}
	return out
}

// The e2e-schedule-failure run: every node is a pending task, the two with no
// upstream are dispatched when the run is created, and the run is running.
func TestStart_StoresEveryTaskAndDispatchesTheReadyOnes(t *testing.T) {
	g := failureSchedule()
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.NoError(t, err)
	r := newCronRun(t, model.OperationRun)
	tc := newFakeTaskCollection()

	events, err := r.Start(context.Background(), tc, plan, g, taskDefaults, startNow)
	require.NoError(t, err)

	require.Len(t, tc.bulkCreated, 8)
	tasks := createdByKey(tc)
	c := tasks[fkey("ftable_c")]
	jobName, err := domain.ComputeJobName("service-3", "e2e_schema", "ftable_c", r.RunID().String(), 0)
	require.NoError(t, err)
	assert.Equal(t, run.Task{
		TaskID: run.TaskIDFor("default", r.RunID(), fkey("ftable_c")), ScheduleID: r.RunID(),
		CreatedAt: startNow, ServiceName: "service-3", SchemaName: "e2e_schema", TableName: "ftable_c",
		NodeKey: fkey("ftable_c"), JobName: jobName, Status: run.TaskStatusPending, MaxRetries: 2,
		ImageTag: "service-3:5", Operation: model.OperationRun,
	}, c, "a task that waits for its upstreams is stored undispatched")
	for _, table := range []string{"ftable_a", "ftable_b"} {
		require.NotNil(t, tasks[fkey(table)].DispatchedAt, table)
		assert.Equal(t, startNow, *tasks[fkey(table)].DispatchedAt, table)
	}

	a, b := tasks[fkey("ftable_a")], tasks[fkey("ftable_b")]
	nodeA, _ := g.Node(fkey("ftable_a"))
	nodeB, _ := g.Node(fkey("ftable_b"))
	assert.Equal(t, []run.DomainEvent{
		run.TaskDispatched{ID: r.RunID(), TenantID: r.TenantID(), ScheduleName: "e2e-schedule-failure", TaskID: a.TaskID, Attempt: 0,
			KnownStatus: run.AttemptDispatched, Node: nodeA, JobName: a.JobName, Operation: model.OperationRun,
			Timeout: 24 * time.Hour},
		run.TaskDispatched{ID: r.RunID(), TenantID: r.TenantID(), ScheduleName: "e2e-schedule-failure", TaskID: b.TaskID, Attempt: 0,
			KnownStatus: run.AttemptDispatched, Node: nodeB, JobName: b.JobName, Operation: model.OperationRun,
			Timeout: 24 * time.Hour},
	}, events)
	for _, task := range []run.Task{a, b} {
		assert.Equal(t, run.TaskStatusDispatched, task.Status, "a ready task is stored dispatched")
	}
	at := startNow
	assert.Equal(t, []run.Attempt{
		{TaskID: a.TaskID, Number: 0, JobName: a.JobName, Status: run.AttemptDispatched, DispatchedAt: &at, NextCheckIn: 5 * time.Minute},
		{TaskID: b.TaskID, Number: 0, JobName: b.JobName, Status: run.AttemptDispatched, DispatchedAt: &at, NextCheckIn: 5 * time.Minute},
	}, tc.added, "each ready task's attempt 0 is stored dispatched, due for its first check in 5 minutes")
	assert.Equal(t, [][]run.LeaseClaim{{claimOf(r, "ftable_a"), claimOf(r, "ftable_b")}}, tc.leaseTakes,
		"a run writes its nodes: its ready tasks claim their nodes' leases in one call")
	assert.Equal(t, map[topology.NodeKey]uuid.UUID{fkey("ftable_a"): a.TaskID, fkey("ftable_b"): b.TaskID}, tc.leases)

	assert.Equal(t, run.RunStatusRunning, r.Status())
	require.NotNil(t, r.TotalTaskCount())
	assert.Equal(t, int32(8), *r.TotalTaskCount())
	assert.Equal(t, int32(0), r.TerminalTaskCount())
	require.NotNil(t, r.PlannedAt())
	assert.Equal(t, startNow, *r.PlannedAt())
	require.NotNil(t, r.StartedAt())
	assert.Equal(t, startNow, *r.StartedAt())
	require.NotNil(t, r.LastProgressAt())
	assert.Nil(t, r.CompletedAt())
	ch := r.Changes()
	assert.True(t, ch.IsPlannedDirty() && ch.IsStatusDirty() && ch.IsStartedDirty() &&
		ch.IsTotalTaskCountDirty() && ch.IsTerminalTaskCountDirty() && ch.IsHeartbeatDirty())
}

// A ready task of a writing run whose node another run's task holds is stored
// pending, waiting for the node's lease, and is not dispatched: it has no
// attempt and no dispatch. The other ready task takes its free node and is
// dispatched.
func TestStart_AWritingReadyTaskWithAHeldNodeWaits(t *testing.T) {
	g := failureSchedule()
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.NoError(t, err)
	r := newCronRun(t, model.OperationRun)
	tc := newFakeTaskCollection()
	tc.leases[fkey("ftable_a")] = otherRunsTask

	events, err := r.Start(context.Background(), tc, plan, g, taskDefaults, startNow)
	require.NoError(t, err)

	a, b := taskOf(r, "ftable_a"), taskOf(r, "ftable_b")
	assert.Equal(t, [][]run.LeaseClaim{{claimOf(r, "ftable_a"), claimOf(r, "ftable_b")}}, tc.leaseTakes)
	assert.Equal(t, map[topology.NodeKey]uuid.UUID{fkey("ftable_a"): otherRunsTask, fkey("ftable_b"): b}, tc.leases,
		"ftable_a stays with the run that holds it")
	assert.Equal(t, map[uuid.UUID]run.WaitReason{a: run.WaitReasonNodeLease}, tc.waits)
	assert.Equal(t, run.TaskStatusPending, tc.statuses[a])
	assert.Nil(t, createdByKey(tc)[fkey("ftable_a")].DispatchedAt)
	_, issued := attemptOf(t, tc, r, "ftable_a", 0)
	assert.False(t, issued, "a task waiting for its node has no attempt")

	require.Len(t, events, 1, "only ftable_b is dispatched")
	assert.Equal(t, b, events[0].(run.TaskDispatched).TaskID)
	assert.Equal(t, run.TaskStatusDispatched, tc.statuses[b])
	assert.Equal(t, run.RunStatusRunning, r.Status())
	assert.Equal(t, int32(0), r.TerminalTaskCount())
}

// A test run writes no node: its ready tasks claim no lease, and a task whose
// node another run's task holds is dispatched all the same.
func TestStart_ATestRunTakesNoLease(t *testing.T) {
	g := failureSchedule()
	plan := runplan.Plan{
		Tasks: []runplan.PlannedTask{{Key: fkey("ftable_a")}, {Key: fkey("ftable_b")}},
		Ready: []topology.NodeKey{fkey("ftable_a"), fkey("ftable_b")},
	}
	r := newCronRun(t, model.OperationTest)
	tc := newFakeTaskCollection()
	tc.leases[fkey("ftable_a")] = otherRunsTask

	events, err := r.Start(context.Background(), tc, plan, g, taskDefaults, startNow)
	require.NoError(t, err)

	assert.Empty(t, tc.leaseTakes)
	assert.Empty(t, tc.waits)
	assert.Equal(t, map[topology.NodeKey]uuid.UUID{fkey("ftable_a"): otherRunsTask}, tc.leases)
	require.Len(t, events, 2)
	for _, table := range []string{"ftable_a", "ftable_b"} {
		assert.Equal(t, run.TaskStatusDispatched, tc.statuses[taskOf(r, table)], table)
		_, issued := attemptOf(t, tc, r, table, 0)
		assert.True(t, issued, table)
	}
}

// A rerun of the e2e-schedule-failure run: the carried-over successes are stored
// succeeded, with no retry budget, pointing at the task that ran them, and are
// counted terminal; only the re-executed failures are dispatched.
func TestStart_InheritedTasksAreSucceededAndNeverDispatched(t *testing.T) {
	g := failureSchedule()
	sourceTask := func(table string, succeeded bool) runplan.SourceTask {
		return runplan.SourceTask{Key: fkey(table), TaskID: uuid.NewSHA1(uuid.NameSpaceURL, []byte(table)), Succeeded: succeeded}
	}
	src := runplan.Source{Operation: model.OperationRun, Tasks: []runplan.SourceTask{
		sourceTask("ftable_a", true), sourceTask("ftable_b", true), sourceTask("ftable_c", true), sourceTask("ftable_d", true),
		sourceTask("ftable_e", false), sourceTask("ftable_f", false), sourceTask("ftable_g", false), sourceTask("ftable_h", false),
	}}
	plan, err := runplan.Rerun{Source: src}.Select(g)
	require.NoError(t, err)
	r, err := run.NewDerivedRun(pkgevents.DefaultTenantID, uuid.New(), "e2e-schedule-failure", run.KindRerun, uuid.New(), run.ReleaseStamp{ReleaseID: "R5", PromotionSeq: 12},
		identity.SystemUserID, model.OperationRun, startNow)
	require.NoError(t, err)
	tc := newFakeTaskCollection()

	events, err := r.Start(context.Background(), tc, plan, g, taskDefaults, startNow)
	require.NoError(t, err)

	tasks := createdByKey(tc)
	d := tasks[fkey("ftable_d")]
	assert.Equal(t, run.TaskStatusSucceeded, d.Status)
	assert.Equal(t, 0, d.MaxRetries)
	require.NotNil(t, d.InheritedFromTaskID)
	assert.Equal(t, uuid.NewSHA1(uuid.NameSpaceURL, []byte("ftable_d")), *d.InheritedFromTaskID)
	assert.Nil(t, d.DispatchedAt)

	var dispatched []topology.NodeKey
	for _, e := range events {
		dispatched = append(dispatched, e.(run.TaskDispatched).Node.Key)
	}
	assert.Equal(t, []topology.NodeKey{fkey("ftable_e"), fkey("ftable_g")}, dispatched)
	assert.Equal(t, [][]run.LeaseClaim{{claimOf(r, "ftable_e"), claimOf(r, "ftable_g")}}, tc.leaseTakes,
		"an inherited task is never dispatched, so it claims no lease")
	assert.Equal(t, int32(8), *r.TotalTaskCount())
	assert.Equal(t, int32(4), r.TerminalTaskCount(), "the four inherited tasks are terminal")
	assert.Equal(t, run.RunStatusRunning, r.Status())
}

// A plan in which every task is inherited has nothing left to run: the run
// finalizes succeeded in the same call.
func TestStart_APlanWithoutPendingTasksFinalizesSucceeded(t *testing.T) {
	g := failureSchedule()
	root := uuid.New()
	plan := runplan.Plan{Tasks: []runplan.PlannedTask{{Key: fkey("ftable_a"), Inherited: true, InheritedFrom: root}}}
	r := newCronRun(t, model.OperationRun)

	events, err := r.Start(context.Background(), newFakeTaskCollection(), plan, g, taskDefaults, startNow)
	require.NoError(t, err)

	assert.Equal(t, []run.DomainEvent{
		run.RunFinalized{ID: r.RunID(), Name: "e2e-schedule-failure", Outcome: run.RunStatusSucceeded},
	}, events)
	assert.Equal(t, run.RunStatusSucceeded, r.Status())
	require.NotNil(t, r.CompletedAt())
	assert.Equal(t, startNow, *r.CompletedAt())
	assert.Equal(t, int32(1), r.TerminalTaskCount())
	assert.True(t, r.Changes().IsCompletedDirty())
}

// The run's operation reaches every task and every dispatch.
func TestStart_StampsTheRunsOperation(t *testing.T) {
	g := failureSchedule()
	plan, err := runplan.WholeDAG{Operation: model.OperationBuild}.Select(g)
	require.NoError(t, err)
	r := newCronRun(t, model.OperationBuild)
	tc := newFakeTaskCollection()

	events, err := r.Start(context.Background(), tc, plan, g, taskDefaults, startNow)
	require.NoError(t, err)
	for _, task := range tc.bulkCreated {
		assert.Equal(t, model.OperationBuild, task.Operation, task.NodeKey)
	}
	for _, e := range events {
		assert.Equal(t, model.OperationBuild, e.(run.TaskDispatched).Operation)
	}
}

func TestStart_IsRefusedWithoutStoringAnything(t *testing.T) {
	g := failureSchedule()
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.NoError(t, err)

	t.Run("an empty plan", func(t *testing.T) {
		tc := newFakeTaskCollection()
		_, err := newCronRun(t, model.OperationRun).Start(context.Background(), tc, runplan.Plan{}, g, taskDefaults, startNow)
		assert.ErrorIs(t, err, run.ErrNoNodes)
		assert.Empty(t, tc.bulkCreated)
	})
	t.Run("a node outside the graph", func(t *testing.T) {
		tc := newFakeTaskCollection()
		ghost := runplan.Plan{Tasks: append(slices.Clone(plan.Tasks), runplan.PlannedTask{Key: "e2e_schema.ghost"})}
		_, err := newCronRun(t, model.OperationRun).Start(context.Background(), tc, ghost, g, taskDefaults, startNow)
		require.ErrorIs(t, err, run.ErrPlanNodeNotInGraph)
		assert.Contains(t, err.Error(), "e2e_schema.ghost")
		assert.Empty(t, tc.bulkCreated)
	})
	t.Run("a ready task outside the plan", func(t *testing.T) {
		tc := newFakeTaskCollection()
		bad := runplan.Plan{Tasks: slices.Clone(plan.Tasks), Ready: append(slices.Clone(plan.Ready), "e2e_schema.ghost")}
		_, err := newCronRun(t, model.OperationRun).Start(context.Background(), tc, bad, g, taskDefaults, startNow)
		require.ErrorIs(t, err, run.ErrPlanReadyNotPending)
		assert.Contains(t, err.Error(), "e2e_schema.ghost")
		assert.Empty(t, tc.bulkCreated)
	})
	t.Run("a ready task the plan inherits", func(t *testing.T) {
		tc := newFakeTaskCollection()
		bad := runplan.Plan{Tasks: slices.Clone(plan.Tasks), Ready: slices.Clone(plan.Ready)}
		bad.Tasks[0].Inherited, bad.Tasks[0].InheritedFrom = true, uuid.New()
		bad.Ready = append(bad.Ready, bad.Tasks[0].Key)
		_, err := newCronRun(t, model.OperationRun).Start(context.Background(), tc, bad, g, taskDefaults, startNow)
		require.ErrorIs(t, err, run.ErrPlanReadyNotPending)
		assert.Contains(t, err.Error(), string(bad.Tasks[0].Key))
		assert.Empty(t, tc.bulkCreated)
	})
	t.Run("a run already planned", func(t *testing.T) {
		r := newCronRun(t, model.OperationRun)
		_, err := r.Start(context.Background(), newFakeTaskCollection(), plan, g, taskDefaults, startNow)
		require.NoError(t, err)
		tc := newFakeTaskCollection()
		_, err = r.Start(context.Background(), tc, plan, g, taskDefaults, startNow)
		assert.ErrorIs(t, err, run.ErrAlreadyPlanned)
		assert.Empty(t, tc.bulkCreated)
	})
	t.Run("a terminal run", func(t *testing.T) {
		r := newCronRun(t, model.OperationRun)
		_, err := r.MarkDispatchTerminal(false, run.RunFailureEmptySelection, "test", startNow)
		require.NoError(t, err)
		tc := newFakeTaskCollection()
		_, err = r.Start(context.Background(), tc, plan, g, taskDefaults, startNow)
		assert.ErrorIs(t, err, run.ErrAlreadyTerminal)
		assert.Empty(t, tc.bulkCreated)
	})
}

func TestTaskIDFor_IsStableAndNamesOneTaskPerRunAndNode(t *testing.T) {
	runID := uuid.MustParse("7f3e0c52-9a4d-4e61-b1f7-2d8c5e9a1b30")
	id := run.TaskIDFor("default", runID, "e2e_schema.ftable_c")
	assert.Equal(t, id, run.TaskIDFor("default", runID, "e2e_schema.ftable_c"))
	assert.Equal(t, uuid.Version(5), id.Version())
	assert.NotEqual(t, id, run.TaskIDFor("default", runID, "e2e_schema.ftable_d"))
	assert.NotEqual(t, id, run.TaskIDFor("default", uuid.New(), "e2e_schema.ftable_c"))
	assert.NotEqual(t, id, run.TaskIDFor("other", runID, "e2e_schema.ftable_c"))
}

// fxNode is the node table of the fx release, of type nodeType, materialized
// as materialized.
func fxNode(table string, nodeType model.NodeType, materialized string) topology.Node {
	return topology.Node{Key: topology.KeyOf("analytics", table), ServiceName: "finance", SchemaName: "analytics",
		TableName: table, NodeType: string(nodeType), Materialized: materialized, ImageTag: "finance:3", ScheduleName: "fx"}
}

// fxGraph is the fx release: analytics.seed_fx_rates_eur is a seed that the
// view analytics.fx_rates_current and the table analytics.fx_transactions_eur
// read. extra are nodes of the release that read nothing.
func fxGraph(extra ...topology.Node) topology.Graph {
	seed := topology.KeyOf("analytics", "seed_fx_rates_eur")
	return topology.NewGraph(append([]topology.Node{
		fxNode("seed_fx_rates_eur", model.NodeTypeDbtSeed, "seed"),
		fxNode("fx_rates_current", model.NodeTypeDbtModel, "view"),
		fxNode("fx_transactions_eur", model.NodeTypeDbtModel, "table"),
	}, extra...), []topology.Edge{
		{Upstream: seed, Downstream: topology.KeyOf("analytics", "fx_rates_current")},
		{Upstream: seed, Downstream: topology.KeyOf("analytics", "fx_transactions_eur")},
	})
}

// checkPreflight is the check-mode preflight of the seed's load on a
// schema_bound engine, which allows the view the run rebuilds.
func checkPreflight() *seedreload.Preflight {
	return &seedreload.Preflight{Mode: seedreload.PreflightCheck, Family: warehouse.SchemaBound,
		Relation: "analytics.seed_fx_rates_eur", Allowed: []string{"analytics.fx_rates_current"}, Acknowledged: []string{}}
}

// fxReloadPlan is the plan of a load of the seed that drops its table: the
// seed loads with a full refresh and a preflight, and the view it drops is
// rebuilt after it.
func fxReloadPlan(pf *seedreload.Preflight) runplan.Plan {
	full := model.OperationFullRefresh
	return runplan.Plan{Tasks: []runplan.PlannedTask{
		{Key: "analytics.fx_rates_current", Reason: "rebuild: dropped by the load of analytics.seed_fx_rates_eur", RebuildOf: "analytics.seed_fx_rates_eur"},
		{Key: "analytics.seed_fx_rates_eur", Operation: &full, Reason: "columns changed", Preflight: pf},
	}, Ready: []topology.NodeKey{"analytics.seed_fx_rates_eur"}}
}

// promotedSeedsRun is a pending promote-seed run on the fx release, which
// names no operation, with an empty task collection.
func promotedSeedsRun(t *testing.T) (*run.Run, *fakeTaskCollection) {
	t.Helper()
	r, err := run.NewPromotedSeedsRun(pkgevents.DefaultTenantID, uuid.New(), "promote-seed-ab12cd34",
		run.ReleaseStamp{ReleaseID: "R7", PromotionSeq: 7}, startNow)
	require.NoError(t, err)
	tc := newFakeTaskCollection()
	tc.derived = true
	return r, tc
}

// scheduleRun is a pending cron run of the fx schedule, with an empty task
// collection.
func scheduleRun(t *testing.T) (*run.Run, *fakeTaskCollection) {
	t.Helper()
	r, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "fx", run.KindCron, nil, identity.SystemUserID,
		model.OperationRun, run.ReleaseStamp{ReleaseID: "R7", PromotionSeq: 7}, startNow)
	require.NoError(t, err)
	tc := newFakeTaskCollection()
	tc.derived = true
	return r, tc
}

// dispatchesOf returns the TaskDispatched events of events, in order.
func dispatchesOf(events []run.DomainEvent) []run.TaskDispatched {
	var out []run.TaskDispatched
	for _, e := range events {
		if d, ok := e.(run.TaskDispatched); ok {
			out = append(out, d)
		}
	}
	return out
}

// onlyDispatch returns the one TaskDispatched of events.
func onlyDispatch(t *testing.T, events []run.DomainEvent) run.TaskDispatched {
	t.Helper()
	d := dispatchesOf(events)
	require.Len(t, d, 1)
	return d[0]
}

// refusalsOf returns the TaskRefused events of events, in order.
func refusalsOf(events []run.DomainEvent) []run.TaskRefused {
	var out []run.TaskRefused
	for _, e := range events {
		if d, ok := e.(run.TaskRefused); ok {
			out = append(out, d)
		}
	}
	return out
}

// Each task is stored with its own operation, plan reason and seed preflight,
// and the run's operation where the plan names none; the dispatch of the seed
// carries its own.
func TestStart_StampsEachTasksOwnOperationReasonAndPreflight(t *testing.T) {
	pf := checkPreflight()
	r, tc := promotedSeedsRun(t)

	events, err := r.Start(context.Background(), tc, fxReloadPlan(pf), fxGraph(), taskDefaults, startNow)
	require.NoError(t, err)

	tasks := createdByKey(tc)
	seed, view := tasks["analytics.seed_fx_rates_eur"], tasks["analytics.fx_rates_current"]
	assert.Equal(t, model.OperationFullRefresh, seed.Operation)
	assert.Equal(t, "columns changed", seed.PlanReason)
	assert.Equal(t, pf, seed.SeedPreflight)
	assert.Empty(t, seed.RebuildOf)
	assert.Equal(t, model.OperationRun, view.Operation)
	assert.Equal(t, "rebuild: dropped by the load of analytics.seed_fx_rates_eur", view.PlanReason)
	assert.Nil(t, view.SeedPreflight)
	assert.Equal(t, topology.NodeKey("analytics.seed_fx_rates_eur"), view.RebuildOf)
	d := onlyDispatch(t, events)
	assert.Equal(t, seed.TaskID, d.TaskID)
	assert.Equal(t, model.OperationFullRefresh, d.Operation)
	assert.Equal(t, "columns changed", d.Reason)
	assert.Equal(t, pf, d.Preflight)
	assert.Empty(t, refusalsOf(events))
}

// A seed the planner refused is stored failed, with an attempt 0 that names
// the refusal and was never dispatched; the task that reads it is stored
// skipped; nothing is dispatched, the refusal is recorded, and with every task
// ended the run fails at once.
func TestStart_StoresARefusedSeedFailedAndSkipsWhatReadsIt(t *testing.T) {
	full := model.OperationFullRefresh
	reason := "columns changed; +full_refresh: false keeps the table, so no command can apply the change"
	plan := runplan.Plan{Tasks: []runplan.PlannedTask{
		{Key: "analytics.fx_transactions_eur"},
		{Key: "analytics.seed_fx_rates_eur", Operation: &full, Reason: reason, Refusal: model.FailureReasonSeedFullRefreshDisabled},
	}}
	r, tc := scheduleRun(t)

	events, err := r.Start(context.Background(), tc, plan, fxGraph(), taskDefaults, startNow)
	require.NoError(t, err)

	tasks := createdByKey(tc)
	seed := tasks["analytics.seed_fx_rates_eur"]
	assert.Equal(t, run.TaskStatusFailed, seed.Status)
	assert.Nil(t, seed.DispatchedAt)
	a, ok := tc.attemptRows[attemptKey{seed.TaskID, 0}]
	require.True(t, ok, "the refused seed has its attempt 0")
	assert.Equal(t, run.AttemptFailed, a.Status)
	assert.Equal(t, "invalid", *a.FailureClass)
	assert.Equal(t, "seed_full_refresh_disabled", *a.FailureReason)
	assert.Equal(t, reason, *a.ErrorMessage)
	assert.Equal(t, seed.JobName, a.JobName)
	assert.Nil(t, a.DispatchedAt)
	require.NotNil(t, a.FinishedAt)
	assert.Equal(t, startNow, *a.FinishedAt)
	assert.Equal(t, run.TaskStatusSkipped, tasks["analytics.fx_transactions_eur"].Status)
	assert.Empty(t, dispatchesOf(events))
	assert.Empty(t, tc.leaseTakes, "a refused or skipped task claims no lease")
	assert.Equal(t, []run.TaskRefused{{ID: r.RunID(), TenantID: r.TenantID(), TaskID: seed.TaskID,
		Key: "analytics.seed_fx_rates_eur", Operation: model.OperationFullRefresh,
		Reason: model.FailureReasonSeedFullRefreshDisabled, Detail: reason}}, refusalsOf(events))
	assert.Equal(t, run.RunStatusFailed, r.Status())
	assert.Equal(t, int32(2), r.TerminalTaskCount())
	assert.Nil(t, r.FailureReason(), "a run that failed because a task failed records no run failure reason")
}

// A refused seed skips only what reads it: the other ready task is dispatched
// and the run keeps running.
func TestStart_ARefusedSeedLeavesTheRestOfTheRunRunning(t *testing.T) {
	full := model.OperationFullRefresh
	g := fxGraph(fxNode("fx_calendar", model.NodeTypeDbtModel, "table"))
	plan := runplan.Plan{Tasks: []runplan.PlannedTask{
		{Key: "analytics.fx_calendar"},
		{Key: "analytics.fx_transactions_eur"},
		{Key: "analytics.seed_fx_rates_eur", Operation: &full, Reason: "r", Refusal: model.FailureReasonSeedFullRefreshDisabled},
	}, Ready: []topology.NodeKey{"analytics.fx_calendar"}}
	r, tc := scheduleRun(t)

	events, err := r.Start(context.Background(), tc, plan, g, taskDefaults, startNow)
	require.NoError(t, err)

	assert.Equal(t, createdByKey(tc)["analytics.fx_calendar"].TaskID, onlyDispatch(t, events).TaskID)
	assert.Len(t, refusalsOf(events), 1)
	assert.Equal(t, run.RunStatusRunning, r.Status())
	assert.Equal(t, int32(2), r.TerminalTaskCount())
}

// A plan whose ready list names a refused task, or a task below one, is
// refused before anything is stored.
func TestStart_RefusesAReadyTaskThePlanRefusedOrBlocked(t *testing.T) {
	full := model.OperationFullRefresh
	for _, ready := range []topology.NodeKey{"analytics.seed_fx_rates_eur", "analytics.fx_transactions_eur"} {
		plan := runplan.Plan{Tasks: []runplan.PlannedTask{
			{Key: "analytics.fx_transactions_eur"},
			{Key: "analytics.seed_fx_rates_eur", Operation: &full, Refusal: model.FailureReasonSeedFullRefreshDisabled},
		}, Ready: []topology.NodeKey{ready}}
		r, tc := scheduleRun(t)
		_, err := r.Start(context.Background(), tc, plan, fxGraph(), taskDefaults, startNow)
		assert.ErrorIs(t, err, run.ErrPlanReadyNotPending, ready)
		assert.Empty(t, tc.bulkCreated, ready)
	}
}
