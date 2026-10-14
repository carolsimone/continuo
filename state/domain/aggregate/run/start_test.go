package run_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"
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
	r, _, err := run.NewPendingRun("e2e-schedule-failure", run.KindCron, nil, identity.SystemUserID, nil, op,
		run.ReleaseStamp{ReleaseID: "R5", PromotionSeq: 12}, startNow)
	require.NoError(t, err)
	return r
}

func createdByKey(tc *fakeTaskCollection) map[topology.NodeKey]run.Task {
	out := make(map[topology.NodeKey]run.Task, len(tc.bulkCreated))
	for _, task := range tc.bulkCreated {
		out[task.NodeKey] = task
	}
	return out
}

// The design's example run: every node is a pending task, the two with no
// upstream are dispatched when the run is created, and the run is running.
func TestStart_StoresEveryTaskAndDispatchesTheReadyOnes(t *testing.T) {
	g := failureSchedule()
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.NoError(t, err)
	r := newCronRun(t, model.OperationRun)
	tc := newFakeTaskCollection()

	events, err := r.Start(context.Background(), tc, "default", plan, g, 2, startNow)
	require.NoError(t, err)

	require.Len(t, tc.bulkCreated, 8)
	tasks := createdByKey(tc)
	c := tasks[fkey("ftable_c")]
	jobName, err := domain.ComputeJobName("service-3", "e2e_schema", "ftable_c", r.ScheduleID().String())
	require.NoError(t, err)
	assert.Equal(t, run.Task{
		TaskID: run.TaskIDFor("default", r.ScheduleID(), fkey("ftable_c")), ScheduleID: r.ScheduleID(),
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
		run.TaskDispatched{ID: r.ScheduleID(), ScheduleName: "e2e-schedule-failure", TaskID: a.TaskID, Node: nodeA,
			JobName: a.JobName, Operation: model.OperationRun, MaxRetries: 2},
		run.TaskDispatched{ID: r.ScheduleID(), ScheduleName: "e2e-schedule-failure", TaskID: b.TaskID, Node: nodeB,
			JobName: b.JobName, Operation: model.OperationRun, MaxRetries: 2},
	}, events)

	assert.Equal(t, run.SchedulerStatusRunning, r.Status())
	assert.Equal(t, run.InitStatusCompleted, r.InitializationStatus())
	require.NotNil(t, r.TotalTaskCount())
	assert.Equal(t, int32(8), *r.TotalTaskCount())
	assert.Equal(t, int32(0), r.TerminalTaskCount())
	require.NotNil(t, r.PlannedAt())
	assert.Equal(t, startNow, *r.PlannedAt())
	require.NotNil(t, r.StartedAt())
	assert.Equal(t, startNow, *r.StartedAt())
	require.NotNil(t, r.LastHeartbeatAt())
	assert.Nil(t, r.CompletedAt())
	ch := r.Changes()
	assert.True(t, ch.IsPlannedDirty() && ch.IsStatusDirty() && ch.IsStartedDirty() && ch.IsInitStatusDirty() &&
		ch.IsTotalTaskCountDirty() && ch.IsTerminalTaskCountDirty() && ch.IsHeartbeatDirty())
}

// A rerun of the design's example: the carried-over successes are stored
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
	r, _, err := run.NewDerivedRun("e2e-schedule-failure", run.KindRerun, uuid.New(), run.ReleaseStamp{ReleaseID: "R5", PromotionSeq: 12},
		identity.SystemUserID, model.OperationRun, startNow)
	require.NoError(t, err)
	tc := newFakeTaskCollection()

	events, err := r.Start(context.Background(), tc, "default", plan, g, 2, startNow)
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
	assert.Equal(t, int32(8), *r.TotalTaskCount())
	assert.Equal(t, int32(4), r.TerminalTaskCount(), "the four inherited tasks are terminal")
	assert.Equal(t, run.SchedulerStatusRunning, r.Status())
}

// A plan in which every task is inherited has nothing left to run: the run
// finalizes succeeded in the same call.
func TestStart_APlanWithoutPendingTasksFinalizesSucceeded(t *testing.T) {
	g := failureSchedule()
	root := uuid.New()
	plan := runplan.Plan{Tasks: []runplan.PlannedTask{{Key: fkey("ftable_a"), Inherited: true, InheritedFrom: root}}}
	r := newCronRun(t, model.OperationRun)

	events, err := r.Start(context.Background(), newFakeTaskCollection(), "default", plan, g, 2, startNow)
	require.NoError(t, err)

	assert.Equal(t, []run.DomainEvent{
		run.RunFinalized{ID: r.ScheduleID(), Name: "e2e-schedule-failure", Outcome: run.SchedulerStatusSucceeded},
	}, events)
	assert.Equal(t, run.SchedulerStatusSucceeded, r.Status())
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

	events, err := r.Start(context.Background(), tc, "default", plan, g, 2, startNow)
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
		_, err := newCronRun(t, model.OperationRun).Start(context.Background(), tc, "default", runplan.Plan{}, g, 2, startNow)
		assert.ErrorIs(t, err, run.ErrNoNodes)
		assert.Empty(t, tc.bulkCreated)
	})
	t.Run("a node outside the graph", func(t *testing.T) {
		tc := newFakeTaskCollection()
		ghost := runplan.Plan{Tasks: append(slices.Clone(plan.Tasks), runplan.PlannedTask{Key: "e2e_schema.ghost"})}
		_, err := newCronRun(t, model.OperationRun).Start(context.Background(), tc, "default", ghost, g, 2, startNow)
		require.ErrorIs(t, err, run.ErrPlanNodeNotInGraph)
		assert.Contains(t, err.Error(), "e2e_schema.ghost")
		assert.Empty(t, tc.bulkCreated)
	})
	t.Run("a run already planned", func(t *testing.T) {
		r := newCronRun(t, model.OperationRun)
		_, err := r.Start(context.Background(), newFakeTaskCollection(), "default", plan, g, 2, startNow)
		require.NoError(t, err)
		tc := newFakeTaskCollection()
		_, err = r.Start(context.Background(), tc, "default", plan, g, 2, startNow)
		assert.ErrorIs(t, err, run.ErrAlreadyPlanned)
		assert.Empty(t, tc.bulkCreated)
	})
	t.Run("a terminal run", func(t *testing.T) {
		r := newCronRun(t, model.OperationRun)
		_, err := r.MarkDispatchTerminal(false, "test", startNow)
		require.NoError(t, err)
		tc := newFakeTaskCollection()
		_, err = r.Start(context.Background(), tc, "default", plan, g, 2, startNow)
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
