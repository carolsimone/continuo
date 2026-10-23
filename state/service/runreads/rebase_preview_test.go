package runreads_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/runreads"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sourceRuns holds one source run; its schedule always has an active run,
// which a preview does not look at.
type sourceRuns struct {
	repository.RunRepository
	source *run.Run
}

func (r *sourceRuns) GetRun(context.Context, uuid.UUID) (*run.Run, error) { return r.source, nil }
func (r *sourceRuns) HasActiveSchedule(context.Context, string) (bool, error) {
	return true, nil
}

// sourceTasks holds the source run's tasks.
type sourceTasks struct {
	run.TaskCollection
	tasks []run.Task
}

func (t *sourceTasks) HasNonSucceeded(context.Context, uuid.UUID) (bool, error) { return true, nil }
func (t *sourceTasks) ListRunTasks(context.Context, uuid.UUID) ([]run.Task, error) {
	return t.tasks, nil
}

// releaseGraphs serves each release's graph as every slice of it.
type releaseGraphs struct {
	uow.LiveReleaseOnly
	graphs map[string]topology.Graph
}

func (r *releaseGraphs) ScheduleGraph(_ context.Context, _, releaseID, _ string) (topology.Graph, error) {
	return r.graphs[releaseID], nil
}
func (r *releaseGraphs) Graph(_ context.Context, _, releaseID string, keys []topology.NodeKey) (topology.Graph, error) {
	return r.graphs[releaseID].Restrict(keys), nil
}

// durations answers LastSuccessDurations from known and records the nodes it
// was asked for.
type durations struct {
	fakeReads
	known map[run.NodeID]time.Duration
	asked [][]run.NodeID
}

func (d *durations) LastSuccessDurations(_ context.Context, nodes []run.NodeID) (map[run.NodeID]time.Duration, error) {
	d.asked = append(d.asked, nodes)
	out := map[run.NodeID]time.Duration{}
	for _, n := range nodes {
		if v, ok := d.known[n]; ok {
			out[n] = v
		}
	}
	return out, nil
}

// fanOut is raw.a feeding mart.b and mart.c, and raw.d feeding mart.c, in
// service svc; hashA and hashD are the content hashes of raw.a and raw.d.
func fanOut(hashA, hashD string) topology.Graph {
	node := func(schema, table, hash string) topology.Node {
		return topology.Node{
			Key: topology.KeyOf(schema, table), ServiceName: "svc", SchemaName: schema, TableName: table,
			NodeType: "dbt-model", ContentHash: hash, ScheduleName: "daily",
		}
	}
	return topology.NewGraph(
		[]topology.Node{node("raw", "a", hashA), node("raw", "d", hashD), node("mart", "b", "h-b"), node("mart", "c", "h-c")},
		[]topology.Edge{
			{Upstream: "raw.a", Downstream: "mart.b"}, {Upstream: "raw.a", Downstream: "mart.c"},
			{Upstream: "raw.d", Downstream: "mart.c"},
		},
	)
}

type previewFixture struct {
	u      *uow.FakeUnitOfWork
	runs   *sourceRuns
	reads  *durations
	newUoW func() uow.UnitOfWork
}

// newPreviewFixture is a failed run of daily on rel-3 whose mart.b failed and
// skipped mart.c; the live rel-7 changed raw.a and raw.d.
func newPreviewFixture(t *testing.T, status run.SchedulerStatus) *previewFixture {
	t.Helper()
	created := time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)
	source := run.HydrateRun(uuid.New(), "daily", status, run.InitStatusCompleted, run.KindCron, nil,
		identity.SystemUserID, model.OperationRun, created, nil, &created, nil, nil, nil, nil, nil, 0, nil)
	source.HydrateRelease(run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9})
	f := &previewFixture{
		u: &uow.FakeUnitOfWork{TopologyRepo: &releaseGraphs{
			LiveReleaseOnly: uow.LiveReleaseOnly{Live: topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}},
			graphs:          map[string]topology.Graph{"rel-3": fanOut("h-a", "h-d"), "rel-7": fanOut("h-a2", "h-d2")},
		}},
		runs: &sourceRuns{source: source},
		reads: &durations{known: map[run.NodeID]time.Duration{
			{ServiceName: "svc", SchemaName: "raw", TableName: "a"}: 252 * time.Second,
		}},
	}
	f.u.SetRunRepo(f.runs)
	f.u.SetTaskCollection(&sourceTasks{tasks: []run.Task{
		{TaskID: uuid.New(), NodeKey: "mart.b", Status: run.TaskStatusFailed},
		{TaskID: uuid.New(), NodeKey: "mart.c", Status: run.TaskStatusSkipped},
		{TaskID: uuid.New(), NodeKey: "raw.a", Status: run.TaskStatusSucceeded},
		{TaskID: uuid.New(), NodeKey: "raw.d", Status: run.TaskStatusSucceeded},
	}})
	f.newUoW = func() uow.UnitOfWork { return f.u }
	return f
}

// A preview selects what the rebase would on the live release, writes
// nothing, and attaches to each changed node it names, in the selection's
// order, its last successful duration (0 for a node without one). A
// schedule's active run does not refuse it.
func TestRebasePreviewer_PreviewsTheRebaseAndWritesNothing(t *testing.T) {
	f := newPreviewFixture(t, run.SchedulerStatusFailed)
	p := runreads.NewRebasePreviewer(runstart.Planner{TenantID: "default", Tasks: run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}}, f.newUoW, f.reads)

	got, err := p.Preview(context.Background(), f.runs.source.ScheduleID(), true)

	require.NoError(t, err)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, got.Release)
	assert.Equal(t, []runreads.ChangedUpstream{
		{
			ChangedNode: runplan.ChangedNode{Key: "raw.a", ServiceName: "svc", SchemaName: "raw", TableName: "a", Downstream: 2},
			LastSuccess: 252 * time.Second,
		},
		{ChangedNode: runplan.ChangedNode{Key: "raw.d", ServiceName: "svc", SchemaName: "raw", TableName: "d", Downstream: 1}},
	}, got.Changed)
	assert.Equal(t, 2, got.Preview.FailedOrSkipped)
	assert.Equal(t, 2, got.Preview.ChangedUpstream)
	assert.Equal(t, [][]run.NodeID{{
		{ServiceName: "svc", SchemaName: "raw", TableName: "a"}, {ServiceName: "svc", SchemaName: "raw", TableName: "d"},
	}}, f.reads.asked)
	assert.Equal(t, 1, f.u.BeginCalled)
	assert.Equal(t, 1, f.u.RollbackCalled)
	assert.Equal(t, 0, f.u.CommitCalled)

	off, err := p.Preview(context.Background(), f.runs.source.ScheduleID(), false)
	require.NoError(t, err)
	assert.Equal(t, 0, off.Preview.ChangedUpstream)
	assert.Equal(t, got.Changed, off.Changed)
}

// A source that is not failed or cancelled is refused before the
// transaction begins.
func TestRebasePreviewer_RefusesASourceThatCannotBeRebased(t *testing.T) {
	f := newPreviewFixture(t, run.SchedulerStatusSucceeded)
	p := runreads.NewRebasePreviewer(runstart.Planner{TenantID: "default", Tasks: run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}}, f.newUoW, f.reads)

	_, err := p.Preview(context.Background(), f.runs.source.ScheduleID(), true)

	require.ErrorIs(t, err, run.ErrSourceMustBeTerminallyFailedOrCancelled)
	assert.Equal(t, 0, f.u.BeginCalled)
}

// A selection the planner refuses is returned as is, and the transaction is
// rolled back.
func TestRebasePreviewer_ReturnsTheSelectionsRefusal(t *testing.T) {
	f := newPreviewFixture(t, run.SchedulerStatusFailed)
	f.u.TopologyRepo = &releaseGraphs{}
	p := runreads.NewRebasePreviewer(runstart.Planner{TenantID: "default", Tasks: run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}}, f.newUoW, f.reads)

	_, err := p.Preview(context.Background(), f.runs.source.ScheduleID(), true)

	require.ErrorIs(t, err, runplan.ErrEmptySelection, "no release is live")
	assert.Equal(t, 1, f.u.RollbackCalled)
	assert.Empty(t, f.reads.asked)
}
