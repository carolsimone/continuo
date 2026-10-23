package runstart_test

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
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var planNow = time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)

// graphTopology serves each release's graph as every schedule's and every key
// set's slice of it, records each read, and grants the topology lock. A
// release without an entry in releases is served graph.
type graphTopology struct {
	uow.LiveReleaseOnly
	graph    topology.Graph
	releases map[string]topology.Graph
	calls    []string
}

func (g *graphTopology) of(releaseID string) topology.Graph {
	if r, ok := g.releases[releaseID]; ok {
		return r
	}
	return g.graph
}

func (g *graphTopology) LockTopology(ctx context.Context) error {
	g.calls = append(g.calls, "lock")
	return g.LiveReleaseOnly.LockTopology(ctx)
}

func (g *graphTopology) LivePointer(ctx context.Context, tenant string) (topology.Pointer, error) {
	g.calls = append(g.calls, "live "+tenant)
	return g.LiveReleaseOnly.LivePointer(ctx, tenant)
}

func (g *graphTopology) ScheduleGraph(_ context.Context, tenant, releaseID, schedule string) (topology.Graph, error) {
	g.calls = append(g.calls, "schedule "+tenant+" "+releaseID+" "+schedule)
	return g.of(releaseID), nil
}

func (g *graphTopology) Graph(_ context.Context, tenant, releaseID string, keys []topology.NodeKey) (topology.Graph, error) {
	g.calls = append(g.calls, "graph "+tenant+" "+releaseID)
	return g.of(releaseID).Restrict(keys), nil
}

// sourceTasks is a TaskCollection holding one run's tasks and the release of
// each other task's run in releases; RunReleasesOf records the ids it is asked
// for, and BulkCreate records the tasks a run starts with.
type sourceTasks struct {
	run.TaskCollection
	attempts []run.Attempt
	tasks    []run.Task
	releases map[uuid.UUID]string
	asked    [][]uuid.UUID
	created  []run.Task
}

func (s *sourceTasks) ListRunTasks(context.Context, uuid.UUID) ([]run.Task, error) {
	return s.tasks, nil
}

func (s *sourceTasks) RunReleasesOf(_ context.Context, taskIDs []uuid.UUID) (map[uuid.UUID]string, error) {
	s.asked = append(s.asked, taskIDs)
	out := make(map[uuid.UUID]string)
	for _, id := range taskIDs {
		if r, ok := s.releases[id]; ok {
			out[id] = r
		}
	}
	return out, nil
}

func (s *sourceTasks) BulkCreate(_ context.Context, tasks []run.Task) error {
	s.created = append(s.created, tasks...)
	return nil
}

func (s *sourceTasks) AddAttempts(_ context.Context, attempts []run.Attempt) error {
	s.attempts = append(s.attempts, attempts...)
	return nil
}

// savedRuns records SaveRun.
type savedRuns struct {
	repository.RunRepository
	saved int
}

func (s *savedRuns) SaveRun(context.Context, *run.Run) error {
	s.saved++
	return nil
}

// appendedEvents records Append.
type appendedEvents struct{ events []run.DomainEvent }

func (a *appendedEvents) Append(_ context.Context, events []run.DomainEvent, _ uuid.UUID) error {
	a.events = append(a.events, events...)
	return nil
}

// threeNodes is a release slice: raw.a feeds mart.b, which feeds mart.c.
func threeNodes() topology.Graph {
	node := func(schema, table string) topology.Node {
		return topology.Node{
			Key: topology.KeyOf(schema, table), ServiceName: "svc", SchemaName: schema, TableName: table,
			NodeType: "dbt-model", ImageTag: "svc:7", ScheduleName: "daily",
		}
	}
	return topology.NewGraph(
		[]topology.Node{node("raw", "a"), node("mart", "b"), node("mart", "c")},
		[]topology.Edge{{Upstream: "raw.a", Downstream: "mart.b"}, {Upstream: "mart.b", Downstream: "mart.c"}},
	)
}

// withRawAChanged is threeNodes with a new content hash for raw.a.
func withRawAChanged() topology.Graph {
	g := threeNodes()
	nodes := make([]topology.Node, 0, g.Len())
	for _, k := range g.Keys() {
		n, _ := g.Node(k)
		if k == "raw.a" {
			n.ContentHash = "h-fixed"
		}
		nodes = append(nodes, n)
	}
	return topology.NewGraph(nodes, []topology.Edge{{Upstream: "raw.a", Downstream: "mart.b"}, {Upstream: "mart.b", Downstream: "mart.c"}})
}

func fakeUoW(live topology.Pointer, tasks []run.Task) (*uow.FakeUnitOfWork, *graphTopology) {
	topo := &graphTopology{LiveReleaseOnly: uow.LiveReleaseOnly{Live: live}, graph: threeNodes()}
	u := &uow.FakeUnitOfWork{TopologyRepo: topo}
	u.SetTaskCollection(&sourceTasks{tasks: tasks})
	return u, topo
}

var planner = runstart.Planner{TenantID: "default", Tasks: run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}}

// The topology lock is taken before the live release is read, so the release a
// run names cannot be pruned before the run commits.
func TestPlanner_LiveStampLocksTheTopologyBeforeReadingTheLiveRelease(t *testing.T) {
	u, topo := fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, nil)

	got, err := planner.LiveStamp(context.Background(), u)

	require.NoError(t, err)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, got)
	assert.Equal(t, []string{"lock", "live default"}, topo.calls)
}

// A schedule's run is chosen from the schedule's graph on the live release.
func TestPlanner_ScheduleSelectsTheScheduleGraphOnTheLiveRelease(t *testing.T) {
	u, topo := fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, nil)

	sel, err := planner.Schedule(context.Background(), u, "daily", model.OperationRun)

	require.NoError(t, err)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, sel.Stamp)
	assert.Equal(t, 3, sel.Graph.Len())
	assert.Len(t, sel.Plan.Tasks, 3)
	assert.Equal(t, []topology.NodeKey{"raw.a"}, sel.Plan.Ready)
	assert.Equal(t, []string{"lock", "live default", "schedule default rel-7 daily"}, topo.calls)
}

// Without a live release a schedule's selection is empty; a refusal still
// carries the live release, so a cron fire can record its failed run on it.
func TestPlanner_ScheduleRefusals(t *testing.T) {
	u, _ := fakeUoW(topology.Pointer{}, nil)
	_, err := planner.Schedule(context.Background(), u, "daily", model.OperationRun)
	assert.ErrorIs(t, err, runplan.ErrEmptySelection)

	u, _ = fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, nil)
	sel, err := planner.Schedule(context.Background(), u, "daily", model.OperationTest)
	assert.ErrorIs(t, err, runplan.ErrNoTests)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, sel.Stamp)
}

func sourceRun(t *testing.T, stamp run.ReleaseStamp) *run.Run {
	t.Helper()
	r, err := run.NewPendingRun("daily", run.KindCron, nil, identity.SystemUserID, nil, model.OperationRun, stamp, planNow)
	require.NoError(t, err)
	return r
}

func task(key topology.NodeKey, status run.TaskStatus) run.Task {
	return run.Task{TaskID: uuid.New(), NodeKey: key, Status: status}
}

// A rerun is chosen from the source's tasks on the source's own release.
func TestPlanner_RerunReadsTheSourceTasksOnTheSourceRelease(t *testing.T) {
	stamp := run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}
	tasks := []run.Task{task("mart.b", run.TaskStatusFailed), task("mart.c", run.TaskStatusSkipped), task("raw.a", run.TaskStatusSucceeded)}
	u, topo := fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, tasks)

	sel, err := planner.Rerun(context.Background(), u, sourceRun(t, stamp))

	require.NoError(t, err)
	assert.Equal(t, stamp, sel.Stamp)
	assert.Equal(t, []string{"graph default rel-3"}, topo.calls)
	assert.Equal(t, []runplan.PlannedTask{
		{Key: "mart.b"}, {Key: "mart.c"}, {Key: "raw.a", Inherited: true, InheritedFrom: tasks[2].TaskID},
	}, sel.Plan.Tasks)
	assert.Equal(t, []topology.NodeKey{"mart.b"}, sel.Plan.Ready)
}

// A rebase is chosen from the source's tasks over its schedule's graph on the
// live release, compared with the source's nodes on the source's release: the
// live release changed raw.a, upstream of the failed mart.b.
func TestPlanner_RebaseReadsTheSourceScheduleOnTheLiveRelease(t *testing.T) {
	tasks := []run.Task{task("mart.b", run.TaskStatusFailed), task("raw.a", run.TaskStatusSucceeded)}
	u, topo := fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, tasks)
	topo.releases = map[string]topology.Graph{"rel-3": threeNodes(), "rel-7": withRawAChanged()}
	source := sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9})

	sel, pv, err := planner.Rebase(context.Background(), u, source, runstart.RebaseOptions{IncludeChangedUpstream: true})

	require.NoError(t, err)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, sel.Stamp)
	assert.Equal(t, []string{"lock", "live default", "graph default rel-3", "schedule default rel-7 daily"}, topo.calls)
	assert.Empty(t, u.TaskCollection().(*sourceTasks).asked, "a source that inherited nothing reads no other run's release")
	assert.Equal(t, []topology.NodeKey{"raw.a"}, sel.Plan.Ready, "mart.b waits for the changed raw.a; mart.c is new and waits for mart.b")
	assert.Len(t, sel.Plan.Tasks, 3)
	assert.Equal(t, pv.Plan, sel.Plan)
	assert.Equal(t, 1, pv.ChangedUpstream)
	assert.Equal(t, []runplan.ChangedNode{{Key: "raw.a", ServiceName: "svc", SchemaName: "raw", TableName: "a", Downstream: 2}}, pv.Changed)

	topo.calls = nil
	sel, pv, err = planner.Rebase(context.Background(), u, source, runstart.RebaseOptions{})
	require.NoError(t, err)
	assert.Equal(t, []topology.NodeKey{"mart.b"}, sel.Plan.Ready, "without the option raw.a is inherited")
	assert.Equal(t, 0, pv.ChangedUpstream)
	assert.Len(t, pv.Changed, 1)
}

// inheritedTask is a succeeded task of key the source inherited from root.
func inheritedTask(key topology.NodeKey, root uuid.UUID) run.Task {
	t := task(key, run.TaskStatusSucceeded)
	t.InheritedFromTaskID = &root
	return t
}

// The source, on rel-5, failed at mart.b and inherited raw.a from a run on
// rel-3; rel-5 and the live rel-7 both change raw.a. The rebase reads the
// release of the run that built raw.a and compares raw.a with it, so raw.a is
// changed; each release is read once, restricted to the source's nodes.
func TestPlanner_RebaseComparesAnInheritedSuccessWithTheReleaseThatBuiltIt(t *testing.T) {
	root := uuid.New()
	u, topo := fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12},
		[]run.Task{task("mart.b", run.TaskStatusFailed), inheritedTask("raw.a", root)})
	tasks := u.TaskCollection().(*sourceTasks)
	tasks.releases = map[uuid.UUID]string{root: "rel-3"}
	topo.releases = map[string]topology.Graph{"rel-3": threeNodes(), "rel-5": withRawAChanged(), "rel-7": withRawAChanged()}
	source := sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-5", PromotionSeq: 10})

	sel, pv, err := planner.Rebase(context.Background(), u, source, runstart.RebaseOptions{IncludeChangedUpstream: true})

	require.NoError(t, err)
	assert.Equal(t, [][]uuid.UUID{{root}}, tasks.asked)
	assert.Equal(t, []string{"lock", "live default", "graph default rel-5", "graph default rel-3", "schedule default rel-7 daily"}, topo.calls)
	assert.Equal(t, []topology.NodeKey{"raw.a"}, sel.Plan.Ready)
	assert.Equal(t, []runplan.ChangedNode{{Key: "raw.a", ServiceName: "svc", SchemaName: "raw", TableName: "a", Downstream: 2}}, pv.Changed)
}

// An inherited success whose root task is gone, or whose run names no
// release, is compared with the source's own release.
func TestPlanner_RebaseComparesAnInheritedSuccessWithoutAKnownRootWithTheSourceRelease(t *testing.T) {
	root := uuid.New()
	u, topo := fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12},
		[]run.Task{task("mart.b", run.TaskStatusFailed), inheritedTask("raw.a", root)})
	topo.releases = map[string]topology.Graph{"rel-3": threeNodes(), "rel-5": withRawAChanged(), "rel-7": withRawAChanged()}
	source := sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-5", PromotionSeq: 10})

	sel, pv, err := planner.Rebase(context.Background(), u, source, runstart.RebaseOptions{IncludeChangedUpstream: true})

	require.NoError(t, err)
	assert.Equal(t, [][]uuid.UUID{{root}}, u.TaskCollection().(*sourceTasks).asked)
	assert.Equal(t, []string{"lock", "live default", "graph default rel-5", "schedule default rel-7 daily"}, topo.calls)
	assert.Equal(t, []topology.NodeKey{"mart.b"}, sel.Plan.Ready)
	assert.Empty(t, pv.Changed)
}

// A rebase previewed on another live release is refused under the topology
// lock, before anything else is read.
func TestPlanner_RebaseRefusesAMovedLiveRelease(t *testing.T) {
	tasks := []run.Task{task("mart.b", run.TaskStatusFailed), task("raw.a", run.TaskStatusSucceeded)}
	u, topo := fakeUoW(topology.Pointer{ReleaseID: "rel-8", PromotionSeq: 13}, tasks)
	source := sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9})

	sel, _, err := planner.Rebase(context.Background(), u, source, runstart.RebaseOptions{ExpectedReleaseID: "rel-7"})

	require.ErrorIs(t, err, runstart.ErrLiveReleaseMoved)
	assert.Equal(t, "the live release changed; preview again", err.Error())
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-8", PromotionSeq: 13}, sel.Stamp)
	assert.Equal(t, []string{"lock", "live default"}, topo.calls)

	topo.calls = nil
	sel, _, err = planner.Rebase(context.Background(), u, source, runstart.RebaseOptions{ExpectedReleaseID: "rel-8"})
	require.NoError(t, err)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-8", PromotionSeq: 13}, sel.Stamp)
}

// Without a live release a rebase's selection is empty.
func TestPlanner_RebaseWithoutALiveReleaseIsEmpty(t *testing.T) {
	u, _ := fakeUoW(topology.Pointer{}, []run.Task{task("mart.b", run.TaskStatusFailed)})
	_, _, err := planner.Rebase(context.Background(), u, sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}), runstart.RebaseOptions{})
	assert.ErrorIs(t, err, runplan.ErrEmptySelection)
}

// A source run that names no release, or a task of it that names no node,
// cannot be read in any release.
func TestPlanner_ASourceWithoutAReleaseIsRefused(t *testing.T) {
	ctx := context.Background()
	u, _ := fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, []run.Task{task("raw.a", run.TaskStatusFailed)})
	noRelease := sourceRun(t, run.ReleaseStamp{})

	_, err := planner.Rerun(ctx, u, noRelease)
	assert.ErrorIs(t, err, runstart.ErrSourceHasNoRelease)
	_, _, err = planner.Rebase(ctx, u, noRelease, runstart.RebaseOptions{})
	assert.ErrorIs(t, err, runstart.ErrSourceHasNoRelease)
	_, err = planner.SingleNode(ctx, u, run.NodeID{ServiceName: "svc", SchemaName: "raw", TableName: "a"}, model.OperationRun, noRelease)
	assert.ErrorIs(t, err, runstart.ErrSourceHasNoRelease)

	u, _ = fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, []run.Task{task("", run.TaskStatusFailed)})
	_, err = planner.Rerun(ctx, u, sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}))
	assert.ErrorIs(t, err, runstart.ErrSourceHasNoRelease)
	_, _, err = planner.Rebase(ctx, u, sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}), runstart.RebaseOptions{})
	assert.ErrorIs(t, err, runstart.ErrSourceHasNoRelease)
}

// A single-node run reads its one node on the live release, or on the source
// run's release for a run of a node as that run built it.
func TestPlanner_SingleNodeReadsOneNodeOfTheLiveOrTheSourceRelease(t *testing.T) {
	ctx := context.Background()
	target := run.NodeID{ServiceName: "svc", SchemaName: "mart", TableName: "b"}
	u, topo := fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, nil)

	sel, err := planner.SingleNode(ctx, u, target, model.OperationRun, nil)
	require.NoError(t, err)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, sel.Stamp)
	assert.Equal(t, []topology.NodeKey{"mart.b"}, sel.Plan.Ready)
	assert.Equal(t, []string{"lock", "live default", "graph default rel-7"}, topo.calls)

	topo.calls = nil
	sel, err = planner.SingleNode(ctx, u, target, model.OperationRun, sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}))
	require.NoError(t, err)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}, sel.Stamp)
	assert.Equal(t, []string{"graph default rel-3"}, topo.calls)

	_, err = planner.SingleNode(ctx, u, run.NodeID{ServiceName: "other", SchemaName: "mart", TableName: "b"}, model.OperationRun, nil)
	assert.ErrorIs(t, err, runplan.ErrTargetNotFound)
}

// A promote-seed run is chosen from the given keys on the release the caller
// read.
func TestPlanner_SeedsReadTheKeysOnTheGivenRelease(t *testing.T) {
	u, topo := fakeUoW(topology.Pointer{}, nil)

	sel, err := planner.Seeds(context.Background(), u, topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, []topology.NodeKey{"raw.a"})

	require.NoError(t, err)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, sel.Stamp)
	assert.Equal(t, []topology.NodeKey{"raw.a"}, sel.Plan.Ready)
	assert.Equal(t, []string{"graph default rel-7"}, topo.calls)
}

// Start stores the run's tasks, saves the run again and writes one dispatch
// per ready task.
func TestPlanner_StartStoresTheTasksAndWritesTheDispatches(t *testing.T) {
	ctx := context.Background()
	u, _ := fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, nil)
	runs, outbox := &savedRuns{}, &appendedEvents{}
	u.SetRunRepo(runs)
	u.SetOutboxPublisher(outbox)
	sel, err := planner.Schedule(ctx, u, "daily", model.OperationRun)
	require.NoError(t, err)
	r := sourceRun(t, sel.Stamp)

	require.NoError(t, planner.Start(ctx, u, r, sel, uuid.Nil))

	assert.Equal(t, 1, runs.saved)
	assert.Len(t, u.TaskCollection().(*sourceTasks).created, 3)
	require.Len(t, outbox.events, 1)
	d, ok := outbox.events[0].(run.TaskDispatched)
	require.True(t, ok, "%T", outbox.events[0])
	assert.Equal(t, run.TaskIDFor("default", r.ScheduleID(), "raw.a"), d.TaskID)
	assert.NotNil(t, r.PlannedAt())
}
