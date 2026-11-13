package runstart_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	"github.com/carolsimone/continuo/run-controller/domain/seeds"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/carolsimone/continuo/run-controller/service/runstart"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var planNow = time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)

// graphTopology serves each release's graph as every key set's slice of it
// and as every schedule's, restricted to the schedule's keys in schedules when
// it has an entry there; records each read, and grants the topology lock. A
// release without an entry in releases is served graph, and a release is
// bootstrap when bootstrap says so.
type graphTopology struct {
	uow.LiveReleaseOnly
	graph     topology.Graph
	releases  map[string]topology.Graph
	schedules map[string][]topology.NodeKey
	bootstrap map[string]bool
	calls     []string
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
	if keys, ok := g.schedules[schedule]; ok {
		return g.of(releaseID).Restrict(keys), nil
	}
	return g.of(releaseID), nil
}

// ViewClosure walks down from seeds through the nodes materialized as a view,
// a materialized view or ephemeral, as the Postgres repository does.
func (g *graphTopology) ViewClosure(_ context.Context, _, releaseID string, seeds []topology.NodeKey) (topology.Graph, error) {
	rg := g.of(releaseID)
	seen := map[topology.NodeKey]bool{}
	var keys []topology.NodeKey
	queue := slices.Clone(seeds)
	for len(queue) > 0 {
		k := queue[0]
		queue = queue[1:]
		if seen[k] {
			continue
		}
		seen[k] = true
		keys = append(keys, k)
		for _, d := range rg.Downstreams(k) {
			if n, _ := rg.Node(d); slices.Contains([]string{"view", "materialized_view", "ephemeral"}, n.Materialized) {
				queue = append(queue, d)
			}
		}
	}
	return rg.Restrict(keys), nil
}

func (g *graphTopology) IsBootstrapRelease(_ context.Context, _, releaseID string) (bool, error) {
	return g.bootstrap[releaseID], nil
}

func (g *graphTopology) Graph(_ context.Context, tenant, releaseID string, keys []topology.NodeKey) (topology.Graph, error) {
	g.calls = append(g.calls, "graph "+tenant+" "+releaseID)
	return g.of(releaseID).Restrict(keys), nil
}

// sourceTasks is a TaskCollection holding one run's tasks and the release of
// each other task's run in releases; RunReleasesOf records the ids it is asked
// for, BulkCreate records the tasks a run starts with, and every ready task
// gets its node's lease and is dispatched.
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

func (s *sourceTasks) SaveAttempt(_ context.Context, a run.Attempt) error {
	s.attempts = append(s.attempts, a)
	return nil
}

func (s *sourceTasks) BulkCreate(_ context.Context, tasks []run.Task) error {
	s.created = append(s.created, tasks...)
	return nil
}

func (s *sourceTasks) AddAttempts(_ context.Context, attempts []run.Attempt) error {
	s.attempts = append(s.attempts, attempts...)
	return nil
}

func (s *sourceTasks) TakeLeases(_ context.Context, claims []run.LeaseClaim) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0, len(claims))
	for _, c := range claims {
		ids = append(ids, c.TaskID)
	}
	return ids, nil
}

func (s *sourceTasks) MarkDispatched(_ context.Context, ids []uuid.UUID, _ time.Time) ([]uuid.UUID, error) {
	return ids, nil
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

var planner = runstart.Planner{Tasks: run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}}

// The topology lock is taken before the live release is read, so the release a
// run names cannot be pruned before the run commits.
func TestPlanner_LiveStampLocksTheTopologyBeforeReadingTheLiveRelease(t *testing.T) {
	u, topo := fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, nil)

	got, err := planner.LiveStamp(context.Background(), u, pkgevents.DefaultTenantID)

	require.NoError(t, err)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, got)
	assert.Equal(t, []string{"lock", "live default"}, topo.calls)
}

// A schedule's run is chosen from the schedule's graph on the live release.
func TestPlanner_ScheduleSelectsTheScheduleGraphOnTheLiveRelease(t *testing.T) {
	u, topo := fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, nil)

	sel, err := planner.Schedule(context.Background(), u, pkgevents.DefaultTenantID, "daily", model.OperationRun)

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
	_, err := planner.Schedule(context.Background(), u, pkgevents.DefaultTenantID, "daily", model.OperationRun)
	assert.ErrorIs(t, err, runplan.ErrEmptySelection)

	u, _ = fakeUoW(topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, nil)
	sel, err := planner.Schedule(context.Background(), u, pkgevents.DefaultTenantID, "daily", model.OperationTest)
	assert.ErrorIs(t, err, runplan.ErrNoTests)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, sel.Stamp)
}

func sourceRun(t *testing.T, stamp run.ReleaseStamp) *run.Run {
	t.Helper()
	return sourceRunOf(t, pkgevents.DefaultTenantID, stamp)
}

// sourceRunOf is a pending daily run of tenantID on stamp.
func sourceRunOf(t *testing.T, tenantID string, stamp run.ReleaseStamp) *run.Run {
	t.Helper()
	r, err := run.NewPendingRun(tenantID, uuid.New(), "daily", run.KindCron, nil, identity.SystemUserID, model.OperationRun, stamp, planNow)
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
	assert.Empty(t, u.TaskCollection(pkgevents.DefaultTenantID).(*sourceTasks).asked, "a source that inherited nothing reads no other run's release")
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
	tasks := u.TaskCollection(pkgevents.DefaultTenantID).(*sourceTasks)
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
	assert.Equal(t, [][]uuid.UUID{{root}}, u.TaskCollection(pkgevents.DefaultTenantID).(*sourceTasks).asked)
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
	_, err = planner.SingleNode(ctx, u, pkgevents.DefaultTenantID, run.NodeID{ServiceName: "svc", SchemaName: "raw", TableName: "a"}, model.OperationRun, noRelease, nil)
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

	sel, err := planner.SingleNode(ctx, u, pkgevents.DefaultTenantID, target, model.OperationRun, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, sel.Stamp)
	assert.Equal(t, []topology.NodeKey{"mart.b"}, sel.Plan.Ready)
	assert.Equal(t, []string{"lock", "live default", "graph default rel-7"}, topo.calls)

	topo.calls = nil
	sel, err = planner.SingleNode(ctx, u, pkgevents.DefaultTenantID, target, model.OperationRun, sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}), nil)
	require.NoError(t, err)
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}, sel.Stamp)
	assert.Equal(t, []string{"graph default rel-3"}, topo.calls)

	_, err = planner.SingleNode(ctx, u, pkgevents.DefaultTenantID, run.NodeID{ServiceName: "other", SchemaName: "mart", TableName: "b"}, model.OperationRun, nil, nil)
	assert.ErrorIs(t, err, runplan.ErrTargetNotFound)
}

// A promote-seed run is chosen from the given keys on the release the caller
// read.
func TestPlanner_SeedsReadTheKeysOnTheGivenRelease(t *testing.T) {
	u, topo := fakeUoW(topology.Pointer{}, nil)

	sel, err := planner.Seeds(context.Background(), u, pkgevents.DefaultTenantID, topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}, []topology.NodeKey{"raw.a"})

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
	sel, err := planner.Schedule(ctx, u, pkgevents.DefaultTenantID, "daily", model.OperationRun)
	require.NoError(t, err)
	r := sourceRun(t, sel.Stamp)

	require.NoError(t, planner.Start(ctx, u, r, sel, uuid.Nil))

	assert.Equal(t, 1, runs.saved)
	assert.Len(t, u.TaskCollection(pkgevents.DefaultTenantID).(*sourceTasks).created, 3)
	require.Len(t, outbox.events, 1)
	d, ok := outbox.events[0].(run.TaskDispatched)
	require.True(t, ok, "%T", outbox.events[0])
	assert.Equal(t, run.TaskIDFor("default", r.RunID(), "raw.a"), d.TaskID)
	assert.NotNil(t, r.PlannedAt())
}

// Each selection reads the releases of the tenant it is given, and a
// selection derived from a source run reads the source's tenant's; Start
// stores a run's tasks under the run's tenant, with that tenant's task ids.
func TestPlanner_ReadsAndWritesTheTenantItIsGivenOrTheRunsTenant(t *testing.T) {
	ctx := context.Background()
	live := topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}
	target := run.NodeID{ServiceName: "svc", SchemaName: "mart", TableName: "b"}

	u, topo := fakeUoW(live, nil)
	sel, err := planner.Schedule(ctx, u, "acme", "daily", model.OperationRun)
	require.NoError(t, err)
	_, err = planner.SingleNode(ctx, u, "acme", target, model.OperationRun, nil, nil)
	require.NoError(t, err)
	_, err = planner.Seeds(ctx, u, "acme", live, []topology.NodeKey{"raw.a"})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"lock", "live acme", "schedule acme rel-7 daily",
		"lock", "live acme", "graph acme rel-7",
		"graph acme rel-7",
	}, topo.calls)

	runs, outbox := &savedRuns{}, &appendedEvents{}
	u.SetRunRepo(runs)
	u.SetOutboxPublisher(outbox)
	r := sourceRunOf(t, "acme", sel.Stamp)
	require.NoError(t, planner.Start(ctx, u, r, sel, uuid.Nil))
	assert.Equal(t, []string{"acme"}, u.TaskCollectionTenants)
	require.Len(t, outbox.events, 1)
	assert.Equal(t, run.TaskIDFor("acme", r.RunID(), "raw.a"), outbox.events[0].(run.TaskDispatched).TaskID)

	tasks := []run.Task{task("mart.b", run.TaskStatusFailed), task("raw.a", run.TaskStatusSucceeded)}
	u, topo = fakeUoW(live, tasks)
	topo.releases = map[string]topology.Graph{"rel-3": threeNodes(), "rel-7": withRawAChanged()}
	source := sourceRunOf(t, "acme", run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9})
	_, err = planner.Rerun(ctx, u, source)
	require.NoError(t, err)
	_, _, err = planner.Rebase(ctx, u, source, runstart.RebaseOptions{})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"graph acme rel-3",
		"lock", "live acme", "graph acme rel-3", "schedule acme rel-7 daily",
	}, topo.calls)
	assertEvery(t, "acme", u.TaskCollectionTenants)
}

// assertEvery asserts got is not empty and holds only want.
func assertEvery(t *testing.T, want string, got []string) {
	t.Helper()
	require.NotEmpty(t, got)
	for _, g := range got {
		assert.Equal(t, want, g)
	}
}

const (
	seed    topology.NodeKey = "analytics.seed_fx_rates_eur"
	current topology.NodeKey = "analytics.fx_rates_current"
	latest  topology.NodeKey = "analytics.fx_rates_latest_eur"
	table   topology.NodeKey = "analytics.fx_transactions_eur"
	summary topology.NodeKey = "analytics.fx_summary"
)

var (
	livePointer = topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}
	defaults    = run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}
	tenant      = pkgevents.DefaultTenantID
	fxPlanner   = runstart.Planner{Tasks: defaults, Family: warehouse.SchemaBound, Engine: "postgres"}
)

// fxNode is a node of the fx release: the svc service's, in the analytics
// schema, materialized as materialized and in schedule.
func fxNode(key topology.NodeKey, nodeType, materialized, schedule string) topology.Node {
	table := string(key)[len("analytics."):]
	return topology.Node{
		Key: key, ServiceName: "svc", SchemaName: "analytics", TableName: table, NodeType: nodeType,
		Materialized: materialized, ImageTag: "svc:7", ScheduleName: schedule,
	}
}

// fxTopology is the seed (no +full_refresh, columns sha256:new), the view
// fx_rates_current that reads it, the view fx_rates_latest_eur that reads that
// view, and the table fx_transactions_eur that reads the seed. The seed and the
// table are in the daily schedule; the views are in none.
func fxTopology() topology.Graph {
	s := fxNode(seed, "dbt-seed", "seed", "daily")
	s.SeedColumnsHash = "sha256:new"
	return topology.NewGraph(
		[]topology.Node{s, fxNode(current, "dbt-model", "view", ""), fxNode(latest, "dbt-model", "view", ""),
			fxNode(table, "dbt-model", "table", "daily")},
		[]topology.Edge{{Upstream: seed, Downstream: current}, {Upstream: current, Downstream: latest}, {Upstream: seed, Downstream: table}},
	)
}

// withSeed is g with the seed changed by change.
func withSeed(g topology.Graph, change func(*topology.Node)) topology.Graph {
	nodes := make([]topology.Node, 0, g.Len())
	var edges []topology.Edge
	for _, k := range g.Keys() {
		n, _ := g.Node(k)
		if k == seed {
			change(&n)
		}
		nodes = append(nodes, n)
		for _, d := range g.Downstreams(k) {
			edges = append(edges, topology.Edge{Upstream: k, Downstream: d})
		}
	}
	return topology.NewGraph(nodes, edges)
}

// loadedColumns is a MaterializationRepository that answers only
// LoadedSeedColumns, with the column fingerprint each seed last loaded.
type loadedColumns map[topology.NodeKey]string

var _ repository.MaterializationRepository = loadedColumns{}

func (l loadedColumns) LoadedSeedColumns(_ context.Context, _ string, keys []topology.NodeKey) (map[topology.NodeKey]string, error) {
	out := map[topology.NodeKey]string{}
	for _, k := range keys {
		if c, ok := l[k]; ok {
			out[k] = c
		}
	}
	return out, nil
}
func (loadedColumns) List(context.Context, string) (map[topology.NodeKey]seeds.Materialization, error) {
	return nil, uow.ErrNotFaked
}
func (loadedColumns) RecordAttempt(context.Context, string, topology.NodeKey, string, uuid.UUID, time.Time) error {
	return uow.ErrNotFaked
}
func (loadedColumns) RecordBuilt(context.Context, string, seeds.Built) error { return uow.ErrNotFaked }
func (loadedColumns) DeleteAbsentFrom(context.Context, string, string) (int64, error) {
	return 0, uow.ErrNotFaked
}

// fxUoW is a unit of work over fxTopology on the live rel-7, whose seed last
// loaded sha256:old, and the source tasks given.
func fxUoW(tasks []run.Task) (*uow.FakeUnitOfWork, *graphTopology) {
	u, topo := fakeUoW(livePointer, tasks)
	topo.graph = fxTopology()
	u.MaterializationRepo = loadedColumns{seed: "sha256:old"}
	return u, topo
}

func keysOf(tasks []runplan.PlannedTask) []topology.NodeKey {
	out := make([]topology.NodeKey, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, t.Key)
	}
	return out
}

func taskOf(p runplan.Plan, k topology.NodeKey) runplan.PlannedTask {
	for _, t := range p.Tasks {
		if t.Key == k {
			return t
		}
	}
	return runplan.PlannedTask{}
}

// A promote-seed run of a seed whose columns changed fully refreshes it, after
// a check that only the views it rebuilds read it, and rebuilds the views
// below it once it has loaded.
func TestPlanner_SeedsPlansTheLoadAndTheRebuildsBelowIt(t *testing.T) {
	u, _ := fxUoW(nil)

	sel, err := fxPlanner.Seeds(context.Background(), u, tenant, livePointer, []topology.NodeKey{seed})

	require.NoError(t, err)
	assert.Equal(t, []topology.NodeKey{current, latest, seed}, keysOf(sel.Plan.Tasks))
	full := model.OperationFullRefresh
	assert.Equal(t, &full, taskOf(sel.Plan, seed).Operation)
	assert.Equal(t, "columns changed", taskOf(sel.Plan, seed).Reason)
	require.NotNil(t, taskOf(sel.Plan, seed).Preflight)
	assert.Equal(t, seedreload.PreflightCheck, taskOf(sel.Plan, seed).Preflight.Mode)
	assert.Equal(t, []string{string(current), string(latest)}, taskOf(sel.Plan, seed).Preflight.Allowed)
	assert.Equal(t, seed, taskOf(sel.Plan, current).RebuildOf)
	assert.Equal(t, seed, taskOf(sel.Plan, latest).RebuildOf)
	assert.Equal(t, []topology.NodeKey{seed}, sel.Plan.Ready)
	_, held := sel.Graph.Node(latest)
	assert.True(t, held)
}

// A seed whose table recorded no column fingerprint, or whose fingerprints
// match, has new rows only: it loads with the run's operation, drops nothing
// and rebuilds nothing.
func TestPlanner_AnEmptyFingerprintIsARowChange(t *testing.T) {
	for name, recorded := range map[string]loadedColumns{"no row": {}, "same columns": {seed: "sha256:new"}} {
		t.Run(name, func(t *testing.T) {
			u, _ := fxUoW(nil)
			u.MaterializationRepo = recorded

			sel, err := fxPlanner.Seeds(context.Background(), u, tenant, livePointer, []topology.NodeKey{seed})

			require.NoError(t, err)
			assert.Equal(t, []runplan.PlannedTask{{Key: seed}}, sel.Plan.Tasks)
			assert.Equal(t, []topology.NodeKey{seed}, sel.Plan.Ready)
		})
	}
}

// Every kind of run that loads a seed rebuilds the views its load drops.
func TestPlanner_EveryRunKindThatLoadsASeedAppliesTheRules(t *testing.T) {
	ctx := context.Background()
	failedSeed := func() []run.Task {
		return []run.Task{task(seed, run.TaskStatusFailed), task(table, run.TaskStatusSkipped)}
	}
	kinds := map[string]func(t *testing.T) runstart.Selection{
		"schedule": func(t *testing.T) runstart.Selection {
			u, topo := fxUoW(nil)
			topo.schedules = map[string][]topology.NodeKey{"daily": {table, seed}}
			sel, err := fxPlanner.Schedule(ctx, u, tenant, "daily", model.OperationRun)
			require.NoError(t, err)
			return sel
		},
		"rerun": func(t *testing.T) runstart.Selection {
			u, _ := fxUoW(failedSeed())
			sel, err := fxPlanner.Rerun(ctx, u, sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}))
			require.NoError(t, err)
			return sel
		},
		"single node": func(t *testing.T) runstart.Selection {
			u, _ := fxUoW(nil)
			target := run.NodeID{ServiceName: "svc", SchemaName: "analytics", TableName: "seed_fx_rates_eur"}
			sel, err := fxPlanner.SingleNode(ctx, u, tenant, target, model.OperationRun, nil, nil)
			require.NoError(t, err)
			return sel
		},
		"rebase": func(t *testing.T) runstart.Selection {
			u, topo := fxUoW(failedSeed())
			topo.schedules = map[string][]topology.NodeKey{"daily": {table, seed}}
			sel, pv, err := fxPlanner.Rebase(ctx, u, sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}), runstart.RebaseOptions{})
			require.NoError(t, err)
			assert.Equal(t, sel.Plan, pv.Plan)
			return sel
		},
	}
	for name, plan := range kinds {
		t.Run(name, func(t *testing.T) {
			sel := plan(t)
			assert.Equal(t, seed, taskOf(sel.Plan, current).RebuildOf)
			assert.Equal(t, seed, taskOf(sel.Plan, latest).RebuildOf)
			require.NotNil(t, taskOf(sel.Plan, seed).Preflight)
			assert.Equal(t, []topology.NodeKey{seed}, sel.Plan.Ready)
		})
	}
}

// A test run of a schedule loads no seed, so it plans no reload.
func TestPlanner_ATestRunPlansNoReload(t *testing.T) {
	u, topo := fxUoW(nil)
	two := 2
	topo.graph = withSeed(fxTopology(), func(n *topology.Node) { n.TestCount = &two })
	topo.schedules = map[string][]topology.NodeKey{"daily": {table, seed}}

	sel, err := fxPlanner.Schedule(context.Background(), u, tenant, "daily", model.OperationTest)

	require.NoError(t, err)
	assert.Equal(t, []topology.NodeKey{seed}, keysOf(sel.Plan.Tasks))
	for _, task := range sel.Plan.Tasks {
		assert.Nil(t, task.Preflight, task.Key)
		assert.Empty(t, task.Reason, task.Key)
		assert.Empty(t, task.RebuildOf, task.Key)
		assert.Nil(t, task.Operation, task.Key)
	}
}

// A table of the schedule that reads a view the run rebuilds waits for the
// rebuild, though the view is in no schedule.
func TestPlanner_AScheduleNodeThatReadsARebuiltViewWaitsForTheRebuild(t *testing.T) {
	u, topo := fxUoW(nil)
	g := fxTopology()
	nodes := []topology.Node{fxNode(summary, "dbt-model", "table", "daily")}
	edges := []topology.Edge{{Upstream: seed, Downstream: summary}, {Upstream: current, Downstream: summary}}
	for _, k := range g.Keys() {
		n, _ := g.Node(k)
		nodes = append(nodes, n)
		for _, d := range g.Downstreams(k) {
			edges = append(edges, topology.Edge{Upstream: k, Downstream: d})
		}
	}
	topo.graph = topology.NewGraph(nodes, edges)
	topo.schedules = map[string][]topology.NodeKey{"daily": {summary, seed}}

	sel, err := fxPlanner.Schedule(context.Background(), u, tenant, "daily", model.OperationRun)

	require.NoError(t, err)
	assert.Contains(t, keysOf(sel.Plan.Tasks), current)
	assert.NotContains(t, sel.Plan.Ready, summary)
	assert.Equal(t, []topology.NodeKey{seed}, sel.Plan.Ready)
	assert.Equal(t, []topology.NodeKey{current, seed}, sel.Graph.Upstreams(summary))
}

// A run of the daily schedule whose load of the seed drops the seed's table
// rebuilds the dormant views below the seed, though no schedule holds them: on
// Postgres when the seed's columns changed, and on Trino on every load, the
// seed's columns unchanged. A dormant table below the seed is no rebuild, and
// neither is the dormant view below that table: the walk stops at a table.
// The stop at a table comes from ViewClosure, the walk of this test's fake
// (graphTopology); TestViewClosure_ReachesADormantViewBelowASeed pins the same
// stop against the real SQL.
func TestPlanner_ADormantViewBelowAReloadedSeedIsRebuilt(t *testing.T) {
	const (
		dormantTable topology.NodeKey = "analytics.fx_rates_archive"
		belowTable   topology.NodeKey = "analytics.fx_rates_archive_eur"
	)
	g := fxTopology()
	nodes := []topology.Node{fxNode(dormantTable, "dbt-model", "table", ""), fxNode(belowTable, "dbt-model", "view", "")}
	edges := []topology.Edge{{Upstream: seed, Downstream: dormantTable}, {Upstream: dormantTable, Downstream: belowTable}}
	for _, k := range g.Keys() {
		n, _ := g.Node(k)
		nodes = append(nodes, n)
		for _, d := range g.Downstreams(k) {
			edges = append(edges, topology.Edge{Upstream: k, Downstream: d})
		}
	}
	withDormantTable := topology.NewGraph(nodes, edges)
	for _, k := range []topology.NodeKey{current, latest, dormantTable, belowTable} {
		n, ok := withDormantTable.Node(k)
		require.True(t, ok, "%s is in the release", k)
		require.True(t, n.IsDormant(), "%s is dormant", k)
	}

	cases := map[string]struct {
		planner runstart.Planner
		loaded  string
		reasons map[topology.NodeKey]string
	}{
		"postgres, columns changed": {
			planner: fxPlanner,
			loaded:  "sha256:old",
			reasons: map[topology.NodeKey]string{
				current: "rebuild: dropped by the load of analytics.seed_fx_rates_eur",
				latest:  "rebuild: dropped by the load of analytics.seed_fx_rates_eur, through analytics.fx_rates_current",
			},
		},
		"trino, columns unchanged": {
			planner: runstart.Planner{Tasks: defaults, Family: warehouse.LateBinding, Engine: "trino"},
			loaded:  "sha256:new",
			reasons: map[topology.NodeKey]string{
				current: "rebuild: may be stale on Trino after the load of analytics.seed_fx_rates_eur",
				latest:  "rebuild: may be stale on Trino after the load of analytics.seed_fx_rates_eur, through analytics.fx_rates_current",
			},
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			u, topo := fxUoW(nil)
			topo.graph = withDormantTable
			topo.schedules = map[string][]topology.NodeKey{"daily": {table, seed}}
			u.MaterializationRepo = loadedColumns{seed: c.loaded}

			sel, err := c.planner.Schedule(context.Background(), u, tenant, "daily", model.OperationRun)

			require.NoError(t, err)
			assert.Equal(t, []topology.NodeKey{current, latest, table, seed}, keysOf(sel.Plan.Tasks))
			for k, reason := range c.reasons {
				task := taskOf(sel.Plan, k)
				require.Equal(t, k, task.Key, "the dormant view %s is a task of the schedule's run", k)
				assert.Equal(t, reason, task.Reason, "%s", k)
				assert.Equal(t, seed, task.RebuildOf, "%s", k)
				n, ok := sel.Graph.Node(k)
				require.True(t, ok, "%s is in the run's graph", k)
				assert.True(t, n.IsDormant(), "%s is dormant in the run's graph", k)
			}
			assert.NotContains(t, keysOf(sel.Plan.Tasks), dormantTable, "a dormant table below the seed is not rebuilt")
			assert.NotContains(t, keysOf(sel.Plan.Tasks), belowTable, "a dormant view below that table is not rebuilt")
		})
	}
}

// A column change of a release that skipped validation says so at the end of
// the seed's reason.
func TestPlanner_ABootstrapReleaseMarksAColumnChangeUnvalidated(t *testing.T) {
	u, topo := fxUoW(nil)
	topo.bootstrap = map[string]bool{livePointer.ReleaseID: true}

	sel, err := fxPlanner.Seeds(context.Background(), u, tenant, livePointer, []topology.NodeKey{seed})

	require.NoError(t, err)
	assert.Equal(t, "columns changed; unvalidated: its release skipped validation", taskOf(sel.Plan, seed).Reason)
}

// What a seed's load drops depends on the warehouse family, so a planner
// without one refuses to plan a seed's load; a run that loads no seed needs
// none.
func TestPlanner_RefusesToPlanASeedLoadWithoutAWarehouseFamily(t *testing.T) {
	u, _ := fxUoW(nil)
	_, err := runstart.Planner{Tasks: defaults}.Seeds(context.Background(), u, tenant, livePointer, []topology.NodeKey{seed})
	assert.ErrorIs(t, err, runstart.ErrNoWarehouseFamily)

	_, err = runstart.Planner{Tasks: defaults}.Seeds(context.Background(), u, tenant, livePointer, []topology.NodeKey{table})
	assert.NoError(t, err)
}

// reloadCounts records the seed reload series.
type reloadCounts struct{ ended, stopped []string }

func (r *reloadCounts) SeedReloadEnded(action, outcome string) {
	r.ended = append(r.ended, action+" "+outcome)
}
func (r *reloadCounts) SeedReloadStopped(reason string) { r.stopped = append(r.stopped, reason) }
func (r *reloadCounts) ViewRebuilt(time.Duration)       {}

// A refused seed is counted and logged only once the run's transaction
// commits.
func TestPlanner_StartCountsAndLogsARefusalAfterCommit(t *testing.T) {
	ctx := context.Background()
	u, topo := fxUoW(nil)
	keep := false
	topo.graph = withSeed(fxTopology(), func(n *topology.Node) { n.FullRefresh = &keep })
	u.SetRunRepo(&savedRuns{})
	u.SetOutboxPublisher(&appendedEvents{})
	counts := &reloadCounts{}
	var logs bytes.Buffer
	p := fxPlanner
	p.Metrics = counts
	p.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	require.NoError(t, u.Begin(ctx))
	sel, err := p.Seeds(ctx, u, tenant, livePointer, []topology.NodeKey{seed})
	require.NoError(t, err)
	require.Equal(t, model.FailureReasonSeedFullRefreshDisabled, taskOf(sel.Plan, seed).Refusal)
	r := sourceRun(t, sel.Stamp)

	require.NoError(t, p.Start(ctx, u, r, sel, uuid.Nil))
	assert.Empty(t, counts.ended)
	assert.Empty(t, counts.stopped)
	assert.Empty(t, logs.String())

	require.NoError(t, u.Commit())
	assert.Equal(t, []string{"full_refresh refused"}, counts.ended)
	assert.Equal(t, []string{"seed_full_refresh_disabled"}, counts.stopped)
	var record map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &record))
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, tenant, record["tenant_id"])
	assert.Equal(t, "rel-7", record["release_id"])
	assert.Equal(t, r.RunID().String(), record["run_id"])
	assert.Equal(t, string(seed), record["node_key"])
	assert.Equal(t, "seed_full_refresh_disabled", record["reason"])
}

// A table of the schedule that reads only a view the run rebuilds, not the
// seed, still waits for the rebuild: without the view's edge it would be ready
// at once.
func TestPlanner_AScheduleNodeThatReadsOnlyARebuiltViewWaitsForTheRebuild(t *testing.T) {
	u, topo := fxUoW(nil)
	g := fxTopology()
	nodes := []topology.Node{fxNode(summary, "dbt-model", "table", "daily")}
	edges := []topology.Edge{{Upstream: current, Downstream: summary}}
	for _, k := range g.Keys() {
		n, _ := g.Node(k)
		nodes = append(nodes, n)
		for _, d := range g.Downstreams(k) {
			edges = append(edges, topology.Edge{Upstream: k, Downstream: d})
		}
	}
	topo.graph = topology.NewGraph(nodes, edges)
	topo.schedules = map[string][]topology.NodeKey{"daily": {summary, seed}}

	sel, err := fxPlanner.Schedule(context.Background(), u, tenant, "daily", model.OperationRun)

	require.NoError(t, err)
	assert.Contains(t, keysOf(sel.Plan.Tasks), current)
	assert.NotContains(t, sel.Plan.Ready, summary)
	assert.Equal(t, []topology.NodeKey{seed}, sel.Plan.Ready)
	assert.Equal(t, []topology.NodeKey{current}, sel.Graph.Upstreams(summary))
}

// An operator's full refresh of a seed with unchanged columns fully refreshes
// it for that reason.
func TestPlanner_ASingleNodeFullRefreshOfASeedIsAnOperatorsRequest(t *testing.T) {
	ctx := context.Background()
	target := run.NodeID{ServiceName: "svc", SchemaName: "analytics", TableName: "seed_fx_rates_eur"}
	u, _ := fxUoW(nil)
	u.MaterializationRepo = loadedColumns{seed: "sha256:new"}

	sel, err := fxPlanner.SingleNode(ctx, u, tenant, target, model.OperationFullRefresh, nil, nil)

	require.NoError(t, err)
	assert.Nil(t, taskOf(sel.Plan, seed).Operation, "the run's full_refresh")
	assert.Equal(t, "full refresh requested by an operator", taskOf(sel.Plan, seed).Reason)
	assert.Equal(t, seed, taskOf(sel.Plan, current).RebuildOf)
}

// A full refresh, or a load of changed columns, of a seed with
// +full_refresh: false on a schema_bound warehouse cannot be applied by any
// command, so the request is refused and plans no run. On a late_binding warehouse every load re-creates the table,
// so the same seed is planned and its preflight only reports.
func TestPlanner_SingleNodeRefusesAFullRefreshOfAFalseSeedOnPostgres(t *testing.T) {
	ctx := context.Background()
	target := run.NodeID{ServiceName: "svc", SchemaName: "analytics", TableName: "seed_fx_rates_eur"}
	keep := false
	falseSeed := withSeed(fxTopology(), func(n *topology.Node) { n.FullRefresh = &keep })

	u, topo := fxUoW(nil)
	topo.graph = falseSeed
	_, err := fxPlanner.SingleNode(ctx, u, tenant, target, model.OperationFullRefresh, nil, nil)
	assert.ErrorIs(t, err, runplan.ErrSeedFullRefreshDisabled)

	u, topo = fxUoW(nil)
	topo.graph = falseSeed
	lateBinding := fxPlanner
	lateBinding.Family, lateBinding.Engine = warehouse.LateBinding, "trino"
	sel, err := lateBinding.SingleNode(ctx, u, tenant, target, model.OperationFullRefresh, nil, nil)
	require.NoError(t, err)
	assert.Empty(t, taskOf(sel.Plan, seed).Refusal)
	require.NotNil(t, taskOf(sel.Plan, seed).Preflight)
	assert.Equal(t, seedreload.PreflightReport, taskOf(sel.Plan, seed).Preflight.Mode)
	assert.Equal(t, []topology.NodeKey{seed}, sel.Plan.Ready)

	u, topo = fxUoW(nil)
	topo.graph = falseSeed
	_, err = fxPlanner.SingleNode(ctx, u, tenant, target, model.OperationRun, nil, nil)
	assert.ErrorIs(t, err, runplan.ErrSeedFullRefreshDisabled, "a load of changed columns needs the table rebuilt too")

	u, topo = fxUoW(nil)
	topo.graph = falseSeed
	u.MaterializationRepo = loadedColumns{seed: "sha256:new"}
	sel, err = fxPlanner.SingleNode(ctx, u, tenant, target, model.OperationRun, nil, nil)
	require.NoError(t, err, "a load of new rows only drops nothing")
	assert.Empty(t, taskOf(sel.Plan, seed).Refusal)
	assert.Equal(t, []topology.NodeKey{seed}, sel.Plan.Ready)
}

// The objects an operator names reach the seed's preflight, and the run is the
// same DAG as a reload of the seed.
func TestPlanner_SingleNodeFullRefreshAllowsTheNamedObjects(t *testing.T) {
	target := run.NodeID{ServiceName: "svc", SchemaName: "analytics", TableName: "seed_fx_rates_eur"}
	u, _ := fxUoW(nil)

	sel, err := fxPlanner.SingleNode(context.Background(), u, tenant, target, model.OperationFullRefresh, nil, []string{"bi.fx_rates_report"})

	require.NoError(t, err)
	require.NotNil(t, taskOf(sel.Plan, seed).Preflight)
	assert.Equal(t, []string{"bi.fx_rates_report"}, taskOf(sel.Plan, seed).Preflight.Acknowledged)
	assert.Equal(t, []topology.NodeKey{current, latest, seed}, keysOf(sel.Plan.Tasks))
}

// Acknowledged dependents are valid for a full refresh of a dbt seed only.
func TestPlanner_SingleNodeRefusesAnAcknowledgementOutsideASeedFullRefresh(t *testing.T) {
	ctx := context.Background()
	seedTarget := run.NodeID{ServiceName: "svc", SchemaName: "analytics", TableName: "seed_fx_rates_eur"}
	viewTarget := run.NodeID{ServiceName: "svc", SchemaName: "analytics", TableName: "fx_rates_current"}
	for name, c := range map[string]struct {
		target run.NodeID
		op     model.Operation
	}{
		"a load of a seed":         {seedTarget, model.OperationRun},
		"a full refresh of a view": {viewTarget, model.OperationFullRefresh},
		"a run of a view":          {viewTarget, model.OperationRun},
		"a build of a seed":        {seedTarget, model.OperationBuild},
	} {
		t.Run(name, func(t *testing.T) {
			u, _ := fxUoW(nil)
			_, err := fxPlanner.SingleNode(ctx, u, tenant, c.target, c.op, nil, []string{"bi.x"})
			assert.ErrorIs(t, err, runplan.ErrAcknowledgementNotAllowed)
		})
	}
}

// A rerun of a failed single-node full refresh of a seed is still the
// operator's full refresh, though the seed's columns did not change.
func TestPlanner_ARerunOfASeedsFullRefreshIsStillAnOperatorsRequest(t *testing.T) {
	u, _ := fxUoW([]run.Task{task(seed, run.TaskStatusFailed)})
	u.MaterializationRepo = loadedColumns{seed: "sha256:new"}
	source, err := run.NewSingleNodeRun(tenant, uuid.New(), "single-node-run-1", model.OperationFullRefresh, nil, identity.SystemUserID,
		run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, planNow)
	require.NoError(t, err)

	sel, err := fxPlanner.Rerun(context.Background(), u, source)

	require.NoError(t, err)
	assert.Nil(t, taskOf(sel.Plan, seed).Operation, "the rerun's full_refresh")
	assert.Equal(t, "full refresh requested by an operator", taskOf(sel.Plan, seed).Reason)
	run := model.OperationRun
	assert.Equal(t, &run, taskOf(sel.Plan, current).Operation)
}

// A rerun of a run whose seed loaded and whose materialized-view rebuild
// failed re-executes the rebuild as the source planned it: a late-binding
// materialized view is fully refreshed, for the seed whose load dropped it.
func TestPlanner_ARerunKeepsAFailedRebuildOfAnInheritedSeed(t *testing.T) {
	const mv topology.NodeKey = "analytics.fx_rates_mv"
	reason := "rebuild: dropped by the load of analytics.seed_fx_rates_eur"
	rebuild := task(mv, run.TaskStatusFailed)
	rebuild.Operation, rebuild.PlanReason, rebuild.RebuildOf = model.OperationFullRefresh, reason, seed
	u, topo := fxUoW([]run.Task{task(seed, run.TaskStatusSucceeded), rebuild})
	s := fxNode(seed, "dbt-seed", "seed", "daily")
	topo.graph = topology.NewGraph([]topology.Node{s, fxNode(mv, "dbt-model", "materialized_view", "")},
		[]topology.Edge{{Upstream: seed, Downstream: mv}})
	p := runstart.Planner{Tasks: defaults, Family: warehouse.LateBinding, Engine: "trino"}

	sel, err := p.Rerun(context.Background(), u, sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}))

	require.NoError(t, err)
	full := model.OperationFullRefresh
	assert.Equal(t, runplan.PlannedTask{Key: mv, Operation: &full, Reason: reason, RebuildOf: seed}, taskOf(sel.Plan, mv))
	assert.True(t, taskOf(sel.Plan, seed).Inherited)
	assert.Equal(t, []topology.NodeKey{mv}, sel.Plan.Ready)
}

// A rebase of a run whose seed loaded and whose rebuilds did not finish
// executes them again, as the source planned them, though the views are in no
// schedule.
func TestPlanner_ARebaseExecutesTheRebuildsItsSourceOwes(t *testing.T) {
	reason := "rebuild: dropped by the load of analytics.seed_fx_rates_eur"
	owed := func(key topology.NodeKey, status run.TaskStatus) run.Task {
		t := task(key, status)
		t.PlanReason, t.RebuildOf = reason, seed
		return t
	}
	u, topo := fxUoW([]run.Task{
		task(seed, run.TaskStatusSucceeded), task(table, run.TaskStatusSucceeded),
		owed(current, run.TaskStatusFailed), owed(latest, run.TaskStatusSkipped),
	})
	topo.schedules = map[string][]topology.NodeKey{"daily": {table, seed}}

	sel, pv, err := fxPlanner.Rebase(context.Background(), u, sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}), runstart.RebaseOptions{})

	require.NoError(t, err)
	assert.Equal(t, runplan.PlannedTask{Key: current, Reason: reason, RebuildOf: seed}, taskOf(sel.Plan, current))
	assert.Equal(t, runplan.PlannedTask{Key: latest, Reason: reason, RebuildOf: seed}, taskOf(sel.Plan, latest))
	assert.True(t, taskOf(sel.Plan, seed).Inherited)
	assert.True(t, taskOf(sel.Plan, table).Inherited)
	assert.Equal(t, []topology.NodeKey{current}, sel.Plan.Ready)
	assert.Equal(t, 2, pv.FailedOrSkipped)
	assert.Equal(t, 2, pv.Inherited)
}

// A rebase of a single-node load of a seed whose rebuilds did not finish
// executes them again, as the source planned them, though the source's
// schedule name is no schedule of the release and so holds no node.
func TestPlanner_ARebaseOfASingleNodeSeedLoadExecutesTheRebuildsItOwes(t *testing.T) {
	reason := "rebuild: dropped by the load of analytics.seed_fx_rates_eur"
	owed := func(key topology.NodeKey, status run.TaskStatus) run.Task {
		t := task(key, status)
		t.PlanReason, t.RebuildOf = reason, seed
		return t
	}
	u, topo := fxUoW([]run.Task{
		task(seed, run.TaskStatusSucceeded), owed(current, run.TaskStatusFailed), owed(latest, run.TaskStatusSkipped),
	})
	topo.schedules = map[string][]topology.NodeKey{"single-node-run-1": {}}
	source, err := run.NewSingleNodeRun(tenant, uuid.New(), "single-node-run-1", model.OperationRun, nil, identity.SystemUserID,
		run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}, planNow)
	require.NoError(t, err)

	sel, pv, err := fxPlanner.Rebase(context.Background(), u, source, runstart.RebaseOptions{})

	require.NoError(t, err)
	assert.Equal(t, []topology.NodeKey{current, latest, seed}, keysOf(sel.Plan.Tasks))
	assert.Equal(t, runplan.PlannedTask{Key: current, Reason: reason, RebuildOf: seed}, taskOf(sel.Plan, current))
	assert.Equal(t, runplan.PlannedTask{Key: latest, Reason: reason, RebuildOf: seed}, taskOf(sel.Plan, latest))
	assert.True(t, taskOf(sel.Plan, seed).Inherited)
	assert.Equal(t, []topology.NodeKey{current}, sel.Plan.Ready)
	assert.Equal(t, 2, pv.FailedOrSkipped)
	assert.Equal(t, 1, pv.Inherited)
}

// A rebase preview counts the rebuilds of the seeds it loads among what it
// executes.
func TestPlanner_ARebasePreviewCountsTheRebuilds(t *testing.T) {
	u, topo := fxUoW([]run.Task{task(seed, run.TaskStatusFailed), task(table, run.TaskStatusSkipped)})
	topo.schedules = map[string][]topology.NodeKey{"daily": {table, seed}}

	sel, pv, err := fxPlanner.Rebase(context.Background(), u, sourceRun(t, run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9}), runstart.RebaseOptions{})

	require.NoError(t, err)
	assert.Equal(t, 2, pv.FailedOrSkipped)
	assert.Equal(t, 2, pv.Downstream, "the rebuilds of current and latest")
	assert.Equal(t, 4, pv.WillRun())
	assert.Equal(t, len(sel.Plan.Tasks), pv.Total())
	assert.Equal(t, sel.Plan, pv.Plan)
}
