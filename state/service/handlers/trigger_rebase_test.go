package handlers_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errNoRun is what rebaseRuns answers for a run it does not hold.
var errNoRun = errors.New("no such run")

// rebaseRuns holds one source run, under its own tenant only, answers whether
// its schedule has an active run, and records the runs saved.
type rebaseRuns struct {
	repository.RunRepository
	source *run.Run
	active bool
	saved  []*run.Run
}

func (r *rebaseRuns) GetRun(_ context.Context, tenantID string, _ uuid.UUID) (*run.Run, error) {
	if tenantID != r.source.TenantID() {
		return nil, errNoRun
	}
	return r.source, nil
}
func (r *rebaseRuns) HasActiveRun(context.Context, string, string) (bool, error) {
	return r.active, nil
}
func (r *rebaseRuns) SaveRun(_ context.Context, rn *run.Run) error {
	r.saved = append(r.saved, rn)
	return nil
}

// rebaseTasks holds the source run's tasks and records the tasks a new run
// starts with; every ready task gets its node's lease and is dispatched.
type rebaseTasks struct {
	run.TaskCollection
	attempts []run.Attempt
	tasks    []run.Task
	created  []run.Task
}

func (t *rebaseTasks) HasNonSucceeded(context.Context, uuid.UUID) (bool, error) { return true, nil }
func (t *rebaseTasks) ListRunTasks(context.Context, uuid.UUID) ([]run.Task, error) {
	return t.tasks, nil
}
func (t *rebaseTasks) BulkCreate(_ context.Context, tasks []run.Task) error {
	t.created = append(t.created, tasks...)
	return nil
}

func (t *rebaseTasks) AddAttempts(_ context.Context, attempts []run.Attempt) error {
	t.attempts = append(t.attempts, attempts...)
	return nil
}

func (t *rebaseTasks) TakeLeases(_ context.Context, claims []run.LeaseClaim) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0, len(claims))
	for _, c := range claims {
		ids = append(ids, c.TaskID)
	}
	return ids, nil
}

func (t *rebaseTasks) MarkDispatched(_ context.Context, ids []uuid.UUID, _ time.Time) ([]uuid.UUID, error) {
	return ids, nil
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

type recordedEvents struct{ events []run.DomainEvent }

func (o *recordedEvents) Append(_ context.Context, events []run.DomainEvent, _ uuid.UUID) error {
	o.events = append(o.events, events...)
	return nil
}

// chain is raw.a feeding mart.b feeding mart.c; hash is raw.a's content hash.
func chain(hash string) topology.Graph {
	node := func(schema, table, contentHash string) topology.Node {
		return topology.Node{
			Key: topology.KeyOf(schema, table), ServiceName: "svc", SchemaName: schema, TableName: table,
			NodeType: "dbt-model", ContentHash: contentHash, ImageTag: "svc:1", ScheduleName: "daily",
		}
	}
	return topology.NewGraph(
		[]topology.Node{node("raw", "a", hash), node("mart", "b", "h-b"), node("mart", "c", "h-c")},
		[]topology.Edge{{Upstream: "raw.a", Downstream: "mart.b"}, {Upstream: "mart.b", Downstream: "mart.c"}},
	)
}

// rebaseFixture is a failed run of daily on rel-3 whose mart.b failed and
// skipped mart.c, and a live rel-7 that changed raw.a.
type rebaseFixture struct {
	u       *uow.FakeUnitOfWork
	runs    *rebaseRuns
	tasks   *rebaseTasks
	outbox  *recordedEvents
	planner runstart.Planner
}

func newRebaseFixture(t *testing.T) *rebaseFixture {
	t.Helper()
	return newRebaseFixtureOf(t, pkgevents.DefaultTenantID)
}

// newRebaseFixtureOf is newRebaseFixture with a source run of tenantID.
func newRebaseFixtureOf(t *testing.T, tenantID string) *rebaseFixture {
	t.Helper()
	created := time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)
	source := run.HydrateRun(tenantID, uuid.New(), "daily", run.RunStatusFailed, run.KindCron, nil,
		identity.SystemUserID, model.OperationRun, created, nil, &created, nil, nil, nil, nil, nil, 0, nil)
	source.HydrateRelease(run.ReleaseStamp{ReleaseID: "rel-3", PromotionSeq: 9})
	f := &rebaseFixture{
		u:      &uow.FakeUnitOfWork{},
		runs:   &rebaseRuns{source: source},
		outbox: &recordedEvents{},
		tasks: &rebaseTasks{tasks: []run.Task{
			{TaskID: uuid.New(), NodeKey: "mart.b", Status: run.TaskStatusFailed},
			{TaskID: uuid.New(), NodeKey: "mart.c", Status: run.TaskStatusSkipped},
			{TaskID: uuid.New(), NodeKey: "raw.a", Status: run.TaskStatusSucceeded},
		}},
		planner: runstart.Planner{Tasks: run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}},
	}
	f.u.TopologyRepo = &releaseGraphs{
		LiveReleaseOnly: uow.LiveReleaseOnly{Live: topology.Pointer{ReleaseID: "rel-7", PromotionSeq: 12}},
		graphs:          map[string]topology.Graph{"rel-3": chain("h-a"), "rel-7": chain("h-a-fixed")},
	}
	f.u.SetRunRepo(f.runs)
	f.u.SetTaskCollection(f.tasks)
	f.u.SetOutboxPublisher(f.outbox)
	return f
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// A rebase with the option runs the changed raw.a and everything below it on
// the live release, in one committed transaction.
func TestTriggerRebase_CreatesTheRunOnTheLiveRelease(t *testing.T) {
	f := newRebaseFixture(t)

	id, name, err := svchandlers.NewTriggerRebaseHandler(f.planner, discard()).Handle(context.Background(), f.u, svchandlers.TriggerRebaseInput{
		TenantID: pkgevents.DefaultTenantID, SourceRunID: f.runs.source.RunID(), IncludeChangedUpstream: true, ExpectedReleaseID: "rel-7",
		Initiator: identity.FromUserID("okta|alice"),
	})

	require.NoError(t, err)
	assert.Equal(t, "daily", name)
	assert.Equal(t, 1, f.u.CommitCalled)
	require.NotEmpty(t, f.runs.saved)
	r := f.runs.saved[0]
	assert.Equal(t, id, r.RunID())
	assert.Equal(t, run.KindRebase, r.Kind())
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-7", PromotionSeq: 12}, r.Release())
	assert.Len(t, f.tasks.created, 3)
	require.Len(t, f.outbox.events, 1)
	d, ok := f.outbox.events[0].(run.TaskDispatched)
	require.True(t, ok, "%T", f.outbox.events[0])
	assert.Equal(t, run.TaskIDFor("default", id, "raw.a"), d.TaskID, "the changed raw.a is dispatched first")
}

// Without the option raw.a is inherited and the failed mart.b is dispatched.
func TestTriggerRebase_WithoutTheOptionInheritsTheChangedUpstream(t *testing.T) {
	f := newRebaseFixture(t)

	id, _, err := svchandlers.NewTriggerRebaseHandler(f.planner, discard()).Handle(context.Background(), f.u, svchandlers.TriggerRebaseInput{
		TenantID: pkgevents.DefaultTenantID, SourceRunID: f.runs.source.RunID(), Initiator: identity.FromUserID("okta|alice"),
	})

	require.NoError(t, err)
	require.Len(t, f.outbox.events, 1)
	d, ok := f.outbox.events[0].(run.TaskDispatched)
	require.True(t, ok, "%T", f.outbox.events[0])
	assert.Equal(t, run.TaskIDFor("default", id, "mart.b"), d.TaskID)
}

// A rebase previewed on another live release writes nothing.
func TestTriggerRebase_RefusesAMovedLiveReleaseAndWritesNothing(t *testing.T) {
	f := newRebaseFixture(t)

	_, _, err := svchandlers.NewTriggerRebaseHandler(f.planner, discard()).Handle(context.Background(), f.u, svchandlers.TriggerRebaseInput{
		TenantID: pkgevents.DefaultTenantID, SourceRunID: f.runs.source.RunID(), IncludeChangedUpstream: true, ExpectedReleaseID: "rel-6",
	})

	require.ErrorIs(t, err, runstart.ErrLiveReleaseMoved)
	assert.Equal(t, 0, f.u.CommitCalled)
	assert.Equal(t, 1, f.u.RollbackCalled)
	assert.Empty(t, f.runs.saved)
	assert.Empty(t, f.outbox.events)
}

// A schedule with an active run is refused before the transaction begins.
func TestTriggerRebase_RefusesAScheduleWithAnActiveRun(t *testing.T) {
	f := newRebaseFixture(t)
	f.runs.active = true

	_, _, err := svchandlers.NewTriggerRebaseHandler(f.planner, discard()).Handle(context.Background(), f.u, svchandlers.TriggerRebaseInput{
		TenantID: pkgevents.DefaultTenantID, SourceRunID: f.runs.source.RunID(),
	})

	require.ErrorIs(t, err, run.ErrScheduleHasActiveRun)
	assert.Equal(t, 0, f.u.BeginCalled)
	assert.Empty(t, f.runs.saved)
}

// A rebase is derived under its source's tenant: the request's tenant finds
// the source, and the new run, its tasks and their ids are that tenant's.
// Another tenant's source is not found, and nothing is written.
func TestTriggerRebase_DerivesTheRunUnderTheSourcesTenant(t *testing.T) {
	f := newRebaseFixtureOf(t, "acme")
	h := svchandlers.NewTriggerRebaseHandler(f.planner, discard())

	_, _, err := h.Handle(context.Background(), f.u, svchandlers.TriggerRebaseInput{
		TenantID: pkgevents.DefaultTenantID, SourceRunID: f.runs.source.RunID(),
	})
	require.ErrorIs(t, err, errNoRun)
	assert.Empty(t, f.runs.saved)
	assert.Equal(t, 0, f.u.BeginCalled)

	id, _, err := h.Handle(context.Background(), f.u, svchandlers.TriggerRebaseInput{
		TenantID: "acme", SourceRunID: f.runs.source.RunID(),
	})
	require.NoError(t, err)
	require.NotEmpty(t, f.runs.saved)
	assert.Equal(t, "acme", f.runs.saved[0].TenantID())
	require.NotEmpty(t, f.u.TaskCollectionTenants)
	for _, tenant := range f.u.TaskCollectionTenants {
		assert.Equal(t, "acme", tenant)
	}
	require.Len(t, f.outbox.events, 1)
	assert.Equal(t, run.TaskIDFor("acme", id, "mart.b"), f.outbox.events[0].(run.TaskDispatched).TaskID)
}
