package seedreconcile_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/seeds"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/seedreconcile"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testNow = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

type fixedClock struct{}

func (fixedClock) Now() time.Time { return testNow }

// fakeTopology serves one tenant's live pointer and its releases' nodes, and
// records the calls it receives.
type fakeTopology struct {
	live  topology.Pointer
	nodes map[string][]topology.Node // by release id
	calls []string
}

var _ repository.TopologyRepository = (*fakeTopology)(nil)

func (f *fakeTopology) LockTopology(context.Context) error {
	f.calls = append(f.calls, "lock")
	return nil
}

func (f *fakeTopology) SaveRelease(context.Context, topology.Release) (bool, error) {
	return false, errors.New("SaveRelease is not used by the seed reconcile")
}

func (f *fakeTopology) LivePointer(context.Context, string) (topology.Pointer, error) {
	f.calls = append(f.calls, "live")
	return f.live, nil
}

func (f *fakeTopology) SetLivePointer(context.Context, string, topology.Pointer, time.Time) error {
	return errors.New("SetLivePointer is not used by the seed reconcile")
}

func (f *fakeTopology) ReleaseNodes(_ context.Context, _ string, releaseID, nodeType string) ([]topology.Node, error) {
	f.calls = append(f.calls, "nodes:"+releaseID)
	var out []topology.Node
	for _, n := range f.nodes[releaseID] {
		if nodeType == "" || n.NodeType == nodeType {
			out = append(out, n)
		}
	}
	return out, nil
}

func (f *fakeTopology) ScheduleGraph(context.Context, string, string, string) (topology.Graph, error) {
	return topology.Graph{}, errors.New("ScheduleGraph is not used by the seed reconcile")
}
func (f *fakeTopology) Graph(_ context.Context, _ string, releaseID string, keys []topology.NodeKey) (topology.Graph, error) {
	f.calls = append(f.calls, "graph:"+releaseID)
	want := make(map[topology.NodeKey]bool, len(keys))
	for _, k := range keys {
		want[k] = true
	}
	var nodes []topology.Node
	for _, n := range f.nodes[releaseID] {
		if want[n.Key] {
			nodes = append(nodes, n)
		}
	}
	return topology.NewGraph(nodes, nil), nil
}

func (f *fakeTopology) NodeContentHash(context.Context, string, string, topology.NodeKey) (string, bool, error) {
	return "", false, errors.New("NodeContentHash is not used by the seed reconcile")
}

func (f *fakeTopology) PruneReleases(context.Context, time.Time) (int64, error) {
	return 0, errors.New("PruneReleases is not used by the seed reconcile")
}

// attempt is one RecordAttempt call.
type attempt struct {
	key   topology.NodeKey
	hash  string
	runID uuid.UUID
}

// fakeMaterializations keeps node_materializations in memory. A build request
// leaves its seed's attempt in flight, as a freshly created run is.
type fakeMaterializations struct {
	rows     map[topology.NodeKey]seeds.Materialization
	built    []seeds.Built
	attempts []attempt
}

var _ repository.MaterializationRepository = (*fakeMaterializations)(nil)

func (f *fakeMaterializations) List(context.Context, string) (map[topology.NodeKey]seeds.Materialization, error) {
	out := make(map[topology.NodeKey]seeds.Materialization, len(f.rows))
	for k, v := range f.rows {
		out[k] = v
	}
	return out, nil
}

func (f *fakeMaterializations) RecordAttempt(_ context.Context, _ string, key topology.NodeKey, hash string, runID uuid.UUID, _ time.Time) error {
	f.attempts = append(f.attempts, attempt{key: key, hash: hash, runID: runID})
	m := f.rows[key]
	m.Key, m.AttemptContentHash, m.AttemptRunID, m.AttemptState = key, hash, runID, seeds.AttemptInFlight
	f.rows[key] = m
	return nil
}

func (f *fakeMaterializations) RecordBuilt(_ context.Context, _ string, b seeds.Built) error {
	f.built = append(f.built, b)
	m := f.rows[b.Key]
	m.Key, m.ContentHash = b.Key, b.ContentHash
	f.rows[b.Key] = m
	return nil
}

func (f *fakeMaterializations) DeleteAbsentFrom(context.Context, string, string) (int64, error) {
	return 0, nil
}

// fakeRuns persists runs in memory. taken holds the ids already persisted
// (including runs that name no release); count is what CountReleaseRuns
// answers and grows with every run saved.
type fakeRuns struct {
	taken     map[uuid.UUID]bool
	count     int
	saved     []*run.Run
	saveErr   error
	existsAsk []uuid.UUID
}

var _ repository.RunRepository = (*fakeRuns)(nil)

// fakeTasks records the tasks a run starts with; every ready task gets its
// node's lease and is dispatched.
type fakeTasks struct {
	run.TaskCollection
	attempts []run.Attempt
	created  []run.Task
}

func (f *fakeTasks) BulkCreate(_ context.Context, tasks []run.Task) error {
	f.created = append(f.created, tasks...)
	return nil
}

func (f *fakeTasks) AddAttempts(_ context.Context, attempts []run.Attempt) error {
	f.attempts = append(f.attempts, attempts...)
	return nil
}

func (f *fakeTasks) TakeLeases(_ context.Context, claims []run.LeaseClaim) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0, len(claims))
	for _, c := range claims {
		ids = append(ids, c.TaskID)
	}
	return ids, nil
}

func (f *fakeTasks) MarkDispatched(_ context.Context, ids []uuid.UUID, _ time.Time) ([]uuid.UUID, error) {
	return ids, nil
}

func (f *fakeRuns) SaveRun(_ context.Context, r *run.Run) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	if !r.Changes().IsCreated() {
		r.ResetChanges()
		return nil
	}
	if f.taken[r.ScheduleID()] {
		return run.ErrRunAlreadyExists
	}
	f.taken[r.ScheduleID()] = true
	f.saved = append(f.saved, r)
	f.count++
	r.ResetChanges()
	return nil
}

func (f *fakeRuns) RunExists(_ context.Context, id uuid.UUID) (bool, error) {
	f.existsAsk = append(f.existsAsk, id)
	return f.taken[id], nil
}

func (f *fakeRuns) ListUnplannedActive(context.Context) ([]uuid.UUID, error) {
	panic("ListUnplannedActive is not used here")
}

func (f *fakeRuns) CountReleaseRuns(context.Context, string, run.Kind) (int, error) {
	return f.count, nil
}

func (f *fakeRuns) GetRun(context.Context, uuid.UUID) (*run.Run, error) {
	panic("GetRun is not used by the seed reconcile")
}

func (f *fakeRuns) LoadRunForUpdate(context.Context, uuid.UUID) (*run.Run, error) {
	panic("LoadRunForUpdate is not used by the seed reconcile")
}

func (f *fakeRuns) HasActiveSchedule(context.Context, string) (bool, error) {
	panic("HasActiveSchedule is not used by the seed reconcile")
}

func (f *fakeRuns) GetActiveScheduler(context.Context, string) (*run.Run, error) {
	panic("GetActiveScheduler is not used by the seed reconcile")
}

func (f *fakeRuns) GetLastRunPerSchedule(context.Context) (map[string]repository.LastRunSummary, error) {
	panic("GetLastRunPerSchedule is not used by the seed reconcile")
}

// fakeOutbox captures appended events and the message-processing id of each
// append.
type fakeOutbox struct {
	appended   []run.DomainEvent
	msgProcIDs []uuid.UUID
}

func (f *fakeOutbox) Append(_ context.Context, evts []run.DomainEvent, msgProcID uuid.UUID) error {
	f.appended = append(f.appended, evts...)
	f.msgProcIDs = append(f.msgProcIDs, msgProcID)
	return nil
}

// world is one tenant's state behind fake repositories; every unit of work it
// hands out shares them.
type world struct {
	topo   *fakeTopology
	mats   *fakeMaterializations
	runs   *fakeRuns
	tasks  *fakeTasks
	outbox *fakeOutbox
	uows   []*uow.FakeUnitOfWork
}

func newWorld(live topology.Pointer, nodes ...topology.Node) *world {
	return &world{
		topo:   &fakeTopology{live: live, nodes: map[string][]topology.Node{live.ReleaseID: nodes}},
		mats:   &fakeMaterializations{rows: map[topology.NodeKey]seeds.Materialization{}},
		runs:   &fakeRuns{taken: map[uuid.UUID]bool{}},
		tasks:  &fakeTasks{},
		outbox: &fakeOutbox{},
	}
}

func (w *world) newUoW() *uow.FakeUnitOfWork {
	u := &uow.FakeUnitOfWork{TopologyRepo: w.topo, MaterializationRepo: w.mats}
	u.SetRunRepo(w.runs)
	u.SetTaskCollection(w.tasks)
	u.SetOutboxPublisher(w.outbox)
	u.SetClock(fixedClock{})
	w.uows = append(w.uows, u)
	return u
}

func (w *world) reconciler(maintenance bool) *seedreconcile.Reconciler {
	return seedreconcile.New(func() uow.UnitOfWork { return w.newUoW() }, maintenance, "default", run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// loaded records table as last loaded at hash.
func (w *world) loaded(table, hash string) {
	key := topology.KeyOf("raw", table)
	w.mats.rows[key] = seeds.Materialization{Key: key, ContentHash: hash}
}

func seedNode(table, hash string) topology.Node {
	return topology.Node{
		Key: topology.KeyOf("raw", table), UniqueID: "seed.svc." + table, ServiceName: "svc", SchemaName: "raw",
		TableName: table, NodeType: "dbt-seed", ContentHash: hash, ImageTag: "svc:2",
	}
}

func modelNode(table string) topology.Node {
	return topology.Node{
		Key: topology.KeyOf("mart", table), UniqueID: "model.svc." + table, ServiceName: "svc", SchemaName: "mart",
		TableName: table, NodeType: "dbt-model", ContentHash: "h-" + table, ImageTag: "svc:2",
	}
}

// dispatchedKeys lists the node of each TaskDispatched in events.
func dispatchedKeys(events []run.DomainEvent) []topology.NodeKey {
	var out []topology.NodeKey
	for _, e := range events {
		if d, ok := e.(run.TaskDispatched); ok {
			out = append(out, d.Node.Key)
		}
	}
	return out
}

func moved(releaseID string, seq int64) ports.PromotionTrigger {
	return ports.PromotionTrigger{
		TenantID: "default", ReleaseID: releaseID, PromotionSeq: seq, Outcome: topology.PointerMoved,
		MessageProcessingID: uuid.New(),
	}
}

func scheduleNameOf(id uuid.UUID) string {
	return "promote-seed-" + strings.ReplaceAll(id.String(), "-", "")[:8]
}

var live7 = topology.Pointer{ReleaseID: "rel-2", PromotionSeq: 7}

// A pass that follows a pointer move builds exactly the seeds whose live
// content differs from what was loaded, in one run on the live release that
// dispatches each of them, under the message's dedup row.
func TestAfterPromotion_MovedBuildsTheSeedsWhoseContentDiffers(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"), seedNode("users", "h-users-1"), seedNode("blank", ""), modelNode("revenue"))
	w.loaded("orders", "h-orders-1")
	w.loaded("users", "h-users-1")
	u := w.newUoW()
	trigger := moved("rel-2", 7)

	require.NoError(t, w.reconciler(false).AfterPromotion(context.Background(), u, trigger))

	wantID := seeds.RunID("rel-2", 1)
	require.Len(t, w.runs.saved, 1)
	created := w.runs.saved[0]
	assert.Equal(t, wantID, created.ScheduleID())
	assert.Equal(t, scheduleNameOf(wantID), created.ScheduleName())
	assert.Equal(t, run.KindPromoteSeed, created.Kind())
	assert.Equal(t, run.SchedulerStatusRunning, created.Status())
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-2", PromotionSeq: 7}, created.Release())
	assert.NotNil(t, created.PlannedAt())
	require.Len(t, w.outbox.appended, 1)
	d, ok := w.outbox.appended[0].(run.TaskDispatched)
	require.True(t, ok, "%T", w.outbox.appended[0])
	assert.Equal(t, run.TaskIDFor("default", wantID, "raw.orders"), d.TaskID)
	assert.Equal(t, seedNode("orders", "h-orders-2"), d.Node)
	assert.Equal(t, scheduleNameOf(wantID), d.ScheduleName)
	assert.Equal(t, []uuid.UUID{trigger.MessageProcessingID}, w.outbox.msgProcIDs)
	assert.Equal(t, []attempt{{key: "raw.orders", hash: "h-orders-2", runID: wantID}}, w.mats.attempts)
	assert.Empty(t, w.mats.built)
	assert.Zero(t, u.BeginCalled, "the pass runs in the handler's transaction")
}

// A redelivered, older or conflicting promotion left the live release as it
// was: no pass runs.
func TestAfterPromotion_OtherOutcomesRunNoPass(t *testing.T) {
	for _, outcome := range []topology.PointerOutcome{topology.PointerRedelivered, topology.PointerStale, topology.PointerConflict} {
		t.Run(outcome.String(), func(t *testing.T) {
			w := newWorld(live7, seedNode("orders", "h-orders-2"))
			trigger := moved("rel-2", 7)
			trigger.Outcome = outcome

			require.NoError(t, w.reconciler(false).AfterPromotion(context.Background(), w.newUoW(), trigger))

			assert.Empty(t, w.topo.calls, "nothing is read")
			assert.Empty(t, w.runs.saved)
			assert.Empty(t, w.outbox.appended)
		})
	}
}

// Adoption belongs to a promotion that moved the live release: one that left it
// as it was adopts nothing, whatever the first-promotion flag says.
func TestAfterPromotion_OtherOutcomesAdoptNothing(t *testing.T) {
	for _, outcome := range []topology.PointerOutcome{topology.PointerRedelivered, topology.PointerStale, topology.PointerConflict} {
		t.Run(outcome.String(), func(t *testing.T) {
			w := newWorld(live7, seedNode("orders", "h-orders-2"), seedNode("users", "h-users-1"))
			trigger := moved("rel-2", 7)
			trigger.Outcome = outcome
			trigger.FirstPromotion = true
			trigger.ChangedNodeIDs = []string{"seed.svc.orders"}

			require.NoError(t, w.reconciler(false).AfterPromotion(context.Background(), w.newUoW(), trigger))

			assert.Empty(t, w.topo.calls, "nothing is read")
			assert.Empty(t, w.runs.existsAsk)
			assert.Empty(t, w.mats.built)
			assert.Empty(t, w.runs.saved)
			assert.Empty(t, w.outbox.appended)
		})
	}
}

// An install's first release adopts nothing: every seed with a content hash is
// built, and the first-run lookup is not made.
func TestAfterPromotion_FirstBootstrapPromotionBuildsEverySeed(t *testing.T) {
	w := newWorld(live7, seedNode("users", "h-users-1"), seedNode("orders", "h-orders-2"), seedNode("blank", ""))
	trigger := moved("rel-2", 7)
	trigger.FirstPromotion, trigger.Bootstrap = true, true

	require.NoError(t, w.reconciler(false).AfterPromotion(context.Background(), w.newUoW(), trigger))

	assert.Empty(t, w.mats.built, "nothing is adopted")
	assert.Empty(t, w.runs.existsAsk)
	assert.Equal(t, []topology.NodeKey{"raw.orders", "raw.users"}, dispatchedKeys(w.outbox.appended),
		"every hashed seed, in key order")
}

// On the first promotion of an install that already ran, the seeds the
// release did not change are recorded as loaded; the changed one is built.
func TestAfterPromotion_FirstPromotionAdoptsTheUnchangedSeeds(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"), seedNode("users", "h-users-1"), seedNode("blank", ""))
	trigger := moved("rel-2", 7)
	trigger.FirstPromotion = true
	trigger.ChangedNodeIDs = []string{"seed.svc.orders"}

	require.NoError(t, w.reconciler(false).AfterPromotion(context.Background(), w.newUoW(), trigger))

	assert.Equal(t, []uuid.UUID{seeds.RunID("rel-2", 1)}, w.runs.existsAsk)
	assert.Equal(t, []seeds.Built{{
		Key: "raw.users", ContentHash: "h-users-1", ImageTag: "svc:2", ReleaseID: "rel-2", At: testNow, Source: seeds.SourceAdopted,
	}}, w.mats.built)
	assert.Equal(t, []topology.NodeKey{"raw.orders"}, dispatchedKeys(w.outbox.appended))
}

// When the release's first promote-seed run already exists, every seed is
// adopted and nothing is built.
func TestAfterPromotion_FirstPromotionAdoptsEverySeedWhenItsFirstRunExists(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"), seedNode("users", "h-users-1"))
	w.runs.taken[seeds.RunID("rel-2", 1)] = true
	trigger := moved("rel-2", 7)
	trigger.FirstPromotion = true
	trigger.ChangedNodeIDs = []string{"seed.svc.orders"}

	require.NoError(t, w.reconciler(false).AfterPromotion(context.Background(), w.newUoW(), trigger))

	assert.Len(t, w.mats.built, 2)
	assert.Empty(t, w.runs.saved)
	assert.Empty(t, w.outbox.appended)
}

// During maintenance no run is created, by a promotion or by the loop; the
// first promotion's adoption is still recorded, since it starts no work.
func TestReconciler_MaintenanceCreatesNoRun(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"), seedNode("users", "h-users-1"))
	r := w.reconciler(true)
	trigger := moved("rel-2", 7)
	trigger.FirstPromotion = true
	trigger.ChangedNodeIDs = []string{"seed.svc.orders"}

	require.NoError(t, r.AfterPromotion(context.Background(), w.newUoW(), trigger))
	require.NoError(t, r.RunOnce(context.Background()))

	require.Len(t, w.mats.built, 1)
	assert.Equal(t, topology.NodeKey("raw.users"), w.mats.built[0].Key)
	assert.Empty(t, w.runs.saved)
	assert.Empty(t, w.mats.attempts)
	assert.Len(t, w.uows, 1, "RunOnce opens no unit of work during maintenance")
}

// The n-th run of a release takes the id numbered one past the runs that
// already name it.
func TestPass_NumbersTheRunAfterTheReleasesRuns(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"))
	w.runs.count = 2

	require.NoError(t, w.reconciler(false).AfterPromotion(context.Background(), w.newUoW(), moved("rel-2", 7)))

	require.Len(t, w.runs.saved, 1)
	assert.Equal(t, seeds.RunID("rel-2", 3), w.runs.saved[0].ScheduleID())
}

// A run that holds the next id without naming the release moves the pass on to
// the following number.
func TestPass_SkipsARunIDAlreadyTaken(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"))
	w.runs.taken[seeds.RunID("rel-2", 1)] = true

	require.NoError(t, w.reconciler(false).AfterPromotion(context.Background(), w.newUoW(), moved("rel-2", 7)))

	require.Len(t, w.runs.saved, 1)
	assert.Equal(t, seeds.RunID("rel-2", 2), w.runs.saved[0].ScheduleID())
	assert.Equal(t, seeds.RunID("rel-2", 2), w.mats.attempts[0].runID)
}

// A seed whose build is still in flight waits, and a seed whose build of the
// live content failed is not retried.
func TestPass_WaitsForAnInFlightBuildAndDoesNotRetryAFailedOne(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"), seedNode("users", "h-users-2"))
	w.mats.rows["raw.orders"] = seeds.Materialization{Key: "raw.orders", ContentHash: "h-orders-1",
		AttemptContentHash: "h-orders-2", AttemptRunID: uuid.New(), AttemptState: seeds.AttemptInFlight}
	w.mats.rows["raw.users"] = seeds.Materialization{Key: "raw.users", ContentHash: "h-users-1",
		AttemptContentHash: "h-users-2", AttemptRunID: uuid.New(), AttemptState: seeds.AttemptFailed}

	require.NoError(t, w.reconciler(false).AfterPromotion(context.Background(), w.newUoW(), moved("rel-2", 7)))

	assert.Empty(t, w.runs.saved)
	assert.Empty(t, w.outbox.appended)
}

// RunOnce takes the topology lock in a transaction of its own, runs the pass
// and commits; its trigger names no message.
func TestRunOnce_LocksPassesAndCommits(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"))

	require.NoError(t, w.reconciler(false).RunOnce(context.Background()))

	require.Len(t, w.uows, 1)
	assert.Equal(t, 1, w.uows[0].BeginCalled)
	assert.Equal(t, 1, w.uows[0].CommitCalled)
	assert.Zero(t, w.uows[0].RollbackCalled)
	assert.Equal(t, "lock", w.topo.calls[0], "the lock comes before any read")
	require.Len(t, w.runs.saved, 1)
	assert.Equal(t, []uuid.UUID{uuid.Nil}, w.outbox.msgProcIDs)
}

// A second pass while the first run is still in flight creates nothing.
func TestRunOnce_SecondPassWhileTheRunIsInFlightCreatesNothing(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"))
	r := w.reconciler(false)

	require.NoError(t, r.RunOnce(context.Background()))
	require.NoError(t, r.RunOnce(context.Background()))

	assert.Len(t, w.runs.saved, 1)
	assert.Len(t, w.outbox.appended, 1)
}

// A failed pass rolls its transaction back and reports the error.
func TestRunOnce_RollsBackAFailedPass(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"))
	boom := errors.New("connection reset by peer")
	w.runs.saveErr = boom

	err := w.reconciler(false).RunOnce(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
	assert.Zero(t, w.uows[0].CommitCalled)
	assert.Equal(t, 1, w.uows[0].RollbackCalled)
}

// With no release live, a pass reads nothing beyond the pointer.
func TestRunOnce_NothingLiveReadsNothingElse(t *testing.T) {
	w := newWorld(topology.Pointer{})

	require.NoError(t, w.reconciler(false).RunOnce(context.Background()))

	assert.Equal(t, []string{"lock", "live"}, w.topo.calls)
}
