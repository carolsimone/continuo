package seedreconcile_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/seeds"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/carolsimone/continuo/run-controller/service/ports"
	"github.com/carolsimone/continuo/run-controller/service/seedreconcile"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testNow = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

type fixedClock struct{}

func (fixedClock) Now() time.Time { return testNow }

// fakeTopology serves each tenant's live pointer and the releases' nodes, and
// records the calls it receives.
type fakeTopology struct {
	live  map[string]topology.Pointer // by tenant
	nodes map[string][]topology.Node  // by release id
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

func (f *fakeTopology) LivePointer(_ context.Context, tenantID string) (topology.Pointer, error) {
	f.calls = append(f.calls, "live")
	return f.live[tenantID], nil
}

// LivePointers lists every tenant's live pointer in tenant order.
func (f *fakeTopology) LivePointers(context.Context) ([]topology.Pointer, error) {
	f.calls = append(f.calls, "pointers")
	tenants := make([]string, 0, len(f.live))
	for tenant, p := range f.live {
		if !p.IsZero() {
			tenants = append(tenants, tenant)
		}
	}
	slices.Sort(tenants)
	out := make([]topology.Pointer, 0, len(tenants))
	for _, tenant := range tenants {
		out = append(out, f.live[tenant])
	}
	return out, nil
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
	tenant string
	key    topology.NodeKey
	hash   string
	runID  uuid.UUID
}

// fakeMaterializations keeps each tenant's node_materializations in memory. A
// build request leaves its seed's attempt in flight, as a freshly created run
// is.
type fakeMaterializations struct {
	rows     map[string]map[topology.NodeKey]seeds.Materialization // by tenant
	built    []seeds.Built
	attempts []attempt
}

var _ repository.MaterializationRepository = (*fakeMaterializations)(nil)

// of returns the rows of tenantID, which it creates on first use.
func (f *fakeMaterializations) of(tenantID string) map[topology.NodeKey]seeds.Materialization {
	if f.rows[tenantID] == nil {
		f.rows[tenantID] = map[topology.NodeKey]seeds.Materialization{}
	}
	return f.rows[tenantID]
}

func (f *fakeMaterializations) List(_ context.Context, tenantID string) (map[topology.NodeKey]seeds.Materialization, error) {
	rows := f.of(tenantID)
	out := make(map[topology.NodeKey]seeds.Materialization, len(rows))
	for k, v := range rows {
		out[k] = v
	}
	return out, nil
}

func (f *fakeMaterializations) RecordAttempt(_ context.Context, tenantID string, key topology.NodeKey, hash string, runID uuid.UUID, _ time.Time) error {
	f.attempts = append(f.attempts, attempt{tenant: tenantID, key: key, hash: hash, runID: runID})
	rows := f.of(tenantID)
	m := rows[key]
	m.Key, m.AttemptContentHash, m.AttemptRunID, m.AttemptState = key, hash, runID, seeds.AttemptInFlight
	rows[key] = m
	return nil
}

func (f *fakeMaterializations) RecordBuilt(_ context.Context, tenantID string, b seeds.Built) error {
	f.built = append(f.built, b)
	rows := f.of(tenantID)
	m := rows[b.Key]
	m.Key, m.ContentHash = b.Key, b.ContentHash
	rows[b.Key] = m
	return nil
}

func (f *fakeMaterializations) DeleteAbsentFrom(context.Context, string, string) (int64, error) {
	return 0, nil
}

// fakeRuns persists runs in memory. taken holds the ids already persisted
// (including runs that name no release); counts holds, by tenant and release
// (countKey), what CountReleaseRuns answers, and grows with every run saved.
// saveErr, when set, fails every save of a run of the tenant failTenant names,
// or of every tenant when failTenant is empty.
type fakeRuns struct {
	taken      map[uuid.UUID]bool
	counts     map[string]int
	saved      []*run.Run
	saveErr    error
	failTenant string
	existsAsk  []uuid.UUID
}

// countKey names the runs of tenantID that name releaseID.
func countKey(tenantID, releaseID string) string { return tenantID + "/" + releaseID }

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
	if f.saveErr != nil && (f.failTenant == "" || f.failTenant == r.TenantID()) {
		return f.saveErr
	}
	if !r.Changes().IsCreated() {
		r.ResetChanges()
		return nil
	}
	if f.taken[r.RunID()] {
		return run.ErrRunAlreadyExists
	}
	f.taken[r.RunID()] = true
	f.saved = append(f.saved, r)
	f.counts[countKey(r.TenantID(), r.ReleaseID())]++
	r.ResetChanges()
	return nil
}

func (f *fakeRuns) RunExists(_ context.Context, _ string, id uuid.UUID) (bool, error) {
	f.existsAsk = append(f.existsAsk, id)
	return f.taken[id], nil
}

func (f *fakeRuns) ListUnplannedActive(context.Context) ([]repository.RunRef, error) {
	panic("ListUnplannedActive is not used here")
}

func (f *fakeRuns) CountReleaseRuns(_ context.Context, tenantID, releaseID string, _ run.Kind) (int, error) {
	return f.counts[countKey(tenantID, releaseID)], nil
}

func (f *fakeRuns) GetRun(context.Context, string, uuid.UUID) (*run.Run, error) {
	panic("GetRun is not used by the seed reconcile")
}

func (f *fakeRuns) LoadRunForUpdate(context.Context, string, uuid.UUID) (*run.Run, error) {
	panic("LoadRunForUpdate is not used by the seed reconcile")
}

func (f *fakeRuns) HasActiveRun(context.Context, string, string) (bool, error) {
	panic("HasActiveRun is not used by the seed reconcile")
}

func (f *fakeRuns) GetActiveRun(context.Context, string, string) (*run.Run, error) {
	panic("GetActiveRun is not used by the seed reconcile")
}

func (f *fakeRuns) GetLastRunPerSchedule(context.Context, string) (map[string]repository.LastRunSummary, error) {
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

// world is the tenants' state behind fake repositories; every unit of work it
// hands out shares them.
type world struct {
	topo   *fakeTopology
	mats   *fakeMaterializations
	runs   *fakeRuns
	tasks  *fakeTasks
	outbox *fakeOutbox
	uows   []*uow.FakeUnitOfWork
}

// newWorld makes live, holding nodes, the default tenant's live release; the
// zero live leaves no release live.
func newWorld(live topology.Pointer, nodes ...topology.Node) *world {
	w := &world{
		topo:   &fakeTopology{live: map[string]topology.Pointer{}, nodes: map[string][]topology.Node{}},
		mats:   &fakeMaterializations{rows: map[string]map[topology.NodeKey]seeds.Materialization{}},
		runs:   &fakeRuns{taken: map[uuid.UUID]bool{}, counts: map[string]int{}},
		tasks:  &fakeTasks{},
		outbox: &fakeOutbox{},
	}
	w.addTenant(pkgevents.DefaultTenantID, live, nodes...)
	return w
}

// addTenant makes live, holding nodes, tenantID's live release.
func (w *world) addTenant(tenantID string, live topology.Pointer, nodes ...topology.Node) {
	if !live.IsZero() {
		live.TenantID = tenantID
	}
	w.topo.live[tenantID] = live
	w.topo.nodes[live.ReleaseID] = nodes
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
	return seedreconcile.New(func() uow.UnitOfWork { return w.newUoW() }, maintenance, run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// loaded records table as last loaded at hash for the default tenant.
func (w *world) loaded(table, hash string) {
	key := topology.KeyOf("raw", table)
	w.mats.of(pkgevents.DefaultTenantID)[key] = seeds.Materialization{Key: key, ContentHash: hash}
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

	wantID := seeds.RunID(pkgevents.DefaultTenantID, "rel-2", 1)
	require.Len(t, w.runs.saved, 1)
	created := w.runs.saved[0]
	assert.Equal(t, wantID, created.RunID())
	assert.Equal(t, scheduleNameOf(wantID), created.ScheduleName())
	assert.Equal(t, run.KindPromoteSeed, created.Kind())
	assert.Equal(t, run.RunStatusRunning, created.Status())
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-2", PromotionSeq: 7}, created.Release())
	assert.NotNil(t, created.PlannedAt())
	require.Len(t, w.outbox.appended, 1)
	d, ok := w.outbox.appended[0].(run.TaskDispatched)
	require.True(t, ok, "%T", w.outbox.appended[0])
	assert.Equal(t, run.TaskIDFor("default", wantID, "raw.orders"), d.TaskID)
	assert.Equal(t, seedNode("orders", "h-orders-2"), d.Node)
	assert.Equal(t, scheduleNameOf(wantID), d.ScheduleName)
	assert.Equal(t, []uuid.UUID{trigger.MessageProcessingID}, w.outbox.msgProcIDs)
	assert.Equal(t, []attempt{{tenant: "default", key: "raw.orders", hash: "h-orders-2", runID: wantID}}, w.mats.attempts)
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

	assert.Equal(t, []uuid.UUID{seeds.RunID(pkgevents.DefaultTenantID, "rel-2", 1)}, w.runs.existsAsk)
	assert.Equal(t, []seeds.Built{{
		Key: "raw.users", ContentHash: "h-users-1", ImageTag: "svc:2", ReleaseID: "rel-2", At: testNow, Source: seeds.SourceAdopted,
	}}, w.mats.built)
	assert.Equal(t, []topology.NodeKey{"raw.orders"}, dispatchedKeys(w.outbox.appended))
}

// When the release's first promote-seed run already exists, every seed is
// adopted and nothing is built.
func TestAfterPromotion_FirstPromotionAdoptsEverySeedWhenItsFirstRunExists(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"), seedNode("users", "h-users-1"))
	w.runs.taken[seeds.RunID(pkgevents.DefaultTenantID, "rel-2", 1)] = true
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
	w.runs.counts[countKey("default", "rel-2")] = 2

	require.NoError(t, w.reconciler(false).AfterPromotion(context.Background(), w.newUoW(), moved("rel-2", 7)))

	require.Len(t, w.runs.saved, 1)
	assert.Equal(t, seeds.RunID(pkgevents.DefaultTenantID, "rel-2", 3), w.runs.saved[0].RunID())
}

// A run that holds the next id without naming the release moves the pass on to
// the following number.
func TestPass_SkipsARunIDAlreadyTaken(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"))
	w.runs.taken[seeds.RunID(pkgevents.DefaultTenantID, "rel-2", 1)] = true

	require.NoError(t, w.reconciler(false).AfterPromotion(context.Background(), w.newUoW(), moved("rel-2", 7)))

	require.Len(t, w.runs.saved, 1)
	assert.Equal(t, seeds.RunID(pkgevents.DefaultTenantID, "rel-2", 2), w.runs.saved[0].RunID())
	assert.Equal(t, seeds.RunID(pkgevents.DefaultTenantID, "rel-2", 2), w.mats.attempts[0].runID)
}

// A seed whose build is still in flight waits, and a seed whose build of the
// live content failed is not retried.
func TestPass_WaitsForAnInFlightBuildAndDoesNotRetryAFailedOne(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"), seedNode("users", "h-users-2"))
	rows := w.mats.of("default")
	rows["raw.orders"] = seeds.Materialization{Key: "raw.orders", ContentHash: "h-orders-1",
		AttemptContentHash: "h-orders-2", AttemptRunID: uuid.New(), AttemptState: seeds.AttemptInFlight}
	rows["raw.users"] = seeds.Materialization{Key: "raw.users", ContentHash: "h-users-1",
		AttemptContentHash: "h-users-2", AttemptRunID: uuid.New(), AttemptState: seeds.AttemptFailed}

	require.NoError(t, w.reconciler(false).AfterPromotion(context.Background(), w.newUoW(), moved("rel-2", 7)))

	assert.Empty(t, w.runs.saved)
	assert.Empty(t, w.outbox.appended)
}

// RunOnce lists the live releases, then takes the topology lock in a
// transaction of its own, runs the pass and commits; its trigger names no
// message.
func TestRunOnce_LocksPassesAndCommits(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"))

	require.NoError(t, w.reconciler(false).RunOnce(context.Background()))

	require.Len(t, w.uows, 2, "one unit of work lists the live releases, one runs the pass")
	assert.Zero(t, w.uows[0].BeginCalled, "the live releases are listed outside any transaction")
	assert.Equal(t, 1, w.uows[1].BeginCalled)
	assert.Equal(t, 1, w.uows[1].CommitCalled)
	assert.Zero(t, w.uows[1].RollbackCalled)
	assert.Equal(t, []string{"pointers", "lock", "live"}, w.topo.calls[:3], "the pass's lock comes before any of its reads")
	require.Len(t, w.runs.saved, 1)
	assert.Equal(t, []uuid.UUID{uuid.Nil}, w.outbox.msgProcIDs)
}

// RunOnce makes one pass per live release, each for the tenant whose release
// it is and in a transaction of its own under the topology lock: two tenants
// whose live releases each hold a seed whose content differs from what the
// tenant loaded get one promote-seed run each, of their own tenant, on their
// own release.
func TestSeedReconcile_PassesOncePerLiveRelease(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"))
	w.addTenant("acme", topology.Pointer{ReleaseID: "rel-acme", PromotionSeq: 3}, seedNode("orders", "h-orders-acme"))
	w.loaded("orders", "h-orders-1")

	require.NoError(t, w.reconciler(false).RunOnce(context.Background()))

	require.Len(t, w.runs.saved, 2, "one run per tenant")
	acme, def := w.runs.saved[0], w.runs.saved[1]
	assert.Equal(t, "acme", acme.TenantID(), "the passes run in tenant order")
	assert.Equal(t, seeds.RunID("acme", "rel-acme", 1), acme.RunID())
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-acme", PromotionSeq: 3}, acme.Release())
	assert.Equal(t, "default", def.TenantID())
	assert.Equal(t, seeds.RunID("default", "rel-2", 1), def.RunID())
	assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-2", PromotionSeq: 7}, def.Release())
	assert.Equal(t, []attempt{
		{tenant: "acme", key: "raw.orders", hash: "h-orders-acme", runID: acme.RunID()},
		{tenant: "default", key: "raw.orders", hash: "h-orders-2", runID: def.RunID()},
	}, w.mats.attempts, "each seed's build request is its own tenant's")
	assert.Equal(t, []topology.NodeKey{"raw.orders", "raw.orders"}, dispatchedKeys(w.outbox.appended))
	require.Len(t, w.uows, 3, "one unit of work lists the live releases, one runs each pass")
	for i, tenant := range []string{"acme", "default"} {
		u := w.uows[i+1]
		assert.Equal(t, 1, u.BeginCalled, tenant)
		assert.Equal(t, 1, u.CommitCalled, tenant)
		assert.Equal(t, []string{tenant}, u.TaskCollectionTenants, "the pass of %s starts its run under its tenant", tenant)
	}
	assert.Equal(t, []string{"pointers", "lock", "live", "nodes:rel-acme", "graph:rel-acme", "lock", "live", "nodes:rel-2", "graph:rel-2"},
		w.topo.calls, "each pass takes the lock and reads its tenant's live release under it")
}

// A pass that fails rolls back its own transaction only: the other tenants'
// passes still run and commit, and RunOnce returns the failure.
func TestRunOnce_AFailedPassDoesNotStopTheOtherTenants(t *testing.T) {
	w := newWorld(live7, seedNode("orders", "h-orders-2"))
	w.addTenant("acme", topology.Pointer{ReleaseID: "rel-acme", PromotionSeq: 3}, seedNode("orders", "h-orders-acme"))
	boom := errors.New("connection reset by peer")
	w.runs.saveErr, w.runs.failTenant = boom, "acme"

	err := w.reconciler(false).RunOnce(context.Background())

	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "acme")
	require.Len(t, w.uows, 3)
	assert.Equal(t, 1, w.uows[1].RollbackCalled, "the acme pass rolls back")
	assert.Zero(t, w.uows[1].CommitCalled)
	assert.Equal(t, 1, w.uows[2].CommitCalled, "the default tenant's pass still commits")
	require.Len(t, w.runs.saved, 1)
	assert.Equal(t, "default", w.runs.saved[0].TenantID())
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
	require.Len(t, w.uows, 2)
	assert.Zero(t, w.uows[1].CommitCalled)
	assert.Equal(t, 1, w.uows[1].RollbackCalled)
}

// With no release live, RunOnce reads nothing beyond the list of live
// releases and opens no transaction.
func TestRunOnce_NothingLiveReadsNothingElse(t *testing.T) {
	w := newWorld(topology.Pointer{})

	require.NoError(t, w.reconciler(false).RunOnce(context.Background()))

	assert.Equal(t, []string{"pointers"}, w.topo.calls)
	require.Len(t, w.uows, 1)
	assert.Zero(t, w.uows[0].BeginCalled)
}
