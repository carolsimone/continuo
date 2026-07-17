package pool_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/pool"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
	"github.com/carolsimone/continuo/executor-controller/service/routing"
	"github.com/carolsimone/continuo/executor-controller/service/workerapi"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- fakes ----------------------------------------------------------------

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

// fakePools is an in-memory WorkerPoolRepository. It keeps every value handed to
// Add and Save, not just the last state, so a test can assert on what was
// actually written rather than on what survived being written twice.
type fakePools struct {
	registered   map[string]model.WorkerPool
	unregistered []model.PoolIdentity
	addErr       error
	// written records every pool value the reconciler passed to Add or Save.
	written []model.WorkerPool
}

func newFakePools() *fakePools {
	return &fakePools{registered: map[string]model.WorkerPool{}}
}

func (f *fakePools) Get(_ context.Context, key string) (*model.WorkerPool, error) {
	p, ok := f.registered[key]
	if !ok {
		return nil, nil
	}
	return &p, nil
}

func (f *fakePools) Add(_ context.Context, p model.WorkerPool) error {
	if f.addErr != nil {
		return f.addErr
	}
	f.written = append(f.written, p)
	f.registered[p.PoolKey] = p
	return nil
}

func (f *fakePools) Save(_ context.Context, p model.WorkerPool) error {
	if _, ok := f.registered[p.PoolKey]; !ok {
		return errors.New("not registered")
	}
	f.written = append(f.written, p)
	f.registered[p.PoolKey] = p
	return nil
}

func (f *fakePools) SaveInitializationError(_ context.Context, key, e string, at time.Time) error {
	p, ok := f.registered[key]
	if !ok {
		return errors.New("not registered")
	}
	p.InitializationError, p.UpdatedAt = e, at
	f.registered[key] = p
	return nil
}

func (f *fakePools) List(_ context.Context) ([]model.WorkerPool, error) {
	out := make([]model.WorkerPool, 0, len(f.registered))
	for _, p := range f.registered {
		out = append(out, p)
	}
	// Ordered by key, as the real repository is.
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j].PoolKey < out[i].PoolKey {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out, nil
}

func (f *fakePools) ListUnregistered(_ context.Context) ([]model.PoolIdentity, error) {
	return f.unregistered, nil
}

// fakeDeployments answers only the reads and the demotion the reconciler makes.
// Every other method of the port is unreachable from the reconciler, and panics
// rather than quietly returning a zero that a test could mistake for an answer.
type fakeDeployments struct {
	demand      []model.PoolDemand
	activeSlots int
	demoted     map[string]int64
	demoteRows  int64
}

func newFakeDeployments() *fakeDeployments {
	return &fakeDeployments{demoted: map[string]int64{}}
}

func (f *fakeDeployments) ListPoolDemand(_ context.Context, _ time.Time) ([]model.PoolDemand, error) {
	return f.demand, nil
}
func (f *fakeDeployments) ActiveSlotCount(_ context.Context) (int, error) { return f.activeSlots, nil }
func (f *fakeDeployments) DemotePendingPoolToJobs(_ context.Context, key string, _ time.Time) (int64, error) {
	f.demoted[key] += f.demoteRows
	return f.demoteRows, nil
}

func (f *fakeDeployments) Add(context.Context, *model.Deployment) error { panic("unused") }
func (f *fakeDeployments) GetDueJobs(context.Context, int) ([]*model.Deployment, error) {
	panic("unused")
}
func (f *fakeDeployments) Save(context.Context, *model.Deployment) error { panic("unused") }
func (f *fakeDeployments) GetByID(context.Context, uuid.UUID) (*model.Deployment, error) {
	panic("unused")
}
func (f *fakeDeployments) GetByIDForUpdate(context.Context, uuid.UUID) (*model.Deployment, error) {
	panic("unused")
}
func (f *fakeDeployments) GetNonTerminalByScheduleForUpdate(context.Context, uuid.UUID) ([]*model.Deployment, error) {
	panic("unused")
}
func (f *fakeDeployments) GetByReleaseNode(context.Context, string, string, model.Mode) (*model.Deployment, error) {
	panic("unused")
}
func (f *fakeDeployments) PendingValidationCount(context.Context, string, model.Mode) (int, error) {
	panic("unused")
}
func (f *fakeDeployments) ListValidationResults(context.Context, string, model.Mode) ([]*model.Deployment, error) {
	panic("unused")
}
func (f *fakeDeployments) ListValidationByRelease(context.Context, string, model.Mode) ([]*model.Deployment, error) {
	panic("unused")
}
func (f *fakeDeployments) LockCapacity(context.Context) error { panic("unused") }
func (f *fakeDeployments) ReleaseSlot(context.Context, uuid.UUID, time.Time) (bool, error) {
	panic("unused")
}
func (f *fakeDeployments) GetDueWorkerForPool(context.Context, string) (*model.Deployment, error) {
	panic("unused")
}
func (f *fakeDeployments) GetExpiredLeaseForUpdate(context.Context, time.Time) (*model.Deployment, error) {
	panic("unused")
}
func (f *fakeDeployments) GetStaleDispatchingForUpdate(context.Context, time.Time) (*model.Deployment, error) {
	panic("unused")
}

// fakeRuntime records what the reconciler asked the cluster for.
type fakeRuntime struct {
	ensured map[string]ports.WorkerPoolSpec
	calls   []string
	status  map[string]ports.PoolStatus
	deleted []string
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{ensured: map[string]ports.WorkerPoolSpec{}, status: map[string]ports.PoolStatus{}}
}

func (f *fakeRuntime) Ensure(_ context.Context, spec ports.WorkerPoolSpec) error {
	f.ensured[spec.PoolKey] = spec
	f.calls = append(f.calls, spec.PoolKey)
	// A pool that is ensured now has its Secret, as the real runtime would.
	s := f.status[spec.PoolKey]
	if spec.Credential != "" {
		s.SecretExists = true
	}
	s.DesiredReplicas = int(spec.DesiredReplicas)
	f.status[spec.PoolKey] = s
	return nil
}

func (f *fakeRuntime) Status(_ context.Context, key string) (ports.PoolStatus, bool, error) {
	s, ok := f.status[key]
	return s, ok, nil
}

func (f *fakeRuntime) DeletePod(_ context.Context, name, _ string) error {
	f.deleted = append(f.deleted, name)
	return nil
}

// ---- harness --------------------------------------------------------------

const (
	finance     = "finance"
	financeSHA  = "man-1"
	financeTag  = "sha-abc"
	idleTimeout = time.Minute
)

var financePool = pkgmodel.WorkerPoolKey(finance, financeTag, financeSHA)

func financeRef() pkgmodel.RuntimeManifestRef {
	return pkgmodel.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://continuo/finance/partial_parse.msgpack",
		RuntimeManifestSHA256:             financeSHA,
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: "ctx-1",
	}
}

type harness struct {
	pools   *fakePools
	deps    *fakeDeployments
	runtime *fakeRuntime
	clock   *fakeClock
	logs    *strings.Builder
	rec     *pool.Reconciler
}

// newHarness wires a reconciler over fakes, with workers the default mode.
func newHarness(t *testing.T, overrides map[string]model.ExecutionMode) *harness {
	t.Helper()
	h := &harness{
		pools:   newFakePools(),
		deps:    newFakeDeployments(),
		runtime: newFakeRuntime(),
		clock:   &fakeClock{now: time.Unix(10_000, 0).UTC()},
		logs:    &strings.Builder{},
	}
	h.rec = pool.NewReconciler(pool.Deps{
		Pools:       h.pools,
		Deployments: h.deps,
		Runtime:     h.runtime,
		Policy:      routing.NewPolicy(model.ExecutionModeWorkers, overrides),
		ControllerContext: func(service string) (string, error) {
			return `{"service":"` + service + `"}`, nil
		},
		Clock:  h.clock,
		Logger: slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}, pool.Config{
		MaxConcurrentExecutions: 10,
		IdleTimeout:             idleTimeout,
	})
	return h
}

// wanted makes the reconciler discover a pool from waiting work.
func (h *harness) wanted(key, service, tag string, ref pkgmodel.RuntimeManifestRef) {
	h.pools.unregistered = append(h.pools.unregistered, model.PoolIdentity{
		PoolKey: key, ServiceName: service, ImageTag: tag, RuntimeManifest: ref,
	})
}

// registered puts a pool in the repository as an earlier tick would have.
func (h *harness) registered(key, service string, replicas int, activity time.Time) model.WorkerPool {
	p := model.WorkerPool{
		PoolKey: key, ServiceName: service, ImageTag: financeTag,
		RuntimeManifest:  financeRef(),
		CredentialSHA256: workerapi.HashCredential("already-minted"),
		DesiredReplicas:  replicas, LastActivityAt: activity,
		CreatedAt: activity, UpdatedAt: activity,
	}
	h.pools.registered[key] = p
	h.runtime.status[key] = ports.PoolStatus{DesiredReplicas: replicas, SecretExists: true}
	return p
}

// ---- tests ----------------------------------------------------------------

// TestReconcileRegistersAPoolItsWaitingWorkNeeds proves a pool comes into being
// from the work routed to it: nothing else declares pools.
func TestReconcileRegistersAPoolItsWaitingWorkNeeds(t *testing.T) {
	h := newHarness(t, nil)
	h.wanted(financePool, finance, financeTag, financeRef())
	h.deps.demand = []model.PoolDemand{{
		PoolKey: financePool, ServiceName: finance, ImageTag: financeTag,
		RuntimeManifest: financeRef(), Pending: 3, OldestReadyAt: h.clock.now,
	}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	got, err := h.pools.Get(context.Background(), financePool)
	require.NoError(t, err)
	require.NotNil(t, got, "the pool is registered")
	assert.Equal(t, finance, got.ServiceName)
	assert.Equal(t, financeRef(), got.RuntimeManifest)
	assert.NotEmpty(t, got.CredentialSHA256, "the pool has a credential")

	spec, ok := h.runtime.ensured[financePool]
	require.True(t, ok, "the pool is reconciled into the cluster")
	assert.EqualValues(t, 3, spec.DesiredReplicas, "it is sized for its backlog")
}

// TestReconcileStoresOnlyTheCredentialsDigest is the credential invariant: the
// raw value reaches the pool's Secret and nothing else. A repository read must
// never yield something that authenticates.
func TestReconcileStoresOnlyTheCredentialsDigest(t *testing.T) {
	h := newHarness(t, nil)
	h.wanted(financePool, finance, financeTag, financeRef())
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool, Pending: 1, OldestReadyAt: h.clock.now}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	raw := h.runtime.ensured[financePool].Credential
	require.NotEmpty(t, raw, "the Secret is given the raw credential")

	// EVERY value handed to the repository is checked, not just the one that
	// survived: a write that carried the raw credential and was later corrected
	// still put it in the database.
	require.NotEmpty(t, h.pools.written)
	for i, w := range h.pools.written {
		assert.NotEqual(t, raw, w.CredentialSHA256,
			"write %d handed the repository the raw credential", i)
		assert.Equal(t, workerapi.HashCredential(raw), w.CredentialSHA256,
			"write %d must carry the credential's digest", i)
	}

	stored, err := h.pools.Get(context.Background(), financePool)
	require.NoError(t, err)
	assert.NotEqual(t, raw, stored.CredentialSHA256, "the row stores a digest, not the credential")
	assert.Equal(t, workerapi.HashCredential(raw), stored.CredentialSHA256)
	assert.True(t, workerapi.VerifyCredential(raw, stored.CredentialSHA256),
		"a worker holding the raw credential authenticates against the stored digest")
	assert.False(t, workerapi.VerifyCredential("guessed", stored.CredentialSHA256))
}

// TestReconcileNeverLogsTheRawCredential proves the one copy of the credential
// does not leak into the log on the way to the Secret.
func TestReconcileNeverLogsTheRawCredential(t *testing.T) {
	h := newHarness(t, nil)
	h.wanted(financePool, finance, financeTag, financeRef())
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool, Pending: 1, OldestReadyAt: h.clock.now}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	raw := h.runtime.ensured[financePool].Credential
	require.NotEmpty(t, raw)
	assert.NotContains(t, h.logs.String(), raw, "the raw credential must never be logged")
}

// TestReconcileMintsADistinctCredentialPerPool proves two pools cannot
// impersonate each other.
func TestReconcileMintsADistinctCredentialPerPool(t *testing.T) {
	h := newHarness(t, nil)
	other := pkgmodel.WorkerPoolKey("sales", financeTag, financeSHA)
	h.wanted(financePool, finance, financeTag, financeRef())
	h.wanted(other, "sales", financeTag, financeRef())
	h.deps.demand = []model.PoolDemand{
		{PoolKey: financePool, Pending: 1, OldestReadyAt: h.clock.now},
		{PoolKey: other, Pending: 1, OldestReadyAt: h.clock.now},
	}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	a := h.runtime.ensured[financePool].Credential
	b := h.runtime.ensured[other].Credential
	require.NotEmpty(t, a)
	require.NotEmpty(t, b)
	assert.NotEqual(t, a, b, "each pool gets its own credential")

	poolA, _ := h.pools.Get(context.Background(), financePool)
	assert.False(t, workerapi.VerifyCredential(b, poolA.CredentialSHA256),
		"one pool's credential does not authenticate another's")
}

// TestReconcileDoesNotRotateAPoolWhoseSecretIsIntact proves a routine tick
// leaves a live pool's credential alone. Rotating it would fence every worker
// currently authenticated with it.
func TestReconcileDoesNotRotateAPoolWhoseSecretIsIntact(t *testing.T) {
	h := newHarness(t, nil)
	before := h.registered(financePool, finance, 2, h.clock.now)
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool, Pending: 1, OldestReadyAt: h.clock.now}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	assert.Empty(t, h.runtime.ensured[financePool].Credential,
		"a reconcile with an intact Secret carries no credential")
	after, _ := h.pools.Get(context.Background(), financePool)
	assert.Equal(t, before.CredentialSHA256, after.CredentialSHA256, "the digest is unchanged")
}

// TestReconcileRotatesAPoolWhoseSecretWentMissing proves the pool is repaired in
// ONE attempt: the row's digest and the cluster's Secret are replaced together,
// so the pool is never left in a state where nothing can authenticate to it.
func TestReconcileRotatesAPoolWhoseSecretWentMissing(t *testing.T) {
	h := newHarness(t, nil)
	before := h.registered(financePool, finance, 2, h.clock.now)
	h.runtime.status[financePool] = ports.PoolStatus{DesiredReplicas: 2, SecretExists: false}
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool, Pending: 1, OldestReadyAt: h.clock.now}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	raw := h.runtime.ensured[financePool].Credential
	require.NotEmpty(t, raw, "the Secret is rewritten")
	after, _ := h.pools.Get(context.Background(), financePool)
	assert.NotEqual(t, before.CredentialSHA256, after.CredentialSHA256, "the digest rotated with it")
	assert.True(t, workerapi.VerifyCredential(raw, after.CredentialSHA256),
		"the new Secret's credential authenticates against the new digest")
}

// TestReconcileSharesCapacityOldestFirstAcrossPools proves the executor's one
// budget is split by how long each pool's work has waited.
func TestReconcileSharesCapacityOldestFirstAcrossPools(t *testing.T) {
	h := newHarness(t, nil)
	old := pkgmodel.WorkerPoolKey("old", financeTag, financeSHA)
	recent := pkgmodel.WorkerPoolKey("recent", financeTag, financeSHA)
	h.registered(old, "old", 0, h.clock.now)
	h.registered(recent, "recent", 0, h.clock.now)
	h.deps.activeSlots = 8 // of 10 — two free
	h.deps.demand = []model.PoolDemand{
		{PoolKey: recent, Pending: 5, OldestReadyAt: h.clock.now.Add(-time.Second)},
		{PoolKey: old, Pending: 5, OldestReadyAt: h.clock.now.Add(-time.Hour)},
	}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	assert.EqualValues(t, 2, h.runtime.ensured[old].DesiredReplicas, "the oldest work takes the free slots")
	assert.EqualValues(t, 0, h.runtime.ensured[recent].DesiredReplicas)
}

// TestReconcileNeverOversubscribesTheConfiguredLimit proves worker pods are
// bounded by the same budget Kubernetes Jobs draw on, so turning workers on
// cannot double the load on the warehouse.
func TestReconcileNeverOversubscribesTheConfiguredLimit(t *testing.T) {
	h := newHarness(t, nil)
	h.registered(financePool, finance, 0, h.clock.now)
	h.deps.activeSlots = 10 // the whole budget is held by Jobs-mode work
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool, Pending: 50, OldestReadyAt: h.clock.now}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	assert.EqualValues(t, 0, h.runtime.ensured[financePool].DesiredReplicas,
		"no slot is free, so no worker starts")
}

// TestReconcileKeepsABusyPoolWhileItsServiceIsTurnedBackToJobs proves a rollback
// does not strand work: pending tasks move to the Jobs path, but the pods
// holding leases are left running until their tasks settle.
func TestReconcileKeepsABusyPoolWhileItsServiceIsTurnedBackToJobs(t *testing.T) {
	h := newHarness(t, map[string]model.ExecutionMode{finance: model.ExecutionModeJobs})
	h.registered(financePool, finance, 3, h.clock.now)
	h.deps.demoteRows = 4
	h.deps.demand = []model.PoolDemand{{
		PoolKey: financePool, Pending: 4, ActiveLeases: 2, OldestReadyAt: h.clock.now,
	}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	assert.EqualValues(t, 4, h.deps.demoted[financePool], "the pending work moved to the Jobs path")
	assert.EqualValues(t, 3, h.runtime.ensured[financePool].DesiredReplicas,
		"the pods holding leases are not taken away")
}

// TestReconcileGivesNoNewCapacityToADemotedPool proves a rolled-back service
// stops being scaled up for a backlog it is no longer allowed to run.
func TestReconcileGivesNoNewCapacityToADemotedPool(t *testing.T) {
	h := newHarness(t, map[string]model.ExecutionMode{finance: model.ExecutionModeJobs})
	h.registered(financePool, finance, 0, h.clock.now.Add(-time.Hour))
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool, Pending: 9, OldestReadyAt: h.clock.now}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	assert.EqualValues(t, 0, h.runtime.ensured[financePool].DesiredReplicas,
		"a demoted pool is never scaled up for work it will not run")
}

// TestReconcileDoesNotDemoteAServiceStillOnWorkers is the guard on the rollback
// path: converting a live pool's work to Jobs would run every task twice.
func TestReconcileDoesNotDemoteAServiceStillOnWorkers(t *testing.T) {
	h := newHarness(t, nil)
	h.registered(financePool, finance, 1, h.clock.now)
	h.deps.demoteRows = 4
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool, Pending: 4, OldestReadyAt: h.clock.now}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	assert.Empty(t, h.deps.demoted, "a service on workers is never demoted")
}

// TestReconcileWithdrawsCapacityFromAFailedPoolButKeepsOneDiagnostic proves a
// pool whose workers cannot hydrate stops consuming the cluster while staying
// inspectable.
func TestReconcileWithdrawsCapacityFromAFailedPoolButKeepsOneDiagnostic(t *testing.T) {
	h := newHarness(t, nil)
	p := h.registered(financePool, finance, 5, h.clock.now)
	p.RecordInitializationFailure("runtime_manifest_rejected", "digest mismatch", h.clock.now)
	h.pools.registered[financePool] = p
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool, Pending: 9, OldestReadyAt: h.clock.now}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	assert.EqualValues(t, 1, h.runtime.ensured[financePool].DesiredReplicas,
		"a pool that cannot initialize runs one diagnostic pod")
}

// TestReconcileDoesNotLetAFailedPoolStarveAHealthyOne proves the executor's
// budget is not spent on a pool that cannot execute anything. A failed pool with
// a large backlog would otherwise consume the allocation and leave a healthy
// pool at zero.
func TestReconcileDoesNotLetAFailedPoolStarveAHealthyOne(t *testing.T) {
	h := newHarness(t, nil)
	healthy := pkgmodel.WorkerPoolKey("healthy", financeTag, financeSHA)
	// A pool only learns it cannot initialize by running a pod that says so, so a
	// failed pool always has pods; this one is being wound down to its diagnostic.
	broken := h.registered(financePool, finance, 3, h.clock.now)
	broken.RecordInitializationFailure("runtime_manifest_rejected", "digest mismatch", h.clock.now)
	h.pools.registered[financePool] = broken
	h.registered(healthy, "healthy", 0, h.clock.now)

	h.deps.activeSlots = 8 // two free slots
	h.deps.demand = []model.PoolDemand{
		// The broken pool's work is older, so it would win the allocation.
		{PoolKey: financePool, Pending: 9, OldestReadyAt: h.clock.now.Add(-time.Hour)},
		{PoolKey: healthy, Pending: 2, OldestReadyAt: h.clock.now},
	}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	assert.EqualValues(t, 2, h.runtime.ensured[healthy].DesiredReplicas,
		"the healthy pool gets the free capacity")
	assert.EqualValues(t, 1, h.runtime.ensured[financePool].DesiredReplicas,
		"the broken pool is capped at its diagnostic pod")
}

// TestReconcileRetiresAnIdlePool proves an unused pool eventually costs nothing.
func TestReconcileRetiresAnIdlePool(t *testing.T) {
	h := newHarness(t, nil)
	h.registered(financePool, finance, 4, h.clock.now.Add(-2*idleTimeout))
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	assert.EqualValues(t, 0, h.runtime.ensured[financePool].DesiredReplicas)
}

// TestReconcileKeepsAnActivePoolsIdleClockMoving proves work resets the idle
// clock. Without this a pool that has been busy for longer than the timeout
// would be retired the moment its last task finished, discarding warm pods that
// a steady stream of work will need again immediately.
func TestReconcileKeepsAnActivePoolsIdleClockMoving(t *testing.T) {
	h := newHarness(t, nil)
	h.registered(financePool, finance, 2, h.clock.now.Add(-2*idleTimeout))
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool, ActiveLeases: 2, OldestReadyAt: h.clock.now}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	after, _ := h.pools.Get(context.Background(), financePool)
	assert.Equal(t, h.clock.now, after.LastActivityAt, "a pool doing work is not idle")
}

// TestReconcileLeavesAQuietPoolsIdleClockAlone proves the clock is only reset by
// actual work; a tick observing nothing must not keep an idle pool alive forever.
func TestReconcileLeavesAQuietPoolsIdleClockAlone(t *testing.T) {
	h := newHarness(t, nil)
	was := h.clock.now.Add(-30 * time.Second)
	h.registered(financePool, finance, 2, was)
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	after, _ := h.pools.Get(context.Background(), financePool)
	assert.Equal(t, was, after.LastActivityAt, "a tick over a quiet pool does not reset its idle clock")
}

// TestReconcilePersistsTheReplicaCountItAskedFor keeps the row an operator reads
// agreeing with the cluster.
func TestReconcilePersistsTheReplicaCountItAskedFor(t *testing.T) {
	h := newHarness(t, nil)
	h.registered(financePool, finance, 0, h.clock.now)
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool, Pending: 3, OldestReadyAt: h.clock.now}}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	after, _ := h.pools.Get(context.Background(), financePool)
	assert.Equal(t, 3, after.DesiredReplicas)
	assert.EqualValues(t, 3, h.runtime.ensured[financePool].DesiredReplicas)
}

// TestReconcileCarriesEachPoolsOwnRuntimeManifest proves two pools are never
// given each other's artifact — the binding the worker checks its download
// against.
func TestReconcileCarriesEachPoolsOwnRuntimeManifest(t *testing.T) {
	h := newHarness(t, nil)
	otherRef := financeRef()
	otherRef.RuntimeManifestSHA256 = "man-2"
	other := pkgmodel.WorkerPoolKey(finance, financeTag, "man-2")

	h.wanted(financePool, finance, financeTag, financeRef())
	h.wanted(other, finance, financeTag, otherRef)
	h.deps.demand = []model.PoolDemand{
		{PoolKey: financePool, Pending: 1, OldestReadyAt: h.clock.now},
		{PoolKey: other, Pending: 1, OldestReadyAt: h.clock.now},
	}

	require.NoError(t, h.rec.Reconcile(context.Background()))

	assert.Equal(t, financeSHA, h.runtime.ensured[financePool].RuntimeManifest.RuntimeManifestSHA256)
	assert.Equal(t, "man-2", h.runtime.ensured[other].RuntimeManifest.RuntimeManifestSHA256)
	assert.Equal(t, `{"service":"finance"}`, h.runtime.ensured[financePool].ControllerContextJSON)
}

// TestReconcileIsIdempotent proves a second tick over an unchanged world does
// not churn the pool: the reconciler runs on a timer, so a tick that changed
// something every time would restart pods forever.
func TestReconcileIsIdempotent(t *testing.T) {
	h := newHarness(t, nil)
	h.wanted(financePool, finance, financeTag, financeRef())
	h.deps.demand = []model.PoolDemand{{PoolKey: financePool, Pending: 2, OldestReadyAt: h.clock.now}}

	require.NoError(t, h.rec.Reconcile(context.Background()))
	first := h.runtime.ensured[financePool]
	h.pools.unregistered = nil // the pool is registered now

	require.NoError(t, h.rec.Reconcile(context.Background()))
	second := h.runtime.ensured[financePool]

	assert.Equal(t, first.DesiredReplicas, second.DesiredReplicas)
	assert.Empty(t, second.Credential, "the second tick does not rotate the credential")
}

// TestReconcileReportsAFailureToRegisterAPool proves a broken tick is reported
// rather than swallowed into a silent no-op.
func TestReconcileReportsAFailureToRegisterAPool(t *testing.T) {
	h := newHarness(t, nil)
	h.pools.addErr = errors.New("database is down")
	h.wanted(financePool, finance, financeTag, financeRef())

	err := h.rec.Reconcile(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "database is down")
	assert.Empty(t, h.runtime.ensured, "no pool is reconciled into the cluster")
}

// TestReconcileWithNoPoolsDoesNothing is the default posture: with no service on
// workers there is no pool, and a tick must be a harmless no-op.
func TestReconcileWithNoPoolsDoesNothing(t *testing.T) {
	h := newHarness(t, nil)

	require.NoError(t, h.rec.Reconcile(context.Background()))

	assert.Empty(t, h.runtime.ensured, "nothing is created")
	assert.Empty(t, h.runtime.calls)
}
