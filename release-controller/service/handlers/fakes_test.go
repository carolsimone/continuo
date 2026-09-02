package handlers_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/google/uuid"
)

// --- fakeClock ---

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time { return f.t }

var _ ports.Clock = (*fakeClock)(nil)

// --- spyTelemetry ---

// spyTelemetry embeds NoOpTelemetry and records ReleasePromoted calls so
// tests can assert the promoted span does not fire on a route that must not
// promote (e.g. a verification run stopping at Passed).
type spyTelemetry struct {
	ports.NoOpTelemetry
	releasePromotedCalls int
}

func (s *spyTelemetry) ReleasePromoted(_ context.Context, _ string, _ int) {
	s.releasePromotedCalls++
}

var _ ports.Telemetry = (*spyTelemetry)(nil)

// --- fakeStore ---

// fakeStore is the shared in-memory state backing every fakeUoW produced by
// the NewUoW factory. All fakeUoW instances created from the same store read
// and write through the same data, so handler tests can inspect accumulated
// state after multiple handler calls — each of which obtains its own UoW.
type fakeStore struct {
	mu            sync.Mutex
	releases      map[string]*pipeline.Run
	order         []string // insertion order, oldest first; drives NextQueued FIFO
	cp            *release.CurrentProd
	cpUpsertCalls int
	serviceProd   map[string]*release.ServiceProd
	entries       []*pkgoutbox.Entry
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		releases:    map[string]*pipeline.Run{},
		serviceProd: map[string]*release.ServiceProd{},
	}
}

// GetRelease returns the stored release by ID, or an error if not found.
// Convenience accessor for test assertions.
func (s *fakeStore) GetRelease(id string) (*pipeline.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.releases[id]
	if !ok {
		return nil, fmt.Errorf("release %s not found", id)
	}
	return r, nil
}

// SeedRelease installs a release directly, simulating a row already persisted
// by an earlier stage of the pipeline.
func (s *fakeStore) SeedRelease(r *pipeline.Run) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.releases[r.ID()]; !exists {
		s.order = append(s.order, r.ID())
	}
	s.releases[r.ID()] = r
}

// SeedCurrentProd installs a current-prod snapshot directly, simulating a
// prior promotion. Used by handler tests to exercise the content_hash diff.
func (s *fakeStore) SeedCurrentProd(cp *release.CurrentProd) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cp = cp
}

// GetCurrentProd returns the stored current-prod record, or a fresh one if
// none has been written yet.
func (s *fakeStore) GetCurrentProd() *release.CurrentProd {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cp == nil {
		return release.NewCurrentProd()
	}
	return s.cp
}

// SeedServiceProd installs a service_prod pointer directly so advance-queue
// tests can assert that other services' manifest keys are picked up.
func (s *fakeStore) SeedServiceProd(sp *release.ServiceProd) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.serviceProd[sp.ServiceName()] = sp
}

// GetServiceProd returns the service_prod pointer written for the given service
// name, or nil if no upsert has been recorded yet. Used by handler tests to
// assert the values written by promoteToProduction.
func (s *fakeStore) GetServiceProd(serviceName string) *release.ServiceProd {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.serviceProd[serviceName]
}

// CurrentProdUpsertCalls returns how many times CurrentProdRepo.Upsert has
// been called against this store, across every UoW instance backed by it.
// Used by tests to assert that a route which must never promote (e.g. a
// shadow release) never touches current_prod, rather than only inferring it
// from the stored value.
func (s *fakeStore) CurrentProdUpsertCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cpUpsertCalls
}

// OutboxEntries returns a snapshot of all outbox entries written so far.
func (s *fakeStore) OutboxEntries() []*pkgoutbox.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*pkgoutbox.Entry, len(s.entries))
	copy(out, s.entries)
	return out
}

// --- fakeRunRepo ---

type fakeRunRepo struct {
	store              *fakeStore
	lastCutoff         time.Time
	lastKeepReleaseIDs []string
	deletedCount       int
}

// Get mirrors the Postgres RunRepository contract: a missing run yields
// (nil, nil), not an error. Handlers must nil-check the result, so the fake must
// reproduce that contract or it would mask nil-deref bugs.
func (f *fakeRunRepo) Get(_ context.Context, id string) (*pipeline.Run, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	return f.store.releases[id], nil
}

// Load mirrors Get: the in-memory fake has no row-level locking to simulate,
// so it returns the same result as Get.
func (f *fakeRunRepo) Load(_ context.Context, id string) (*pipeline.Run, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	return f.store.releases[id], nil
}

func (f *fakeRunRepo) Save(_ context.Context, r *pipeline.Run) error {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if _, exists := f.store.releases[r.ID()]; !exists {
		f.store.order = append(f.store.order, r.ID())
	}
	f.store.releases[r.ID()] = r
	return nil
}

// NextQueued returns the oldest run in StatusReceived, or nil.
func (f *fakeRunRepo) NextQueued(_ context.Context) (*pipeline.Run, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	for _, id := range f.store.order {
		if f.store.releases[id].Status() == pipeline.StatusReceived {
			return f.store.releases[id], nil
		}
	}
	return nil, nil
}

// Active returns the single run in StatusCompiling, StatusParsing,
// StatusSeedBuilding, or StatusValidating, or nil. Mirrors the Postgres query
// that guards AdvanceQueue from launching a second concurrent run.
func (f *fakeRunRepo) Active(_ context.Context) (*pipeline.Run, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	for _, id := range f.store.order {
		s := f.store.releases[id].Status()
		if s == pipeline.StatusCompiling || s == pipeline.StatusParsing ||
			s == pipeline.StatusSeedBuilding || s == pipeline.StatusValidating {
			return f.store.releases[id], nil
		}
	}
	return nil, nil
}

// List returns an empty page; handler unit tests do not exercise the list path.
func (f *fakeRunRepo) List(_ context.Context, _ repository.ListFilter) ([]*pipeline.Run, *repository.ListCursor, error) {
	return nil, nil, nil
}

// DeleteFinishedBefore records the call parameters and returns the configured
// deletedCount so handler tests can assert on retention behaviour.
func (f *fakeRunRepo) DeleteFinishedBefore(_ context.Context, cutoff time.Time, keepReleaseIDs []string) (int, error) {
	f.lastCutoff = cutoff
	f.lastKeepReleaseIDs = keepReleaseIDs
	return f.deletedCount, nil
}

var _ repository.RunRepository = (*fakeRunRepo)(nil)

// --- fakeCurrentProdRepo ---

type fakeCurrentProdRepo struct {
	store *fakeStore
}

func (f *fakeCurrentProdRepo) Get(_ context.Context) (*release.CurrentProd, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if f.store.cp == nil {
		return release.NewCurrentProd(), nil
	}
	return f.store.cp, nil
}

func (f *fakeCurrentProdRepo) Upsert(_ context.Context, cp *release.CurrentProd) error {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	f.store.cp = cp
	f.store.cpUpsertCalls++
	return nil
}

var _ repository.CurrentProdRepository = (*fakeCurrentProdRepo)(nil)

// --- fakeServiceProdRepo ---

type fakeServiceProdRepo struct {
	store *fakeStore
}

func (f *fakeServiceProdRepo) List(_ context.Context) ([]*release.ServiceProd, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	out := make([]*release.ServiceProd, 0, len(f.store.serviceProd))
	for _, sp := range f.store.serviceProd {
		out = append(out, sp)
	}
	return out, nil
}

func (f *fakeServiceProdRepo) Get(_ context.Context, serviceName string) (*release.ServiceProd, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	return f.store.serviceProd[serviceName], nil
}

func (f *fakeServiceProdRepo) Upsert(_ context.Context, sp *release.ServiceProd) error {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	f.store.serviceProd[sp.ServiceName()] = sp
	return nil
}

var _ repository.ServiceProdRepository = (*fakeServiceProdRepo)(nil)

// --- fakeOutbox ---

// fakeOutbox records every pkgoutbox.Entry written by handlers. Tests inspect
// the recorded entries via the fakeStore.OutboxEntries() accessor. The
// consume-side methods (GetPendingBatch, MarkProcessed, MarkFailed,
// IncrementRetry) are no-ops because handler tests do not exercise the
// publisher path.
type fakeOutbox struct {
	store *fakeStore
}

func (f *fakeOutbox) Create(_ context.Context, entry *pkgoutbox.Entry) error {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	f.store.entries = append(f.store.entries, entry)
	return nil
}

func (f *fakeOutbox) GetPendingBatch(_ context.Context, _ int) ([]*pkgoutbox.Entry, error) {
	return nil, nil
}

func (f *fakeOutbox) MarkProcessed(_ context.Context, _ uuid.UUID) error        { return nil }
func (f *fakeOutbox) MarkProcessedBatch(_ context.Context, _ []uuid.UUID) error { return nil }
func (f *fakeOutbox) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (f *fakeOutbox) IncrementRetry(_ context.Context, _ uuid.UUID) error { return nil }

var _ pkgoutbox.Repository = (*fakeOutbox)(nil)

// --- fakeMessageProcessing ---

// fakeMessageProcessing is a no-op implementation of messageprocessing.Repository.
// Handler tests do not exercise the dedup path; an always-insert (new UUID,
// inserted=true) stance keeps handlers that call InsertIfNotExists happy.
type fakeMessageProcessing struct{}

func (fakeMessageProcessing) InsertIfNotExists(_ context.Context, _ *messageprocessing.MessageProcessing) (uuid.UUID, bool, error) {
	return uuid.New(), true, nil
}

func (fakeMessageProcessing) AlreadyProcessed(_ context.Context, _, _ string, _ *uuid.UUID) (bool, error) {
	return false, nil
}

func (fakeMessageProcessing) GetByMessageIDAndStream(_ context.Context, _, _ string) (*messageprocessing.MessageProcessing, error) {
	return nil, nil
}

func (fakeMessageProcessing) GetByID(_ context.Context, _ uuid.UUID) (*messageprocessing.MessageProcessing, error) {
	return nil, nil
}

func (fakeMessageProcessing) UpdateState(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func (fakeMessageProcessing) DeleteTerminalOlderThan(_ context.Context, _ time.Duration, _ int) (int64, error) {
	return 0, nil
}

var _ messageprocessing.Repository = (*fakeMessageProcessing)(nil)

// --- fakeProposals ---

// fakeProposals is a stub ports.ProposalReader. items is returned verbatim
// unless err is set, in which case err is returned instead — letting tests
// simulate the agent-remediation gRPC call being unavailable.
type fakeProposals struct {
	items []ports.ProposalSummary
	err   error
}

func (f *fakeProposals) ListProposalsForRelease(_ context.Context, _ string) ([]ports.ProposalSummary, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.items, nil
}

var _ ports.ProposalReader = (*fakeProposals)(nil)

// --- fakeUoW ---

// fakeUoW wraps the four fakes backed by a shared fakeStore. Begin/Commit/Rollback
// are no-ops so handler tests run without a database. Multiple fakeUoW instances
// created from the same store accumulate state together, matching how real
// handlers each obtain a fresh UoW from the factory while still seeing all
// writes from prior calls.
type fakeUoW struct {
	releases *fakeRunRepo
	cp       *fakeCurrentProdRepo
	sp       *fakeServiceProdRepo
	outbox   *fakeOutbox
	msgProc  fakeMessageProcessing
}

func newFakeUoW(store *fakeStore) *fakeUoW {
	return &fakeUoW{
		releases: &fakeRunRepo{store: store},
		cp:       &fakeCurrentProdRepo{store: store},
		sp:       &fakeServiceProdRepo{store: store},
		outbox:   &fakeOutbox{store: store},
	}
}

func (f *fakeUoW) RunRepo() repository.RunRepository                   { return f.releases }
func (f *fakeUoW) CurrentProdRepo() repository.CurrentProdRepository   { return f.cp }
func (f *fakeUoW) ServiceProdRepo() repository.ServiceProdRepository   { return f.sp }
func (f *fakeUoW) OutboxRepo() pkgoutbox.Repository                    { return f.outbox }
func (f *fakeUoW) MessageProcessingRepo() messageprocessing.Repository { return f.msgProc }
func (f *fakeUoW) Begin(_ context.Context) error                       { return nil }
func (f *fakeUoW) Commit() error                                       { return nil }
func (f *fakeUoW) Rollback() error                                     { return nil }

// LockReleaseQueue is a no-op in the fake: unit tests execute sequentially,
// so the tx-scoped advisory lock semantics are proven end-to-end against
// real Postgres in integration_test/concurrent_advance_no_duplicate_test.go.
func (f *fakeUoW) LockReleaseQueue(_ context.Context) error { return nil }

var _ uow.UnitOfWork = (*fakeUoW)(nil)

// --- helpers ---

// newDeps constructs a Deps with a fakeClock pinned to now and NoOpTelemetry.
// Returns both the Deps and the fakeStore so tests can inspect accumulated state.
// Each call to Deps.NewUoW() returns a fresh fakeUoW backed by the same store,
// mirroring how production code wraps a *sqlx.DB pool per handler invocation.
func newDeps(now time.Time) (*handlers.Deps, *fakeStore) {
	store := newFakeStore()
	return &handlers.Deps{
		NewUoW:    func() uow.UnitOfWork { return newFakeUoW(store) },
		Clock:     &fakeClock{t: now},
		Telemetry: ports.NoOpTelemetry{},
		Logger:    slog.Default(),
		Proposals: &fakeProposals{},
	}, store
}

// outboxEntries returns a snapshot of the pkgoutbox.Entry slice recorded in the
// store. Use this in handler tests to inspect outbox state after handler calls.
func outboxEntries(store *fakeStore) []*pkgoutbox.Entry {
	return store.OutboxEntries()
}
