package handlers_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
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

// --- fakeReleaseRepo ---

type fakeReleaseRepo struct {
	mu       sync.Mutex
	releases map[string]*release.Release
	order    []string // insertion order, oldest first; drives NextQueuedRelease FIFO
}

func newFakeReleaseRepo() *fakeReleaseRepo {
	return &fakeReleaseRepo{releases: map[string]*release.Release{}}
}

func (f *fakeReleaseRepo) Get(_ context.Context, id string) (*release.Release, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.releases[id]
	if !ok {
		return nil, fmt.Errorf("release %s not found", id)
	}
	return r, nil
}

func (f *fakeReleaseRepo) Save(_ context.Context, r *release.Release) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.releases[r.ID()]; !exists {
		f.order = append(f.order, r.ID())
	}
	f.releases[r.ID()] = r
	return nil
}

// NextQueuedRelease returns the oldest release in StatusReceived, or nil.
func (f *fakeReleaseRepo) NextQueuedRelease(_ context.Context) (*release.Release, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range f.order {
		if f.releases[id].Status() == release.StatusReceived {
			return f.releases[id], nil
		}
	}
	return nil, nil
}

// ActiveRelease returns the single release in StatusParsing or StatusValidating, or nil.
func (f *fakeReleaseRepo) ActiveRelease(_ context.Context) (*release.Release, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range f.order {
		s := f.releases[id].Status()
		if s == release.StatusParsing || s == release.StatusValidating {
			return f.releases[id], nil
		}
	}
	return nil, nil
}

var _ repository.ReleaseRepository = (*fakeReleaseRepo)(nil)

// --- fakeCurrentProdRepo ---

type fakeCurrentProdRepo struct {
	mu sync.Mutex
	cp *release.CurrentProd
}

func (f *fakeCurrentProdRepo) Get(_ context.Context) (*release.CurrentProd, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cp == nil {
		return release.NewCurrentProd(), nil
	}
	return f.cp, nil
}

func (f *fakeCurrentProdRepo) Upsert(_ context.Context, cp *release.CurrentProd) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cp = cp
	return nil
}

var _ repository.CurrentProdRepository = (*fakeCurrentProdRepo)(nil)

// --- fakeOutbox ---

// fakeOutbox records every pkgoutbox.Entry written by handlers. Tests inspect
// the recorded entries via outboxEntries(u). The consume-side methods
// (GetPendingBatch, MarkProcessed, MarkFailed, IncrementRetry) are no-ops
// because handler tests do not exercise the publisher path.
type fakeOutbox struct {
	mu      sync.Mutex
	entries []*pkgoutbox.Entry
}

func (f *fakeOutbox) Create(_ context.Context, entry *pkgoutbox.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, entry)
	return nil
}

func (f *fakeOutbox) GetPendingBatch(_ context.Context, _ int) ([]*pkgoutbox.Entry, error) {
	return nil, nil
}

func (f *fakeOutbox) MarkProcessed(_ context.Context, _ uuid.UUID) error { return nil }
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

func (fakeMessageProcessing) GetByMessageIDAndStream(_ context.Context, _, _ string) (*messageprocessing.MessageProcessing, error) {
	return nil, nil
}

func (fakeMessageProcessing) GetByID(_ context.Context, _ uuid.UUID) (*messageprocessing.MessageProcessing, error) {
	return nil, nil
}

func (fakeMessageProcessing) UpdateState(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

var _ messageprocessing.Repository = (*fakeMessageProcessing)(nil)

// --- fakeUoW ---

// fakeUoW wraps the four fakes. Begin/Commit/Rollback are no-ops so handler
// tests run without a database. The same instance is shared across all handler
// calls in a test — state accumulates in the fake repos as it would in a real
// Postgres transaction sequence.
type fakeUoW struct {
	releases *fakeReleaseRepo
	cp       *fakeCurrentProdRepo
	outbox   *fakeOutbox
	msgProc  fakeMessageProcessing
}

func newFakeUoW() *fakeUoW {
	return &fakeUoW{
		releases: newFakeReleaseRepo(),
		cp:       &fakeCurrentProdRepo{},
		outbox:   &fakeOutbox{},
	}
}

func (f *fakeUoW) ReleaseRepo() repository.ReleaseRepository           { return f.releases }
func (f *fakeUoW) CurrentProdRepo() repository.CurrentProdRepository   { return f.cp }
func (f *fakeUoW) OutboxRepo() pkgoutbox.Repository                    { return f.outbox }
func (f *fakeUoW) MessageProcessingRepo() messageprocessing.Repository { return f.msgProc }
func (f *fakeUoW) Begin(_ context.Context) error                       { return nil }
func (f *fakeUoW) Commit() error                                       { return nil }
func (f *fakeUoW) Rollback() error                                     { return nil }

var _ uow.UnitOfWork = (*fakeUoW)(nil)

// --- helpers ---

// newDeps constructs a Deps with a fakeClock pinned to now and NoOpTelemetry.
// Returns both the Deps and the fakeUoW so tests can inspect recorded state.
func newDeps(now time.Time) (*handlers.Deps, *fakeUoW) {
	u := newFakeUoW()
	return &handlers.Deps{
		UoW:       u,
		Clock:     &fakeClock{t: now},
		Telemetry: ports.NoOpTelemetry{},
		Logger:    slog.Default(),
	}, u
}

// outboxEntries returns a copy of the pkgoutbox.Entry slice recorded by the
// fake outbox. Use this in handler tests instead of accessing u.outbox directly.
func outboxEntries(u *fakeUoW) []*pkgoutbox.Entry {
	u.outbox.mu.Lock()
	defer u.outbox.mu.Unlock()
	out := make([]*pkgoutbox.Entry, len(u.outbox.entries))
	copy(out, u.outbox.entries)
	return out
}
