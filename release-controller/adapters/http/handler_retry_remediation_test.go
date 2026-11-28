package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/liveness"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/release-controller/adapters/serialization"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// newTestServer wraps NewServer with a throwaway registry and a discarding
// logger, so a handler test only has to supply the Deps it cares about.
func newTestServer(deps *handlers.Deps) *Server {
	s := NewServer(liveness.NewRegistry(), "0", slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.ServeAPI(deps)
	return s
}

// --- fakeReleaseRepo: in-memory ReleaseCheckRepository backing retry-remediation tests ---

type fakeReleaseRepo struct {
	releases map[string]*releasecheck.Check
}

func (f *fakeReleaseRepo) Get(_ context.Context, id string) (*releasecheck.Check, error) {
	return f.releases[id], nil
}

func (f *fakeReleaseRepo) Load(_ context.Context, id string) (*releasecheck.Check, error) {
	return f.releases[id], nil
}

func (f *fakeReleaseRepo) Save(_ context.Context, r *releasecheck.Check) error {
	f.releases[r.ID()] = r
	return nil
}

func (f *fakeReleaseRepo) Create(_ context.Context, r *releasecheck.Check) (bool, error) {
	if _, exists := f.releases[r.ID()]; exists {
		return false, nil
	}
	f.releases[r.ID()] = r
	return true, nil
}

func (f *fakeReleaseRepo) NextQueued(context.Context) (*releasecheck.Check, error) {
	return nil, nil
}

func (f *fakeReleaseRepo) NextQueuedOfKind(context.Context, releasecheck.Kind) (*releasecheck.Check, error) {
	return nil, nil
}

// Active returns the single check in a non-terminal, non-received status, or
// nil. Mirrors the Postgres query that guards AdvanceQueue from launching a
// second concurrent check.
func (f *fakeReleaseRepo) Active(context.Context) (*releasecheck.Check, error) {
	for _, r := range f.releases {
		switch r.Status() {
		case releasecheck.StatusCompiling, releasecheck.StatusParsing, releasecheck.StatusSeedBuilding, releasecheck.StatusValidating:
			return r, nil
		}
	}
	return nil, nil
}

// List returns every stored check matching filter.Kind, filter.VerifiesReleaseID,
// and filter.Status (each optional), newest first. Pagination is not
// simulated: HTTP handler tests exercise small, unpaginated fixtures.
func (f *fakeReleaseRepo) List(_ context.Context, filter repository.ListFilter) ([]*releasecheck.Check, *repository.ListCursor, error) {
	var out []*releasecheck.Check
	for _, r := range f.releases {
		if filter.Kind != nil && r.Kind() != *filter.Kind {
			continue
		}
		if filter.VerifiesReleaseID != nil && r.VerifiesReleaseID() != *filter.VerifiesReleaseID {
			continue
		}
		if filter.Status != nil && string(r.Status()) != *filter.Status {
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt().After(out[j].CreatedAt()) })
	return out, nil, nil
}

// ActiveCandidateProjectsWithKind lists the projects of the candidate checks of
// the given manifest kind that have not ended, sorted and without repeats.
func (f *fakeReleaseRepo) ActiveCandidateProjectsWithKind(_ context.Context, kind release.ManifestKind) ([]string, error) {
	seen := map[string]bool{}
	names := []string{}
	for _, r := range f.releases {
		if r.Kind() != releasecheck.KindCandidate || r.ManifestKind() != kind || r.Status().IsTerminal() || seen[r.ChangedProject()] {
			continue
		}
		seen[r.ChangedProject()] = true
		names = append(names, r.ChangedProject())
	}
	sort.Strings(names)
	return names, nil
}

func (f *fakeReleaseRepo) DeleteFinishedBefore(context.Context, time.Time, []string) (int, error) {
	return 0, nil
}

var _ repository.ReleaseCheckRepository = (*fakeReleaseRepo)(nil)

// --- fakeProductionPointerRepo: in-memory ProductionPointerRepository ---

type fakeProductionPointerRepo struct {
	rows map[string]*release.ProductionPointer
}

func newFakeProductionPointerRepo() *fakeProductionPointerRepo {
	return &fakeProductionPointerRepo{rows: map[string]*release.ProductionPointer{}}
}

// seedProductionPointer stores pointer as a production pointer the deps' unit of work
// reads.
func seedProductionPointer(t *testing.T, deps *handlers.Deps, pointer *release.ProductionPointer) {
	t.Helper()
	require.NoError(t, deps.NewUoW().ProductionPointerRepo().Upsert(context.Background(), pointer))
}

func (f *fakeProductionPointerRepo) List(context.Context) ([]*release.ProductionPointer, error) {
	out := make([]*release.ProductionPointer, 0, len(f.rows))
	for _, pointer := range f.rows {
		out = append(out, pointer)
	}
	return out, nil
}

func (f *fakeProductionPointerRepo) Get(_ context.Context, name string) (*release.ProductionPointer, error) {
	return f.rows[name], nil
}

func (f *fakeProductionPointerRepo) Upsert(_ context.Context, pointer *release.ProductionPointer) error {
	f.rows[pointer.ProjectName()] = pointer
	return nil
}

func (f *fakeProductionPointerRepo) ProjectsWithKind(_ context.Context, kind release.ManifestKind) ([]string, error) {
	names := []string{}
	for name, pointer := range f.rows {
		if pointer.ManifestKind() == kind {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

var _ repository.ProductionPointerRepository = (*fakeProductionPointerRepo)(nil)

// --- fakeOutboxRepo: records every entry Create writes ---

type fakeOutboxRepo struct {
	entries []*pkgoutbox.Entry
}

func (f *fakeOutboxRepo) Create(_ context.Context, e *pkgoutbox.Entry) error {
	f.entries = append(f.entries, e)
	return nil
}

func (f *fakeOutboxRepo) GetPendingBatch(context.Context, int) ([]*pkgoutbox.Entry, error) {
	return nil, nil
}
func (f *fakeOutboxRepo) MarkProcessed(context.Context, uuid.UUID) error        { return nil }
func (f *fakeOutboxRepo) MarkProcessedBatch(context.Context, []uuid.UUID) error { return nil }
func (f *fakeOutboxRepo) MarkFailed(context.Context, uuid.UUID, string) error   { return nil }
func (f *fakeOutboxRepo) IncrementRetry(context.Context, uuid.UUID) error       { return nil }

var _ pkgoutbox.Repository = (*fakeOutboxRepo)(nil)

// --- fakeUoW: a no-op transaction wrapping the fakes above ---

type fakeUoW struct {
	releases          *fakeReleaseRepo
	productionPointer *fakeProductionPointerRepo
	outbox            *fakeOutboxRepo
}

func (u *fakeUoW) ReleaseCheckRepo() repository.ReleaseCheckRepository { return u.releases }
func (u *fakeUoW) LiveReleaseRepo() repository.LiveReleaseRepository   { return nil }
func (u *fakeUoW) ProductionPointerRepo() repository.ProductionPointerRepository {
	return u.productionPointer
}
func (u *fakeUoW) PromotionSequenceRepo() repository.PromotionSequenceRepository { return nil }
func (u *fakeUoW) OutboxRepo() pkgoutbox.Repository                              { return u.outbox }
func (u *fakeUoW) MessageProcessingRepo() messageprocessing.Repository           { return nil }
func (u *fakeUoW) Begin(context.Context) error                                   { return nil }
func (u *fakeUoW) Commit() error                                                 { return nil }
func (u *fakeUoW) Rollback() error                                               { return nil }
func (u *fakeUoW) LockReleaseQueue(context.Context) error                        { return nil }

var _ uow.UnitOfWork = (*fakeUoW)(nil)

// --- fakeFixReader ---

type fakeFixReader struct {
	items []ports.FixSummary
	err   error
}

func (f *fakeFixReader) ListFixesForRelease(context.Context, string) ([]ports.FixSummary, error) {
	return f.items, f.err
}

var _ ports.FixReader = (*fakeFixReader)(nil)

// --- fakeClock ---

type fakeClock struct{ t time.Time }

func (f fakeClock) Now() time.Time { return f.t }

var _ ports.Clock = (fakeClock{})

// newRetryRemediationDeps builds a handlers.Deps wired to fresh, empty fakes.
func newRetryRemediationDeps(now time.Time) (*handlers.Deps, *fakeReleaseRepo) {
	releases := &fakeReleaseRepo{releases: map[string]*releasecheck.Check{}}
	u := &fakeUoW{releases: releases, productionPointer: newFakeProductionPointerRepo(), outbox: &fakeOutboxRepo{}}
	deps := &handlers.Deps{
		NewUoW:    func() uow.UnitOfWork { return u },
		Clock:     fakeClock{t: now},
		Telemetry: ports.NoOpTelemetry{},
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Fixes:     &fakeFixReader{},

		Rejections: serialization.ReleaseRejectedJSON{},
	}
	return deps, releases
}

// rejectedReleaseForRetry builds a release that reached StatusRejected with a
// healable reason and a stored rejection payload, ready for RetryRemediation.
func rejectedReleaseForRetry(t *testing.T, id string, now time.Time) *releasecheck.Check {
	t.Helper()
	r := releasecheck.NewCandidate(id, "finance", "abc", false, false, "o/r", "sha", release.ManifestKindDbt, now)
	require.NoError(t, r.TransitionToCompiling(now))
	require.NoError(t, r.Fail("compile_failed", "", []string{"finance"}, now))
	r.SetRejectionPayload([]byte(`{"release_id":"` + id + `","stage":"compile","reason":"compile_failed"}`))
	return r
}

func postRetryRemediation(srv *Server, releaseID string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/releases/"+releaseID+"/retry-remediation", nil)
	srv.Routes().ServeHTTP(rec, req)
	return rec
}

func TestRetryRemediationHandler_Accepted(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["rel-1"] = rejectedReleaseForRetry(t, "rel-1", now)
	deps.Fixes = &fakeFixReader{items: []ports.FixSummary{
		{ID: "p1", NodeID: "finance", Try: 1, Status: "handed_over", Round: 1},
	}}

	rec := postRetryRemediation(newTestServer(deps), "rel-1")

	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "rel-1", body["release_id"])
	require.EqualValues(t, 2, body["round"])
}

func TestRetryRemediationHandler_FixOpen(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["rel-1"] = rejectedReleaseForRetry(t, "rel-1", now)
	deps.Fixes = &fakeFixReader{items: []ports.FixSummary{
		{ID: "p2", NodeID: "finance", Try: 2, Status: "proposed", PullRequests: []ports.FixPR{{PRState: "open", PRURL: "https://x/pr/7"}}},
	}}

	rec := postRetryRemediation(newTestServer(deps), "rel-1")

	require.Equal(t, http.StatusConflict, rec.Code)
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "fix_open", body["error"])
	require.Equal(t, "p2", body["fix_id"])
	require.Equal(t, "https://x/pr/7", body["pr_url"])
}

func TestRetryRemediationHandler_RoundsExhausted(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	deps, releases := newRetryRemediationDeps(now)
	r := rejectedReleaseForRetry(t, "rel-1", now)
	_, err := r.StartRound(now)
	require.NoError(t, err)
	_, err = r.StartRound(now)
	require.NoError(t, err)
	releases.releases["rel-1"] = r

	rec := postRetryRemediation(newTestServer(deps), "rel-1")

	require.Equal(t, http.StatusConflict, rec.Code)
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "rounds_exhausted", body["error"])
}

func TestRetryRemediationHandler_NotFound(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	deps, _ := newRetryRemediationDeps(now)

	rec := postRetryRemediation(newTestServer(deps), "nope")

	require.Equal(t, http.StatusNotFound, rec.Code)
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "not_found", body["error"])
}

func TestRetryRemediationHandler_ReaderDown(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["rel-1"] = rejectedReleaseForRetry(t, "rel-1", now)
	deps.Fixes = &fakeFixReader{err: context.DeadlineExceeded}

	rec := postRetryRemediation(newTestServer(deps), "rel-1")

	require.Equal(t, http.StatusBadGateway, rec.Code)
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "fix_reader_unavailable", body["error"])
}

func TestRetryRemediationHandler_RetryInProgress(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["rel-1"] = rejectedReleaseForRetry(t, "rel-1", now)
	deps.Fixes = &fakeFixReader{items: nil}

	rec := postRetryRemediation(newTestServer(deps), "rel-1")

	require.Equal(t, http.StatusConflict, rec.Code)
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "retry_in_progress", body["error"])
}

// erroringReleaseRepo forces Load to fail, so a handler test can drive the
// generic-error (500) branch without any of the specific sentinels. It embeds
// *fakeReleaseRepo so every other method keeps that fake's behavior.
type erroringReleaseRepo struct{ *fakeReleaseRepo }

func (erroringReleaseRepo) Load(context.Context, string) (*releasecheck.Check, error) {
	return nil, errors.New("row-lock timeout")
}

func TestRetryRemediationHandler_GenericErrorIsInternal(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	repo := erroringReleaseRepo{&fakeReleaseRepo{releases: map[string]*releasecheck.Check{}}}
	u := &fakeUoW{releases: repo.fakeReleaseRepo, productionPointer: newFakeProductionPointerRepo(), outbox: &fakeOutboxRepo{}}
	deps := &handlers.Deps{
		NewUoW: func() uow.UnitOfWork {
			return &loadErrUoW{fakeUoW: u, releaseRepo: repo}
		},
		Clock:  fakeClock{t: now},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Fixes:  &fakeFixReader{},

		Rejections: serialization.ReleaseRejectedJSON{},
	}

	rec := postRetryRemediation(newTestServer(deps), "rel-1")

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "internal", body["error"])
}

// loadErrUoW wraps a fakeUoW but serves ReleaseRepo from an erroringReleaseRepo,
// so RetryRemediation's Load call fails with a plain (non-sentinel) error.
type loadErrUoW struct {
	*fakeUoW
	releaseRepo erroringReleaseRepo
}

func (u *loadErrUoW) ReleaseCheckRepo() repository.ReleaseCheckRepository { return u.releaseRepo }
