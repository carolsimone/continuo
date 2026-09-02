package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/liveness"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// newTestServer wraps NewServer with a throwaway registry and a discarding
// logger, so a handler test only has to supply the Deps it cares about.
func newTestServer(deps *handlers.Deps) *Server {
	return NewServer(deps, liveness.NewRegistry(), "0", slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// --- fakeReleaseRepo: in-memory RunRepository backing retry-remediation tests ---

type fakeReleaseRepo struct {
	releases map[string]*pipeline.Run
}

func (f *fakeReleaseRepo) Get(_ context.Context, id string) (*pipeline.Run, error) {
	return f.releases[id], nil
}

func (f *fakeReleaseRepo) Load(_ context.Context, id string) (*pipeline.Run, error) {
	return f.releases[id], nil
}

func (f *fakeReleaseRepo) Save(_ context.Context, r *pipeline.Run) error {
	f.releases[r.ID()] = r
	return nil
}

func (f *fakeReleaseRepo) NextQueued(context.Context) (*pipeline.Run, error) {
	return nil, nil
}
func (f *fakeReleaseRepo) Active(context.Context) (*pipeline.Run, error) { return nil, nil }

func (f *fakeReleaseRepo) List(context.Context, repository.ListFilter) ([]*pipeline.Run, *repository.ListCursor, error) {
	return nil, nil, nil
}

func (f *fakeReleaseRepo) DeleteFinishedBefore(context.Context, time.Time, []string) (int, error) {
	return 0, nil
}

var _ repository.RunRepository = (*fakeReleaseRepo)(nil)

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
	releases *fakeReleaseRepo
	outbox   *fakeOutboxRepo
}

func (u *fakeUoW) RunRepo() repository.RunRepository                   { return u.releases }
func (u *fakeUoW) CurrentProdRepo() repository.CurrentProdRepository   { return nil }
func (u *fakeUoW) ServiceProdRepo() repository.ServiceProdRepository   { return nil }
func (u *fakeUoW) OutboxRepo() pkgoutbox.Repository                    { return u.outbox }
func (u *fakeUoW) MessageProcessingRepo() messageprocessing.Repository { return nil }
func (u *fakeUoW) Begin(context.Context) error                         { return nil }
func (u *fakeUoW) Commit() error                                       { return nil }
func (u *fakeUoW) Rollback() error                                     { return nil }
func (u *fakeUoW) LockReleaseQueue(context.Context) error              { return nil }

var _ uow.UnitOfWork = (*fakeUoW)(nil)

// --- fakeProposalReader ---

type fakeProposalReader struct {
	items []ports.ProposalSummary
	err   error
}

func (f *fakeProposalReader) ListProposalsForRelease(context.Context, string) ([]ports.ProposalSummary, error) {
	return f.items, f.err
}

var _ ports.ProposalReader = (*fakeProposalReader)(nil)

// --- fakeClock ---

type fakeClock struct{ t time.Time }

func (f fakeClock) Now() time.Time { return f.t }

var _ ports.Clock = (fakeClock{})

// newRetryRemediationDeps builds a handlers.Deps wired to fresh, empty fakes.
func newRetryRemediationDeps(now time.Time) (*handlers.Deps, *fakeReleaseRepo) {
	releases := &fakeReleaseRepo{releases: map[string]*pipeline.Run{}}
	u := &fakeUoW{releases: releases, outbox: &fakeOutboxRepo{}}
	deps := &handlers.Deps{
		NewUoW:    func() uow.UnitOfWork { return u },
		Clock:     fakeClock{t: now},
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Proposals: &fakeProposalReader{},
	}
	return deps, releases
}

// rejectedReleaseForRetry builds a release that reached StatusRejected with a
// healable reason and a stored rejection payload, ready for RetryRemediation.
func rejectedReleaseForRetry(t *testing.T, id string, now time.Time) *pipeline.Run {
	t.Helper()
	r := pipeline.NewCandidate(id, "finance", "abc", false, "o/r", "sha", release.ManifestKindDbt, now)
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
	deps.Proposals = &fakeProposalReader{items: []ports.ProposalSummary{
		{ID: "p1", NodeID: "finance", Attempt: 1, Status: "escalated", RemediationRound: 1},
	}}

	rec := postRetryRemediation(newTestServer(deps), "rel-1")

	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "rel-1", body["release_id"])
	require.EqualValues(t, 2, body["remediation_round"])
}

func TestRetryRemediationHandler_ProposalOpen(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["rel-1"] = rejectedReleaseForRetry(t, "rel-1", now)
	deps.Proposals = &fakeProposalReader{items: []ports.ProposalSummary{
		{ID: "p2", NodeID: "finance", Attempt: 2, Status: "proposed", PRState: "open", PRURL: "https://x/pr/7"},
	}}

	rec := postRetryRemediation(newTestServer(deps), "rel-1")

	require.Equal(t, http.StatusConflict, rec.Code)
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "proposal_open", body["error"])
	require.Equal(t, "p2", body["proposal_id"])
	require.Equal(t, "https://x/pr/7", body["pr_url"])
}

func TestRetryRemediationHandler_RoundsExhausted(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	deps, releases := newRetryRemediationDeps(now)
	r := rejectedReleaseForRetry(t, "rel-1", now)
	_, err := r.StartRemediationRound(now)
	require.NoError(t, err)
	_, err = r.StartRemediationRound(now)
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
	deps.Proposals = &fakeProposalReader{err: context.DeadlineExceeded}

	rec := postRetryRemediation(newTestServer(deps), "rel-1")

	require.Equal(t, http.StatusBadGateway, rec.Code)
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "proposal_reader_unavailable", body["error"])
}

func TestRetryRemediationHandler_RetryInProgress(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	deps, releases := newRetryRemediationDeps(now)
	releases.releases["rel-1"] = rejectedReleaseForRetry(t, "rel-1", now)
	deps.Proposals = &fakeProposalReader{items: nil}

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

func (erroringReleaseRepo) Load(context.Context, string) (*pipeline.Run, error) {
	return nil, errors.New("row-lock timeout")
}

func TestRetryRemediationHandler_GenericErrorIsInternal(t *testing.T) {
	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	repo := erroringReleaseRepo{&fakeReleaseRepo{releases: map[string]*pipeline.Run{}}}
	u := &fakeUoW{releases: repo.fakeReleaseRepo, outbox: &fakeOutboxRepo{}}
	deps := &handlers.Deps{
		NewUoW: func() uow.UnitOfWork {
			return &loadErrUoW{fakeUoW: u, releaseRepo: repo}
		},
		Clock:     fakeClock{t: now},
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Proposals: &fakeProposalReader{},
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

func (u *loadErrUoW) RunRepo() repository.RunRepository { return u.releaseRepo }
