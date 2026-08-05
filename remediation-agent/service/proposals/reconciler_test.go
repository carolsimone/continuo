package proposals_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
	"github.com/carolsimone/continuo/remediation-agent/service/proposals"
)

// fakeChecker returns a canned status (or error) per PR number.
type fakeChecker struct {
	statuses map[int]ports.PRStatus
	errs     map[int]error
}

func (f *fakeChecker) PRStatus(_ context.Context, _ string, number int) (ports.PRStatus, error) {
	if err, ok := f.errs[number]; ok {
		return ports.PRStatus{}, err
	}
	return f.statuses[number], nil
}

// fakeRecorder captures RecordOutcome calls.
type fakeRecorder struct {
	calls []struct {
		ID       string
		Outcome  proposal.PROutcome
		ClosedAt time.Time
	}
}

func (f *fakeRecorder) RecordOutcome(_ context.Context, id string, outcome proposal.PROutcome, closedAt time.Time) error {
	f.calls = append(f.calls, struct {
		ID       string
		Outcome  proposal.PROutcome
		ClosedAt time.Time
	}{id, outcome, closedAt})
	return nil
}

// fakeLister returns a fixed open-PR set.
type fakeLister struct{ prs []proposal.OpenPR }

func (f *fakeLister) ListOpenPullRequests(_ context.Context, _ int) ([]proposal.OpenPR, error) {
	return f.prs, nil
}

func newReconciler(l *fakeLister, c *fakeChecker, r *fakeRecorder) *proposals.Reconciler {
	return proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:   l,
		Checker:  c,
		Recorder: r,
		Clock:    fixedClock{},
		Logger:   slog.Default(),
	})
}

// TestReconcileOnce_MapsOutcomes verifies merged -> merged, closed-unmerged ->
// rejected, and still-open -> no call.
func TestReconcileOnce_MapsOutcomes(t *testing.T) {
	closedAt, _ := time.Parse(time.RFC3339, "2026-07-03T10:00:00Z")
	lister := &fakeLister{prs: []proposal.OpenPR{
		{ID: "p-merged", Repo: "acme/r", PRNumber: 1},
		{ID: "p-rejected", Repo: "acme/r", PRNumber: 2},
		{ID: "p-open", Repo: "acme/r", PRNumber: 3},
	}}
	checker := &fakeChecker{statuses: map[int]ports.PRStatus{
		1: {Closed: true, Merged: true, ClosedAt: closedAt},
		2: {Closed: true, Merged: false, ClosedAt: closedAt},
		3: {Closed: false},
	}}
	recorder := &fakeRecorder{}

	newReconciler(lister, checker, recorder).ReconcileOnce(context.Background())

	require.Len(t, recorder.calls, 2, "the still-open PR must not be recorded")
	require.Equal(t, "p-merged", recorder.calls[0].ID)
	require.Equal(t, proposal.PROutcomeMerged, recorder.calls[0].Outcome)
	require.Equal(t, closedAt, recorder.calls[0].ClosedAt)
	require.Equal(t, "p-rejected", recorder.calls[1].ID)
	require.Equal(t, proposal.PROutcomeRejected, recorder.calls[1].Outcome)
}

// TestReconcileOnce_ErrorOnOneRowContinues verifies a per-row GitHub failure is
// skipped without blocking the remaining rows.
func TestReconcileOnce_ErrorOnOneRowContinues(t *testing.T) {
	lister := &fakeLister{prs: []proposal.OpenPR{
		{ID: "p-err", Repo: "acme/r", PRNumber: 1},
		{ID: "p-ok", Repo: "acme/r", PRNumber: 2},
	}}
	checker := &fakeChecker{
		errs:     map[int]error{1: errors.New("boom")},
		statuses: map[int]ports.PRStatus{2: {Closed: true, Merged: true}},
	}
	recorder := &fakeRecorder{}

	newReconciler(lister, checker, recorder).ReconcileOnce(context.Background())

	require.Len(t, recorder.calls, 1)
	require.Equal(t, "p-ok", recorder.calls[0].ID)
}

// TestReconcileOnce_ZeroClosedAtFallsBackToClock verifies a missing GitHub
// timestamp falls back to the injected clock instead of a zero time.
func TestReconcileOnce_ZeroClosedAtFallsBackToClock(t *testing.T) {
	lister := &fakeLister{prs: []proposal.OpenPR{{ID: "p1", Repo: "acme/r", PRNumber: 1}}}
	checker := &fakeChecker{statuses: map[int]ports.PRStatus{1: {Closed: true, Merged: true}}}
	recorder := &fakeRecorder{}

	newReconciler(lister, checker, recorder).ReconcileOnce(context.Background())

	require.Len(t, recorder.calls, 1)
	require.Equal(t, fixedClock{}.Now(), recorder.calls[0].ClosedAt)
}

// TestReconcileOnce_PermissionErrorMarksDegraded verifies a token that cannot
// read PR status flips the reconciler health to degraded so operators get a
// signal instead of a silent, forever-retrying loop.
func TestReconcileOnce_PermissionErrorMarksDegraded(t *testing.T) {
	lister := &fakeLister{prs: []proposal.OpenPR{{ID: "p1", Repo: "acme/r", PRNumber: 1}}}
	checker := &fakeChecker{errs: map[int]error{1: ports.ErrPermissionDenied}}
	rec := newReconciler(lister, checker, &fakeRecorder{})

	require.False(t, rec.Degraded(), "health starts healthy")
	rec.ReconcileOnce(context.Background())

	require.True(t, rec.Degraded())
}

// TestReconcileOnce_TransientErrorDoesNotDegrade verifies a non-permission
// error (e.g. a network blip) on one row does not flip health to degraded when
// another row reads cleanly.
func TestReconcileOnce_TransientErrorDoesNotDegrade(t *testing.T) {
	lister := &fakeLister{prs: []proposal.OpenPR{
		{ID: "p-boom", Repo: "acme/r", PRNumber: 1},
		{ID: "p-ok", Repo: "acme/r", PRNumber: 2},
	}}
	checker := &fakeChecker{
		errs:     map[int]error{1: errors.New("connection reset")},
		statuses: map[int]ports.PRStatus{2: {Closed: true, Merged: true}},
	}
	rec := newReconciler(lister, checker, &fakeRecorder{})

	rec.ReconcileOnce(context.Background())

	require.False(t, rec.Degraded())
}

// TestReconcileOnce_RecoversAfterSuccessfulRead verifies health clears once a
// pass reads PR status cleanly again after a permission failure.
func TestReconcileOnce_RecoversAfterSuccessfulRead(t *testing.T) {
	lister := &fakeLister{prs: []proposal.OpenPR{{ID: "p1", Repo: "acme/r", PRNumber: 1}}}
	checker := &fakeChecker{errs: map[int]error{1: ports.ErrPermissionDenied}}
	rec := newReconciler(lister, checker, &fakeRecorder{})

	rec.ReconcileOnce(context.Background())
	require.True(t, rec.Degraded())

	// The permission is granted: the next pass reads cleanly.
	checker.errs = nil
	checker.statuses = map[int]ports.PRStatus{1: {Closed: true, Merged: true}}
	rec.ReconcileOnce(context.Background())

	require.False(t, rec.Degraded(), "a clean read must clear the degraded state")
}

// TestReconcileOnce_LogsErrorOnceOnDegradeTransition verifies the actionable
// ERROR is logged only when health transitions into degraded, not on every
// pass, so a standing permission gap does not flood the logs.
func TestReconcileOnce_LogsErrorOnceOnDegradeTransition(t *testing.T) {
	lister := &fakeLister{prs: []proposal.OpenPR{{ID: "p1", Repo: "acme/r", PRNumber: 1}}}
	checker := &fakeChecker{errs: map[int]error{1: ports.ErrPermissionDenied}}
	counter := &levelCounter{}
	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:   lister,
		Checker:  checker,
		Recorder: &fakeRecorder{},
		Clock:    fixedClock{},
		Logger:   slog.New(counter),
	})

	rec.ReconcileOnce(context.Background())
	rec.ReconcileOnce(context.Background())

	require.Equal(t, 1, counter.errors, "the actionable ERROR must fire only on the degrade transition")
}

// settableClock is a mutable ports.Clock for tests that need to move time
// forward between reconcile passes to exercise wall-clock grace-period math.
type settableClock struct{ now time.Time }

func (c *settableClock) Now() time.Time { return c.now }

// timePtr returns a pointer to t, for building proposal.OpeningPR.ClaimedAt
// literals inline.
func timePtr(t time.Time) *time.Time { return &t }

// fakeOpeningLister returns a fixed set of stuck 'opening' claims.
type fakeOpeningLister struct{ opening []proposal.OpeningPR }

func (f *fakeOpeningLister) ListStuckOpening(_ context.Context, _ int) ([]proposal.OpeningPR, error) {
	return f.opening, nil
}

// fakeBranchFinder returns a canned PR ref (or error) per branch.
type fakeBranchFinder struct {
	refs map[string]ports.PullRequestRef
	errs map[string]error
}

func (f *fakeBranchFinder) FindByBranch(_ context.Context, _, branch string) (ports.PullRequestRef, bool, error) {
	if err, ok := f.errs[branch]; ok {
		return ports.PullRequestRef{}, false, err
	}
	ref, ok := f.refs[branch]
	return ref, ok, nil
}

// fakeOpeningRecorder captures Record calls made to recover a stranded PR.
type fakeOpeningRecorder struct {
	calls []proposals.RecordInput
	err   error
}

func (f *fakeOpeningRecorder) Record(_ context.Context, in proposals.RecordInput) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, in)
	return nil
}

// fakeFailer captures FailPR calls made to release a stale claim.
type fakeFailer struct {
	calls []string
	err   error
}

func (f *fakeFailer) Fail(_ context.Context, id string) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, id)
	return nil
}

// TestReconcileOnce_OpeningSweep_PRFoundRecords verifies a stuck 'opening'
// claim whose branch already has a pull request on GitHub is recorded on the
// very first pass — finding an existing PR is unambiguous and safe
// regardless of how fresh the claim is.
func TestReconcileOnce_OpeningSweep_PRFoundRecords(t *testing.T) {
	branch := proposals.BuildBranch("rel-1", "model.p.orders", 1)
	opening := &fakeOpeningLister{opening: []proposal.OpeningPR{
		{ID: "p1", Repo: "acme/r", ReleaseID: "rel-1", NodeID: "model.p.orders", Attempt: 1, ClaimedAt: timePtr(fixedClock{}.Now())},
	}}
	finder := &fakeBranchFinder{refs: map[string]ports.PullRequestRef{
		branch: {Number: 9, URL: "https://github.com/acme/r/pull/9"},
	}}
	recorder := &fakeOpeningRecorder{}
	failer := &fakeFailer{}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:          &fakeLister{},
		Checker:         &fakeChecker{},
		Recorder:        &fakeRecorder{},
		OpeningLister:   opening,
		BranchFinder:    finder,
		OpeningRecorder: recorder,
		Failer:          failer,
		Clock:           fixedClock{},
		Logger:          slog.Default(),
	})
	rec.ReconcileOnce(context.Background())

	require.Len(t, recorder.calls, 1)
	require.Equal(t, "p1", recorder.calls[0].ProposalID)
	require.Equal(t, "https://github.com/acme/r/pull/9", recorder.calls[0].PrURL)
	require.Equal(t, 9, recorder.calls[0].PrNumber)
	require.Empty(t, failer.calls, "a found PR must never be failed")
}

// TestReconcileOnce_OpeningSweep_PRAbsentAgedFails verifies a stuck 'opening'
// claim with no matching PR on GitHub is released back to 'failed' once its
// ClaimedAt is further in the past than OpeningGracePeriod. A single pass is
// enough to decide this: age is read directly from the stored wall-clock
// claim time, not accumulated across passes.
func TestReconcileOnce_OpeningSweep_PRAbsentAgedFails(t *testing.T) {
	now := fixedClock{}.Now()
	opening := &fakeOpeningLister{opening: []proposal.OpeningPR{
		{ID: "p1", Repo: "acme/r", ReleaseID: "rel-1", NodeID: "model.p.orders", Attempt: 1,
			ClaimedAt: timePtr(now.Add(-4 * time.Minute))},
	}}
	finder := &fakeBranchFinder{} // no PR for any branch
	recorder := &fakeOpeningRecorder{}
	failer := &fakeFailer{}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:             &fakeLister{},
		Checker:            &fakeChecker{},
		Recorder:           &fakeRecorder{},
		OpeningLister:      opening,
		BranchFinder:       finder,
		OpeningRecorder:    recorder,
		Failer:             failer,
		Clock:              &settableClock{now: now},
		Logger:             slog.Default(),
		OpeningGracePeriod: 3 * time.Minute,
	})

	rec.ReconcileOnce(context.Background())

	require.Empty(t, recorder.calls)
	require.Equal(t, []string{"p1"}, failer.calls, "a claim older than the grace period must be failed on a single pass")
}

// TestReconcileOnce_OpeningSweep_PRAbsentFreshLeftAlone proves the invariant
// that must never break: a claim taken seconds ago, with a GitHub call still
// plausibly in flight, is left untouched even though no matching PR is found
// yet — the sweep must never race a healthy in-flight PR creation.
func TestReconcileOnce_OpeningSweep_PRAbsentFreshLeftAlone(t *testing.T) {
	now := fixedClock{}.Now()
	opening := &fakeOpeningLister{opening: []proposal.OpeningPR{
		{ID: "p1", Repo: "acme/r", ReleaseID: "rel-1", NodeID: "model.p.orders", Attempt: 1,
			ClaimedAt: timePtr(now.Add(-5 * time.Second))},
	}}
	finder := &fakeBranchFinder{}
	recorder := &fakeOpeningRecorder{}
	failer := &fakeFailer{}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:             &fakeLister{},
		Checker:            &fakeChecker{},
		Recorder:           &fakeRecorder{},
		OpeningLister:      opening,
		BranchFinder:       finder,
		OpeningRecorder:    recorder,
		Failer:             failer,
		Clock:              &settableClock{now: now},
		Logger:             slog.Default(),
		OpeningGracePeriod: 10 * time.Minute,
	})
	rec.ReconcileOnce(context.Background())

	require.Empty(t, recorder.calls)
	require.Empty(t, failer.calls, "a claim younger than the grace period must never be failed")
}

// TestReconcileOnce_OpeningSweep_AgesOutByWallClockNotPassCount verifies the
// grace-period decision tracks real elapsed time rather than how many passes
// have run: a claim left alone on earlier passes is failed once enough
// wall-clock time has actually passed, proven here by advancing a settable
// clock between calls to ReconcileOnce instead of by calling it more times.
func TestReconcileOnce_OpeningSweep_AgesOutByWallClockNotPassCount(t *testing.T) {
	clock := &settableClock{now: fixedClock{}.Now()}
	claimedAt := clock.now
	opening := &fakeOpeningLister{opening: []proposal.OpeningPR{
		{ID: "p1", Repo: "acme/r", ReleaseID: "rel-1", NodeID: "model.p.orders", Attempt: 1, ClaimedAt: &claimedAt},
	}}
	finder := &fakeBranchFinder{}
	recorder := &fakeOpeningRecorder{}
	failer := &fakeFailer{}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:             &fakeLister{},
		Checker:            &fakeChecker{},
		Recorder:           &fakeRecorder{},
		OpeningLister:      opening,
		BranchFinder:       finder,
		OpeningRecorder:    recorder,
		Failer:             failer,
		Clock:              clock,
		Logger:             slog.Default(),
		OpeningGracePeriod: 10 * time.Minute,
	})

	rec.ReconcileOnce(context.Background())
	require.Empty(t, failer.calls, "must be untouched immediately after the claim")

	clock.now = clock.now.Add(9 * time.Minute)
	rec.ReconcileOnce(context.Background())
	require.Empty(t, failer.calls, "must still be untouched just under the grace period, regardless of pass count")

	clock.now = clock.now.Add(2 * time.Minute) // total elapsed: 11m > 10m grace period
	rec.ReconcileOnce(context.Background())
	require.Equal(t, []string{"p1"}, failer.calls, "must fail once wall-clock age exceeds the grace period")
}

// TestReconcileOnce_OpeningSweep_NilClaimedAtLeftAlone verifies a stuck
// 'opening' row with no claim time — the shape of a row that predates the
// pr_claimed_at column and was not backfilled a value — is left untouched
// rather than failed, no matter how many passes run or how short the grace
// period is: an unmeasurable claim can never be mistaken for a stale one.
func TestReconcileOnce_OpeningSweep_NilClaimedAtLeftAlone(t *testing.T) {
	opening := &fakeOpeningLister{opening: []proposal.OpeningPR{
		{ID: "p1", Repo: "acme/r", ReleaseID: "rel-1", NodeID: "model.p.orders", Attempt: 1, ClaimedAt: nil},
	}}
	finder := &fakeBranchFinder{}
	recorder := &fakeOpeningRecorder{}
	failer := &fakeFailer{}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:             &fakeLister{},
		Checker:            &fakeChecker{},
		Recorder:           &fakeRecorder{},
		OpeningLister:      opening,
		BranchFinder:       finder,
		OpeningRecorder:    recorder,
		Failer:             failer,
		Clock:              fixedClock{},
		Logger:             slog.Default(),
		OpeningGracePeriod: time.Nanosecond, // even a near-zero grace period must not matter
	})

	rec.ReconcileOnce(context.Background())
	rec.ReconcileOnce(context.Background())

	require.Empty(t, recorder.calls)
	require.Empty(t, failer.calls, "a nil ClaimedAt must never be failed, regardless of grace period or pass count")
}

// TestReconcileOnce_OpeningSweep_ErrorOnOneRowContinues verifies a per-row
// GitHub lookup failure is skipped without blocking the remaining rows, and
// never fails the errored row even when its claim has aged past the grace
// period — an inconclusive read must not be treated as a confirmed miss.
func TestReconcileOnce_OpeningSweep_ErrorOnOneRowContinues(t *testing.T) {
	branchOK := proposals.BuildBranch("rel-2", "model.p.customers", 1)
	branchErr := proposals.BuildBranch("rel-1", "model.p.orders", 1)
	now := fixedClock{}.Now()
	opening := &fakeOpeningLister{opening: []proposal.OpeningPR{
		{ID: "p-err", Repo: "acme/r", ReleaseID: "rel-1", NodeID: "model.p.orders", Attempt: 1,
			ClaimedAt: timePtr(now.Add(-time.Hour))}, // aged well past the grace period below
		{ID: "p-ok", Repo: "acme/r", ReleaseID: "rel-2", NodeID: "model.p.customers", Attempt: 1,
			ClaimedAt: timePtr(now.Add(-time.Second))},
	}}
	finder := &fakeBranchFinder{
		errs: map[string]error{branchErr: errors.New("boom")},
		refs: map[string]ports.PullRequestRef{
			branchOK: {Number: 3, URL: "https://github.com/acme/r/pull/3"},
		},
	}
	recorder := &fakeOpeningRecorder{}
	failer := &fakeFailer{}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:             &fakeLister{},
		Checker:            &fakeChecker{},
		Recorder:           &fakeRecorder{},
		OpeningLister:      opening,
		BranchFinder:       finder,
		OpeningRecorder:    recorder,
		Failer:             failer,
		Clock:              &settableClock{now: now},
		Logger:             slog.Default(),
		OpeningGracePeriod: time.Minute,
	})
	rec.ReconcileOnce(context.Background())

	require.Len(t, recorder.calls, 1)
	require.Equal(t, "p-ok", recorder.calls[0].ProposalID)
	require.Empty(t, failer.calls, "the errored row must not be failed even though its claim is old — an inconclusive read is not a confirmed miss")
}

// TestReconcileOnce_OpeningSweepSkippedWithoutDeps verifies a Reconciler built
// without opening-sweep collaborators (the pre-existing wiring shape) runs the
// open-PR mirror only, and never panics on nil opening-sweep fields.
func TestReconcileOnce_OpeningSweepSkippedWithoutDeps(t *testing.T) {
	lister := &fakeLister{prs: []proposal.OpenPR{{ID: "p1", Repo: "acme/r", PRNumber: 1}}}
	checker := &fakeChecker{statuses: map[int]ports.PRStatus{1: {Closed: true, Merged: true}}}
	recorder := &fakeRecorder{}

	require.NotPanics(t, func() {
		newReconciler(lister, checker, recorder).ReconcileOnce(context.Background())
	})
	require.Len(t, recorder.calls, 1, "the open-PR mirror must still run when opening-sweep deps are absent")
}

// levelCounter is a minimal slog.Handler that counts records by level.
type levelCounter struct{ errors int }

func (c *levelCounter) Enabled(context.Context, slog.Level) bool { return true }
func (c *levelCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelError {
		c.errors++
	}
	return nil
}
func (c *levelCounter) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *levelCounter) WithGroup(string) slog.Handler      { return c }
