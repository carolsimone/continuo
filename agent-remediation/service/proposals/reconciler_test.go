package proposals_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/proposals"
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
		Service  string
		Outcome  proposal.PROutcome
		ClosedAt time.Time
		Edits    []event.ClosedEdit
		Resolved []string
	}
}

func (f *fakeRecorder) RecordOutcome(_ context.Context, id, service string, outcome proposal.PROutcome, closedAt time.Time, edits []event.ClosedEdit, resolved []string) error {
	f.calls = append(f.calls, struct {
		ID       string
		Service  string
		Outcome  proposal.PROutcome
		ClosedAt time.Time
		Edits    []event.ClosedEdit
		Resolved []string
	}{id, service, outcome, closedAt, edits, resolved})
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
		Getter:   &fakeGetter{},
		Sources:  &fakeSourceReader{},
		Evidence: &fakeEvidence{},
		Clock:    fixedClock{},
		Logger:   slog.Default(),
	})
}

// fakeGetter returns a canned proposal View per id (or an injected error).
type fakeGetter struct {
	views map[string]proposal.View
	errs  map[string]error
}

func (f *fakeGetter) Get(_ context.Context, id string) (proposal.View, error) {
	if err, ok := f.errs[id]; ok {
		return proposal.View{}, err
	}
	return f.views[id], nil
}

// fakeSourceReader returns canned merged file content keyed on ref+"\x00"+path;
// an unset key returns ErrSourceNotFound (the adapter's 404 shape). reads counts
// ReadFile calls so a test can assert the compare was skipped entirely.
type fakeSourceReader struct {
	files map[string]string
	errs  map[string]error
	reads int
}

func (f *fakeSourceReader) ReadFile(_ context.Context, _, ref, path string) (string, error) {
	f.reads++
	key := ref + "\x00" + path
	if err, ok := f.errs[key]; ok {
		return "", err
	}
	c, ok := f.files[key]
	if !ok {
		return "", ports.ErrSourceNotFound
	}
	return c, nil
}

func (f *fakeSourceReader) ListDir(context.Context, string, string, string) ([]string, error) {
	return nil, nil
}

// fakeEvidence returns canned S3 object text per URI; fetches counts calls.
type fakeEvidence struct {
	objs    map[string]string
	errs    map[string]error
	fetches int
}

func (f *fakeEvidence) Fetch(_ context.Context, uri string) (string, error) {
	f.fetches++
	if err, ok := f.errs[uri]; ok {
		return "", err
	}
	c, ok := f.objs[uri]
	if !ok {
		return "", ports.ErrNotFound
	}
	return c, nil
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

// amendServiceRepoPaths and amendView build a split proposal whose two edits
// land under different services ("core" and "finance"), each fixing one node,
// so a per-service PR's compare and resolved subset can be asserted to cover
// only its own files and nodes.
func amendServiceRepoPaths() map[string]string {
	return map[string]string{"core": "services/core", "finance": "services/finance"}
}

func amendView() proposal.View {
	return proposal.View{
		ID:              "prop-1",
		ReleaseID:       "rel-1",
		Attempt:         1,
		ResolvedNodeIDs: []string{"model.core.a", "model.finance.b"},
		NodeOutcomes: map[string]proposal.NodeOutcome{
			"model.core.a":    {Status: proposal.StatusProposed},
			"model.finance.b": {Status: proposal.StatusProposed},
		},
		Edits: []proposal.FileEdit{
			{Path: "services/core/models/a.sql", ContentURI: "s3://b/core-content", DiffURI: "s3://b/core-diff",
				TargetNodeID: "model.core.a", MemberNodeIDs: []string{"model.core.a"}},
			{Path: "services/finance/models/b.sql", ContentURI: "s3://b/fin-content", DiffURI: "s3://b/fin-diff",
				TargetNodeID: "model.finance.b", MemberNodeIDs: []string{"model.finance.b"}},
		},
	}
}

// TestReconcileOnce_MergedRunsAmendCompareOverServiceSubset verifies a merged
// per-service PR drives resolveClosedEdits over ONLY that service's edits (the
// finance edit is never fetched), stamps the human amendment it finds, and
// passes both the resolved close detail and the per-service resolved node set
// into RecordOutcome — the non-empty subset, not the empty-fallback the
// outcome-mirror pass relies on.
func TestReconcileOnce_MergedRunsAmendCompareOverServiceSubset(t *testing.T) {
	closedAt, _ := time.Parse(time.RFC3339, "2026-07-03T10:00:00Z")
	lister := &fakeLister{prs: []proposal.OpenPR{
		{ID: "prop-1", Repo: "acme/r", PRNumber: 7, ReleaseID: "rel-1", NodeID: "model.core.a", Attempt: 1, Service: "core"},
	}}
	checker := &fakeChecker{statuses: map[int]ports.PRStatus{
		7: {Closed: true, Merged: true, ClosedAt: closedAt, MergeCommitSHA: "mergesha"},
	}}
	getter := &fakeGetter{views: map[string]proposal.View{"prop-1": amendView()}}
	// The merged core file differs from the proposal -> amended. Only the core
	// URIs are stocked: were the finance edit compared, its content/diff fetch
	// would miss and abort the whole resolution, so RecordOutcome would never
	// fire — the assertions below would then fail, guarding the subset.
	sources := &fakeSourceReader{files: map[string]string{
		"mergesha\x00services/core/models/a.sql": "SELECT amended\n",
	}}
	evidence := &fakeEvidence{objs: map[string]string{
		"s3://b/core-content": "SELECT original\n",
		"s3://b/core-diff":    "core diff text",
	}}
	recorder := &fakeRecorder{}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:           lister,
		Checker:          checker,
		Recorder:         recorder,
		Getter:           getter,
		Sources:          sources,
		Evidence:         evidence,
		ServiceRepoPaths: amendServiceRepoPaths(),
		Clock:            fixedClock{},
		Logger:           slog.Default(),
	})
	rec.ReconcileOnce(context.Background())

	require.Len(t, recorder.calls, 1)
	call := recorder.calls[0]
	require.Equal(t, "prop-1", call.ID)
	require.Equal(t, "core", call.Service)
	require.Equal(t, proposal.PROutcomeMerged, call.Outcome)
	require.Equal(t, closedAt, call.ClosedAt)
	require.Len(t, call.Edits, 1, "only the core service's edit is compared, not finance")
	require.Equal(t, "services/core/models/a.sql", call.Edits[0].Path)
	require.True(t, call.Edits[0].Amended, "the merged core file differs from the proposal")
	require.Equal(t, "core diff text", call.Edits[0].Diff)
	require.Equal(t, []string{"model.core.a"}, call.Resolved,
		"RecordOutcome must receive the non-empty per-service resolved subset, not rely on the empty-fallback")
}

// TestReconcileOnce_MergedAmendCompareErrorRecordsNothing verifies a transient
// fetch error during the amend compare leaves the row untouched — no
// RecordOutcome this tick, so it stays 'open' for the next pass — and does NOT
// flip the reconciler to degraded (only a PRStatus permission error does that).
func TestReconcileOnce_MergedAmendCompareErrorRecordsNothing(t *testing.T) {
	lister := &fakeLister{prs: []proposal.OpenPR{
		{ID: "prop-1", Repo: "acme/r", PRNumber: 7, ReleaseID: "rel-1", NodeID: "model.core.a", Attempt: 1, Service: "core"},
	}}
	checker := &fakeChecker{statuses: map[int]ports.PRStatus{
		7: {Closed: true, Merged: true, MergeCommitSHA: "mergesha"},
	}}
	getter := &fakeGetter{views: map[string]proposal.View{"prop-1": amendView()}}
	sources := &fakeSourceReader{errs: map[string]error{
		"mergesha\x00services/core/models/a.sql": errors.New("502 bad gateway"),
	}}
	evidence := &fakeEvidence{}
	recorder := &fakeRecorder{}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:           lister,
		Checker:          checker,
		Recorder:         recorder,
		Getter:           getter,
		Sources:          sources,
		Evidence:         evidence,
		ServiceRepoPaths: amendServiceRepoPaths(),
		Clock:            fixedClock{},
		Logger:           slog.Default(),
	})
	rec.ReconcileOnce(context.Background())

	require.Empty(t, recorder.calls, "a compare error must record nothing this tick")
	require.False(t, rec.Degraded(), "a compare error must not flip the degraded health signal")
}

// TestReconcileOnce_RejectedSkipsAmendCompare verifies a rejected (closed but
// not merged) PR records its outcome with no edits and never runs the amend
// compare — no source or evidence read happens — while still naming the
// per-service resolved node subset.
func TestReconcileOnce_RejectedSkipsAmendCompare(t *testing.T) {
	closedAt, _ := time.Parse(time.RFC3339, "2026-07-03T10:00:00Z")
	lister := &fakeLister{prs: []proposal.OpenPR{
		{ID: "prop-1", Repo: "acme/r", PRNumber: 8, ReleaseID: "rel-1", NodeID: "model.core.a", Attempt: 1, Service: "core"},
	}}
	checker := &fakeChecker{statuses: map[int]ports.PRStatus{
		8: {Closed: true, Merged: false, ClosedAt: closedAt},
	}}
	getter := &fakeGetter{views: map[string]proposal.View{"prop-1": amendView()}}
	// Would error if read, proving the compare is skipped for a rejected PR.
	sources := &fakeSourceReader{errs: map[string]error{
		"\x00services/core/models/a.sql": errors.New("must not be read"),
	}}
	evidence := &fakeEvidence{}
	recorder := &fakeRecorder{}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:           lister,
		Checker:          checker,
		Recorder:         recorder,
		Getter:           getter,
		Sources:          sources,
		Evidence:         evidence,
		ServiceRepoPaths: amendServiceRepoPaths(),
		Clock:            fixedClock{},
		Logger:           slog.Default(),
	})
	rec.ReconcileOnce(context.Background())

	require.Len(t, recorder.calls, 1)
	call := recorder.calls[0]
	require.Equal(t, proposal.PROutcomeRejected, call.Outcome)
	require.Empty(t, call.Edits, "a rejected PR carries no per-file close detail")
	require.Equal(t, []string{"model.core.a"}, call.Resolved)
	require.Zero(t, sources.reads, "the amend compare must not read any source for a rejected PR")
	require.Zero(t, evidence.fetches, "the amend compare must not fetch any evidence for a rejected PR")
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

// ListStuckOpening simulates the real repository's keyset pagination: rows
// must already be sorted by (CreatedAt, ID, Service) in f.opening, since
// callers rely on that same ordering to build a resumable cursor. Service is
// part of the key because one proposal can now have several 'opening' children
// (one per owning service) that share a (CreatedAt, ID) — the service tiebreak
// keeps a second child from being skipped past on the following page.
func (f *fakeOpeningLister) ListStuckOpening(_ context.Context, limit int, cursor *repository.OpeningCursor) ([]proposal.OpeningPR, *repository.OpeningCursor, error) {
	start := 0
	if cursor != nil {
		start = len(f.opening)
		for i, o := range f.opening {
			if openingAfterCursor(o, cursor) {
				start = i
				break
			}
		}
	}
	end := len(f.opening)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	if start > end {
		start = end
	}
	page := append([]proposal.OpeningPR{}, f.opening[start:end]...)
	var next *repository.OpeningCursor
	if end < len(f.opening) {
		last := page[len(page)-1]
		next = &repository.OpeningCursor{CreatedAt: last.CreatedAt, ID: last.ID, Service: last.Service}
	}
	return page, next, nil
}

// openingAfterCursor reports whether o sorts strictly after the cursor under
// the (CreatedAt, ID, Service) keyset the real repository orders by.
func openingAfterCursor(o proposal.OpeningPR, c *repository.OpeningCursor) bool {
	if !o.CreatedAt.Equal(c.CreatedAt) {
		return o.CreatedAt.After(c.CreatedAt)
	}
	if o.ID != c.ID {
		return o.ID > c.ID
	}
	return o.Service > c.Service
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

// fakeFailer captures FailStuckClaim calls made to release a stale claim,
// simulating the real repository's compare-and-set: an id listed in
// reClaimed reports hit=false (as if a re-claim raced ahead of this call)
// without being recorded as failed.
type fakeFailer struct {
	calls         []string
	observedClaim map[string]time.Time
	observedSvc   map[string]string
	reClaimed     map[string]bool
	err           error
}

func (f *fakeFailer) FailStuckClaim(_ context.Context, id, service string, observedClaimedAt time.Time) (bool, error) {
	if f.observedClaim == nil {
		f.observedClaim = map[string]time.Time{}
		f.observedSvc = map[string]string{}
	}
	f.observedClaim[id] = observedClaimedAt
	f.observedSvc[id] = service
	if f.err != nil {
		return false, f.err
	}
	if f.reClaimed[id] {
		return false, nil
	}
	f.calls = append(f.calls, id)
	return true, nil
}

// TestReconcileOnce_OpeningSweep_PRFoundRecords verifies a stuck 'opening'
// claim whose branch already has a pull request on GitHub is recorded on the
// very first pass — finding an existing PR is unambiguous and safe
// regardless of how fresh the claim is.
func TestReconcileOnce_OpeningSweep_PRFoundRecords(t *testing.T) {
	branch := proposals.BuildBranch("rel-1", 1, "")
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
	branchOK := proposals.BuildBranch("rel-2", 1, "")
	branchErr := proposals.BuildBranch("rel-1", 1, "")
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

// TestReconcileOnce_OpeningSweepPermissionErrorMarksDegraded verifies a
// permission-denied error from the opening sweep's branch lookup feeds the
// same degraded signal as the outcome loop's PRStatus reads: both read the
// same GitHub token, so a permission gap discovered by either loop must be
// visible via Degraded(), not silently swallowed into a generic warn.
func TestReconcileOnce_OpeningSweepPermissionErrorMarksDegraded(t *testing.T) {
	branch := proposals.BuildBranch("rel-1", 1, "")
	opening := &fakeOpeningLister{opening: []proposal.OpeningPR{
		{ID: "p1", Repo: "acme/r", ReleaseID: "rel-1", NodeID: "model.p.orders", Attempt: 1,
			ClaimedAt: timePtr(fixedClock{}.Now())},
	}}
	finder := &fakeBranchFinder{errs: map[string]error{branch: ports.ErrPermissionDenied}}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:          &fakeLister{},
		Checker:         &fakeChecker{},
		Recorder:        &fakeRecorder{},
		OpeningLister:   opening,
		BranchFinder:    finder,
		OpeningRecorder: &fakeOpeningRecorder{},
		Failer:          &fakeFailer{},
		Clock:           fixedClock{},
		Logger:          slog.Default(),
	})

	require.False(t, rec.Degraded(), "health starts healthy")
	rec.ReconcileOnce(context.Background())

	require.True(t, rec.Degraded(), "a permission error from the opening sweep must degrade health")
}

// TestReconcileOnce_OpeningSweepNilClaimedAtLogsWarnEveryPass verifies a
// stuck 'opening' row with no claim time produces a warn log on every pass it
// is seen, not just once — so an operator has a standing signal for a claim
// that can never be resolved by age, rather than it silently occupying a
// batch slot forever.
func TestReconcileOnce_OpeningSweepNilClaimedAtLogsWarnEveryPass(t *testing.T) {
	opening := &fakeOpeningLister{opening: []proposal.OpeningPR{
		{ID: "p1", Repo: "acme/r", ReleaseID: "rel-1", NodeID: "model.p.orders", Attempt: 1, ClaimedAt: nil},
	}}
	counter := &levelCounter{}
	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:          &fakeLister{},
		Checker:         &fakeChecker{},
		Recorder:        &fakeRecorder{},
		OpeningLister:   opening,
		BranchFinder:    &fakeBranchFinder{},
		OpeningRecorder: &fakeOpeningRecorder{},
		Failer:          &fakeFailer{},
		Clock:           fixedClock{},
		Logger:          slog.New(counter),
	})

	rec.ReconcileOnce(context.Background())
	rec.ReconcileOnce(context.Background())
	rec.ReconcileOnce(context.Background())

	require.Equal(t, 3, counter.warnings, "an unmeasurable claim must be logged every pass it is seen, unlike the degrade-transition ERROR")
}

// TestReconcileOnce_OpeningSweep_ReClaimedRowLeftUntouched is the regression
// test for the lost-update C1 fixes: when the claim the sweep is about to
// fail was released and re-claimed since this pass listed it (simulated here
// via fakeFailer.reClaimed, which mirrors the real repository's CAS miss),
// the sweep must not record the row as failed, must not treat the miss as an
// error, and must pass the ORIGINALLY-OBSERVED claim time — not some other
// value — into the CAS call.
func TestReconcileOnce_OpeningSweep_ReClaimedRowLeftUntouched(t *testing.T) {
	now := fixedClock{}.Now()
	observedClaim := now.Add(-time.Hour) // aged well past the grace period below
	opening := &fakeOpeningLister{opening: []proposal.OpeningPR{
		{ID: "p1", Repo: "acme/r", ReleaseID: "rel-1", NodeID: "model.p.orders", Attempt: 1,
			ClaimedAt: timePtr(observedClaim)},
	}}
	finder := &fakeBranchFinder{} // no PR for any branch
	recorder := &fakeOpeningRecorder{}
	failer := &fakeFailer{reClaimed: map[string]bool{"p1": true}}

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
	require.Empty(t, failer.calls, "a CAS miss must never be recorded as a successful fail")
	require.Equal(t, observedClaim, failer.observedClaim["p1"],
		"the sweep must pass the claim time it actually observed, not the row's current (re-claimed) one")
}

// TestReconcileOnce_OpeningSweep_RotatesPastPersistentlyErroringRows is the
// regression test for the starvation C3 fixes: with a batch limit smaller
// than the stuck set, and the oldest rows permanently unresolvable (a
// standing GitHub error, never found and never aged out), later rows must
// still be visited within a bounded number of passes — proving the sweep
// advances past a stuck prefix instead of re-reading the same oldest rows
// forever.
func TestReconcileOnce_OpeningSweep_RotatesPastPersistentlyErroringRows(t *testing.T) {
	now := fixedClock{}.Now()
	// One release, five attempts of it: the branch is keyed on
	// (releaseID, attempt) alone, so varying Attempt on a single ReleaseID is
	// what keeps the five branches distinct here — proving the sweep's
	// per-row branch lookup discriminates attempts of the SAME release, not
	// just different releases.
	const releaseID = "rel-p"
	branch := func(attempt int) string { return proposals.BuildBranch(releaseID, attempt, "") }

	// Five rows, strictly increasing CreatedAt (and thus stable sweep order).
	// attempt 1 and 2 error on every pass and never resolve; 3-5 succeed.
	ids := []string{"p1", "p2", "p3", "p4", "p5"}
	var opening []proposal.OpeningPR
	for i, id := range ids {
		attempt := i + 1
		opening = append(opening, proposal.OpeningPR{
			ID: id, Repo: "acme/r", ReleaseID: releaseID, NodeID: id, Attempt: attempt,
			ClaimedAt: timePtr(now),
			CreatedAt: now.Add(time.Duration(i) * time.Second),
		})
	}
	lister := &fakeOpeningLister{opening: opening}
	finder := &fakeBranchFinder{
		errs: map[string]error{branch(1): errors.New("boom"), branch(2): errors.New("boom")},
		refs: map[string]ports.PullRequestRef{
			branch(3): {Number: 3, URL: "https://github.com/acme/r/pull/3"},
			branch(4): {Number: 4, URL: "https://github.com/acme/r/pull/4"},
			branch(5): {Number: 5, URL: "https://github.com/acme/r/pull/5"},
		},
	}
	recorder := &fakeOpeningRecorder{}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:          &fakeLister{},
		Checker:         &fakeChecker{},
		Recorder:        &fakeRecorder{},
		OpeningLister:   lister,
		BranchFinder:    finder,
		OpeningRecorder: recorder,
		Failer:          &fakeFailer{},
		Clock:           fixedClock{},
		Logger:          slog.Default(),
		BatchLimit:      2, // smaller than len(ids): a single pass cannot see every row
	})

	// Pass 1 sees [p1, p2]: both error, nothing recorded.
	rec.ReconcileOnce(context.Background())
	require.Empty(t, recorder.calls, "pass 1 only sees the two permanently-erroring rows")

	// Pass 2 must resume AFTER p2, not re-read [p1, p2] again: it sees
	// [p3, p4] and records both.
	rec.ReconcileOnce(context.Background())
	require.Len(t, recorder.calls, 2, "pass 2 must have advanced past the stuck prefix to reach p3 and p4")

	// Pass 3 sees the remainder, [p5], and wraps the cursor back to the start.
	rec.ReconcileOnce(context.Background())
	require.Len(t, recorder.calls, 3, "pass 3 must reach p5, the row furthest behind the stuck prefix")

	got := make([]string, len(recorder.calls))
	for i, c := range recorder.calls {
		got[i] = c.ProposalID
	}
	require.ElementsMatch(t, []string{"p3", "p4", "p5"}, got,
		"every resolvable row must be visited within one full rotation, despite p1/p2 never resolving")
}

// TestReconcileOnce_OpeningSweepRecordsGitHubCreatedAt verifies a recovered
// PR is recorded with GitHub's own creation time, not the moment the sweep
// happened to run — a claim can be recovered minutes or hours after GitHub
// actually created the PR, and pr_opened_at should reflect the true value.
func TestReconcileOnce_OpeningSweepRecordsGitHubCreatedAt(t *testing.T) {
	branch := proposals.BuildBranch("rel-1", 1, "")
	githubCreatedAt := fixedClock{}.Now().Add(-45 * time.Minute)
	opening := &fakeOpeningLister{opening: []proposal.OpeningPR{
		{ID: "p1", Repo: "acme/r", ReleaseID: "rel-1", NodeID: "model.p.orders", Attempt: 1,
			ClaimedAt: timePtr(githubCreatedAt)},
	}}
	finder := &fakeBranchFinder{refs: map[string]ports.PullRequestRef{
		branch: {Number: 9, URL: "https://github.com/acme/r/pull/9", CreatedAt: githubCreatedAt},
	}}
	recorder := &fakeOpeningRecorder{}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:          &fakeLister{},
		Checker:         &fakeChecker{},
		Recorder:        &fakeRecorder{},
		OpeningLister:   opening,
		BranchFinder:    finder,
		OpeningRecorder: recorder,
		Failer:          &fakeFailer{},
		Clock:           fixedClock{},
		Logger:          slog.Default(),
	})
	rec.ReconcileOnce(context.Background())

	require.Len(t, recorder.calls, 1)
	require.Equal(t, githubCreatedAt, recorder.calls[0].OpenedAt,
		"a recovered PR must be recorded with GitHub's own created_at, not the recovery time")
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

// TestReconcileOnce_OpeningSweep_MultipleServicesSameProposalNotStarved is the
// regression test for the F5 cursor fix: one proposal now has two 'opening'
// children (a per-service PR each for "core" and "finance") that share the
// parent's (created_at, id). With a batch limit small enough to split the two
// children across pages, the sweep must still visit both — the keyset cursor
// includes the service, so resuming after the first child does not skip past
// the second. Under the old (created_at, id)-only cursor the second child was
// stranded forever, since `> (created_at, id)` excluded a sibling sharing that
// key.
func TestReconcileOnce_OpeningSweep_MultipleServicesSameProposalNotStarved(t *testing.T) {
	now := fixedClock{}.Now()
	coreBranch := proposals.BuildBranch("rel-1", 1, "core")
	financeBranch := proposals.BuildBranch("rel-1", 1, "finance")
	// Two children of the SAME proposal (same ID and CreatedAt), sorted by the
	// (CreatedAt, ID, Service) keyset the real repository orders by: core < finance.
	opening := &fakeOpeningLister{opening: []proposal.OpeningPR{
		{ID: "p1", Service: "core", Repo: "acme/r", ReleaseID: "rel-1", NodeID: "model.core.a", Attempt: 1,
			ClaimedAt: timePtr(now), CreatedAt: now},
		{ID: "p1", Service: "finance", Repo: "acme/r", ReleaseID: "rel-1", NodeID: "model.finance.b", Attempt: 1,
			ClaimedAt: timePtr(now), CreatedAt: now},
	}}
	finder := &fakeBranchFinder{refs: map[string]ports.PullRequestRef{
		coreBranch:    {Number: 1, URL: "https://github.com/acme/r/pull/1"},
		financeBranch: {Number: 2, URL: "https://github.com/acme/r/pull/2"},
	}}
	recorder := &fakeOpeningRecorder{}

	rec := proposals.NewReconciler(proposals.ReconcilerDeps{
		Lister:          &fakeLister{},
		Checker:         &fakeChecker{},
		Recorder:        &fakeRecorder{},
		OpeningLister:   opening,
		BranchFinder:    finder,
		OpeningRecorder: recorder,
		Failer:          &fakeFailer{},
		Clock:           fixedClock{},
		Logger:          slog.Default(),
		BatchLimit:      1, // one child per page: the two siblings land on different pages
	})

	// Pass 1 records "core" and leaves a cursor keyed on (…, id=p1, service=core).
	rec.ReconcileOnce(context.Background())
	// Pass 2 must resume AFTER (p1, core) and reach (p1, finance), not skip it.
	rec.ReconcileOnce(context.Background())

	require.Len(t, recorder.calls, 2, "both per-service children of one proposal must be recovered")
	got := make([][2]string, len(recorder.calls))
	for i, c := range recorder.calls {
		got[i] = [2]string{c.ProposalID, c.Service}
	}
	require.ElementsMatch(t, [][2]string{{"p1", "core"}, {"p1", "finance"}}, got,
		"a second per-service child sharing the parent's (created_at, id) must not be starved by the cursor")
}

// levelCounter is a minimal slog.Handler that counts records by level.
type levelCounter struct {
	errors   int
	warnings int
}

func (c *levelCounter) Enabled(context.Context, slog.Level) bool { return true }
func (c *levelCounter) Handle(_ context.Context, r slog.Record) error {
	switch {
	case r.Level >= slog.LevelError:
		c.errors++
	case r.Level == slog.LevelWarn:
		c.warnings++
	}
	return nil
}
func (c *levelCounter) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *levelCounter) WithGroup(string) slog.Handler      { return c }
