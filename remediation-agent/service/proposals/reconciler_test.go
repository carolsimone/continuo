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
