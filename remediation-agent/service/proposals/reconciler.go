package proposals

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// OpenPRLister is the repository slice the reconciler reads: proposals whose
// PR awaits a terminal outcome.
type OpenPRLister interface {
	ListOpenPullRequests(ctx context.Context, limit int) ([]proposal.OpenPR, error)
}

// OutcomeRecorder is the Service slice the reconciler drives; the concrete
// *Service satisfies it.
type OutcomeRecorder interface {
	RecordOutcome(ctx context.Context, id string, outcome proposal.PROutcome, closedAt time.Time) error
}

// ReconcilerDeps holds every collaborator the Reconciler needs, all behind
// ports or narrow interfaces.
type ReconcilerDeps struct {
	Lister   OpenPRLister
	Checker  ports.PullRequestStatusChecker
	Recorder OutcomeRecorder
	Clock    ports.Clock
	Logger   *slog.Logger
	// Interval between reconcile passes; <=0 falls back to one minute.
	Interval time.Duration
	// BatchLimit caps the open-PR rows fetched per pass; <=0 falls back to 50.
	BatchLimit int
}

// Reconciler mirrors terminal GitHub PR outcomes onto proposal rows: each pass
// lists proposals with an open PR, reads the PR state from GitHub, and records
// merged/rejected outcomes. Rows are handled best-effort: one failing row is
// logged and skipped so it never blocks the rest, and is retried next pass.
type Reconciler struct {
	lister     OpenPRLister
	checker    ports.PullRequestStatusChecker
	recorder   OutcomeRecorder
	clock      ports.Clock
	logger     *slog.Logger
	interval   time.Duration
	batchLimit int
	// degraded records whether the last pass that probed GitHub could not read
	// PR status because of a permission error. Accessed only from the single
	// reconcile goroutine (and tests), so it needs no synchronization.
	degraded bool
}

// NewReconciler constructs a Reconciler, applying defaults for Interval (1m)
// and BatchLimit (50).
func NewReconciler(d ReconcilerDeps) *Reconciler {
	if d.Interval <= 0 {
		d.Interval = time.Minute
	}
	if d.BatchLimit <= 0 {
		d.BatchLimit = 50
	}
	return &Reconciler{
		lister:     d.Lister,
		checker:    d.Checker,
		recorder:   d.Recorder,
		clock:      d.Clock,
		logger:     d.Logger,
		interval:   d.Interval,
		batchLimit: d.BatchLimit,
	}
}

// Degraded reports whether PR reads are currently failing on a permission
// error, exposed for tests to assert the reconciler's health tracking.
func (r *Reconciler) Degraded() bool { return r.degraded }

// Run executes ReconcileOnce on every tick until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.ReconcileOnce(ctx)
		}
	}
}

// ReconcileOnce performs a single reconcile pass.
func (r *Reconciler) ReconcileOnce(ctx context.Context) {
	open, err := r.lister.ListOpenPullRequests(ctx, r.batchLimit)
	if err != nil {
		r.logger.Warn("pr reconciler: list open pull requests", "error", err)
		return
	}
	permissionDenied := false
	cleanRead := false
	for _, pr := range open {
		st, err := r.checker.PRStatus(ctx, pr.Repo, pr.PRNumber)
		if err != nil {
			if errors.Is(err, ports.ErrPermissionDenied) {
				permissionDenied = true
			}
			r.logger.Warn("pr reconciler: fetch pr status",
				"proposal_id", pr.ID, "repo", pr.Repo, "pr_number", pr.PRNumber, "error", err)
			continue
		}
		cleanRead = true
		if !st.Closed {
			continue
		}
		outcome := proposal.PROutcomeRejected
		if st.Merged {
			outcome = proposal.PROutcomeMerged
		}
		closedAt := st.ClosedAt
		if closedAt.IsZero() {
			closedAt = r.clock.Now()
		}
		if err := r.recorder.RecordOutcome(ctx, pr.ID, outcome, closedAt); err != nil {
			r.logger.Warn("pr reconciler: record outcome",
				"proposal_id", pr.ID, "outcome", string(outcome), "error", err)
			continue
		}
		r.logger.Info("pr reconciler: proposal PR reached terminal outcome",
			"proposal_id", pr.ID, "repo", pr.Repo, "pr_number", pr.PRNumber, "outcome", string(outcome))
	}
	r.updateHealth(permissionDenied, cleanRead)
}

// updateHealth reconciles the degraded flag from one pass. A permission error
// degrades it; a clean read recovers it. A pass that neither read cleanly nor
// hit a permission error (no open PRs, or only transient errors) leaves the
// prior state untouched so momentary blips do not flap the signal. The
// actionable log fires only on the healthy<->degraded transition, so a standing
// permission gap does not flood the logs every pass.
func (r *Reconciler) updateHealth(permissionDenied, cleanRead bool) {
	switch {
	case permissionDenied:
		if !r.degraded {
			r.degraded = true
			r.logger.Error("pr reconciler: cannot read pull request status; PR outcomes will not reconcile until fixed",
				"action", "grant the GitHub token 'Pull requests: Read' on the target repository")
		}
	case cleanRead:
		if r.degraded {
			r.degraded = false
			r.logger.Info("pr reconciler: pull request reads recovered; resuming outcome reconciliation")
		}
	}
}
