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

// OpeningLister is the repository slice the opening sweep reads: proposals
// claimed for PR creation but never recorded.
type OpeningLister interface {
	ListStuckOpening(ctx context.Context, limit int) ([]proposal.OpeningPR, error)
}

// OpeningRecorder is the Service slice the opening sweep drives to record a
// pull request recovered from GitHub for a stuck claim.
type OpeningRecorder interface {
	Record(ctx context.Context, in RecordInput) error
}

// PRFailer is the Service slice the opening sweep drives to release a stale
// claim back to 'failed' when no pull request is found for it.
type PRFailer interface {
	Fail(ctx context.Context, id string) error
}

// defaultOpeningGracePeriod bounds how long a pr_state='opening' claim can go
// with no matching GitHub PR, measured from its pr_claimed_at, before the
// sweep releases it back to 'failed'. Ten minutes comfortably exceeds the
// seconds-scale S3-fetch-then-create-PR round trip a healthy claim completes
// in, so it never races an in-flight creation, while still recovering a
// genuinely abandoned claim promptly.
const defaultOpeningGracePeriod = 10 * time.Minute

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
	// BatchLimit caps the open-PR rows (and, for the opening sweep, the stuck
	// 'opening' rows) fetched per pass; <=0 falls back to 50.
	BatchLimit int

	// OpeningLister, BranchFinder, OpeningRecorder, and Failer drive the
	// opening sweep. The sweep is skipped entirely when OpeningLister is nil,
	// so a Reconciler built without them only ever mirrors open-PR outcomes.
	OpeningLister   OpeningLister
	BranchFinder    ports.PullRequestBranchFinder
	OpeningRecorder OpeningRecorder
	Failer          PRFailer
	// OpeningGracePeriod bounds how long a claim with no matching GitHub PR
	// waits — measured from its pr_claimed_at — before FailPR releases it. It
	// never gates the positive branch (recording a PR that is found) — only
	// the decision to declare a claim dead. <=0 falls back to
	// defaultOpeningGracePeriod.
	OpeningGracePeriod time.Duration
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

	openingLister      OpeningLister
	branchFinder       ports.PullRequestBranchFinder
	openingRecorder    OpeningRecorder
	failer             PRFailer
	openingGracePeriod time.Duration

	// degraded records whether the last pass that probed GitHub could not read
	// PR status because of a permission error. Accessed only from the single
	// reconcile goroutine (and tests), so it needs no synchronization.
	degraded bool
}

// NewReconciler constructs a Reconciler, applying defaults for Interval (1m),
// BatchLimit (50), and OpeningGracePeriod (10m).
func NewReconciler(d ReconcilerDeps) *Reconciler {
	if d.Interval <= 0 {
		d.Interval = time.Minute
	}
	if d.BatchLimit <= 0 {
		d.BatchLimit = 50
	}
	if d.OpeningGracePeriod <= 0 {
		d.OpeningGracePeriod = defaultOpeningGracePeriod
	}
	return &Reconciler{
		lister:             d.Lister,
		checker:            d.Checker,
		recorder:           d.Recorder,
		clock:              d.Clock,
		logger:             d.Logger,
		interval:           d.Interval,
		batchLimit:         d.BatchLimit,
		openingLister:      d.OpeningLister,
		branchFinder:       d.BranchFinder,
		openingRecorder:    d.OpeningRecorder,
		failer:             d.Failer,
		openingGracePeriod: d.OpeningGracePeriod,
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

// ReconcileOnce performs a single reconcile pass: it mirrors terminal outcomes
// for open PRs, then sweeps stuck 'opening' claims (skipped when the
// Reconciler was built without opening-sweep dependencies). The degraded
// health signal is updated once per pass from the combined result of both
// loops, since both read the same GitHub token and a permission gap affects
// either one identically.
func (r *Reconciler) ReconcileOnce(ctx context.Context) {
	permissionDenied, cleanRead := r.reconcileOpen(ctx)
	if r.openingLister != nil {
		sweepPermissionDenied, sweepCleanRead := r.sweepOpening(ctx)
		permissionDenied = permissionDenied || sweepPermissionDenied
		cleanRead = cleanRead || sweepCleanRead
	}
	r.updateHealth(permissionDenied, cleanRead)
}

// reconcileOpen mirrors terminal GitHub PR outcomes onto proposals whose
// pr_state is 'open'. It returns whether any PRStatus read hit a permission
// error and whether any read succeeded, for ReconcileOnce to fold into the
// combined health signal.
func (r *Reconciler) reconcileOpen(ctx context.Context) (permissionDenied, cleanRead bool) {
	open, err := r.lister.ListOpenPullRequests(ctx, r.batchLimit)
	if err != nil {
		r.logger.Warn("pr reconciler: list open pull requests", "error", err)
		return false, false
	}
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
	return permissionDenied, cleanRead
}

// sweepOpening resolves proposals claimed for PR creation but never recorded
// (pr_state='opening'). For each one it recomputes the deterministic branch
// name and looks the PR up on GitHub directly: if found — at any age,
// including a claim taken moments ago — the claim is recorded exactly as the
// client-side flow would have, since a found PR is unambiguous regardless of
// how fresh the claim is, using GitHub's own PR-creation time rather than the
// recovery moment. If no PR is found, the claim is released back to 'failed'
// only once its ClaimedAt is at least openingGracePeriod in the past, so a
// healthy in-flight PR creation (an S3 fetch and a GitHub POST, normally
// seconds) is never raced. A row with a nil ClaimedAt — its age is unknown —
// is left alone rather than failed, so an unmeasurable claim can never be
// mistaken for a stale one; each occurrence is logged so a claim stuck this
// way is visible rather than silently retried forever. Each row is handled
// best-effort: one failing row is logged and skipped, and retried next pass.
// The return values feed the reconciler's combined health signal — see
// ReconcileOnce.
func (r *Reconciler) sweepOpening(ctx context.Context) (permissionDenied, cleanRead bool) {
	stuck, err := r.openingLister.ListStuckOpening(ctx, r.batchLimit)
	if err != nil {
		r.logger.Warn("pr reconciler: list stuck opening claims", "error", err)
		return false, false
	}
	now := r.clock.Now()
	for _, o := range stuck {
		branch := BuildBranch(o.ReleaseID, o.NodeID, o.Attempt)
		ref, found, err := r.branchFinder.FindByBranch(ctx, o.Repo, branch)
		if err != nil {
			if errors.Is(err, ports.ErrPermissionDenied) {
				permissionDenied = true
			}
			r.logger.Warn("pr reconciler: find pull request by branch",
				"proposal_id", o.ID, "repo", o.Repo, "branch", branch, "error", err)
			continue
		}
		cleanRead = true
		if found {
			if err := r.openingRecorder.Record(ctx, RecordInput{
				ProposalID: o.ID,
				PrURL:      ref.URL,
				PrNumber:   ref.Number,
				OpenedAt:   ref.CreatedAt,
			}); err != nil {
				r.logger.Warn("pr reconciler: record recovered pull request",
					"proposal_id", o.ID, "repo", o.Repo, "branch", branch, "error", err)
				continue
			}
			r.logger.Info("pr reconciler: recovered a stranded pull request",
				"proposal_id", o.ID, "repo", o.Repo, "branch", branch,
				"pr_url", ref.URL, "pr_number", ref.Number)
			continue
		}
		if o.ClaimedAt == nil {
			// Unknown claim age: never fail a claim we cannot measure, so an
			// unmeasurable row is retried next pass rather than swept. Logged
			// every pass so a claim stuck in this state stays visible instead
			// of silently occupying a batch slot forever.
			r.logger.Warn("pr reconciler: stuck opening claim has no pr_claimed_at; leaving untouched",
				"proposal_id", o.ID, "repo", o.Repo, "branch", branch)
			continue
		}
		age := now.Sub(*o.ClaimedAt)
		if age < r.openingGracePeriod {
			continue
		}
		if err := r.failer.Fail(ctx, o.ID); err != nil {
			r.logger.Warn("pr reconciler: fail stuck opening claim",
				"proposal_id", o.ID, "repo", o.Repo, "branch", branch, "error", err)
			continue
		}
		r.logger.Info("pr reconciler: released a stale opening claim back to failed",
			"proposal_id", o.ID, "repo", o.Repo, "branch", branch, "age", age.String())
	}
	return permissionDenied, cleanRead
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
			r.logger.Error("pr reconciler: cannot read pull request status; PR outcomes and the opening sweep will not reconcile until fixed",
				"action", "grant the GitHub token 'Pull requests: Read' on the target repository")
		}
	case cleanRead:
		if r.degraded {
			r.degraded = false
			r.logger.Info("pr reconciler: pull request reads recovered; resuming outcome and opening-sweep reconciliation")
		}
	}
}
