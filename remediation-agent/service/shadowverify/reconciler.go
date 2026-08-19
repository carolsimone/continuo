// Package shadowverify holds the reconciler that resolves fix proposals whose
// correctness a shadow release is still judging.
//
// A fix for a python node cannot be judged by reading it back, only by running
// it, so the fixer that produces one ends by submitting a shadow release — a
// real release that runs the full parse -> candidate-schema -> validation
// pipeline but stops at "validated" instead of promoting — and leaves the
// attempt in 'verifying'. This package is the other half: a polling loop that
// reads each waiting attempt's release, and either finalizes the attempt as a
// fix a human can review, or records why it failed and starts the next one.
package shadowverify

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/handlers"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
	"github.com/carolsimone/continuo/remediation-agent/service/uow"
)

// VerifyingLister is the repository slice this reconciler reads: the proposals
// whose fix a shadow release is still judging, oldest first. It is a narrower
// view of the ListVerifying method the proposal repository already declares,
// so a consumer that only needs this one slice does not have to depend on the
// full repository.
type VerifyingLister interface {
	ListVerifying(ctx context.Context) ([]proposal.View, error)
}

// TriggerDecoder rebuilds the inbound trigger an attempt was produced from,
// out of the raw payload stored on its row. The payload's wire shape is a
// transport concern, so the composition root supplies the same decoder the
// stream consumer uses rather than this package parsing the bytes itself, and
// a next attempt is therefore driven by exactly the trigger the first one was.
type TriggerDecoder func(raw []byte) (handlers.Trigger, error)

// FixProposer starts one fix attempt for a trigger. It is the driver that
// counts prior attempts, enforces the attempt cap, and records the outcome.
type FixProposer func(ctx context.Context, t handlers.Trigger) error

// defaultInterval paces the poll loop. Fifteen seconds keeps a verified fix at
// most one tick behind the release that proved it, while costing
// release-controller one cheap read per waiting attempt.
const defaultInterval = 15 * time.Second

// defaultTimeout bounds how long an attempt may wait for its shadow release to
// reach a verdict. Twenty minutes comfortably exceeds the compile+parse+
// validate pipeline a healthy shadow release completes in, while still ending
// an attempt whose release wedges rather than pinning the proposal in
// 'verifying' forever.
const defaultTimeout = 20 * time.Minute

// timedOutError is the reason recorded on an attempt whose shadow release
// never reached a verdict inside the budget.
const timedOutError = "shadow verification timed out"

// retryMessageIDPrefix namespaces the dedup identity minted for the attempt
// that follows a failed verification, keeping it distinct from any Redis
// Stream message id.
const retryMessageIDPrefix = "shadow-verify:"

// Deps holds every collaborator the Reconciler needs, all behind ports or
// narrow interfaces so no adapter or infrastructure package is imported.
type Deps struct {
	Lister   VerifyingLister
	Releases ports.ReleaseGateway
	NewUoW   func() uow.UnitOfWork
	Decode   TriggerDecoder
	Propose  FixProposer
	Clock    ports.Clock
	Logger   *slog.Logger
	// Interval between passes; <=0 falls back to defaultInterval.
	Interval time.Duration
	// Timeout bounds how long an attempt waits for a terminal verdict, measured
	// from the moment the attempt was recorded; <=0 falls back to defaultTimeout.
	Timeout time.Duration
}

// Reconciler drives every proposal awaiting shadow verification to a terminal
// outcome. Rows are handled best-effort: one row that cannot be resolved is
// logged and skipped so it never blocks the rest, and is retried next pass.
type Reconciler struct {
	lister   VerifyingLister
	releases ports.ReleaseGateway
	newUoW   func() uow.UnitOfWork
	decode   TriggerDecoder
	propose  FixProposer
	clock    ports.Clock
	logger   *slog.Logger
	interval time.Duration
	timeout  time.Duration
}

// New constructs a Reconciler, applying defaults for Interval and Timeout.
func New(d Deps) *Reconciler {
	if d.Interval <= 0 {
		d.Interval = defaultInterval
	}
	if d.Timeout <= 0 {
		d.Timeout = defaultTimeout
	}
	return &Reconciler{
		lister:   d.Lister,
		releases: d.Releases,
		newUoW:   d.NewUoW,
		decode:   d.Decode,
		propose:  d.Propose,
		clock:    d.Clock,
		logger:   d.Logger,
		interval: d.Interval,
		timeout:  d.Timeout,
	}
}

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

// ReconcileOnce performs a single pass over the proposals awaiting a shadow
// verdict. A listing that fails ends the pass without touching anything; the
// next pass reads again.
func (r *Reconciler) ReconcileOnce(ctx context.Context) {
	rows, err := r.lister.ListVerifying(ctx)
	if err != nil {
		r.logger.Warn("shadow verify: list verifying proposals", "error", err)
		return
	}
	for _, v := range rows {
		r.resolve(ctx, v)
	}
}

// resolve routes one waiting attempt on its shadow release's current verdict.
//
// A verdict that cannot be read is deliberately NOT treated as a rejection.
// The gateway reports a release-controller that is briefly unreachable and a
// release id it will never know with the same error, so counting either as a
// failed attempt would let one outage burn the per-failure attempt budget on
// fixes that were never judged. The read is retried next pass instead — and
// the wait is still bounded, because the same timeout that ends a shadow
// release which never reaches a verdict also ends one whose verdict never
// becomes readable.
func (r *Reconciler) resolve(ctx context.Context, v proposal.View) {
	verdict, err := r.releases.Verdict(ctx, v.ShadowReleaseID)
	if err != nil {
		r.logger.Warn("shadow verify: read shadow release verdict",
			"proposal_id", v.ID, "shadow_release", v.ShadowReleaseID, "error", err)
		r.failIfExpired(ctx, v)
		return
	}
	switch {
	case verdict.Terminal && verdict.Validated:
		r.verified(ctx, v)
	case verdict.Terminal:
		r.rejected(ctx, v, verifyError(v, verdict))
	default:
		r.failIfExpired(ctx, v)
	}
}

// verified finalizes an attempt whose shadow release validated the fix: the
// proposal moves to 'proposed' and the remediation.proposed:v1 event that
// surfaces it for human review is enqueued, both in one transaction so the
// row and the announcement cannot disagree. The status transition is a
// compare-and-set, so a repeated pass over an already-finalized row writes
// nothing and emits nothing.
func (r *Reconciler) verified(ctx context.Context, v proposal.View) {
	u := r.newUoW()
	if err := u.Begin(ctx); err != nil {
		r.logger.Warn("shadow verify: begin (mark verified)", "proposal_id", v.ID, "error", err)
		return
	}
	defer func() { _ = u.Rollback() }()

	hit, err := u.ProposalRepo().MarkVerified(ctx, v.ID)
	if err != nil {
		r.logger.Warn("shadow verify: mark verified", "proposal_id", v.ID, "error", err)
		return
	}
	if !hit {
		return
	}
	// The event is built from the attempt exactly as it was recorded; the
	// verification decided only whether it may be shown, not what it says.
	// There is no inbound message behind this write — the reconciler acts on a
	// release's verdict, not on a consumed trigger — so the outbox entry
	// carries no message_processing provenance.
	if err := handlers.Enqueue(ctx, u, r.clock, proposedEvent(v), "", v.SourceResolved, uuid.Nil); err != nil {
		r.logger.Warn("shadow verify: enqueue proposed event", "proposal_id", v.ID, "error", err)
		return
	}
	if err := u.Commit(); err != nil {
		r.logger.Warn("shadow verify: commit (mark verified)", "proposal_id", v.ID, "error", err)
		return
	}
	r.logger.Info("shadow verify: shadow release validated the fix; proposal is ready for review",
		"proposal_id", v.ID, "node", v.NodeID, "release", v.ReleaseID,
		"shadow_release", v.ShadowReleaseID, "attempt", v.Attempt)
}

// rejected finalizes an attempt whose fix did not hold up, recording verifyErr
// as the evidence the next attempt is shown, and then starts that next
// attempt. The order matters: the failed row must be committed first, because
// the driver counts terminal attempts to decide both the next attempt's number
// and whether the cap has been reached. The transition is a compare-and-set,
// so only the pass that actually finalizes the row starts a next attempt —
// a repeated pass finds the row already terminal and does nothing.
func (r *Reconciler) rejected(ctx context.Context, v proposal.View, verifyErr string) {
	u := r.newUoW()
	if err := u.Begin(ctx); err != nil {
		r.logger.Warn("shadow verify: begin (mark verify failed)", "proposal_id", v.ID, "error", err)
		return
	}
	defer func() { _ = u.Rollback() }()

	hit, err := u.ProposalRepo().MarkVerifyFailed(ctx, v.ID, verifyErr)
	if err != nil {
		r.logger.Warn("shadow verify: mark verify failed", "proposal_id", v.ID, "error", err)
		return
	}
	if !hit {
		return
	}
	if err := u.Commit(); err != nil {
		r.logger.Warn("shadow verify: commit (mark verify failed)", "proposal_id", v.ID, "error", err)
		return
	}
	r.logger.Info("shadow verify: shadow release did not validate the fix",
		"proposal_id", v.ID, "node", v.NodeID, "release", v.ReleaseID,
		"shadow_release", v.ShadowReleaseID, "attempt", v.Attempt, "verify_error", verifyErr)
	r.retry(ctx, v)
}

// failIfExpired ends an attempt whose shadow release has had longer than the
// verification budget — measured from when the attempt was recorded — to
// reach a readable verdict. Inside the budget the row is left exactly as it
// is, so a release still working is never cut short.
func (r *Reconciler) failIfExpired(ctx context.Context, v proposal.View) {
	if r.clock.Now().Sub(v.CreatedAt) < r.timeout {
		return
	}
	r.rejected(ctx, v, timedOutError)
}

// retry starts the attempt that follows a failed verification, from the
// trigger stored on the attempt's own row.
//
// The rebuilt trigger is given a dedup identity of its own. The driver opens
// by asking whether this trigger was already handled, keyed on the Redis
// message id it carries, and the first attempt's own transaction already
// claimed the message that started it — so replaying the stored payload
// verbatim would report "already processed" and return having done nothing,
// with no attempt, no row, and no error anywhere to show for it. The shadow
// release that judged the attempt being retried names this retry uniquely
// (there is exactly one shadow release per attempt), so it is both a fresh key
// for each attempt and a stable one for repeated passes over the same attempt.
// The upstream outbox id is dropped for the same reason: it is the second
// dedup axis, and the original value would collide on it just as the message
// id would.
//
// A next attempt that fails to start is logged rather than retried here: the
// row it would follow is already terminal, so the following pass no longer
// lists it. The failure stands as the recorded outcome for the node.
func (r *Reconciler) retry(ctx context.Context, v proposal.View) {
	if len(v.TriggerPayload) == 0 {
		r.logger.Warn("shadow verify: attempt has no stored trigger; no further attempt can be started",
			"proposal_id", v.ID, "node", v.NodeID, "release", v.ReleaseID)
		return
	}
	t, err := r.decode(v.TriggerPayload)
	if err != nil {
		r.logger.Error("shadow verify: stored trigger cannot be decoded; no further attempt can be started",
			"proposal_id", v.ID, "node", v.NodeID, "release", v.ReleaseID, "error", err)
		return
	}
	t.MessageID = retryMessageIDPrefix + v.ShadowReleaseID
	t.OutboxEntryID = nil
	if err := r.propose(ctx, t); err != nil {
		r.logger.Error("shadow verify: could not start the next fix attempt",
			"proposal_id", v.ID, "node", v.NodeID, "release", v.ReleaseID, "error", err)
	}
}

// proposedEvent projects a recorded attempt back onto the aggregate the
// outbox event is built from. Only the fields that describe the proposed fix
// are carried; the verification added no content of its own.
func proposedEvent(v proposal.View) proposal.Proposal {
	return proposal.Proposal{
		Source:         v.Source,
		ReleaseID:      v.ReleaseID,
		NodeID:         v.NodeID,
		ErrorSignature: v.ErrorSignature,
		Attempt:        v.Attempt,
		Status:         proposal.StatusProposed,
		Confidence:     v.Confidence,
		Rationale:      v.Rationale,
		ProposedSQLURI: v.ProposedSQLURI,
		DiffURI:        v.DiffURI,
		Model:          v.Model,
	}
}

// verifyError is the text recorded on a rejected attempt, and the evidence the
// next attempt is shown. The fixed node's own error is preferred; when the
// fixed node passed and other nodes did not — a fix that broke something
// downstream of itself — every failing node is named instead, in a stable
// order, so the reason describes what actually failed. A rejection carrying no
// per-node error at all still records which release rejected it, so the
// attempt never ends with a blank reason.
func verifyError(v proposal.View, verdict ports.ShadowVerdict) string {
	if msg := verdict.NodeErrors[v.NodeID]; msg != "" {
		return msg
	}
	if len(verdict.NodeErrors) > 0 {
		nodes := make([]string, 0, len(verdict.NodeErrors))
		for node := range verdict.NodeErrors {
			nodes = append(nodes, node)
		}
		sort.Strings(nodes)
		parts := make([]string, 0, len(nodes))
		for _, node := range nodes {
			parts = append(parts, node+": "+verdict.NodeErrors[node])
		}
		return strings.Join(parts, "; ")
	}
	return fmt.Sprintf("shadow release %s was rejected without a per-node error", v.ShadowReleaseID)
}
