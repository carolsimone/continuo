// Package shadowverify holds the reconciler that resolves fix proposals whose
// correctness shadow releases are still judging.
//
// A fix cannot be judged by reading it back, only by running it, so the driver
// that produces one ends by submitting a shadow release for every service it
// edited — a real release that runs the full parse -> candidate-schema ->
// validation pipeline but stops at "validated" instead of promoting — and
// leaves the attempt in 'verifying'. One attempt addresses a rejected release's
// whole failing set, so one proposal waits on all of those shadow releases at
// once, and is resolved as a unit: it becomes a fix a human can review only
// when every one of them validated, and a single rejection fails the whole
// attempt.
//
// This package is the other half of that handoff: a polling loop that reads
// each waiting attempt's releases and either finalizes it as a proposal, or
// records why it failed and starts the next attempt over the same failing set —
// the whole set, because the attempt's edits stand or fall together, and the
// errors the releases reported are what the next attempt is shown.
//
// Those releases share one global queue and run one at a time, so an attempt's
// shadow releases are answered one after another rather than together. Every
// wait here is therefore bounded per release, from the moment that release
// itself started running: queueing spends nothing, and an attempt is never
// timed out for the accumulated wall-clock of several healthy releases.
package shadowverify

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
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
// reach a verdict, counted from the moment that release started running.
// Twenty minutes comfortably exceeds the compile+parse+validate pipeline a
// healthy shadow release completes in, while still ending an attempt whose
// release wedges rather than pinning the proposal in 'verifying' forever.
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
	// from the moment its shadow release left the release queue and started
	// running — or, when no verdict can be read at all, from the moment the
	// attempt was recorded. <=0 falls back to defaultTimeout.
	Timeout time.Duration
}

// Reconciler drives every proposal awaiting shadow verification to a terminal
// outcome. Rows are handled best-effort and sequentially: one row that cannot
// be resolved is logged and skipped, so no row's failure ends the pass, and it
// is retried next pass. Slowness is not isolated the same way — a rejected row
// runs the next attempt's whole fix pipeline, model call included, inline in
// the pass — so one slow retry delays every row behind it until the next tick.
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

// resolve routes one waiting attempt on the current verdicts of every shadow
// release judging it, read once each per pass.
//
// The attempt is one proposal over one set of edits, so the verdicts are
// combined rather than acted on individually: one rejection fails it (the edits
// cannot be split, and the errors of every rejected release together are the
// evidence the next attempt reads), otherwise every release must have reached a
// terminal verdict before it can be announced.
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
	nodeErrors := map[string]string{}
	anyRejected, allTerminal := false, true
	var runningSince []time.Time
	for _, ver := range verificationsOf(v) {
		verdict, err := r.releases.Verdict(ctx, ver.RunID)
		if err != nil {
			r.logger.Warn("shadow verify: read shadow release verdict",
				"proposal_id", v.ID, "shadow_release", ver.RunID, "error", err)
			r.failIfUnreadableTooLong(ctx, v)
			return
		}
		switch {
		case verdict.Terminal && !verdict.Validated:
			anyRejected = true
			for node, msg := range verdict.NodeErrors {
				nodeErrors[node] = msg
			}
		case !verdict.Terminal:
			allTerminal = false
			// Each release carries its own budget, spent from its own
			// activation. A release still queued has not started, so it has
			// spent nothing and is not collected here.
			if !verdict.ActivatedAt.IsZero() {
				runningSince = append(runningSince, verdict.ActivatedAt)
			}
		}
	}
	switch {
	case anyRejected:
		r.rejected(ctx, v, verifyError(v, nodeErrors))
	case allTerminal:
		r.verified(ctx, v)
	default:
		r.failIfVerificationExpired(ctx, v, runningSince)
	}
}

// verificationsOf is the set of verification runs judging one attempt. A row
// that recorded per-service verifications names them all; a row that posted a
// single verification run names only VerificationRunID, and is that one
// verification.
func verificationsOf(v proposal.View) []proposal.Verification {
	if len(v.Verifications) > 0 {
		return v.Verifications
	}
	return []proposal.Verification{{RunID: v.VerificationRunID}}
}

// verified finalizes an attempt every shadow release validated: the proposal
// moves to 'proposed' — carrying each of its nodes with it — and the
// remediation.proposed:v1 event that surfaces it for human review is enqueued,
// both in one transaction so the row and the announcement cannot disagree. The
// status transition is a compare-and-set, so a repeated pass over an
// already-finalized row writes nothing and emits nothing.
func (r *Reconciler) verified(ctx context.Context, v proposal.View) {
	u := r.newUoW()
	if err := u.Begin(ctx); err != nil {
		r.logger.Warn("shadow verify: begin (mark verified)", "proposal_id", v.ID, "error", err)
		return
	}
	defer func() { _ = u.Rollback() }()

	hit, err := u.ProposalRepo().MarkVerified(ctx, v.ID, v.Verifications)
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
	if err := handlers.Enqueue(ctx, u, r.clock, proposedEvent(v), v.SourceResolved, uuid.Nil); err != nil {
		r.logger.Warn("shadow verify: enqueue proposed event", "proposal_id", v.ID, "error", err)
		return
	}
	if err := u.Commit(); err != nil {
		r.logger.Warn("shadow verify: commit (mark verified)", "proposal_id", v.ID, "error", err)
		return
	}
	r.logger.Info("shadow verify: every shadow release validated the fix; proposal is ready for review",
		"proposal_id", v.ID, "nodes", len(v.ResolvedNodeIDs), "release", v.ReleaseID,
		"shadow_releases", len(verificationsOf(v)), "attempt", v.Attempt)
}

// rejected finalizes an attempt whose fix did not hold up, recording verifyErr
// as the evidence the next attempt is shown, and then starts that next attempt
// over the attempt's whole failing set. The order matters: the failed row must
// be committed first, because the driver counts terminal attempts to decide
// both the next attempt's number and whether the cap has been reached. The
// transition is a compare-and-set, so only the pass that actually finalizes the
// row starts a next attempt — a repeated pass finds the row already terminal
// and does nothing.
func (r *Reconciler) rejected(ctx context.Context, v proposal.View, verifyErr string) {
	u := r.newUoW()
	if err := u.Begin(ctx); err != nil {
		r.logger.Warn("shadow verify: begin (mark verify failed)", "proposal_id", v.ID, "error", err)
		return
	}
	defer func() { _ = u.Rollback() }()

	hit, err := u.ProposalRepo().MarkVerifyFailed(ctx, v.ID, verifyErr, v.Verifications)
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
	r.logger.Info("shadow verify: a shadow release did not validate the fix",
		"proposal_id", v.ID, "nodes", len(v.ResolvedNodeIDs), "release", v.ReleaseID,
		"shadow_releases", len(verificationsOf(v)), "attempt", v.Attempt, "verify_error", verifyErr)
	r.retry(ctx, v)
}

// failIfVerificationExpired ends an attempt one of whose shadow releases has
// been RUNNING longer than the verification budget. runningSince carries the
// activation of every release that has started and not yet reached a verdict;
// inside the budget the row is left exactly as it is, so a release still
// working is never cut short.
//
// The budget belongs to each release individually, measured from when that
// release itself left the queue. Two things follow, and both matter.
//
// A shadow release joins the same global FIFO queue as every other release and
// only one release runs at a time, so measuring from when the attempt was
// recorded would let a backlog fail an attempt whose release never ran — and do
// the same to every retry behind it, spending the whole per-failure budget on
// releases that were never given a chance to answer. A release still queued (no
// activation) is therefore left alone however long ago the attempt was
// recorded: it has not started, so it has not spent anything.
//
// For the same reason the budget is not shared across an attempt's releases.
// They run one after another rather than side by side, so the span from the
// first activation to the last verdict grows with the number of services the
// attempt edited, while no single release runs any longer than it otherwise
// would. Charging that whole span to one budget would fail an attempt whose
// releases are all healthy, purely for having touched more than one service —
// and a release that has already answered is not waiting on anything, so it
// spends nothing further while the next one runs.
//
// The wait is still bounded: a queue that never advances is a stopped pipeline,
// and a release that runs and then wedges is caught by its own budget from the
// moment it started.
func (r *Reconciler) failIfVerificationExpired(ctx context.Context, v proposal.View, runningSince []time.Time) {
	now := r.clock.Now()
	for _, since := range runningSince {
		if now.Sub(since) < r.timeout {
			continue
		}
		r.rejected(ctx, v, timedOutError)
		return
	}
}

// failIfUnreadableTooLong ends an attempt one of whose shadow releases has not
// produced a READABLE verdict inside the budget. Here the budget runs from when
// the attempt was recorded, because no verdict could be read there is nothing
// else to measure from — and this is the path that must stay bounded
// regardless: a release id release-controller will never know reads exactly
// like a release controller that is briefly unreachable, so without this an
// attempt whose submission was lost would sit in 'verifying' for as long as the
// row exists.
func (r *Reconciler) failIfUnreadableTooLong(ctx context.Context, v proposal.View) {
	if r.clock.Now().Sub(v.CreatedAt) < r.timeout {
		return
	}
	r.rejected(ctx, v, timedOutError)
}

// retry starts the attempt that follows a failed verification, from the trigger
// stored on the attempt's own row, replayed in full.
//
// A shadow release judges every node the attempt addressed at once, so its
// rejection may well name only some of them. The retry still covers the whole
// failing set, because the attempt is one proposal over one set of edits and it
// failed as a unit: the edits for the nodes that did pass were never offered to
// anyone, and are discarded with the failed attempt. Retrying only what still
// fails would therefore end in a pull request carrying fixes for those nodes
// alone, silently dropping the ones an earlier attempt had already got right.
// Which nodes failed is not lost — it is recorded as the attempt's verify error,
// which is exactly the evidence the next attempt is shown.
//
// The rebuilt trigger is given a dedup identity of its own. The driver opens
// by asking whether this trigger was already handled, keyed on the Redis
// message id it carries, and the first attempt's own transaction already
// claimed the message that started it — so replaying the stored payload
// verbatim would report "already processed" and return having done nothing,
// with no attempt, no row, and no error anywhere to show for it. The row of the
// attempt being retried names this retry uniquely (there is exactly one row per
// attempt), so it is both a fresh key for each attempt and a stable one for
// repeated passes over the same attempt. The upstream outbox id is dropped for
// the same reason: it is the second dedup axis, and the original value would
// collide on it just as the message id would.
//
// A next attempt that fails to start is not retried here: the row it would
// follow is already terminal, so the following pass no longer lists it. The
// failure stands as the recorded outcome for the release — but the driver has
// by then committed an in-flight row for that attempt, which abandonInFlight
// closes out so the failure is what the release's last row actually says.
func (r *Reconciler) retry(ctx context.Context, v proposal.View) {
	if len(v.TriggerPayload) == 0 {
		r.logger.Warn("shadow verify: attempt has no stored trigger; no further attempt can be started",
			"proposal_id", v.ID, "release", v.ReleaseID)
		return
	}
	t, err := r.decode(v.TriggerPayload)
	if err != nil {
		r.logger.Error("shadow verify: stored trigger cannot be decoded; no further attempt can be started",
			"proposal_id", v.ID, "release", v.ReleaseID, "error", err)
		return
	}
	t.MessageID = retryMessageIDPrefix + v.ID
	t.OutboxEntryID = nil
	if err := r.propose(ctx, t); err != nil {
		r.logger.Error("shadow verify: could not start the next fix attempt",
			"proposal_id", v.ID, "release", v.ReleaseID, "error", err)
		r.abandonInFlight(ctx, v, err)
	}
}

// stillFailing is the attempt's own nodes that the rejection still reports an
// error for, in the order the attempt recorded them (sorted). Nodes the
// rejection names that this attempt never addressed are not returned: the
// trigger carries no failure for them, so there is nothing to re-fix.
func stillFailing(resolved []string, nodeErrors map[string]string) []string {
	still := make([]string, 0, len(resolved))
	for _, id := range resolved {
		if nodeErrors[id] != "" {
			still = append(still, id)
		}
	}
	return still
}

// abandonInFlight closes out the in-flight row a next attempt left behind when
// it could not start. It names the release the attempt belongs to, so an
// attempt in flight for a DIFFERENT release is neither failed nor charged an
// attempt for this one. One attempt covers a release's whole failing set, so
// the release alone identifies the row unambiguously.
//
// The driver commits a 'generating' row of its own, in its own transaction,
// immediately before calling the model — that row is what the release page
// renders as "Generating fix…" for the nodes it covers. On the stream
// consumer's path a driver error leaves the message unacknowledged, and the
// redelivery reuses that row. Nothing redelivers this attempt: it was started
// by a release's verdict, not by a consumed message. Left alone the row would
// report a fix as still being generated for as long as the database keeps it,
// and no sweep exists to notice. It is failed here instead, carrying why, so
// the release's last row says what actually happened.
func (r *Reconciler) abandonInFlight(ctx context.Context, v proposal.View, cause error) {
	u := r.newUoW()
	if err := u.Begin(ctx); err != nil {
		r.logger.Warn("shadow verify: begin (abandon in-flight attempt)", "proposal_id", v.ID, "error", err)
		return
	}
	defer func() { _ = u.Rollback() }()

	n, err := u.ProposalRepo().FailGenerating(ctx, v.ReleaseID,
		"the next fix attempt could not be started: "+cause.Error())
	if err != nil {
		r.logger.Warn("shadow verify: abandon in-flight attempt",
			"proposal_id", v.ID, "release", v.ReleaseID, "error", err)
		return
	}
	if n == 0 {
		return
	}
	if err := u.Commit(); err != nil {
		r.logger.Warn("shadow verify: commit (abandon in-flight attempt)", "proposal_id", v.ID, "error", err)
		return
	}
	r.logger.Info("shadow verify: closed out the attempt that could not be started",
		"proposal_id", v.ID, "release", v.ReleaseID, "rows", n)
}

// proposedEvent projects a recorded attempt back onto the aggregate the outbox
// event is built from: the failure it addressed, every node it resolved, every
// file it changed, and the shadow releases that judged it. The verification
// added no content of its own — it decided only that the attempt may be shown —
// so the only thing it changes is the status, on the attempt and on each of the
// nodes that were waiting for it.
func proposedEvent(v proposal.View) proposal.Proposal {
	return proposal.Proposal{
		Source:            v.Source,
		ReleaseID:         v.ReleaseID,
		RemediationRound:  v.RemediationRound,
		NodeID:            v.NodeID,
		ResolvedNodeIDs:   append([]string(nil), v.ResolvedNodeIDs...),
		ErrorSignature:    v.ErrorSignature,
		Attempt:           v.Attempt,
		Status:            proposal.StatusProposed,
		NodeOutcomes:      proposedOutcomes(v.NodeOutcomes),
		Verifications:     append([]proposal.Verification(nil), v.Verifications...),
		VerificationRunID: v.VerificationRunID,
		Confidence:        v.Confidence,
		Rationale:         v.Rationale,
		ProposedSQLURI:    v.ProposedSQLURI,
		DiffURI:           v.DiffURI,
		Edits:             append([]proposal.FileEdit(nil), v.Edits...),
		Model:             v.Model,
	}
}

// proposedOutcomes finalizes the per-node outcomes of a verified attempt: a
// node that was waiting on a shadow release is now proposed, while a node the
// attempt had already settled — skipped for want of source, or failed while
// being fixed — keeps the outcome and reason it was recorded with, since no
// release judged it.
func proposedOutcomes(outcomes map[string]proposal.NodeOutcome) map[string]proposal.NodeOutcome {
	if len(outcomes) == 0 {
		return nil
	}
	out := make(map[string]proposal.NodeOutcome, len(outcomes))
	for node, o := range outcomes {
		if o.Status == proposal.StatusVerifying {
			o.Status = proposal.StatusProposed
		}
		out[node] = o
	}
	return out
}

// verifyError is the text recorded on a rejected attempt, and the evidence the
// next attempt is shown. The errors of the nodes this attempt fixed come first,
// named and in a stable order; when every one of them passed and other nodes
// did not — a fix that broke something downstream of itself — every failing
// node is named instead, so the reason describes what actually failed. A
// rejection carrying no per-node error at all still records which release
// rejected it, so the attempt never ends with a blank reason.
func verifyError(v proposal.View, nodeErrors map[string]string) string {
	if own := namedErrors(nodeErrors, stillFailing(v.ResolvedNodeIDs, nodeErrors)); own != "" {
		return own
	}
	if all := namedErrors(nodeErrors, sortedNodes(nodeErrors)); all != "" {
		return all
	}
	return fmt.Sprintf("shadow release %s was rejected without a per-node error",
		verificationsOf(v)[0].RunID)
}

// namedErrors renders the errors of the given nodes as "node: message" joined
// by "; ", in the order the nodes are given. It is empty when no node is given.
func namedErrors(nodeErrors map[string]string, nodes []string) string {
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		parts = append(parts, node+": "+nodeErrors[node])
	}
	return strings.Join(parts, "; ")
}

// sortedNodes is every node the rejection reports an error for, in a stable
// order, so the recorded reason does not depend on map iteration order.
func sortedNodes(nodeErrors map[string]string) []string {
	nodes := make([]string, 0, len(nodeErrors))
	for node := range nodeErrors {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}
