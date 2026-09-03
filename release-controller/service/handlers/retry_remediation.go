package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/google/uuid"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// Refusals RetryRemediation reports; the HTTP layer maps each to a status.
var (
	ErrReleaseNotFound = errors.New("release not found")
	ErrNotHealable     = errors.New("rejection reason is not healable")
	ErrNotRetryable    = errors.New("release was rejected without a stored rejection payload")
	// ErrRetryInProgress refuses a retry when the current remediation round has
	// not yet produced a proposal row: either its trigger has not reached
	// agent-remediation yet, or the classifier dropped it. Either way, spending
	// another round now would race the round already in flight.
	ErrRetryInProgress = errors.New("a remediation round is in progress")
	// ErrProposalReaderUnavailable wraps a failure of the gRPC read to
	// agent-remediation, distinct from a legitimate "an attempt is open" answer.
	ErrProposalReaderUnavailable = errors.New("could not read remediation proposals")
)

// ErrProposalOpen refuses a retry because an attempt is still in flight or a
// fix is already proposed; it names what to look at instead.
type ErrProposalOpen struct {
	ProposalID string
	PRURL      string
}

func (e ErrProposalOpen) Error() string { return "a proposal is already open for this release" }

// healableRejectReasons are the rejections the classifier turns into heal
// triggers. Every other reason is dropped on the normal path too, so a retry
// of it would only spend a round for nothing.
var healableRejectReasons = map[string]bool{
	"compile_failed":    true,
	"seed_build_failed": true,
	"validation_failed": true,
	"duplicate_table":   true,
}

// RetryRemediationResult is the round the retry started.
type RetryRemediationResult struct {
	ReleaseID        string
	RemediationRound int
}

// RetryRemediation starts another remediation round on a rejected candidate:
// it replays the rejection the candidate stored, tagged with the new round,
// on remediation.retry_requested:v1. It refuses when the id names a
// verification run rather than a candidate (a verification is not a release,
// so it is reported the same as an unknown id), when the candidate is not
// rejected, when its reason is not healable, when the round cap is reached,
// when the candidate has no stored rejection, when the current round has not
// yet produced a proposal row (a second click before the first has landed),
// or when an attempt is still in flight or a fix is already proposed — the
// caller should look at that instead of retrying.
func RetryRemediation(ctx context.Context, deps *Deps, releaseID string) (RetryRemediationResult, error) {
	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return RetryRemediationResult{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	r, err := u.RunRepo().Load(ctx, releaseID)
	if err != nil {
		return RetryRemediationResult{}, fmt.Errorf("load release: %w", err)
	}
	if r == nil {
		return RetryRemediationResult{}, ErrReleaseNotFound
	}
	if r.Status() != pipeline.StatusRejected {
		return RetryRemediationResult{}, pipeline.ErrNotRejected
	}
	if r.Kind() != pipeline.KindCandidate {
		return RetryRemediationResult{}, ErrReleaseNotFound
	}
	if !healableRejectReasons[r.FailReason()] {
		return RetryRemediationResult{}, ErrNotHealable
	}
	if len(r.RejectionPayload()) == 0 {
		return RetryRemediationResult{}, ErrNotRetryable
	}
	if r.RemediationRound() >= pipeline.MaxRemediationRounds {
		return RetryRemediationResult{}, pipeline.ErrRoundsExhausted
	}

	proposals, err := deps.Proposals.ListProposalsForRelease(ctx, releaseID)
	if err != nil {
		return RetryRemediationResult{}, fmt.Errorf("%w: %v", ErrProposalReaderUnavailable, err)
	}
	current := currentRoundProposals(proposals, r.RemediationRound())
	if len(current) == 0 {
		return RetryRemediationResult{}, ErrRetryInProgress
	}
	if open, ok := openProposal(current, proposals); ok {
		return RetryRemediationResult{}, open
	}

	now := deps.Clock.Now()
	round, err := r.StartRemediationRound(now)
	if err != nil {
		return RetryRemediationResult{}, err
	}
	if err := u.RunRepo().Save(ctx, r); err != nil {
		return RetryRemediationResult{}, fmt.Errorf("save release: %w", err)
	}

	var body map[string]any
	if err := json.Unmarshal(r.RejectionPayload(), &body); err != nil {
		return RetryRemediationResult{}, fmt.Errorf("decode stored rejection: %w", err)
	}
	body["remediation_round"] = round
	payload, err := json.Marshal(body)
	if err != nil {
		return RetryRemediationResult{}, fmt.Errorf("marshal retry payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte("remediation-retry:"+releaseID+":"+strconv.Itoa(round))),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(releaseID),
		EventType:     "remediation_retry_requested",
		Payload:       payload,
		StreamName:    streams.RemediationRetryRequestedV1,
		Status:        "pending",
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
		CreatedAt:     now,
	}); err != nil {
		return RetryRemediationResult{}, fmt.Errorf("outbox insert: %w", err)
	}
	if err := u.Commit(); err != nil {
		return RetryRemediationResult{}, fmt.Errorf("commit: %w", err)
	}
	deps.Logger.Info("remediation retry started", "release", releaseID, "round", round)
	return RetryRemediationResult{ReleaseID: releaseID, RemediationRound: round}, nil
}

// effectiveRound is the remediation round a proposal belongs to: its own
// RemediationRound field, or 1 for a proposal recorded before that field
// existed.
func effectiveRound(p ports.ProposalSummary) int {
	if p.RemediationRound == 0 {
		return 1
	}
	return p.RemediationRound
}

// currentRoundProposals filters ps to the ones belonging to round.
func currentRoundProposals(ps []ports.ProposalSummary, round int) []ports.ProposalSummary {
	out := make([]ports.ProposalSummary, 0, len(ps))
	for _, p := range ps {
		if effectiveRound(p) == round {
			out = append(out, p)
		}
	}
	return out
}

// openProposal reports the attempt that makes a retry pointless: one from the
// current round still in flight or already proposed, or — checked across
// every round — one whose PR is being opened, open, or merged. Node id order
// is sorted before picking so the answer is deterministic when several nodes
// qualify.
func openProposal(current, all []ports.ProposalSummary) (ErrProposalOpen, bool) {
	if open, ok := openInCurrentRound(current); ok {
		return open, true
	}
	return openPRAcrossRounds(all)
}

// openInCurrentRound reports whether any node's latest attempt in the current
// round is still generating or verifying, or is proposed with a fix that could
// still land. A batched attempt opens one pull request per owning service, so
// the decision weighs all of them: the attempt is a dead end — closed without
// landing a fix — only once every owning service's pull request has been
// rejected. If any owning service has no pull request yet, or one in any
// non-rejected state, a fix may still land and a new round would duplicate it.
// Only each node's latest attempt is consulted; an earlier attempt superseded
// by a later one on the same node does not count.
func openInCurrentRound(current []ports.ProposalSummary) (ErrProposalOpen, bool) {
	latest := map[string]ports.ProposalSummary{}
	for _, p := range current {
		if cur, ok := latest[p.NodeID]; !ok || p.Attempt > cur.Attempt {
			latest[p.NodeID] = p
		}
	}
	for _, id := range sortedNodeIDs(latest) {
		p := latest[id]
		switch p.Status {
		case "generating", "verifying":
			return ErrProposalOpen{ProposalID: p.ID, PRURL: p.PRURL}, true
		case "proposed":
			if isDeadEnd(p) {
				continue
			}
			return ErrProposalOpen{ProposalID: p.ID, PRURL: representativePRURL(p)}, true
		}
	}
	return ErrProposalOpen{}, false
}

// openPRAcrossRounds reports whether any proposal from any round — not just the
// current one — has a pull request that is opening, open, or merged: a fix
// already out for human review makes a new round redundant regardless of which
// round proposed it. A batched attempt's per-service pull requests are all
// weighed, so one service's still-open PR blocks even when a sibling service's
// PR was rejected.
func openPRAcrossRounds(all []ports.ProposalSummary) (ErrProposalOpen, bool) {
	byNode := map[string]ports.ProposalSummary{}
	for _, p := range all {
		if !hasOutstandingPR(p) {
			continue
		}
		if cur, ok := byNode[p.NodeID]; !ok || p.Attempt > cur.Attempt {
			byNode[p.NodeID] = p
		}
	}
	ids := sortedNodeIDs(byNode)
	if len(ids) == 0 {
		return ErrProposalOpen{}, false
	}
	p := byNode[ids[0]]
	return ErrProposalOpen{ProposalID: p.ID, PRURL: representativePRURL(p)}, true
}

// isDeadEnd reports whether a proposed attempt has run out of road: every
// owning service's pull request has been rejected, so no fix from this attempt
// can still land. An owning service with no pull request yet, or one in any
// non-rejected state, means the attempt is still live.
func isDeadEnd(p ports.ProposalSummary) bool {
	byService := map[string]ports.ProposalPR{}
	for _, pr := range p.EffectivePRs() {
		byService[pr.Service] = pr
	}
	for _, svc := range owningServices(p) {
		pr, ok := byService[svc]
		if !ok || pr.PRState != "rejected" {
			return false
		}
	}
	return true
}

// owningServices names the services whose fixes an attempt must land before it
// is exhausted: PRServices when the attempt carries them, otherwise the
// services named by its effective pull requests (the legacy single-group case).
func owningServices(p ports.ProposalSummary) []string {
	if len(p.PRServices) > 0 {
		return p.PRServices
	}
	prs := p.EffectivePRs()
	svcs := make([]string, 0, len(prs))
	for _, pr := range prs {
		svcs = append(svcs, pr.Service)
	}
	return svcs
}

// hasOutstandingPR reports whether any of an attempt's pull requests is being
// opened, open, or merged.
func hasOutstandingPR(p ports.ProposalSummary) bool {
	for _, pr := range p.EffectivePRs() {
		switch pr.PRState {
		case "opening", "open", "merged":
			return true
		}
	}
	return false
}

// representativePRURL is the pull request URL to name when an attempt blocks a
// retry: the first non-rejected pull request's URL in service order, so a human
// is pointed at a PR that could still land rather than a closed one; the legacy
// singular URL when the attempt carries no per-service pull requests.
func representativePRURL(p ports.ProposalSummary) string {
	prs := append([]ports.ProposalPR(nil), p.EffectivePRs()...)
	sort.Slice(prs, func(i, j int) bool { return prs[i].Service < prs[j].Service })
	for _, pr := range prs {
		if pr.PRState != "rejected" {
			return pr.PRURL
		}
	}
	return p.PRURL
}

// sortedNodeIDs returns m's keys in sorted order, so a scan over the map
// picks the same entry every time.
func sortedNodeIDs(m map[string]ports.ProposalSummary) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
