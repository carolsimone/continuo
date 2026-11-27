package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/google/uuid"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/maintenance"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// Refusals RetryRemediation reports; the HTTP layer maps each to a status.
var (
	ErrReleaseNotFound = errors.New("release not found")
	ErrNotHealable     = errors.New("rejection reason is not healable")
	ErrNotRetryable    = errors.New("release was rejected without a stored rejection payload")
	// ErrRetryInProgress refuses a retry when the current remediation round has
	// not yet produced a fix row: either its trigger has not reached
	// agent-remediation yet, or the classifier dropped it. Either way, spending
	// another round now would race the round already in flight.
	ErrRetryInProgress = errors.New("a remediation round is in progress")
	// ErrFixReaderUnavailable wraps a failure of the gRPC read to
	// agent-remediation, distinct from a legitimate "a try is open" answer.
	ErrFixReaderUnavailable = errors.New("could not read remediation fixes")
)

// ErrFixOpen refuses a retry because a try is still in flight or a
// fix is already proposed; it names what to look at instead.
type ErrFixOpen struct {
	FixID string
	PRURL string
}

func (e ErrFixOpen) Error() string { return "a fix is already open for this release" }

// IsHealableReason reports whether a reject reason is one the classifier turns
// into a heal trigger, which is the `healable` flag the reject_reason
// vocabulary carries in pkg/streams/contract.yaml. Every other reason — and
// any token this build does not declare — is dropped on the normal path too,
// so a retry of it would only spend a remediation round for nothing.
func IsHealableReason(reason string) bool {
	return pkg_model.RejectReason(reason).Healable()
}

// RetryRemediationResult is the round the retry started.
type RetryRemediationResult struct {
	ReleaseID        string
	RemediationRound int
}

// RetryRemediation starts another remediation round on a rejected candidate:
// it writes an outbox row for remediation.retry_requested:v1 whose payload is
// the typed retry (the release id, the new round and the stored rejection,
// unchanged), stored under events.DefaultTenantID. It refuses when the id
// names a fix's release check rather than a candidate (a fix is not a
// release, so it is reported the same as an unknown id), when the candidate is
// not rejected, when its reason is not healable, when the round cap is
// reached, when the candidate has no stored rejection, when the current round
// has not yet produced a fix row (a second click before the first has
// landed), or when a try is still in flight or a fix is already proposed
// — the caller should look at that instead of retrying. It refuses with
// maintenance.ErrActive while maintenance mode is on.
func RetryRemediation(ctx context.Context, deps *Deps, releaseID string) (RetryRemediationResult, error) {
	if deps.Maintenance {
		return RetryRemediationResult{}, maintenance.ErrActive
	}
	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return RetryRemediationResult{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	r, err := u.ReleaseCheckRepo().Load(ctx, releaseID)
	if err != nil {
		return RetryRemediationResult{}, fmt.Errorf("load release: %w", err)
	}
	if r == nil {
		return RetryRemediationResult{}, ErrReleaseNotFound
	}
	if r.Status() != releasecheck.StatusRejected {
		return RetryRemediationResult{}, releasecheck.ErrNotRejected
	}
	if r.Kind() != releasecheck.KindCandidate {
		return RetryRemediationResult{}, ErrReleaseNotFound
	}
	if !IsHealableReason(r.FailReason()) {
		return RetryRemediationResult{}, ErrNotHealable
	}
	if len(r.RejectionPayload()) == 0 {
		return RetryRemediationResult{}, ErrNotRetryable
	}
	if r.RemediationRound() >= releasecheck.MaxRemediationRounds {
		return RetryRemediationResult{}, releasecheck.ErrRoundsExhausted
	}

	fixes, err := deps.Fixes.ListFixesForRelease(ctx, releaseID)
	if err != nil {
		return RetryRemediationResult{}, fmt.Errorf("%w: %v", ErrFixReaderUnavailable, err)
	}
	current := currentRoundFixes(fixes, r.RemediationRound())
	if len(current) == 0 {
		return RetryRemediationResult{}, ErrRetryInProgress
	}
	if open, ok := openFix(current, fixes); ok {
		return RetryRemediationResult{}, open
	}

	now := deps.Clock.Now()
	round, err := r.StartRemediationRound(now)
	if err != nil {
		return RetryRemediationResult{}, err
	}
	if err := u.ReleaseCheckRepo().Save(ctx, r); err != nil {
		return RetryRemediationResult{}, fmt.Errorf("save release: %w", err)
	}

	retry, err := json.Marshal(events.RemediationRetryRequested{
		ReleaseID:        releaseID,
		RemediationRound: round,
		Rejection:        r.RejectionPayload(),
	})
	if err != nil {
		return RetryRemediationResult{}, fmt.Errorf("marshal retry payload: %w", err)
	}
	payload, err := pkgoutbox.WrapTenant(events.DefaultTenantID, retry)
	if err != nil {
		return RetryRemediationResult{}, fmt.Errorf("wrap retry payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte("remediation-retry:"+releaseID+":"+strconv.Itoa(round))),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(releaseID),
		EventType:     "remediation_retry_requested",
		Payload:       payload,
		StreamName:    streams.RemediationRetryRequestedV1,
		Status:        "pending",
	}); err != nil {
		return RetryRemediationResult{}, fmt.Errorf("outbox insert: %w", err)
	}
	if err := u.Commit(); err != nil {
		return RetryRemediationResult{}, fmt.Errorf("commit: %w", err)
	}
	deps.Logger.Info("remediation retry started", "release", releaseID, "round", round)
	return RetryRemediationResult{ReleaseID: releaseID, RemediationRound: round}, nil
}

// effectiveRound is the remediation round a fix belongs to: its own
// RemediationRound field, or 1 for a fix that names no round.
func effectiveRound(p ports.FixSummary) int {
	if p.RemediationRound == 0 {
		return 1
	}
	return p.RemediationRound
}

// laterTry reports whether try a comes after try b: a later round, or a later
// try of the same round. A round's hand-over row (try 0) is written after the
// round's last try, so it orders after every other try of its round.
func laterTry(a, b ports.FixSummary) bool {
	if ra, rb := effectiveRound(a), effectiveRound(b); ra != rb {
		return ra > rb
	}
	if a.Try == 0 || b.Try == 0 {
		return a.Try == 0 && b.Try != 0
	}
	return a.Try > b.Try
}

// currentRoundFixes filters ps to the ones belonging to round.
func currentRoundFixes(ps []ports.FixSummary, round int) []ports.FixSummary {
	out := make([]ports.FixSummary, 0, len(ps))
	for _, p := range ps {
		if effectiveRound(p) == round {
			out = append(out, p)
		}
	}
	return out
}

// openFix reports the try that makes a retry pointless: one from the
// current round still in flight or already proposed, or — checked across
// every round — one whose PR is being opened, open, or merged. Node id order
// is sorted before picking so the answer is deterministic when several nodes
// qualify.
func openFix(current, all []ports.FixSummary) (ErrFixOpen, bool) {
	if open, ok := openInCurrentRound(current); ok {
		return open, true
	}
	return openPRAcrossRounds(all)
}

// openInCurrentRound reports whether any node's latest try in the current
// round is still generating or verifying, or is proposed with a fix that could
// still land. A batched try opens one pull request per owning project, so
// the decision weighs all of them: the try is a dead end — closed without
// landing a fix — only once every owning project's pull request has been
// rejected. If any owning project has no pull request yet, or one in any
// non-rejected state, a fix may still land and a new round would duplicate it.
// Only each node's latest try is consulted; an earlier try superseded
// by a later one on the same node does not count.
func openInCurrentRound(current []ports.FixSummary) (ErrFixOpen, bool) {
	latest := map[string]ports.FixSummary{}
	for _, p := range current {
		if cur, ok := latest[p.NodeID]; !ok || laterTry(p, cur) {
			latest[p.NodeID] = p
		}
	}
	for _, id := range sortedNodeIDs(latest) {
		p := latest[id]
		switch p.Status {
		case "generating", "verifying":
			return ErrFixOpen{FixID: p.ID}, true
		case "proposed":
			if isDeadEnd(p) {
				continue
			}
			return ErrFixOpen{FixID: p.ID, PRURL: representativePRURL(p)}, true
		}
	}
	return ErrFixOpen{}, false
}

// openPRAcrossRounds reports whether any fix from any round — not just the
// current one — has a pull request that is opening, open, or merged: a fix
// already out for human review makes a new round redundant regardless of which
// round proposed it. A batched try's per-project pull requests are all
// weighed, so one project's still-open PR blocks even when a sibling project's
// PR was rejected.
func openPRAcrossRounds(all []ports.FixSummary) (ErrFixOpen, bool) {
	byNode := map[string]ports.FixSummary{}
	for _, p := range all {
		if !hasOutstandingPR(p) {
			continue
		}
		if cur, ok := byNode[p.NodeID]; !ok || laterTry(p, cur) {
			byNode[p.NodeID] = p
		}
	}
	ids := sortedNodeIDs(byNode)
	if len(ids) == 0 {
		return ErrFixOpen{}, false
	}
	p := byNode[ids[0]]
	return ErrFixOpen{FixID: p.ID, PRURL: representativePRURL(p)}, true
}

// isDeadEnd reports whether a proposed try has run out of road: every
// owning project's pull request has been rejected, so no fix from this try
// can still land. An owning project with no pull request yet, or one in any
// non-rejected state, means the try is still live.
func isDeadEnd(p ports.FixSummary) bool {
	byProject := map[string]ports.FixPR{}
	for _, pr := range p.PullRequests {
		byProject[pr.Project] = pr
	}
	for _, project := range owningProjects(p) {
		pr, ok := byProject[project]
		if !ok || pr.PRState != "rejected" {
			return false
		}
	}
	return true
}

// owningProjects names the projects whose fixes a try must land before it
// is exhausted: PRProjects, or the one group of a fix not split by project,
// named by the empty string, when the try names none.
func owningProjects(p ports.FixSummary) []string {
	if len(p.PRProjects) > 0 {
		return p.PRProjects
	}
	return []string{""}
}

// hasOutstandingPR reports whether any of a try's pull requests is being
// opened, open, or merged.
func hasOutstandingPR(p ports.FixSummary) bool {
	for _, pr := range p.PullRequests {
		switch pr.PRState {
		case "opening", "open", "merged":
			return true
		}
	}
	return false
}

// representativePRURL is the pull request URL to name when a try blocks a
// retry: the first non-rejected pull request's URL in project order, so a human
// is pointed at a PR that could still land rather than a closed one; the first
// pull request's URL when every one was rejected, and "" when the try has
// none.
func representativePRURL(p ports.FixSummary) string {
	prs := append([]ports.FixPR(nil), p.PullRequests...)
	sort.Slice(prs, func(i, j int) bool { return prs[i].Project < prs[j].Project })
	for _, pr := range prs {
		if pr.PRState != "rejected" {
			return pr.PRURL
		}
	}
	if len(prs) > 0 {
		return prs[0].PRURL
	}
	return ""
}

// sortedNodeIDs returns m's keys in sorted order, so a scan over the map
// picks the same entry every time.
func sortedNodeIDs(m map[string]ports.FixSummary) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
