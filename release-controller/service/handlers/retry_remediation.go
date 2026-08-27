package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// Refusals RetryRemediation reports; the HTTP layer maps each to a status.
var (
	ErrReleaseNotFound = errors.New("release not found")
	ErrNotHealable     = errors.New("rejection reason is not healable")
	ErrNotRetryable    = errors.New("release was rejected without a stored rejection payload")
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

// RetryRemediation starts another remediation round on a rejected release: it
// replays the rejection the release stored, tagged with the new round, on
// remediation.retry_requested:v1. It refuses when the release is not rejected,
// when its reason is not healable, when the round cap is reached, when the
// release has no stored rejection, or when an attempt is still in flight or a
// fix is already proposed — the caller should look at that instead of retrying.
func RetryRemediation(ctx context.Context, deps *Deps, releaseID string) (RetryRemediationResult, error) {
	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return RetryRemediationResult{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	r, err := u.ReleaseRepo().Load(ctx, releaseID)
	if err != nil {
		return RetryRemediationResult{}, fmt.Errorf("load release: %w", err)
	}
	if r == nil {
		return RetryRemediationResult{}, ErrReleaseNotFound
	}
	if r.Status() != release.StatusRejected {
		return RetryRemediationResult{}, release.ErrNotRejected
	}
	if !healableRejectReasons[r.RejectReason()] {
		return RetryRemediationResult{}, ErrNotHealable
	}
	if len(r.RejectionPayload()) == 0 {
		return RetryRemediationResult{}, ErrNotRetryable
	}
	if r.RemediationRound() >= release.MaxRemediationRounds {
		return RetryRemediationResult{}, release.ErrRoundsExhausted
	}

	proposals, err := deps.Proposals.ListProposalsForRelease(ctx, releaseID)
	if err != nil {
		return RetryRemediationResult{}, fmt.Errorf("list proposals: %w", err)
	}
	if open, ok := openProposal(proposals); ok {
		return RetryRemediationResult{}, open
	}

	now := deps.Clock.Now()
	round, err := r.StartRemediationRound(now)
	if err != nil {
		return RetryRemediationResult{}, err
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
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

// openProposal reports the attempt that makes a retry pointless: one still in
// flight, one already proposed, or one whose PR is being opened, open, or
// merged. Only each node's latest attempt is consulted; an earlier failed
// attempt superseded by a later proposal does not make the release a dead end.
func openProposal(ps []ports.ProposalSummary) (ErrProposalOpen, bool) {
	latest := map[string]ports.ProposalSummary{}
	for _, p := range ps {
		if cur, ok := latest[p.NodeID]; !ok || p.Attempt > cur.Attempt {
			latest[p.NodeID] = p
		}
	}
	for _, p := range latest {
		switch p.Status {
		case "generating", "verifying", "proposed":
			return ErrProposalOpen{ProposalID: p.ID, PRURL: p.PRURL}, true
		}
		switch p.PRState {
		case "opening", "open", "merged":
			return ErrProposalOpen{ProposalID: p.ID, PRURL: p.PRURL}, true
		}
	}
	return ErrProposalOpen{}, false
}
