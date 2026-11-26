package outputs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

const namePullRequest workflow.StepName = "pull_request"

// PullRequest is the pass output `pull_request`. It marks a verified fix
// proposed and announces it on remediation.proposed:v1, which is what puts it
// in front of a human for review.
type PullRequest struct {
	clock ports.Clock
}

var _ step.PassOutput = (*PullRequest)(nil)

// NewPullRequest builds the pull_request step. clock stamps the event's
// proposed_at.
func NewPullRequest(clock ports.Clock) *PullRequest {
	return &PullRequest{clock: clock}
}

// Meta describes the step. It reads no evidence.
func (*PullRequest) Meta() step.Meta {
	return step.Meta{
		Name: namePullRequest,
		Kind: step.KindOutput,
		Description: "Marks the verified proposal proposed, stores the approvers of the nodes it fixes on its row and " +
			"enqueues remediation.proposed:v1 for human review, in the same transaction. A proposal that is already " +
			"final is left alone and announces nothing.",
	}
}

// Pass finalizes the proposal v names with the release checks v carries, stores
// on its row the approvers in.Approvers names (the owners of the nodes the fix
// covers), and enqueues its remediation.proposed:v1 event in the same
// transaction, stored under the remediation's tenant, so the row and the
// announcement cannot disagree. The status change is a
// compare-and-set: when the row is no longer verifying, nothing is written and
// nothing is announced.
func (p *PullRequest) Pass(ctx context.Context, u uow.UnitOfWork, in step.OutputInput, v remediation.Verified) error {
	repo := u.ProposalRepo()
	hit, err := repo.MarkFixVerified(ctx, v)
	if err != nil {
		return fmt.Errorf("mark fix verified: %w", err)
	}
	if !hit {
		return nil
	}
	row, err := repo.Get(ctx, v.ProposalID())
	if err != nil {
		return fmt.Errorf("read verified proposal %s: %w", v.ProposalID(), err)
	}
	if len(in.Approvers) > 0 {
		if err := repo.RecordApprovers(ctx, row.ReleaseID, row.Attempt, in.Approvers); err != nil {
			return fmt.Errorf("record the approvers of verified proposal %s: %w", v.ProposalID(), err)
		}
	}
	return p.enqueue(ctx, u, in.Remediation.TenantID, proposedEvent(row), row.SourceResolved)
}

// proposedEvent is the attempt as it was recorded. The release check decided
// only whether the attempt may be shown, not what it says.
func proposedEvent(v proposal.View) proposal.Proposal {
	return proposal.Proposal{
		Source:           v.Source,
		ReleaseID:        v.ReleaseID,
		RemediationRound: v.RemediationRound,
		NodeID:           v.NodeID,
		ResolvedNodeIDs:  append([]string(nil), v.ResolvedNodeIDs...),
		ErrorSignature:   v.ErrorSignature,
		Attempt:          v.Attempt,
		Status:           proposal.StatusProposed,
		ReleaseChecks:    append([]proposal.ReleaseCheck(nil), v.ReleaseChecks...),
		ReleaseCheckID:   v.ReleaseCheckID,
		Confidence:       v.Confidence,
		Rationale:        v.Rationale,
		ProposedSQLURI:   v.ProposedSQLURI,
		DiffURI:          v.DiffURI,
		Edits:            append([]proposal.FileEdit(nil), v.Edits...),
		Model:            v.Model,
	}
}

// enqueue writes the remediation.proposed:v1 outbox entry for p in u's
// transaction, its payload stored under tenantID. The entry id is derived from
// the event id, which is derived from (release, attempt), so one attempt
// announces once. sourceResolved says whether the fix rewrote real
// version-controlled source. No inbound message drove the write, so the entry
// carries no message-processing provenance.
func (p *PullRequest) enqueue(ctx context.Context, u uow.UnitOfWork, tenantID string, pr proposal.Proposal, sourceResolved bool) error {
	eventID := event.RemediationEventID(pr.ReleaseID, pr.Attempt)
	edits := make([]event.ProposedEdit, 0, len(pr.Edits))
	for _, e := range pr.Edits {
		edits = append(edits, event.ProposedEdit{
			Path:         e.Path,
			ContentURI:   e.ContentURI,
			DiffURI:      e.DiffURI,
			TargetNodeID: e.TargetNodeID,
		})
	}
	payload := event.RemediationProposed{
		EventID:          eventID.String(),
		Source:           pr.Source,
		ReleaseID:        pr.ReleaseID,
		RemediationRound: pr.RemediationRound,
		NodeID:           pr.NodeID,
		ResolvedNodeIDs:  append([]string(nil), pr.ResolvedNodeIDs...),
		ErrorSignature:   pr.ErrorSignature,
		ProposedSQLURI:   pr.ProposedSQLURI,
		DiffURI:          pr.DiffURI,
		Edits:            edits,
		Rationale:        pr.Rationale,
		Confidence:       string(pr.Confidence),
		Model:            pr.Model,
		Attempt:          pr.Attempt,
		SourceResolved:   sourceResolved,
		ProposedAt:       p.clock.Now().Format(time.RFC3339),
	}
	body, err := json.Marshal(serialization.RemediationProposedFromDomain(payload))
	if err != nil {
		return fmt.Errorf("marshal proposed event: %w", err)
	}
	stored, err := outbox.WrapTenant(tenantID, body)
	if err != nil {
		return fmt.Errorf("store proposed event under its tenant: %w", err)
	}
	return u.OutboxRepo().Create(ctx, &outbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte(eventID.String())),
		AggregateType: event.AggregateType,
		AggregateID:   event.AggregateIDForRelease(pr.ReleaseID),
		EventType:     event.EventType,
		Payload:       stored,
		StreamName:    streams.RemediationProposedV1,
		Status:        "pending",
	})
}
