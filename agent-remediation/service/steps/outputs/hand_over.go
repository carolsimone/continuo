package outputs

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

const nameHandOver workflow.StepName = "hand_over"

// HandOver is the give-up output `hand_over`. It records that the tries ran
// out with no proposal, as an escalated attempt that names every failing node.
type HandOver struct {
	clock ports.Clock
}

var _ step.GiveUpOutput = (*HandOver)(nil)

// NewHandOver builds the hand_over step. clock stamps the attempt row.
func NewHandOver(clock ports.Clock) *HandOver {
	return &HandOver{clock: clock}
}

// Meta describes the step. It reads no evidence.
func (*HandOver) Meta() step.Meta {
	return step.Meta{
		Name: nameHandOver,
		Kind: step.KindOutput,
		Description: "Records the remediation as escalated for the whole failing set, with no proposal, " +
			"for a human to take over once the tries are spent.",
	}
}

// GiveUp writes the escalated attempt row in u's transaction: every node of the
// trigger is escalated and no reason is given. The attempt number follows the
// release's sequence: one past the terminal attempts of every round up to the
// trigger's, in-flight attempts not counted.
func (h *HandOver) GiveUp(ctx context.Context, u uow.UnitOfWork, in step.OutputInput) error {
	t := in.Trigger
	round := t.Round
	if round < 1 {
		round = 1
	}
	repo := u.ProposalRepo()
	attempt := 1
	for r := 1; r <= round; r++ {
		n, err := repo.CountAttempts(ctx, t.ReleaseID, r)
		if err != nil {
			return fmt.Errorf("count attempts (round %d): %w", r, err)
		}
		attempt += n
	}

	nodeIDs := t.NodeIDs()
	outcomes := make(map[string]proposal.NodeOutcome, len(nodeIDs))
	for _, id := range nodeIDs {
		outcomes[id] = proposal.NodeOutcome{Status: proposal.StatusEscalated}
	}
	p := proposal.Proposal{
		Source:           t.Source,
		ReleaseID:        t.ReleaseID,
		RemediationID:    in.Remediation.ID.String(),
		RemediationRound: round,
		Attempt:          attempt,
		Status:           proposal.StatusEscalated,
		ResolvedNodeIDs:  nodeIDs,
		NodeOutcomes:     outcomes,
		Services:         proposal.UnionServices(t.Services()),
		CreatedAt:        h.clock.Now(),
	}
	p.NormalizeRepresentativeViews()
	p.ErrorSignature = signatureOf(t, p.NodeID)
	if err := repo.Upsert(ctx, p); err != nil {
		return fmt.Errorf("upsert escalated proposal: %w", err)
	}
	return nil
}

// signatureOf is the error signature of the trigger node nodeID, or empty when
// the trigger has no such node.
func signatureOf(t remediation.Trigger, nodeID string) string {
	for _, n := range t.Nodes {
		if n.NodeID == nodeID {
			return n.ErrorSignature
		}
	}
	return ""
}
