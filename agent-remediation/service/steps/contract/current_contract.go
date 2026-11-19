package contract

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

// CurrentContract is the context source `current_contract`: the failing node's
// contract as continuo last promoted it, from the catalog.
type CurrentContract struct {
	contracts ports.CurrentContractReader
	logger    *slog.Logger
}

var _ step.ContextSource = (*CurrentContract)(nil)

// NewCurrentContract builds the current_contract step. It calls no port until
// Gather; a nil logger logs to the default logger.
func NewCurrentContract(contracts ports.CurrentContractReader, logger *slog.Logger) *CurrentContract {
	return &CurrentContract{contracts: contracts, logger: shared.LoggerOrDefault(logger)}
}

// Meta describes the step.
func (*CurrentContract) Meta() step.Meta {
	return step.Meta{
		Name: "current_contract",
		Kind: step.KindContext,
		Description: "Reads the failing node's current contract from the catalog: the published ODCS YAML of the last promotion that changed it. " +
			"Provides an empty text when the node has no contract yet, or when the catalog cannot answer.",
		Needs:    []remediation.EvidenceKey{remediation.KeyFailure},
		Provides: []remediation.EvidenceKey{remediation.KeyCurrentContract},
	}
}

// Gather returns the node's current contract. A node the catalog holds no contract for gives an empty
// text. The contract is reference only, so a failed read (a catalog outage,
// or a catalog that serves no contract reads yet) is logged as a warning and
// gives an empty text too: it never fails or declines the cluster.
func (s *CurrentContract) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	nodeID := in.Failure.Target.NodeID
	doc, err := s.contracts.CurrentContract(ctx, nodeID)
	if err != nil {
		s.logger.Warn("current contract unavailable; proceeding without it",
			"node", nodeID, "error", err)
		doc = ""
	}
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyCurrentContract: remediation.Text(doc),
	}}, nil
}
