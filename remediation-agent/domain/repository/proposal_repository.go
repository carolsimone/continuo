// Package repository declares the remediation-agent domain repository ports;
// implementations live in adapters/postgres.
package repository

import (
	"context"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
)

// ProposalRepository persists fix-proposal attempts and answers the attempt-cap
// query. The repository is bound to its transaction at construction by the
// UnitOfWork, so methods take only ctx + domain types.
type ProposalRepository interface {
	CountAttempts(ctx context.Context, source, nodeID, errorSignature string) (int, error)
	Insert(ctx context.Context, p proposal.Proposal) error
}
