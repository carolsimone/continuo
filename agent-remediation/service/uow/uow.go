// Package uow declares the UnitOfWork seam; the Postgres implementation lives
// in adapters/postgres.
package uow

import (
	"context"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
)

// UnitOfWork groups the write path for a single remediation proposal attempt
// into one Postgres transaction. Begin must be called before any repo method;
// Commit or Rollback closes the transaction.
type UnitOfWork interface {
	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
	// ProposalRepo returns the ProposalRepository bound to the active transaction.
	ProposalRepo() repository.ProposalRepository
	// OutboxRepo returns the outbox.Repository bound to the active transaction.
	OutboxRepo() outbox.Repository
	// MessageProcessingRepo returns the messageprocessing.Repository bound to the
	// active transaction, used for inbound dedup of remediation.requested:v1.
	MessageProcessingRepo() messageprocessing.Repository
}
