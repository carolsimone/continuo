package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// UnitOfWork manages a single Postgres transaction and the repositories scoped
// to it.
type UnitOfWork struct {
	db               *sqlx.DB
	tx               *sqlx.Tx
	logger           *slog.Logger
	serviceRepoPaths map[string]string
}

var _ uow.UnitOfWork = (*UnitOfWork)(nil)

// NewUnitOfWork constructs a Postgres-backed UnitOfWork. serviceRepoPaths is
// forwarded to the transaction-scoped ProposalRepository so a per-service PR
// claim can split a proposal's edits by owning service.
func NewUnitOfWork(db *sqlx.DB, logger *slog.Logger, serviceRepoPaths map[string]string) *UnitOfWork {
	return &UnitOfWork{db: db, logger: logger, serviceRepoPaths: serviceRepoPaths}
}

func (u *UnitOfWork) Begin(ctx context.Context) error {
	tx, err := u.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	u.tx = tx
	return nil
}

func (u *UnitOfWork) Commit() error {
	if u.tx == nil {
		return nil
	}
	err := u.tx.Commit()
	u.tx = nil
	return err
}

func (u *UnitOfWork) Rollback() error {
	if u.tx == nil {
		return nil
	}
	err := u.tx.Rollback()
	u.tx = nil
	return err
}

// ProposalRepo returns the ProposalRepository bound to the current transaction.
func (u *UnitOfWork) ProposalRepo() repository.ProposalRepository {
	return NewProposalRepository(u.tx, u.serviceRepoPaths)
}

// OutboxRepo returns the pkg/outbox repository bound to remediation_agent_outbox
// on the current transaction.
func (u *UnitOfWork) OutboxRepo() outbox.Repository {
	return outbox.NewPostgresRepository(u.tx, "remediation_agent_outbox", u.logger)
}

// MessageProcessingRepo returns the messageprocessing.Repository bound to the
// current transaction, used for inbound dedup of remediation.requested:v1.
func (u *UnitOfWork) MessageProcessingRepo() messageprocessing.Repository {
	return messageprocessing.NewPostgresRepository(u.tx, u.logger)
}
