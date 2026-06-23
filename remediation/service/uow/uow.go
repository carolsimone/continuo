// Package uow declares the UnitOfWork seam for the remediation service. The
// Postgres implementation lives in adapters/postgres.
package uow

import (
	"context"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/remediation/domain/repository"
)

// UnitOfWork manages a single Postgres transaction and exposes the
// transaction-scoped repositories the handler needs. Callers own the
// lifecycle: Begin → work → Commit, with Rollback on any failure. There is
// intentionally no Tx() accessor: both repositories are bound to the
// transaction at construction, so handlers never need the raw *sql.Tx.
type UnitOfWork interface {
	Begin(ctx context.Context) error
	Commit() error
	Rollback() error
	DecisionRepo() repository.ClassificationDecisionRepository
	OutboxRepo() outbox.Repository
}
