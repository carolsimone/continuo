// Package repository declares the domain repository ports for the remediation
// service; concrete implementations live in adapters/postgres.
package repository

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/carolsimone/continuo/remediation/domain/failure"
)

// ClassificationDecision is the append-only audit record for one classified
// node — recorded for both emit and drop so no dropped failure is invisible.
type ClassificationDecision struct {
	Source         failure.Source
	ReleaseID      string
	NodeID         string
	Category       failure.Category
	ErrorSignature string
	Decision       failure.Decision
	Reason         string
	DBTLogURI      string
	CreatedAt      time.Time
}

// ClassificationDecisionRepository persists classification decisions. Upsert
// is idempotent on (source, release_id, node_id): a redelivered rejection is a
// no-op. It returns inserted=true only when a new row was written, so callers
// can skip re-emitting a trigger for an already-classified node.
type ClassificationDecisionRepository interface {
	Upsert(ctx context.Context, tx *sqlx.Tx, d ClassificationDecision) (inserted bool, err error)
}
