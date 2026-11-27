// Package repository declares the domain repository ports for the remediation
// component; concrete implementations live in adapters/postgres.
package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/remediation/domain/failure"
)

// ClassificationDecision is the append-only audit record for one classified
// node — recorded for both emit and drop so no dropped failure is invisible.
type ClassificationDecision struct {
	Source    model.RemediationSource
	ReleaseID string
	// Round is the release's round this decision
	// belongs to: 1 for the rejection itself, +1 per human "try again" on
	// the release. Part of the natural key, so a release can be
	// re-classified once per round rather than only once ever.
	Round          int
	NodeID         string
	Category       failure.Category
	ErrorSignature string
	Decision       failure.Decision
	Reason         string
	DBTLogURI      string
	CreatedAt      time.Time
}

// ClassificationDecisionRepository persists classification decisions. Upsert
// is idempotent on (source, release_id, round, node_id): a
// redelivered rejection or retry-round replay is a no-op. It returns
// inserted=true only when a new row was written, so callers can skip
// re-emitting a trigger for an already-classified (node, round).
//
// The repository is bound to its transaction at construction by the UnitOfWork
// (matching the release-controller repository pattern), so the method takes
// only ctx and domain types — no infrastructure type leaks into the domain port.
type ClassificationDecisionRepository interface {
	Upsert(ctx context.Context, d ClassificationDecision) (inserted bool, err error)
}
