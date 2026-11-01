package uow

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/catalog/domain/repository"
	"github.com/google/uuid"
)

// ErrWritesPaused means an export or a restore holds the tenant's export
// lease, so the write did not run. The consumer backs off and retries, as for
// an outage, and writes no dead letter.
var ErrWritesPaused = errors.New("catalog writes are paused for an export or a restore")

// Marker names one message of one consumer group of one tenant. Its group
// processed the message once a write recorded the marker.
type Marker struct {
	TenantID      string
	Group         string
	MessageID     string
	OutboxEntryID *uuid.UUID
}

// UnitOfWork runs a consumer group's writes for one message in Neo4j, with the
// message's processed-message marker in the same transaction as its data.
type UnitOfWork interface {
	// Processed reports whether m's group already processed the message, by
	// message id or, when m carries one, by outbox entry id. It opens no write
	// transaction and takes no gate.
	Processed(ctx context.Context, m Marker) (bool, error)
	// WritesPaused reports whether an export or a restore holds tenantID's
	// lease, so that a Write now would return ErrWritesPaused. It runs in a
	// read transaction and takes no gate: it only saves work a write would
	// discard, and the Write itself is what guarantees correctness.
	WritesPaused(ctx context.Context, tenantID string) (bool, error)
	// Write runs fn in one Neo4j write transaction for m.TenantID: it locks the
	// tenant's export lease node and returns ErrWritesPaused while a lease is
	// held; re-checks the marker (a found marker skips fn and returns nil);
	// runs fn; when final, CREATEs the marker; commits. An error from fn rolls
	// the transaction back and is returned as it is.
	//
	// A write that is not final marks m's message in flight on the tenant's
	// lease node, until the message's final write or until the mark lapses, so
	// an export never starts between a message's writes.
	Write(ctx context.Context, m Marker, final bool, fn func(ctx context.Context, tx Tx) error) error
}

// Tx is the open transaction of one Write. Each repository it returns runs
// its statements on that transaction, under the marker's tenant.
type Tx interface {
	Topology() repository.ReleasePromotionRepository
	CodeVersions() repository.CodeVersionRepository
	CaseBase() repository.CaseBaseRepository
}
