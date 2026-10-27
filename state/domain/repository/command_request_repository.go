package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/state/domain/commandrequest"
	"github.com/google/uuid"
)

// CommandRequestRepository stores the command requests that make a command run
// once per request id. A request belongs to its tenant: the same id in two
// tenants is two requests.
type CommandRequestRepository interface {
	// Find returns the tenant's record of requestID and whether there is one.
	// It sees committed records only.
	Find(ctx context.Context, tenantID string, requestID uuid.UUID) (commandrequest.Record, bool, error)

	// Claim inserts rec and reports whether it inserted it. When the tenant
	// already has a record under the id, Claim inserts nothing and returns
	// false; when another open transaction has inserted it but not yet ended,
	// Claim waits for that transaction, then returns false if it committed and
	// true if it rolled back. Inside a transaction, the claim ends with it.
	Claim(ctx context.Context, rec commandrequest.Record) (bool, error)

	// DeleteOlderThan deletes the records created more than age ago, of every
	// tenant, and returns how many it deleted.
	DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error)
}
