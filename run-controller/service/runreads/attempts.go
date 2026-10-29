package runreads

import (
	"context"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/run-controller/domain/projection"
	"github.com/carolsimone/continuo/run-controller/domain/repository"
)

// Attempts reads the attempts of the tasks of a tenant's run.
type Attempts struct {
	attempts repository.TaskAttemptReader
}

// NewAttempts returns an Attempts over attempts.
func NewAttempts(attempts repository.TaskAttemptReader) *Attempts {
	return &Attempts{attempts: attempts}
}

// ListRunAttempts returns one page of the attempts of tenantID's run runID,
// only taskID's when it is not nil, and how many attempts match. A page size
// below 1 becomes DefaultPageSize, one above MaxPageSize becomes MaxPageSize,
// and a negative offset becomes 0. A run the tenant does not have has no
// attempts.
func (a *Attempts) ListRunAttempts(ctx context.Context, tenantID string, runID uuid.UUID, taskID *uuid.UUID, pageSize, pageOffset int) ([]projection.TaskAttempt, int, error) {
	if pageSize < 1 {
		pageSize = DefaultPageSize
	}
	pageSize = min(pageSize, MaxPageSize)
	pageOffset = max(pageOffset, 0)
	return a.attempts.ListRunAttempts(ctx, tenantID, runID, taskID, pageSize, pageOffset)
}
