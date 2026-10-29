package repository

import (
	"context"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/run-controller/domain/projection"
)

// TaskAttemptReader reads the attempts of a run's tasks.
type TaskAttemptReader interface {
	// ListRunAttempts returns one page of the attempts of the tenant's run
	// runID, only taskID's when taskID is not nil, ordered by task id then
	// attempt number, and how many attempts match.
	ListRunAttempts(ctx context.Context, tenantID string, runID uuid.UUID, taskID *uuid.UUID, pageSize, pageOffset int) ([]projection.TaskAttempt, int, error)
}
