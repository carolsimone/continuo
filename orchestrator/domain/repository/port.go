package repository

import (
	"context"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/google/uuid"
)

// OutboxRepository defines operations for the outbox table.
type OutboxRepository interface {
	Create(ctx context.Context, entry *domain.OutboxEntry) error
	GetPendingBatch(ctx context.Context, limit int) ([]*domain.OutboxEntry, error)
	MarkProcessed(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error
	IncrementRetry(ctx context.Context, id uuid.UUID) error
	UpdateStatus(ctx context.Context, id uuid.UUID, newStatus, expectedStatus string) error
}

// PublishedMessagesRepository handles published_messages table operations.
type PublishedMessagesRepository interface {
	Exists(ctx context.Context, outboxEntryID uuid.UUID) (bool, error)
	Create(ctx context.Context, pm *domain.PublishedMessage) error
}
