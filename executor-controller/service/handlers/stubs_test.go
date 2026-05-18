package handlers_test

import (
	"context"
	"sync"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/google/uuid"
)

// stubOutboxRepo captures creates for assertion without a real DB.
type stubOutboxRepo struct {
	mu      sync.Mutex
	entries []*model.DeploymentOutboxEntry
}

func (r *stubOutboxRepo) Create(_ context.Context, entry *model.DeploymentOutboxEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, entry)
	return nil
}

func (r *stubOutboxRepo) GetPendingBatch(_ context.Context, _ int) ([]*model.DeploymentOutboxEntry, error) {
	return nil, nil
}

func (r *stubOutboxRepo) MarkProcessed(_ context.Context, _ uuid.UUID) error { return nil }
func (r *stubOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (r *stubOutboxRepo) IncrementRetry(_ context.Context, _ uuid.UUID) error  { return nil }
func (r *stubOutboxRepo) MarkTerminallyFailed(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
