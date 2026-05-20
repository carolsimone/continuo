package handlers_test

import (
	"context"
	"sync"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// stubOutboxRepo captures creates for assertion without a real DB.
type stubOutboxRepo struct {
	mu      sync.Mutex
	entries []*pkgoutbox.Entry
}

func (r *stubOutboxRepo) Create(_ context.Context, entry *pkgoutbox.Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, entry)
	return nil
}

func (r *stubOutboxRepo) GetPendingBatch(_ context.Context, _ int) ([]*pkgoutbox.Entry, error) {
	return nil, nil
}

func (r *stubOutboxRepo) MarkProcessed(_ context.Context, _ uuid.UUID) error { return nil }
func (r *stubOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (r *stubOutboxRepo) IncrementRetry(_ context.Context, _ uuid.UUID) error { return nil }
