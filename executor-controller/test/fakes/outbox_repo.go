package fakes

import (
	"context"
	"sync"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/google/uuid"
)

// MarkFailedCall records one call to MarkFailed.
type MarkFailedCall struct {
	ID      uuid.UUID
	Message string
}

// FakeOutboxRepo is an in-memory OutboxRepository for unit tests.
type FakeOutboxRepo struct {
	mu sync.RWMutex

	entries         []*model.DeploymentOutboxEntry
	MarkFailedCalls []MarkFailedCall
	IncrementCalls  []uuid.UUID
	ProcessedCalls  []uuid.UUID

	// Injectable errors
	MarkFailedErr error
}

// NewFakeOutboxRepo creates a new FakeOutboxRepo.
func NewFakeOutboxRepo(entries ...*model.DeploymentOutboxEntry) *FakeOutboxRepo {
	r := &FakeOutboxRepo{
		entries: make([]*model.DeploymentOutboxEntry, 0, len(entries)),
	}
	r.entries = append(r.entries, entries...)
	return r
}

// SetMarkFailedErr configures MarkFailed to return err.
func (f *FakeOutboxRepo) SetMarkFailedErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.MarkFailedErr = err
}

func (f *FakeOutboxRepo) Create(_ context.Context, entry *model.DeploymentOutboxEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, entry)
	return nil
}

func (f *FakeOutboxRepo) GetPendingBatch(_ context.Context, limit int) ([]*model.DeploymentOutboxEntry, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []*model.DeploymentOutboxEntry
	for _, e := range f.entries {
		if e.Status == string(model.OutboxStatusPending) {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (f *FakeOutboxRepo) MarkProcessed(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ProcessedCalls = append(f.ProcessedCalls, id)
	for _, e := range f.entries {
		if e.ID == id {
			e.Status = string(model.OutboxStatusProcessed)
		}
	}
	return nil
}

func (f *FakeOutboxRepo) MarkFailed(_ context.Context, id uuid.UUID, msg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.MarkFailedErr != nil {
		return f.MarkFailedErr
	}
	f.MarkFailedCalls = append(f.MarkFailedCalls, MarkFailedCall{ID: id, Message: msg})
	for _, e := range f.entries {
		if e.ID == id {
			e.Status = string(model.OutboxStatusFailed)
		}
	}
	return nil
}

func (f *FakeOutboxRepo) IncrementRetry(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.IncrementCalls = append(f.IncrementCalls, id)
	for _, e := range f.entries {
		if e.ID == id {
			e.OutboxRetryCount++
		}
	}
	return nil
}
