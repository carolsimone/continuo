package fakes

import (
	"context"

	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/google/uuid"
)

// FakeUnitOfWork is a fake implementation of UnitOfWork for testing
type FakeUnitOfWork struct {
	OutboxRepoFunc          postgres.OutboxRepository
	ProcessedEventsRepoFunc postgres.ProcessedEventsRepository
	BeginFunc               func(ctx context.Context) error
	CommitFunc              func() error
	RollbackFunc            func() error

	BeginCallCount    int
	CommitCallCount   int
	RollbackCallCount int
}

func (f *FakeUnitOfWork) OutboxRepo() postgres.OutboxRepository {
	if f.OutboxRepoFunc != nil {
		return f.OutboxRepoFunc
	}
	return &FakeOutboxRepository{}
}

func (f *FakeUnitOfWork) ProcessedEventsRepo() postgres.ProcessedEventsRepository {
	if f.ProcessedEventsRepoFunc != nil {
		return f.ProcessedEventsRepoFunc
	}
	return &FakeProcessedEventsRepository{}
}

func (f *FakeUnitOfWork) Begin(ctx context.Context) error {
	f.BeginCallCount++
	if f.BeginFunc != nil {
		return f.BeginFunc(ctx)
	}
	return nil
}

func (f *FakeUnitOfWork) Commit() error {
	f.CommitCallCount++
	if f.CommitFunc != nil {
		return f.CommitFunc()
	}
	return nil
}

func (f *FakeUnitOfWork) Rollback() error {
	f.RollbackCallCount++
	if f.RollbackFunc != nil {
		return f.RollbackFunc()
	}
	return nil
}

// FakeOutboxRepository is a fake implementation of OutboxRepository for testing
type FakeOutboxRepository struct {
	CreateFunc           func(ctx context.Context, entry *model.K8sStatusOutboxEntry) error
	GetPendingBatchFunc  func(ctx context.Context, limit int) ([]*model.K8sStatusOutboxEntry, error)
	MarkProcessedFunc    func(ctx context.Context, id uuid.UUID) error
	MarkFailedFunc       func(ctx context.Context, id uuid.UUID, errorMessage string) error
	IncrementRetryFunc   func(ctx context.Context, id uuid.UUID) error
	GetStuckEntriesFunc  func(ctx context.Context, limit int, stuckThresholdSeconds int) ([]*model.K8sStatusOutboxEntry, error)
	ForceMarkFailedFunc  func(ctx context.Context, id uuid.UUID, errorMessage string) error

	CreateCallCount          int
	GetPendingBatchCallCount int
	MarkProcessedCallCount   int
	MarkFailedCallCount      int
	IncrementRetryCallCount  int
	GetStuckEntriesCallCount int
	ForceMarkFailedCallCount int
}

func (f *FakeOutboxRepository) Create(ctx context.Context, entry *model.K8sStatusOutboxEntry) error {
	f.CreateCallCount++
	if f.CreateFunc != nil {
		return f.CreateFunc(ctx, entry)
	}
	return nil
}

func (f *FakeOutboxRepository) GetPendingBatch(ctx context.Context, limit int) ([]*model.K8sStatusOutboxEntry, error) {
	f.GetPendingBatchCallCount++
	if f.GetPendingBatchFunc != nil {
		return f.GetPendingBatchFunc(ctx, limit)
	}
	return nil, nil
}

func (f *FakeOutboxRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	f.MarkProcessedCallCount++
	if f.MarkProcessedFunc != nil {
		return f.MarkProcessedFunc(ctx, id)
	}
	return nil
}

func (f *FakeOutboxRepository) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	f.MarkFailedCallCount++
	if f.MarkFailedFunc != nil {
		return f.MarkFailedFunc(ctx, id, errorMessage)
	}
	return nil
}

func (f *FakeOutboxRepository) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	f.IncrementRetryCallCount++
	if f.IncrementRetryFunc != nil {
		return f.IncrementRetryFunc(ctx, id)
	}
	return nil
}

func (f *FakeOutboxRepository) GetStuckEntries(ctx context.Context, limit int, stuckThresholdSeconds int) ([]*model.K8sStatusOutboxEntry, error) {
	f.GetStuckEntriesCallCount++
	if f.GetStuckEntriesFunc != nil {
		return f.GetStuckEntriesFunc(ctx, limit, stuckThresholdSeconds)
	}
	return nil, nil
}

func (f *FakeOutboxRepository) ForceMarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	f.ForceMarkFailedCallCount++
	if f.ForceMarkFailedFunc != nil {
		return f.ForceMarkFailedFunc(ctx, id, errorMessage)
	}
	return nil
}

// FakeProcessedEventsRepository is a fake for testing
type FakeProcessedEventsRepository struct {
	TryMarkProcessedFunc func(ctx context.Context, id uuid.UUID) (bool, error)

	TryMarkProcessedCallCount int
	ProcessedIDs              []uuid.UUID
}

func (f *FakeProcessedEventsRepository) TryMarkProcessed(ctx context.Context, id uuid.UUID) (bool, error) {
	f.TryMarkProcessedCallCount++
	if f.TryMarkProcessedFunc != nil {
		return f.TryMarkProcessedFunc(ctx, id)
	}
	for _, seen := range f.ProcessedIDs {
		if seen == id {
			return true, nil // duplicate
		}
	}
	f.ProcessedIDs = append(f.ProcessedIDs, id)
	return false, nil // newly claimed
}
