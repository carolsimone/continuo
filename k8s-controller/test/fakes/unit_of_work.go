package fakes

import (
	"context"

	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// FakeTransaction exposes fake repositories for one transaction scope.
type FakeTransaction struct {
	OutboxRepoFunc          pkgoutbox.Repository
	ProcessedEventsRepoFunc postgres.ProcessedEventsRepository
}

func (f *FakeTransaction) OutboxRepo() pkgoutbox.Repository {
	if f.OutboxRepoFunc != nil {
		return f.OutboxRepoFunc
	}
	return &FakeOutboxRepository{}
}

func (f *FakeTransaction) ProcessedEventsRepo() postgres.ProcessedEventsRepository {
	if f.ProcessedEventsRepoFunc != nil {
		return f.ProcessedEventsRepoFunc
	}
	return &FakeProcessedEventsRepository{}
}

// FakeTransactionRunner is a fake implementation of TransactionRunner for testing.
type FakeTransactionRunner struct {
	Transaction           uow.Transaction
	WithinTransactionFunc func(context.Context, func(uow.Transaction) error) error

	WithinTransactionCallCount int
}

func (f *FakeTransactionRunner) WithinTransaction(ctx context.Context, fn func(uow.Transaction) error) error {
	f.WithinTransactionCallCount++
	if f.WithinTransactionFunc != nil {
		return f.WithinTransactionFunc(ctx, fn)
	}
	if f.Transaction == nil {
		f.Transaction = &FakeTransaction{}
	}
	return fn(f.Transaction)
}

var _ uow.Transaction = (*FakeTransaction)(nil)
var _ uow.TransactionRunner = (*FakeTransactionRunner)(nil)

// FakeOutboxRepository is a fake implementation of pkgoutbox.Repository for testing
type FakeOutboxRepository struct {
	CreateFunc          func(ctx context.Context, entry *pkgoutbox.Entry) error
	GetPendingBatchFunc func(ctx context.Context, limit int) ([]*pkgoutbox.Entry, error)
	MarkProcessedFunc   func(ctx context.Context, id uuid.UUID) error
	MarkFailedFunc      func(ctx context.Context, id uuid.UUID, errorMessage string) error
	IncrementRetryFunc  func(ctx context.Context, id uuid.UUID) error

	CreateCallCount          int
	GetPendingBatchCallCount int
	MarkProcessedCallCount   int
	MarkFailedCallCount      int
	IncrementRetryCallCount  int
}

func (f *FakeOutboxRepository) Create(ctx context.Context, entry *pkgoutbox.Entry) error {
	f.CreateCallCount++
	if f.CreateFunc != nil {
		return f.CreateFunc(ctx, entry)
	}
	return nil
}

func (f *FakeOutboxRepository) GetPendingBatch(ctx context.Context, limit int) ([]*pkgoutbox.Entry, error) {
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
