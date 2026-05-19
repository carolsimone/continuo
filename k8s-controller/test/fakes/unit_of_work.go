package fakes

import (
	"context"

	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// FakeTransaction exposes fake repositories for one transaction scope.
type FakeTransaction struct {
	OutboxRepoFunc             pkgoutbox.Repository
	MessageProcessingRepoFunc  messageprocessing.Repository
}

func (f *FakeTransaction) OutboxRepo() pkgoutbox.Repository {
	if f.OutboxRepoFunc != nil {
		return f.OutboxRepoFunc
	}
	return &FakeOutboxRepository{}
}

func (f *FakeTransaction) MessageProcessingRepo() messageprocessing.Repository {
	if f.MessageProcessingRepoFunc != nil {
		return f.MessageProcessingRepoFunc
	}
	return &FakeMessageProcessingRepository{}
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

// FakeMessageProcessingRepository is a fake implementation of messageprocessing.Repository for testing.
// InsertIfNotExists tracks seen (messageID, streamName) pairs: the first call inserts (returns true),
// subsequent calls with the same pair are duplicates (returns false).
type FakeMessageProcessingRepository struct {
	InsertIfNotExistsFunc        func(ctx context.Context, msgProc *messageprocessing.MessageProcessing) (uuid.UUID, bool, error)
	GetByMessageIDAndStreamFunc  func(ctx context.Context, messageID, streamName string) (*messageprocessing.MessageProcessing, error)
	UpdateStateFunc              func(ctx context.Context, id uuid.UUID, state string) error

	InsertIfNotExistsCallCount int
	seen                       map[string]uuid.UUID // key: "messageID\x00streamName"
}

func (f *FakeMessageProcessingRepository) InsertIfNotExists(ctx context.Context, msgProc *messageprocessing.MessageProcessing) (uuid.UUID, bool, error) {
	f.InsertIfNotExistsCallCount++
	if f.InsertIfNotExistsFunc != nil {
		return f.InsertIfNotExistsFunc(ctx, msgProc)
	}
	if f.seen == nil {
		f.seen = make(map[string]uuid.UUID)
	}
	key := msgProc.MessageID + "\x00" + msgProc.StreamName
	if id, exists := f.seen[key]; exists {
		return id, false, nil // already seen → duplicate
	}
	id := uuid.New()
	f.seen[key] = id
	return id, true, nil // newly inserted
}

func (f *FakeMessageProcessingRepository) GetByMessageIDAndStream(ctx context.Context, messageID, streamName string) (*messageprocessing.MessageProcessing, error) {
	if f.GetByMessageIDAndStreamFunc != nil {
		return f.GetByMessageIDAndStreamFunc(ctx, messageID, streamName)
	}
	if f.seen != nil {
		key := messageID + "\x00" + streamName
		if id, exists := f.seen[key]; exists {
			return &messageprocessing.MessageProcessing{
				ID:         id,
				MessageID:  messageID,
				StreamName: streamName,
				State:      "completed",
			}, nil
		}
	}
	return &messageprocessing.MessageProcessing{
		ID:         uuid.New(),
		MessageID:  messageID,
		StreamName: streamName,
		State:      "completed",
	}, nil
}

func (f *FakeMessageProcessingRepository) UpdateState(ctx context.Context, id uuid.UUID, state string) error {
	if f.UpdateStateFunc != nil {
		return f.UpdateStateFunc(ctx, id, state)
	}
	return nil
}
