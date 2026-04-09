package fakes

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/dependency-controller/adapters/postgres"
	"github.com/carolsimone/continuo/dependency-controller/domain/model"
	"github.com/google/uuid"
)

// FakeOutboxRepository is a fake implementation of postgres.OutboxRepository
type FakeOutboxRepository struct {
	CreateFunc          func(ctx context.Context, entry *model.OutboxEntry) error
	GetPendingBatchFunc func(ctx context.Context, limit int) ([]*model.OutboxEntry, error)
	MarkProcessedFunc   func(ctx context.Context, id uuid.UUID) error
	MarkFailedFunc      func(ctx context.Context, id uuid.UUID, errorMessage string) error
	IncrementRetryFunc  func(ctx context.Context, id uuid.UUID) error
	UpdateStatusFunc    func(ctx context.Context, id uuid.UUID, newStatus, expectedStatus string) error

	CreatedEntries []*model.OutboxEntry
}

func (f *FakeOutboxRepository) Create(ctx context.Context, entry *model.OutboxEntry) error {
	f.CreatedEntries = append(f.CreatedEntries, entry)
	if f.CreateFunc != nil {
		return f.CreateFunc(ctx, entry)
	}
	return nil
}

func (f *FakeOutboxRepository) GetPendingBatch(ctx context.Context, limit int) ([]*model.OutboxEntry, error) {
	if f.GetPendingBatchFunc != nil {
		return f.GetPendingBatchFunc(ctx, limit)
	}
	return nil, nil
}

func (f *FakeOutboxRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	if f.MarkProcessedFunc != nil {
		return f.MarkProcessedFunc(ctx, id)
	}
	return nil
}

func (f *FakeOutboxRepository) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	if f.MarkFailedFunc != nil {
		return f.MarkFailedFunc(ctx, id, errorMessage)
	}
	return nil
}

func (f *FakeOutboxRepository) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	if f.IncrementRetryFunc != nil {
		return f.IncrementRetryFunc(ctx, id)
	}
	return nil
}

func (f *FakeOutboxRepository) UpdateStatus(ctx context.Context, id uuid.UUID, newStatus, expectedStatus string) error {
	if f.UpdateStatusFunc != nil {
		return f.UpdateStatusFunc(ctx, id, newStatus, expectedStatus)
	}
	return nil
}

// FakeMessageProcessingRepository is a fake implementation of postgres.MessageProcessingRepository
type FakeMessageProcessingRepository struct {
	InsertIfNotExistsFunc func(ctx context.Context, msgProc *model.MessageProcessing) (uuid.UUID, bool, error)
	GetByMessageIDFunc    func(ctx context.Context, messageID string) (*model.MessageProcessing, error)
	UpdateStateFunc       func(ctx context.Context, id uuid.UUID, state model.MessageProcessingState) error

	InsertedMessages map[string]*model.MessageProcessing
}

func NewFakeMessageProcessingRepository() *FakeMessageProcessingRepository {
	return &FakeMessageProcessingRepository{
		InsertedMessages: make(map[string]*model.MessageProcessing),
	}
}

func (f *FakeMessageProcessingRepository) InsertIfNotExists(ctx context.Context, msgProc *model.MessageProcessing) (uuid.UUID, bool, error) {
	if f.InsertIfNotExistsFunc != nil {
		return f.InsertIfNotExistsFunc(ctx, msgProc)
	}

	// Default behavior: track message and return new ID
	if _, exists := f.InsertedMessages[msgProc.MessageID]; exists {
		return f.InsertedMessages[msgProc.MessageID].ID, false, nil
	}

	msgProc.ID = uuid.New()
	f.InsertedMessages[msgProc.MessageID] = msgProc
	return msgProc.ID, true, nil
}

func (f *FakeMessageProcessingRepository) GetByMessageID(ctx context.Context, messageID string) (*model.MessageProcessing, error) {
	if f.GetByMessageIDFunc != nil {
		return f.GetByMessageIDFunc(ctx, messageID)
	}

	msg, exists := f.InsertedMessages[messageID]
	if !exists {
		return nil, fmt.Errorf("message not found: %s", messageID)
	}
	return msg, nil
}

func (f *FakeMessageProcessingRepository) UpdateState(ctx context.Context, id uuid.UUID, state model.MessageProcessingState) error {
	if f.UpdateStateFunc != nil {
		return f.UpdateStateFunc(ctx, id, state)
	}

	// Default behavior: find and update state
	for _, msg := range f.InsertedMessages {
		if msg.ID == id {
			msg.State = state
			return nil
		}
	}
	return nil
}

// FakeUnitOfWork is a fake implementation of uow.UnitOfWork
type FakeUnitOfWork struct {
	OutboxRepository  *FakeOutboxRepository
	msgProcessingRepo *FakeMessageProcessingRepository
	BegunTx           bool
	CommittedTx       bool
	RolledBackTx      bool
}

func NewFakeUnitOfWork() *FakeUnitOfWork {
	return &FakeUnitOfWork{
		OutboxRepository:  &FakeOutboxRepository{},
		msgProcessingRepo: NewFakeMessageProcessingRepository(),
	}
}

func (f *FakeUnitOfWork) OutboxRepo() postgres.OutboxRepository {
	return f.OutboxRepository
}

func (f *FakeUnitOfWork) MessageProcessingRepo() postgres.MessageProcessingRepository {
	return f.msgProcessingRepo
}

func (f *FakeUnitOfWork) Begin(ctx context.Context) error {
	f.BegunTx = true
	return nil
}

func (f *FakeUnitOfWork) Commit() error {
	f.CommittedTx = true
	return nil
}

func (f *FakeUnitOfWork) Rollback() error {
	f.RolledBackTx = true
	return nil
}
