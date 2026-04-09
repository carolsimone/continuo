package fakes

import (
	"context"

	"github.com/carolsimone/continuo/dependency-controller/domain/model"
	"github.com/google/uuid"
)

// FakePublishedMessagesRepository is a fake implementation of postgres.PublishedMessagesRepository
type FakePublishedMessagesRepository struct {
	ExistsFunc func(ctx context.Context, outboxEntryID uuid.UUID) (bool, error)
	CreateFunc func(ctx context.Context, pm *model.PublishedMessage) error

	CreatedMessages []*model.PublishedMessage
	existingIDs     map[uuid.UUID]bool
}

func NewFakePublishedMessagesRepository() *FakePublishedMessagesRepository {
	return &FakePublishedMessagesRepository{
		existingIDs: make(map[uuid.UUID]bool),
	}
}

func (f *FakePublishedMessagesRepository) Exists(ctx context.Context, outboxEntryID uuid.UUID) (bool, error) {
	if f.ExistsFunc != nil {
		return f.ExistsFunc(ctx, outboxEntryID)
	}
	return f.existingIDs[outboxEntryID], nil
}

func (f *FakePublishedMessagesRepository) Create(ctx context.Context, pm *model.PublishedMessage) error {
	f.CreatedMessages = append(f.CreatedMessages, pm)
	f.existingIDs[pm.OutboxEntryID] = true
	if f.CreateFunc != nil {
		return f.CreateFunc(ctx, pm)
	}
	return nil
}
