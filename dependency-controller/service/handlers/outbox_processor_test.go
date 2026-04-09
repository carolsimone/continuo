package handlers_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/dependency-controller/adapters/redis"
	"github.com/carolsimone/continuo/dependency-controller/domain/model"
	"github.com/carolsimone/continuo/dependency-controller/service/handlers"
	"github.com/carolsimone/continuo/dependency-controller/test/fakes"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func pendingEntry() *model.OutboxEntry {
	return &model.OutboxEntry{
		ID:         uuid.New(),
		Status:     string(model.OutboxStatusPending),
		RetryCount: 0,
		MaxRetries: 3,
	}
}

// TestProcessBatch_UpdateStatusFails_DoesNotMarkProcessed verifies the fix for the nil-return bug:
// when UpdateStatus fails (another instance claimed the entry), MarkProcessed must NOT be called.
func TestProcessBatch_UpdateStatusFails_DoesNotMarkProcessed(t *testing.T) {
	ctx := context.Background()
	entry := pendingEntry()

	outboxRepo := &fakes.FakeOutboxRepository{
		GetPendingBatchFunc: func(ctx context.Context, limit int) ([]*model.OutboxEntry, error) {
			return []*model.OutboxEntry{entry}, nil
		},
		UpdateStatusFunc: func(ctx context.Context, id uuid.UUID, newStatus, expectedStatus string) error {
			return errors.New("status mismatch or entry not found")
		},
		MarkProcessedFunc: func(ctx context.Context, id uuid.UUID) error {
			t.Errorf("MarkProcessed must NOT be called when UpdateStatus fails, but was called for %s", id)
			return nil
		},
	}
	publishedRepo := fakes.NewFakePublishedMessagesRepository()
	// nil producer is safe here because we never reach Publish
	processor := handlers.NewOutboxProcessor(outboxRepo, publishedRepo, (*redis.Producer)(nil), newLogger())

	err := processor.ProcessBatch(ctx)
	require.NoError(t, err)
}

// TestProcessBatch_PublishFails_StatusResetToPending verifies that when publishing to Redis
// fails after claiming the entry (status='publishing'), IncrementRetry is called so that
// the outbox_repository resets status back to 'pending' for the next attempt.
// This test verifies the CONTRACT expected of IncrementRetry — the SQL fix is in Task 5.
func TestProcessBatch_PublishFails_StatusResetToPending(t *testing.T) {
	ctx := context.Background()
	entry := pendingEntry()

	incrementRetryCalled := false
	claimedID := uuid.Nil

	outboxRepo := &fakes.FakeOutboxRepository{
		GetPendingBatchFunc: func(ctx context.Context, limit int) ([]*model.OutboxEntry, error) {
			return []*model.OutboxEntry{entry}, nil
		},
		UpdateStatusFunc: func(ctx context.Context, id uuid.UUID, newStatus, expectedStatus string) error {
			claimedID = id
			return nil // claim succeeds
		},
		IncrementRetryFunc: func(ctx context.Context, id uuid.UUID) error {
			incrementRetryCalled = true
			assert.Equal(t, claimedID, id, "IncrementRetry should be called for the claimed entry")
			return nil
		},
	}
	publishedRepo := fakes.NewFakePublishedMessagesRepository()

	// Unmarshal fails (invalid payload), causing processEntry to return a non-nil,
	// non-errAlreadyClaimed error after UpdateStatus succeeds — simulating a publish failure path.
	entry.Payload = []byte(`not valid json`)

	processor := handlers.NewOutboxProcessor(outboxRepo, publishedRepo, (*redis.Producer)(nil), newLogger())

	err := processor.ProcessBatch(ctx)
	require.NoError(t, err)
	assert.True(t, incrementRetryCalled, "IncrementRetry must be called when processEntry fails after claiming")
}

// TestProcessBatch_UpdateStatusFails_DoesNotIncrementRetry verifies that a "already claimed"
// skip does not count as a retry failure for the entry.
func TestProcessBatch_UpdateStatusFails_DoesNotIncrementRetry(t *testing.T) {
	ctx := context.Background()
	entry := pendingEntry()
	incrementCalled := false

	outboxRepo := &fakes.FakeOutboxRepository{
		GetPendingBatchFunc: func(ctx context.Context, limit int) ([]*model.OutboxEntry, error) {
			return []*model.OutboxEntry{entry}, nil
		},
		UpdateStatusFunc: func(ctx context.Context, id uuid.UUID, newStatus, expectedStatus string) error {
			return errors.New("status mismatch or entry not found")
		},
		IncrementRetryFunc: func(ctx context.Context, id uuid.UUID) error {
			incrementCalled = true
			return nil
		},
	}
	publishedRepo := fakes.NewFakePublishedMessagesRepository()
	processor := handlers.NewOutboxProcessor(outboxRepo, publishedRepo, (*redis.Producer)(nil), newLogger())

	err := processor.ProcessBatch(ctx)
	require.NoError(t, err)
	assert.False(t, incrementCalled, "IncrementRetry must NOT be called when UpdateStatus fails")
}
