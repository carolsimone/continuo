package redis_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	stateRedis "github.com/carolsimone/continuo/state/adapters/redis"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeOutboxRepo implements postgres.OutboxRepository for unit tests.
type fakeOutboxRepo struct {
	entries        []*postgres.OutboxEntry
	markPublished  []uuid.UUID
	incrementRetry []uuid.UUID
}

func (f *fakeOutboxRepo) Create(_ context.Context, _ *sqlx.Tx, entry *postgres.OutboxEntry) error {
	f.entries = append(f.entries, entry)
	return nil
}
func (f *fakeOutboxRepo) ListPending(_ context.Context, limit int) ([]*postgres.OutboxEntry, error) {
	if limit > len(f.entries) {
		return f.entries, nil
	}
	return f.entries[:limit], nil
}
func (f *fakeOutboxRepo) MarkPublished(_ context.Context, id uuid.UUID) error {
	f.markPublished = append(f.markPublished, id)
	return nil
}
func (f *fakeOutboxRepo) IncrementRetry(_ context.Context, id uuid.UUID) error {
	f.incrementRetry = append(f.incrementRetry, id)
	return nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func pendingEntry() *postgres.OutboxEntry {
	return &postgres.OutboxEntry{
		ID:         uuid.New(),
		StreamName: "command.rerun:v1",
		Payload:    []byte(`{"schedule_id":"abc"}`),
		Status:     "pending",
		MaxRetries: 3,
		RetryCount: 0,
		CreatedAt:  time.Now(),
	}
}

func TestOutboxProcessor_PublishesAndMarksPublished(t *testing.T) {
	entry := pendingEntry()
	repo := &fakeOutboxRepo{entries: []*postgres.OutboxEntry{entry}}

	published := false
	processor := stateRedis.NewOutboxProcessorWithPublisher(repo, func(_ context.Context, stream string, _ map[string]interface{}) error {
		assert.Equal(t, "command.rerun:v1", stream)
		published = true
		return nil
	}, discardLogger())

	err := processor.ProcessBatch(context.Background())
	require.NoError(t, err)

	assert.True(t, published)
	require.Len(t, repo.markPublished, 1)
	assert.Equal(t, entry.ID, repo.markPublished[0])
	assert.Empty(t, repo.incrementRetry)
}

func TestOutboxProcessor_IncrementsRetryOnRedisFailure(t *testing.T) {
	entry := pendingEntry()
	repo := &fakeOutboxRepo{entries: []*postgres.OutboxEntry{entry}}

	processor := stateRedis.NewOutboxProcessorWithPublisher(repo, func(_ context.Context, _ string, _ map[string]interface{}) error {
		return errors.New("redis unavailable")
	}, discardLogger())

	err := processor.ProcessBatch(context.Background())
	require.NoError(t, err)

	assert.Empty(t, repo.markPublished)
	require.Len(t, repo.incrementRetry, 1)
	assert.Equal(t, entry.ID, repo.incrementRetry[0])
}

func TestOutboxProcessor_UsesStreamNameFromEntry(t *testing.T) {
	entry := &postgres.OutboxEntry{
		ID:         uuid.New(),
		StreamName: "scheduler.started:v1",
		Payload:    []byte(`{"runner_id":"abc","schedule_name":"daily"}`),
		Status:     "pending",
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}
	repo := &fakeOutboxRepo{entries: []*postgres.OutboxEntry{entry}}

	var publishedStream string
	processor := stateRedis.NewOutboxProcessorWithPublisher(repo, func(_ context.Context, stream string, _ map[string]interface{}) error {
		publishedStream = stream
		return nil
	}, discardLogger())

	require.NoError(t, processor.ProcessBatch(context.Background()))
	assert.Equal(t, "scheduler.started:v1", publishedStream)
}

func TestOutboxProcessor_StringifiesNestedManifestVersionsForRedis(t *testing.T) {
	entry := &postgres.OutboxEntry{
		ID:         uuid.New(),
		StreamName: "scheduler.started:v1",
		Payload: []byte(`{
			"runner_id":"abc",
			"schedule_name":"daily",
			"manifest_versions":{"svc-a":"v3","svc-b":"v5"}
		}`),
		Status:     "pending",
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}
	repo := &fakeOutboxRepo{entries: []*postgres.OutboxEntry{entry}}

	var publishedFields map[string]interface{}
	processor := stateRedis.NewOutboxProcessorWithPublisher(repo, func(_ context.Context, _ string, fields map[string]interface{}) error {
		publishedFields = fields
		return nil
	}, discardLogger())

	require.NoError(t, processor.ProcessBatch(context.Background()))
	require.NotNil(t, publishedFields)
	assert.Equal(t, `{"svc-a":"v3","svc-b":"v5"}`, publishedFields["manifest_versions"])
}
