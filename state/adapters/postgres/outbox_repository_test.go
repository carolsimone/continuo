package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newOutboxEntry() *postgres.OutboxEntry {
	// MessageProcessingID left nil — the FK column is nullable. Tests that
	// exercise the FK provenance link should seed a real message_processing
	// row and use its ID; these CRUD tests don't.
	return &postgres.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: nil,
		AggregateType:       "scheduler",
		AggregateID:         uuid.New(),
		EventType:           "rerun_node",
		Payload:             []byte(`{"schedule_id":"test"}`),
		StreamName:          "trigger.rerun:v1",
		Status:              "pending",
		MaxRetries:          3,
		RetryCount:          0,
		CreatedAt:           time.Now(),
	}
}

func TestOutboxRepository_CreateAndListPending(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewOutboxRepository(db, discardLogger())

	entry := newOutboxEntry()

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	err = repo.Create(context.Background(), tx, entry)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	defer db.ExecContext(context.Background(), "DELETE FROM state_outbox WHERE id = $1", entry.ID)

	pending, err := repo.ListPending(context.Background(), 10)
	require.NoError(t, err)

	found := false
	for _, e := range pending {
		if e.ID == entry.ID {
			found = true
			break
		}
	}
	assert.True(t, found, "created entry should appear in ListPending")
}

func TestOutboxRepository_MarkPublished(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewOutboxRepository(db, discardLogger())

	entry := newOutboxEntry()

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	err = repo.Create(context.Background(), tx, entry)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	defer db.ExecContext(context.Background(), "DELETE FROM state_outbox WHERE id = $1", entry.ID)

	err = repo.MarkPublished(context.Background(), entry.ID)
	require.NoError(t, err)

	pending, err := repo.ListPending(context.Background(), 10)
	require.NoError(t, err)

	for _, e := range pending {
		assert.NotEqual(t, entry.ID, e.ID, "published entry should not appear in ListPending")
	}
}
