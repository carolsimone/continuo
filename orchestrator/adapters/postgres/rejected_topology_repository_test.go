package postgres_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/adapters/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRejectedTopologyRepository_Insert_PersistsRow(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRejectedTopologyRepository(db)
	ctx := context.Background()

	// Use a unique message_id so the test is parallel-safe and self-cleanable.
	messageID := "msg-" + uuid.NewString()
	rawPayload := json.RawMessage(`{"nodes":[{"service_name":"svc-1","schema_name":"raw","table_name":"users","image_tag":""}]}`)
	reason := "image_tag empty for 1 node(s): svc-1/raw/users"

	t.Cleanup(func() {
		db.ExecContext(ctx, `DELETE FROM rejected_topology_messages WHERE message_id = $1`, messageID)
	})

	require.NoError(t, repo.Insert(ctx, messageID, reason, rawPayload))

	var (
		gotMessageID string
		gotReason    string
		gotPayload   []byte
	)
	row := db.QueryRowContext(ctx,
		`SELECT message_id, reason, payload
		   FROM rejected_topology_messages
		  WHERE message_id = $1`, messageID)
	require.NoError(t, row.Scan(&gotMessageID, &gotReason, &gotPayload))

	assert.Equal(t, messageID, gotMessageID)
	assert.Equal(t, reason, gotReason)
	assert.JSONEq(t, string(rawPayload), string(gotPayload))
}

func TestRejectedTopologyRepository_Insert_AllowsDuplicateMessageID(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRejectedTopologyRepository(db)
	ctx := context.Background()

	messageID := "dup-" + uuid.NewString()
	payload := json.RawMessage(`{}`)

	t.Cleanup(func() {
		db.ExecContext(ctx, `DELETE FROM rejected_topology_messages WHERE message_id = $1`, messageID)
	})

	require.NoError(t, repo.Insert(ctx, messageID, "first", payload))
	require.NoError(t, repo.Insert(ctx, messageID, "second", payload),
		"duplicate message_id must be allowed — XCLAIM redelivery must not raise unique violations")

	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT count(*) FROM rejected_topology_messages WHERE message_id = $1`, messageID,
	).Scan(&count))
	assert.Equal(t, 2, count, "both inserts must persist")
}
