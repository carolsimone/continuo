//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"

	executorredis "github.com/carolsimone/continuo/executor-controller/adapters/redis"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildValidationBinding constructs a ValidationRequestedBinding backed by the
// given *sqlx.DB, mirroring buildBinding for the query.model path.
func buildValidationBinding(db *sqlx.DB) func(ctx context.Context, msg goredis.XMessage) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	uowFactory := func() uow.UnitOfWork {
		return uow.NewPostgresUnitOfWork(db, logger)
	}
	handler := handlers.NewValidationRequestedHandler(logger)
	return executorredis.NewValidationRequestedBinding(uowFactory, handler, logger)
}

// validationNode is the per-node shape release-controller emits inside the
// validation.requested:v1 "payload" JSON body.
type validationNode struct {
	UniqueID    string `json:"unique_id"`
	ServiceName string `json:"service_name"`
	NodeType    string `json:"node_type"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	ImageTag    string `json:"image_tag"`
}

// validationRequestedXMessage builds a goredis.XMessage fixture for
// validation.requested:v1 carrying the given nodes for a release. The wire
// format is a single "payload" field with the flat JSON body, matching the
// outbox publisher and ParseValidationRequested.
func validationRequestedXMessage(t *testing.T, msgID, releaseID string, nodeIDs ...string) goredis.XMessage {
	t.Helper()
	nodes := make([]validationNode, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		nodes = append(nodes, validationNode{
			UniqueID:    id,
			ServiceName: "shop",
			NodeType:    "dbt-model",
			SchemaName:  "public",
			TableName:   id,
			ImageTag:    "sha-abc",
		})
	}
	body := map[string]interface{}{
		"release_id":        releaseID,
		"mode":              "validation",
		"nodes":             nodes,
		"node_ids_in_order": nodeIDs,
		"candidate_schema":  "candidate_" + releaseID,
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return goredis.XMessage{ID: msgID, Values: map[string]interface{}{
		"payload": string(raw),
	}}
}

func TestValidationRequestedBinding_HappyPath_EnqueuesAllNodes(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	binding := buildValidationBinding(db)
	releaseID := "rel-happy-1"
	msg := validationRequestedXMessage(t, "100-0", releaseID,
		"model.shop.orders", "model.shop.customers", "model.shop.line_items")

	require.NoError(t, binding(context.Background(), msg))

	assert.Equal(t, 3, countRows(t, db,
		`SELECT COUNT(*) FROM executor_deployments WHERE mode = 'validation' AND release_id = $1`, releaseID),
		"one validation deployment row per node")
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM executor_deployments WHERE mode = 'validation' AND release_id = $1 AND node_id = $2`,
		releaseID, "model.shop.orders"))
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE stream_name = $1`, streams.ValidationRequestedV1))
}

func TestValidationRequestedBinding_RedeliveredMessageIsDedupedAndAcked(t *testing.T) {
	// One validation.requested:v1 message carries every node for a release, so
	// dedup is per-release on a deterministic release-derived outbox_entry_id.
	// A redelivery with a fresh Redis message_id collides on that key: the
	// handler is skipped, no extra rows are written, and the binding returns
	// nil so the consumer ACKs.
	db, cleanup := setupPostgres(t)
	defer cleanup()

	binding := buildValidationBinding(db)
	releaseID := "rel-redeliver-1"
	nodeIDs := []string{"model.shop.orders", "model.shop.customers"}

	msg1 := validationRequestedXMessage(t, "200-0", releaseID, nodeIDs...)
	require.NoError(t, binding(context.Background(), msg1))

	// Redelivery with a DIFFERENT msg.ID — same release. Per-release dedup must
	// catch it; ACK (nil) and no duplicate rows.
	msg2 := validationRequestedXMessage(t, "201-0", releaseID, nodeIDs...)
	require.NoError(t, binding(context.Background(), msg2))

	assert.Equal(t, 2, countRows(t, db,
		`SELECT COUNT(*) FROM executor_deployments WHERE mode = 'validation' AND release_id = $1`, releaseID),
		"redelivered message must not re-enqueue the release's nodes")
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE stream_name = $1`, streams.ValidationRequestedV1),
		"per-release dedup also prevents a second message_processing row")
}

func TestValidationRequestedBinding_ParseFailureReturnsPermanent(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	binding := buildValidationBinding(db)
	// Missing the "payload" field entirely → permanent parse failure.
	msg := goredis.XMessage{ID: "300-0", Values: map[string]interface{}{
		"not_payload": "garbage",
	}}

	err := binding(context.Background(), msg)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent),
		"parse failure must wrap events.ErrPermanent so the consumer ACKs and drops")

	assert.Equal(t, 0, countRows(t, db,
		`SELECT COUNT(*) FROM executor_deployments`),
		"no rows written on parse failure")
	assert.Equal(t, 0, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing`),
		"no dedup row written on parse failure")
}
