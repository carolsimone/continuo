//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	executorpg "github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	executorredis "github.com/carolsimone/continuo/executor-controller/adapters/redis"
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildNodeCompletedBinding constructs a ValidationNodeCompletedBinding backed
// by the given *sqlx.DB.
func buildNodeCompletedBinding(db *sqlx.DB) func(ctx context.Context, msg goredis.XMessage) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	uowFactory := func() uow.UnitOfWork {
		return uow.NewPostgresUnitOfWork(db, logger)
	}
	handler := handlers.NewValidationNodeCompletedHandler(logger)
	return executorredis.NewValidationNodeCompletedBinding(uowFactory, handler, logger)
}

// seedDeployedValidationNode inserts a mode=validation row for (releaseID,
// nodeID) and drives it to status=deployed (the state a node sits in after the
// dispatcher dispatches its Job, before the terminal outcome arrives).
func seedDeployedValidationNode(t *testing.T, db *sqlx.DB, releaseID, nodeID string) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := executorpg.NewDeploymentsRepository(db, logger)
	now := time.Now()
	dep := model.NewValidationDeployment(command.ValidationDeployTask{
		ReleaseID: releaseID, NodeID: nodeID, ServiceName: "shop",
		SchemaName: "public", TableName: nodeID, NodeType: "dbt-model",
		ImageTag: "sha-abc", JobName: "validate-" + nodeID,
	}, nil, now, false)
	require.NoError(t, repo.Add(context.Background(), dep))
	require.NoError(t, dep.MarkDeployed(now))
	require.NoError(t, repo.Save(context.Background(), dep))
}

// nodeCompletedXMessage builds a goredis.XMessage fixture for
// validation.node.completed:v1. The wire format is a single "payload" field
// with the flat JSON body, matching the k8s-controller outbox publisher.
func nodeCompletedXMessage(t *testing.T, msgID, releaseID, nodeID, outcome string) goredis.XMessage {
	t.Helper()
	body := map[string]interface{}{
		"release_id":  releaseID,
		"node_id":     nodeID,
		"outcome":     outcome,
		"dbt_log_uri": "s3://logs/" + releaseID + "/" + nodeID,
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return goredis.XMessage{ID: msgID, Values: map[string]interface{}{
		"payload": string(raw),
	}}
}

func TestValidationNodeCompletedBinding_RecordsOutcome(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	releaseID := "rel-nc-1"
	nodeID := "model.shop.orders"
	seedDeployedValidationNode(t, db, releaseID, nodeID)

	binding := buildNodeCompletedBinding(db)
	msg := nodeCompletedXMessage(t, "400-0", releaseID, nodeID, "ok")
	require.NoError(t, binding(context.Background(), msg))

	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM executor_deployments
		 WHERE mode='validation' AND release_id=$1 AND node_id=$2
		   AND outcome='ok' AND outcome_at IS NOT NULL`, releaseID, nodeID),
		"outcome recorded on the matching deployment row")
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE stream_name = $1`,
		streams.ValidationNodeCompletedV1))
	// Single-node release: this terminal node triggers the aggregate emission.
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM executor_outbox WHERE stream_name = $1`,
		streams.ValidationCompletedV1),
		"aggregate validation terminal (kind=complete) emitted once the only node is terminal")
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM validation_aggregates WHERE release_id = $1`, releaseID),
		"sentinel claimed exactly once")
}

func TestValidationNodeCompletedBinding_RedeliveryIsDeduped(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	releaseID := "rel-nc-redeliver"
	nodeID := "model.shop.orders"
	seedDeployedValidationNode(t, db, releaseID, nodeID)

	binding := buildNodeCompletedBinding(db)
	msg := nodeCompletedXMessage(t, "401-0", releaseID, nodeID, "ok")

	require.NoError(t, binding(context.Background(), msg))
	// Redelivery with the SAME msg.ID: standard (msg.ID, stream_name) dedup skips
	// the handler entirely; the binding commits an empty txn and ACKs.
	require.NoError(t, binding(context.Background(), msg))

	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM message_processing WHERE message_id=$1 AND stream_name=$2`,
		msg.ID, streams.ValidationNodeCompletedV1))
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM executor_outbox WHERE stream_name=$1`,
		streams.ValidationCompletedV1),
		"redelivery must not emit a second aggregate")
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM validation_aggregates WHERE release_id=$1`, releaseID))
}

func TestValidationNodeCompletedBinding_ParseFailureReturnsPermanent(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	binding := buildNodeCompletedBinding(db)
	// Missing the "payload" field entirely → permanent parse failure.
	msg := goredis.XMessage{ID: "402-0", Values: map[string]interface{}{"not_payload": "garbage"}}

	err := binding(context.Background(), msg)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkgevents.ErrPermanent),
		"parse failure must wrap events.ErrPermanent so the consumer ACKs and drops")

	assert.Equal(t, 0, countRows(t, db, `SELECT COUNT(*) FROM message_processing`),
		"no dedup row written on parse failure")
}
