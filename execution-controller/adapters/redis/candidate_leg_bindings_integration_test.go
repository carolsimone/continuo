//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	executorredis "github.com/carolsimone/continuo/execution-controller/adapters/redis"
	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// payloadXMessage wraps body as the single "payload" field the release-leg streams carry.
func payloadXMessage(t *testing.T, msgID string, body map[string]any) goredis.XMessage {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return goredis.XMessage{ID: msgID, Values: map[string]interface{}{"payload": string(raw)}}
}

func TestSeedBuildRequestedBinding_MarksDedupRowCompleted(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	binding := executorredis.NewSeedBuildRequestedBinding(
		func() uow.UnitOfWork { return postgres.NewPostgresUnitOfWork(db, logger) },
		handlers.NewSeedBuildRequestedHandler(logger), &recordingSchemaCreator{}, logger)
	releaseID := "rel-seed-completed-1"
	require.NoError(t, binding(context.Background(), payloadXMessage(t, "400-0", map[string]any{
		"release_id": releaseID,
		"mode":       "seed_build",
		"seeds": []map[string]any{{
			"unique_id": "seed.shop.country_codes", "service_name": "shop", "node_type": "dbt-seed",
			"schema_name": "public", "table_name": "country_codes", "image_tag": "sha-seed",
		}},
		"seed_ids_in_order": []string{"seed.shop.country_codes"},
		"candidate_schema":  "_candidate_rel_seed_completed_1",
	})))
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM deployments WHERE mode = 'seed_build' AND release_id = $1`, releaseID))
	assert.Equal(t, messageprocessing.StateCompleted, dedupState(t, db, streams.SeedBuildRequestedV1),
		"the dedup row is marked completed in the handler's transaction")
}

func TestCompileRequestedBinding_MarksDedupRowCompleted(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	binding := executorredis.NewCompileRequestedBinding(
		func() uow.UnitOfWork { return postgres.NewPostgresUnitOfWork(db, logger) },
		handlers.NewCompileRequestedHandler(logger), logger)
	releaseID := "rel-compile-completed-1"
	require.NoError(t, binding(context.Background(), payloadXMessage(t, "500-0", map[string]any{
		"release_id": releaseID, "service": "finance", "image_tag": "sha-compile", "bucket": "continuo",
	})))
	assert.Equal(t, 1, countRows(t, db,
		`SELECT COUNT(*) FROM deployments WHERE mode = 'compile' AND release_id = $1`, releaseID))
	assert.Equal(t, messageprocessing.StateCompleted, dedupState(t, db, streams.CompileRequestedV1),
		"the dedup row is marked completed in the handler's transaction")
}
