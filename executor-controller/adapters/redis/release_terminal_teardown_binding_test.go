package redis_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	redis "github.com/carolsimone/continuo/executor-controller/adapters/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// --- release.rejected:v1 teardown ---

func TestReleaseRejectedTeardownBinding_DropsCandidateSchema(t *testing.T) {
	var dropped string
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { dropped = schema; return nil })
	h := redis.NewReleaseRejectedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"release_id":"rel","reason":"seed_build_failed","candidate_schema":"_candidate_rel"}`,
	}}
	require.NoError(t, h(context.Background(), msg))
	require.Equal(t, "_candidate_rel", dropped)
}

func TestReleaseRejectedTeardownBinding_BestEffortOnCleanerError(t *testing.T) {
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { return errors.New("boom") })
	h := redis.NewReleaseRejectedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"release_id":"rel","candidate_schema":"_candidate_rel"}`,
	}}
	// best-effort: cleaner error is logged, message ACKed (nil)
	require.NoError(t, h(context.Background(), msg))
}

func TestReleaseRejectedTeardownBinding_NoSchemaIsNoop(t *testing.T) {
	called := false
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { called = true; return nil })
	h := redis.NewReleaseRejectedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{"payload": `{"release_id":"rel","reason":"seed_build_failed"}`}}
	require.NoError(t, h(context.Background(), msg))
	require.False(t, called) // no candidate_schema => nothing to drop
}

func TestReleaseRejectedTeardownBinding_MissingPayloadIsNoop(t *testing.T) {
	called := false
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { called = true; return nil })
	h := redis.NewReleaseRejectedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{}}
	require.NoError(t, h(context.Background(), msg))
	require.False(t, called)
}

// --- release.promoted:v1 teardown ---

func TestReleasePromotedTeardownBinding_DropsCandidateSchema(t *testing.T) {
	var dropped string
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { dropped = schema; return nil })
	h := redis.NewReleasePromotedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"release_id":"rel","topology":[],"candidate_schema":"_candidate_rel"}`,
	}}
	require.NoError(t, h(context.Background(), msg))
	require.Equal(t, "_candidate_rel", dropped)
}

func TestReleasePromotedTeardownBinding_BestEffortOnCleanerError(t *testing.T) {
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { return errors.New("boom") })
	h := redis.NewReleasePromotedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"release_id":"rel","candidate_schema":"_candidate_rel"}`,
	}}
	// best-effort: cleaner error is logged, message ACKed (nil)
	require.NoError(t, h(context.Background(), msg))
}

func TestReleasePromotedTeardownBinding_NoSchemaIsNoop(t *testing.T) {
	// Normal validation-pass path: candidate_schema absent because validation.completed
	// already tore it down. This consumer must be a no-op.
	called := false
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { called = true; return nil })
	h := redis.NewReleasePromotedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"release_id":"rel","topology":[],"image_tags":{}}`,
	}}
	require.NoError(t, h(context.Background(), msg))
	require.False(t, called)
}

func TestReleasePromotedTeardownBinding_MissingPayloadIsNoop(t *testing.T) {
	called := false
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { called = true; return nil })
	h := redis.NewReleasePromotedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{}}
	require.NoError(t, h(context.Background(), msg))
	require.False(t, called)
}
