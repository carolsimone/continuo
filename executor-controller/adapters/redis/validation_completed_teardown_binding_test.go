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

// cleanerFunc adapts a func to ports.CandidateSchemaCleaner.
type cleanerFunc func(ctx context.Context, schema string) error

func (f cleanerFunc) DropCandidateSchema(ctx context.Context, schema string) error {
	return f(ctx, schema)
}

func TestValidationCompletedTeardownBinding_DropsCandidateSchema(t *testing.T) {
	var dropped string
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { dropped = schema; return nil })
	h := redis.NewValidationCompletedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"release_id":"rel","aggregate_status":"ok","candidate_schema":"_candidate_rel","per_node_results":[]}`,
	}}
	require.NoError(t, h(context.Background(), msg))
	require.Equal(t, "_candidate_rel", dropped)
}

func TestValidationCompletedTeardownBinding_BestEffortOnCleanerError(t *testing.T) {
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { return errors.New("boom") })
	h := redis.NewValidationCompletedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"release_id":"rel","candidate_schema":"_candidate_rel"}`,
	}}
	// best-effort: cleaner error is logged, message ACKed (nil)
	require.NoError(t, h(context.Background(), msg))
}

func TestValidationCompletedTeardownBinding_NoSchemaIsNoop(t *testing.T) {
	called := false
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { called = true; return nil })
	h := redis.NewValidationCompletedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{"payload": `{"release_id":"rel"}`}}
	require.NoError(t, h(context.Background(), msg))
	require.False(t, called) // no candidate_schema => nothing to drop
}
