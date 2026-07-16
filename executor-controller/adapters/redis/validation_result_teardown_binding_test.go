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

func TestValidationResultTeardownBinding_CompleteDropsCandidateSchema(t *testing.T) {
	var dropped string
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { dropped = schema; return nil })
	h := redis.NewValidationResultTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"kind":"complete","release_id":"rel","aggregate_status":"ok","candidate_schema":"_candidate_rel"}`,
	}}
	require.NoError(t, h(context.Background(), msg))
	require.Equal(t, "_candidate_rel", dropped)
}

func TestValidationResultTeardownBinding_BestEffortOnCleanerError(t *testing.T) {
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { return errors.New("boom") })
	h := redis.NewValidationResultTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"kind":"complete","release_id":"rel","candidate_schema":"_candidate_rel"}`,
	}}
	// best-effort: cleaner error is logged, message ACKed (nil)
	require.NoError(t, h(context.Background(), msg))
}

func TestValidationResultTeardownBinding_CompleteNoSchemaIsNoop(t *testing.T) {
	called := false
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { called = true; return nil })
	h := redis.NewValidationResultTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{"payload": `{"kind":"complete","release_id":"rel"}`}}
	require.NoError(t, h(context.Background(), msg))
	require.False(t, called) // no candidate_schema => nothing to drop
}

// A "kind":"node" per-node row must be acked without ever touching the
// candidate schema — dropping it mid-validation would break the still
// in-flight sibling nodes on the same release.
func TestValidationResultTeardownBinding_NodeKindIsAckedWithoutDrop(t *testing.T) {
	called := false
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { called = true; return nil })
	h := redis.NewValidationResultTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"kind":"node","release_id":"rel","stage":"validation","node_id":"n1","status":"ok","candidate_schema":"_candidate_rel"}`,
	}}
	require.NoError(t, h(context.Background(), msg))
	require.False(t, called, "per-node rows must never trigger candidate-schema teardown")
}

// An unrecognized/missing kind is acked without any teardown action, matching
// the best-effort contract: only a "complete" row triggers a drop.
func TestValidationResultTeardownBinding_UnknownKindIsAckedWithoutDrop(t *testing.T) {
	called := false
	cleaner := cleanerFunc(func(ctx context.Context, schema string) error { called = true; return nil })
	h := redis.NewValidationResultTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{
		"payload": `{"release_id":"rel","candidate_schema":"_candidate_rel"}`,
	}}
	require.NoError(t, h(context.Background(), msg))
	require.False(t, called)
}
