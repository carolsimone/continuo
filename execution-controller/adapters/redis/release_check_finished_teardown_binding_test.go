package redis_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	redis "github.com/carolsimone/continuo/execution-controller/adapters/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestReleaseCheckFinishedTeardownBinding_DropsCandidateSchemaForEitherKind(t *testing.T) {
	for _, tc := range []struct{ name, payload string }{
		{"candidate rejected", `{"check_id":"rel-1","check_kind":"candidate","outcome":"rejected","candidate_schema":"_candidate_rel_1"}`},
		{"candidate promoted", `{"check_id":"rel-1","check_kind":"candidate","outcome":"promoted","candidate_schema":"_candidate_rel_1"}`},
		{"fix passed", `{"check_id":"verify-rel-1-core-a1","check_kind":"fix","outcome":"passed","candidate_schema":"_candidate_verify_rel_1_core_a1"}`},
		{"fix failed", `{"check_id":"verify-rel-1-core-a1","check_kind":"fix","outcome":"failed","candidate_schema":"_candidate_verify_rel_1_core_a1"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dropped string
			cleaner := cleanerFunc(func(_ context.Context, schema string) error { dropped = schema; return nil })
			h := redis.NewReleaseCheckFinishedTeardownBinding(cleaner, slog.Default())
			require.NoError(t, h(context.Background(), goredis.XMessage{Values: map[string]any{"payload": tc.payload}}))
			require.NotEmpty(t, dropped)
		})
	}
}

func TestReleaseCheckFinishedTeardownBinding_BestEffortOnCleanerError(t *testing.T) {
	cleaner := cleanerFunc(func(context.Context, string) error { return errors.New("boom") })
	h := redis.NewReleaseCheckFinishedTeardownBinding(cleaner, slog.Default())
	msg := goredis.XMessage{Values: map[string]any{"payload": `{"check_id":"r","candidate_schema":"_candidate_r"}`}}
	require.NoError(t, h(context.Background(), msg), "a cleaner failure must be logged and ACKed, never redelivered")
}

func TestReleaseCheckFinishedTeardownBinding_NoSchemaIsNoop(t *testing.T) {
	called := false
	cleaner := cleanerFunc(func(context.Context, string) error { called = true; return nil })
	h := redis.NewReleaseCheckFinishedTeardownBinding(cleaner, slog.Default())
	require.NoError(t, h(context.Background(), goredis.XMessage{Values: map[string]any{"payload": `{"check_id":"r"}`}}))
	require.False(t, called)
}
