package redis_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	redis "github.com/carolsimone/continuo/execution-controller/adapters/redis"
	"github.com/carolsimone/continuo/execution-controller/service/ports"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Every teardown binding swallows a cleaner failure so a leftover candidate
// schema never blocks a decision, except when the handler's context is done:
// pkg/redis acknowledges a nil result, so a drop cut short by shutdown would
// otherwise be acknowledged unfinished instead of retried after the restart.
func TestTeardownBindings_ReturnCleanerErrorOnlyWhenContextIsDone(t *testing.T) {
	bindings := []struct {
		name   string
		build  func(ports.CandidateSchemaCleaner, *slog.Logger) pkgredis.MessageHandler
		values map[string]any
	}{
		{"validation.result", redis.NewValidationResultTeardownBinding,
			map[string]any{"payload": `{"kind":"complete","release_id":"rel","candidate_schema":"_candidate_rel"}`}},
		{"pipeline.run.finished", redis.NewPipelineRunFinishedTeardownBinding,
			map[string]any{"payload": `{"run_id":"rel","run_kind":"candidate","outcome":"rejected","candidate_schema":"_candidate_rel"}`}},
		{"release.rejected", redis.NewReleaseRejectedTeardownBinding,
			map[string]any{"payload": `{"release_id":"rel","candidate_schema":"_candidate_rel"}`}},
		{"release.promoted", redis.NewReleasePromotedTeardownBinding, releasePromotedFields(t, "_candidate_rel")},
	}
	for _, b := range bindings {
		t.Run(b.name, func(t *testing.T) {
			dropErr := errors.New("drop failed")
			cleaner := cleanerFunc(func(context.Context, string) error { return dropErr })
			h := b.build(cleaner, slog.Default())
			msg := goredis.XMessage{ID: "1-0", Values: b.values}

			t.Run("live context stays best-effort", func(t *testing.T) {
				require.NoError(t, h(context.Background(), msg))
			})

			t.Run("cancelled context returns the cleaner error", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				require.ErrorIs(t, h(ctx, msg), dropErr)
			})

			t.Run("expired deadline returns the cleaner error", func(t *testing.T) {
				ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
				defer cancel()
				require.ErrorIs(t, h(ctx, msg), dropErr)
			})
		})
	}
}
