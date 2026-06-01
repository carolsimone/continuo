// executor-controller/adapters/redis/validation_completed_teardown_binding.go
package redis

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/service/ports"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewValidationCompletedTeardownBinding returns a pkg/redis.MessageHandler that
// consumes validation.completed:v1 and drops the release's candidate schema from
// the dbt warehouse. validation.completed is emitted from two call sites (the
// node-completed handler and the dispatcher's fail-at-dispatch path), so reacting
// to the published event is a single, path-independent trigger that runs outside
// any write transaction. Teardown is best-effort: a parse or cleaner failure is
// logged and the message is ACKed (returns nil), because a leftover candidate
// schema must never block a release decision.
func NewValidationCompletedTeardownBinding(cleaner ports.CandidateSchemaCleaner, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		raw := stringField(msg.Values, "payload")
		if raw == "" {
			logger.Warn("validation.completed teardown: missing payload", "message_id", msg.ID)
			return nil
		}
		var dto struct {
			ReleaseID       string `json:"release_id"`
			CandidateSchema string `json:"candidate_schema"`
		}
		if err := json.Unmarshal([]byte(raw), &dto); err != nil {
			logger.Warn("validation.completed teardown: bad payload", "message_id", msg.ID, "error", err)
			return nil
		}
		if dto.CandidateSchema == "" {
			logger.Info("validation.completed teardown: no candidate_schema, skipping", "release_id", dto.ReleaseID)
			return nil
		}
		if err := cleaner.DropCandidateSchema(ctx, dto.CandidateSchema); err != nil {
			logger.Error("validation.completed teardown: drop failed (best-effort)",
				"release_id", dto.ReleaseID, "candidate_schema", dto.CandidateSchema, "error", err)
			return nil
		}
		logger.Info("validation.completed teardown: candidate schema dropped",
			"release_id", dto.ReleaseID, "candidate_schema", dto.CandidateSchema)
		return nil
	}
}
