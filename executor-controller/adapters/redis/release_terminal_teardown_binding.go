// executor-controller/adapters/redis/release_terminal_teardown_binding.go
package redis

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/service/ports"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// newReleaseTerminalTeardownBinding returns a pkg/redis.MessageHandler that
// consumes a terminal release stream (release.rejected:v1 or
// release.promoted:v1) and drops the release's candidate schema from the dbt
// warehouse when candidate_schema is present in the payload.
//
// These two streams are the terminal paths for seed-build releases that skip
// validation.completed (failed seed-build → release.rejected; seed-only
// zero-validation success → release.promoted). The validation.completed path
// already tears down via NewValidationCompletedTeardownBinding; that binding
// runs first, leaving candidate_schema empty or absent on the promote event for
// normal validation releases, so this consumer is a harmless no-op there.
//
// Teardown is best-effort: a parse or cleaner failure is logged and the message
// is ACKed (returns nil), because a leftover candidate schema must never block
// a release decision.
//
// label is a short human-readable name used only in log messages (e.g.
// "release.rejected teardown"); it must not be a raw stream-name literal — pass
// a descriptive tag instead so the no-inlined-stream-names lint stays green.
func newReleaseTerminalTeardownBinding(label string, cleaner ports.CandidateSchemaCleaner, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		raw := stringField(msg.Values, "payload")
		if raw == "" {
			logger.Warn("release terminal teardown: missing payload",
				"binding", label, "message_id", msg.ID)
			return nil
		}
		var dto struct {
			ReleaseID       string `json:"release_id"`
			CandidateSchema string `json:"candidate_schema"`
		}
		if err := json.Unmarshal([]byte(raw), &dto); err != nil {
			logger.Warn("release terminal teardown: bad payload",
				"binding", label, "message_id", msg.ID, "error", err)
			return nil
		}
		if dto.CandidateSchema == "" {
			logger.Info("release terminal teardown: no candidate_schema, skipping",
				"binding", label, "release_id", dto.ReleaseID)
			return nil
		}
		if err := cleaner.DropCandidateSchema(ctx, dto.CandidateSchema); err != nil {
			logger.Error("release terminal teardown: drop failed (best-effort)",
				"binding", label, "release_id", dto.ReleaseID,
				"candidate_schema", dto.CandidateSchema, "error", err)
			return nil
		}
		logger.Info("release terminal teardown: candidate schema dropped",
			"binding", label, "release_id", dto.ReleaseID,
			"candidate_schema", dto.CandidateSchema)
		return nil
	}
}

// NewReleaseRejectedTeardownBinding returns a pkg/redis.MessageHandler that
// consumes release.rejected:v1 and drops the release's candidate schema if
// present. Mirrors NewValidationCompletedTeardownBinding for the failed
// seed-build terminal path.
func NewReleaseRejectedTeardownBinding(cleaner ports.CandidateSchemaCleaner, logger *slog.Logger) pkgredis.MessageHandler {
	return newReleaseTerminalTeardownBinding("release-rejected teardown", cleaner, logger)
}

// NewReleasePromotedTeardownBinding returns a pkg/redis.MessageHandler that
// consumes release.promoted:v1 and drops the release's candidate schema if
// present. Mirrors NewValidationCompletedTeardownBinding for the seed-only
// zero-validation promote terminal path. For normal validation releases the
// candidate_schema field will be absent (validation.completed already cleaned
// up), so this is a no-op.
func NewReleasePromotedTeardownBinding(cleaner ports.CandidateSchemaCleaner, logger *slog.Logger) pkgredis.MessageHandler {
	return newReleaseTerminalTeardownBinding("release-promoted teardown", cleaner, logger)
}
