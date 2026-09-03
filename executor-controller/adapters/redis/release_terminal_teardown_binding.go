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
// These consumers back up the kind-neutral pipeline.run.finished:v1 teardown:
// a candidate terminal that reached executor-controller on release.promoted:v1
// or release.rejected:v1 without a matching pipeline.run.finished:v1 (a message
// left in the group before that stream carried the teardown) still has its
// schema reclaimed here. The drop is idempotent, so a run whose schema the
// finished-event or validation-result path already reclaimed costs a harmless
// no-op.
//
// candidate_schema is always present on release.promoted (set by
// handle_validation_result.go). For normal validation releases the schema is
// already torn down by the validation.result:v1 terminal row (via
// NewValidationResultTeardownBinding), so DropCandidateSchema here is a no-op
// (idempotent drop of a schema that no longer exists). For the seed-only
// zero-validation path (seed-build only, no validation terminal row fires)
// this binding performs the live teardown.
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
// present. Mirrors NewValidationResultTeardownBinding's terminal-row teardown
// for the failed seed-build path.
func NewReleaseRejectedTeardownBinding(cleaner ports.CandidateSchemaCleaner, logger *slog.Logger) pkgredis.MessageHandler {
	return newReleaseTerminalTeardownBinding("release-rejected teardown", cleaner, logger)
}

// NewReleasePromotedTeardownBinding returns a pkg/redis.MessageHandler that
// consumes release.promoted:v1 and drops the release's candidate schema.
// candidate_schema is always present on release.promoted; the drop is a no-op
// for normal validation releases (already torn down by the validation.result:v1
// terminal row) and the live teardown for the seed-only zero-validation path.
func NewReleasePromotedTeardownBinding(cleaner ports.CandidateSchemaCleaner, logger *slog.Logger) pkgredis.MessageHandler {
	return newReleaseTerminalTeardownBinding("release-promoted teardown", cleaner, logger)
}
