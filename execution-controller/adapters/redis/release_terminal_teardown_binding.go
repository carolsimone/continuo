// execution-controller/adapters/redis/release_terminal_teardown_binding.go
package redis

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// dropCandidateSchema drops a release's candidate schema from the dbt warehouse
// once a terminal release stream (release.rejected:v1 or release.promoted:v2)
// names it.
//
// These consumers back up the kind-neutral pipeline.run.finished:v1 teardown:
// a candidate terminal that reached execution-controller on a terminal release
// stream without a matching pipeline.run.finished:v1 (a message left in the
// group before that stream carried the teardown) still has its schema reclaimed
// here. The drop is idempotent, so a run whose schema the finished-event or
// validation-result path already reclaimed costs a harmless no-op.
//
// Teardown is best-effort: a cleaner failure is logged and the message is ACKed
// (nil), because a leftover candidate schema must never block a release
// decision. A cleaner failure that arrives with a done ctx (shutdown, or the
// handler deadline) is returned instead, so the drop is retried rather than
// acknowledged unfinished.
//
// label is a short human-readable name used only in log messages (e.g.
// "release-rejected teardown"); it must not be a raw stream-name literal — pass
// a descriptive tag instead so the no-inlined-stream-names lint stays green.
func dropCandidateSchema(ctx context.Context, cleaner ports.CandidateSchemaCleaner, logger *slog.Logger,
	label, releaseID, candidateSchema string) error {
	if candidateSchema == "" {
		logger.Info("release terminal teardown: no candidate_schema, skipping",
			"binding", label, "release_id", releaseID)
		return nil
	}
	if err := cleaner.DropCandidateSchema(ctx, candidateSchema); err != nil {
		if ctx.Err() != nil {
			return err
		}
		logger.Error("release terminal teardown: drop failed (best-effort)",
			"binding", label, "release_id", releaseID,
			"candidate_schema", candidateSchema, "error", err)
		return nil
	}
	logger.Info("release terminal teardown: candidate schema dropped",
		"binding", label, "release_id", releaseID,
		"candidate_schema", candidateSchema)
	return nil
}

// NewReleaseRejectedTeardownBinding returns a pkg/redis.MessageHandler that
// consumes release.rejected:v1 and drops the release's candidate schema if
// present. Mirrors NewValidationResultTeardownBinding's terminal-row teardown
// for the failed seed-build path. A missing or unparseable payload is logged
// and acknowledged.
func NewReleaseRejectedTeardownBinding(cleaner ports.CandidateSchemaCleaner, logger *slog.Logger) pkgredis.MessageHandler {
	const label = "release-rejected teardown"
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
		return dropCandidateSchema(ctx, cleaner, logger, label, dto.ReleaseID, dto.CandidateSchema)
	}
}

// NewReleasePromotedTeardownBinding returns a pkg/redis.MessageHandler that
// consumes release.promoted:v2 and drops the release's candidate schema. It
// reads release_id and candidate_schema from the envelope payload and nothing
// else. A real promotion names its candidate schema: the drop is a no-op for a
// normal validation release (already torn down by the validation.result:v1
// terminal row) and the live teardown for the seed-only zero-validation path.
// A topology announced with announce-topology names none, so nothing is
// dropped. An entry without a readable envelope is logged and acknowledged,
// like an unreadable release.rejected:v1 payload: the teardown never
// dead-letters.
func NewReleasePromotedTeardownBinding(cleaner ports.CandidateSchemaCleaner, logger *slog.Logger) pkgredis.MessageHandler {
	const label = "release-promoted teardown"
	return func(ctx context.Context, msg goredis.XMessage) error {
		_, p, err := events.DecodeReleasePromoted(stringFields(msg.Values))
		if err != nil {
			logger.Warn("release terminal teardown: unreadable entry",
				"binding", label, "message_id", msg.ID, "error", err)
			return nil
		}
		return dropCandidateSchema(ctx, cleaner, logger, label, p.ReleaseID, p.CandidateSchema)
	}
}

// stringFields returns the entry's string-valued fields, the form the
// pkg/events envelope decoders read.
func stringFields(values map[string]any) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}
