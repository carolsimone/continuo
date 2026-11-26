package redis

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewReleaseCheckFinishedTeardownBinding returns a pkg/redis.MessageHandler
// that consumes release_check.finished:v1 — emitted by release-controller for
// every terminal decision of every release check, a candidate release's or a
// fix's alike — and drops the check's candidate schema from the warehouse.
// Validation builds are --empty, so the schema is disposable whatever the
// outcome. For a check that reached the validation leg the schema is
// normally gone already (the validation.result:v1 terminal row drops it
// first), and the drop here is an idempotent no-op; for a check that ended
// before or without that leg — a seed-build failure, a seed-only pass — this
// is the drop that actually reclaims the schema.
//
// Teardown is best-effort: a parse or cleaner failure is logged and the
// message ACKed (returns nil), because a leftover candidate schema must never
// block a release check's terminal decision. A cleaner failure that arrives with a done
// ctx (shutdown, or the handler deadline) is returned instead, so the drop is
// retried rather than acknowledged unfinished.
func NewReleaseCheckFinishedTeardownBinding(cleaner ports.CandidateSchemaCleaner, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		raw := stringField(msg.Values, "payload")
		if raw == "" {
			logger.Warn("release check teardown: missing payload", "message_id", msg.ID)
			return nil
		}
		var dto struct {
			CheckID         string `json:"check_id"`
			CheckKind       string `json:"check_kind"`
			Outcome         string `json:"outcome"`
			CandidateSchema string `json:"candidate_schema"`
		}
		if err := json.Unmarshal([]byte(raw), &dto); err != nil {
			logger.Warn("release check teardown: bad payload", "message_id", msg.ID, "error", err)
			return nil
		}
		if dto.CandidateSchema == "" {
			logger.Info("release check teardown: no candidate_schema, skipping", "check_id", dto.CheckID)
			return nil
		}
		if err := cleaner.DropCandidateSchema(ctx, dto.CandidateSchema); err != nil {
			if ctx.Err() != nil {
				return err
			}
			logger.Error("release check teardown: drop failed (best-effort)",
				"check_id", dto.CheckID, "check_kind", dto.CheckKind, "outcome", dto.Outcome,
				"candidate_schema", dto.CandidateSchema, "error", err)
			return nil
		}
		logger.Info("release check teardown: candidate schema dropped",
			"check_id", dto.CheckID, "check_kind", dto.CheckKind, "outcome", dto.Outcome,
			"candidate_schema", dto.CandidateSchema)
		return nil
	}
}
