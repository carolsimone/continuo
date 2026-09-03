package redis

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/service/ports"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewPipelineRunFinishedTeardownBinding returns a pkg/redis.MessageHandler
// that consumes pipeline.run.finished:v1 — emitted by release-controller for
// every terminal decision of every pipeline run, candidate release or
// fix-verification run alike — and drops the run's candidate schema from the
// warehouse. Validation runs are --empty, so the schema is disposable whatever
// the outcome. For a run that reached the validation leg the schema is
// normally gone already (the validation.result:v1 terminal row drops it
// first), and the drop here is an idempotent no-op; for a run that ended
// before or without that leg — a seed-build failure, a seed-only pass — this
// is the drop that actually reclaims the schema.
//
// Teardown is best-effort: a parse or cleaner failure is logged and the
// message ACKed (returns nil), because a leftover candidate schema must never
// block a run's terminal decision.
func NewPipelineRunFinishedTeardownBinding(cleaner ports.CandidateSchemaCleaner, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		raw := stringField(msg.Values, "payload")
		if raw == "" {
			logger.Warn("pipeline run teardown: missing payload", "message_id", msg.ID)
			return nil
		}
		var dto struct {
			RunID           string `json:"run_id"`
			RunKind         string `json:"run_kind"`
			Outcome         string `json:"outcome"`
			CandidateSchema string `json:"candidate_schema"`
		}
		if err := json.Unmarshal([]byte(raw), &dto); err != nil {
			logger.Warn("pipeline run teardown: bad payload", "message_id", msg.ID, "error", err)
			return nil
		}
		if dto.CandidateSchema == "" {
			logger.Info("pipeline run teardown: no candidate_schema, skipping", "run_id", dto.RunID)
			return nil
		}
		if err := cleaner.DropCandidateSchema(ctx, dto.CandidateSchema); err != nil {
			logger.Error("pipeline run teardown: drop failed (best-effort)",
				"run_id", dto.RunID, "run_kind", dto.RunKind, "outcome", dto.Outcome,
				"candidate_schema", dto.CandidateSchema, "error", err)
			return nil
		}
		logger.Info("pipeline run teardown: candidate schema dropped",
			"run_id", dto.RunID, "run_kind", dto.RunKind, "outcome", dto.Outcome,
			"candidate_schema", dto.CandidateSchema)
		return nil
	}
}
