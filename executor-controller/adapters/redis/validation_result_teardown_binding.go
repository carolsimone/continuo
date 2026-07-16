// executor-controller/adapters/redis/validation_result_teardown_binding.go
package redis

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/service/ports"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewValidationResultTeardownBinding returns a pkg/redis.MessageHandler that
// consumes validation.result:v1 — the merged stream that carries both per-node
// ("kind":"node") and terminal ("kind":"complete") validation rows — and drops
// the release's candidate schema from the dbt warehouse once the terminal row
// arrives. Per-node rows are acked without any teardown action: dropping the
// candidate schema mid-validation would break the still-in-flight sibling
// nodes. The terminal kind is emitted from two call sites (the node-completed
// handler and the dispatcher's fail-at-dispatch path), so reacting to the
// published stream is a single, path-independent trigger that runs outside any
// write transaction. Teardown is best-effort: a parse or cleaner failure is
// logged and the message is ACKed (returns nil), because a leftover candidate
// schema must never block a release decision.
func NewValidationResultTeardownBinding(cleaner ports.CandidateSchemaCleaner, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		raw := stringField(msg.Values, "payload")
		if raw == "" {
			logger.Warn("validation.result teardown: missing payload", "message_id", msg.ID)
			return nil
		}
		var envelope struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
			logger.Warn("validation.result teardown: bad payload", "message_id", msg.ID, "error", err)
			return nil
		}
		if envelope.Kind != "complete" {
			// Per-node rows ("kind":"node") and anything unrecognized carry no
			// teardown action; ack and move on.
			return nil
		}
		var dto struct {
			ReleaseID       string `json:"release_id"`
			CandidateSchema string `json:"candidate_schema"`
		}
		if err := json.Unmarshal([]byte(raw), &dto); err != nil {
			logger.Warn("validation.result teardown: bad complete payload", "message_id", msg.ID, "error", err)
			return nil
		}
		if dto.CandidateSchema == "" {
			logger.Info("validation.result teardown: no candidate_schema, skipping", "release_id", dto.ReleaseID)
			return nil
		}
		if err := cleaner.DropCandidateSchema(ctx, dto.CandidateSchema); err != nil {
			logger.Error("validation.result teardown: drop failed (best-effort)",
				"release_id", dto.ReleaseID, "candidate_schema", dto.CandidateSchema, "error", err)
			return nil
		}
		logger.Info("validation.result teardown: candidate schema dropped",
			"release_id", dto.ReleaseID, "candidate_schema", dto.CandidateSchema)
		return nil
	}
}
