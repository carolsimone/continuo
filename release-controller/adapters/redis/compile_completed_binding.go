package redis

import (
	"context"
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
)

// compileResultDTO is the JSON shape of a compile.completed:v1 payload. It
// carries the json tags for the aggregated compile outcome so the handler
// input stays tag-free.
type compileResultDTO struct {
	ReleaseID   string               `json:"release_id"`
	Status      string               `json:"status"` // "ok" | "failed"
	PerNode     []stageNodeResultDTO `json:"per_node"`
	ErrorDetail string               `json:"error_detail,omitempty"`
}

// toInput maps the decoded wire DTO to the handler's input.
func (d compileResultDTO) toInput() handlers.HandleCompileResultInput {
	return handlers.HandleCompileResultInput{
		ReleaseID:   d.ReleaseID,
		Status:      d.Status,
		PerNode:     stageNodeResultsToInput(d.PerNode),
		ErrorDetail: d.ErrorDetail,
	}
}

// NewCompileCompletedConsumer consumes compile.completed:v1 and dispatches to
// handlers.HandleCompileResult (advancing to release.requested or rejecting).
func NewCompileCompletedConsumer(rc *goredis.Client, deps *handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	return pkgredis.NewStreamConsumer(rc, streams.CompileCompletedV1,
		streams.ReleaseControllerCompileCompleted, newCompileCompletedHandler(deps, logger), logger)
}

func newCompileCompletedHandler(deps *handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		var dto compileResultDTO
		if err := decodePayload(msg, &dto); err != nil {
			logger.Error("compile.completed:v1 decode failure — discarding", "message_id", msg.ID, "error", err)
			return nil
		}
		if err := handlers.HandleCompileResult(ctx, deps, dto.toInput()); err != nil {
			return err
		}
		// Advance the queue after every compile result. On the success path the
		// release stays active (Parsing → will receive manifest.loaded.candidate),
		// so this is a no-op. On the failure path the release was just Rejected,
		// so this unblocks the next queued candidate.
		if err := handlers.AdvanceQueue(ctx, deps); err != nil {
			logger.Error("advance queue after compile.completed", "error", err)
			// Non-fatal: the periodic trigger in main.go will eventually advance
			// the queue; do not fail the ACK for this release.
		}
		return nil
	}
}
