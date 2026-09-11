package redis

import (
	"context"
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
)

// seedBuildResultDTO is the JSON shape of a seed.build.completed:v1 payload.
// It carries the json tags for the aggregated seed-build outcome so the
// handler input stays tag-free.
type seedBuildResultDTO struct {
	ReleaseID   string               `json:"release_id"`
	Status      string               `json:"status"` // "ok" | "failed"
	PerNode     []stageNodeResultDTO `json:"per_node"`
	ErrorDetail string               `json:"error_detail,omitempty"`
}

// toInput maps the decoded wire DTO to the handler's input.
func (d seedBuildResultDTO) toInput() handlers.HandleSeedBuildResultInput {
	return handlers.HandleSeedBuildResultInput{
		ReleaseID:   d.ReleaseID,
		Status:      d.Status,
		PerNode:     stageNodeResultsToInput(d.PerNode),
		ErrorDetail: d.ErrorDetail,
	}
}

// NewSeedBuildCompletedConsumer consumes seed.build.completed:v1 and dispatches to
// handlers.HandleSeedBuildResult (advancing to validation.requested or rejecting).
func NewSeedBuildCompletedConsumer(rc *goredis.Client, deps *handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	return pkgredis.NewStreamConsumer(rc, streams.SeedBuildCompletedV1,
		streams.ReleaseControllerSeedBuildCompleted, newSeedBuildCompletedHandler(deps, logger), logger)
}

func newSeedBuildCompletedHandler(deps *handlers.Deps, logger *slog.Logger) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		var dto seedBuildResultDTO
		if err := decodePayload(msg, &dto); err != nil {
			logger.Error("seed.build.completed:v1 decode failure — discarding", "message_id", msg.ID, "error", err)
			return nil
		}
		if err := handlers.HandleSeedBuildResult(ctx, deps, dto.toInput()); err != nil {
			return err
		}
		// Advance the queue after every seed-build result. On the success path the
		// release stays active (Validating or promoted), so this is a no-op. On the
		// failure path the release was just Rejected, so this unblocks the next
		// queued candidate.
		if err := handlers.AdvanceQueue(ctx, deps); err != nil {
			logger.Error("advance queue after seed.build.completed", "error", err)
			// Non-fatal: the periodic trigger in main.go will eventually advance
			// the queue; do not fail the ACK for this release.
		}
		return nil
	}
}
